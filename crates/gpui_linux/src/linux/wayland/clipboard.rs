use std::{
    fs::File,
    io::{ErrorKind, Write},
    os::fd::{AsFd, BorrowedFd, OwnedFd},
    sync::{
        Arc, Weak,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

use calloop::{LoopHandle, PostAction};
use filedescriptor::{FileDescriptor, Pipe};
use futures::{AsyncReadExt, FutureExt, StreamExt, stream::FuturesUnordered};
use strum::IntoEnumIterator;
use wayland_client::{Connection, protocol::wl_data_offer::WlDataOffer};
use wayland_protocols::wp::primary_selection::zv1::client::zwp_primary_selection_offer_v1::ZwpPrimarySelectionOfferV1;

use crate::linux::platform::{PIPE_READ_TIMEOUT, read_fd_with_timeout};
use gpui::{
    BackgroundExecutor, ClipboardEntry, ClipboardItem, ClipboardReadError, ClipboardReadMode,
    ClipboardRevision, ClipboardSnapshot, ClipboardSnapshotRead, ClipboardSnapshotResolver,
    ForegroundExecutor, Image, ImageFormat, WeakClipboardSnapshotRead, hash,
};
use uuid::Uuid;

/// Text mime types that we'll offer to other programs.
pub(crate) const TEXT_MIME_TYPES: [&str; 3] =
    ["text/plain;charset=utf-8", "UTF8_STRING", "text/plain"];
pub(crate) const FILE_LIST_MIME_TYPE: &str = "text/uri-list";

/// Text mime types that we'll accept from other programs.
pub(crate) const ALLOWED_TEXT_MIME_TYPES: [&str; 2] = ["text/plain;charset=utf-8", "UTF8_STRING"];

pub(crate) struct Clipboard {
    connection: Connection,
    loop_handle: LoopHandle<'static, ()>,
    keyboard_focus: Option<ClipboardFocusToken>,
    focus_sync: Option<ClipboardFocusToken>,
    selection_readiness: SelectionReadiness,
    primary_readiness: SelectionReadiness,
    snapshot_reader: SnapshotReader,
    revision: Option<ClipboardRevision>,
    primary_revision: Option<ClipboardRevision>,
    sources: Vec<Weak<ClipboardSource>>,
    primary_sources: Vec<Weak<ClipboardSource>>,

    // Internal clipboard
    contents: Option<Arc<ClipboardSource>>,
    primary_contents: Option<Arc<ClipboardSource>>,

    // External clipboard
    cached_read: Option<ClipboardItem>,
    current_offer: Option<DataOffer<WlDataOffer>>,
    cached_primary_read: Option<ClipboardItem>,
    current_primary_offer: Option<DataOffer<ZwpPrimarySelectionOfferV1>>,
}

#[derive(Clone)]
pub(crate) struct ClipboardFocusToken(Arc<()>);

#[derive(Default)]
#[must_use]
pub(crate) struct ClipboardCompletions(Vec<Box<dyn FnOnce()>>);

impl ClipboardCompletions {
    pub fn resolve(self) {
        for complete in self.0 {
            complete();
        }
    }

    fn extend(&mut self, other: Self) {
        self.0.extend(other.0);
    }
}

#[derive(Clone, Debug)]
pub(crate) struct ClipboardSource {
    pub revision: ClipboardRevision,
    pub mime_type: String,
    item: ClipboardItem,
}

impl ClipboardSource {
    fn new(item: ClipboardItem) -> Arc<Self> {
        Arc::new(Self {
            revision: ClipboardRevision::default(),
            mime_type: format!("application/x-gpui-clipboard-{}", Uuid::new_v4()),
            item,
        })
    }

    fn for_offer<T: ReceiveData>(
        sources: &mut Vec<Weak<Self>>,
        offer: Option<&DataOffer<T>>,
    ) -> Option<Arc<Self>> {
        let mut offered = false;
        let mut latest = None;
        sources.retain(|source| {
            let Some(source) = source.upgrade() else {
                return false;
            };
            offered |= offer.is_some_and(|offer| offer.has_mime_type(&source.mime_type));
            latest = Some(source);
            true
        });
        if offered { latest } else { None }
    }
}

pub(crate) trait ReceiveData {
    fn receive_data(&self, mime_type: String, fd: BorrowedFd<'_>);
}

impl ReceiveData for WlDataOffer {
    fn receive_data(&self, mime_type: String, fd: BorrowedFd<'_>) {
        self.receive(mime_type, fd);
    }
}

impl ReceiveData for ZwpPrimarySelectionOfferV1 {
    fn receive_data(&self, mime_type: String, fd: BorrowedFd<'_>) {
        self.receive(mime_type, fd);
    }
}

#[derive(Clone, Debug)]
/// Wrapper for `WlDataOffer` and `ZwpPrimarySelectionOfferV1`, used to help track mime types.
pub(crate) struct DataOffer<T: ReceiveData> {
    pub inner: T,
    mime_types: Vec<String>,
}

impl<T: ReceiveData> DataOffer<T> {
    pub fn new(offer: T) -> Self {
        Self {
            inner: offer,
            mime_types: Vec::new(),
        }
    }

    pub fn add_mime_type(&mut self, mime_type: String) {
        self.mime_types.push(mime_type)
    }

    fn has_mime_type(&self, mime_type: &str) -> bool {
        self.mime_types.iter().any(|t| t == mime_type)
    }

    fn receive_bytes(
        &self,
        connection: &Connection,
        mime_type: &str,
    ) -> anyhow::Result<FileDescriptor> {
        let pipe = Pipe::new()?;
        self.inner
            .receive_data(mime_type.to_owned(), pipe.write.as_fd());
        drop(pipe.write);
        match connection.flush() {
            Ok(()) => {}
            Err(wayland_client::backend::WaylandError::Io(error))
                if error.kind() == ErrorKind::WouldBlock => {}
            Err(error) => return Err(error.into()),
        }
        Ok(pipe.read)
    }

    fn read_bytes(&self, connection: &Connection, mime_type: &str) -> Option<Vec<u8>> {
        let result = self
            .receive_bytes(connection, mime_type)
            .and_then(|fd| read_fd_with_timeout(fd, PIPE_READ_TIMEOUT));
        match result {
            Ok(bytes) => Some(bytes),
            Err(err) => {
                log::error!("error reading clipboard pipe: {err:?}");
                None
            }
        }
    }

    fn read_text(&self, connection: &Connection) -> Option<ClipboardItem> {
        let mime_type = self.mime_types.iter().find(|&mime_type| {
            ALLOWED_TEXT_MIME_TYPES
                .iter()
                .any(|&allowed| allowed == mime_type)
        })?;
        let bytes = self.read_bytes(connection, mime_type)?;
        let text_content = match String::from_utf8(bytes) {
            Ok(content) => content,
            Err(e) => {
                log::error!("Failed to convert clipboard content to UTF-8: {}", e);
                return None;
            }
        };

        // Normalize the text to unix line endings, otherwise
        // copying from eg: firefox inserts a lot of blank
        // lines, and that is super annoying.
        let result = text_content.replace("\r\n", "\n");
        Some(ClipboardItem::new_string(result))
    }

    fn read_image(&self, connection: &Connection) -> Option<ClipboardItem> {
        for format in ImageFormat::iter() {
            let mime_type = format.mime_type();
            if !self.has_mime_type(mime_type) {
                continue;
            }

            if let Some(bytes) = self.read_bytes(connection, mime_type) {
                let id = hash(&bytes);
                return Some(ClipboardItem {
                    entries: vec![ClipboardEntry::Image(Image { format, bytes, id })],
                });
            }
        }
        None
    }
}

impl Clipboard {
    pub fn new(
        connection: Connection,
        loop_handle: LoopHandle<'static, ()>,
        background_executor: BackgroundExecutor,
        foreground_executor: ForegroundExecutor,
    ) -> Self {
        Self {
            connection,
            loop_handle,
            snapshot_reader: SnapshotReader {
                background_executor,
                foreground_executor,
                budget: Arc::new(SnapshotBudget::default()),
                timeout: PIPE_READ_TIMEOUT,
            },
            keyboard_focus: None,
            focus_sync: None,
            selection_readiness: SelectionReadiness::default(),
            primary_readiness: SelectionReadiness::default(),
            revision: Some(ClipboardRevision::default()),
            primary_revision: Some(ClipboardRevision::default()),
            sources: Vec::new(),
            primary_sources: Vec::new(),

            contents: None,
            primary_contents: None,

            cached_read: None,
            current_offer: None,
            cached_primary_read: None,
            current_primary_offer: None,
        }
    }

    pub fn set(&mut self, item: ClipboardItem) -> Arc<ClipboardSource> {
        let source = ClipboardSource::new(item);
        self.sources.retain(|source| source.strong_count() > 0);
        self.sources.push(Arc::downgrade(&source));
        self.revision = Some(source.revision.clone());
        self.contents = Some(source.clone());
        self.selection_readiness.contents = None;
        self.cached_read = None;
        source
    }

    pub fn set_primary(&mut self, item: ClipboardItem) -> Arc<ClipboardSource> {
        let source = ClipboardSource::new(item);
        self.primary_sources
            .retain(|source| source.strong_count() > 0);
        self.primary_sources.push(Arc::downgrade(&source));
        self.primary_revision = Some(source.revision.clone());
        self.primary_contents = Some(source.clone());
        self.primary_readiness.contents = None;
        self.cached_primary_read = None;
        source
    }

    pub fn set_offer(
        &mut self,
        data_offer: Option<DataOffer<WlDataOffer>>,
    ) -> ClipboardCompletions {
        self.contents = ClipboardSource::for_offer(&mut self.sources, data_offer.as_ref());
        self.revision = Some(
            self.contents
                .as_ref()
                .map(|source| source.revision.clone())
                .unwrap_or_default(),
        );
        self.cached_read = None;
        self.current_offer = data_offer;
        self.selection_readiness.contents = None;
        let mut value = self.selection_value();
        self.selection_readiness.authority =
            if self.keyboard_focus.is_some() || self.current_offer.is_some() {
                SelectionAuthority::Ready
            } else {
                SelectionAuthority::Unknown
            };
        let completions =
            self.selection_readiness
                .complete(&mut value, &self.connection, &self.snapshot_reader);
        self.cached_read = value.cached;
        completions
    }

    pub fn set_primary_offer(
        &mut self,
        data_offer: Option<DataOffer<ZwpPrimarySelectionOfferV1>>,
    ) -> ClipboardCompletions {
        self.primary_contents =
            ClipboardSource::for_offer(&mut self.primary_sources, data_offer.as_ref());
        self.primary_revision = Some(
            self.primary_contents
                .as_ref()
                .map(|source| source.revision.clone())
                .unwrap_or_default(),
        );
        self.cached_primary_read = None;
        self.current_primary_offer = data_offer;
        self.primary_readiness.contents = None;
        let mut value = self.primary_value();
        self.primary_readiness.authority =
            if self.keyboard_focus.is_some() || self.current_primary_offer.is_some() {
                SelectionAuthority::Ready
            } else {
                SelectionAuthority::Unknown
            };
        let completions =
            self.primary_readiness
                .complete(&mut value, &self.connection, &self.snapshot_reader);
        self.cached_primary_read = value.cached;
        completions
    }

    pub fn keyboard_focus_gained(&mut self) -> ClipboardFocusToken {
        let token = ClipboardFocusToken(Arc::new(()));
        self.keyboard_focus = Some(token.clone());
        token
    }

    pub fn start_focus_sync(&mut self) -> Option<ClipboardFocusToken> {
        if self.focus_sync.is_some()
            || (self.selection_readiness.authority != SelectionAuthority::Unknown
                && self.primary_readiness.authority != SelectionAuthority::Unknown)
        {
            return None;
        }
        let token = self.keyboard_focus.clone()?;
        self.focus_sync = Some(token.clone());
        Some(token)
    }

    pub fn keyboard_focus_synced(&mut self, token: &ClipboardFocusToken) -> ClipboardCompletions {
        if !self
            .focus_sync
            .as_ref()
            .is_some_and(|pending| Arc::ptr_eq(&pending.0, &token.0))
        {
            return ClipboardCompletions::default();
        }
        self.focus_sync = None;
        let mut completions = ClipboardCompletions::default();
        if self
            .keyboard_focus
            .as_ref()
            .is_some_and(|focus| Arc::ptr_eq(&focus.0, &token.0))
        {
            let mut value = self.selection_value();
            completions.extend(self.selection_readiness.confirm(
                &mut value,
                &self.connection,
                &self.snapshot_reader,
            ));
            self.cached_read = value.cached;
            let mut value = self.primary_value();
            completions.extend(self.primary_readiness.confirm(
                &mut value,
                &self.connection,
                &self.snapshot_reader,
            ));
            self.cached_primary_read = value.cached;
        }
        completions
    }

    pub fn keyboard_focus_timed_out(
        &mut self,
        token: &ClipboardFocusToken,
    ) -> ClipboardCompletions {
        let mut completions = ClipboardCompletions::default();
        if self
            .keyboard_focus
            .as_ref()
            .is_some_and(|focus| Arc::ptr_eq(&focus.0, &token.0))
        {
            completions.extend(self.selection_readiness.fail());
            completions.extend(self.primary_readiness.fail());
        }
        completions
    }

    pub fn keyboard_focus_lost(&mut self) -> ClipboardCompletions {
        self.keyboard_focus = None;
        let mut completions = self.selection_readiness.fail();
        completions.extend(self.primary_readiness.fail());
        self.selection_readiness.authority = SelectionAuthority::Unknown;
        self.primary_readiness.authority = SelectionAuthority::Unknown;
        completions
    }

    pub fn can_publish(&self, primary: bool) -> bool {
        self.keyboard_focus.is_some()
            && if primary {
                self.primary_readiness.authority == SelectionAuthority::Ready
            } else {
                self.selection_readiness.authority == SelectionAuthority::Ready
            }
    }

    pub fn capture_snapshot(&mut self, mode: ClipboardReadMode) -> ClipboardSnapshotRead {
        let mut value = self.selection_value();
        let read = self.selection_readiness.capture(
            self.keyboard_focus.is_some(),
            mode,
            &mut value,
            &self.connection,
            &self.snapshot_reader,
        );
        self.cached_read = value.cached;
        read
    }

    pub fn capture_primary_snapshot(&mut self, mode: ClipboardReadMode) -> ClipboardSnapshotRead {
        let mut value = self.primary_value();
        let read = self.primary_readiness.capture(
            self.keyboard_focus.is_some(),
            mode,
            &mut value,
            &self.connection,
            &self.snapshot_reader,
        );
        self.cached_primary_read = value.cached;
        read
    }

    pub fn cancel(&mut self, source: &ClipboardSource) {
        self.sources.retain(|current| {
            current
                .upgrade()
                .is_some_and(|current| current.revision != source.revision)
        });
        if self
            .contents
            .as_ref()
            .is_some_and(|current| current.revision == source.revision)
        {
            self.contents = None;
            self.selection_readiness.contents = None;
            self.current_offer = None;
            self.cached_read = None;
            self.revision = None;
        }
    }

    pub fn cancel_primary(&mut self, source: &ClipboardSource) {
        self.primary_sources.retain(|current| {
            current
                .upgrade()
                .is_some_and(|current| current.revision != source.revision)
        });
        if self
            .primary_contents
            .as_ref()
            .is_some_and(|current| current.revision == source.revision)
        {
            self.primary_contents = None;
            self.primary_readiness.contents = None;
            self.current_primary_offer = None;
            self.cached_primary_read = None;
            self.primary_revision = None;
        }
    }

    pub fn send(&self, source: &ClipboardSource, fd: OwnedFd) {
        if let Some(text) = source.item.text() {
            self.send_bytes(fd, text.into_bytes());
        }
    }

    pub fn read_snapshot(&mut self, mode: ClipboardReadMode) -> ClipboardSnapshot {
        if !self.can_publish(false) && self.contents.is_none() {
            return ClipboardSnapshot::default();
        }
        ClipboardSnapshot {
            item: (mode == ClipboardReadMode::Contents)
                .then(|| self.read())
                .flatten(),
            revision: self.revision.clone(),
        }
    }

    pub fn read_primary_snapshot(&mut self, mode: ClipboardReadMode) -> ClipboardSnapshot {
        if !self.can_publish(true) && self.primary_contents.is_none() {
            return ClipboardSnapshot::default();
        }
        ClipboardSnapshot {
            item: (mode == ClipboardReadMode::Contents)
                .then(|| self.read_primary())
                .flatten(),
            revision: self.primary_revision.clone(),
        }
    }

    pub fn read(&mut self) -> Option<ClipboardItem> {
        if let Some(source) = &self.contents {
            return Some(source.item.clone());
        }
        if !self.can_publish(false) {
            return None;
        }
        let offer = self.current_offer.as_ref()?;
        if let Some(cached) = self.cached_read.clone() {
            return Some(cached);
        }

        let item = offer
            .read_text(&self.connection)
            .or_else(|| offer.read_image(&self.connection))?;

        self.cached_read = Some(item.clone());
        Some(item)
    }

    pub fn read_primary(&mut self) -> Option<ClipboardItem> {
        if let Some(source) = &self.primary_contents {
            return Some(source.item.clone());
        }
        if !self.can_publish(true) {
            return None;
        }
        let offer = self.current_primary_offer.as_ref()?;
        if let Some(cached) = self.cached_primary_read.clone() {
            return Some(cached);
        }

        let item = offer
            .read_text(&self.connection)
            .or_else(|| offer.read_image(&self.connection))?;

        self.cached_primary_read = Some(item.clone());
        Some(item)
    }

    pub fn send_bytes(&self, fd: OwnedFd, bytes: Vec<u8>) {
        let mut written = 0;
        self.loop_handle
            .insert_source(
                calloop::generic::Generic::new(
                    File::from(fd),
                    calloop::Interest::WRITE,
                    calloop::Mode::Level,
                ),
                move |_, file, _| {
                    let file = unsafe { file.get_mut() };
                    loop {
                        match file.write(&bytes[written..]) {
                            Ok(n) if written + n == bytes.len() => {
                                written += n;
                                break Ok(PostAction::Remove);
                            }
                            Ok(n) => written += n,
                            Err(err) if err.kind() == ErrorKind::WouldBlock => {
                                break Ok(PostAction::Continue);
                            }
                            Err(_) => break Ok(PostAction::Remove),
                        }
                    }
                },
            )
            .unwrap();
    }

    fn selection_value(&mut self) -> SelectionValue<WlDataOffer> {
        SelectionValue {
            source: self.contents.clone(),
            revision: self.revision.clone(),
            offer: self.current_offer.clone(),
            cached: self.cached_read.take(),
        }
    }

    fn primary_value(&mut self) -> SelectionValue<ZwpPrimarySelectionOfferV1> {
        SelectionValue {
            source: self.primary_contents.clone(),
            revision: self.primary_revision.clone(),
            offer: self.current_primary_offer.clone(),
            cached: self.cached_primary_read.take(),
        }
    }
}

struct SelectionValue<T: ReceiveData> {
    source: Option<Arc<ClipboardSource>>,
    revision: Option<ClipboardRevision>,
    offer: Option<DataOffer<T>>,
    cached: Option<ClipboardItem>,
}

impl<T: ReceiveData> SelectionValue<T> {
    fn capture(
        &self,
        mode: ClipboardReadMode,
        connection: &Connection,
        reader: &SnapshotReader,
    ) -> ClipboardSnapshotRead {
        let Some(revision) = self.revision.clone() else {
            return ClipboardSnapshotRead::failed(ClipboardReadError::Unavailable);
        };
        let item = if mode == ClipboardReadMode::RevisionOnly {
            None
        } else if let Some(source) = &self.source {
            Some(source.item.clone())
        } else if let Some(item) = &self.cached {
            let size = item.entries.iter().try_fold(0usize, |size, entry| {
                size.checked_add(match entry {
                    ClipboardEntry::String(text) => text.text().len(),
                    ClipboardEntry::Image(image) => image.bytes.len(),
                    ClipboardEntry::ExternalPaths(_) => return None,
                })
            });
            if size.is_none_or(|size| size > SNAPSHOT_PAYLOAD_LIMIT) {
                let (read, resolver) = ClipboardSnapshotRead::pending();
                resolver.resolve_with_revision(
                    Err(ClipboardReadError::UnsupportedContent),
                    Some(revision),
                );
                return read;
            }
            Some(item.clone())
        } else if let Some(offer) = &self.offer {
            return reader.capture(offer, connection, revision);
        } else {
            None
        };
        ClipboardSnapshotRead::ready(ClipboardSnapshot {
            item,
            revision: Some(revision),
        })
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum SelectionAuthority {
    Unknown,
    Ready,
    Failed,
}

struct SelectionReadiness {
    authority: SelectionAuthority,
    captures: Vec<(ClipboardReadMode, ClipboardSnapshotResolver)>,
    contents: Option<(Option<ClipboardRevision>, WeakClipboardSnapshotRead)>,
}

impl Default for SelectionReadiness {
    fn default() -> Self {
        Self {
            authority: SelectionAuthority::Unknown,
            captures: Vec::new(),
            contents: None,
        }
    }
}

impl SelectionReadiness {
    fn capture<T: ReceiveData>(
        &mut self,
        focused: bool,
        mode: ClipboardReadMode,
        value: &mut SelectionValue<T>,
        connection: &Connection,
        reader: &SnapshotReader,
    ) -> ClipboardSnapshotRead {
        if (focused && self.authority == SelectionAuthority::Ready)
            || (!focused && value.source.is_some() && mode == ClipboardReadMode::Contents)
        {
            return self.admit(mode, value, connection, reader);
        }
        if !focused || self.authority == SelectionAuthority::Failed {
            return ClipboardSnapshotRead::failed(ClipboardReadError::Unavailable);
        }
        let (read, resolver) = ClipboardSnapshotRead::pending();
        self.captures.push((mode, resolver));
        if mode == ClipboardReadMode::RevisionOnly {
            read.revision_only()
        } else {
            read
        }
    }

    fn admit<T: ReceiveData>(
        &mut self,
        mode: ClipboardReadMode,
        value: &SelectionValue<T>,
        connection: &Connection,
        reader: &SnapshotReader,
    ) -> ClipboardSnapshotRead {
        if mode == ClipboardReadMode::RevisionOnly {
            return value.capture(mode, connection, reader);
        }
        if let Some((revision, read)) = &self.contents
            && *revision == value.revision
            && let Some(read) = read.upgrade()
            && !read.is_failed()
        {
            return read;
        }
        let read = value.capture(mode, connection, reader);
        self.contents = Some((value.revision.clone(), read.downgrade()));
        read
    }

    fn confirm<T: ReceiveData>(
        &mut self,
        value: &mut SelectionValue<T>,
        connection: &Connection,
        reader: &SnapshotReader,
    ) -> ClipboardCompletions {
        self.authority = SelectionAuthority::Ready;
        self.complete(value, connection, reader)
    }

    fn fail(&mut self) -> ClipboardCompletions {
        if self.authority == SelectionAuthority::Unknown {
            self.authority = SelectionAuthority::Failed;
        }
        ClipboardCompletions(
            std::mem::take(&mut self.captures)
                .into_iter()
                .map(|(_, resolver)| {
                    Box::new(move || resolver.resolve(Err(ClipboardReadError::Unavailable)))
                        as Box<dyn FnOnce()>
                })
                .collect(),
        )
    }

    fn complete<T: ReceiveData>(
        &mut self,
        value: &mut SelectionValue<T>,
        connection: &Connection,
        reader: &SnapshotReader,
    ) -> ClipboardCompletions {
        ClipboardCompletions(
            std::mem::take(&mut self.captures)
                .into_iter()
                .filter(|(_, resolver)| !resolver.is_canceled())
                .map(|(mode, resolver)| {
                    let read = self.admit(mode, value, connection, reader);
                    let revision = value.revision.clone();
                    let executor = reader.foreground_executor.clone();
                    Box::new(move || {
                        if revision.is_some() {
                            resolver.resolve_revision(revision);
                        }
                        if read.is_ready() {
                            read.on_ready(move |result| resolver.resolve(result));
                        } else {
                            resolver.resolve_async(&executor, async move { read.wait().await });
                        }
                    }) as Box<dyn FnOnce()>
                })
                .collect(),
        )
    }
}

const SNAPSHOT_PAYLOAD_LIMIT: usize = 64 * 1024 * 1024;
const SNAPSHOT_TRANSFER_LIMIT: usize = 16;
const SNAPSHOT_CANDIDATE_LIMIT: usize = 10;

struct SnapshotReader {
    background_executor: BackgroundExecutor,
    foreground_executor: ForegroundExecutor,
    budget: Arc<SnapshotBudget>,
    timeout: Duration,
}

impl SnapshotReader {
    fn capture<T: ReceiveData>(
        &self,
        offer: &DataOffer<T>,
        connection: &Connection,
        revision: ClipboardRevision,
    ) -> ClipboardSnapshotRead {
        let (read, resolver) = ClipboardSnapshotRead::pending();
        resolver.resolve_revision(Some(revision.clone()));
        let formats = offer
            .mime_types
            .iter()
            .find(|mime_type| ALLOWED_TEXT_MIME_TYPES.contains(&mime_type.as_str()))
            .map(|mime_type| (mime_type.as_str(), None))
            .into_iter()
            .chain(
                ImageFormat::iter()
                    .filter(|format| offer.has_mime_type(format.mime_type()))
                    .map(|format| (format.mime_type(), Some(format))),
            )
            .collect::<Vec<_>>();
        if formats.is_empty() {
            resolver.resolve(Ok(Arc::new(ClipboardSnapshot {
                item: None,
                revision: Some(revision),
            })));
            return read;
        }
        if formats.len() > SNAPSHOT_CANDIDATE_LIMIT {
            resolver.resolve(Err(ClipboardReadError::UnsupportedContent));
            return read;
        }
        let permits = match self.budget.acquire(formats.len()) {
            Ok(permits) => permits,
            Err(error) => {
                resolver.resolve(Err(error));
                return read;
            }
        };
        let deadline = self.background_executor.now() + self.timeout;
        let timer = self.background_executor.timer(self.timeout);
        let candidates = formats
            .into_iter()
            .map(|(mime_type, format)| {
                let file = offer
                    .receive_bytes(connection, mime_type)
                    .and_then(|fd| Ok(File::from(fd.as_fd().try_clone_to_owned()?)))
                    .map_err(|error| {
                        log::error!("error initiating clipboard snapshot: {error}");
                        ClipboardReadError::Unavailable
                    });
                (file, format)
            })
            .collect::<Vec<_>>();
        let executor = self.background_executor.clone();
        #[cfg(test)]
        let budget = self.budget.clone();
        let task = self.background_executor.spawn(async move {
            let mut completed = std::iter::repeat_with(|| None)
                .take(candidates.len())
                .collect::<Vec<_>>();
            let mut candidates = candidates
                .into_iter()
                .zip(permits)
                .enumerate()
                .map(|(index, ((file, format), mut permit))| {
                    let executor = executor.clone();
                    async move {
                        let result = match file {
                            Ok(file) => {
                                read_snapshot_candidate(file, format, &mut permit, &executor, deadline)
                                    .await
                            }
                            Err(error) => Err(error),
                        };
                        (index, result.map(|item| (item, permit)))
                    }
                })
                .collect::<FuturesUnordered<_>>();
            let timer = timer.fuse();
            futures::pin_mut!(timer);
            let timed_out = loop {
                futures::select_biased! {
                    result = candidates.next() => {
                        let Some((index, result)) = result else {
                            break false;
                        };
                        completed[index] = Some(result);
                        #[cfg(test)]
                        if let Some(sender) = budget.completed.lock().take() {
                            sender.send(index).ok();
                        }
                        if completed.iter().take_while(|result| result.is_some()).flatten().any(Result::is_ok) {
                            break false;
                        }
                    }
                    _ = timer => {
                        log::warn!("clipboard snapshot exceeded its total transfer timeout");
                        break true;
                    }
                }
            };
            drop(candidates);
            let mut error = ClipboardReadError::Unavailable;
            for result in completed.into_iter().flatten() {
                match result {
                    Ok((item, permit)) => {
                        return Ok((
                            Arc::new(ClipboardSnapshot {
                                item: Some(item),
                                revision: Some(revision),
                            }),
                            permit,
                        ));
                    }
                    Err(failure) => error = failure,
                }
            }
            Err(if timed_out { ClipboardReadError::Unavailable } else { error })
        });
        resolver.resolve_async(&self.foreground_executor, async move {
            let (snapshot, _permit) = task.await?;
            Ok(snapshot)
        });
        read
    }
}

async fn read_snapshot_candidate(
    file: File,
    format: Option<ImageFormat>,
    permit: &mut SnapshotPermit,
    executor: &BackgroundExecutor,
    deadline: Instant,
) -> Result<ClipboardItem, ClipboardReadError> {
    let mut pipe = smol::Async::new(file).map_err(|error| {
        log::error!("error opening clipboard snapshot pipe: {error}");
        ClipboardReadError::Unavailable
    })?;
    let mut bytes = Vec::new();
    let mut length = 0;
    let mut probe = [0];
    loop {
        if executor.now() >= deadline {
            return Err(ClipboardReadError::Unavailable);
        }
        let full = length == bytes.len();
        let destination = if full {
            &mut probe[..]
        } else {
            &mut bytes[length..]
        };
        let count = match pipe.read(destination).await {
            Ok(0) => break,
            Ok(count) => count,
            Err(error) if error.kind() == ErrorKind::Interrupted => continue,
            Err(error) => {
                log::error!("error reading clipboard snapshot: {error}");
                return Err(ClipboardReadError::Unavailable);
            }
        };
        if full {
            if length == SNAPSHOT_PAYLOAD_LIMIT {
                log::warn!("clipboard snapshot exceeds the 64 MiB payload limit");
                return Err(ClipboardReadError::UnsupportedContent);
            }
            let capacity = (bytes.len() * 2).clamp(8192, SNAPSHOT_PAYLOAD_LIMIT);
            permit.reserve(capacity)?;
            bytes
                .try_reserve_exact(capacity - bytes.len())
                .map_err(|_| ClipboardReadError::Unavailable)?;
            permit.reserve(bytes.capacity())?;
            bytes.resize(capacity, 0);
            bytes[length] = probe[0];
        }
        length += count;
        #[cfg(test)]
        if let Some(sender) = permit.budget.progress.lock().take() {
            sender.send(length).ok();
        }
    }
    bytes.truncate(length);
    let item = if let Some(format) = format {
        let id = hash(&bytes);
        ClipboardItem {
            entries: vec![ClipboardEntry::Image(Image { format, bytes, id })],
        }
    } else {
        let mut previous = None;
        bytes.reverse();
        bytes.retain(|byte| {
            let retain = *byte != b'\r' || previous != Some(b'\n');
            previous = Some(*byte);
            retain
        });
        bytes.reverse();
        let text = String::from_utf8(bytes).map_err(|_| ClipboardReadError::UnsupportedContent)?;
        ClipboardItem::new_string(text)
    };
    if executor.now() >= deadline {
        return Err(ClipboardReadError::Unavailable);
    }
    Ok(item)
}

#[derive(Default)]
struct SnapshotBudget {
    bytes: AtomicUsize,
    transfers: AtomicUsize,
    #[cfg(test)]
    progress: parking_lot::Mutex<Option<futures::channel::oneshot::Sender<usize>>>,
    #[cfg(test)]
    completed: parking_lot::Mutex<Option<futures::channel::oneshot::Sender<usize>>>,
}

impl SnapshotBudget {
    fn acquire(self: &Arc<Self>, count: usize) -> Result<Vec<SnapshotPermit>, ClipboardReadError> {
        self.transfers
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |transfers| {
                transfers
                    .checked_add(count)
                    .filter(|total| *total <= SNAPSHOT_TRANSFER_LIMIT)
            })
            .map_err(|_| {
                log::warn!("clipboard snapshot exceeds the 16-pipe in-flight limit");
                ClipboardReadError::Unavailable
            })?;
        Ok((0..count)
            .map(|_| SnapshotPermit {
                budget: self.clone(),
                capacity: 0,
            })
            .collect())
    }
}

struct SnapshotPermit {
    budget: Arc<SnapshotBudget>,
    capacity: usize,
}

impl SnapshotPermit {
    fn reserve(&mut self, capacity: usize) -> Result<(), ClipboardReadError> {
        if capacity > SNAPSHOT_PAYLOAD_LIMIT {
            return Err(ClipboardReadError::UnsupportedContent);
        }
        if capacity < self.capacity {
            self.budget
                .bytes
                .fetch_sub(self.capacity - capacity, Ordering::Relaxed);
            self.capacity = capacity;
            return Ok(());
        }
        let additional = capacity - self.capacity;
        self.budget
            .bytes
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |bytes| {
                (bytes + additional <= SNAPSHOT_PAYLOAD_LIMIT).then_some(bytes + additional)
            })
            .map_err(|_| {
                log::warn!("clipboard snapshot exceeds the 64 MiB in-flight budget");
                ClipboardReadError::Unavailable
            })?;
        self.capacity += additional;
        Ok(())
    }
}

impl Drop for SnapshotPermit {
    fn drop(&mut self) {
        self.budget
            .bytes
            .fetch_sub(self.capacity, Ordering::Relaxed);
        self.budget.transfers.fetch_sub(1, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use calloop::EventLoop;
    use futures::AsyncWriteExt;
    use gpui::TestAppContext;
    use std::{
        cell::{Cell, RefCell},
        os::unix::net::UnixStream,
        rc::Rc,
    };
    use wayland_client::Proxy;

    #[gpui::test]
    fn test_clipboard_source_lifetimes(cx: &mut TestAppContext) {
        for primary in [false, true] {
            let (socket, _server) = UnixStream::pair().expect("socket pair");
            let connection = Connection::from_socket(socket).expect("test connection");
            let event_loop = EventLoop::try_new().expect("event loop");
            let mut clipboard = Clipboard::new(
                connection,
                event_loop.handle(),
                cx.background_executor.clone(),
                cx.foreground_executor.clone(),
            );
            let other_revision = if primary {
                clipboard.revision.clone()
            } else {
                clipboard.primary_revision.clone()
            };
            let first = set_test_source(&mut clipboard, primary, "A");
            let second = set_test_source(&mut clipboard, primary, "B");
            offer_test_source(&mut clipboard, primary, &first);
            assert_eq!(
                test_snapshot(&mut clipboard, primary, ClipboardReadMode::Contents),
                (Some("B".to_owned()), Some(second.revision.clone())),
            );
            if primary {
                clipboard.cancel_primary(&first);
            } else {
                clipboard.cancel(&first);
            }
            assert_eq!(
                test_snapshot(&mut clipboard, primary, ClipboardReadMode::Contents),
                (Some("B".to_owned()), Some(second.revision.clone())),
            );
            offer_test_source(&mut clipboard, primary, &first);
            assert_eq!(test_source_text(&clipboard, primary), None);
            offer_test_source(&mut clipboard, primary, &second);
            assert_eq!(test_source_text(&clipboard, primary).as_deref(), Some("B"));
            if primary {
                clipboard.cancel_primary(&second);
            } else {
                clipboard.cancel(&second);
            }
            let third = set_test_source(&mut clipboard, primary, "C");
            offer_test_source(&mut clipboard, primary, &second);
            assert_eq!(test_source_text(&clipboard, primary), None);
            offer_test_source(&mut clipboard, primary, &third);
            assert_eq!(test_source_text(&clipboard, primary).as_deref(), Some("C"));
            assert_eq!(
                if primary {
                    &clipboard.revision
                } else {
                    &clipboard.primary_revision
                },
                &other_revision,
            );
        }
    }

    #[gpui::test]
    fn test_clipboard_focus_provenance(cx: &mut TestAppContext) {
        for own_clipboard in [false, true] {
            for own_primary in [false, true] {
                let (socket, _server) = UnixStream::pair().expect("socket pair");
                let connection = Connection::from_socket(socket).expect("test connection");
                let event_loop = EventLoop::try_new().expect("event loop");
                let mut clipboard = Clipboard::new(
                    connection,
                    event_loop.handle(),
                    cx.background_executor.clone(),
                    cx.foreground_executor.clone(),
                );
                let sources = [
                    own_clipboard.then(|| set_test_source(&mut clipboard, false, "clipboard")),
                    own_primary.then(|| set_test_source(&mut clipboard, true, "primary")),
                ];
                let channels = [(false, "clipboard"), (true, "primary")];
                for ((primary, text), source) in channels.into_iter().zip(&sources) {
                    if let Some(source) = source {
                        offer_test_source(&mut clipboard, primary, source);
                    } else {
                        offer_test_external(&mut clipboard, primary, text);
                    }
                }
                let revisions = [
                    clipboard.revision.clone(),
                    clipboard.primary_revision.clone(),
                ];
                clipboard.keyboard_focus_gained();
                for ((primary, text), revision) in channels.into_iter().zip(&revisions) {
                    assert!(revision.is_some());
                    assert_eq!(
                        test_snapshot(&mut clipboard, primary, ClipboardReadMode::Contents),
                        (Some(text.to_owned()), revision.clone()),
                    );
                }

                clipboard.keyboard_focus_lost().resolve();
                clipboard.keyboard_focus_lost().resolve();
                for entering in [false, true] {
                    if entering {
                        clipboard.keyboard_focus_gained();
                    }
                    for (((primary, text), source), revision) in
                        channels.into_iter().zip(&sources).zip(&revisions)
                    {
                        let readable = source.is_some();
                        let expected_text = readable.then(|| text.to_owned());
                        let expected_revision = readable.then(|| revision.clone()).flatten();
                        for mode in [ClipboardReadMode::Contents, ClipboardReadMode::RevisionOnly] {
                            assert_eq!(
                                test_snapshot(&mut clipboard, primary, mode),
                                (
                                    (mode == ClipboardReadMode::Contents)
                                        .then(|| expected_text.clone())
                                        .flatten(),
                                    expected_revision.clone(),
                                ),
                            );
                        }
                        let item = if primary {
                            clipboard.read_primary()
                        } else {
                            clipboard.read()
                        };
                        assert_eq!(item.and_then(|item| item.text()), expected_text);
                        if let Some(source) = source {
                            assert_eq!(source.item.text().as_deref(), Some(text));
                        }
                    }
                }
                let captures = channels.map(|(primary, _)| {
                    let capture =
                        capture_test_snapshot(&mut clipboard, primary, ClipboardReadMode::Contents);
                    assert!(!capture.is_ready());
                    assert!(!clipboard.can_publish(primary));
                    capture
                });
                let token = clipboard.start_focus_sync().expect("focus sync");
                assert!(clipboard.start_focus_sync().is_none());
                clipboard.keyboard_focus_synced(&token).resolve();
                for (((primary, text), revision), capture) in
                    channels.into_iter().zip(&revisions).zip(captures)
                {
                    assert_eq!(
                        resolved_test_snapshot(&capture),
                        Ok((Some(text.to_owned()), revision.clone()))
                    );
                    assert!(clipboard.can_publish(primary));
                }
            }
        }
    }

    #[gpui::test]
    fn test_clipboard_focus_selection_events(cx: &mut TestAppContext) {
        for primary in [false, true] {
            let (socket, _server) = UnixStream::pair().expect("socket pair");
            let connection = Connection::from_socket(socket).expect("test connection");
            let event_loop = EventLoop::try_new().expect("event loop");
            let mut clipboard = Clipboard::new(
                connection,
                event_loop.handle(),
                cx.background_executor.clone(),
                cx.foreground_executor.clone(),
            );
            clipboard.keyboard_focus_gained();
            assert_eq!(
                test_snapshot(&mut clipboard, primary, ClipboardReadMode::Contents),
                (None, None),
            );
            let empty_capture =
                capture_test_snapshot(&mut clipboard, primary, ClipboardReadMode::RevisionOnly);
            assert!(!empty_capture.is_ready());
            let token = clipboard.start_focus_sync().expect("initial empty sync");
            clipboard.keyboard_focus_synced(&token).resolve();
            let (empty_item, empty_revision) =
                resolved_test_snapshot(&empty_capture).expect("authoritative empty selection");
            assert_eq!(empty_item, None);
            assert!(empty_revision.is_some());
            let other = set_test_source(&mut clipboard, !primary, "other channel");
            for offer_before_enter in [false, true] {
                offer_test_external(&mut clipboard, primary, "before");
                let (_, before) =
                    test_snapshot(&mut clipboard, primary, ClipboardReadMode::Contents);
                clipboard.keyboard_focus_lost().resolve();
                if offer_before_enter {
                    offer_test_external(&mut clipboard, primary, "after");
                }
                assert_eq!(
                    test_snapshot(&mut clipboard, primary, ClipboardReadMode::Contents),
                    (None, None),
                );
                clipboard.keyboard_focus_gained();
                let other_capture = capture_test_snapshot(
                    &mut clipboard,
                    !primary,
                    ClipboardReadMode::RevisionOnly,
                );
                let capture =
                    capture_test_snapshot(&mut clipboard, primary, ClipboardReadMode::RevisionOnly);
                if !offer_before_enter {
                    assert!(!capture.is_ready());
                    assert_eq!(
                        test_snapshot(&mut clipboard, primary, ClipboardReadMode::Contents),
                        (None, None),
                    );
                    offer_test_external(&mut clipboard, primary, "after");
                }
                let revision = if primary {
                    clipboard.primary_revision.clone()
                } else {
                    clipboard.revision.clone()
                };
                assert_eq!(resolved_test_snapshot(&capture), Ok((None, revision)));
                assert!(!other_capture.is_ready());
                assert!(clipboard.start_focus_sync().is_some());
                let token = clipboard.focus_sync.clone().expect("other channel sync");
                clipboard.keyboard_focus_synced(&token).resolve();
                let (item, after) =
                    test_snapshot(&mut clipboard, primary, ClipboardReadMode::Contents);
                assert_eq!(item.as_deref(), Some("after"));
                assert!(after.is_some());
                assert_ne!(after, before);
            }
            let (_, after) = test_snapshot(&mut clipboard, primary, ClipboardReadMode::Contents);

            clipboard.keyboard_focus_lost().resolve();
            if primary {
                clipboard.set_primary_offer(None).resolve();
            } else {
                clipboard.set_offer(None).resolve();
            }
            assert_eq!(
                test_snapshot(&mut clipboard, primary, ClipboardReadMode::Contents),
                (None, None),
            );
            clipboard.keyboard_focus_gained();
            let capture =
                capture_test_snapshot(&mut clipboard, primary, ClipboardReadMode::Contents);
            assert!(!capture.is_ready());
            let token = clipboard.start_focus_sync().expect("empty selection sync");
            clipboard.keyboard_focus_synced(&token).resolve();
            let (item, empty) = test_snapshot(&mut clipboard, primary, ClipboardReadMode::Contents);
            assert_eq!(resolved_test_snapshot(&capture), Ok((None, empty.clone())));
            assert_eq!(item, None);
            assert!(empty.is_some());
            assert_ne!(empty, after);
            clipboard.keyboard_focus_lost().resolve();
            clipboard.keyboard_focus_gained();
            let token = clipboard.start_focus_sync().expect("same-client sync");
            clipboard.keyboard_focus_synced(&token).resolve();
            for mode in [ClipboardReadMode::Contents, ClipboardReadMode::RevisionOnly] {
                assert_eq!(
                    test_snapshot(&mut clipboard, primary, mode),
                    (None, empty.clone()),
                );
            }

            let first = set_test_source(&mut clipboard, primary, "first");
            let second = set_test_source(&mut clipboard, primary, "second");
            clipboard.keyboard_focus_lost().resolve();
            if primary {
                clipboard.cancel_primary(&first);
            } else {
                clipboard.cancel(&first);
            }
            assert_eq!(
                test_snapshot(&mut clipboard, primary, ClipboardReadMode::Contents),
                (Some("second".to_owned()), Some(second.revision.clone())),
            );
            if primary {
                clipboard.cancel_primary(&second);
            } else {
                clipboard.cancel(&second);
            }
            clipboard.keyboard_focus_gained();
            assert_eq!(
                test_snapshot(&mut clipboard, primary, ClipboardReadMode::Contents),
                (None, None),
            );
            assert_eq!(first.item.text().as_deref(), Some("first"));
            assert_eq!(second.item.text().as_deref(), Some("second"));
            assert_eq!(
                test_snapshot(&mut clipboard, !primary, ClipboardReadMode::Contents),
                (
                    Some("other channel".to_owned()),
                    Some(other.revision.clone())
                ),
            );
        }
    }

    #[gpui::test]
    fn test_clipboard_focus_churn(cx: &mut TestAppContext) {
        for primary in [false, true] {
            let (socket, _server) = UnixStream::pair().expect("socket pair");
            let connection = Connection::from_socket(socket).expect("test connection");
            let event_loop = EventLoop::try_new().expect("event loop");
            let mut clipboard = Clipboard::new(
                connection,
                event_loop.handle(),
                cx.background_executor.clone(),
                cx.foreground_executor.clone(),
            );
            clipboard.keyboard_focus_gained();
            let mut previous_revision = None;
            for text in ["first", "second", "third"] {
                clipboard.keyboard_focus_lost().resolve();
                clipboard.keyboard_focus_lost().resolve();
                for mode in [ClipboardReadMode::Contents, ClipboardReadMode::RevisionOnly] {
                    assert_eq!(test_snapshot(&mut clipboard, primary, mode), (None, None));
                }
                offer_test_external(&mut clipboard, primary, text);
                assert_eq!(
                    test_snapshot(&mut clipboard, primary, ClipboardReadMode::Contents),
                    (None, None),
                );
                clipboard.keyboard_focus_gained();
                let (item, revision) =
                    test_snapshot(&mut clipboard, primary, ClipboardReadMode::Contents);
                assert_eq!(item.as_deref(), Some(text));
                assert!(revision.is_some());
                assert_ne!(revision, previous_revision);
                assert_eq!(
                    test_snapshot(&mut clipboard, primary, ClipboardReadMode::RevisionOnly),
                    (None, revision.clone()),
                );
                assert_eq!(
                    test_snapshot(&mut clipboard, !primary, ClipboardReadMode::Contents),
                    (None, None),
                );
                previous_revision = revision;
            }
            clipboard.keyboard_focus_lost().resolve();
            clipboard.keyboard_focus_gained();
            let old_token = clipboard.start_focus_sync().expect("old focus sync");
            let cancelled =
                capture_test_snapshot(&mut clipboard, primary, ClipboardReadMode::Contents);
            assert!(!cancelled.is_ready());
            clipboard.keyboard_focus_lost().resolve();
            assert_eq!(
                resolved_test_snapshot(&cancelled),
                Err(ClipboardReadError::Unavailable)
            );
            let token = clipboard.keyboard_focus_gained();
            let pending =
                capture_test_snapshot(&mut clipboard, primary, ClipboardReadMode::Contents);
            assert!(clipboard.start_focus_sync().is_none());
            clipboard.keyboard_focus_synced(&old_token).resolve();
            assert!(!pending.is_ready());
            let sync = clipboard.start_focus_sync().expect("current focus sync");
            clipboard.keyboard_focus_timed_out(&old_token).resolve();
            assert!(!pending.is_ready());
            clipboard.keyboard_focus_timed_out(&token).resolve();
            assert_eq!(
                resolved_test_snapshot(&pending),
                Err(ClipboardReadError::Unavailable)
            );
            let rejected =
                capture_test_snapshot(&mut clipboard, primary, ClipboardReadMode::Contents);
            assert_eq!(
                resolved_test_snapshot(&rejected),
                Err(ClipboardReadError::Unavailable)
            );
            clipboard.keyboard_focus_synced(&sync).resolve();
            assert!(clipboard.can_publish(primary));
            let recovered =
                capture_test_snapshot(&mut clipboard, primary, ClipboardReadMode::Contents);
            assert_eq!(
                resolved_test_snapshot(&recovered),
                Ok((Some("third".to_owned()), previous_revision.clone()))
            );
            assert_eq!(
                resolved_test_snapshot(&pending),
                Err(ClipboardReadError::Unavailable)
            );
            assert_eq!(
                resolved_test_snapshot(&rejected),
                Err(ClipboardReadError::Unavailable)
            );
            offer_test_external(&mut clipboard, primary, "recovered");
            let recovered =
                capture_test_snapshot(&mut clipboard, primary, ClipboardReadMode::Contents);
            assert_eq!(
                resolved_test_snapshot(&recovered)
                    .expect("fresh offer")
                    .0
                    .as_deref(),
                Some("recovered")
            );
            clipboard.keyboard_focus_lost().resolve();
            clipboard.keyboard_focus_gained();
            let dropped =
                capture_test_snapshot(&mut clipboard, primary, ClipboardReadMode::Contents);
            drop(clipboard);
            assert_eq!(
                resolved_test_snapshot(&dropped),
                Err(ClipboardReadError::Unavailable)
            );
        }
    }

    #[gpui::test]
    fn test_clipboard_capture_completion_order_and_immutability(cx: &mut TestAppContext) {
        for primary in [false, true] {
            let (socket, _server) = UnixStream::pair().expect("socket pair");
            let connection = Connection::from_socket(socket).expect("test connection");
            let event_loop = EventLoop::try_new().expect("event loop");
            let mut clipboard = Clipboard::new(
                connection,
                event_loop.handle(),
                cx.background_executor.clone(),
                cx.foreground_executor.clone(),
            );
            offer_test_external(&mut clipboard, primary, "A");
            clipboard.keyboard_focus_gained();
            let frozen =
                capture_test_snapshot(&mut clipboard, primary, ClipboardReadMode::Contents);
            let revision = if primary {
                clipboard.primary_revision.clone()
            } else {
                clipboard.revision.clone()
            };
            let expected = Ok((Some("A".to_owned()), revision));
            assert_eq!(resolved_test_snapshot(&frozen), expected);
            clipboard.keyboard_focus_lost().resolve();
            clipboard.keyboard_focus_gained();
            let first =
                capture_test_snapshot(&mut clipboard, primary, ClipboardReadMode::RevisionOnly);
            let second =
                capture_test_snapshot(&mut clipboard, primary, ClipboardReadMode::Contents);
            let order = Rc::new(RefCell::new(Vec::new()));
            for (index, capture) in [&first, &second].into_iter().enumerate() {
                let order = order.clone();
                capture.on_ready(move |result| {
                    order
                        .borrow_mut()
                        .push((index, result.expect("resolved capture").revision.clone()));
                });
            }
            let token = clipboard
                .start_focus_sync()
                .expect("same-client focus sync");
            clipboard.keyboard_focus_synced(&token).resolve();
            assert_eq!(resolved_test_snapshot(&second), expected);
            let revision = expected.clone().expect("original snapshot").1;
            assert_eq!(*order.borrow(), [(0, revision.clone()), (1, revision)]);
            offer_test_external(&mut clipboard, primary, "B");
            assert_eq!(resolved_test_snapshot(&frozen), expected);
            assert_eq!(resolved_test_snapshot(&second), expected);
            let fresh = capture_test_snapshot(&mut clipboard, primary, ClipboardReadMode::Contents);
            assert_eq!(
                resolved_test_snapshot(&fresh)
                    .expect("new capture")
                    .0
                    .as_deref(),
                Some("B")
            );

            let source = set_test_source(&mut clipboard, primary, "owned");
            clipboard.keyboard_focus_lost().resolve();
            clipboard.keyboard_focus_gained();
            let owned = capture_test_snapshot(&mut clipboard, primary, ClipboardReadMode::Contents);
            assert!(!owned.is_ready());
            if primary {
                clipboard.cancel_primary(&source);
            } else {
                clipboard.cancel(&source);
            }
            let token = clipboard.start_focus_sync().expect("cancelled source sync");
            clipboard.keyboard_focus_synced(&token).resolve();
            assert_eq!(
                resolved_test_snapshot(&owned),
                Err(ClipboardReadError::Unavailable)
            );

            offer_test_external(&mut clipboard, primary, "old");
            clipboard.keyboard_focus_lost().resolve();
            clipboard.keyboard_focus_gained();
            let replacement =
                capture_test_snapshot(&mut clipboard, primary, ClipboardReadMode::Contents);
            assert!(!replacement.is_ready());
            let source = set_test_source(&mut clipboard, primary, "replacement");
            offer_test_source(&mut clipboard, primary, &source);
            assert_eq!(
                resolved_test_snapshot(&replacement),
                Ok((
                    Some("replacement".to_owned()),
                    Some(source.revision.clone())
                ))
            );
        }
    }

    #[gpui::test]
    fn test_clipboard_completion_callbacks_can_reenter(cx: &mut TestAppContext) {
        for primary in [false, true] {
            for event in ["sync", "selection", "leave", "timeout"] {
                let (socket, _server) = UnixStream::pair().expect("socket pair");
                let connection = Connection::from_socket(socket).expect("test connection");
                let event_loop = EventLoop::try_new().expect("event loop");
                let mut clipboard = Clipboard::new(
                    connection,
                    event_loop.handle(),
                    cx.background_executor.clone(),
                    cx.foreground_executor.clone(),
                );
                offer_test_external(&mut clipboard, primary, "A");
                clipboard.keyboard_focus_gained();
                clipboard.keyboard_focus_lost().resolve();
                let token = clipboard.keyboard_focus_gained();
                assert!(clipboard.start_focus_sync().is_some());
                let captures = [
                    capture_test_snapshot(&mut clipboard, primary, ClipboardReadMode::Contents),
                    capture_test_snapshot(&mut clipboard, primary, ClipboardReadMode::Contents),
                ];
                let clipboard = Rc::new(RefCell::new(clipboard));
                let observed = Rc::new(RefCell::new(Vec::new()));
                for (index, capture) in captures.iter().enumerate() {
                    let clipboard = clipboard.clone();
                    let observed = observed.clone();
                    capture.on_ready(move |result| {
                        let (snapshot, recaptured) = {
                            let mut clipboard = clipboard.borrow_mut();
                            (
                                test_snapshot(&mut clipboard, primary, ClipboardReadMode::Contents),
                                capture_test_snapshot(
                                    &mut clipboard,
                                    primary,
                                    ClipboardReadMode::RevisionOnly,
                                ),
                            )
                        };
                        let expected = if event == "sync" || event == "selection" {
                            Ok((None, snapshot.1))
                        } else {
                            Err(ClipboardReadError::Unavailable)
                        };
                        assert_eq!(resolved_test_snapshot(&recaptured), expected);
                        observed.borrow_mut().push((
                            index,
                            result.map(|snapshot| {
                                snapshot.item.as_ref().and_then(|item| item.text())
                            }),
                        ));
                    });
                }
                let completions = {
                    let mut clipboard = clipboard.borrow_mut();
                    match event {
                        "sync" => clipboard.keyboard_focus_synced(&token),
                        "selection" if primary => clipboard.set_primary_offer(None),
                        "selection" => clipboard.set_offer(None),
                        "leave" => clipboard.keyboard_focus_lost(),
                        "timeout" => clipboard.keyboard_focus_timed_out(&token),
                        _ => unreachable!(),
                    }
                };
                assert!(observed.borrow().is_empty());
                assert!(captures.iter().all(|capture| !capture.is_ready()));
                completions.resolve();
                let expected = match event {
                    "sync" => Ok(Some("A".to_owned())),
                    "selection" => Ok(None),
                    _ => Err(ClipboardReadError::Unavailable),
                };
                assert_eq!(*observed.borrow(), [(0, expected.clone()), (1, expected)]);
            }
        }
    }

    #[gpui::test]
    async fn test_clipboard_capture_uncached_order(cx: &mut TestAppContext) {
        cx.dispatcher.allow_parking();
        for deferred in [false, true] {
            let (socket, _server) = UnixStream::pair().expect("socket pair");
            let connection = Connection::from_socket(socket).expect("test connection");
            let reader = snapshot_test_reader(cx);
            let mut readiness = SelectionReadiness::default();
            if !deferred {
                readiness.authority = SelectionAuthority::Ready;
            }
            let (mut value, offer) = uncached_test_value("text/plain;charset=utf-8");
            let first_revision = value.revision.clone();
            let first = readiness.capture(
                true,
                ClipboardReadMode::Contents,
                &mut value,
                &connection,
                &reader,
            );
            let duplicate = readiness.capture(
                true,
                ClipboardReadMode::Contents,
                &mut value,
                &connection,
                &reader,
            );
            let revisions = Rc::new(RefCell::new(Vec::new()));
            first.revision_only().on_ready({
                let revisions = revisions.clone();
                move |result| {
                    revisions
                        .borrow_mut()
                        .push(result.expect("first revision").revision.clone())
                }
            });
            if deferred {
                assert!(!first.revision_only().is_ready());
                let completions = readiness.confirm(&mut value, &connection, &reader);
                assert_eq!(offer.requests.get(), 1);
                assert!(!first.revision_only().is_ready());
                completions.resolve();
            }
            assert_eq!(offer.requests.get(), 1);
            assert!(first.revision_only().is_ready());
            assert!(duplicate.revision_only().is_ready());
            assert!(!first.is_ready());
            offer.valid.set(false);
            value.offer = None;
            cx.run_until_parked();
            let responsive = Rc::new(Cell::new(false));
            let foreground = cx.foreground_executor.spawn({
                let responsive = responsive.clone();
                async move {
                    responsive.set(true);
                }
            });
            foreground.await;
            assert!(responsive.get());
            assert!(!first.is_ready());

            let (mut replacement, replacement_offer) =
                uncached_test_value("text/plain;charset=utf-8");
            let second_revision = replacement.revision.clone();
            let metadata = readiness.capture(
                true,
                ClipboardReadMode::RevisionOnly,
                &mut replacement,
                &connection,
                &reader,
            );
            assert!(metadata.is_ready());
            assert_eq!(replacement_offer.requests.get(), 0);
            let second = readiness.capture(
                true,
                ClipboardReadMode::Contents,
                &mut replacement,
                &connection,
                &reader,
            );
            second.revision_only().on_ready({
                let revisions = revisions.clone();
                move |result| {
                    revisions
                        .borrow_mut()
                        .push(result.expect("second revision").revision.clone())
                }
            });
            assert_eq!(
                *revisions.borrow(),
                [first_revision.clone(), second_revision.clone()]
            );
            cx.run_until_parked();
            let mut writer = replacement_offer.take_writer("text/plain;charset=utf-8");
            writer
                .write_all(b"second\r\n\r\r\n\r")
                .expect("second payload");
            drop(writer);
            let snapshot = second.wait().await.expect("second snapshot");
            assert_eq!(
                snapshot
                    .item
                    .as_ref()
                    .and_then(|item| item.text())
                    .as_deref(),
                Some("second\n\r\n\r")
            );
            assert_eq!(snapshot.revision, second_revision);
            assert!(!first.is_ready());
            let mut writer = offer.take_writer("text/plain;charset=utf-8");
            writer
                .write_all("first 🦀".as_bytes())
                .expect("first payload");
            drop(writer);
            let first_snapshot = first.wait().await.expect("historical snapshot");
            let duplicate_snapshot = duplicate.wait().await.expect("deduplicated snapshot");
            assert!(Arc::ptr_eq(&first_snapshot, &duplicate_snapshot));
            for snapshot in [first_snapshot, duplicate_snapshot] {
                assert_eq!(
                    snapshot
                        .item
                        .as_ref()
                        .and_then(|item| item.text())
                        .as_deref(),
                    Some("first 🦀")
                );
                assert_eq!(snapshot.revision, first_revision);
            }
            assert_eq!(*revisions.borrow(), [first_revision, second_revision]);
            assert_eq!(reader.budget.bytes.load(Ordering::Relaxed), 0);
            assert_eq!(reader.budget.transfers.load(Ordering::Relaxed), 0);
        }
    }

    #[gpui::test]
    async fn test_clipboard_capture_total_timeout_and_cancellation(cx: &mut TestAppContext) {
        cx.dispatcher.allow_parking();
        let (socket, _server) = UnixStream::pair().expect("socket pair");
        let connection = Connection::from_socket(socket).expect("test connection");
        let reader = snapshot_test_reader(cx);
        let (mut value, offer) = uncached_test_value("text/plain;charset=utf-8");
        value
            .offer
            .as_mut()
            .expect("offer")
            .add_mime_type("image/png".to_owned());
        let read = value.capture(ClipboardReadMode::Contents, &connection, &reader);
        cx.run_until_parked();
        assert!(!read.is_ready());
        cx.background_executor.advance_clock(Duration::from_secs(3));
        let (sender, consumed) = futures::channel::oneshot::channel();
        *reader.budget.progress.lock() = Some(sender);
        let mut writer = offer.take_writer("text/plain;charset=utf-8");
        writer.write_all(&[0xff]).expect("invalid text payload");
        assert_eq!(consumed.await.expect("text read observed"), 1);
        drop(writer);
        let (sender, consumed) = futures::channel::oneshot::channel();
        *reader.budget.progress.lock() = Some(sender);
        let mut writer = offer.take_writer("image/png");
        writer.write_all(b"p").expect("partial image payload");
        assert_eq!(consumed.await.expect("fallback read observed"), 1);
        assert!(!read.is_ready());
        cx.background_executor.advance_clock(Duration::from_secs(1));
        cx.run_until_parked();
        assert_eq!(
            resolved_test_snapshot(&read),
            Err(ClipboardReadError::Unavailable)
        );
        assert_eq!(
            resolved_test_snapshot(&read.revision_only()),
            Ok((None, value.revision))
        );
        assert_eq!(reader.budget.bytes.load(Ordering::Relaxed), 0);
        assert_eq!(reader.budget.transfers.load(Ordering::Relaxed), 0);

        for deferred in [false, true] {
            let mut readiness = SelectionReadiness::default();
            if !deferred {
                readiness.authority = SelectionAuthority::Ready;
            }
            let (mut value, offer) = uncached_test_value("text/plain;charset=utf-8");
            let first = readiness.capture(
                true,
                ClipboardReadMode::Contents,
                &mut value,
                &connection,
                &reader,
            );
            let second = readiness.capture(
                true,
                ClipboardReadMode::Contents,
                &mut value,
                &connection,
                &reader,
            );
            drop(first);
            if deferred {
                readiness
                    .confirm(&mut value, &connection, &reader)
                    .resolve();
            }
            cx.run_until_parked();
            assert_eq!(offer.requests.get(), 1);
            assert_eq!(reader.budget.transfers.load(Ordering::Relaxed), 1);
            drop(second);
            cx.run_until_parked();
            assert!(
                readiness
                    .contents
                    .as_ref()
                    .expect("deduplication entry")
                    .1
                    .upgrade()
                    .is_none()
            );
            assert_eq!(reader.budget.bytes.load(Ordering::Relaxed), 0);
            assert_eq!(reader.budget.transfers.load(Ordering::Relaxed), 0);
        }

        let mut readiness = SelectionReadiness::default();
        let (mut value, offer) = uncached_test_value("text/plain;charset=utf-8");
        let abandoned = readiness.capture(
            true,
            ClipboardReadMode::Contents,
            &mut value,
            &connection,
            &reader,
        );
        drop(abandoned);
        readiness
            .confirm(&mut value, &connection, &reader)
            .resolve();
        assert_eq!(offer.requests.get(), 0);

        let (value, offer) = uncached_test_value("text/plain;charset=utf-8");
        let read = value.capture(ClipboardReadMode::Contents, &connection, &reader);
        offer
            .take_writer("text/plain;charset=utf-8")
            .write_all(b"late")
            .expect("late payload");
        assert!(cx.dispatcher.advance_clock_to_next_timer());
        cx.run_until_parked();
        assert_eq!(
            resolved_test_snapshot(&read),
            Err(ClipboardReadError::Unavailable)
        );
    }

    #[gpui::test]
    async fn test_clipboard_capture_payload_limit_and_failures(cx: &mut TestAppContext) {
        cx.dispatcher.allow_parking();
        let (socket, _server) = UnixStream::pair().expect("socket pair");
        let connection = Connection::from_socket(socket).expect("test connection");
        let reader = snapshot_test_reader(cx);
        for length in [0, 8192, SNAPSHOT_PAYLOAD_LIMIT, SNAPSHOT_PAYLOAD_LIMIT + 1] {
            let mut competing = reader
                .budget
                .acquire(1)
                .expect("competing transfer")
                .pop()
                .expect("one permit");
            if length == 8192 {
                competing
                    .reserve(SNAPSHOT_PAYLOAD_LIMIT - length)
                    .expect("remaining capacity");
            }
            let (value, offer) = uncached_test_value("image/png");
            let read = value.capture(ClipboardReadMode::Contents, &connection, &reader);
            let writer = offer.take_writer("image/png");
            let write = cx.background_executor.spawn(async move {
                let mut writer = smol::Async::new(writer).expect("async writer");
                let chunk = [b'x'; 8192];
                let mut remaining = length;
                while remaining > 0 {
                    let count = remaining.min(chunk.len());
                    writer
                        .write_all(&chunk[..count])
                        .await
                        .expect("payload chunk");
                    remaining -= count;
                }
            });
            let result = read.wait().await;
            write.await;
            if length > SNAPSHOT_PAYLOAD_LIMIT {
                assert_eq!(
                    result.expect_err("oversize rejected"),
                    ClipboardReadError::UnsupportedContent
                );
            } else {
                let snapshot = result.expect("bounded payload");
                let item = snapshot.item.as_ref().expect("image item");
                let [ClipboardEntry::Image(image)] = item.entries.as_slice() else {
                    panic!("expected one image");
                };
                assert_eq!(image.format, ImageFormat::Png);
                assert_eq!(image.bytes.len(), length);
                assert_eq!(
                    image.bytes.iter().filter(|byte| **byte == b'x').count(),
                    length
                );
            }
            assert_eq!(
                reader.budget.bytes.load(Ordering::Relaxed),
                competing.capacity
            );
            assert_eq!(reader.budget.transfers.load(Ordering::Relaxed), 1);
        }
        let (value, offer) = uncached_test_value("text/plain;charset=utf-8");
        let read = value.capture(ClipboardReadMode::Contents, &connection, &reader);
        offer
            .take_writer("text/plain;charset=utf-8")
            .write_all(&[0xff])
            .expect("invalid UTF-8");
        assert_eq!(
            read.wait().await.expect_err("invalid UTF-8 rejected"),
            ClipboardReadError::UnsupportedContent
        );
        assert_eq!(
            resolved_test_snapshot(&read.revision_only()),
            Ok((None, value.revision))
        );
        let (mut value, offer) = uncached_test_value("text/plain;charset=utf-8");
        value.cached = Some(ClipboardItem::new_string(
            "x".repeat(SNAPSHOT_PAYLOAD_LIMIT + 1),
        ));
        let read = value.capture(ClipboardReadMode::Contents, &connection, &reader);
        assert_eq!(
            resolved_test_snapshot(&read),
            Err(ClipboardReadError::UnsupportedContent)
        );
        assert_eq!(
            resolved_test_snapshot(&read.revision_only()),
            Ok((None, value.revision))
        );
        assert_eq!(offer.requests.get(), 0);
    }

    #[gpui::test]
    async fn test_clipboard_capture_format_fallback(cx: &mut TestAppContext) {
        cx.dispatcher.allow_parking();
        let (socket, _server) = UnixStream::pair().expect("socket pair");
        let connection = Connection::from_socket(socket).expect("test connection");
        let reader = snapshot_test_reader(cx);
        for (first, second, format) in [
            ("text/plain;charset=utf-8", "image/png", ImageFormat::Png),
            ("image/png", "image/jpeg", ImageFormat::Jpeg),
        ] {
            let mut reserved = reader
                .budget
                .acquire(1)
                .expect("competing transfer")
                .pop()
                .expect("one permit");
            let payload = if first == "image/png" {
                reserved
                    .reserve(SNAPSHOT_PAYLOAD_LIMIT - 8192)
                    .expect("competing capacity");
                vec![b'x'; 8193]
            } else {
                vec![0xff]
            };
            let (mut value, offer) = uncached_test_value(first);
            value
                .offer
                .as_mut()
                .expect("offer")
                .add_mime_type(second.to_owned());
            let mut readiness = SelectionReadiness::default();
            readiness.authority = SelectionAuthority::Ready;
            let read = readiness.capture(
                true,
                ClipboardReadMode::Contents,
                &mut value,
                &connection,
                &reader,
            );
            assert_eq!(offer.requests.get(), 2);
            offer.valid.set(false);
            value.offer = None;
            let (sender, completed) = futures::channel::oneshot::channel();
            *reader.budget.completed.lock() = Some(sender);
            let writer = offer.take_writer(first);
            let write = cx.background_executor.spawn(async move {
                smol::Async::new(writer)
                    .expect("async writer")
                    .write_all(&payload)
                    .await
                    .expect("first payload");
            });
            assert_eq!(completed.await.expect("preferred failure observed"), 0);
            write.await;
            offer
                .take_writer(second)
                .write_all(b"fallback")
                .expect("fallback payload");
            let snapshot = read.wait().await.expect("fallback snapshot");
            let [ClipboardEntry::Image(image)] =
                snapshot.item.as_ref().expect("image").entries.as_slice()
            else {
                panic!("expected one image");
            };
            assert_eq!(image.format, format);
            assert_eq!(image.bytes, b"fallback");
            assert_eq!(snapshot.revision, value.revision);
            assert_eq!(
                reader.budget.bytes.load(Ordering::Relaxed),
                reserved.capacity
            );
            assert_eq!(reader.budget.transfers.load(Ordering::Relaxed), 1);
        }

        for preferred_ready in [false, true] {
            let (mut value, offer) = uncached_test_value("text/plain;charset=utf-8");
            for mime_type in ["image/png", "image/jpeg"] {
                value
                    .offer
                    .as_mut()
                    .expect("offer")
                    .add_mime_type(mime_type.to_owned());
            }
            let read = value.capture(ClipboardReadMode::Contents, &connection, &reader);
            assert_eq!(offer.requests.get(), 3);
            offer.valid.set(false);
            value.offer = None;
            let mut text_writer = offer.take_writer("text/plain;charset=utf-8");
            let mut png_writer = offer.take_writer("image/png");
            let mut jpeg_writer = offer.take_writer("image/jpeg");
            if preferred_ready {
                text_writer
                    .write_all(b"preferred")
                    .expect("preferred payload");
                drop(text_writer);
                let snapshot = read.wait().await.expect("preferred snapshot");
                assert_eq!(
                    snapshot
                        .item
                        .as_ref()
                        .and_then(|item| item.text())
                        .as_deref(),
                    Some("preferred")
                );
                assert_eq!(
                    png_writer
                        .write(b"unused")
                        .expect_err("PNG canceled")
                        .kind(),
                    ErrorKind::BrokenPipe
                );
                assert_eq!(
                    jpeg_writer
                        .write(b"unused")
                        .expect_err("JPEG canceled")
                        .kind(),
                    ErrorKind::BrokenPipe
                );
            } else {
                let (sender, completed) = futures::channel::oneshot::channel();
                *reader.budget.completed.lock() = Some(sender);
                jpeg_writer.write_all(b"jpeg").expect("JPEG payload");
                drop(jpeg_writer);
                assert_eq!(completed.await.expect("JPEG completion observed"), 2);
                assert!(!read.is_ready());
                assert_eq!(reader.budget.bytes.load(Ordering::Relaxed), 8192);
                assert_eq!(reader.budget.transfers.load(Ordering::Relaxed), 3);

                let (sender, consumed) = futures::channel::oneshot::channel();
                *reader.budget.progress.lock() = Some(sender);
                png_writer.write_all(b"p").expect("partial PNG payload");
                assert_eq!(consumed.await.expect("partial PNG read observed"), 1);
                assert!(!read.is_ready());
                let (sender, completed) = futures::channel::oneshot::channel();
                *reader.budget.completed.lock() = Some(sender);
                png_writer.write_all(b"ng").expect("remaining PNG payload");
                drop(png_writer);
                assert_eq!(completed.await.expect("PNG completion observed"), 1);
                assert!(!read.is_ready());
                assert_eq!(reader.budget.bytes.load(Ordering::Relaxed), 16384);
                assert_eq!(reader.budget.transfers.load(Ordering::Relaxed), 3);
                cx.background_executor.advance_clock(Duration::from_secs(3));
                assert!(!read.is_ready());
                cx.background_executor.advance_clock(Duration::from_secs(1));
                let snapshot = read.wait().await.expect("completed fallback at deadline");
                let [ClipboardEntry::Image(image)] =
                    snapshot.item.as_ref().expect("image").entries.as_slice()
                else {
                    panic!("expected one image");
                };
                assert_eq!(image.format, ImageFormat::Png);
                assert_eq!(image.bytes, b"png");
                assert_eq!(snapshot.revision, value.revision);
                assert_eq!(
                    text_writer
                        .write(b"late")
                        .expect_err("text canceled")
                        .kind(),
                    ErrorKind::BrokenPipe
                );
            }
            assert_eq!(reader.budget.bytes.load(Ordering::Relaxed), 0);
            assert_eq!(reader.budget.transfers.load(Ordering::Relaxed), 0);
        }
    }

    #[gpui::test]
    fn test_clipboard_capture_shared_budget(cx: &mut TestAppContext) {
        let (socket, _server) = UnixStream::pair().expect("socket pair");
        let connection = Connection::from_socket(socket).expect("test connection");
        let reader = snapshot_test_reader(cx);
        let mut reserved = reader
            .budget
            .acquire(1)
            .expect("first transfer")
            .pop()
            .expect("one permit");
        reserved
            .reserve(SNAPSHOT_PAYLOAD_LIMIT)
            .expect("64 MiB budget");
        let mut competing = reader
            .budget
            .acquire(1)
            .expect("second transfer")
            .pop()
            .expect("one permit");
        assert_eq!(competing.reserve(1), Err(ClipboardReadError::Unavailable));
        assert_eq!(
            reserved.reserve(SNAPSHOT_PAYLOAD_LIMIT + 1),
            Err(ClipboardReadError::UnsupportedContent)
        );
        drop(reserved);
        competing
            .reserve(SNAPSHOT_PAYLOAD_LIMIT)
            .expect("reclaimed budget");
        drop(competing);
        let mut captures = Vec::new();
        let mut offers = Vec::new();
        for count in [SNAPSHOT_CANDIDATE_LIMIT, SNAPSHOT_CANDIDATE_LIMIT]
            .into_iter()
            .chain(std::iter::repeat_n(
                1,
                SNAPSHOT_TRANSFER_LIMIT - SNAPSHOT_CANDIDATE_LIMIT,
            ))
        {
            let (mut value, offer) = uncached_test_value("text/plain;charset=utf-8");
            for format in ImageFormat::iter().take(count - 1) {
                value
                    .offer
                    .as_mut()
                    .expect("offer")
                    .add_mime_type(format.mime_type().to_owned());
            }
            let read = value.capture(ClipboardReadMode::Contents, &connection, &reader);
            if !captures.is_empty() && count == SNAPSHOT_CANDIDATE_LIMIT {
                assert_eq!(
                    resolved_test_snapshot(&read),
                    Err(ClipboardReadError::Unavailable)
                );
                assert_eq!(offer.requests.get(), 0);
                assert_eq!(
                    reader.budget.transfers.load(Ordering::Relaxed),
                    SNAPSHOT_CANDIDATE_LIMIT
                );
            } else {
                assert_eq!(offer.requests.get(), count);
                captures.push(read);
                offers.push(offer);
            }
        }
        assert_eq!(
            reader.budget.transfers.load(Ordering::Relaxed),
            SNAPSHOT_TRANSFER_LIMIT
        );
        let (value, offer) = uncached_test_value("text/plain;charset=utf-8");
        let rejected = value.capture(ClipboardReadMode::Contents, &connection, &reader);
        assert_eq!(
            resolved_test_snapshot(&rejected),
            Err(ClipboardReadError::Unavailable)
        );
        assert_eq!(offer.requests.get(), 0);
        assert_eq!(
            resolved_test_snapshot(&rejected.revision_only()),
            Ok((None, value.revision))
        );
        drop(captures);
        cx.run_until_parked();
        assert_eq!(reader.budget.bytes.load(Ordering::Relaxed), 0);
        assert_eq!(reader.budget.transfers.load(Ordering::Relaxed), 0);
    }

    #[derive(Clone)]
    struct TestOffer {
        writers: Rc<RefCell<Vec<(String, File)>>>,
        requests: Rc<Cell<usize>>,
        valid: Rc<Cell<bool>>,
    }

    impl TestOffer {
        fn take_writer(&self, mime_type: &str) -> File {
            let mut writers = self.writers.borrow_mut();
            let index = writers
                .iter()
                .position(|(mime, _)| mime == mime_type)
                .expect("receive initiated");
            writers.remove(index).1
        }
    }

    impl ReceiveData for TestOffer {
        fn receive_data(&self, mime_type: String, fd: BorrowedFd<'_>) {
            assert!(self.valid.get());
            self.requests.set(self.requests.get() + 1);
            self.writers.borrow_mut().push((
                mime_type,
                File::from(fd.try_clone_to_owned().expect("duplicate writer")),
            ));
        }
    }

    fn uncached_test_value(mime_type: &str) -> (SelectionValue<TestOffer>, TestOffer) {
        let offer = TestOffer {
            writers: Rc::new(RefCell::new(Vec::new())),
            requests: Rc::new(Cell::new(0)),
            valid: Rc::new(Cell::new(true)),
        };
        let mut data_offer = DataOffer::new(offer.clone());
        data_offer.add_mime_type(mime_type.to_owned());
        (
            SelectionValue {
                source: None,
                revision: Some(ClipboardRevision::default()),
                offer: Some(data_offer),
                cached: None,
            },
            offer,
        )
    }

    fn snapshot_test_reader(cx: &TestAppContext) -> SnapshotReader {
        SnapshotReader {
            background_executor: cx.background_executor.clone(),
            foreground_executor: cx.foreground_executor.clone(),
            budget: Arc::new(SnapshotBudget::default()),
            timeout: PIPE_READ_TIMEOUT,
        }
    }

    fn capture_test_snapshot(
        clipboard: &mut Clipboard,
        primary: bool,
        mode: ClipboardReadMode,
    ) -> ClipboardSnapshotRead {
        if primary {
            clipboard.capture_primary_snapshot(mode)
        } else {
            clipboard.capture_snapshot(mode)
        }
    }

    fn resolved_test_snapshot(
        capture: &ClipboardSnapshotRead,
    ) -> Result<(Option<String>, Option<ClipboardRevision>), ClipboardReadError> {
        let result = Rc::new(RefCell::new(None));
        capture.on_ready({
            let result = result.clone();
            move |snapshot| {
                *result.borrow_mut() = Some(snapshot.map(|snapshot| {
                    (
                        snapshot.item.as_ref().and_then(|item| item.text()),
                        snapshot.revision.clone(),
                    )
                }));
            }
        });
        result.borrow_mut().take().expect("completed capture")
    }

    fn set_test_source(
        clipboard: &mut Clipboard,
        primary: bool,
        text: &str,
    ) -> Arc<ClipboardSource> {
        let item = ClipboardItem::new_string(text.to_owned());
        if primary {
            clipboard.set_primary(item)
        } else {
            clipboard.set(item)
        }
    }

    fn offer_test_source(clipboard: &mut Clipboard, primary: bool, source: &ClipboardSource) {
        let backend = clipboard.connection.backend().downgrade();
        if primary {
            let mut offer = DataOffer::new(ZwpPrimarySelectionOfferV1::inert(backend));
            offer.add_mime_type(source.mime_type.clone());
            clipboard.set_primary_offer(Some(offer)).resolve();
        } else {
            let mut offer = DataOffer::new(WlDataOffer::inert(backend));
            offer.add_mime_type(source.mime_type.clone());
            clipboard.set_offer(Some(offer)).resolve();
        }
    }

    fn offer_test_external(clipboard: &mut Clipboard, primary: bool, text: &str) {
        let backend = clipboard.connection.backend().downgrade();
        let item = ClipboardItem::new_string(text.to_owned());
        if primary {
            let mut offer = DataOffer::new(ZwpPrimarySelectionOfferV1::inert(backend));
            offer.add_mime_type("text/plain;charset=utf-8".to_owned());
            clipboard.set_primary_offer(Some(offer)).resolve();
            clipboard.cached_primary_read = Some(item);
        } else {
            let mut offer = DataOffer::new(WlDataOffer::inert(backend));
            offer.add_mime_type("text/plain;charset=utf-8".to_owned());
            clipboard.set_offer(Some(offer)).resolve();
            clipboard.cached_read = Some(item);
        }
    }

    fn test_snapshot(
        clipboard: &mut Clipboard,
        primary: bool,
        mode: ClipboardReadMode,
    ) -> (Option<String>, Option<ClipboardRevision>) {
        let snapshot = if primary {
            clipboard.read_primary_snapshot(mode)
        } else {
            clipboard.read_snapshot(mode)
        };
        (
            snapshot.item.as_ref().and_then(|item| item.text()),
            snapshot.revision,
        )
    }

    fn test_source_text(clipboard: &Clipboard, primary: bool) -> Option<String> {
        if primary {
            clipboard.primary_contents.as_ref()
        } else {
            clipboard.contents.as_ref()
        }
        .and_then(|source| source.item.text())
    }
}
