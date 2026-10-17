use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

use crate::disconnected_overlay::DisconnectedOverlay;
use anyhow::{Context as _, Result};
use askpass::EncryptedPassword;
use editor::Editor;
use futures::{FutureExt as _, channel::oneshot, select};
use gpui::{AppContext, AsyncApp, PromptLevel, WindowHandle};

use project::trusted_worktrees;
use remote::{
    DockerConnectionOptions, Interactive, RemoteConnection, RemoteConnectionOptions,
    SshConnectionOptions,
};
pub use settings::SshConnection;
use settings::{DevContainerConnection, ExtendingVec, RegisterSetting, Settings, WslConnection};
use util::paths::PathWithPosition;
use workspace::{
    AppState, MultiWorkspace, OpenMode, OpenOptions, SerializedWorkspaceLocation, Workspace,
    WorkspaceDb, WorkspaceMatch, WorkspaceMatching, find_existing_workspace,
};

pub use remote_connection::{
    RemoteClientDelegate, RemoteConnectionModal, RemoteConnectionPrompt, SshConnectionHeader,
    connect,
};

#[derive(RegisterSetting)]
pub struct RemoteSettings {
    pub ssh_connections: ExtendingVec<SshConnection>,
    pub wsl_connections: ExtendingVec<WslConnection>,
    /// Whether to read ~/.ssh/config for ssh connection sources.
    pub read_ssh_config: bool,
}

impl RemoteSettings {
    pub fn ssh_connections(&self) -> impl Iterator<Item = SshConnection> + use<> {
        self.ssh_connections.clone().0.into_iter()
    }

    pub fn wsl_connections(&self) -> impl Iterator<Item = WslConnection> + use<> {
        self.wsl_connections.clone().0.into_iter()
    }

    pub fn fill_connection_options_from_settings(&self, options: &mut SshConnectionOptions) {
        for conn in self.ssh_connections() {
            if conn.host == options.host.to_string()
                && conn.username == options.username
                && conn.port == options.port
            {
                options.nickname = conn.nickname;
                options.upload_binary_over_ssh = conn.upload_binary_over_ssh.unwrap_or_default();
                options.args = Some(conn.args);
                options.port_forwards = conn.port_forwards;
                break;
            }
        }
    }

    pub fn connection_options_for(
        &self,
        host: String,
        port: Option<u16>,
        username: Option<String>,
    ) -> SshConnectionOptions {
        let mut options = SshConnectionOptions {
            host: host.into(),
            port,
            username,
            ..Default::default()
        };
        self.fill_connection_options_from_settings(&mut options);
        options
    }
}

#[derive(Clone, PartialEq)]
pub enum Connection {
    Ssh(SshConnection),
    Wsl(WslConnection),
    DevContainer(DevContainerConnection),
}

impl From<Connection> for RemoteConnectionOptions {
    fn from(val: Connection) -> Self {
        match val {
            Connection::Ssh(conn) => RemoteConnectionOptions::Ssh(conn.into()),
            Connection::Wsl(conn) => RemoteConnectionOptions::Wsl(conn.into()),
            Connection::DevContainer(conn) => {
                RemoteConnectionOptions::Docker(DockerConnectionOptions {
                    name: conn.name,
                    remote_user: conn.remote_user,
                    container_id: conn.container_id,
                    upload_binary_over_docker_exec: false,
                    use_podman: conn.use_podman,
                    remote_env: conn.remote_env,
                })
            }
        }
    }
}

impl From<SshConnection> for Connection {
    fn from(val: SshConnection) -> Self {
        Connection::Ssh(val)
    }
}

impl From<WslConnection> for Connection {
    fn from(val: WslConnection) -> Self {
        Connection::Wsl(val)
    }
}

impl Settings for RemoteSettings {
    fn from_settings(content: &settings::SettingsContent) -> Self {
        let remote = &content.remote;
        Self {
            ssh_connections: remote.ssh_connections.clone().unwrap_or_default().into(),
            wsl_connections: remote.wsl_connections.clone().unwrap_or_default().into(),
            read_ssh_config: remote.read_ssh_config.unwrap(),
        }
    }
}

pub async fn open_remote_project(
    connection_options: RemoteConnectionOptions,
    paths: Vec<PathBuf>,
    app_state: Arc<AppState>,
    mut open_options: workspace::OpenOptions,
    cx: &mut AsyncApp,
) -> Result<WindowHandle<MultiWorkspace>> {
    let focus = open_options.focus.unwrap_or(true);
    if open_options.open_mode == OpenMode::NewWindow {
        open_options.workspace_matching = WorkspaceMatching::None;
        open_options.requesting_window = None;
    }

    let (existing, open_visible) = loop {
        let (existing, open_visible) = find_existing_workspace(
            &paths,
            &open_options,
            &SerializedWorkspaceLocation::Remote(connection_options.clone()),
            cx,
        )
        .await?;
        let Some((owner, WorkspaceMatch::Unloaded(workspace_id))) = existing else {
            break (existing, open_visible);
        };
        let saved =
            cx.update(|cx| WorkspaceDb::global(cx).session_workspace_for_id(workspace_id))?;
        Box::pin(open_remote_project(
            connection_options.clone(),
            saved.paths.ordered_paths().cloned().collect::<Vec<_>>(),
            app_state.clone(),
            OpenOptions {
                workspace_id: Some(workspace_id),
                requesting_window: Some(owner),
                focus: Some(false),
                ..OpenOptions::default()
            },
            cx,
        ))
        .await?;
        if !owner.read_with(cx, |multi_workspace, cx| {
            multi_workspace.workspaces().any(|workspace| {
                let workspace = workspace.read(cx);
                workspace.database_id() == Some(workspace_id) && !workspace.is_restoring()
            })
        })? {
            return Ok(owner);
        }
    };

    if let Some((existing_window, WorkspaceMatch::Loaded(existing_workspace))) = existing {
        open_options.requesting_window = Some(existing_window);
        let remote_connection = cx.update(|cx| {
            existing_workspace
                .read(cx)
                .project()
                .read(cx)
                .remote_client()
                .and_then(|client| client.read(cx).remote_connection())
        });

        if let Some(remote_connection) = remote_connection {
            let (resolved_paths, paths_with_positions) =
                determine_paths_with_positions(&remote_connection, paths).await;

            let open_results = existing_window
                .update(cx, |multi_workspace, window, cx| {
                    if focus {
                        window.activate_window();
                    }
                    multi_workspace.activate(existing_workspace.clone(), None, window, cx);
                    existing_workspace.update(cx, |workspace, cx| {
                        workspace.open_paths(
                            resolved_paths,
                            OpenOptions {
                                visible: Some(open_visible),
                                focus: open_options.focus,
                                ..Default::default()
                            },
                            None,
                            window,
                            cx,
                        )
                    })
                })?
                .await;

            _ = existing_window.update(cx, |multi_workspace, _, cx| {
                let workspace = multi_workspace.workspace().clone();
                workspace.update(cx, |workspace, cx| {
                    for item in open_results.iter().flatten() {
                        if let Err(e) = item {
                            workspace.show_error(format!("{e}"), cx);
                        }
                    }
                });
            });

            let items = open_results
                .into_iter()
                .map(|r| r.and_then(|r| r.ok()))
                .collect::<Vec<_>>();
            navigate_to_positions(&existing_window, items, &paths_with_positions, cx);

            return Ok(existing_window);
        }
        // If the remote connection is dead (e.g. server not running after failed reconnect),
        // fall through to establish a fresh connection instead of showing an error.
        log::info!(
            "existing remote workspace found but connection is dead, starting fresh connection"
        );
        existing_window
            .update(cx, |_, window, cx| {
                existing_workspace.update(cx, |workspace, cx| {
                    open_options.workspace_id = workspace.database_id();
                    if let Some(overlay) = workspace.active_modal::<DisconnectedOverlay>(cx) {
                        overlay.update(cx, |overlay, cx| overlay.finished(cx));
                    }
                    workspace.flush_serialization_checked(window, cx)
                })
            })?
            .await?;
    }

    let created_new_window = open_options.requesting_window.is_none();
    let (window, initial_workspace) = if let Some(window) = open_options.requesting_window {
        let workspace = window.update(cx, |multi_workspace, _, _| {
            multi_workspace.workspace().clone()
        })?;
        (window, workspace)
    } else {
        let workspace_position = cx
            .update(|cx| {
                workspace::remote_workspace_position_from_db(
                    connection_options.clone(),
                    &paths,
                    open_options.workspace_id,
                    cx,
                )
            })
            .await
            .context("fetching remote workspace position from db")?;

        let mut options =
            cx.update(|cx| (app_state.build_window_options)(workspace_position.display, cx));
        options.window_bounds = workspace_position.window_bounds;
        options.focus = open_options.focus.unwrap_or(options.focus);
        options.show |= !focus;

        let window = cx.open_window(options, |window, cx| {
            let project = project::Project::local(
                app_state.client.clone(),
                app_state.node_runtime.clone(),
                app_state.user_store.clone(),
                app_state.languages.clone(),
                app_state.fs.clone(),
                None,
                project::LocalProjectFlags {
                    init_worktree_trust: false,
                    ..Default::default()
                },
                cx,
            );
            let workspace = cx.new(|cx| {
                let mut workspace = Workspace::new(None, project, app_state.clone(), window, cx);
                workspace.centered_layout = workspace_position.centered_layout;
                workspace
            });
            cx.new(|cx| {
                let mut multi_workspace = MultiWorkspace::new(workspace, window, cx);
                if let Some(workspace_id) = open_options.workspace_id {
                    multi_workspace.set_restore_key(workspace_id);
                }
                multi_workspace
            })
        })?;
        let workspace = window.update(cx, |multi_workspace, _, _cx| {
            multi_workspace.workspace().clone()
        })?;
        (window, workspace)
    };

    let remove_untouched_placeholder = |cx: &mut AsyncApp| {
        if created_new_window {
            window
                .update(cx, |multi_workspace, window, cx| {
                    let workspace = initial_workspace.read(cx);
                    if multi_workspace.workspaces().eq([&initial_workspace])
                        && !multi_workspace.is_workspace_retained(&initial_workspace)
                        && multi_workspace.project_group_keys().is_empty()
                        && workspace.database_id().is_none()
                        && workspace.items(cx).next().is_none()
                        && workspace.worktrees(cx).next().is_none()
                    {
                        window.remove_window();
                    }
                })
                .ok();
        }
    };

    loop {
        let (cancel_tx, mut cancel_rx) = oneshot::channel();
        let delegate = window.update(cx, {
            let paths = paths.clone();
            let connection_options = connection_options.clone();
            let initial_workspace = initial_workspace.clone();
            move |_multi_workspace: &mut MultiWorkspace, window, cx| {
                if focus {
                    window.activate_window();
                }
                initial_workspace.update(cx, |workspace, cx| {
                    workspace.hide_modal(window, cx);
                    workspace.toggle_modal(window, cx, |window, cx| {
                        RemoteConnectionModal::new(&connection_options, paths, window, cx)
                    });

                    let ui = workspace
                        .active_modal::<RemoteConnectionModal>(cx)?
                        .read(cx)
                        .prompt
                        .clone();

                    ui.update(cx, |ui, _cx| {
                        ui.set_cancellation_tx(cancel_tx);
                    });

                    Some(Arc::new(RemoteClientDelegate::new(
                        window.window_handle(),
                        ui.downgrade(),
                        if let RemoteConnectionOptions::Ssh(options) = &connection_options {
                            options
                                .password
                                .as_deref()
                                .and_then(|pw| EncryptedPassword::try_from(pw).ok())
                        } else {
                            None
                        },
                    )))
                })
            }
        })?;

        let Some(delegate) = delegate else { break };

        let connection = remote::connect(connection_options.clone(), delegate.clone(), cx);
        let connection = select! {
            _ = cancel_rx => {
                initial_workspace.update(cx, |workspace, cx| {
                    if let Some(ui) = workspace.active_modal::<RemoteConnectionModal>(cx) {
                        ui.update(cx, |modal, cx| modal.finished(cx))
                    }
                });

                break;
            },
            result = connection.fuse() => result,
        };
        let remote_connection = match connection {
            Ok(connection) => connection,
            Err(e) => {
                initial_workspace.update(cx, |workspace, cx| {
                    if let Some(ui) = workspace.active_modal::<RemoteConnectionModal>(cx) {
                        ui.update(cx, |modal, cx| modal.finished(cx))
                    }
                });
                log::error!("Failed to open project: {e:#}");
                let response = window
                    .update(cx, |_, window, cx| {
                        window.prompt(
                            PromptLevel::Critical,
                            match connection_options {
                                RemoteConnectionOptions::Ssh(_) => "Failed to connect over SSH",
                                RemoteConnectionOptions::Wsl(_) => "Failed to connect to WSL",
                                RemoteConnectionOptions::Docker(_) => {
                                    "Failed to connect to Dev Container"
                                }
                                #[cfg(any(test, feature = "test-support"))]
                                RemoteConnectionOptions::Mock(_) => {
                                    "Failed to connect to mock server"
                                }
                            },
                            Some(&format!("{e:#}")),
                            &["Retry", "Cancel"],
                            cx,
                        )
                    })?
                    .await;

                if response == Ok(0) {
                    continue;
                }

                remove_untouched_placeholder(cx);
                return Ok(window);
            }
        };

        let (paths, paths_with_positions) =
            determine_paths_with_positions(&remote_connection, paths.clone()).await;

        let opened = cx
            .update(|cx| {
                workspace::open_remote_project_with_new_connection(
                    window,
                    remote_connection,
                    cancel_rx,
                    delegate.clone(),
                    app_state.clone(),
                    paths.clone(),
                    open_options.workspace_id,
                    focus,
                    cx,
                )
            })
            .await;

        initial_workspace.update(cx, |workspace, cx| {
            if let Some(ui) = workspace.active_modal::<RemoteConnectionModal>(cx) {
                ui.update(cx, |modal, cx| modal.finished(cx))
            }
        });

        match opened {
            Err(e) => {
                log::error!("Failed to open project: {e:#}");
                let response = window
                    .update(cx, |_, window, cx| {
                        window.prompt(
                            PromptLevel::Critical,
                            match connection_options {
                                RemoteConnectionOptions::Ssh(_) => "Failed to connect over SSH",
                                RemoteConnectionOptions::Wsl(_) => "Failed to connect to WSL",
                                RemoteConnectionOptions::Docker(_) => {
                                    "Failed to connect to Dev Container"
                                }
                                #[cfg(any(test, feature = "test-support"))]
                                RemoteConnectionOptions::Mock(_) => {
                                    "Failed to connect to mock server"
                                }
                            },
                            Some(&format!("{e:#}")),
                            &["Retry", "Cancel"],
                            cx,
                        )
                    })?
                    .await;
                if response == Ok(0) {
                    continue;
                }

                remove_untouched_placeholder(cx);
                initial_workspace.update(cx, |workspace, cx| {
                    trusted_worktrees::track_worktree_trust(
                        workspace.project().read(cx).worktree_store(),
                        None,
                        None,
                        None,
                        cx,
                    );
                });
            }

            Ok((_, items)) => {
                navigate_to_positions(&window, items, &paths_with_positions, cx);
            }
        }

        break;
    }

    Ok(window)
}

pub fn navigate_to_positions(
    window: &WindowHandle<MultiWorkspace>,
    items: impl IntoIterator<Item = Option<Box<dyn workspace::item::ItemHandle>>>,
    positions: &[PathWithPosition],
    cx: &mut AsyncApp,
) {
    for (item, path) in items.into_iter().zip(positions) {
        let Some(item) = item else {
            continue;
        };
        let Some(row) = path.row else {
            continue;
        };
        if let Some(active_editor) = item.downcast::<Editor>() {
            window
                .update(cx, |_, window, cx| {
                    active_editor.update(cx, |editor, cx| {
                        let row = row.saturating_sub(1);
                        let col = path.column.unwrap_or(0).saturating_sub(1);
                        let Some(buffer) = editor.buffer().read(cx).as_singleton() else {
                            return;
                        };
                        let buffer_snapshot = buffer.read(cx).snapshot();
                        let point = buffer_snapshot.point_from_external_input(row, col);
                        editor.go_to_singleton_buffer_point(point, window, cx);
                    });
                })
                .ok();
        }
    }
}

pub(crate) async fn determine_paths_with_positions(
    remote_connection: &Arc<dyn RemoteConnection>,
    mut paths: Vec<PathBuf>,
) -> (Vec<PathBuf>, Vec<PathWithPosition>) {
    let mut paths_with_positions = Vec::<PathWithPosition>::new();
    for path in &mut paths {
        if let Some(path_str) = path.to_str() {
            let path_with_position = PathWithPosition::parse_str(&path_str);
            if path_with_position.row.is_some() {
                if !path_exists(&remote_connection, &path).await {
                    *path = path_with_position.path.clone();
                    paths_with_positions.push(path_with_position);
                    continue;
                }
            }
        }
        paths_with_positions.push(PathWithPosition::from_path(path.clone()))
    }
    (paths, paths_with_positions)
}

async fn path_exists(connection: &Arc<dyn RemoteConnection>, path: &Path) -> bool {
    let Ok(command) = connection.build_command(
        Some("test".to_string()),
        &["-e".to_owned(), path.to_string_lossy().to_string()],
        &Default::default(),
        None,
        None,
        Interactive::No,
    ) else {
        return false;
    };
    let Ok(mut child) = util::command::new_command(command.program)
        .args(command.args)
        .envs(command.env)
        .spawn()
    else {
        return false;
    };
    child.status().await.is_ok_and(|status| status.success())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tests::init_test;
    use extension::ExtensionHostProxy;
    use fs::FakeFs;
    use gpui::{AppContext, Entity, FutureExt as _, TestAppContext};
    use http_client::BlockedHttpClient;
    use language::Point;
    use node_runtime::NodeRuntime;
    use project::{Project, ProjectGroupKey};
    use remote::RemoteClient;
    use remote_server::{HeadlessAppState, HeadlessProject};
    use rpc::{AnyProtoClient, proto};
    use serde_json::json;
    use std::time::Duration;
    use util::{path, path_list::PathList};
    use workspace::{
        MultiWorkspaceState, SerializedMultiWorkspace, SerializedProjectGroup, SessionWorkspace,
        apply_restored_multiworkspace_state, find_existing_workspace,
    };

    #[gpui::test]
    async fn test_open_remote_project_with_mock_connection(
        cx: &mut TestAppContext,
        server_cx: &mut TestAppContext,
    ) {
        let app_state = init_test(cx);
        let executor = cx.executor();

        cx.update(|cx| {
            release_channel::init(semver::Version::new(0, 0, 0), cx);
        });
        server_cx.update(|cx| {
            release_channel::init(semver::Version::new(0, 0, 0), cx);
        });

        let (opts, server_session, connect_guard) = RemoteClient::fake_server(cx, server_cx);

        let remote_fs = FakeFs::new(server_cx.executor());
        remote_fs
            .insert_tree(
                path!("/project"),
                json!({
                    "src": {
                        "main.rs": "fn main() {}",
                    },
                    "README.md": "# Test Project",
                }),
            )
            .await;

        server_cx.update(HeadlessProject::init);
        let _headless = create_headless_project(server_session, remote_fs, server_cx);

        drop(connect_guard);

        let paths = vec![PathBuf::from(path!("/project"))];
        let open_options = workspace::OpenOptions::default();

        let mut async_cx = cx.to_async();
        let result = open_remote_project(opts, paths, app_state, open_options, &mut async_cx).await;

        executor.run_until_parked();

        assert!(result.is_ok(), "open_remote_project should succeed");

        let windows = cx.update(|cx| cx.windows().len());
        assert_eq!(windows, 1, "Should have opened a window");

        let multi_workspace_handle =
            cx.update(|cx| cx.windows()[0].downcast::<MultiWorkspace>().unwrap());

        multi_workspace_handle
            .update(cx, |multi_workspace, _, cx| {
                let workspace = multi_workspace.workspace().clone();
                workspace.update(cx, |workspace, cx| {
                    let project = workspace.project().read(cx);
                    assert!(project.is_remote(), "Project should be a remote project");
                });
            })
            .unwrap();
    }

    #[gpui::test]
    async fn test_cancel_remote_error_preserves_reused_placeholder(
        cx: &mut TestAppContext,
        server_cx: &mut TestAppContext,
    ) {
        let app_state = init_test(cx);
        cx.update(|cx| release_channel::init(semver::Version::new(0, 0, 0), cx));
        server_cx.update(|cx| release_channel::init(semver::Version::new(0, 0, 0), cx));

        for fail_connection in [true, false] {
            for reuse in ["untouched", "edited", "held", "replaced"] {
                let (options, server_session, connect_guard) =
                    RemoteClient::fake_server(cx, server_cx);
                let ping_handler = server_cx.new(|_| ());
                server_session.add_request_handler::<proto::Ping, _, _, _>(
                    ping_handler.downgrade(),
                    |_, _, _| async { anyhow::bail!("mock setup failed") },
                );
                if fail_connection {
                    let RemoteConnectionOptions::Mock(options) = &options else {
                        unreachable!();
                    };
                    cx.update(|cx| {
                        assert!(
                            cx.global_mut::<remote::MockConnectionRegistry>()
                                .take(options)
                                .is_some()
                        );
                    });
                }
                drop(connect_guard);
                let opening = cx.spawn({
                    let app_state = app_state.clone();
                    async move |mut cx| {
                        open_remote_project(
                            options,
                            Vec::new(),
                            app_state,
                            OpenOptions::default(),
                            &mut cx,
                        )
                        .await
                    }
                });
                cx.run_until_parked();
                assert!(cx.has_pending_prompt());
                assert_eq!(cx.windows().len(), 1);
                let window = cx.windows()[0].downcast::<MultiWorkspace>().unwrap();
                let initial_workspace = window
                    .read_with(cx, |multi_workspace, _| multi_workspace.workspace().clone())
                    .unwrap();

                let editor = if reuse == "edited" {
                    let editor = window
                        .update(cx, |_, window, cx| {
                            initial_workspace.update(cx, |workspace, cx| {
                                Editor::new_in_workspace(workspace, window, cx)
                            })
                        })
                        .unwrap()
                        .await
                        .unwrap();
                    window
                        .update(cx, |_, window, cx| {
                            editor.update(cx, |editor, cx| {
                                editor.insert("unsaved local work", window, cx);
                            });
                        })
                        .unwrap();
                    Some(editor)
                } else {
                    None
                };
                if reuse == "held" || reuse == "replaced" {
                    let project = Project::test(app_state.fs.clone(), [], cx).await;
                    window
                        .update(cx, |multi_workspace, window, cx| {
                            let workspace = cx.new(|cx| {
                                Workspace::new(None, project, app_state.clone(), window, cx)
                            });
                            if reuse == "held" {
                                multi_workspace.add(workspace, window, cx);
                            } else {
                                multi_workspace.activate(workspace, None, window, cx);
                            }
                        })
                        .unwrap();
                }
                cx.run_until_parked();
                let expected_workspaces = window
                    .read_with(cx, |multi_workspace, _| {
                        multi_workspace.workspaces().cloned().collect::<Vec<_>>()
                    })
                    .unwrap();

                cx.simulate_prompt_answer("Cancel");
                assert_eq!(opening.await.unwrap(), window);
                cx.run_until_parked();
                if reuse == "untouched" {
                    assert!(cx.windows().is_empty());
                } else {
                    assert_eq!(cx.windows().len(), 1);
                    window
                        .update(cx, |multi_workspace, window, cx| {
                            assert_eq!(
                                multi_workspace.workspaces().cloned().collect::<Vec<_>>(),
                                expected_workspaces
                            );
                            if let Some(editor) = editor {
                                assert_eq!(editor.read(cx).text(cx), "unsaved local work");
                            }
                            window.remove_window();
                        })
                        .unwrap();
                }
            }
        }
    }

    #[gpui::test]
    async fn test_find_existing_workspace_does_not_reuse_workspace_from_another_remote(
        cx: &mut TestAppContext,
        server_cx: &mut TestAppContext,
    ) {
        let app_state = init_test(cx);
        let executor = cx.executor();

        cx.update(|cx| {
            release_channel::init(semver::Version::new(0, 0, 0), cx);
        });
        server_cx.update(|cx| {
            release_channel::init(semver::Version::new(0, 0, 0), cx);
        });

        let (opts, server_session, connect_guard) = RemoteClient::fake_server(cx, server_cx);

        let remote_fs = FakeFs::new(server_cx.executor());
        remote_fs
            .insert_tree(
                path!("/project"),
                json!({
                    "src": {
                        "main.rs": "fn main() {}",
                        "lib.rs": "pub fn hello() {}",
                    },
                    "README.md": "# Test Project",
                }),
            )
            .await;

        server_cx.update(HeadlessProject::init);
        let _headless = create_headless_project(server_session, remote_fs, server_cx);

        drop(connect_guard);

        // First open: create a new window for the remote project.
        let paths = vec![PathBuf::from(path!("/project"))];
        let mut async_cx = cx.to_async();
        open_remote_project(
            opts.clone(),
            paths,
            app_state.clone(),
            workspace::OpenOptions::default(),
            &mut async_cx,
        )
        .await
        .expect("first open_remote_project should succeed");

        executor.run_until_parked();

        assert_eq!(
            cx.update(|cx| cx.windows().len()),
            1,
            "First open should create exactly one window"
        );

        let first_window = cx.update(|cx| cx.windows()[0].downcast::<MultiWorkspace>().unwrap());
        let first_workspace = first_window
            .read_with(cx, |multi_workspace, _| multi_workspace.workspace().clone())
            .unwrap();

        let (other_opts, other_server_session, other_connect_guard) =
            RemoteClient::fake_server(cx, server_cx);
        let ping_handler = server_cx.new(|_| ());
        other_server_session.add_request_handler::<proto::Ping, _, _, _>(
            ping_handler.downgrade(),
            |_, _, _| async { Ok(proto::Ack {}) },
        );
        drop(other_connect_guard);
        let other_remote_client = RemoteClient::connect_mock(other_opts.clone(), cx).await;
        let other_project = cx.update(|cx| {
            Project::remote(
                other_remote_client,
                app_state.client.clone(),
                app_state.node_runtime.clone(),
                app_state.user_store.clone(),
                app_state.languages.clone(),
                app_state.fs.clone(),
                false,
                cx,
            )
        });
        first_window
            .update(cx, |multi_workspace, window, cx| {
                let other_workspace =
                    cx.new(|cx| Workspace::new(None, other_project, app_state.clone(), window, cx));
                multi_workspace.add(other_workspace, window, cx);
            })
            .unwrap();

        let search_paths = vec![PathBuf::from(path!("/project/src/lib.rs"))];
        let (found, _) = find_existing_workspace(
            &search_paths,
            &workspace::OpenOptions::default(),
            &SerializedWorkspaceLocation::Remote(other_opts),
            &mut async_cx,
        )
        .await
        .unwrap();
        assert!(
            found.is_none(),
            "a matching path on another remote must not satisfy the request"
        );

        // Verify find_existing_workspace discovers the remote workspace.
        let (found, _open_visible) = find_existing_workspace(
            &search_paths,
            &workspace::OpenOptions::default(),
            &SerializedWorkspaceLocation::Remote(opts.clone()),
            &mut async_cx,
        )
        .await
        .unwrap();

        assert!(
            found.is_some(),
            "find_existing_workspace should locate the existing remote workspace"
        );
        let (found_window, WorkspaceMatch::Loaded(found_workspace)) = found.unwrap() else {
            panic!("expected the loaded remote workspace");
        };
        assert_eq!(
            found_window, first_window,
            "find_existing_workspace should return the same window"
        );
        assert_eq!(
            found_workspace, first_workspace,
            "find_existing_workspace should return the workspace for the requested remote"
        );

        // Second open with the same connection options should reuse the window.
        let second_paths = vec![PathBuf::from(path!("/project/src/lib.rs"))];
        open_remote_project(
            opts.clone(),
            second_paths,
            app_state.clone(),
            workspace::OpenOptions::default(),
            &mut async_cx,
        )
        .await
        .expect("second open_remote_project should succeed via reuse");

        executor.run_until_parked();

        assert_eq!(
            cx.update(|cx| cx.windows().len()),
            1,
            "Second open should reuse the existing window, not create a new one"
        );

        let still_first_window =
            cx.update(|cx| cx.windows()[0].downcast::<MultiWorkspace>().unwrap());
        assert_eq!(
            still_first_window, first_window,
            "The window handle should be the same after reuse"
        );
    }

    #[gpui::test]
    async fn test_reopen_existing_remote_root_treats_root_as_directory(
        cx: &mut TestAppContext,
        server_cx: &mut TestAppContext,
    ) {
        let app_state = init_test(cx);
        let executor = cx.executor();

        cx.update(|cx| {
            release_channel::init(semver::Version::new(0, 0, 0), cx);
        });
        server_cx.update(|cx| {
            release_channel::init(semver::Version::new(0, 0, 0), cx);
        });

        let (opts, server_session, connect_guard) = RemoteClient::fake_server(cx, server_cx);

        let remote_fs = FakeFs::new(server_cx.executor());
        let remote_home = paths::home_dir();
        let canonical_project_path = remote_home.join("remote-reopen-root-project");
        remote_fs
            .insert_tree(
                &canonical_project_path,
                json!({
                    "src": {
                        "main.rs": "fn main() {}",
                    },
                    "README.md": "# Test Project",
                }),
            )
            .await;

        server_cx.update(HeadlessProject::init);
        let _headless = create_headless_project(server_session, remote_fs, server_cx);

        drop(connect_guard);

        let mut async_cx = cx.to_async();
        let window = open_remote_project(
            opts,
            vec![canonical_project_path.clone()],
            app_state,
            workspace::OpenOptions::default(),
            &mut async_cx,
        )
        .await
        .expect("initial open_remote_project should succeed");

        executor.run_until_parked();

        let open_results = window
            .update(cx, |multi_workspace, window, cx| {
                let workspace = multi_workspace.workspace().clone();
                workspace.update(cx, |workspace, cx| {
                    workspace.open_paths(
                        vec![canonical_project_path.clone()],
                        workspace::OpenOptions {
                            visible: Some(workspace::OpenVisible::All),
                            ..Default::default()
                        },
                        None,
                        window,
                        cx,
                    )
                })
            })
            .unwrap()
            .await;

        assert_eq!(open_results.len(), 1, "should return one open result");
        assert!(
            open_results[0].is_none(),
            "reopening a remote root directory should not try to open it as a file"
        );
    }

    #[gpui::test]
    async fn test_reconnect_when_server_not_running(
        cx: &mut TestAppContext,
        server_cx: &mut TestAppContext,
    ) {
        let app_state = init_test(cx);
        let executor = cx.executor();

        cx.update(|cx| {
            release_channel::init(semver::Version::new(0, 0, 0), cx);
        });
        server_cx.update(|cx| {
            release_channel::init(semver::Version::new(0, 0, 0), cx);
        });

        let (opts, server_session, connect_guard) = RemoteClient::fake_server(cx, server_cx);

        let remote_fs = FakeFs::new(server_cx.executor());
        remote_fs
            .insert_tree(
                path!("/project"),
                json!({
                    "src": {
                        "main.rs": "fn main() {}",
                    },
                }),
            )
            .await;

        server_cx.update(HeadlessProject::init);
        let _headless = create_headless_project(server_session, remote_fs.clone(), server_cx);

        drop(connect_guard);

        // Open the remote project normally.
        let paths = vec![PathBuf::from(path!("/project"))];
        let mut async_cx = cx.to_async();
        open_remote_project(
            opts.clone(),
            paths.clone(),
            app_state.clone(),
            workspace::OpenOptions::default(),
            &mut async_cx,
        )
        .await
        .expect("initial open should succeed");

        executor.run_until_parked();

        assert_eq!(cx.update(|cx| cx.windows().len()), 1);
        let window = cx.update(|cx| cx.windows()[0].downcast::<MultiWorkspace>().unwrap());
        let (workspace_id, workspace_ids) = window
            .read_with(cx, |multi_workspace, cx| {
                let mut workspace_ids = multi_workspace
                    .workspaces()
                    .map(|workspace| workspace.read(cx).database_id())
                    .collect::<Vec<_>>();
                workspace_ids.sort();
                (
                    multi_workspace.workspace().read(cx).database_id().unwrap(),
                    workspace_ids,
                )
            })
            .unwrap();
        let items = window
            .update(cx, |multi_workspace, window, cx| {
                multi_workspace.workspace().update(cx, |workspace, cx| {
                    workspace.open_paths(
                        vec![PathBuf::from(path!("/project/src/main.rs"))],
                        OpenOptions::default(),
                        None,
                        window,
                        cx,
                    )
                })
            })
            .unwrap()
            .await;
        assert_eq!(items.len(), 1);
        let file = items.into_iter().next().unwrap().unwrap().unwrap();
        let file = file.downcast::<Editor>().unwrap();
        assert_eq!(
            file.read_with(cx, |editor, cx| editor.text(cx)),
            "fn main() {}"
        );
        let draft = window
            .update(cx, |multi_workspace, window, cx| {
                multi_workspace.workspace().update(cx, |workspace, cx| {
                    Editor::new_in_workspace(workspace, window, cx)
                })
            })
            .unwrap()
            .await
            .unwrap();
        window
            .update(cx, |multi_workspace, window, cx| {
                draft.update(cx, |editor, cx| {
                    editor.insert("saved dirty draft", window, cx);
                });
                multi_workspace.workspace().update(cx, |workspace, cx| {
                    workspace.flush_serialization_checked(window, cx)
                })
            })
            .unwrap()
            .await
            .unwrap();

        // Force the remote client into ServerNotRunning state (simulates the
        // scenario where the remote server died and reconnection failed).
        window
            .update(cx, |multi_workspace, window, cx| {
                draft.update(cx, |editor, cx| {
                    editor.insert(" with pending edits", window, cx);
                });
                let workspace = multi_workspace.workspace().clone();
                workspace.update(cx, |workspace, cx| {
                    let client = workspace
                        .project()
                        .read(cx)
                        .remote_client()
                        .expect("should have remote client");
                    client.update(cx, |client, cx| {
                        client.force_server_not_running(cx);
                    });
                });
            })
            .unwrap();

        executor.run_until_parked();
        draft.read_with(cx, |editor, cx| {
            assert_eq!(editor.text(cx), "saved dirty draft with pending edits");
            assert!(
                editor
                    .buffer()
                    .read(cx)
                    .as_singleton()
                    .unwrap()
                    .read(cx)
                    .has_unsaved_edits()
            );
        });

        // Register a new mock server under the same options so the reconnect
        // path can establish a fresh connection.
        let (server_session_2, connect_guard_2) =
            RemoteClient::fake_server_with_opts(&opts, cx, server_cx);

        let _headless_2 = create_headless_project(server_session_2, remote_fs, server_cx);

        drop(connect_guard_2);

        let result = open_remote_project(
            opts,
            paths,
            app_state,
            OpenOptions::default(),
            &mut async_cx,
        )
        .with_timeout(Duration::from_secs(10), &cx.executor())
        .await;
        assert_eq!(cx.pending_prompt(), None);
        let result = result.expect("remote reconnect timed out");

        executor.run_until_parked();

        assert!(
            result.is_ok(),
            "reconnect should succeed but got: {:?}",
            result.err()
        );

        assert_eq!(result.unwrap(), window);

        // Should still be a single window with a working remote project.
        assert_eq!(cx.update(|cx| cx.windows().len()), 1);

        window
            .update(cx, |multi_workspace, _, cx| {
                let mut restored_workspace_ids = multi_workspace
                    .workspaces()
                    .map(|workspace| workspace.read(cx).database_id())
                    .collect::<Vec<_>>();
                restored_workspace_ids.sort();
                assert_eq!(restored_workspace_ids, workspace_ids);
                let workspace = multi_workspace.workspace().clone();
                workspace.update(cx, |workspace, cx| {
                    assert!(
                        workspace.project().read(cx).is_remote(),
                        "project should be remote after reconnect"
                    );
                    assert_eq!(workspace.database_id(), Some(workspace_id));
                    assert!(
                        workspace
                            .project()
                            .read(cx)
                            .remote_client()
                            .unwrap()
                            .read(cx)
                            .remote_connection()
                            .is_some()
                    );
                    let mut restored = workspace
                        .items_of_type::<Editor>(cx)
                        .map(|editor| {
                            let editor = editor.read(cx);
                            (
                                editor.text(cx),
                                editor
                                    .buffer()
                                    .read(cx)
                                    .as_singleton()
                                    .unwrap()
                                    .read(cx)
                                    .is_dirty(),
                            )
                        })
                        .collect::<Vec<_>>();
                    restored.sort();
                    assert_eq!(
                        restored,
                        vec![
                            ("fn main() {}".to_owned(), false),
                            ("saved dirty draft with pending edits".to_owned(), true),
                        ]
                    );
                });
            })
            .unwrap();
    }

    #[gpui::test]
    async fn test_reopen_unloaded_remote_with_local_workspace_active(
        cx: &mut TestAppContext,
        server_cx: &mut TestAppContext,
    ) {
        let app_state = init_test(cx);
        cx.update(|cx| release_channel::init(semver::Version::new(0, 0, 0), cx));
        server_cx.update(|cx| {
            release_channel::init(semver::Version::new(0, 0, 0), cx);
            HeadlessProject::init(cx);
        });
        let (options, server_session, connect_guard) = RemoteClient::fake_server(cx, server_cx);
        let remote_fs = FakeFs::new(server_cx.executor());
        remote_fs
            .insert_tree(
                path!("/project"),
                json!({"src": {"main.rs": "fn main() {}"}}),
            )
            .await;
        let headless = create_headless_project(server_session, remote_fs.clone(), server_cx);
        drop(connect_guard);

        let mut async_cx = cx.to_async();
        let remote_window = open_remote_project(
            options.clone(),
            vec![PathBuf::from(path!("/project"))],
            app_state.clone(),
            OpenOptions::default(),
            &mut async_cx,
        )
        .await
        .unwrap();
        let (remote_id, flush) = remote_window
            .update(cx, |multi_workspace, window, cx| {
                multi_workspace.workspace().update(cx, |workspace, cx| {
                    (
                        workspace.database_id().unwrap(),
                        workspace.flush_serialization_checked(window, cx),
                    )
                })
            })
            .unwrap();
        flush.await.unwrap();
        close_remote_window(remote_window, cx);
        drop(headless);
        cx.run_until_parked();

        let local_fs = FakeFs::new(cx.executor());
        local_fs
            .insert_tree(path!("/local"), json!({"local.txt": "local content"}))
            .await;
        let local_project = Project::test(local_fs.clone(), [path!("/local").as_ref()], cx).await;
        let db = cx.update(|cx| WorkspaceDb::global(cx));
        let local_id = db.next_id().await.unwrap();
        let window = cx.add_window(|window, cx| {
            let workspace = cx.new(|cx| {
                Workspace::new(Some(local_id), local_project, app_state.clone(), window, cx)
            });
            let mut multi_workspace = MultiWorkspace::new(workspace, window, cx);
            multi_workspace.retain_active_workspace(cx);
            multi_workspace
        });
        let local_workspace = window
            .read_with(cx, |multi_workspace, _| multi_workspace.workspace().clone())
            .unwrap();
        apply_restored_multiworkspace_state(
            window,
            &SerializedMultiWorkspace {
                active_workspace: SessionWorkspace {
                    workspace_id: local_id,
                    location: SerializedWorkspaceLocation::Local,
                    paths: PathList::new(&[PathBuf::from(path!("/local"))]),
                    window_id: Some(window.window_id()),
                },
                workspace_ids: vec![local_id, remote_id],
                scratch_workspaces: Vec::new(),
                state: MultiWorkspaceState {
                    active_workspace_id: Some(local_id),
                    project_groups: vec![SerializedProjectGroup::from_group(
                        &ProjectGroupKey::new(
                            Some(options.clone()),
                            PathList::new(&[PathBuf::from(path!("/project"))]),
                        ),
                        true,
                    )],
                    ..MultiWorkspaceState::default()
                },
            },
            local_fs,
            &mut async_cx,
        )
        .await
        .unwrap();
        window
            .read_with(cx, |multi_workspace, cx| {
                assert_eq!(multi_workspace.workspace(), &local_workspace);
                assert_eq!(multi_workspace.workspaces().count(), 1);
                assert!(!local_workspace.read(cx).project().read(cx).is_remote());
            })
            .unwrap();

        let (server_session, connect_guard) =
            RemoteClient::fake_server_with_opts(&options, cx, server_cx);
        let _headless = create_headless_project(server_session, remote_fs, server_cx);
        drop(connect_guard);

        let subdirectory = vec![PathBuf::from(path!("/project/src"))];
        let location = SerializedWorkspaceLocation::Remote(options.clone());
        let candidate = find_existing_workspace(
            &subdirectory,
            &OpenOptions::default(),
            &location,
            &mut async_cx,
        )
        .await
        .unwrap();
        assert_eq!(
            candidate.0,
            Some((window, WorkspaceMatch::Unloaded(remote_id)))
        );

        let reopened = open_remote_project(
            options,
            vec![PathBuf::from(path!("/project/src/main.rs"))],
            app_state,
            OpenOptions::default(),
            &mut async_cx,
        )
        .with_timeout(Duration::from_secs(10), &cx.executor())
        .await;
        assert_eq!(cx.pending_prompt(), None);
        let reopened = reopened.expect("remote reopen timed out").unwrap();
        assert_eq!(reopened, window);
        let loaded_match = find_existing_workspace(
            &subdirectory,
            &OpenOptions::default(),
            &location,
            &mut async_cx,
        )
        .await
        .unwrap();
        assert_eq!(loaded_match.0, None);
        assert_eq!(cx.windows().len(), 1);
        window
            .read_with(cx, |multi_workspace, cx| {
                let workspace = multi_workspace.workspace().read(cx);
                assert_eq!(workspace.database_id(), Some(remote_id));
                assert_eq!(
                    workspace
                        .root_paths(cx)
                        .iter()
                        .map(|path| path.to_path_buf())
                        .collect::<Vec<_>>(),
                    vec![PathBuf::from(path!("/project"))]
                );
                assert_eq!(
                    workspace
                        .items_of_type::<Editor>(cx)
                        .map(|editor| editor.read(cx).text(cx))
                        .collect::<Vec<_>>(),
                    vec!["fn main() {}".to_owned()]
                );
                assert_eq!(
                    multi_workspace
                        .workspaces()
                        .filter(|workspace| **workspace == local_workspace)
                        .count(),
                    1
                );
                assert_eq!(multi_workspace.workspaces().count(), 2);
            })
            .unwrap();
    }

    #[gpui::test]
    async fn test_reopen_remote_file_roots_preserves_requested_positions(
        cx: &mut TestAppContext,
        server_cx: &mut TestAppContext,
    ) {
        let app_state = init_test(cx);
        cx.update(|cx| release_channel::init(semver::Version::new(0, 0, 0), cx));
        server_cx.update(|cx| {
            release_channel::init(semver::Version::new(0, 0, 0), cx);
            HeadlessProject::init(cx);
        });
        let (options, server_session, connect_guard) = RemoteClient::fake_server(cx, server_cx);
        let remote_fs = FakeFs::new(server_cx.executor());
        let alpha = "alpha one\nalpha two\nalpha three\n";
        let beta = "beta one\nbeta two\nbeta three\n";
        remote_fs
            .insert_tree(path!("/project"), json!({"a.txt": alpha, "b.txt": beta}))
            .await;
        let headless = create_headless_project(server_session, remote_fs.clone(), server_cx);
        drop(connect_guard);
        let roots = vec![
            PathBuf::from(path!("/project/a.txt")),
            PathBuf::from(path!("/project/b.txt")),
        ];
        let mut async_cx = cx.to_async();
        let window = open_remote_project(
            options.clone(),
            roots.clone(),
            app_state.clone(),
            OpenOptions::default(),
            &mut async_cx,
        )
        .await
        .unwrap();
        let (workspace_id, flush) = window
            .update(cx, |multi_workspace, window, cx| {
                multi_workspace.workspace().update(cx, |workspace, cx| {
                    (
                        workspace.database_id().unwrap(),
                        workspace.flush_serialization_checked(window, cx),
                    )
                })
            })
            .unwrap();
        flush.await.unwrap();
        close_remote_window(window, cx);
        drop(headless);
        cx.run_until_parked();

        let (server_session, connect_guard) =
            RemoteClient::fake_server_with_opts(&options, cx, server_cx);
        let _headless = create_headless_project(server_session, remote_fs, server_cx);
        drop(connect_guard);
        let window = open_remote_project(
            options,
            vec![
                PathBuf::from(path!("/project/b.txt:3:2")),
                PathBuf::from(path!("/project/a.txt:2:4")),
            ],
            app_state,
            OpenOptions {
                workspace_id: Some(workspace_id),
                ..OpenOptions::default()
            },
            &mut async_cx,
        )
        .with_timeout(Duration::from_secs(10), &cx.executor())
        .await;
        assert_eq!(cx.pending_prompt(), None);
        let window = window.expect("remote reopen timed out").unwrap();
        window
            .update(cx, |multi_workspace, _, cx| {
                let workspace = multi_workspace.workspace().read(cx);
                assert_eq!(workspace.database_id(), Some(workspace_id));
                assert_eq!(
                    workspace
                        .root_paths(cx)
                        .iter()
                        .map(|path| path.to_path_buf())
                        .collect::<Vec<_>>(),
                    roots
                );
                let editors = workspace.items_of_type::<Editor>(cx).collect::<Vec<_>>();
                let mut positions = editors
                    .into_iter()
                    .map(|editor| {
                        editor.update(cx, |editor, cx| {
                            (
                                editor.text(cx),
                                editor
                                    .selections
                                    .newest::<Point>(&editor.display_snapshot(cx))
                                    .head(),
                            )
                        })
                    })
                    .collect::<Vec<_>>();
                positions.sort_by(|left, right| left.0.cmp(&right.0));
                assert_eq!(
                    positions,
                    vec![
                        (alpha.to_owned(), Point::new(1, 3)),
                        (beta.to_owned(), Point::new(2, 1))
                    ]
                );
            })
            .unwrap();
    }

    fn close_remote_window(window: WindowHandle<MultiWorkspace>, cx: &mut TestAppContext) {
        window
            .update(cx, |multi_workspace, window, cx| {
                let client = multi_workspace
                    .workspace()
                    .read(cx)
                    .project()
                    .read(cx)
                    .remote_client()
                    .unwrap();
                client.update(cx, |client, cx| client.force_server_not_running(cx));
                window.remove_window();
            })
            .unwrap();
    }

    fn create_headless_project(
        session: AnyProtoClient,
        fs: Arc<FakeFs>,
        cx: &mut TestAppContext,
    ) -> Entity<HeadlessProject> {
        let languages = Arc::new(language::LanguageRegistry::new(cx.executor()));
        cx.new(|cx| {
            HeadlessProject::new(
                HeadlessAppState {
                    session,
                    fs,
                    http_client: Arc::new(BlockedHttpClient),
                    node_runtime: NodeRuntime::unavailable(),
                    languages,
                    extension_host_proxy: Arc::new(ExtensionHostProxy::new()),
                    startup_time: std::time::Instant::now(),
                },
                false,
                cx,
            )
        })
    }
}
