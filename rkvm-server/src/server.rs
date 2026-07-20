use crate::config::{DeviceGroup, DeviceMatch};
pub(crate) use crate::routing::SwitchBinding;
use crate::routing::{Action as RoutingAction, Router};
use rkvm_input::abs::{AbsAxis, AbsInfo};
use rkvm_input::event::Event;
use rkvm_input::interceptor::Frame;
use rkvm_input::key::Key;
use rkvm_input::monitor::{
    ActivationId, CandidatePolicy, GroupPolicy, Monitor, MonitorEvent, ReleaseCause,
};
use rkvm_input::rel::RelAxis;
use rkvm_net::auth::{AuthChallenge, AuthResponse, AuthStatus};
use rkvm_net::message::Message;
use rkvm_net::version::Version;
use rkvm_net::{Pong, Update};
use slab::Slab;
use std::collections::{HashMap, HashSet};
use std::ffi::CString;
use std::io::{self, ErrorKind};
use std::net::SocketAddr;
use std::time::{Duration, Instant};
use thiserror::Error;
use tokio::io::{AsyncWriteExt, BufStream};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc::error::TrySendError;
use tokio::sync::mpsc::{self, Receiver, Sender};
use tokio::task::JoinHandle;
use tokio::time;
use tokio_rustls::TlsAcceptor;
use tracing::Instrument;

pub(crate) const DEFAULT_CLIENT_QUEUE_SIZE: usize = 256;

#[derive(Error, Debug)]
pub enum Error {
    #[error("Network error: {0}")]
    Network(io::Error),
    #[error("Input error: {0}")]
    Input(io::Error),
    #[error("Event queue overflow")]
    Overflow,
}

pub async fn run(
    listen: SocketAddr,
    acceptor: TlsAcceptor,
    password: &str,
    switch_bindings: &[SwitchBinding],
    propagate_switch_keys: bool,
    device_whitelist: Option<Vec<DeviceMatch>>,
    device_groups: Option<Vec<DeviceGroup>>,
    client_queue_size: usize,
) -> Result<(), Error> {
    let listener = TcpListener::bind(&listen).await.map_err(Error::Network)?;
    tracing::info!("Listening on {}", listen);

    let mut monitor = match (device_whitelist, device_groups) {
        (_, Some(groups)) => Monitor::with_groups(
            groups
                .into_iter()
                .map(|group| {
                    GroupPolicy::new(
                        group.name,
                        group
                            .candidates
                            .into_iter()
                            .map(|candidate| {
                                let exact_path = candidate.matcher.path.clone();
                                let delay = Duration::from_millis(
                                    candidate.grab_delay_ms.unwrap_or_default(),
                                );
                                CandidatePolicy::new(exact_path, delay, move |device| {
                                    candidate.matcher.matches(device)
                                })
                            })
                            .collect(),
                    )
                })
                .collect(),
        ),
        (Some(device_whitelist), None) => Monitor::with_filter(move |device| {
            device_whitelist.iter().any(|item| item.matches(device))
        }),
        (None, None) => Monitor::new(),
    };
    let mut devices = Slab::<Device>::new();
    let mut clients = Slab::<(Sender<_>, SocketAddr)>::new();
    let mut router = Router::new(switch_bindings, propagate_switch_keys);

    let (events_sender, mut events_receiver) = mpsc::channel(1);
    let (authenticated_sender, mut authenticated_receiver) = mpsc::channel(32);

    loop {
        let event = async { events_receiver.recv().await.unwrap() };

        tokio::select! {
            result = listener.accept() => {
                let (stream, addr) = result.map_err(Error::Network)?;
                stream.set_nodelay(true).map_err(Error::Network)?;

                let acceptor = acceptor.clone();
                let password = password.to_owned();
                let authenticated_sender = authenticated_sender.clone();

                remove_dead_clients(&mut clients);
                stabilize_routes(&mut router, &devices, &mut clients)?;

                let span = tracing::info_span!("connection", addr = %addr);
                tokio::spawn(
                    async move {
                        tracing::info!("Connected");

                        let stream = match authenticate(stream, acceptor, &password).await {
                            Ok(stream) => stream,
                            Err(err) => {
                                tracing::error!("Disconnected: {}", err);
                                return;
                            }
                        };

                        let (sender, receiver) = mpsc::channel(client_queue_size);
                        if authenticated_sender
                            .send(AuthenticatedClient { sender, addr })
                            .await
                            .is_err()
                        {
                            return;
                        }

                        match client(receiver, stream).await {
                            Ok(()) => tracing::info!("Disconnected"),
                            Err(err) => tracing::error!("Disconnected: {}", err),
                        }
                    }
                    .instrument(span),
                );
            }
            result = authenticated_receiver.recv() => {
                let Some(authenticated) = result else {
                    return Err(Error::Network(io::Error::new(
                        ErrorKind::BrokenPipe,
                        "Authenticated client channel closed",
                    )));
                };

                remove_dead_clients(&mut clients);
                stabilize_routes(&mut router, &devices, &mut clients)?;

                let key = clients.insert((authenticated.sender, authenticated.addr));
                tracing::info!(idx = %(key + 1), addr = %authenticated.addr, "Client authenticated");

                let init_updates = devices
                    .iter()
                    .map(|(id, device)| create_device_update(id, device))
                    .collect::<Vec<_>>();

                for update in init_updates {
                    if !try_send_update(&mut clients, key, update) {
                        break;
                    }
                }
                stabilize_routes(&mut router, &devices, &mut clients)?;
            }
            result = monitor.read() => match result.map_err(Error::Input)? {
                MonitorEvent::Remove { activation_id } => {
                    let Some(id) = devices
                        .iter()
                        .find_map(|(id, device)| (device.activation_id == activation_id).then_some(id))
                    else {
                        tracing::debug!(activation_id, "Ignoring removal for stale input activation");
                        continue;
                    };

                    if let Some(released) = destroy_device(
                        &mut devices,
                        &mut clients,
                        &mut router,
                        id,
                    ).await? {
                        monitor
                            .release(released, ReleaseCause::Disconnected)
                            .await
                            .map_err(Error::Input)?;
                    }
                }
                MonitorEvent::Activated(activation) => {
                let activation_id = activation.id();
                let mut interceptor = activation.into_interceptor();

                let name = interceptor.name().to_owned();
                let id = devices.vacant_key();
                let version = interceptor.version();
                let vendor = interceptor.vendor();
                let product = interceptor.product();
                let rel = interceptor.rel().collect::<HashSet<_>>();
                let abs = interceptor.abs().collect::<HashMap<_,_>>();
                let keys = interceptor.key().collect::<HashSet<_>>();
                let pressed_keys = interceptor.pressed_keys();
                let repeat = interceptor.repeat();

                let client_keys = clients.iter().map(|(key, _)| key).collect::<Vec<_>>();
                for key in client_keys {
                    let update = Update::CreateDevice {
                        id,
                        name: name.clone(),
                        version,
                        vendor,
                        product,
                        rel: rel.clone(),
                        abs: abs.clone(),
                        keys: keys.clone(),
                        delay: repeat.delay,
                        period: repeat.period,
                    };

                    try_send_update(&mut clients, key, update);
                }

                let (interceptor_sender, mut interceptor_receiver) = mpsc::channel(32);
                let events_sender = events_sender.clone();
                let device_task = tokio::spawn(async move {
                    loop {
                        tokio::select! {
                            frame = interceptor.read_frame() => {
                                let input_lost = matches!(frame, Ok(Frame::InputLost { .. }));
                                let failed = frame.is_err();
                                if events_sender.send((id, activation_id, frame)).await.is_err() || failed {
                                    break;
                                }
                                if input_lost && !reset_local_device(id, activation_id, &mut interceptor, &mut interceptor_receiver, &events_sender).await {
                                    break;
                                }
                            }
                            command = interceptor_receiver.recv() => {
                                let command = match command {
                                    Some(command) => command,
                                    None => break,
                                };

                                if !handle_device_command(id, activation_id, &mut interceptor, command, &events_sender).await {
                                    break;
                                }
                            }
                        }
                    }
                });

                devices.insert(Device {
                    activation_id,
                    task: Some(device_task),
                    name,
                    version,
                    vendor,
                    product,
                    rel,
                    abs,
                    keys,
                    delay: repeat.delay,
                    period: repeat.period,
                    sender: interceptor_sender,
                });

                let actions = router.add_device(id, pressed_keys, &available_routes(&clients));
                dispatch_actions(actions, &devices, &mut clients)?;
                stabilize_routes(&mut router, &devices, &mut clients)?;

                let device = &devices[id];

                tracing::info!(
                    id = %id,
                    name = ?device.name,
                    vendor = %device.vendor,
                    product = %device.product,
                    version = %device.version,
                    activation_id,
                    "Registered new device"
                );
                }
            },
            (id, event_activation_id, result) = event => {
                if !is_current_device_event(&devices, id, event_activation_id) {
                    tracing::debug!(id, activation_id = event_activation_id, "Ignoring stale input task event");
                    continue;
                }

                match result {
                Ok(Frame::Events(events)) => {
                    if !devices.contains(id) {
                        continue;
                    }

                    let old_route = router.current();
                    let actions = router.process_frame(id, events, &available_routes(&clients));
                    if router.current() != old_route {
                        log_route(&clients, router.current(), "Switched client");
                    }
                    dispatch_actions(actions, &devices, &mut clients)?;
                    stabilize_routes(&mut router, &devices, &mut clients)?;
                }
                Ok(Frame::InputLost { pressed_keys }) => {
                    if !devices.contains(id) {
                        continue;
                    }

                    tracing::warn!(id = %id, "Input device lost synchronization; reconciling physical state");

                    let client_keys = clients.iter().map(|(key, _)| key).collect::<Vec<_>>();
                    for key in client_keys {
                        reset_client_device(&mut clients, key, id, &devices[id]);
                    }

                    let sender = devices[id].sender.clone();
                    if sender.send(DeviceCommand::Reset).await.is_err() {
                        if let Some(released) = destroy_device(
                            &mut devices,
                            &mut clients,
                            &mut router,
                            id,
                        ).await? {
                            monitor
                                .release(released, ReleaseCause::Failed)
                                .await
                                .map_err(Error::Input)?;
                        }
                    } else {
                        let actions = router.reset_device(
                            id,
                            pressed_keys,
                            &available_routes(&clients),
                        );
                        dispatch_actions(actions, &devices, &mut clients)?;
                        stabilize_routes(&mut router, &devices, &mut clients)?;
                    }
                }
                Err(err) => {
                    let cause = if is_device_disconnect(&err) {
                        ReleaseCause::Disconnected
                    } else {
                        tracing::error!(id = %id, error = %err, "Input device failed; other devices remain active");
                        ReleaseCause::Failed
                    };
                    if let Some(released) = destroy_device(
                        &mut devices,
                        &mut clients,
                        &mut router,
                        id,
                    ).await? {
                        monitor
                            .release(released, cause)
                            .await
                            .map_err(Error::Input)?;
                    }
                }
            }
            }
        }
    }
}

enum DeviceCommand {
    Events(Vec<Event>),
    SetKeyState(HashSet<Key>),
    Reset,
}

struct Device {
    activation_id: ActivationId,
    task: Option<JoinHandle<()>>,
    name: CString,
    vendor: u16,
    product: u16,
    version: u16,
    rel: HashSet<RelAxis>,
    abs: HashMap<AbsAxis, AbsInfo>,
    keys: HashSet<Key>,
    delay: Option<i32>,
    period: Option<i32>,
    sender: Sender<DeviceCommand>,
}

struct AuthenticatedClient {
    sender: Sender<Update>,
    addr: SocketAddr,
}

async fn handle_device_command(
    id: usize,
    activation_id: ActivationId,
    interceptor: &mut rkvm_input::interceptor::Interceptor,
    command: DeviceCommand,
    events_sender: &Sender<(usize, ActivationId, Result<Frame, io::Error>)>,
) -> bool {
    let result = match command {
        DeviceCommand::Events(events) => interceptor.write_frame(&events).await,
        DeviceCommand::SetKeyState(pressed_keys) => interceptor.set_key_state(&pressed_keys).await,
        DeviceCommand::Reset => interceptor.reset_writer().await,
    };

    match result {
        Ok(()) => {
            tracing::trace!(id = %id, "Wrote a command to device");
            true
        }
        Err(err) => {
            let _ = events_sender.send((id, activation_id, Err(err))).await;
            false
        }
    }
}

async fn reset_local_device(
    id: usize,
    activation_id: ActivationId,
    interceptor: &mut rkvm_input::interceptor::Interceptor,
    interceptor_receiver: &mut Receiver<DeviceCommand>,
    events_sender: &Sender<(usize, ActivationId, Result<Frame, io::Error>)>,
) -> bool {
    loop {
        let command = match interceptor_receiver.recv().await {
            Some(command) => command,
            None => return false,
        };
        let reset = matches!(command, DeviceCommand::Reset);

        if !handle_device_command(id, activation_id, interceptor, command, events_sender).await {
            return false;
        }
        if reset {
            return true;
        }
    }
}

fn is_current_device_event(devices: &Slab<Device>, id: usize, activation_id: ActivationId) -> bool {
    devices
        .get(id)
        .map_or(false, |device| device.activation_id == activation_id)
}

fn available_routes(clients: &Slab<(Sender<Update>, SocketAddr)>) -> Vec<usize> {
    std::iter::once(0)
        .chain(clients.iter().map(|(key, _)| key + 1))
        .collect()
}

fn create_device_update(id: usize, device: &Device) -> Update {
    Update::CreateDevice {
        id,
        name: device.name.clone(),
        version: device.version,
        vendor: device.vendor,
        product: device.product,
        rel: device.rel.clone(),
        abs: device.abs.clone(),
        keys: device.keys.clone(),
        delay: device.delay,
        period: device.period,
    }
}

fn dispatch_actions(
    actions: Vec<RoutingAction>,
    devices: &Slab<Device>,
    clients: &mut Slab<(Sender<Update>, SocketAddr)>,
) -> Result<(), Error> {
    for action in actions {
        let (route, device_id, local_command, remote_update) = match action {
            RoutingAction::Events {
                route,
                device_id,
                events,
            } if route == 0 => (route, device_id, Some(DeviceCommand::Events(events)), None),
            RoutingAction::Events {
                route,
                device_id,
                events,
            } => (
                route,
                device_id,
                None,
                Some(Update::Events {
                    id: device_id,
                    events,
                }),
            ),
            RoutingAction::SetKeyState {
                route,
                device_id,
                pressed_keys,
            } if route == 0 => (
                route,
                device_id,
                Some(DeviceCommand::SetKeyState(pressed_keys)),
                None,
            ),
            RoutingAction::SetKeyState {
                route,
                device_id,
                pressed_keys,
            } => (
                route,
                device_id,
                None,
                Some(Update::SetKeyState {
                    id: device_id,
                    pressed_keys,
                }),
            ),
        };

        if let Some(command) = local_command {
            let Some(device) = devices.get(device_id) else {
                continue;
            };
            match device.sender.try_send(command) {
                Ok(()) | Err(TrySendError::Closed(_)) => {}
                Err(TrySendError::Full(_)) => return Err(Error::Overflow),
            }
        } else {
            try_send_update(clients, route - 1, remote_update.unwrap());
        }
    }
    Ok(())
}

fn stabilize_routes(
    router: &mut Router,
    devices: &Slab<Device>,
    clients: &mut Slab<(Sender<Update>, SocketAddr)>,
) -> Result<(), Error> {
    loop {
        let old_route = router.current();
        let actions = router.retain_routes(&available_routes(clients));
        if actions.is_empty() {
            return Ok(());
        }
        if router.current() != old_route {
            log_route(clients, router.current(), "Recovered active route");
        }
        dispatch_actions(actions, devices, clients)?;
    }
}

fn log_route(clients: &Slab<(Sender<Update>, SocketAddr)>, route: usize, message: &str) {
    if route == 0 {
        tracing::info!(idx = %route, "{}", message);
    } else if let Some((_, addr)) = clients.get(route - 1) {
        tracing::info!(idx = %route, addr = %addr, "{}", message);
    }
}

async fn destroy_device(
    devices: &mut Slab<Device>,
    clients: &mut Slab<(Sender<Update>, SocketAddr)>,
    router: &mut Router,
    id: usize,
) -> Result<Option<ActivationId>, Error> {
    let Some(device) = devices.try_remove(id) else {
        return Ok(None);
    };

    let client_keys = clients.iter().map(|(key, _)| key).collect::<Vec<_>>();
    for key in client_keys {
        try_send_update(clients, key, Update::DestroyDevice { id });
    }

    // The client lifecycle update must precede releasing the source grab. Stop
    // and await the task next; dropping its future drops the Interceptor and
    // both registry handles before reconciliation is allowed to reacquire it.
    if let Some(task) = device.task {
        task.abort();
        let _ = task.await;
    }

    router.remove_device(id);
    stabilize_routes(router, devices, clients)?;

    tracing::info!(id = %id, activation_id = device.activation_id, "Destroyed device");
    Ok(Some(device.activation_id))
}

fn is_device_disconnect(err: &io::Error) -> bool {
    err.kind() == ErrorKind::BrokenPipe
        || matches!(err.raw_os_error(), Some(libc::ENODEV) | Some(libc::EIO))
}

fn reset_client_device(
    clients: &mut Slab<(Sender<Update>, SocketAddr)>,
    key: usize,
    id: usize,
    device: &Device,
) -> bool {
    try_send_update(clients, key, Update::DestroyDevice { id })
        && try_send_update(clients, key, create_device_update(id, device))
}

fn remove_client(clients: &mut Slab<(Sender<Update>, SocketAddr)>, key: usize, reason: &str) {
    let Some((_, addr)) = clients.try_remove(key) else {
        return;
    };

    tracing::warn!(idx = %(key + 1), addr = %addr, reason = %reason, "Disconnected client");
}

fn remove_dead_clients(clients: &mut Slab<(Sender<Update>, SocketAddr)>) {
    let closed_clients = clients
        .iter()
        .filter_map(|(key, (client, _))| client.is_closed().then_some(key))
        .collect::<Vec<_>>();
    for key in closed_clients {
        remove_client(clients, key, "closed channel");
    }
}

fn try_send_update(
    clients: &mut Slab<(Sender<Update>, SocketAddr)>,
    key: usize,
    update: Update,
) -> bool {
    let result = match clients.get(key) {
        Some((sender, _)) => sender.try_send(update),
        None => return false,
    };

    match result {
        Ok(()) => true,
        Err(TrySendError::Closed(_)) => {
            remove_client(clients, key, "closed channel");
            false
        }
        Err(TrySendError::Full(_)) => {
            remove_client(clients, key, "queue overflow");
            false
        }
    }
}

#[derive(Error, Debug)]
enum ClientError {
    #[error(transparent)]
    Io(#[from] io::Error),
    #[error("Incompatible client version (got {client}, expected {server})")]
    Version { server: Version, client: Version },
    #[error("Invalid password")]
    Auth,
    #[error(transparent)]
    Rand(#[from] rand::Error),
}

type ClientStream = BufStream<tokio_rustls::server::TlsStream<TcpStream>>;

async fn authenticate(
    stream: TcpStream,
    acceptor: TlsAcceptor,
    password: &str,
) -> Result<ClientStream, ClientError> {
    let stream = rkvm_net::timeout(rkvm_net::TLS_TIMEOUT, acceptor.accept(stream)).await?;
    tracing::info!("TLS connected");

    let mut stream = BufStream::with_capacity(1024, 1024, stream);

    rkvm_net::timeout(rkvm_net::WRITE_TIMEOUT, async {
        Version::CURRENT.encode(&mut stream).await?;
        stream.flush().await?;

        Ok(())
    })
    .await?;

    let version = rkvm_net::timeout(rkvm_net::READ_TIMEOUT, Version::decode(&mut stream)).await?;
    if version != Version::CURRENT {
        return Err(ClientError::Version {
            server: Version::CURRENT,
            client: version,
        });
    }

    let challenge = AuthChallenge::generate().await?;

    rkvm_net::timeout(rkvm_net::WRITE_TIMEOUT, async {
        challenge.encode(&mut stream).await?;
        stream.flush().await?;

        Ok(())
    })
    .await?;

    let response =
        rkvm_net::timeout(rkvm_net::READ_TIMEOUT, AuthResponse::decode(&mut stream)).await?;
    let status = match response.verify(&challenge, password) {
        true => AuthStatus::Passed,
        false => AuthStatus::Failed,
    };

    rkvm_net::timeout(rkvm_net::WRITE_TIMEOUT, async {
        status.encode(&mut stream).await?;
        stream.flush().await?;

        Ok(())
    })
    .await?;

    if status == AuthStatus::Failed {
        return Err(ClientError::Auth);
    }

    tracing::info!("Authenticated successfully");

    Ok(stream)
}

async fn client(mut receiver: Receiver<Update>, stream: ClientStream) -> Result<(), ClientError> {
    let (mut read, mut write) = tokio::io::split(stream);
    let (pong_sender, mut pong_receiver) = mpsc::channel(1);
    let pong_handle = tokio::spawn(async move {
        let mut decode_buffer = Vec::new();

        loop {
            let result = Pong::decode_with_buffer(&mut read, &mut decode_buffer)
                .await
                .map(|_| ());
            let failed = result.is_err();
            if pong_sender.send(result).await.is_err() || failed {
                break;
            }
        }
    });

    let result = async {
        let mut interval = time::interval(rkvm_net::PING_INTERVAL);
        interval.set_missed_tick_behavior(time::MissedTickBehavior::Delay);
        interval.tick().await;

        let pong_timeout = time::sleep(rkvm_net::READ_TIMEOUT);
        tokio::pin!(pong_timeout);

        let mut encode_buffer = Vec::new();
        let mut waiting_for_pong = false;

        loop {
            tokio::select! {
                biased;

                result = pong_receiver.recv() => {
                    match result {
                        Some(Ok(())) if waiting_for_pong => {
                            waiting_for_pong = false;
                            tracing::debug!("Received pong");
                        }
                        Some(Ok(())) => {
                            tracing::debug!("Discarding unexpected pong");
                        }
                        Some(Err(err)) => return Err(ClientError::Io(err)),
                        None => return Err(ClientError::Io(io::Error::new(
                            ErrorKind::BrokenPipe,
                            "Pong reader closed",
                        ))),
                    }
                }
                _ = &mut pong_timeout, if waiting_for_pong => {
                    return Err(ClientError::Io(io::Error::new(
                        ErrorKind::TimedOut,
                        "Pong timed out",
                    )));
                }
                recv = receiver.recv() => {
                    let update = match recv {
                        Some(update) => update,
                        None => break,
                    };

                    let start = Instant::now();
                    rkvm_net::timeout(rkvm_net::WRITE_TIMEOUT, async {
                        update.encode_with_buffer(&mut write, &mut encode_buffer).await?;
                        write.flush().await?;

                        Ok(())
                    })
                    .await?;
                    let duration = start.elapsed();

                    interval.reset();
                    tracing::trace!(duration = ?duration, "Wrote an update");
                }
                _ = interval.tick(), if !waiting_for_pong => {
                    let start = Instant::now();
                    rkvm_net::timeout(rkvm_net::WRITE_TIMEOUT, async {
                        Update::Ping.encode_with_buffer(&mut write, &mut encode_buffer).await?;
                        write.flush().await?;

                        Ok(())
                    })
                    .await?;
                    let duration = start.elapsed();

                    waiting_for_pong = true;
                    pong_timeout
                        .as_mut()
                        .reset(time::Instant::now() + rkvm_net::READ_TIMEOUT);
                    tracing::debug!(duration = ?duration, "Sent ping");
                }
            }
        }

        Ok(())
    }
    .await;

    pong_handle.abort();
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    fn client_entry() -> (Sender<Update>, SocketAddr) {
        let (sender, _) = mpsc::channel(1);
        (sender, "127.0.0.1:1234".parse().unwrap())
    }

    fn test_device() -> Device {
        Device {
            activation_id: 0,
            task: None,
            name: CString::new("test").unwrap(),
            vendor: 1,
            product: 2,
            version: 3,
            rel: HashSet::new(),
            abs: HashMap::new(),
            keys: HashSet::new(),
            delay: None,
            period: None,
            sender: mpsc::channel(1).0,
        }
    }

    #[test]
    fn stale_task_event_does_not_target_reused_device_id() {
        let mut devices = Slab::new();
        let mut first = test_device();
        first.activation_id = 10;
        let id = devices.insert(first);
        assert!(is_current_device_event(&devices, id, 10));

        devices.remove(id);
        let mut replacement = test_device();
        replacement.activation_id = 11;
        assert_eq!(devices.insert(replacement), id);

        assert!(!is_current_device_event(&devices, id, 10));
        assert!(is_current_device_event(&devices, id, 11));
    }

    #[test]
    fn available_routes_preserve_sparse_client_ids() {
        let mut clients = Slab::new();
        let first = clients.insert(client_entry());
        let second = clients.insert(client_entry());
        clients.remove(first);

        assert_eq!(available_routes(&clients), vec![0, second + 1]);
    }

    #[test]
    fn local_reset_command_is_ordered_after_queued_frames() {
        let (sender, mut receiver) = mpsc::channel(2);

        sender.try_send(DeviceCommand::Events(Vec::new())).unwrap();
        sender.try_send(DeviceCommand::Reset).unwrap();

        assert!(matches!(
            receiver.try_recv().unwrap(),
            DeviceCommand::Events(events) if events.is_empty()
        ));
        assert!(matches!(receiver.try_recv().unwrap(), DeviceCommand::Reset));
    }

    #[test]
    fn reset_client_device_disconnects_on_partial_barrier_enqueue() {
        let mut clients = Slab::new();
        let (sender, mut receiver) = mpsc::channel(1);
        let key = clients.insert((sender, "127.0.0.1:1234".parse().unwrap()));
        let device = test_device();

        assert!(!reset_client_device(&mut clients, key, 7, &device));
        assert!(!clients.contains(key));
        assert!(matches!(
            receiver.try_recv().unwrap(),
            Update::DestroyDevice { id: 7 }
        ));
    }

    #[test]
    fn dispatches_local_reconciliation_as_one_command() {
        let (sender, mut receiver) = mpsc::channel(1);
        let mut devices = Slab::new();
        let mut device = test_device();
        device.sender = sender;
        let id = devices.insert(device);
        let ctrl = Key::Key(rkvm_input::key::Keyboard::LeftCtrl);

        dispatch_actions(
            vec![RoutingAction::SetKeyState {
                route: 0,
                device_id: id,
                pressed_keys: [ctrl].into_iter().collect(),
            }],
            &devices,
            &mut Slab::new(),
        )
        .unwrap();

        assert!(matches!(
            receiver.try_recv().unwrap(),
            DeviceCommand::SetKeyState(keys) if keys == [ctrl].into_iter().collect()
        ));
    }
}
