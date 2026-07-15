use crate::config::DeviceMatch;
use rkvm_input::abs::{AbsAxis, AbsInfo};
use rkvm_input::event::Event;
use rkvm_input::interceptor::Frame;
use rkvm_input::key::{Key, KeyEvent};
use rkvm_input::monitor::{ActivationId, Monitor, MonitorEvent, ReleaseCause};
use rkvm_input::rel::RelAxis;
use rkvm_input::sync::SyncEvent;
use rkvm_net::auth::{AuthChallenge, AuthResponse, AuthStatus};
use rkvm_net::message::Message;
use rkvm_net::version::Version;
use rkvm_net::{Pong, Update};
use slab::Slab;
use std::collections::{HashMap, HashSet};
use std::ffi::CString;
use std::io::{self, ErrorKind};
use std::net::SocketAddr;
use std::time::Instant;
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

#[derive(Clone, Debug)]
pub(crate) struct SwitchBinding {
    keys: HashSet<Key>,
    trigger: Key,
}

impl SwitchBinding {
    pub(crate) fn new(keys: HashSet<Key>, trigger: Key) -> Self {
        Self { keys, trigger }
    }
}

pub async fn run(
    listen: SocketAddr,
    acceptor: TlsAcceptor,
    password: &str,
    switch_bindings: &[SwitchBinding],
    propagate_switch_keys: bool,
    device_whitelist: Option<Vec<DeviceMatch>>,
    client_queue_size: usize,
) -> Result<(), Error> {
    let listener = TcpListener::bind(&listen).await.map_err(Error::Network)?;
    tracing::info!("Listening on {}", listen);

    let mut monitor = match device_whitelist {
        Some(device_whitelist) => Monitor::with_filter(move |device| {
            device_whitelist.iter().any(|item| item.matches(device))
        }),
        None => Monitor::new(),
    };
    let mut devices = Slab::<Device>::new();
    let mut clients = Slab::<(Sender<_>, SocketAddr)>::new();
    let mut current = 0;
    let mut previous = 0;
    let mut active_binding: Option<HashSet<Key>> = None;
    let mut pressed_keys = HashMap::<usize, HashSet<Key>>::new();
    let switch_keys = switch_key_set(switch_bindings);
    let trigger_bindings = trigger_bindings(switch_bindings);

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

                remove_dead_clients(&mut clients, &mut current, &mut previous);

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

                remove_dead_clients(&mut clients, &mut current, &mut previous);

                let key = clients.insert((authenticated.sender, authenticated.addr));
                tracing::info!(idx = %(key + 1), addr = %authenticated.addr, "Client authenticated");

                let init_updates = devices
                    .iter()
                    .map(|(id, device)| create_device_update(id, device))
                    .collect::<Vec<_>>();

                for update in init_updates {
                    if !try_send_update(
                        &mut clients,
                        key,
                        update,
                        &mut current,
                        &mut previous,
                    ) {
                        break;
                    }
                }
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
                        id,
                        &mut current,
                        &mut previous,
                        &mut active_binding,
                        &mut pressed_keys,
                    ).await {
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

                    try_send_update(
                        &mut clients,
                        key,
                        update,
                        &mut current,
                        &mut previous,
                    );
                }

                let (interceptor_sender, mut interceptor_receiver) = mpsc::channel(32);
                let events_sender = events_sender.clone();
                let device_task = tokio::spawn(async move {
                    loop {
                        tokio::select! {
                            frame = interceptor.read_frame() => {
                                let input_lost = matches!(frame, Ok(Frame::InputLost));
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
                    let mut routed = Vec::new();

                    for event in events {
                        let mut switch_key = false;

                        let key_event = match &event {
                            Event::Key(KeyEvent { key, down }) => Some((*key, *down)),
                            _ => None,
                        };

                        if let Some((key, down)) = key_event {
                            if switch_keys.contains(&key) {
                                switch_key = true;

                                let device_keys = pressed_keys.entry(id).or_default();
                                match down {
                                    true => device_keys.insert(key),
                                    false => device_keys.remove(&key),
                                };
                                if device_keys.is_empty() {
                                    pressed_keys.remove(&id);
                                }
                            }
                        }

                        // Who to send this event to.
                        let mut idx = current;

                        if let Some((key, down)) = key_event.filter(|(key, _)| switch_keys.contains(key)) {
                            if let Some(binding) = &active_binding {
                                if binding.contains(&key) {
                                    idx = previous;
                                }
                            } else if down {
                                let pressed_key_union = pressed_key_union(&pressed_keys);
                                if let Some(binding) = matching_binding(
                                    switch_bindings,
                                    &trigger_bindings,
                                    &pressed_key_union,
                                    key,
                                ) {
                                    current = next_route(&clients, current);

                                    previous = idx;
                                    active_binding = Some(binding.keys.clone());

                                    if current != 0 {
                                        tracing::info!(idx = %current, addr = %clients[current - 1].1, "Switched client");
                                    } else {
                                        tracing::info!(idx = %current, "Switched client");
                                    }
                                }
                            }

                            let pressed_key_union = pressed_key_union(&pressed_keys);
                            if active_binding
                                .as_ref()
                                .map_or(false, |binding| binding.is_disjoint(&pressed_key_union))
                            {
                                active_binding = None;
                            }
                        }

                        if switch_key && !propagate_switch_keys {
                            continue;
                        }

                        routed.push((idx, event));
                        if switch_key {
                            routed.push((idx, Event::Sync(SyncEvent::All)));
                        }
                    }

                    let mut routed = routed.into_iter().peekable();
                    while let Some((idx, event)) = routed.next() {
                        let mut events = vec![event];

                        while matches!(routed.peek(), Some((next_idx, _)) if *next_idx == idx) {
                            let (_, event) = routed.next().unwrap();
                            events.push(event);
                        }

                        // Index 0 - special case to keep the modular arithmetic above working.
                        if idx == 0 {
                            // We do a try_send() here rather than a "blocking" send in order to prevent deadlocks.
                            // In this scenario, the interceptor task is sending events to the main task,
                            // while the main task is simultaneously sending events back to the interceptor.
                            // This creates a classic deadlock situation where both tasks are waiting for each other.
                            for event in events {
                                match devices[id].sender.try_send(DeviceCommand::Event(event)) {
                                    Ok(()) | Err(TrySendError::Closed(_)) => {},
                                    Err(TrySendError::Full(_)) => return Err(Error::Overflow),
                                }
                            }

                            continue;
                        }

                        if !route_exists(&clients, idx) {
                            if current == idx {
                                current = 0;
                            }
                            continue;
                        }

                        try_send_update(
                            &mut clients,
                            idx - 1,
                            Update::Events { id, events },
                            &mut current,
                            &mut previous,
                        );
                    }
                }
                Ok(Frame::InputLost) => {
                    if !devices.contains(id) {
                        continue;
                    }

                    tracing::warn!(id = %id, "Input device lost synchronization; resetting routed state");
                    reset_routing_state(&mut current, &mut previous, &mut active_binding, &mut pressed_keys);

                    let client_keys = clients.iter().map(|(key, _)| key).collect::<Vec<_>>();
                    for key in client_keys {
                        reset_client_device(
                            &mut clients,
                            key,
                            id,
                            &devices[id],
                            &mut current,
                            &mut previous,
                        );
                    }

                    let sender = devices[id].sender.clone();
                    if sender.send(DeviceCommand::Reset).await.is_err() {
                        if let Some(released) = destroy_device(
                            &mut devices,
                            &mut clients,
                            id,
                            &mut current,
                            &mut previous,
                            &mut active_binding,
                            &mut pressed_keys,
                        ).await {
                            monitor
                                .release(released, ReleaseCause::Failed)
                                .await
                                .map_err(Error::Input)?;
                        }
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
                        id,
                        &mut current,
                        &mut previous,
                        &mut active_binding,
                        &mut pressed_keys,
                    ).await {
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
    Event(Event),
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
        DeviceCommand::Event(event) => interceptor.write(&event).await,
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

fn route_exists(clients: &Slab<(Sender<Update>, SocketAddr)>, idx: usize) -> bool {
    idx == 0 || clients.contains(idx - 1)
}

fn next_route(clients: &Slab<(Sender<Update>, SocketAddr)>, current: usize) -> usize {
    clients
        .iter()
        .map(|(key, _)| key + 1)
        .find(|idx| *idx > current)
        .unwrap_or(0)
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

fn reset_routing_state(
    current: &mut usize,
    previous: &mut usize,
    active_binding: &mut Option<HashSet<Key>>,
    pressed_keys: &mut HashMap<usize, HashSet<Key>>,
) {
    *current = 0;
    *previous = 0;
    *active_binding = None;
    pressed_keys.clear();
}

async fn destroy_device(
    devices: &mut Slab<Device>,
    clients: &mut Slab<(Sender<Update>, SocketAddr)>,
    id: usize,
    current: &mut usize,
    previous: &mut usize,
    active_binding: &mut Option<HashSet<Key>>,
    pressed_keys: &mut HashMap<usize, HashSet<Key>>,
) -> Option<ActivationId> {
    let device = devices.try_remove(id)?;

    let client_keys = clients.iter().map(|(key, _)| key).collect::<Vec<_>>();
    for key in client_keys {
        try_send_update(
            clients,
            key,
            Update::DestroyDevice { id },
            current,
            previous,
        );
    }

    // The client lifecycle update must precede releasing the source grab. Stop
    // and await the task next; dropping its future drops the Interceptor and
    // both registry handles before reconciliation is allowed to reacquire it.
    if let Some(task) = device.task {
        task.abort();
        let _ = task.await;
    }

    pressed_keys.remove(&id);
    let pressed_key_union = pressed_key_union(pressed_keys);
    if active_binding
        .as_ref()
        .map_or(false, |binding| binding.is_disjoint(&pressed_key_union))
    {
        *active_binding = None;
    }

    tracing::info!(id = %id, activation_id = device.activation_id, "Destroyed device");
    Some(device.activation_id)
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
    current: &mut usize,
    previous: &mut usize,
) -> bool {
    try_send_update(
        clients,
        key,
        Update::DestroyDevice { id },
        current,
        previous,
    ) && try_send_update(
        clients,
        key,
        create_device_update(id, device),
        current,
        previous,
    )
}

fn pressed_key_union(pressed_keys: &HashMap<usize, HashSet<Key>>) -> HashSet<Key> {
    pressed_keys
        .values()
        .flat_map(|keys| keys.iter())
        .copied()
        .collect()
}

fn switch_key_set(bindings: &[SwitchBinding]) -> HashSet<Key> {
    bindings
        .iter()
        .flat_map(|binding| binding.keys.iter())
        .copied()
        .collect()
}

fn trigger_bindings(bindings: &[SwitchBinding]) -> HashMap<Key, Vec<usize>> {
    let mut triggers = HashMap::<Key, Vec<usize>>::new();
    for (idx, binding) in bindings.iter().enumerate() {
        triggers.entry(binding.trigger).or_default().push(idx);
    }
    triggers
}

fn matching_binding<'a>(
    bindings: &'a [SwitchBinding],
    trigger_bindings: &HashMap<Key, Vec<usize>>,
    pressed_keys: &HashSet<Key>,
    key: Key,
) -> Option<&'a SwitchBinding> {
    trigger_bindings.get(&key)?.iter().find_map(|idx| {
        let binding = &bindings[*idx];
        binding.keys.is_subset(pressed_keys).then_some(binding)
    })
}

fn remove_client(
    clients: &mut Slab<(Sender<Update>, SocketAddr)>,
    key: usize,
    current: &mut usize,
    previous: &mut usize,
    reason: &str,
) {
    let Some((_, addr)) = clients.try_remove(key) else {
        return;
    };

    let idx = key + 1;
    if *current == idx {
        *current = 0;
    }
    if *previous == idx {
        *previous = 0;
    }

    tracing::warn!(idx = %idx, addr = %addr, reason = %reason, "Disconnected client");
}

fn remove_dead_clients(
    clients: &mut Slab<(Sender<Update>, SocketAddr)>,
    current: &mut usize,
    previous: &mut usize,
) {
    let closed_clients = clients
        .iter()
        .filter_map(|(key, (client, _))| client.is_closed().then_some(key))
        .collect::<Vec<_>>();
    for key in closed_clients {
        remove_client(clients, key, current, previous, "closed channel");
    }
    if !route_exists(clients, *current) {
        *current = 0;
    }
}

fn try_send_update(
    clients: &mut Slab<(Sender<Update>, SocketAddr)>,
    key: usize,
    update: Update,
    current: &mut usize,
    previous: &mut usize,
) -> bool {
    let result = match clients.get(key) {
        Some((sender, _)) => sender.try_send(update),
        None => return false,
    };

    match result {
        Ok(()) => true,
        Err(TrySendError::Closed(_)) => {
            remove_client(clients, key, current, previous, "closed channel");
            false
        }
        Err(TrySendError::Full(_)) => {
            remove_client(clients, key, current, previous, "queue overflow");
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
    fn next_route_handles_sparse_client_keys() {
        let mut clients = Slab::new();
        let first = clients.insert(client_entry());
        let second = clients.insert(client_entry());
        clients.remove(first);

        assert_eq!(next_route(&clients, 0), second + 1);
        assert_eq!(next_route(&clients, second + 1), 0);
    }

    #[test]
    fn remove_client_resets_active_routes() {
        let mut clients = Slab::new();
        let key = clients.insert(client_entry());
        let mut current = key + 1;
        let mut previous = key + 1;

        remove_client(&mut clients, key, &mut current, &mut previous, "test");

        assert_eq!(current, 0);
        assert_eq!(previous, 0);
        assert!(!route_exists(&clients, key + 1));
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
    fn reset_routing_state_routes_local_and_clears_switch_state() {
        let mut current = 2;
        let mut previous = 1;
        let mut active_binding = Some(
            [Key::Key(rkvm_input::key::Keyboard::LeftCtrl)]
                .into_iter()
                .collect(),
        );
        let mut pressed_keys = HashMap::from([(
            7,
            [Key::Key(rkvm_input::key::Keyboard::LeftCtrl)]
                .into_iter()
                .collect(),
        )]);

        reset_routing_state(
            &mut current,
            &mut previous,
            &mut active_binding,
            &mut pressed_keys,
        );

        assert_eq!(current, 0);
        assert_eq!(previous, 0);
        assert!(active_binding.is_none());
        assert!(pressed_keys.is_empty());
    }

    #[tokio::test]
    async fn destroying_device_clears_only_its_switch_state() {
        let ctrl = Key::Key(rkvm_input::key::Keyboard::LeftCtrl);
        let alt = Key::Key(rkvm_input::key::Keyboard::LeftAlt);
        let mut devices = Slab::new();
        let removed = devices.insert(test_device());
        let remaining = devices.insert(test_device());
        let mut clients = Slab::new();
        let mut current = 2;
        let mut previous = 1;
        let mut active_binding = Some([ctrl].into_iter().collect());
        let mut pressed_keys = HashMap::from([
            (removed, [ctrl].into_iter().collect()),
            (remaining, [alt].into_iter().collect()),
        ]);

        destroy_device(
            &mut devices,
            &mut clients,
            removed,
            &mut current,
            &mut previous,
            &mut active_binding,
            &mut pressed_keys,
        )
        .await;

        assert!(!devices.contains(removed));
        assert!(devices.contains(remaining));
        assert!(!pressed_keys.contains_key(&removed));
        assert_eq!(
            pressed_key_union(&pressed_keys),
            [alt].into_iter().collect()
        );
        assert!(active_binding.is_none());
        assert_eq!(current, 2);
        assert_eq!(previous, 1);
    }

    #[tokio::test]
    async fn destroying_device_preserves_binding_held_on_another_device() {
        let ctrl = Key::Key(rkvm_input::key::Keyboard::LeftCtrl);
        let mut devices = Slab::new();
        let removed = devices.insert(test_device());
        let remaining = devices.insert(test_device());
        let mut clients = Slab::new();
        let mut current = 0;
        let mut previous = 0;
        let mut active_binding = Some([ctrl].into_iter().collect());
        let mut pressed_keys = HashMap::from([
            (removed, [ctrl].into_iter().collect()),
            (remaining, [ctrl].into_iter().collect()),
        ]);

        destroy_device(
            &mut devices,
            &mut clients,
            removed,
            &mut current,
            &mut previous,
            &mut active_binding,
            &mut pressed_keys,
        )
        .await;

        assert!(active_binding.is_some());
        assert_eq!(
            pressed_key_union(&pressed_keys),
            [ctrl].into_iter().collect()
        );
    }

    #[test]
    fn local_reset_command_is_ordered_after_queued_events() {
        let (sender, mut receiver) = mpsc::channel(2);

        sender
            .try_send(DeviceCommand::Event(Event::Sync(SyncEvent::All)))
            .unwrap();
        sender.try_send(DeviceCommand::Reset).unwrap();

        assert!(matches!(
            receiver.try_recv().unwrap(),
            DeviceCommand::Event(Event::Sync(SyncEvent::All))
        ));
        assert!(matches!(receiver.try_recv().unwrap(), DeviceCommand::Reset));
    }

    #[test]
    fn reset_client_device_disconnects_on_partial_barrier_enqueue() {
        let mut clients = Slab::new();
        let (sender, mut receiver) = mpsc::channel(1);
        let key = clients.insert((sender, "127.0.0.1:1234".parse().unwrap()));
        let mut current = key + 1;
        let mut previous = key + 1;
        let device = test_device();

        assert!(!reset_client_device(
            &mut clients,
            key,
            7,
            &device,
            &mut current,
            &mut previous,
        ));
        assert!(!clients.contains(key));
        assert_eq!(current, 0);
        assert_eq!(previous, 0);

        assert!(matches!(
            receiver.try_recv().unwrap(),
            Update::DestroyDevice { id: 7 }
        ));
    }

    fn key(key: rkvm_input::key::Keyboard) -> Key {
        Key::Key(key)
    }

    fn switch_binding(keys: &[Key], trigger: Key) -> SwitchBinding {
        SwitchBinding::new(keys.iter().copied().collect(), trigger)
    }

    #[test]
    fn switch_key_set_contains_union_of_bindings() {
        let bindings = vec![
            switch_binding(
                &[
                    key(rkvm_input::key::Keyboard::LeftCtrl),
                    key(rkvm_input::key::Keyboard::Space),
                ],
                key(rkvm_input::key::Keyboard::Space),
            ),
            switch_binding(
                &[key(rkvm_input::key::Keyboard::LeftAlt)],
                key(rkvm_input::key::Keyboard::LeftAlt),
            ),
        ];

        let keys = switch_key_set(&bindings);

        assert!(keys.contains(&key(rkvm_input::key::Keyboard::LeftCtrl)));
        assert!(keys.contains(&key(rkvm_input::key::Keyboard::Space)));
        assert!(keys.contains(&key(rkvm_input::key::Keyboard::LeftAlt)));
        assert!(!keys.contains(&key(rkvm_input::key::Keyboard::A)));
    }

    #[test]
    fn matching_binding_requires_complete_chord_and_trigger_key() {
        let bindings = vec![switch_binding(
            &[
                key(rkvm_input::key::Keyboard::LeftCtrl),
                key(rkvm_input::key::Keyboard::Space),
            ],
            key(rkvm_input::key::Keyboard::Space),
        )];
        let triggers = trigger_bindings(&bindings);
        let partial = [key(rkvm_input::key::Keyboard::LeftCtrl)]
            .into_iter()
            .collect::<HashSet<_>>();
        let complete = [
            key(rkvm_input::key::Keyboard::LeftCtrl),
            key(rkvm_input::key::Keyboard::Space),
        ]
        .into_iter()
        .collect::<HashSet<_>>();

        assert!(matching_binding(
            &bindings,
            &triggers,
            &partial,
            key(rkvm_input::key::Keyboard::Space),
        )
        .is_none());
        assert!(matching_binding(
            &bindings,
            &triggers,
            &complete,
            key(rkvm_input::key::Keyboard::LeftCtrl),
        )
        .is_none());
        assert!(matching_binding(
            &bindings,
            &triggers,
            &complete,
            key(rkvm_input::key::Keyboard::Space),
        )
        .is_some());
    }

    #[test]
    fn matching_binding_uses_config_order_for_shared_triggers() {
        let bindings = vec![
            switch_binding(
                &[
                    key(rkvm_input::key::Keyboard::LeftCtrl),
                    key(rkvm_input::key::Keyboard::Space),
                ],
                key(rkvm_input::key::Keyboard::Space),
            ),
            switch_binding(
                &[
                    key(rkvm_input::key::Keyboard::LeftAlt),
                    key(rkvm_input::key::Keyboard::Space),
                ],
                key(rkvm_input::key::Keyboard::Space),
            ),
        ];
        let triggers = trigger_bindings(&bindings);
        let pressed = [
            key(rkvm_input::key::Keyboard::LeftCtrl),
            key(rkvm_input::key::Keyboard::LeftAlt),
            key(rkvm_input::key::Keyboard::Space),
        ]
        .into_iter()
        .collect::<HashSet<_>>();

        let matched = matching_binding(
            &bindings,
            &triggers,
            &pressed,
            key(rkvm_input::key::Keyboard::Space),
        )
        .unwrap();

        assert_eq!(matched.trigger, key(rkvm_input::key::Keyboard::Space));
        assert!(matched
            .keys
            .contains(&key(rkvm_input::key::Keyboard::LeftCtrl)));
    }
}
