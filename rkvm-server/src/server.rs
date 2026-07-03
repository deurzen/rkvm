use crate::config::DeviceMatch;
use rkvm_input::abs::{AbsAxis, AbsInfo};
use rkvm_input::event::Event;
use rkvm_input::key::{Key, KeyEvent};
use rkvm_input::monitor::Monitor;
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
    switch_bindings: &[HashSet<Key>],
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
    let mut pressed_keys = HashSet::new();

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
                    .map(|(id, device)| Update::CreateDevice {
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
                    })
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
            result = monitor.read() => {
                let mut interceptor = result.map_err(Error::Input)?;

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
                devices.insert(Device {
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

                let events_sender = events_sender.clone();
                tokio::spawn(async move {
                    loop {
                        tokio::select! {
                            events = interceptor.read_frame() => {
                                if events.is_err() | events_sender.send((id, events)).await.is_err() {
                                    break;
                                }
                            }
                            event = interceptor_receiver.recv() => {
                                let event = match event {
                                    Some(event) => event,
                                    None => break,
                                };

                                match interceptor.write(&event).await {
                                    Ok(()) => {},
                                    Err(err) => {
                                        let _ = events_sender.send((id, Err(err))).await;
                                        break;
                                    }
                                }

                                tracing::trace!(id = %id, "Wrote an event to device");
                            }
                        }
                    }
                });

                let device = &devices[id];

                tracing::info!(
                    id = %id,
                    name = ?device.name,
                    vendor = %device.vendor,
                    product = %device.product,
                    version = %device.version,
                    "Registered new device"
                );
            }
            (id, result) = event => match result {
                Ok(events) => {
                    let mut routed = Vec::new();

                    for event in events {
                        let mut switch_key = false;

                        let key_event = match &event {
                            Event::Key(KeyEvent { key, down }) => Some((*key, *down)),
                            _ => None,
                        };

                        if let Some((key, down)) = key_event {
                            if binding_contains_key(switch_bindings, key) {
                                switch_key = true;

                                match down {
                                    true => pressed_keys.insert(key),
                                    false => pressed_keys.remove(&key),
                                };
                            }
                        }

                        // Who to send this event to.
                        let mut idx = current;

                        if let Some((key, down)) = key_event.filter(|(key, _)| {
                            binding_contains_key(switch_bindings, *key)
                        }) {
                            if let Some(binding) = &active_binding {
                                if binding.contains(&key) {
                                    idx = previous;
                                }
                            } else if down {
                                if let Some(binding) = matching_binding(switch_bindings, &pressed_keys, key) {
                                    current = next_route(&clients, current);

                                    previous = idx;
                                    active_binding = Some(binding.clone());

                                    if current != 0 {
                                        tracing::info!(idx = %current, addr = %clients[current - 1].1, "Switched client");
                                    } else {
                                        tracing::info!(idx = %current, "Switched client");
                                    }
                                }
                            }

                            if active_binding
                                .as_ref()
                                .map_or(false, |binding| binding.is_disjoint(&pressed_keys))
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
                                match devices[id].sender.try_send(event) {
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
                Err(err) if err.kind() == ErrorKind::BrokenPipe => {
                    let client_keys = clients.iter().map(|(key, _)| key).collect::<Vec<_>>();
                    for key in client_keys {
                        try_send_update(
                            &mut clients,
                            key,
                            Update::DestroyDevice { id },
                            &mut current,
                            &mut previous,
                        );
                    }
                    devices.remove(id);

                    tracing::info!(id = %id, "Destroyed device");
                }
                Err(err) => return Err(Error::Input(err)),
            }
        }
    }
}

struct Device {
    name: CString,
    vendor: u16,
    product: u16,
    version: u16,
    rel: HashSet<RelAxis>,
    abs: HashMap<AbsAxis, AbsInfo>,
    keys: HashSet<Key>,
    delay: Option<i32>,
    period: Option<i32>,
    sender: Sender<Event>,
}

struct AuthenticatedClient {
    sender: Sender<Update>,
    addr: SocketAddr,
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

fn binding_contains_key(bindings: &[HashSet<Key>], key: Key) -> bool {
    bindings.iter().any(|binding| binding.contains(&key))
}

fn matching_binding<'a>(
    bindings: &'a [HashSet<Key>],
    pressed_keys: &HashSet<Key>,
    key: Key,
) -> Option<&'a HashSet<Key>> {
    bindings
        .iter()
        .find(|binding| binding.contains(&key) && binding.is_subset(pressed_keys))
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

async fn client(
    mut receiver: Receiver<Update>,
    mut stream: ClientStream,
) -> Result<(), ClientError> {
    let mut interval = time::interval(rkvm_net::PING_INTERVAL);
    let mut decode_buffer = Vec::new();

    loop {
        let update = tokio::select! {
            // Make sure pings have priority.
            // The client could time out otherwise.
            biased;

            _ = interval.tick() => Some(Update::Ping),
            recv = receiver.recv() => recv,
        };

        let update = match update {
            Some(update) => update,
            None => break,
        };

        let start = Instant::now();
        rkvm_net::timeout(rkvm_net::WRITE_TIMEOUT, async {
            update.encode(&mut stream).await?;
            stream.flush().await?;

            Ok(())
        })
        .await?;
        let duration = start.elapsed();

        if let Update::Ping = update {
            // Keeping these as debug because it's not as frequent as other updates.
            tracing::debug!(duration = ?duration, "Sent ping");

            let start = Instant::now();
            rkvm_net::timeout(
                rkvm_net::READ_TIMEOUT,
                Pong::decode_with_buffer(&mut stream, &mut decode_buffer),
            )
            .await?;
            let duration = start.elapsed();

            tracing::debug!(duration = ?duration, "Received pong");
        }

        tracing::trace!("Wrote an update");
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn client_entry() -> (Sender<Update>, SocketAddr) {
        let (sender, _) = mpsc::channel(1);
        (sender, "127.0.0.1:1234".parse().unwrap())
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

    #[test]
    fn matching_binding_requires_complete_chord() {
        let binding = [
            Key::Key(rkvm_input::key::Keyboard::LeftCtrl),
            Key::Key(rkvm_input::key::Keyboard::Space),
        ]
        .into_iter()
        .collect::<HashSet<_>>();
        let bindings = vec![binding];
        let partial = [Key::Key(rkvm_input::key::Keyboard::LeftCtrl)]
            .into_iter()
            .collect::<HashSet<_>>();
        let complete = [
            Key::Key(rkvm_input::key::Keyboard::LeftCtrl),
            Key::Key(rkvm_input::key::Keyboard::Space),
        ]
        .into_iter()
        .collect::<HashSet<_>>();

        assert!(matching_binding(
            &bindings,
            &partial,
            Key::Key(rkvm_input::key::Keyboard::LeftCtrl)
        )
        .is_none());
        assert!(matching_binding(
            &bindings,
            &complete,
            Key::Key(rkvm_input::key::Keyboard::Space)
        )
        .is_some());
    }
}
