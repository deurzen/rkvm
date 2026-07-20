use rkvm_input::event::Event;
use rkvm_input::key::{Key, KeyEvent};
use rkvm_input::sync::SyncEvent;
use std::collections::{HashMap, HashSet};

pub(crate) type Route = usize;

#[derive(Clone, Debug)]
pub(crate) struct SwitchBinding {
    pub(crate) keys: HashSet<Key>,
    pub(crate) trigger: Key,
}

impl SwitchBinding {
    pub(crate) fn new(keys: HashSet<Key>, trigger: Key) -> Self {
        Self { keys, trigger }
    }
}

#[derive(Debug)]
pub(crate) enum Action {
    Events {
        route: Route,
        device_id: usize,
        events: Vec<Event>,
    },
    SetKeyState {
        route: Route,
        device_id: usize,
        pressed_keys: HashSet<Key>,
    },
}

pub(crate) struct Router {
    current: Route,
    bindings: Vec<SwitchBinding>,
    trigger_bindings: HashMap<Key, Vec<usize>>,
    switch_keys: HashSet<Key>,
    trigger_keys: HashSet<Key>,
    propagate_switch_keys: bool,
    active_binding: Option<usize>,
    physical_keys: HashMap<usize, HashSet<Key>>,
    blocked_keys: HashMap<usize, HashSet<Key>>,
}

impl Router {
    pub(crate) fn new(bindings: &[SwitchBinding], propagate_switch_keys: bool) -> Self {
        let mut trigger_bindings = HashMap::<Key, Vec<usize>>::new();
        for (index, binding) in bindings.iter().enumerate() {
            trigger_bindings
                .entry(binding.trigger)
                .or_default()
                .push(index);
        }

        Self {
            current: 0,
            bindings: bindings.to_vec(),
            trigger_bindings,
            switch_keys: bindings
                .iter()
                .flat_map(|binding| binding.keys.iter())
                .copied()
                .collect(),
            trigger_keys: bindings.iter().map(|binding| binding.trigger).collect(),
            propagate_switch_keys,
            active_binding: None,
            physical_keys: HashMap::new(),
            blocked_keys: HashMap::new(),
        }
    }

    pub(crate) fn current(&self) -> Route {
        self.current
    }

    pub(crate) fn add_device(
        &mut self,
        device_id: usize,
        pressed_keys: HashSet<Key>,
        routes: &[Route],
    ) -> Vec<Action> {
        let blocked = pressed_keys
            .intersection(&self.trigger_keys)
            .copied()
            .collect::<HashSet<_>>();
        self.physical_keys.insert(device_id, pressed_keys);
        if blocked.is_empty() {
            self.blocked_keys.remove(&device_id);
        } else {
            self.blocked_keys.insert(device_id, blocked);
        }

        self.reconcile_device(device_id, routes, ReconcileMode::Recovery)
    }

    pub(crate) fn reset_device(
        &mut self,
        device_id: usize,
        pressed_keys: HashSet<Key>,
        routes: &[Route],
    ) -> Vec<Action> {
        self.active_binding = None;
        let actions = self.add_device(device_id, pressed_keys, routes);
        let pressed = self.pressed_key_union();
        self.active_binding = self
            .bindings
            .iter()
            .position(|binding| binding.keys.is_subset(&pressed));
        actions
    }

    pub(crate) fn remove_device(&mut self, device_id: usize) {
        self.physical_keys.remove(&device_id);
        self.blocked_keys.remove(&device_id);
        self.clear_inactive_binding();
    }

    pub(crate) fn process_frame(
        &mut self,
        device_id: usize,
        events: Vec<Event>,
        routes: &[Route],
    ) -> Vec<Action> {
        self.physical_keys.entry(device_id).or_default();

        let mut actions = Vec::new();
        let mut pending = Vec::<(Route, Event)>::new();

        for event in events {
            if matches!(event, Event::Sync(SyncEvent::All)) {
                flush_events(device_id, &mut pending, &mut actions);
                continue;
            }

            let key_event = match &event {
                Event::Key(event) => Some(*event),
                _ => None,
            };

            let Some(KeyEvent { key, down }) = key_event else {
                pending.push((self.current, event));
                continue;
            };

            let was_blocked = self
                .blocked_keys
                .get(&device_id)
                .map_or(false, |keys| keys.contains(&key));
            self.update_physical_key(device_id, key, down);

            let consumed_by_active = self
                .active_binding
                .map(|index| self.bindings[index].trigger == key)
                .unwrap_or(false);

            if was_blocked {
                if !down {
                    self.unblock_key(device_id, key);
                }
                self.clear_inactive_binding();
                continue;
            }

            if consumed_by_active {
                self.clear_inactive_binding();
                continue;
            }

            let matched = down
                .then(|| self.matching_binding(key))
                .flatten()
                .filter(|_| self.active_binding.is_none());

            if let Some(binding_index) = matched {
                let old_route = self.current;
                if self.propagate_switch_keys {
                    pending.push((old_route, event));
                }
                flush_events(device_id, &mut pending, &mut actions);

                self.current = next_route(routes, old_route);
                self.active_binding = Some(binding_index);
                self.prepare_handoff(binding_index);
                actions.extend(self.reconcile_all(routes, ReconcileMode::Handoff));
                continue;
            }

            if !self.propagate_switch_keys && self.switch_keys.contains(&key) {
                self.clear_inactive_binding();
                continue;
            }

            pending.push((self.current, event));
            self.clear_inactive_binding();
        }

        flush_events(device_id, &mut pending, &mut actions);
        actions
    }

    pub(crate) fn retain_routes(&mut self, routes: &[Route]) -> Vec<Action> {
        if routes.contains(&self.current) {
            return Vec::new();
        }

        self.current = 0;
        self.prepare_handoff_without_binding();
        self.reconcile_all(routes, ReconcileMode::Handoff)
    }

    fn update_physical_key(&mut self, device_id: usize, key: Key, down: bool) {
        let keys = self.physical_keys.entry(device_id).or_default();
        if down {
            keys.insert(key);
        } else {
            keys.remove(&key);
        }
    }

    fn unblock_key(&mut self, device_id: usize, key: Key) {
        let Some(keys) = self.blocked_keys.get_mut(&device_id) else {
            return;
        };
        keys.remove(&key);
        if keys.is_empty() {
            self.blocked_keys.remove(&device_id);
        }
    }

    fn matching_binding(&self, trigger: Key) -> Option<usize> {
        let pressed = self.pressed_key_union();
        self.trigger_bindings
            .get(&trigger)?
            .iter()
            .find_map(|index| {
                self.bindings[*index]
                    .keys
                    .is_subset(&pressed)
                    .then_some(*index)
            })
    }

    fn clear_inactive_binding(&mut self) {
        let Some(index) = self.active_binding else {
            return;
        };
        if self.bindings[index]
            .keys
            .is_disjoint(&self.pressed_key_union())
        {
            self.active_binding = None;
        }
    }

    fn pressed_key_union(&self) -> HashSet<Key> {
        self.physical_keys
            .values()
            .flat_map(|keys| keys.iter())
            .copied()
            .collect()
    }

    fn prepare_handoff(&mut self, binding_index: usize) {
        let binding = &self.bindings[binding_index];
        let switch_keys = &self.switch_keys;
        let propagate = self.propagate_switch_keys;

        self.blocked_keys = self
            .physical_keys
            .iter()
            .filter_map(|(device_id, keys)| {
                let blocked = keys
                    .iter()
                    .filter(|key| {
                        !key.is_modifier()
                            || **key == binding.trigger
                            || (!propagate && switch_keys.contains(key))
                    })
                    .copied()
                    .collect::<HashSet<_>>();
                (!blocked.is_empty()).then_some((*device_id, blocked))
            })
            .collect();
    }

    fn prepare_handoff_without_binding(&mut self) {
        let switch_keys = &self.switch_keys;
        let propagate = self.propagate_switch_keys;
        let active_trigger = self
            .active_binding
            .map(|index| self.bindings[index].trigger);
        self.blocked_keys = self
            .physical_keys
            .iter()
            .filter_map(|(device_id, keys)| {
                let blocked = keys
                    .iter()
                    .filter(|key| {
                        !key.is_modifier()
                            || Some(**key) == active_trigger
                            || (!propagate && switch_keys.contains(key))
                    })
                    .copied()
                    .collect::<HashSet<_>>();
                (!blocked.is_empty()).then_some((*device_id, blocked))
            })
            .collect();
    }

    fn reconcile_all(&self, routes: &[Route], mode: ReconcileMode) -> Vec<Action> {
        let mut actions = Vec::new();

        for route in routes
            .iter()
            .copied()
            .filter(|route| *route != self.current)
        {
            for device_id in self.physical_keys.keys().copied() {
                actions.push(Action::SetKeyState {
                    route,
                    device_id,
                    pressed_keys: HashSet::new(),
                });
            }
        }

        if routes.contains(&self.current) {
            for device_id in self.physical_keys.keys().copied() {
                actions.push(Action::SetKeyState {
                    route: self.current,
                    device_id,
                    pressed_keys: self.desired_keys(device_id, mode),
                });
            }
        }

        actions
    }

    fn reconcile_device(
        &self,
        device_id: usize,
        routes: &[Route],
        mode: ReconcileMode,
    ) -> Vec<Action> {
        routes
            .iter()
            .copied()
            .map(|route| Action::SetKeyState {
                route,
                device_id,
                pressed_keys: if route == self.current {
                    self.desired_keys(device_id, mode)
                } else {
                    HashSet::new()
                },
            })
            .collect()
    }

    fn desired_keys(&self, device_id: usize, mode: ReconcileMode) -> HashSet<Key> {
        let Some(physical) = self.physical_keys.get(&device_id) else {
            return HashSet::new();
        };
        let blocked = self.blocked_keys.get(&device_id);

        physical
            .iter()
            .filter(|key| {
                (mode == ReconcileMode::Recovery || key.is_modifier())
                    && !blocked.map_or(false, |keys| keys.contains(key))
            })
            .copied()
            .collect()
    }
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum ReconcileMode {
    Handoff,
    Recovery,
}

fn next_route(routes: &[Route], current: Route) -> Route {
    routes
        .iter()
        .copied()
        .find(|route| *route > current)
        .unwrap_or(0)
}

fn flush_events(device_id: usize, pending: &mut Vec<(Route, Event)>, actions: &mut Vec<Action>) {
    let mut pending = pending.drain(..).peekable();
    while let Some((route, event)) = pending.next() {
        let mut events = vec![event];
        while matches!(pending.peek(), Some((next_route, _)) if *next_route == route) {
            let (_, event) = pending.next().unwrap();
            events.push(event);
        }
        events.push(Event::Sync(SyncEvent::All));
        actions.push(Action::Events {
            route,
            device_id,
            events,
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rkvm_input::key::Keyboard;

    fn key(key: Keyboard) -> Key {
        Key::Key(key)
    }

    fn binding(keys: &[Keyboard], trigger: Keyboard) -> SwitchBinding {
        SwitchBinding::new(keys.iter().copied().map(key).collect(), key(trigger))
    }

    fn key_event(keyboard: Keyboard, down: bool) -> Event {
        Event::Key(KeyEvent {
            key: key(keyboard),
            down,
        })
    }

    fn frame(events: impl IntoIterator<Item = Event>) -> Vec<Event> {
        events
            .into_iter()
            .chain([Event::Sync(SyncEvent::All)])
            .collect()
    }

    fn set_state(actions: &[Action], route: Route, device_id: usize) -> Option<&HashSet<Key>> {
        actions.iter().find_map(|action| match action {
            Action::SetKeyState {
                route: action_route,
                device_id: action_device,
                pressed_keys,
            } if *action_route == route && *action_device == device_id => Some(pressed_keys),
            _ => None,
        })
    }

    fn routed_key_events(actions: &[Action], route: Route) -> Vec<KeyEvent> {
        actions
            .iter()
            .filter_map(|action| match action {
                Action::Events {
                    route: action_route,
                    events,
                    ..
                } if *action_route == route => Some(events),
                _ => None,
            })
            .flatten()
            .filter_map(|event| match event {
                Event::Key(event) => Some(*event),
                _ => None,
            })
            .collect()
    }

    fn apply_actions(outputs: &mut HashMap<(Route, usize), HashSet<Key>>, actions: &[Action]) {
        for action in actions {
            match action {
                Action::Events {
                    route,
                    device_id,
                    events,
                } => {
                    let state = outputs.entry((*route, *device_id)).or_default();
                    for event in events {
                        if let Event::Key(KeyEvent { key, down }) = event {
                            if *down {
                                state.insert(*key);
                            } else {
                                state.remove(key);
                            }
                        }
                    }
                }
                Action::SetKeyState {
                    route,
                    device_id,
                    pressed_keys,
                } => {
                    outputs.insert((*route, *device_id), pressed_keys.clone());
                }
            }
        }
    }

    fn router(propagate: bool) -> Router {
        Router::new(
            &[
                binding(&[Keyboard::LeftMeta, Keyboard::Grave], Keyboard::Grave),
                binding(&[Keyboard::RightCtrl], Keyboard::RightCtrl),
            ],
            propagate,
        )
    }

    #[test]
    fn handoff_releases_old_route_and_reasserts_held_modifiers() {
        let mut router = router(true);
        let routes = [0, 1];
        router.add_device(7, HashSet::new(), &routes);

        router.process_frame(7, frame([key_event(Keyboard::LeftShift, true)]), &routes);
        router.process_frame(7, frame([key_event(Keyboard::LeftMeta, true)]), &routes);
        let actions = router.process_frame(7, frame([key_event(Keyboard::Grave, true)]), &routes);

        assert_eq!(router.current(), 1);
        assert_eq!(
            routed_key_events(&actions, 0),
            vec![KeyEvent {
                key: key(Keyboard::Grave),
                down: true,
            }]
        );
        assert!(set_state(&actions, 0, 7).unwrap().is_empty());
        assert_eq!(
            set_state(&actions, 1, 7).unwrap(),
            &[key(Keyboard::LeftShift), key(Keyboard::LeftMeta)]
                .into_iter()
                .collect()
        );
    }

    #[test]
    fn held_binding_modifier_applies_to_following_shortcut() {
        let mut router = router(true);
        let routes = [0, 1];
        router.add_device(3, HashSet::new(), &routes);
        router.process_frame(3, frame([key_event(Keyboard::LeftMeta, true)]), &routes);
        router.process_frame(3, frame([key_event(Keyboard::Grave, true)]), &routes);
        router.process_frame(3, frame([key_event(Keyboard::Grave, false)]), &routes);

        let actions = router.process_frame(3, frame([key_event(Keyboard::T, true)]), &routes);
        assert_eq!(
            routed_key_events(&actions, 1),
            vec![KeyEvent {
                key: key(Keyboard::T),
                down: true,
            }]
        );

        let actions =
            router.process_frame(3, frame([key_event(Keyboard::LeftMeta, false)]), &routes);
        assert_eq!(
            routed_key_events(&actions, 1),
            vec![KeyEvent {
                key: key(Keyboard::LeftMeta),
                down: false,
            }]
        );
    }

    #[test]
    fn held_plain_keys_are_released_and_suppressed_until_released() {
        let mut router = router(true);
        let routes = [0, 1];
        router.add_device(2, HashSet::new(), &routes);
        router.process_frame(2, frame([key_event(Keyboard::A, true)]), &routes);
        router.process_frame(2, frame([key_event(Keyboard::LeftMeta, true)]), &routes);
        let actions = router.process_frame(2, frame([key_event(Keyboard::Grave, true)]), &routes);

        assert!(!set_state(&actions, 1, 2)
            .unwrap()
            .contains(&key(Keyboard::A)));
        let release = router.process_frame(2, frame([key_event(Keyboard::A, false)]), &routes);
        assert!(routed_key_events(&release, 1).is_empty());
    }

    #[test]
    fn trigger_does_not_leak_to_new_route_while_binding_is_active() {
        let mut router = router(true);
        let routes = [0, 1];
        router.add_device(1, HashSet::new(), &routes);
        router.process_frame(1, frame([key_event(Keyboard::LeftMeta, true)]), &routes);
        router.process_frame(1, frame([key_event(Keyboard::Grave, true)]), &routes);

        for down in [false, true, false] {
            let actions =
                router.process_frame(1, frame([key_event(Keyboard::Grave, down)]), &routes);
            assert!(routed_key_events(&actions, 1).is_empty());
        }
        assert_eq!(router.current(), 1);
    }

    #[test]
    fn one_key_modifier_trigger_is_not_reasserted() {
        let mut router = router(true);
        let routes = [0, 1];
        router.add_device(4, HashSet::new(), &routes);
        let actions =
            router.process_frame(4, frame([key_event(Keyboard::RightCtrl, true)]), &routes);

        assert_eq!(router.current(), 1);
        assert!(set_state(&actions, 1, 4).unwrap().is_empty());
    }

    #[test]
    fn disabled_propagation_consumes_switch_binding() {
        let mut router = router(false);
        let routes = [0, 1];
        router.add_device(4, HashSet::new(), &routes);
        let meta = router.process_frame(4, frame([key_event(Keyboard::LeftMeta, true)]), &routes);
        let trigger = router.process_frame(4, frame([key_event(Keyboard::Grave, true)]), &routes);

        assert!(routed_key_events(&meta, 0).is_empty());
        assert!(routed_key_events(&trigger, 0).is_empty());
        assert!(set_state(&trigger, 1, 4).unwrap().is_empty());
    }

    #[test]
    fn route_loss_rehomes_held_modifiers_locally() {
        let mut router = router(true);
        let routes = [0, 1];
        router.add_device(8, HashSet::new(), &routes);
        router.process_frame(8, frame([key_event(Keyboard::LeftMeta, true)]), &routes);
        router.process_frame(8, frame([key_event(Keyboard::Grave, true)]), &routes);
        router.process_frame(8, frame([key_event(Keyboard::Grave, false)]), &routes);

        let actions = router.retain_routes(&[0]);

        assert_eq!(router.current(), 0);
        assert_eq!(
            set_state(&actions, 0, 8).unwrap(),
            &[key(Keyboard::LeftMeta)].into_iter().collect()
        );
    }

    #[test]
    fn route_loss_does_not_reassert_active_modifier_trigger() {
        let mut router = router(true);
        let routes = [0, 1];
        router.add_device(8, HashSet::new(), &routes);
        router.process_frame(8, frame([key_event(Keyboard::RightCtrl, true)]), &routes);

        let actions = router.retain_routes(&[0]);

        assert_eq!(router.current(), 0);
        assert!(set_state(&actions, 0, 8).unwrap().is_empty());
    }

    #[test]
    fn recovery_snapshot_restores_full_nontrigger_state() {
        let mut router = router(true);
        let pressed = [key(Keyboard::LeftShift), key(Keyboard::A)]
            .into_iter()
            .collect();
        let actions = router.add_device(5, pressed, &[0, 1]);

        assert_eq!(
            set_state(&actions, 0, 5).unwrap(),
            &[key(Keyboard::LeftShift), key(Keyboard::A)]
                .into_iter()
                .collect()
        );
        assert!(set_state(&actions, 1, 5).unwrap().is_empty());
    }

    #[test]
    fn recovery_snapshot_blocks_held_trigger_until_release() {
        let mut router = router(true);
        let pressed = [key(Keyboard::LeftMeta), key(Keyboard::Grave)]
            .into_iter()
            .collect();
        let actions = router.add_device(6, pressed, &[0]);

        assert_eq!(
            set_state(&actions, 0, 6).unwrap(),
            &[key(Keyboard::LeftMeta)].into_iter().collect()
        );
        let release = router.process_frame(6, frame([key_event(Keyboard::Grave, false)]), &[0]);
        assert!(routed_key_events(&release, 0).is_empty());
    }

    #[test]
    fn every_modifier_handoff_converges_to_empty_outputs() {
        for modifier in [
            Keyboard::LeftAlt,
            Keyboard::LeftCtrl,
            Keyboard::LeftMeta,
            Keyboard::LeftShift,
            Keyboard::RightAlt,
            Keyboard::RightCtrl,
            Keyboard::RightMeta,
            Keyboard::RightShift,
        ] {
            let mut router = Router::new(
                &[binding(
                    &[Keyboard::LeftMeta, Keyboard::Grave],
                    Keyboard::Grave,
                )],
                true,
            );
            let routes = [0, 1, 2];
            let mut outputs = HashMap::new();
            apply_actions(&mut outputs, &router.add_device(9, HashSet::new(), &routes));

            for event in [
                key_event(Keyboard::A, true),
                key_event(modifier, true),
                key_event(Keyboard::LeftMeta, true),
                key_event(Keyboard::Grave, true),
            ] {
                let actions = router.process_frame(9, frame([event]), &routes);
                apply_actions(&mut outputs, &actions);
            }

            assert_eq!(router.current(), 1);
            assert!(outputs.get(&(0, 9)).unwrap().is_empty());
            assert!(outputs.get(&(2, 9)).unwrap().is_empty());
            assert!(!outputs.get(&(1, 9)).unwrap().contains(&key(Keyboard::A)));
            assert!(outputs.get(&(1, 9)).unwrap().contains(&key(modifier)));

            for keyboard in [Keyboard::Grave, Keyboard::LeftMeta, modifier, Keyboard::A] {
                let actions = router.process_frame(9, frame([key_event(keyboard, false)]), &routes);
                apply_actions(&mut outputs, &actions);
            }

            assert!(outputs.values().all(HashSet::is_empty));
        }
    }

    #[test]
    fn chords_can_span_source_devices() {
        let mut router = router(true);
        let routes = [0, 1];
        router.add_device(1, HashSet::new(), &routes);
        router.add_device(2, HashSet::new(), &routes);
        router.process_frame(1, frame([key_event(Keyboard::LeftMeta, true)]), &routes);
        router.process_frame(2, frame([key_event(Keyboard::Grave, true)]), &routes);

        assert_eq!(router.current(), 1);
    }
}
