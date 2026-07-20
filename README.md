# rkvm
[![rkvm](https://img.shields.io/aur/version/rkvm)](https://aur.archlinux.org/packages/rkvm)

rkvm is a tool for sharing keyboard and mouse across multiple Linux machines.
It is based on a client/server architecture, where server is the machine controlling mouse and keyboard and relays events (mouse move, key presses, ...) to clients.

Switching between different clients is done by a configurable keyboard shortcut.

## Features
- TLS encrypted by default, backed by [rustls](https://github.com/rustls/rustls)
- Display server agnostic (in fact, it doesn't require a display server at all)
- Low overhead

## Requirements
- The uinput Linux kernel module, enabled by default in most distros. You can confirm that it's enabled in your distro by checking that `/dev/uinput` exists.
- libevdev development files (`sudo apt install libevdev-dev` on Debian/Ubuntu)
- Clang/LLVM (`sudo apt install clang` on Debian/Ubuntu)

## Manual installation
If you can, it is strongly recommended to use the [AUR package](https://aur.archlinux.org/packages/rkvm) to install rkvm.  
Note that the master branch can contain untested and breaking changes - for regular use, it is recommended to pick the latest [release](https://github.com/htrefil/rkvm/releases) instead.

```
$ cargo build --release
# cp target/release/rkvm-client /usr/bin/
# cp target/release/rkvm-server /usr/bin/
# cp target/release/rkvm-certificate-gen /usr/bin/ # Optional
# cp systemd/rkvm-client.service /usr/lib/systemd/system/
# cp systemd/rkvm-server.service /usr/lib/systemd/system/
```

## Configuration
After installation:
- Generate a certificate and private key using the `rkvm-certificate-gen` tool or provide your own from other sources.
- For server, place both the certificate and private key in `/etc/rkvm/certificate.pem` and `/etc/rkvm/key.pem` respectively.
- For client, place the certificate to `/etc/rkvm/certificate.pem`.
- Create a config if you haven't done so already.  
  Server:  
  ```
  # cp /usr/share/rkvm/examples/server.toml /etc/rkvm/server.toml
  ```
  Client:
  ```
  # cp /usr/share/rkvm/examples/client.toml /etc/rkvm/client.toml
  ```
  Do not edit the example configs, they will be overwritten by your package manager.
- **Change the password** and optionally reconfigure the network listen address, key bindings for switching clients
  (`switch-bindings`, or legacy `switch-keys` for one chord), the per-client queue size (`client-queue-size`),
  and either `device-whitelist` or ordered `device-groups` if you only want rkvm to grab and forward selected
  input devices. The two input policies are mutually exclusive. Switch bindings are
  ordered: the last key is the activation trigger, so `["left-ctrl", "space"]` switches only when `space` is
  pressed while `left-ctrl` is already held. At each switch, rkvm releases all keys on inactive outputs and
  reasserts physically held Ctrl, Shift, Alt, and Meta modifiers on the new output. The trigger and other held
  keys remain suppressed until released, preventing the switch chord or an unrelated held key from leaking to
  the new machine. Prefer stable
  `/dev/input/by-id/*-event-kbd` or `/dev/input/by-path/*-event-kbd` symlinks in the whitelist instead of
  `/dev/input/eventN` paths, because event numbers can change between boots. Use
  `rkvm-server /etc/rkvm/server.toml --list-devices` to inspect candidate paths, aliases, source origin,
  bus type, capabilities, and policy matches; avoid vendor/product-only rules unless you want every matching
  event node from that physical device or receiver.
- Since rkvm-server exclusively grabs every input selected by its policy, it is a good idea to do a test run
  first to make sure your display server is properly configured to receive input from rkvm.

  Run the following command to start rkvm-server for 15 seconds to test that your keyboard, mouse, etc. works properly:
  ```
  # rkvm-server /etc/rkvm/server.toml --shutdown-after 15
  ```

- Enable and start the systemd service.  
  Server:
  ```
  # systemctl enable rkvm-server
  # systemctl start rkvm-server
  ```
  Client:
  ```
  # systemctl enable rkvm-client
  # systemctl start rkvm-client
  ```

## Input preprocessors and fallback devices

rkvm continuously reconciles the evdev inventory. Devices that are temporarily busy, interrupted, or changing
are retried with bounded backoff; hotplug removal affects only that routed device. This allows rkvm to coexist
with exclusive-grab preprocessors such as interception-tools without relying on a service startup sleep.

Use `device-groups` when one logical peripheral can appear as both a preferred processed virtual device and a
physical fallback. Candidates are ordered by precedence, and at most one candidate in a group is active. A
per-candidate `grab-delay-ms` gives a preprocessor time to claim the physical source and publish its virtual
output. A present preferred candidate that is busy or deferred suppresses lower candidates. For input safety,
rkvm does not replace an already active fallback when a preferred candidate appears later; the preferred device
is selected after the active device disconnects or rkvm restarts.

Candidate match fields are `path`, `name`, `vendor`, `product`, `version`, `origin`, and `bustype`. Fields in one
candidate are ANDed. `origin` is derived from sysfs and is either `physical` or `virtual`; `bustype` is the source
evdev bus ID, such as `0x0003` (`BUS_USB`) or `0x0006` (`BUS_VIRTUAL`). A uinput clone may preserve a physical
bus type, so use `origin` and `bustype` together when necessary. See `example/server.toml` for a complete group
example.

## Why rkvm and not Barrier/Synergy?
The author of this program had a lot of problems with said programs, namely his keyboard layout (Czech) not being supported properly, which stems from the fact that the programs send characters which it then attempts to translate back into keycodes. rkvm takes a different approach to solving this problem and doesn't assume anything about your keyboard layout -- it sends raw keycodes only.

Additionally, rkvm doesn't even know or care about X, Wayland or any display server that might be in use, because it uses the uinput API with libevdev to read and generate input events.

Regardless, if you want a working and stable solution for crossplatform keyboard and mouse sharing, you should probably use either of the above mentioned programs for the time being.

## Limitations
- Linux only

## Project structure
- `rkvm-server` - server application code
- `rkvm-client` - client application code
- `rkvm-input` - handles reading from and writing to input devices
- `rkvm-net` - network protocol encoding and decoding
- `rkvm-certificate-gen` - certificate generation tool

[Bincode](https://github.com/servo/bincode) is used for encoding of messages on the network and [Tokio](https://tokio.rs) as an asynchronous runtime.

## Contributions
All contributions, that includes both PRs and issues, are very welcome.

## Donations
If you find rkvm useful, you can donate to the original author and maintainer using [Ko-fi](https://ko-fi.com/htrefil).

## License
[MIT](LICENSE)
