# Changelog

Notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.0.1](https://github.com/imperviousinc/yuki/releases/tag/v0.0.1) - 2026-04-18

### Bug Fixes

- Advertise `inv` before sending `tx`
- *(node)* Remove useless `new_from_config` constructor
- *(chain)* Remove unused `values` method
- *(builder)* Do not reset scripts and peers when adding them
- Typo in persistence
- *(node)* Update stale block time after resetting connections
- *(chain)* Remove invalid check for MTP during fork
- Remove transaction rejection from `Event`
- *(peer_map)* Prefer peers that signal `CompactFilters`
- *(peer_map)* Remove optional service flags from `ManagedPeer`
- *(node)* Remove `is_running` from `Node`
- *(db)* Update port and services when a peer is gossiped
- *(lib)* Elide unnecessary lifetimes
- *(db)* Update port and services for gossiped peers in `StatelessPeerStore`
- *(client)* Separate log and event receivers
- *(filters)* Remove unnecessary `async`
- Prefer `expect` in binary
- *(builder)* Clamp connections to range
- *(ci)* Build only on MSRV
- *(example)* Update recovery height
- *(network)* Use `into_payload`
- *(db)* Use blob for services
- Ignore idea
- Use inline docs for public exports
- Check `bits` from slice
- Audit individual `bits` in batch
- *(chain)* Audit `bits`
- Clippy lints
- Remove unused `IndexedTransaction`
- Remove bits audit
- *(filters)* Remove unsused method, move to `mod`
- *(client)* Remove `async` from `shutdown_blocking`
- *(example)* Update `tor`
- *(example)* New `HeaderCheckpoint` constructor
- *(network)* Limit number of `ADDR` sent by a single peer
- Add unique errors to each SQL database
- *(node)* Remove redundant dialog
- *(chain)* Do not compute merkle root for block we no longer need
- *(chain)* Prevent block request spam
- *(chain)* Blockqueue contains wanted hash
- *(network)* Increase max handshake buffer
- *(node)* Only `getheaders` for `inv` with new blocks
- *(peer)* Shorten connection timeout
- *(client)* Add single ScriptBuf
- *(sp)* [**breaking**] Remove explicit sp module
- *(node)* Disconnect peers on stale tip
- *(node)* Remove blocks from queue earlier
- *(node)* Use next message fn
- *(p2p)* Request witness block with sp enabled
- *(db)* Add persistence failure read/write details
- *(peers)* Disconnect peers for messages gt our version
- *(toml)* License
- *(node)* Detect stale tip
- *(client)* Custom warning for no cbf and hide message
- *(peer)* Advertise correct version
- *(peer)* Do not relay 0 len addrs
- *(peer)* Timeout v2 handshakes
- *(db)* Filter unknown flag
- *(node)* Convey if tx was rejected or remote node dropped
- *(chain)* Remove fallible ops
- *(filters)* Compute filter hash on construction
- *(peer)* Avoid block clone
- *(db)* Add schema versioning to sql
- *(peer)* Use clear messages over tor
- *(peer)* V2 clean up
- *(lints)* Peer
- *(peers)* Clean up unwraps
- *(client)* [**breaking**] Field for warning enum

### Build

- Add rust-version and update dependencies
- Add peer struct

### CI

- Drop lint and test jobs, keep feature matrix only
- Add release-plz and CI workflows, rename crate to bitcoin_yuki
- Reduce audit to weekly
- Add security audit
- Add pin
- Add back msrv
- Remove package id

### Chore

- Update license link
- Pull readme badge from kyoto repo, not bdk
- Remove VSCode IDE configurations
- *(deps)* Remove explicit `serde` feature
- Add mainnet checkpoint
- Rename `peers` module to `network`
- Change non-mutating functions to &self
- Rename `node` module to `core`
- Extract client logic to macro
- Move clippy-allows to source
- *(peers)* Make message reading generic
- Rename `ip` to `address` where it applies

### Documentation

- Standardize comments

### Features

- Introduce `log!` macro and account for `LogLevel`
- Introduce log level
- Configure DNS resolver
- *(chain)* Introduce `HeightMonitor` to associate peers and chain length
- *(client)* [**breaking**] Split `Warning` into separate receiver
- Add `RejectPayload` to `Warning` event
- *(client)* Check if node is running from client
- *(client)* Request the broadcast minimum `FeeRate`
- *(client)* Implement `Display` for `Log`
- *(client)* Request range of headers from persistence
- *(test)* Update `corepc-node` to `0.5.0`
- Add most recent checkpoint constructor
- *(client)* Add random broadcast method
- *(client)* Request a block and get a `Recevier` to await the result
- Request `IndexedBlock` directly from `Client`
- *(test)* Switch to `corepc-node` to start `bitcoind`
- *(example)* Start managed workflow example
- Pass `FeeFilter` to client
- Configure the maximum connection for a peer
- *(client)* Get header in blocking context
- Add progress event
- Add support for testnet4
- *(client)* Get `Header` at height from `Client`
- *(db)* Get `Header` at height
- *(lib)* Use `Into` in `Client` and `builder`
- Shutdown in blocking context
- Make `lib` types more robust
- Build checkpoint from height
- Add conversions for `HeaderCheckpoint`
- *(builder)* Robustness w.r.t `PathBuf` type
- *(client)* Add blocking methods on `Client`
- Add trusted peers with client message
- *(core)* Set connection timeout with client message
- Add type alias for `Node` in `builder`
- *(lib)* Convert `IpAddr` and `SocketAddr` to `TrustedPeer`
- *(chain)* Validate block merkle root
- *(sp)* Add filter messages and `getblock`
- *(node)* Make `Node::run` immmutable
- *(client)* Allow explicit pause for filter sync
- *(sp)* Initial support for scanning blocks
- *(lib)* Build peer from socket addr
- *(client)* Add silent payments key tweak support
- *(peer)* Find new peers after long connections
- *(client)* Add response timeout to config
- *(peers)* Add V2 handshake, message parsing
- *(peers)* Add V2 transport traits
- Add support for `TorV3` addresses

### Fix

- Early return if hash request equals checkpoint

### Refactor

- *(builder)* [**breaking**] Use declarative naming for methods
- [**breaking**] Rename `Dialog` to `Debug` in `Log`
- *(filter)* [**breaking**] Use `Iterator` in method signature
- Refactor!(test, db): move unit trait definitions into test module
- *(test)* Drop test prefix and improve naming
- *(test)* Remove `new_regtest_two_peers` and generalize `new_regtest`
- Use `HeightMonitor` in `PeerMap` and `Chain`
- *(chain)* Make `HeaderBatch` sync
- *(chain)* Make `height_of_hash` sync
- *(chain)* Make `CFHeaderChain` fully sync
- *(chain)* Make `FilterChain` fully sync
- *(chain)* Remove the MTP check
- [**breaking**] Rename `UnlinkableAnchor` to `InvalidStartHeight`
- [**breaking**] Rename `NotEnoughConnections` to `NeedConnections` and add connection details
- [**breaking**] Add named field to `TransactionRejected` enum variant
- Implement `Median` for slice types with macro
- Remove async from `send_warning` method
- [**breaking**] Rename `FailurePayload` to `RejectPayload`
- *(client)* [**breaking**] Rename `EventSender` to `Requester`
- [**breaking**] Remove the `StatelessPeerStore`
- [**breaking**] Track broadcast fee minimum and remove `FeeFilter` event
- [**breaking**] Rename `PeerStatus::New` to `Gossiped`
- *(db)* [**breaking**] Load `Header` from `RangeBounds` of heights
- *(chain)* Remove `unwrap` from filter header chain
- *(client)* Map to error when no header is found
- [**breaking**] Introduce enum for peer db config
- [**breaking**] Rename `Client::get_block` to `request_block`
- [**breaking**] Rename `silent-payments` feat to `filter-control`
- Extract `Progress` into struct
- *(client)* [**breaking**] Remove waiting broadcast
- Simplify error messages
- [**breaking**] Move `NodeError` subvariants into `core`
- Add generic error type to `PeerStore` trait
- Add generic error type to `HeaderStore` trait
- Remove thiserror
- *(reader)* Organize imports

### Tests

- Add `print_logs` utility function
- Remove unnecessary `async` from node building fns
- *(core)* Improve method tests on client
- *(db)* Add tempfile for sql tests
- *(core)* Add signet sync
- *(chain)* Add inv scenarios
- *(chain)* Add reorg scenarios
- *(chain)* Add chain sync scenarios
- *(lib)* Add unit test script
- Add pr script
- Broadcast queue works
- *(lib)* Mine after live reorg
- *(lib)* Depth two reorg, stale tip
- *(lib)* Sql handles regtest reorg
- Regtest long chain
- Broadcast tx to core
- Add regtest reorg
- Add reorg tests chain
- Chain handles forks
- Add reorg tests fn extend

### Bump

- *(tokio)* 1.37

### Chain

- Signet checkpoint and more POW validation
- Remove reorged blocks from queue
- Add mainnet checkpoints
- Load headers outside of constructors
- Better reorg behavior for CBF
- Move headers to disk
- Change CBF header abstraction
- Fix header batch check
- Send block over dialog
- Extend fn MSRV
- BTreeMap for headers/store
- Stronger height type
- Move db out of chain, fix sql stmt
- Test headerchain
- Add BlockQueue
- Define CF header sync result
- Add hashmap for blockhash/filterhash
- Sync from checkpoint, break out header chain
- Rename chain struct
- Rework filter header chain
- Validate filter headers

### Client

- Emit connected to requirement
- Stanardize warning verbage
- Make warning enum
- Remove unnecessary &mut self
- Publish state changes
- Update tx messages
- Update sync event
- Switch to ScriptBuf api
- Minimal tx broadcast, add new scripts
- Rename chan messages
- Make chan broadcast
- Add client sender structure
- Add client -> node messages
- Send blocks and transactions over channel
- Move log stream out of node

### Crate

- Put sql db behind feature

### Db

- Add peer status newtype
- Make SQL db public
- Add in-memory peer db
- Add optional PathBuf for headers
- Add optional PathBuf
- Add header db, modify node constructor

### Deps

- Remove explicit `bitcoin_hashes`
- Use rand reexport
- Remove async_trait

### Doc

- *(client)* Mention `panic` condition for blocking calls
- Misc fixes
- Update `README` with testing guide
- *(example)* Update example descriptions
- Build `HeaderCheckpoint` from height in examples
- Update feature list
- Update TRUC broadcast
- Add `0.3.0` changelog
- Add license section to `README`
- *(core)* Update `Warning` docs with expected behavior
- Add `v0.2.0` to changelog
- Improve contributing with project layout
- Update module documentation
- V0.1.0 changelog
- Update links
- Update details
- Add changelog
- Misc fixes
- Add rustdoc examples
- Update error documentation
- Add contributing guidelines
- Update README.md
- MSRV policy
- Remove DETAILS and CHECKLIST from root
- Add folder and presentation
- Re-export nested types
- Add details
- Add details
- Update README
- *(test)* Steps for regtest
- Update README
- Update checklist
- Update checklist
- Fix code comments, println
- Update checklist
- Fix scope and rexport
- Add checklist
- Add comments for all public members
- Update checklist
- Add lib desc
- Link to article
- Update format README

### Example

- Use `LogLevel::Warning` in `managed` example
- Update Tor to latest version
- Update signet to higher start height
- Update peer list
- Move daemon to example folder

### Headers

- Set up cfheader args

### Lib

- Release `0.9.0`
- Remove `dns` feature and enable DNS by default
- Release `0.8.0`
- Export `tokio` and remove module exports for `mpsc` in client
- Reexport `mpsc` channel receviers
- Release `0.7.0`
- Release `0.6.0`
- Upgrade `bitcoin` to `0.32.5`
- Add checkpoints
- Upgrade `bip324` to `0.6.0`
- Release `0.5.0`
- Fix `just check`
- Update `just` commands
- Update `bip324` to `0.5.0` and `bitcoin` patch
- Update `bip324` dependency
- Release `0.4.0`
- Add `just` command
- Release `v0.3.0`
- Release `v0.2.0`
- Add `just`
- Release `0.1.0`
- Avoid unnecessary `clone`
- Avoid `unwrap` and `expect`
- *(exports)* Re-export useful types
- *(config)* Add port for TorV3 constructor
- *(config)* Accept TorV3 addr
- Add from ip for trusted peers
- Pub fields

### License

- APACHE-2.0/MIT

### Multi

- [doc] fmt changelog [core] use owned u32
- [lint] warn missing doc [peers] random try/new preference
- [test] add v2 transport to core tests, [lib] add services for configured peers
- [deps] remove dns-lookup, [peer] implement dns query
- [peer] add dialog, warnings [node] implement display for node state
- [chain] better err handle, [peer] less addr filter
- [node, client]: add tx reject, condense lib tests
- [node] fix block queue [peer] port req [client] tx sent
- [db] simplify peer db, [node] add peer manager
- [chain] reverse block queue order, [db] load potential fork
- [example] start rescan [chain] handle dup cf headers
- [db] rewrite reorgs [chain] add block check for filter
- [node] broadcast indexed types [chain] make filter messages infallible
- [ci] add build and test [clippy] fix many errors
- [client] add conv methods, [chain] hashset filters instead full cache
- Prelude fix, checklist
- [node] add getblock, [chain] scan block, add block queue
- [node] add ScriptBuf list, [filters] check for ScriptBuf incl
- Add ip whitelist, sync up to height w/ eof
- [db] fix replacement bug [headers] handle forks
- Node state, timeouts, simple DOS protection
- Sync to chaintip, filter by CPF peers
- Rusqlite peer db, tokio-console
- Sync headers to checkpoint, add error handling

### Node

- Add bans for faulty syncing
- Add tor support
- Rm fields
- Signal ADDRV2
- Prevent block req spam
- Add try and ban to peer map
- Broadcast txs in batch
- Move db into peer map
- Fetch locator from db on start
- Make dns optional
- Update node with new peer db
- Add rescan
- Move tx broadcaster to own struct
- Move best height to map
- Return chaintip as sync update
- Break out dialog into struct
- Add regtest support
- Expand builder
- Introduce builder pattern
- Introduce client, make thread safe
- Handle inv and ask for headers, cf headers, filters
- Sync block filters, found performance bottleneck
- Fix lock on advancing node state
- Reduce node states, add CF Header messages
- Introduce PeerMap to manage multiple peers

### Peer

- Add generic connection type
- Move verack
- Clean up reader and peer
- Timeout reader thread
- Add timer for high latency
- Generic message generator

### Peers

- Support AddrV2
- Make reader/writer generic
- Add message counter
- Swap out dns dep

### Prefactor

- Return tuple from handshake

### Refact

- Bump p2p protocol version to 70013
- Remove useless `params_from_network` fn
- Remove duplicate code `HeaderCheckpoint`
- Remove `Into` for `&str` messages
- *(db)* Use `const` for known files and paths
- Remove `Option` from whitelist
- *(chain)* Clean up loaded headers check
- *(chain)* Use alias for u32
- *(peer)* Move timer into counter
- *(meta)* Standardize errors

### Temp

- Remove MSRV toolchain

### Tx

- In memory tx store

## 0.9.0

## Added

- Introduce log level and optimize release builds to remove heap allocations for debug messages
- Configure a custom DNS resolver

## Changed

- `Dialog` field renamed to `Debug`
- `dns` feature is removed and DNS is used by default
- Better naming on the fields of `Warning`
- `NodeBuilder` uses declarative naming for methods

## Fixes

- Tor and Signet examples updated
- Adding scripts or peers twice does not overwrite past changes in `NodeBuilder`
- Remove invalid assessment of median time past in fork scenario
- Use the proper `inv` -> `getdata` -> `tx` message exchange to broadcast transactions

## 0.8.0

## Added

- Request the broadcast minimum fee rate from connected peers.

## Changed

- Removed the `StatelessPeerStore`, used primarily for development
- Export the `tokio` crate
- Further split `Log` and `Event` enumerations into `Log`, `Warning`, and `Event`

## Fixes

- Update the port and services when peers are gossiped
- Reset the timer after disconnecting from peers due to a stale block
- Remove case for invalid median time check

## 0.7.0

## Added

- Request a block using the `Client`
- Add `broadcast_random` convenience method on `Client`
- Request a `Range` of block headers from `Client`

## Changed

- Separate logs from events into different event channels
    - The `Log` channel is bounded in size and contains informational, but non-critical information
    - The `Event` channel is unbounded and contains data that must be handled, like `IndexedBlock`
- Switch to `corepc-node` instead of unmaintained `bitcoincore-rpc`
- Load block headers with `RangeBounds`

## Fixes

- Remove unnecessary `unwrap` when managing filter headers
- Clamp connections to a defined range

## v0.6.0

## Added

- Pass `FeeFilter` to client
- Add Signet and Bitcoin checkpoints

## Changed

- Upgrade `bip324` to `0.6.0`
- Switch to `corepc-node` to start `bitcoind` in CI
- Use `into_payload` in `bitcoin 0.32.5`

## Fixes

- Add check to bits before adjustment
- Remove explicit `serde` feature

## v0.5.0

## Added

- Client may fetch a `Header` at a particular height
- Support for Testnet4 with new example

## Changed

- `HeaderStore` has additional `header_at` method
- Removed unused `IndexedTransaction` variant on `NodeMessage`
- New `Progress` variant on `NodeMessage`

## Fixes

- Use inline docs for rustdoc
- Check the `CompactTarget` of the block headers received with far stricter requirements with respect to the difficulty adjustment
- Bump `bip324` to `0.5.0` and `bitcoin` to `0.32.4`

## v0.4.0

## Added

- New `HeaderCheckpoint` constructor from height
- `shutdown`, `add_scripts`, `broadcast_transaction` methods have blocking APIs
- Add a `TrustedPeer` while the node is running
- Add change the peer timeout while the node is running

## Changed

- Use `impl Into` whenever possible on `NodeBuilder` and `Client` APIs
- Remove the misleading `wait_for_broadcast` method on `Client`

## Fixes

- Remove `Option` from `Whitelist` as it is already a `Vec`
- Limit the amount of `ADDR` messages a single peer can send

## v0.3.0

## Added

- Type alias for `Node` in `builder` with default generics

## Changed

- `HeaderStore` and `PeerStore` traits now have an associated error type
- `Node` is now generic over `H: HeaderStore` and `P: PeerStore`
- Move `NodeError` subvariants into `core`

## v0.2.0

## Added

- `just` and `justfile` for local development
- Additional SQL tests with `tempfile`
- Additional context to database read and write errors
- Support for a new silent payments feature-flag:
  - Receive block filters directly
  - Request blocks directly
  - Pause the node state before downloading filters

## Changed

- Disconnect peers if no block is found after 30 minutes
- Find new peers after 3 hour connections
- `Node::run` is an immutable method
- Check block merkle root
- Download blocks in parallel with filters
- Single `ScriptBuf` may be added at a time
- `Client` and `ClientSender` share methods

## Fixes

- Only request headers for `inv` the node does not have
- Max v2 handshake buffer size increased
- Changes to `BlockQueue` to not spam peers with requests
- Internal module renames

## v0.1.0

## Added

- `NodeBuilder` offers configuration options to build a compact block filters `Node` and `Client`
- `Client` is further split into `ClientSender` and `Receiver<NodeMessage>`
- `ClientSender` may: add scripts, broadcast transactions, rescan block filters, stop the node
- `Node` may run and send `NodeMessage`, `Warning` while running
- Connections to peers are encrypted if `ServiceFlags::P2P_V2` is signaled
- Connections are terminated if peers do not respond within a custom `Duration`
- Peers are selected at random with a target preference of 50% new and 50% tried
- Connections are terminated after long duration times to find new reliable peers
- Blocks are considered "stale" if there has not been a new inv message after 30 minutes. Block headers are requested again to ensure no inv were missed.
- Transactions are broadcast according to a configurable policy, either to all connections for higher reliability, or to a random peer for better privacy
- Default implementers of `PeerStore` and `HeaderStore` are `SqlitePeerDb` and `SqliteHeaderDb`
- Nodes will no peers will bootstrap with DNS
- Experimental support for connections routed over Tor
