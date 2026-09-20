# llmconduit Iroh Mesh: Hub/Worker, Enrollment, Availability, and Capacity Specification

**Status:** Implementation handoff  
**Target repository:** `local-inference-lab/llmconduit`  
**Target branch baseline:** current `master` as reviewed 2026-09-19  
**Primary implementation language:** Rust 2024 / Tokio  
**Transport:** Iroh QUIC, direct-to-controller by default  

---

## 1. Objective

Add a first-class distributed inference transport to `llmconduit` so the same `llmconduit` binary can operate in two complementary roles:

1. **Hub/controller mode**: the normal public `llmconduit` API gateway, plus a public Iroh/QUIC endpoint that accepts authenticated worker connections and routes LLM traffic to them.
2. **Worker/sidecar mode**: a lightweight `llmconduit` process running next to a local vLLM instance. It initiates an outbound Iroh/QUIC connection to the hub, advertises its models, availability, and capacity, and proxies hub-selected requests to the local vLLM TCP port.

The system must work when workers are behind arbitrary NAT/CGNAT and have no inbound ports exposed. The only publicly reachable peer is the hub/controller.

The design is optimized for **minimum data-plane overhead**:

- one long-lived QUIC connection per worker;
- one QUIC bidirectional stream per inference request;
- no SSH tunnel, VPN interface, SOCKS proxy, or separate reverse-proxy daemon;
- no application-level reserialization of inference bodies on the worker;
- no relay in the normal data path;
- streaming responses must remain streaming end-to-end;
- no extra request/response RTT may be introduced solely for capacity admission on the accepted path.

The hub must support **revocable enrollment keys**, persistent node identities, node revocation, dynamic model catalogs, and worker-provided availability/capacity advertisements.

---

## 2. Non-goals for the first implementation

Do **not** turn this into a general VPN or arbitrary port-forwarding product.

The first implementation does not need:

- an L3 virtual interface;
- arbitrary network access from hub to worker;
- arbitrary destination addresses selected by the hub;
- worker-to-worker connectivity;
- P2P worker discovery;
- NAT hole punching between workers;
- a mandatory Iroh relay service;
- distributed tensor/pipeline parallelism across workers;
- cross-worker KV cache sharing;
- central request queuing when every worker is saturated;
- a full marketplace/billing system;
- automatic preemption of active inference requests when capacity drops;
- 0-RTT enrollment;
- a new API translation layer on the worker.

The worker is a constrained reverse transport to configured local inference resources, not a general remote-access agent.

---

## 3. Existing llmconduit architecture that must be preserved

Before editing code, read the repository's `AGENTS.md` and follow it. In particular:

- OpenAI Responses is the canonical internal protocol.
- Existing adapters and engine semantics are load-bearing.
- Failover is pre-first-chunk only.
- Cancellation must propagate when the downstream caller disconnects.
- Use `tracing`, not ad-hoc `println!`, except where CLI output is appropriate.
- Do not add blocking I/O to the Tokio runtime.
- Keep the existing gateway injectable/testable via trait seams.

Relevant current architecture:

- `src/main.rs` — CLI dispatch and server startup.
- `src/cli.rs` — Clap commands.
- `src/config.rs` — persisted/runtime config.
- `src/lib.rs` — dependency-injection root; builds upstream clients and the `Gateway`.
- `src/upstream.rs` — `UpstreamClient`, `ReqwestUpstreamClient`, failover/routing clients, SSE parsing, model catalog handling, upstream health.
- `src/engine.rs` — canonical gateway flow and streaming orchestration.
- `tests/gateway.rs` — broad existing gateway regression suite.

At the reviewed baseline, `UpstreamClient` is already the main abstraction seam, but several routing/failover provider structs still hold concrete `ReqwestUpstreamClient` values. That must be addressed deliberately; do not duplicate the entire gateway stack just to add mesh transport.

### 3.1 Required compatibility rule

All existing non-mesh configurations must continue to behave identically.

Running:

```bash
cargo test
cargo clippy --all-targets
cargo fmt --check
```

must remain clean after the feature is added.

---

## 4. Target topology

```text
                           Public clients
                                |
                                | HTTP / HTTPS
                                v
                    +--------------------------+
                    | llmconduit HUB           |
                    |                          |
                    | existing API gateway     |
                    | model routing            |
                    | mesh registry            |
                    | capacity scheduler       |
                    | join-key/node auth       |
                    | Iroh Endpoint :4433/UDP  |
                    +------------+-------------+
                                 |
                   long-lived authenticated QUIC
                    /            |             \
                   /             |              \
                  v              v               v
         +---------------+ +---------------+ +---------------+
         | llmconduit    | | llmconduit    | | llmconduit    |
         | WORKER        | | WORKER        | | WORKER        |
         |               | |               | |               |
         | outbound only | | outbound only | | outbound only |
         +-------+-------+ +-------+-------+ +-------+-------+
                 |                 |                 |
              TCP local         TCP local         TCP local
                 |                 |                 |
           127.0.0.1:8000    127.0.0.1:8000    127.0.0.1:8000
                 |                 |                 |
               vLLM              vLLM              vLLM
```

The worker always initiates the network connection. Once the QUIC connection exists, the hub opens bidirectional QUIC streams on that connection for inference requests.

---

## 5. One binary, separate runtime roles

Keep a single `llmconduit` executable.

### 5.1 Hub mode

The existing `start` command remains the API server. Mesh hub capability is enabled by configuration.

Example:

```yaml
bind_addr: "0.0.0.0:4000"

mesh:
  controller:
    enabled: true
    bind_addr: "0.0.0.0:4433"
    identity_path: "/var/lib/llmconduit/controller.key"
    state_path: "/var/lib/llmconduit/mesh.sqlite"
    heartbeat_timeout_secs: 30
```

A hub may also retain ordinary HTTP upstreams. Mesh workers are an additional upstream pool, not a replacement for existing functionality.

### 5.2 Worker mode

Add a dedicated worker command, for example:

```bash
llmconduit worker --config /etc/llmconduit/worker.yaml
```

Example config:

```yaml
mesh:
  worker:
    controller_addr: "mesh.example.com:4433"
    controller_endpoint_id: "<iroh endpoint id>"
    identity_path: "/var/lib/llmconduit/worker.key"

    resources:
      - id: "primary"
        target: "127.0.0.1:8000"
        model_refresh_secs: 60

        availability:
          timezone: "America/Chicago"
          default_capacity: 0

          weekly:
            - days: [mon, tue, wed, thu, fri]
              start: "00:00"
              end: "08:00"
              capacity: 32

            - days: [mon, tue, wed, thu, fri]
              start: "08:00"
              end: "18:00"
              capacity: 4

            - days: [mon, tue, wed, thu, fri]
              start: "18:00"
              end: "00:00"
              capacity: 16
```

`worker` mode must not start the public API gateway unless explicitly configured to do so in a future extension.

---

## 6. Transport decision: direct Iroh QUIC

Use Iroh as the QUIC endpoint/identity library.

At the time of this specification, current Iroh documentation is 1.2.x and provides:

- authenticated `EndpointId` identities derived from endpoint keys;
- direct address dialing;
- bidirectional QUIC streams;
- `Connection::remote_id()`;
- endpoint connection hooks;
- relay-disabled/minimal endpoint presets.

### 6.1 Do not use Number 0 infrastructure by default

This topology has a public hub and outbound workers. It does not need relay-assisted discovery or worker-to-worker hole punching.

Build Iroh endpoints with relays and external address lookup disabled by default. Prefer the minimal preset / explicit builder configuration rather than `presets::N0`.

The worker already knows:

- controller hostname/IP;
- controller UDP port;
- controller `EndpointId`.

Resolve the hostname normally, construct an Iroh direct endpoint address using the pinned controller `EndpointId` plus resolved socket address(es), and connect directly.

This keeps the normal path:

```text
worker -> UDP/QUIC -> public hub
```

with no third-party relay dependency.

A configurable self-hosted relay fallback may be added later, but must not be necessary for v1.

### 6.2 Suggested dependency

Use a compatible current Iroh release and disable unnecessary default features where practical. Prefer the ring crypto feature and do not enable relay/test/metrics features unless required by the chosen API.

Do not blindly copy a version number from this document if the repository lockfile or current Iroh API has moved; use the current stable API available when implementing and keep the abstraction isolated under `src/mesh/`.

---

## 7. Identity model

Every hub and every worker has a persistent Iroh secret key.

### 7.1 Controller identity

The hub must load or create a persistent secret key at `controller.identity_path`.

On first creation:

- generate securely using Iroh's key API;
- write atomically;
- set owner-only permissions on Unix (`0600`);
- log/print the public `EndpointId`, never the private key.

The controller `EndpointId` is pinned by workers. DNS/IP may change without changing trust identity.

### 7.2 Worker identity

Each worker must similarly persist its own Iroh secret key.

The derived `EndpointId` is the worker's durable node identity.

If a worker restarts with the same key, it must reconnect without re-enrollment.

If the identity file is lost, that machine is a new node and must enroll again.

---

## 8. Enrollment and revocation

Separate **enrollment credentials** from **node identity**.

### 8.1 Join key semantics

A join key is a bootstrap credential only. It is not the permanent credential used for normal connections.

Join keys must support:

- enabled/disabled state;
- optional label;
- optional expiration timestamp;
- optional maximum use count;
- current use count;
- revocation at any time.

Generate at least 256 bits of cryptographically random entropy.

Suggested displayed format:

```text
llmc_join_<base64url random bytes>
```

Never store plaintext join keys server-side. Store only SHA-256 (already available in the project) or an equivalently strong digest of the random token.

Because these are high-entropy random tokens, a fast cryptographic hash is acceptable; this is not a password-derived secret.

### 8.2 Persistent authorization store

Use a small SQLite store so the running controller and CLI/admin tooling can safely share persistent state.

Suggested tables:

```sql
CREATE TABLE mesh_join_keys (
    id TEXT PRIMARY KEY,
    label TEXT,
    token_hash BLOB NOT NULL UNIQUE,
    enabled INTEGER NOT NULL,
    created_at_ms INTEGER NOT NULL,
    expires_at_ms INTEGER,
    max_uses INTEGER,
    use_count INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE mesh_nodes (
    endpoint_id TEXT PRIMARY KEY,
    label TEXT,
    enabled INTEGER NOT NULL,
    joined_at_ms INTEGER NOT NULL,
    join_key_id TEXT,
    last_seen_at_ms INTEGER,
    FOREIGN KEY(join_key_id) REFERENCES mesh_join_keys(id)
);
```

Use an async-safe wrapper. If using `rusqlite`, all blocking DB work must run under `spawn_blocking`; do not block the Tokio runtime.

### 8.3 Enrollment ALPN

Use a dedicated enrollment protocol, e.g.:

```text
llmconduit-mesh-enroll/1
```

Unknown endpoint identities are permitted to connect only using this enrollment ALPN.

Enrollment flow:

```text
worker                                           hub
  |                                               |
  | QUIC handshake, authenticated worker ID       |
  |---------------------------------------------->|
  |                                               | remote_id = worker EndpointId
  | length-prefixed EnrollRequest                  |
  |---------------------------------------------->|
  |                                               | validate join key transactionally
  |                                               | insert/enable endpoint ID
  |<----------------------------------------------|
  | EnrollAccepted                                |
  |                                               |
  | close enrollment connection                   |
```

`EnrollRequest` may include:

```rust
struct EnrollRequest {
    protocol_version: u16,
    join_key: String,
    node_name: Option<String>,
    agent_version: String,
}
```

The server must derive the enrolling identity from Iroh's authenticated `remote_id()`. Never trust a client-supplied endpoint ID field.

Join-key validation and use-count increment must occur in one transaction so a one-use key cannot be consumed twice concurrently.

Do not use Iroh 0-RTT APIs for enrollment.

### 8.4 Normal worker ALPN

Use a separate normal protocol, e.g.:

```text
llmconduit-mesh-worker/1
```

For this ALPN, only endpoint IDs present in `mesh_nodes` with `enabled = true` may be accepted.

Use Iroh's authenticated remote identity and, where practical, `EndpointHooks::after_handshake()` to reject disabled identities early.

### 8.5 Join-key revocation

Revoking a join key prevents future enrollments immediately. It does **not** revoke nodes already enrolled with that key.

### 8.6 Node revocation

Revoking a node sets `mesh_nodes.enabled = false`.

A revoked node must:

- no longer be selected for new work immediately after the controller observes the change;
- have its live QUIC connection closed as soon as practical;
- be rejected on reconnect.

Existing inference requests may be allowed to finish if the revocation mechanism is invoked as a graceful drain; provide a separate hard-disconnect path if implemented. For the initial implementation, explicit node revocation may close the connection and terminate active requests, while capacity changes must remain graceful as specified below.

---

## 9. Node and resource model

Do not model capacity directly on a model ID. Capacity belongs to an inference **resource**.

This prevents double-counting aliases or multiple model IDs served by the same vLLM process.

### 9.1 Worker

```rust
struct WorkerAdvertisement {
    node_name: Option<String>,
    agent_version: String,
    resources: Vec<ResourceAdvertisement>,
}
```

### 9.2 Resource

```rust
struct ResourceAdvertisement {
    resource_id: String,
    models: Vec<ModelAdvertisement>,
    availability: AvailabilitySchedule,
    effective_capacity: u32,
    accepting_requests: bool,
    revision: u64,
}
```

One resource maps to one configured local target such as `127.0.0.1:8000`.

The hub sends only `resource_id`. It must never be allowed to choose an arbitrary host/port on the worker.

### 9.3 Model advertisement

The worker should query its local vLLM `/v1/models` endpoint and advertise a normalized catalog using llmconduit's existing model parsing semantics where possible.

Include at minimum:

```rust
struct ModelAdvertisement {
    id: String,
    context_limit: Option<i64>,
}
```

On connect:

1. query local `/v1/models`;
2. advertise the catalog;
3. periodically refresh it (default 60 seconds, configurable);
4. send an updated advertisement only when it changes or health changes.

If local model discovery fails, mark that resource unhealthy/unavailable for new work until discovery recovers. Do not tear down the entire worker connection just because one local vLLM target is temporarily unhealthy.

---

## 10. Availability and capacity model

Capacity means **maximum concurrent in-flight inference requests** for a resource. It does not mean number of QUIC connections.

A worker keeps one long-lived QUIC connection while its capacity may change repeatedly.

Examples:

```text
00:00-08:00  capacity 32
08:00-18:00  capacity 4
18:00-00:00  capacity 16
```

### 10.1 Schedule structure

```rust
struct AvailabilitySchedule {
    timezone: String,
    default_capacity: u32,
    weekly: Vec<WeeklyCapacityWindow>,
    exceptions: Vec<CapacityException>,
}

struct WeeklyCapacityWindow {
    days: Vec<Weekday>,
    start_local: LocalTime,
    end_local: LocalTime,
    capacity: u32,
}

struct CapacityException {
    start: DateTime<FixedOffset>,
    end: DateTime<FixedOffset>,
    capacity: u32,
}
```

Use an IANA timezone (`chrono-tz` is appropriate) for recurring weekly schedules so DST behavior is explicit.

One-off exceptions use absolute RFC3339 timestamps and override weekly rules.

### 10.2 Schedule validation

At config load:

- reject invalid timezone names;
- reject empty/zero-length windows;
- reject overlapping weekly windows on the same local day unless deterministic precedence is explicitly implemented;
- reject overlapping exceptions unless deterministic precedence is explicitly implemented;
- allow a window whose end time is earlier than/equal to start time to mean it crosses midnight into the next day, but test this thoroughly;
- capacity may be zero.

### 10.3 Effective capacity

The worker is authoritative for its **effective capacity right now**.

The hub stores the full schedule for visibility, but routing uses the latest worker-advertised `effective_capacity` plus hub-local reservation state.

This avoids relying on synchronized clocks between hub and worker.

Worker schedule transitions must produce an immediate control-plane update, not wait for the next periodic heartbeat.

### 10.4 Capacity reduction is graceful drain

If capacity decreases below current in-flight requests, never kill active requests merely because of the capacity transition.

Example:

```text
17:59
configured capacity = 32
active requests      = 12

18:00
configured capacity = 4
active requests      = 12
available slots      = 0
```

No new requests may be admitted until active requests fall below 4.

Likewise, capacity `4 -> 0` means:

- continue existing requests;
- accept no new requests;
- become fully idle naturally.

### 10.5 Local worker enforcement

The hub must enforce capacity, **and the worker must independently enforce the same limit**.

This protects against stale advertisements, races, controller bugs, and concurrent scheduling decisions.

Do not implement the dynamic cap as a naïve fixed Tokio semaphore that becomes difficult to shrink correctly. Prefer a small custom `CapacityGate`:

```rust
struct CapacityGateState {
    limit: u32,
    active: u32,
}
```

Expose an atomic `try_acquire()` under a short mutex that returns an RAII permit. Dropping the permit decrements `active`.

Capacity updates only change `limit`; they do not revoke existing permits.

The controller should use the same conceptual mechanism for its local reservations.

---

## 11. Control plane

After an authorized worker establishes the normal worker ALPN connection, the worker opens one long-lived bidirectional **control stream**.

Use a simple versioned, length-prefixed JSON control protocol initially. Control-plane bandwidth is negligible and JSON is easy to debug.

Frame format:

```text
u32 big-endian length
JSON bytes
```

Enforce a strict maximum control-frame size, e.g. 64 KiB.

Suggested messages:

```rust
enum WorkerToHub {
    Hello(WorkerAdvertisement),
    Heartbeat(Heartbeat),
    ResourceUpdate(ResourceAdvertisement),
    ModelCatalogUpdate { resource_id: String, models: Vec<ModelAdvertisement>, revision: u64 },
}

enum HubToWorker {
    HelloAck { protocol_version: u16 },
    Ping { nonce: u64 },
    Close { reason: String },
}
```

A separate control-stream protocol version must be included so future evolution is explicit.

### 11.1 Heartbeats

Default heartbeat interval: approximately 10 seconds.

Heartbeat should include observational state, not inference bodies:

```rust
struct Heartbeat {
    sequence: u64,
    resources: Vec<ResourceRuntimeState>,
}

struct ResourceRuntimeState {
    resource_id: String,
    effective_capacity: u32,
    worker_active_requests: u32,
    accepting_requests: bool,
    healthy: bool,
}
```

The hub must use **hub receive time / monotonic timers** to decide staleness. Do not trust worker wall-clock timestamps for liveness.

If no heartbeat/control traffic is seen within `heartbeat_timeout_secs`, mark the worker unavailable and stop scheduling it.

### 11.2 Advertisement revisions

Every resource advertisement/update carries a monotonically increasing `revision` generated by that worker process/identity.

Ignore stale updates with lower revisions than the last accepted value for that connection.

On reconnect, a fresh `Hello` is authoritative even if the process restarted and revision counters reset; connection generation must take precedence over prior connection revision.

---

## 12. Connection registry

Add an in-memory controller registry roughly shaped like:

```rust
struct MeshRegistry {
    workers: DashMap<EndpointId, Arc<WorkerSession>>,
}

struct WorkerSession {
    endpoint_id: EndpointId,
    connection: iroh::endpoint::Connection,
    connected_at: Instant,
    last_heartbeat: Atomic/locked Instant,
    generation: u64,
    resources: ...,
}
```

Use equivalent synchronization primitives if `DashMap` is not desired; do not introduce it solely for fashion. A well-scoped `RwLock<HashMap<...>>` is acceptable if contention is low.

### 12.1 Duplicate connection rule

If the same `EndpointId` reconnects while an old session still exists, **newest authenticated connection wins**.

Register the new generation atomically, mark the old session draining/closed, and ensure new requests cannot race onto the old connection after replacement.

This is important during network changes and controller-side connection recovery.

---

## 13. Scheduler

The scheduler operates on resources, not just nodes.

For a request with resolved model `M`, candidate resources must satisfy all of:

1. node is authorized and enabled;
2. current worker session is connected and heartbeat-fresh;
3. resource is healthy;
4. resource advertises model `M` under llmconduit's normal model normalization semantics;
5. `effective_capacity > 0`;
6. hub-local `in_flight < effective_capacity`;
7. resource is accepting requests.

### 13.1 Selection policy

Initial policy: **lowest utilization**, which naturally distributes traffic roughly proportional to declared capacity.

Compare:

```text
in_flight / effective_capacity
```

Avoid unnecessary floating-point math; cross-multiplication is sufficient when ordering candidates.

Tie-break with round-robin or a rotating index so one equal candidate is not always favored.

Example:

```text
worker A: capacity 32, active 8   -> 25% utilized
worker B: capacity 4,  active 2   -> 50% utilized

choose A
```

Do not use raw free-slot count alone; a 32-slot node should receive proportionally more work than a 4-slot node.

### 13.2 Reservation must be atomic

Do not select a candidate based only on a stale snapshot and increment later.

The scheduler should:

1. rank candidate snapshots;
2. call `try_reserve()` on the first candidate;
3. if reservation loses a race, try the next candidate;
4. keep the returned RAII reservation permit for the entire request lifetime;
5. release on completion, error, cancellation, stream reset, or connection loss.

### 13.3 No central queue in v1

If no candidate can reserve capacity, return a retryable service-unavailable error rather than accumulating an unbounded central queue.

Suggested HTTP behavior at the public API edge:

- `503 Service Unavailable`;
- a stable internal error class such as `mesh_capacity_exhausted`;
- optional `Retry-After: 1`.

Do not mark a healthy worker as failed/cooling merely because it is at capacity.

---

## 14. Model catalog behavior

The mesh pool owns a dynamic model catalog built from current worker advertisements.

Do not poll every remote worker from the hub for `/v1/models`; workers advertise their local catalogs.

The mesh catalog should be the union of model IDs from connected, authorized, healthy worker resources.

**Do not remove a model merely because all slots are momentarily busy.** Capacity saturation is load state, not model disappearance.

If a resource remains connected but has a scheduled capacity of zero, it is acceptable for its model to remain in the mesh catalog; requests will return no-capacity unless another resource can serve them. This avoids `/v1/models` flapping every time availability or load changes.

If all workers that advertised a model disconnect or mark the resource unhealthy/remove the model, remove it from the mesh catalog.

Preserve context-limit metadata where advertised.

---

## 15. Upstream integration strategy

### 15.1 Do not duplicate the gateway engine

The mesh must enter the existing gateway through the upstream seam.

Create a mesh upstream client, e.g.:

```rust
pub struct MeshUpstreamClient {
    registry: Arc<MeshRegistry>,
    scheduler: Arc<MeshScheduler>,
    // existing finalization/log/capture dependencies as needed
}
```

It should implement `UpstreamClient` or a refactored equivalent while preserving existing routing, failover, capture, and canonical-conversion semantics.

### 15.2 Required refactor: remove concrete Reqwest leaves from routing/failover structs

At the reviewed baseline, `FailoverUpstreamProvider.client` and `RoutingUpstreamProvider.primary_client` are concrete `ReqwestUpstreamClient`s.

Refactor those leaf references to an abstract upstream client so a mesh pool can participate as an upstream without duplicating routing logic.

Preferred direction, consistent with the repository's existing trait-object convention:

```rust
type DynUpstreamClient = Arc<dyn UpstreamClient>;

struct FailoverUpstreamProvider {
    ...
    client: DynUpstreamClient,
}

struct RoutingUpstreamProvider {
    ...
    primary_client: DynUpstreamClient,
    ...
}
```

If `Debug` derives become impossible because of trait objects, implement focused manual `Debug` output or remove unnecessary derives rather than abandoning the abstraction.

Update constructors/tests accordingly.

### 15.3 Do not force mesh into fake Reqwest types

`UpstreamClient::list_models()` currently returns `reqwest::Response`, which is an HTTP-leaf implementation detail and awkward for a synthetic dynamic mesh catalog.

Refactor this seam cleanly rather than constructing fake `reqwest::Response` objects just to satisfy the trait.

A reasonable approach is a transport-neutral models DTO, e.g.:

```rust
struct UpstreamModelsResponse {
    status: StatusCode,
    headers: HeaderMap,
    body: Bytes,
}
```

or split catalog semantics from raw HTTP response semantics so:

- ordinary Reqwest providers can still preserve ETag/headers where currently supported;
- mesh can synthesize the union catalog directly;
- `supported_model_catalog()` remains authoritative for routing/context metadata.

Keep the refactor as narrow as possible and lock behavior with existing tests.

### 15.4 Preserve finalization at the true leaf

Current `ReqwestUpstreamClient` intentionally finalizes model-specific policies only after routing/failover selects the final backend model.

The mesh implementation must preserve that invariant.

Do not finalize once before worker selection and then accidentally select a worker serving a different remapped backend model.

Where possible, extract shared leaf behavior from `ReqwestUpstreamClient` into transport-neutral helpers used by both direct HTTP and mesh leaves:

- backend request finalization;
- content sanitization/flattening;
- upstream request logging/capture;
- context-overflow shrink/retry behavior;
- response status classification;
- SSE frame limits and parsing;
- first-chunk semantics;
- served-model/provider metrics.

The worker must not perform those transformations a second time.

---

## 16. Mesh request data plane

### 16.1 One QUIC stream per inference request

For every request selected for a worker resource:

```text
hub MeshUpstreamClient
    |
    | worker.connection.open_bi()
    v
QUIC bidirectional request stream
    |
    v
worker
    |
    | TcpStream::connect(configured resource target)
    v
local vLLM
```

Concurrent requests use independent QUIC streams on the same connection.

### 16.2 Worker should not parse inference JSON

The worker's request path should be effectively byte forwarding after a tiny mesh stream preface.

Do not deserialize OpenAI/Responses/Anthropic request bodies on the worker.

All API normalization happens once on the hub, as it does today.

### 16.3 Request stream preface

The hub needs to identify the target worker resource without allowing arbitrary destination selection.

At the start of each server-opened request stream, send a small bounded preface, e.g.:

```rust
struct RequestOpen {
    protocol_version: u16,
    request_id: Uuid,
    resource_id: String,
}
```

Frame it with a fixed 4-byte big-endian length and cap the size (e.g. 4 KiB).

After the preface, the stream switches to HTTP/1.1 bytes destined for the local vLLM TCP connection.

### 16.4 Worker capacity guard without adding an accepted-path RTT

The worker must re-check local capacity when it receives a `RequestOpen`.

However, do **not** add a synchronous request/ack round trip that prevents the hub from transmitting the HTTP request body until a capacity acknowledgment returns. That would add avoidable latency for large prompts and high-RTT workers.

Implement an optimistic admission shim:

1. Hub writes `RequestOpen`.
2. Hub immediately begins the HTTP/1.1 request over the same stream.
3. Worker reads `RequestOpen`, checks local capacity, and immediately writes a tiny admission status before any raw HTTP response bytes.
4. A hub-side stream wrapper strips this admission status from the receive side before exposing the stream to the HTTP parser.
5. The write side is not blocked waiting for the admission byte.

Possible admission frame:

```rust
enum Admission {
    Accepted,
    Rejected { code: AdmissionRejectCode },
}
```

On `Accepted`, the hub's receive wrapper becomes transparent and the next bytes are the raw HTTP response from vLLM.

On `Rejected`, fail that candidate before any user-visible response chunk and try another eligible worker.

This preserves local enforcement without intentionally adding a new request RTT.

If the chosen HTTP client implementation makes this unnecessarily complex, a simpler preflight acknowledgment may be used for the first prototype, **but benchmark it and remove the extra RTT before considering the performance work complete**.

### 16.5 HTTP-over-stream implementation

Prefer a real HTTP/1.1 parser/client on the hub side rather than hand-rolling HTTP parsing.

A good direction is:

- combine Iroh send/receive halves into an AsyncRead/AsyncWrite adapter;
- use Hyper HTTP/1 client-connection APIs over that adapter;
- send exactly one inference HTTP request on each QUIC stream;
- stream the response body without buffering it in full.

The worker can connect a Tokio TCP stream to the configured vLLM target and copy bytes bidirectionally after the mesh preface/admission handling.

Add direct `hyper` / `hyper-util` dependencies if needed rather than relying on transitive internals.

### 16.6 Request paths

At minimum, mesh v1 must support the path used by normal gateway inference:

```text
POST /v1/chat/completions
```

because Responses/Anthropic traffic is already lowered through the existing gateway into the chat-completions upstream path.

Also support worker-side local model discovery:

```text
GET /v1/models
```

Legacy `/v1/completions` and optional `/tokenize` can be added to the same generic HTTP-over-stream transport once the main path is stable. Do not regress direct HTTP support for them.

---

## 17. Streaming, cancellation, and backpressure

These are mandatory correctness requirements.

### 17.1 No full response buffering

SSE/token output from vLLM must flow:

```text
vLLM -> local TCP -> worker -> QUIC stream -> hub -> existing SSE parser -> caller
```

without accumulating the full completion anywhere.

### 17.2 Cancellation propagation

The existing gateway deliberately cancels upstream work when the caller disconnects.

The mesh path must preserve this behavior.

When the hub-side request future/stream is dropped or cancelled:

- stop reading/writing the request QUIC stream;
- reset/stop the QUIC stream as appropriate;
- worker copy task must observe EOF/reset and close the local vLLM TCP connection;
- both hub and worker capacity permits must be released.

Add a test proving a cancelled public client causes the worker's local upstream connection to close.

### 17.3 Backpressure

Use Tokio/QUIC stream backpressure naturally. Do not create unbounded channels containing prompt or response bytes.

Control-plane channels may be bounded because messages are small.

### 17.4 Mid-stream failure

Never retry a request on another worker after user-visible response content has begun.

Keep the repository's existing pre-first-chunk failover invariant.

If a worker/QUIC path fails before the first upstream chunk, another mesh candidate may be attempted.

If it fails after streaming begins, surface the error and release capacity.

---

## 18. Worker request handling

For every incoming hub-opened request stream:

1. read and validate bounded `RequestOpen` preface;
2. resolve `resource_id` to a locally configured resource;
3. verify resource health and current effective capacity;
4. `try_acquire()` a local capacity permit;
5. send admission result;
6. if accepted, connect only to that resource's preconfigured `target`;
7. forward the HTTP byte stream bidirectionally;
8. on completion/error/reset, drop capacity permit;
9. update heartbeat/runtime counters.

Never honor an arbitrary host, IP, path prefix, or port supplied by the hub.

---

## 19. Failure classes and scheduling semantics

The mesh needs explicit failure classes so normal capacity/load events do not poison provider health.

Suggested internal classes:

```rust
enum MeshAttemptError {
    CapacityExhausted,
    WorkerDisconnected,
    ResourceUnhealthy,
    UnknownResource,
    LocalConnectFailed,
    ProtocolError,
    Timeout,
    UpstreamHttpStatus(StatusCode),
}
```

Behavior:

- `CapacityExhausted`: try another mesh worker; no cooldown/failure penalty.
- `WorkerDisconnected`: remove/mark stale session; try another worker pre-first-chunk.
- `ResourceUnhealthy`: skip resource; no global node penalty if other resources work.
- `UnknownResource`: registry/session race; refresh and try another resource; log warning.
- `LocalConnectFailed`: mark resource unhealthy/backoff; try another worker pre-first-chunk.
- `ProtocolError`: close worker connection; likely node/session failure.
- `Timeout`: normal failover semantics pre-first-chunk.
- upstream request-intrinsic 4xx: preserve existing no-cooldown behavior.
- mid-stream errors: no retry.

Use existing `FailoverDisposition` where it fits instead of inventing a parallel incompatible failover system.

---

## 20. Health behavior

Track health separately for:

- QUIC worker session;
- control heartbeat freshness;
- individual resource/vLLM health;
- capacity availability;
- model catalog availability.

Do not equate `capacity == 0` with unhealthy.

A resource may be:

```text
healthy + scheduled unavailable
healthy + saturated
healthy + accepting
unhealthy
```

Those distinctions should remain available to scheduling/observability.

---

## 21. CLI requirements

Add a focused mesh CLI namespace rather than many unrelated top-level commands if that fits Clap cleanly.

Suggested UX:

```bash
# Run a worker sidecar
llmconduit worker --config /etc/llmconduit/worker.yaml

# Show controller identity/address information
llmconduit mesh info --config /etc/llmconduit/config.yaml

# Create join token
llmconduit mesh join-key create \
  --label community \
  --max-uses 20 \
  --expires-in 7d

# List/revoke join keys
llmconduit mesh join-key list
llmconduit mesh join-key revoke <key-id>

# List known nodes
llmconduit mesh node list

# Revoke/re-enable a node
llmconduit mesh node revoke <endpoint-id>
llmconduit mesh node enable <endpoint-id>
```

The CLI must never print stored plaintext join keys because they do not exist after creation. Print the token exactly once at creation time.

Worker enrollment should support a first-run token from an environment variable or CLI argument, e.g.:

```text
LLMCONDUIT_MESH_JOIN_KEY
```

Do not persist the plaintext token into the normal YAML config.

If the worker is already enrolled (persistent identity accepted by hub), ignore the join key for normal operation.

---

## 22. Configuration requirements

Add config types under the existing config system without disturbing existing defaults.

Suggested shape:

```yaml
mesh:
  controller:
    enabled: false
    bind_addr: "0.0.0.0:4433"
    identity_path: null
    state_path: null
    heartbeat_timeout_secs: 30

  worker:
    controller_addr: null
    controller_endpoint_id: null
    identity_path: null
    heartbeat_interval_secs: 10
    resources: []
```

Normal gateway configs with no `mesh` section must behave exactly as before.

### 22.1 Secrets

Follow the repository's existing rule against casually persisting secrets in debug-printable config structs.

Join tokens should come from one-shot CLI input or environment variables.

Iroh private keys are secret files, not inline YAML values.

---

## 23. Optional runtime capacity override

This is desirable but may be phase 2 if it meaningfully delays the core transport.

Desired semantics:

```text
scheduled capacity
       -> optional temporary override
       -> effective capacity
```

Examples:

```bash
llmconduit mesh capacity set primary 4
llmconduit mesh capacity drain primary
llmconduit mesh capacity restore primary
```

A runtime override may optionally expire at a specified time.

If implemented, it should use a local worker control socket or another secure local-only mechanism. Do not expose an unauthenticated public control endpoint on the worker.

A drain override sets effective capacity to 0 but lets active requests finish.

---

## 24. Observability

Add structured tracing fields for mesh activity without logging prompts, completions, join tokens, or private keys.

Useful fields:

```text
endpoint_id
node_name
resource_id
model
connection_generation
effective_capacity
hub_in_flight
worker_in_flight
available_slots
request_id
mesh_attempt
```

Log important state transitions:

- worker connected/disconnected;
- enrollment accepted/rejected (without token);
- node revoked;
- model catalog changed;
- capacity changed;
- resource became healthy/unhealthy;
- worker heartbeat timed out;
- duplicate session replaced;
- request rerouted before first chunk.

### 24.1 Dashboard integration

Core mesh transport does not need to block on dashboard work.

After core functionality is stable, add mesh state as a **separate dashboard topology domain** rather than overloading existing `ProviderHealth` fields if doing so would break the current strict frontend schema.

Remember the repository rule that topology/flow/metrics domains must keep independent sequence cursors.

Desired eventual view:

```text
NODE        RESOURCE   MODEL           STATE       CAP   ACTIVE  FREE
node-a      primary    Qwen3.5-27B     serving      32      18    14
node-b      primary    Qwen3.5-27B     saturated     4       4     0
node-c      primary    GLM-5.3         scheduled     0       2     0
node-d      primary    GLM-5.3         offline       8       0     0
```

---

## 25. Source layout

Suggested new module layout:

```text
src/
  mesh/
    mod.rs
    protocol.rs       # ALPN constants, control/request framing
    identity.rs       # persistent Iroh key load/create
    auth.rs           # join keys + node auth
    store.rs          # SQLite persistence abstraction
    controller.rs     # Iroh accept loop, session lifecycle
    worker.rs         # outbound sidecar lifecycle
    registry.rs       # connected workers/resources/catalog state
    capacity.rs       # schedules + CapacityGate
    scheduler.rs      # model/resource candidate selection/reservations
    io.rs             # QUIC bidi stream AsyncRead/AsyncWrite adapter
    upstream.rs       # MeshUpstreamClient
```

Existing files likely touched:

```text
Cargo.toml
src/main.rs
src/cli.rs
src/config.rs
src/lib.rs
src/upstream.rs
src/error.rs               # only if stable mesh error taxonomy needs additions
src/dashboard_*            # later/optional phase
```

Tests:

```text
tests/mesh.rs              # focused mesh integration suite is justified
```

Keep ordinary adapter/engine files unchanged unless a real abstraction boundary requires it.

---

## 26. Concurrency model

Controller tasks:

```text
Iroh accept loop
  -> one task per worker connection
       -> control-stream reader task
       -> connection-closed watcher

public request
  -> existing gateway task
       -> MeshUpstreamClient
            -> scheduler reservation
            -> QUIC request stream
            -> streaming response
```

Worker tasks:

```text
worker supervisor
  -> connect/reconnect loop
  -> control heartbeat/update task
  -> schedule-boundary timer
  -> model-refresh task per resource
  -> accept hub-opened request streams
       -> one task per request
            -> local CapacityGate permit
            -> local TCP connect
            -> bidirectional forwarding
```

All spawned tasks must have explicit shutdown behavior. Do not leave detached tasks retaining stale sessions/resources after reconnect.

---

## 27. Reconnect behavior

Worker reconnect loop:

- immediate retry for first transient failure is acceptable;
- exponential backoff with jitter thereafter;
- cap around 30 seconds;
- reset backoff after a stable connection interval.

On controller restart, workers reconnect using the same endpoint identities.

On worker reconnect:

1. authenticate normal ALPN;
2. send full `Hello` advertisement;
3. controller installs a new connection generation;
4. controller closes/replaces any old session for the endpoint ID;
5. scheduler sees new resource state.

Do not require re-enrollment after ordinary connection loss.

---

## 28. Protocol limits and hardening

Set explicit limits, with tests:

- control frame <= 64 KiB;
- request preface <= 4 KiB;
- node name <= 128 bytes;
- resource ID <= 128 bytes;
- sane max resources per node (e.g. 32);
- sane max models per resource (e.g. 1024);
- maximum configured capacity per resource (choose a high but bounded value, e.g. 65,535);
- malformed JSON/control frames close that session;
- unknown protocol version is rejected cleanly;
- invalid UTF-8 where strings are required is rejected;
- no arbitrary local destination from remote input;
- no plaintext join-token logging;
- no secret key logging;
- no unbounded body/channel buffering introduced by mesh code.

---

## 29. Test plan

### 29.1 Identity/auth tests

- controller identity persists across restart;
- worker identity persists across restart;
- valid join key enrolls unknown endpoint;
- invalid join key rejected;
- revoked join key rejected;
- expired join key rejected;
- max-use key stops at exact use count under concurrent enrollment attempts;
- enrolled endpoint reconnects without token;
- disabled/revoked endpoint cannot normal-connect;
- one node cannot claim another node's endpoint ID;
- duplicate connection newest-wins behavior.

### 29.2 Availability/capacity tests

Use paused Tokio time where practical.

- default capacity outside all windows;
- weekly window capacity;
- cross-midnight window;
- DST transition behavior in an IANA timezone;
- exception overrides weekly schedule;
- invalid overlap rejected;
- capacity transition sends update immediately;
- capacity `32 -> 4` with 12 active requests kills none and admits none until active < 4;
- capacity `4 -> 0` drains gracefully;
- capacity increase immediately opens new slots;
- concurrent `try_acquire()` never exceeds limit;
- permit release occurs on success, error, cancellation, and connection loss.

### 29.3 Scheduler tests

- only nodes supporting requested model are candidates;
- unhealthy/stale/revoked nodes are excluded;
- effective capacity 0 excluded;
- saturated nodes excluded;
- lowest-utilization routing is capacity-proportional;
- tie-breaking does not permanently favor first node;
- reservation race retries another node;
- all saturated -> stable 503/no-capacity result;
- capacity exhaustion does not increment provider failure/cooldown counters.

### 29.4 Data-plane tests

Use an in-process fake vLLM HTTP server.

- remote `/v1/chat/completions` request arrives byte-equivalent after hub finalization;
- streaming SSE arrives incrementally;
- large prompt is streamed/sent without an artificial capacity-preflight RTT in the final optimized implementation;
- simultaneous streams do not head-of-line block each other at application layer;
- worker never parses/re-serializes inference JSON;
- worker only connects to configured resource target;
- local vLLM connect failure retries another mesh worker pre-first-chunk;
- worker disconnect pre-first-chunk retries another worker;
- worker disconnect mid-stream does not retry;
- public caller cancellation resets remote stream and closes worker->vLLM TCP;
- hub and worker capacity counters return to zero after every terminal path.

### 29.5 Model catalog tests

- worker advertises local `/v1/models` result;
- multiple workers produce union catalog;
- duplicate model IDs appear once;
- context limit propagated;
- saturation does not remove model from catalog;
- scheduled capacity 0 does not necessarily remove model from catalog;
- disconnected/unhealthy last provider removes model;
- model change is reflected after refresh/update.

### 29.6 Existing regression tests

Every existing gateway test must remain green, especially:

- model routing;
- fallback behavior;
- pre-first-chunk failover;
- context shrink/retry;
- cancellation;
- SSE limits;
- model normalization;
- provider health snapshots;
- raw Responses/Chat/Anthropic conversions.

---

## 30. Performance requirements and benchmarks

The design goal is not just correctness; this feature exists specifically to avoid high-overhead tunnels.

### 30.1 Architectural performance invariants

The final path must satisfy:

- one persistent QUIC connection per worker;
- one QUIC bidi stream per inference request;
- no TCP-over-TCP tunnel;
- no SSH framing;
- no VPN stack;
- no worker inference JSON parsing;
- no worker SSE parsing;
- no full response buffering;
- no relay in normal path;
- no additional accepted-path request/ack RTT before prompt transmission;
- capacity scheduling uses in-memory state only on request hot path;
- persistent auth DB is not queried for every token/chunk.

### 30.2 Benchmark cases

Add a small reproducible benchmark harness or documented manual benchmark comparing:

1. direct hub -> vLLM HTTP baseline;
2. hub -> local worker over Iroh -> vLLM;
3. hub -> simulated 50 ms RTT worker;
4. 100 concurrent streaming requests over one worker connection;
5. packet loss / WAN emulation if easy to reproduce with `tc netem`.

Measure:

- time from dispatch to worker local TCP write;
- TTFT delta versus direct baseline;
- response token inter-arrival jitter;
- prompt upload throughput;
- aggregate streaming throughput;
- hub CPU;
- worker CPU;
- memory per active stream.

Do not set an arbitrary microsecond target before measuring. The acceptance criterion is that mesh adds no avoidable protocol round trip and no obvious serialization/proxy bottleneck beyond QUIC encryption/framing and one worker-local TCP hop.

---

## 31. Phased implementation order

Codex should implement in this order and keep commits/changes reviewable.

### Phase 1: abstractions and identity

1. Read `AGENTS.md` and run baseline tests.
2. Add `src/mesh/` skeleton and protocol constants.
3. Add Iroh dependency with direct/minimal endpoint setup.
4. Add persistent controller/worker identity helpers.
5. Add unit tests for identity persistence/permissions.

**Stop condition:** project builds and all old tests still pass.

### Phase 2: enrollment/auth store

1. Add persistent auth store.
2. Add join-key generation/list/revoke CLI.
3. Add node records.
4. Implement enrollment ALPN.
5. Implement worker normal ALPN authorization.
6. Test invalid/revoked/expired/max-use behavior.

**Stop condition:** unknown node can enroll once and then reconnect by EndpointId without join token.

### Phase 3: worker registry/control plane

1. Implement normal worker connection.
2. Add `Hello`, heartbeat, resource/model advertisements.
3. Add registry/session generations.
4. Add duplicate-session replacement.
5. Add liveness timeout.

**Stop condition:** hub can reliably list connected workers/resources/models and detects disconnects.

### Phase 4: availability/capacity

1. Add schedule config/parser.
2. Add IANA timezone evaluation.
3. Add schedule-boundary task.
4. Add worker CapacityGate.
5. Add hub CapacityGate/reservation state.
6. Add capacity update messages.
7. Add scheduler and tests.

**Stop condition:** a worker can move between 32/4/0 capacity without reconnecting, and active requests drain correctly.

### Phase 5: request data path

1. Implement request stream preface.
2. Implement Iroh stream I/O adapter.
3. Implement worker local TCP forwarding.
4. Implement hub HTTP-over-QUIC request/response handling.
5. Preserve streaming and cancellation.
6. Add local admission enforcement.
7. Remove any extra admission RTT from the accepted path.

**Stop condition:** hub can stream a real/fake vLLM chat completion through a NAT-style worker connection with no second API translation layer.

### Phase 6: upstream integration

1. Refactor concrete Reqwest leaves in routing/failover to abstract clients.
2. Refactor models response seam away from Reqwest-only types as needed.
3. Implement `MeshUpstreamClient`.
4. Integrate mesh catalog into model routing.
5. Preserve leaf finalization, failover, logging, capture, and metrics semantics.

**Stop condition:** public `/v1/responses`, `/v1/chat/completions`, and `/v1/messages` can select remote mesh workers through the normal gateway.

### Phase 7: resilience and performance

1. reconnect/backoff;
2. cancellation/reset hardening;
3. resource-health backoff;
4. protocol limits;
5. benchmark direct vs mesh;
6. optimize hot path based on measurements.

### Phase 8: optional UX

1. runtime capacity override/drain commands;
2. dashboard mesh topology;
3. admin mutations in dashboard if desired;
4. optional self-hosted relay fallback.

Do not make Phase 8 a blocker for core merge unless explicitly requested.

---

## 32. Acceptance criteria

The feature is ready when all of the following are true:

1. The same `llmconduit` binary can run as hub or worker sidecar.
2. A worker behind NAT needs only outbound UDP connectivity to the public controller.
3. No external Iroh relay service is required in the normal configuration.
4. Controller identity is pinned by worker `EndpointId`.
5. Worker identity persists across restart.
6. A revocable join key can enroll a new worker.
7. Revoking a join key prevents future enrollment without disabling existing nodes.
8. Revoking a node prevents future use/reconnect.
9. Worker advertises one or more vLLM resources and model catalogs.
10. Worker can advertise scheduled capacities such as 32 during one period and 4 during another.
11. Capacity changes do not require reconnecting QUIC.
12. Capacity decreases drain existing requests rather than terminating them.
13. Hub never intentionally schedules above advertised effective capacity.
14. Worker independently refuses admission above local capacity.
15. A worker's capacity is measured in concurrent inference requests, not QUIC connections.
16. Hub routes proportionally/fairly using utilization-aware scheduling.
17. Saturation produces a retryable no-capacity result, not a provider-health failure.
18. Each inference request uses an independent QUIC bidi stream.
19. Worker does not parse/re-serialize inference JSON or SSE.
20. Streaming responses remain streaming.
21. Caller cancellation closes the corresponding remote vLLM request path.
22. No retry occurs after the first response chunk is exposed.
23. Existing static HTTP upstreams and existing routing/failover behavior remain compatible.
24. Existing test suite passes.
25. New mesh integration tests pass under concurrency.
26. Final accepted request path has no extra capacity preflight RTT before prompt transmission.
27. Benchmarks demonstrate the mesh overhead is primarily QUIC + WAN + one local TCP hop, with no obvious software bottleneck from the new layer.

---

## 33. Important implementation cautions

### Do not treat every Iroh feature as necessary

This is not a general peer mesh. The controller is public and known. Direct outbound worker connections are the expected path.

### Do not let worker mode become another full gateway

Worker mode is intentionally thin. API conversion/routing/policy occurs at the hub.

### Do not make capacity advertisement advisory only

Capacity is a hard scheduling contract, enforced independently on both sides.

### Do not tie capacity to model aliases

Capacity belongs to a resource/vLLM process.

### Do not query SQLite on the inference hot path

Load authorized node state into the live registry and update it on mutations/rechecks. Persistent storage is for durable control-plane state.

### Do not retry after streaming starts

Preserve llmconduit's existing first-chunk failover invariant.

### Do not break existing provider semantics during abstraction refactor

The current `ReqwestUpstreamClient` carries a large amount of subtle behavior. Extract reusable leaf behavior rather than rewriting it from scratch unless tests prove equivalence.

### Do not add a public arbitrary TCP tunnel

The hub chooses only a worker-defined `resource_id`; the worker maps that to a local configured target.

---

## 34. Reference implementation shape

Conceptually, the final hot path should look like this:

```text
Public request
      |
      v
existing llmconduit Gateway
      |
      v
model/routing resolution
      |
      v
MeshUpstreamClient
      |
      | choose model-compatible resource
      | atomically reserve capacity
      v
WorkerSession::connection.open_bi()
      |
      | RequestOpen(resource_id)
      | HTTP/1.1 request bytes
      v
------------------- QUIC -------------------
      v
worker request handler
      |
      | local capacity permit
      | TcpStream::connect(configured target)
      v
vLLM /v1/chat/completions
      |
      | streaming HTTP/SSE response
      v
worker raw byte forwarding
      |
------------------- QUIC -------------------
      v
hub HTTP response stream
      |
existing llmconduit SSE parser
      |
existing canonical output conversion
      v
Public client
```

Control plane remains separate:

```text
worker <==== one persistent QUIC connection ====> hub
   |
   +-- one long-lived control stream
   |     - Hello
   |     - model catalogs
   |     - availability schedule
   |     - capacity updates
   |     - heartbeat/health
   |
   +-- request stream N
   +-- request stream N+1
   +-- request stream N+2
   +-- ...
```

---

## 35. Current-source references used for this specification

Codex should inspect the live repository rather than relying only on these snapshots:

- Repository: https://github.com/local-inference-lab/llmconduit
- Repo agent guidance: https://github.com/local-inference-lab/llmconduit/blob/master/AGENTS.md
- Current upstream implementation: https://github.com/local-inference-lab/llmconduit/blob/master/src/upstream.rs
- Current DI root: https://github.com/local-inference-lab/llmconduit/blob/master/src/lib.rs
- Current CLI: https://github.com/local-inference-lab/llmconduit/blob/master/src/cli.rs
- Current main: https://github.com/local-inference-lab/llmconduit/blob/master/src/main.rs
- Current Cargo manifest: https://github.com/local-inference-lab/llmconduit/blob/master/Cargo.toml
- Iroh docs: https://docs.rs/iroh/latest/iroh/

As of this spec, Iroh documents direct address connections, mutually authenticated `EndpointId`s, cheap concurrent bidirectional QUIC streams, relay-disabled endpoint construction, and post-handshake authorization hooks. Use the current supported APIs when implementing.

---

## 36. Codex handoff instruction

Implement this feature incrementally against the current repository rather than as a standalone prototype. Start by reading `AGENTS.md`, running the baseline suite, and mapping the current `UpstreamClient`/routing code before editing it.

Prefer small, test-backed abstraction changes over a large rewrite. Preserve all existing gateway semantics. The performance-critical end state is a direct Iroh QUIC worker connection with one stream per request and a worker that performs only bounded mesh framing, local capacity admission, and raw HTTP/TCP forwarding to vLLM.

When implementation details in this spec conflict with a newer library API, preserve the **behavioral requirements** and adapt the code to the current stable API rather than forcing obsolete signatures.
