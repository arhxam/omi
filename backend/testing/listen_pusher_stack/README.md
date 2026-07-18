# Local listen → pusher stack gauntlet

Run this explicit, local-only gauntlet when changing the listen WebSocket,
`ListenPusherSession`, pusher opcode handling, or finalization lifecycle:

```bash
backend/testing/listen_pusher_stack/run.sh --keep
```

For focused local iteration, append `--suite durable` to run only the REST →
outbox → restarted-worker proof, or `--suite inline` for the existing live
listen/pusher scenarios. Blocking CI uses the default `all` suite.

Prerequisites are the backend virtual environment (`backend/scripts/sync-python-deps.sh`),
the root Node dependencies (`npm ci`), Redis, and Java 21+. The runner discovers
Homebrew's `openjdk@21` automatically when `java` is not already on `PATH` and
chooses a per-run Firestore emulator port, so it does not conflict with shared
developer services.

It runs two isolated stacks while Firebase's command owns fresh Firestore
emulator state: the original live listen → pusher path, then an independent
durable REST-finalization path. Each stack starts Redis and local ASGI
processes:

```text
native /v4/listen client → real backend → real pusher
                               ↘ real Parakeet WS client → local protocol stub

REST /v1/conversations/{id}/finalize → real Firestore outbox → recorded Cloud Tasks proto
                                                            ↘ restarted backend → real protected task worker
```

The child-process environment is allowlisted: it has a private empty
`HOME`/cloud config directory and receives no provider credentials, developer
proxies, ADC configuration, or production project settings. The harness also
rejects a non-loopback Firestore endpoint.

The test deliberately exercises the production listen runtime (`main:app`),
real pusher router, binary frames 101/102/103/104/201, Firestore finalization
jobs, leases, fanout admission/idempotency, recording-session binding, and
reconnect code.
It seeds private-cloud mode only to make the real 103 + 101 audio frames flow;
the provider/storage leaves are disabled because this harness has no cloud
credentials.

The pusher entrypoint replaces only these provider-side leaves:

- conversation LLM processing;
- memory extraction;
- external-integration delivery.
- private-cloud audio storage (the queue and 101/103 frame handling remain real).

For the durable REST scenario, a separate test-only backend entrypoint also
replaces remote Cloud Tasks creation/JWKS verification with loopback seams. It
records the task proto, emulates named-task deduplication across a backend
restart, and makes an actual authenticated HTTP request to the production task
worker. It does not replace the REST route, Firestore outbox transaction,
opaque payload parser, Redis run lock, job lease, lifecycle owner, or durable
fanout claim/completion.

The real finalizer still persists through the lifecycle owner, claims and
completes durable fanout, and sends the real pusher result frame. It does not
prove LLM/vector output quality, GCS delivery, or downstream integration
delivery. Trace files record frame metadata and byte counts, never audio or
transcript text.

Scenarios:

1. audio → streaming segment → persisted content → close → durable completed job;
2. completed native UUID reconnect replays the terminal binding without a new job;
3. a stale empty desktop recording is removed by the next-session lifecycle path and creates no job;
4. a pusher process loses the first 104 before claim, is restarted, and the
   live backend session replays the same job ID and dispatch generation exactly once.
5. live audio content is finalized through `POST /v1/conversations/{id}/finalize`;
   the generated named Cloud Tasks wake-up contains exactly `{job_id,
   dispatch_generation}`, no user or conversation ID, and the customer status
   projection stays queued before delivery;
6. an unauthenticated task request is rejected without claiming the job; after
   a real backend restart, the authenticated worker completes the same job and
   durable fanout once, while an at-least-once duplicate delivery is safely
   acknowledged without repeating any provider-side leaf.

This complements, rather than replaces, the storage race test:

```bash
npm run test:listen-lifecycle:emulator
```

It intentionally does not test real Parakeet inference, LLM/vector quality,
GCS, external integration delivery, Google Cloud Tasks service availability,
or production OIDC/JWKS. Those require their own environment and should not
turn this deterministic local failure test into a credentialed integration
suite.
