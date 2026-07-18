# Local Sync Cloud Tasks stack gauntlet

Run this explicit, loopback-only gauntlet when changing Sync v2 admission,
Cloud Tasks dispatch, worker ownership/retry, staged-audio handling, or the
Sync content ledger:

```bash
backend/testing/sync_cloud_tasks_stack/run.sh --keep
```

Prerequisites are the backend virtual environment
(`backend/scripts/sync-python-deps.sh`), root Node dependencies (`npm ci`),
Redis, and Java 21+ for the Firestore emulator. The runner gives Firebase a
fresh loopback port, starts a private loopback Redis, and gives every child an
empty home/cloud-config directory. It does not inherit cloud credentials,
provider keys, proxies, or a developer Redis/Firestore endpoint. The ASGI
wrapper rejects non-loopback DNS and sockets as an additional no-egress guard.

The exercised path is:

```text
multipart PCM v2 upload → real Sync admission → local GCS staging leaf
  → real tasks_v2.Task construction → captured named task
  → real OIDC-protected /v2/sync-jobs/run HTTP delivery
  → real worker ledger/lease/decode/VAD-segmentation pipeline
  → deterministic STT + conversation-processor leaves
  → durable Redis job, Firestore ledger, and conversation
```

Before each upload, the runner seeds the minimal existing server-capture
record using the production conversation-storage encoder, then calls the real
`/v2/sync-capture-manifest` endpoint with the actual file SHA-256 and device
identity. The upload presents that manifest, so the route takes the real
**fresh**, device-bound admission branch rather than a test-only lane override.
The worker therefore exercises the production auto-sync merge/reprocess path
for that capture conversation.

The Cloud Tasks recorder receives and validates a real `tasks_v2.Task` object.
It asserts its queue, loopback handler path, exact OIDC audience/service-account
identity, schema version, raw-blob count, 1500-second dispatch deadline, and the
fresh device identity/trust/recording-age fields. The runner first proves that
an unauthenticated delivery is rejected without changing the queued job, then
posts the exact captured payload to the real worker route using a local token
accepted by the production OIDC dependency. It verifies:

1. a first delivery that fails after terminalization preserves staged material;
   a retry ACKs and cleans it without another transcription;
2. a normal duplicate delivery ACKs the terminal job without rerunning the
   provider pipeline;
3. the completed status, content ledger, and the original capture conversation
   are all readable after the worker request; the worker cannot silently create
   a different conversation.

Only external/provider leaves are replaced:

- Cloud Storage is a filesystem-backed local `storage.Client` replacement;
- Cloud Tasks is a local control-plane recorder, while production task protobuf
  construction remains intact;
- Google JWT signature/JWKS verification accepts one local test token, while
  the production header/audience/identity/retry-count dependency remains in
  use;
- VAD, prerecorded STT, and conversation LLM processing are deterministic.

The gauntlet **does** run the production PCM16 filename decoder, WAV
segmentation/cropping, raw-file staging/download/delete flow, FastAPI routes,
Redis locks/job state, Firestore transaction-backed content ledger, status
polling, and conversation persistence. It does **not** prove real GCS, the
Cloud Tasks service/control plane, Google-signed OIDC tokens, production VAD or
STT quality, or LLM output quality; those are deliberately provider-bound
tests, not local deterministic stack behavior.

With `--keep`, inspect `evidence/*.jsonl` for metadata-only task, job,
provider, and worker events. The runner rejects any test UID or transcript
token in every evidence stream. Process logs and the local storage root live
outside that evidence directory; raw staged test data is never recorded in
evidence. Without `--keep`, all stack state is removed after a successful run;
failed state is retained for diagnosis.
