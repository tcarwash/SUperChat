# SUperChat next steps and acceptance plan

Planning baseline: 2026-10-20. This is a proposed implementation and validation
sequence, not a record of completed tests. No tests or RF transmissions were
performed for this planning pass.

## Current baseline

The checkout implements channels, SQLite history, broadcast gossip and bounded
relaying, a 48-hour sync window, mention delivery receipts, requested thumbnails,
compressed record batching, Data2G broadcast/connected transport, and raw KISS
AX.25 UI transport. A standalone binary build and Compose relay configuration
exist. Simulator fixtures cover several of these paths.

The presence of code or a fixture does not establish reliable RF behavior.
The image dialog now has headless Textual interaction coverage across four
terminal sizes, including path/caption submission and visible error behavior.
Ghostty file-drop delivery and the native Browse chooser still need live
reproduction. Ghostty supports file drops in some configurations; the earlier
blanket claim that it cannot deliver paths to terminal applications is not
established.

ARAP integration already exists in `kisschat/auth.py`: canonical JSON content,
CLI signing/verification, detached proof transport, storage and repair, and status
badges. Known boundaries visible in source:

- `Chat.publish()` sends message bodies before asynchronous signing; this is
  intentional because signing is optional and must never gate delivery.
- The ARAP adapter now uses `inspect-proof` for untrusted signer/fingerprint
  claims, prefers fingerprint-addressed public keys, and requires all structured
  verification fields plus verified claims to agree before showing Verified.
- Fingerprint-addressed lookup has a callsign filename fallback for existing
  setups; explicit trust revision and key lifecycle UI are still missing.
- Verification caches now include a local verifier/profile/key/trust revision.
  Trust changes invalidate prior decisions immediately and queue stored proofs
  for rechecking; pending signatures show an explicit checking state.
- Mention receipts are currently unauthenticated application records.
- ARAP must be installed separately; the default container does not include it.

## Progress log

- 2026-10-22 ARAP airtime/auth update: new signatures publish to configured ARAP
  keyservers and carry only the 19-byte AR1 reference on radio; receivers resolve
  and cache the full proof out of band, then apply local signature/content/trust
  checks. The message UI shows the reference in subdued text beside the verified
  mark. `/auth CALLSIGN` issues a fresh ARAP challenge, returns a signed response
  over radio, and shows a persistent station-auth indicator until local trust
  policy changes. CLI help, startup status, and README describe the network data
  exchange. Python AST parsing and `git diff --check` passed; no tests, simulator,
  live keyserver, or RF validation were run for this update.

- 2026-10-21 orphaned-caption fix: in the current live trace, A's caption
  (origin d6dfd5, sequence 2, "dorc") reached B at 19:18:25 UTC, but the separate
  attachment record collided at 19:18:27. A's read-only database inspection showed
  the 6,245-byte 160x160 attachment; B and C had the caption without attachment
  metadata. Equal message highwaters prevented another body/history request, so
  metadata had no independent repair path. Origin announcements now repeat the
  origin's latest attachment metadata, and periodic inventories rotate through
  one recent attachment record per cycle across all channels. These repair
  advertisements are neighbor-only and do not send image data. A cache learning
  metadata for an existing caption schedules its own announcement for the next
  hop. Late attachment events now refresh the image list immediately as well as
  invalidating the message render cache. No tests or new live run performed.

- 2026-10-21 latency and conversation follow-up: the saved KISS/TCP trace showed
  C's collided chat waiting behind its own image receive window before announcing
  recovery; B stored it 83 seconds after publication and A after 96.5 seconds.
  Local recovery announcements now bypass that background deferral and use an
  existing 45-byte KC1K summary for unsigned history. A single-frame message less
  than two minutes old and its page end use normal chat priority when repaired;
  larger/older history remains bulk traffic. Added explicit local/stored message
  debug events for matching latency across station logs. No new latency benchmark
  or tests were run for these changes; the old timings are the diagnostic baseline.
  The conversation now rebuilds in timestamp order, highlights newly arrived
  messages with a 12-second fading background, and retains the reading position
  while scrolled up. Nicknames and a legend distinguish local/direct/relayed/unknown.
  Raw AX.25 source and used digipeater paths are retained per fragment, aggregated
  when assembled and persisted with the message; source-to-node associations come
  from directly transmitted inventories/summaries. Old history and transports
  without source metadata stay unknown. Rendering and live behavior remain unverified.

- 2026-10-21 thumbnail follow-up: the live trace showed C at 23/45 chunks,
  while A had queued its 45 original chunks plus returning relay copies and C
  emitted many simultaneous repair requests. Senders now register outgoing
  chunks for echo suppression and avoid rebuilding already-cached images as
  partial transfers. Explicit downloads advance in windows of up to eight chunks,
  reduced as needed to fit one request frame;
  the initial response has a 45-second allowance for routed claims, then
  claims/new chunks reset a 20-second silence timer. Repairs request only the
  active window's missing chunks, and the overall expiry stays fixed. The
  requester sends background announcements at window boundaries and stops
  relaying claims addressed to itself. A complete cache serves a request without
  flooding it farther. Every listener with metadata still stores overheard
  chunks and now receives progress/completion UI events; passive listeners do
  not initiate downloads. Added noise-image fixtures because the old solid-color
  JPEG had only three chunks. A 44-chunk line trial verified identical completed
  images at C and non-requesting B. The dropped-chunk fixture also verifies B's
  passive cached copy and rejects repair requests for already-received chunks.
  The 44-test unit suite passes, including
  selective window repair, passive display notification, and sender suppression.
  A local run through actual KISS/TCP sockets with simultaneous chat and an
  eight-chunk image completed in 100.5s: A, B and C cached identical bytes, every
  chat message arrived, and peak application queues were 6/2/3 records. Chat
  convergence at the far station still took 96.5s under hidden-terminal
  contention; this is a remaining latency concern, not RF validation. The status
  timer also now tolerates its Textual screen being unmounted during shutdown.

- 2026-10-21: traced the interactive line-mesh congestion to flooded neighbor
  queries/inventories, duplicate announcements, unconditional unsigned-proof
  probes, synchronized retries, and manifest-only retries even when no data had
  arrived. A separate reproduced expiry race deleted a due page request when
  another inventory arrived, leaving its window blocked for five minutes with
  nothing left to retry. Due requests now survive both inventory and routine-sync
  cleanup. Routine history exchange now stays between neighbors; caches propagate
  messages by advertising their own stored copy. Queries use the existing compact
  binary repair record, inventories carry optional proof availability, and page
  retries use jitter and a silence timer. A lost query retries data directly;
  cached fragments still use manifest/selective repair. Debug traces distinguish
  queue acceptance from modem submission and include bridge connection events.
  Added `--auto-sync` to the scripted simulator because its previous default
  disabled the background traffic used by live clients. In a three-message,
  zero-loss line trial, the old automatic path failed convergence after 94s
  (57 frames/35 collision losses over 97s including drain); the first patched
  trial converged (23 frames/2 collision losses). Separate automatic trials passed
  at the default contention window and with 10% injected loss. An actual local
  KISS/TCP run after retry repair delivered all three messages at all stations in
  57.5s, with empty client transmit queues. These are observed simulator runs,
  not a latency guarantee: random backoff/thread timing varies, and hidden-terminal
  latency remains material. The 32-test unittest suite passes. RF behavior and
  the updated interactive windows still require a fresh live run.

- Added a Textual interaction regression for the actual image attachment modal.
  At 48×24, 80×24, 100×30 and 160×45 it checks both action labels and that
  their rendered regions remain inside the screen; it also exercises path Enter,
  caption Enter and Escape. An integration check through `ChatApp` proves an
  explicit path submit queues one image operation, while a missing path produces
  a visible error with no stored message or transport frame. These checks pass.
  They do not reproduce Ghostty file-drop delivery or launch the native Browse
  chooser, which remain open.
- The complete current unittest suite passes: 16 tests, including auth policy,
  optional-signing delivery and restart recovery, loss model, local-history
  purge, image-dialog checks and shared SUperChat/ARAP profile vectors. These are
  source-level and headless Textual checks, not a live terminal-emulator or RF
  acceptance run.
- Fixed a Textual event-timer teardown race that appeared when the full test
  suite ran after the image-dialog tests: the station-panel refresh now tolerates
  a late timer callback after Textual removes the screen widgets. The complete
  16-test suite passes with the image tests included.
- Added a deterministic simulator fault for one requested image chunk on B→C.
  `fixtures/requested_thumbnail_loss.txt` confirmed metadata/history sync sent
  no image bytes before C's request, dropped chunk 0 of 3, then retried exactly
  that missing chunk through B. C reconstructed the original thumbnail digest,
  and all stations retained matching message histories. The JSON report passed;
  this does not yet cover competing image caches.
- Added `fixtures/offline_origin_thumbnail_relay.txt`. B requests and stores A's
  thumbnail, then A's simulator process, database connection and radio receiver
  stop. C syncs A's message metadata through B, verifies B has the complete image
  while C has no image bytes, then requests and receives the exact thumbnail
  from B. `/tmp/kisschat-offline-thumbnail.json` passed with A stopped and all
  three durable message histories converged. This validates the simulator's
  store-and-forward path, not RF behavior.
- Tightened the lost history page-end check so it measures the actual retry:
  C's manifest-only query must occur after the page-end is dropped, and B must
  send zero message fragments after that retry. The loss-free line fixture now
  passes with all three message stores converged; `/tmp/kisschat-lost-page-end.json`
  records the dropped page-end, observed manifest retry and zero post-retry data
  fragments. This is simulated transport evidence, not a modem ACK or RF test.
- Added a deterministic delayed page-end response in the frame-level simulator.
  `fixtures/delayed_history_page_end.txt` partitions B from C while A sends a
  3,000-byte message, then reconnects C and delays B's completion frame for 10
  simulated seconds. C receives all 19 message fragments, retries with a
  manifest-only query, and gets completion without another message fragment;
  both the retry and delayed completion arrive and all histories converge.
  `/tmp/kisschat-delayed-page-end.json` passed. This exercises the application
  page-end response, not KISS TNC ACKMODE or modem-level delayed ACKs.
- Added `fixtures/restart_all_stations.txt`. A publishes an unsigned message,
  then A, B and C are each stopped and restarted from their SQLite databases.
  The fixture checks process/radio detachment, durable node identity and the
  original message at A, then explicitly syncs C after restart and confirms all
  three histories converge. `/tmp/kisschat-restart-all.json` passed with one
  restart per station. This does not exercise restarting physical modems.
- Added `scripts/run_signed_mesh.py`, which creates temporary encrypted ARAP
  keys and a disposable local trust store, then runs the real SUperChat cores
  over the three-station line simulator. `fixtures/signed_history_relay.txt`
  passed: A's 309-byte proof crossed B to C, all three clients reported
  `verified`, and the stored proof bytes matched exactly. The machine report
  includes per-station verification status and proof size. The run used zero
  configured packet loss; complete proofs verified at A, B, and C. No
  production keys or RF were used. Automatic proof-only retries have since
  been removed: a missing or partial proof stays unverified.
- Added a real-ARAP process-stop proof-loss fixture: A signed a message while
  B↔C was partitioned, B stored and verified it, and the simulator stopped A's
  Chat, database connection and radio receiver before C requested A's history
  through B. C started with neither body nor proof. The fixture dropped B→C
  proof fragment 0; C received the body and left the incomplete proof
  unverified without proof-only retry traffic or body replay. A then restarted
  with the same database identity and trusted proof. The JSON report records A
  stopped/restarted, an offline-origin sync, the incomplete proof at C, and no
  body replay. A separate unsigned fixture now restarts all
  three Chat processes and reconverges after an explicit sync. Modem restart and
  RF remain untested.
- Added deterministic detached-proof loss to the line simulator. The fixture
  transfers an opaque 760-byte proof from A to B while B↔C is partitioned,
  drains and verifies the relay cache, then drops one B→C proof fragment during
  C's history sync. C keeps the message unverified and sends no proof-only
  request or message-body replay. Its JSON report records the dropped index,
  zero proof-page requests and unchanged body count. This validates transport
  and storage only; it does not use a
  cryptographically valid ARAP proof.
- Added `/clear-history` with a confirmation dialog. It purges local messages,
  image cache, receipts and partial transfers, clears in-memory sync retry state,
  and preserves the station identity and sequence counter. The dialog explains
  that other stations' copies cannot be erased and may later sync back. Focused
  storage validation checks inventory is empty after purge and the next local
  message uses a fresh sequence.
- Image-dialog snapshots show readable button labels at 48×24, 80×24, 100×30
  and 160×45. A focused Textual interaction check confirms Enter in the path
  field submits the selected path. These are rendered UI checks, not a live
  Ghostty or native desktop chooser test.
- Signing remains optional and never blocks sending. SUperChat now persists an
  asynchronous signing failure, shows that the message was sent unsigned, and
  includes the failed proof in the existing retry query. Real CLI signing and
  verification work; unavailable-key behavior still needs an integration check.
- Added a close/reopen recovery regression for both interrupted signing and a
  persisted signing failure. The next explicit sync retry queues only proof work
  for the original message identities; it does not send message bodies again.
  A completed proof remains queryable after another restart, while a failed
  proof remains retryable. The full 16-test SUperChat suite passes. This is a
  Store/retry-path test with a fake signer queue; real key outage and over-air
  proof delivery recovery remain open.
- Verification now consumes ARAP's untrusted `inspect-proof` JSON and structured
  `verify-content` result. Three focused unit tests cover accepted, untrusted and
  incomplete results. A temporary real-CLI run generated an encrypted key,
  signed a SUperChat message, trusted the key and verified the proof successfully;
  its key and trust store were removed afterward.
- Began freezing `KISSChat-message-v1` in
  `docs/ARAP-CONTENT-PROFILE.md` and `../arap/docs/KISSCHAT-INTEGRATION.md`.
  Both projects now define the same bounded JSON profile and media/context IDs;
  the ARAP Go adapter independently emits the Python canonical bytes. Shared
  golden vectors cover Unicode multiline text, an empty-caption JPEG thumbnail,
  and uint32 sequence/timestamp edges. Boundary checks include the maximum
  4,096-byte body. Python's full 15-test suite and all ARAP Go package tests
  pass. The real ARAP CLI signed and trusted-verified a SUperChat profile
  message with a disposable encrypted key. Changing the signed body returned
  `content_mismatch` and `invalid`. A separate real-CLI A→B→C line run verified
  the identical 309-byte detached proof at all three stations. Signed loss
  repair, A-offline recovery and RF signing equivalence remain open.
- Trust-policy changes are now polled every two seconds, persisted as a
  verification revision, and invalidate previous proof decisions in one SQLite
  transaction. Proof bytes remain stored; old in-flight jobs cannot restore a
  result from the previous revision. A regression check confirms body frames
  are queued before a full signing queue can refuse the optional proof job.
  Six focused adapter/store/delivery tests pass.
- The mesh simulator now supports temporary symmetric link partitions and
  rejoining. `fixtures/partition_rejoin.txt` verified that A's message stayed
  isolated from B during the outage, then reached B and C through the relay
  after reconnection. This uses the virtual radio model only, not RF.
- Scripted simulation can now write JSON results with run configuration, check
  names, current links, per-station counters and stored message rows. The
  partition/rejoin report was parsed and confirmed `passed: true`, matching
  message identities at all stations, with 12/13/9 frames transmitted. A report
  run with no verification command is treated as failure.
- Added an asymmetric A/B/C topology where C hears A directly but A cannot hear
  C. The fixture proved A's message reached B and C, C's response reached B,
  and A did not receive C's message. Its JSON report passed with nonzero frame
  counts from every station.
- Added `scripts/run_loss_matrix.py` and a targeted history-repair fixture. The
  seeded 0%, 5%, 15% and 30% line-topology matrix now passes with real SUperChat
  instances and JSON/database output. At 30%, the initial attempt exposed that
  B can miss A's first announcement; explicitly re-announcing A's inventory and
  repeating C's request lets the configured retry budget converge. Two further
  30% runs both converged, though host thread scheduling changed exact counters.
  Backoff and loss draws now use separate per-station/per-link seeded streams;
  collision and retry timing are still scheduler-sensitive.
- The receive-only fixture also reports `passed: true` with an explicit
  `verify-quiet` check and zero frames, so intentional quiet-start checks remain
  distinct from scenarios that must exchange messages.
- The bursty-loss runner now passed at 5%, 15% and 30% on the line fixture; the
  saved JSON report records the selected Gilbert-Elliott parameters and each
  run's exit/pass state. A deterministic requested-image-chunk drop also
  converged through selective repair. Incomplete detached proofs now remain
  unverified without proof-only retry traffic or body replay. ARAP-valid
  signed-proof behavior under loss,
  lost inventory announcements and modem-level delayed ACKs remain open; the
  application page-end delay and retry path now pass separately.

## 1. Stabilize the operator workflow

Deliver a reliable image dialog and basic chat navigation before RF trials.

- Reproduce the image dialog at 80×24, 100×30 and 160×45, plus a narrow terminal.
  Inspect rendered labels and clipping, including focus/hover states.
- Exercise keyboard and mouse selection, path Enter, caption Enter, Browse,
  cancel/Escape, spaces/quotes/file URIs, missing files and invalid images.
- Make preparation, local storage, queueing, transport failure and retry visible
  near the composer, including layouts where the activity rail is hidden.
- Confirm choosing a file never transmits; explicit Enter/Send commits it.
  Verify thumbnail bytes remain local until requested by a receiver.
- Diagnose Ghostty separately with a shell drop and app input capture. Support
  the actual emitted path format; document terminal-specific limitations only
  after reproducing them.
- Verify per-channel drafts and unread/mention badges through channel switches,
  restart where persistence is promised, and incoming history updates.

Acceptance: no invisible controls, no silent failure, and one explicit submit
creates exactly one local attachment message. Record screenshots and the terminal
version used. Use an isolated database and fake transport for UI checks.

## 2. Complete optional ARAP message signing

Use the existing sibling `../arap` implementation as the cryptographic and
policy authority. Keep SUperChat's adapter responsible for chat content, transport
and UI. Do not introduce a separate signing algorithm or independent trust model.

### 2a. Freeze the content contract

- [partial] Document a versioned SUperChat content profile in both projects: origin UUID,
  sequence, timestamp, channel, nick, signing callsign, exact body, and optional
  thumbnail digest, dimensions, byte count and media type.
- [partial] Specify exact UTF-8 serialization, integer bounds, absent versus empty fields,
  Unicode handling and context/media-type identifiers. Preserve signed bytes;
  relay TTL, receive time and route are excluded.
- [partial] Separate RF callsign/SSID from signing identity. For example, explicitly bind
  transport `AG7SU-7` to signing identity `AG7SU`; never silently strip arbitrary
  identifiers or claim that a signature authenticates the immediate RF relay.
- Add independent Python/Go golden vectors for text, Unicode, multiline text,
  empty-caption images and boundary-size messages. Reject wrong profile/context,
  identity, channel, sequence, body and image metadata.

Acceptance: the real ARAP CLI accepts the agreed byte stream; every signed-field
mutation fails the appropriate integrity or policy check.

### 2b. Best-effort outgoing signing

- Sending is never gated on ARAP setup or success. With no key, send immediately
  as unsigned. With a key, send the message immediately and sign asynchronously;
  attach and relay the proof when it is ready.
- Missing key, wrong passphrase, unavailable CLI, saturated worker queue or
  signing failure must leave the message sent and clearly marked unsigned or
  signing failed. Offer an explicit retry for the proof without replaying the
  body or creating another sequence number.
- [partial] Recover interrupted proof generation and proof delivery independently after
  restart; prevent duplicate message identities and preserve receive-only
  interactive startup. Store/retry recovery now covers pending and failed local
  signing after restart on the next explicit or automatic sync, without replaying
  the message body. Durable proof delivery through modem disconnect/restart still
  needs transport-level integration coverage. A pending proof must never make an
  already sent message disappear from history or block other chat traffic.
- Unlock encrypted keys through a deliberate user flow; pass secrets through
  ARAP's supported stdin mechanism. Keep secrets out of arguments, logs and the
  database. Define headless secret handling and session lock behavior.
- Measure CLI startup and encrypted-key unlock cost. If repeated invocation is
  too expensive, extend ARAP with a bounded local signing service using its Go
  library, explicit unlock/lock and restricted local access; do not duplicate
  private-key handling in SUperChat.

Acceptance: sends complete immediately with ARAP absent or failing; successful
proofs later verify over a real ARAP CLI. Recovery never resends a body solely
because proof creation or proof delivery was interrupted, and no recovery task
keys an interactive station on initial startup.

### 2c. Verification and key lifecycle

- [x] Replace the handwritten CBOR claim parser with ARAP's bounded
  `inspect-proof` interface. Inspected claims remain untrusted until verification
  succeeds.
- Consume structured verification results: cryptographic signature, content
  integrity, callsign identity, overall policy and reason. `Verified` requires
  valid signature/content and accepted callsign policy for the expected purpose.
- Expose unsigned, proof pending, unknown key, signed/untrusted, trusted,
  rejected/disabled, expired and invalid states without conflating them.
- [partial] Prefer keys by full fingerprint and maintain ARAP-managed explicit
  callsign/purpose bindings. Add import/export, fingerprint review, rotation,
  disabling and multiple historical keys in the operator workflow; use ARAP
  trust commands for trust changes.
- [partial] Include proof/content hashes, verifier/profile/key version and
  trust-store revision in verification cache keys. Trust changes now trigger
  durable revalidation across restart; relevant time-policy boundaries still
  need scheduling and cache coverage.
- Show why a badge is trusted: signer, fingerprint, verification time and trust
  source. Local manual trust must not be represented as independently established
  licensing identity.
- Optional keyserver lookup may retrieve fingerprint-addressed public keys, with
  bounded fetches and hash checks. Retrieval never creates trust. Offline key
  import and complete offline verification remain first-class paths.

Acceptance: disabling a trusted key removes its current verified status without
restarting; rotation preserves old proofs and selects the correct historical key;
malformed or unavailable verifier output cannot produce a verified badge.

### 2d. Proof transport, images and authenticated receipts

- [partial] Preserve original proof bytes through live receive, history batches, relay
  storage, restart and fragment repair. Handle proof/body/metadata arrival in
  any order without premature verification or permanent false-invalid status.
  A real-ARAP A→B→C proof crossed the line and verified byte-identically; proof
  loss with A's process stopped now confirms a dropped proof fragment leaves
  the message unverified, with no proof-only retry or body replay. Restart of
  every station, modem restart, and arbitrary arrival-order coverage remain
  open. Complete proofs continue to verify byte-identically across A→B→C.
- Apply explicit proof size, fragment, retry and queue limits. Fit the actual
  ARAP profile into transport limits and reject oversize proofs visibly.
- Keep proofs with messages by default so verification works without Internet.
  Measure their airtime cost before introducing optional reference-only schemes.
- Verify requested thumbnail bytes against the signed digest before displaying
  them as authenticated content; distinguish signed metadata from received bytes.
- Define a separate versioned ARAP content context for delivery receipts binding
  the original message digest/identity, recipient identity and receipt time.
  Persist and relay receipt proofs. Display verified delivery only for an accepted
  recipient signature; unsigned receipts remain visibly unauthenticated.
- Prevent forged receipt confirmations from retiring authenticated pending
  receipts. Bind confirmations to exact receipts and authenticate them in the
  signed-receipt flow, with bounded retry and expiry behavior.
- Authenticated content does not authenticate inventory, routing or immediate
  RF peers. Keep bounded control traffic; station challenge-response can be a
  separate later feature if peer authentication is needed.

Acceptance: signed text, images and receipts survive A→B→C with A offline;
tampering, proof stripping and spoofed receipts never gain trusted status.

### 2e. Integration and packaging

- Build against a recorded ARAP revision and exercise the real CLI, rather than
  mocking all sign/verify calls. Add vectors and integration coverage to both
  projects where the profile boundary requires it.
- Ship an ARAP-enabled standalone distribution and Compose image, with explicit
  executable discovery, version diagnostics, key/trust mounts and setup guidance.
  Verify encrypted-key signing as the container's unprivileged user.
- ARAP's experimental LoTW enrollment is not a prerequisite for local-trust
  message signing. Integrate enrolled identity only when ARAP's own validation
  and policy gates are satisfied; retain accurate trust provenance in the UI.

## 3. Repeatable mesh and transport validation

Extend the existing simulator into a repeatable suite with machine-readable
results, per-record traces, database comparisons and an overall failure exit code.

- [partial] Run full mesh and A—B—C line scenarios. Partition/rejoin,
  directional asymmetric-hearing fixtures and a bursty-loss line run at 5%,
  15% and 30% now pass.
- [partial] The seeded independent-loss 0%, 5%, 15% and 30% line-topology
  matrix and bursty-loss 5%, 15% and 30% line matrix pass. Exact counters can
  vary with host thread scheduling. Deterministic requested-thumbnail loss
  proves selective repair and digest completion; proof loss proves the message
  remains unverified without proof-only traffic or body replay; lost history
  page-end retry proves a
  manifest-only response without fragment replay; and real ARAP CLI runs verify
  byte-identical complete proofs at A, B and C, while a dropped fragment stays
  unverified at C. Offline-origin thumbnail relay,
  delayed application page-end recovery and restart/reconvergence of all three
  Chat processes pass. Modem-level ACKMODE behavior and restart, and competing
  image caches still need coverage.
- Include realistic radio duration, half-duplex deafness while transmitting,
  hidden-terminal collisions and modem queue limits. Mark any unmodeled behavior.
- Cover modem restart, offline recipients, queue saturation, clock skew,
  sequence holes and history at just before/exactly/just after the 48-hour cutoff.
- Run existing fixtures for startup quietness, bundling, no-echo, selective repair,
  receipts, requested thumbnails and AX.25. Add real-ARAP signed equivalents.
- Exercise real TCP adapters with controllable fake KISS/Data2G services so codec
  tests also cover disconnects, partial reads, ACKMODE tags and connected sessions.

Acceptance: zero-loss cases converge with zero unexpected duplicates; lossy cases
either converge within a declared retry budget or report incomplete state visibly.
No test may pass because no frames were exchanged. Quietness is measured after
excluding explicitly enabled periodic inventory announcements.

## 4. On-air acceptance campaign

Run conducted/attenuated bench checks first, then coordinated RF trials. Record
station callsigns, hardware, application/ARAP/modem versions, mode/bandwidth,
configured rate, topology, clock offsets and the exact script for every run.
Save per-station logs and database snapshots plus modem/PTT traces where available.
Establish actual A↔B, B↔C and A↔C reachability before asserting a line topology.

Start with unsigned Data2G broadcast; repeat the applicable cases with signing
enabled, then raw KISS AX.25 on VHF and Data2G connected ARQ. Use available actual
KISS TNCs to establish compatibility beyond the simulator. Connected sessions are
point-to-point: test A↔B followed by B↔C for store-and-forward, not broadcast gossip.

| Test | Procedure | Pass evidence |
| --- | --- | --- |
| Quiet start | Cold-start/restart an interactive station without auto-sync; open dialogs and select a file | No app-requested RF TX for five minutes or until explicit Send/Sync/Connect; distinguish modem housekeeping |
| Basic channels | Exchange short, Unicode and fragmented messages in two channels | Exact contents and identities at each intended node; correct channel/unread UI |
| Busy transmit batching | While one frame is on air, queue ten short messages; repeat with a history page | All messages arrive once; traces show multiple records per subsequent frame; compare frame count/PTT time against batching disabled in a lab build |
| Gossip/no echo | A requests B's missing history while C listens; then force A's repair request | C caches the response; C does not emit the same history response merely because it heard data or completion traffic |
| Line relay | Confirm A and C cannot decode each other; exchange messages through B | Both directions converge; hop limits and duplicate suppression hold |
| Offline delivery | C is off while A sends; B caches; A then goes off and C returns | B delivers the exact recent history without A; receipts return when the author is reachable again |
| Loss and repair | Introduce controlled frame drops/attenuation on top of logged RF conditions | Missing fragments/proofs recover; complete bodies are not unnecessarily replayed; failures remain visible |
| Partition/rejoin | Isolate an edge station, send independently, reconnect it | Complete union of eligible history without sequence-hole starvation or permanent retry loops |
| History window | Seed messages around the 48-hour boundary, including a future timestamp | Routine sync advertises/serves only eligible history; older local records remain readable |
| Mention receipts | Mention callsign, nick, both aliases and an offline recipient | Receipt only after durable receipt of the full message; relay/restart recovery; no claim of human reading |
| Requested images | Send metadata, wait, request at C through B, lose some chunks, then request from cache with A offline | Zero thumbnail chunks before request; bounded thumbnail size, exact digest, selective repair, eventual completion; quantify competing-cache duplicate cost |
| Signing | Trusted, unknown, disabled and rotated keys; signed images; altered content/proofs | Accurate badges and reasons; byte-identical relayed proofs; no trusted status for tampering; quantify proof airtime |
| Signed receipts | Valid recipient proof, forged receipt, replay and forged confirmation | Only the valid expected identity produces verified delivery; spoofing cannot retire the pending receipt |
| Reconnect | Drop modem TCP, restart modem and restart the relay during transfer | Durable messages remain; reconnect/sync restores delivery without duplicate identities or an unbounded TX queue |
| Hidden terminals | A and C transmit concurrently while only B hears both | Collisions are observable; bounded recovery; queues settle when offered traffic stops |
| Connected ARQ | Call, exchange batches, interrupt/reconnect, change from A↔B to B↔C | Exact stream framing, retained history, responsive chat, correct bandwidth configuration and session status |
| Soak | Operate relay for 24 hours with periodic conversation, outages and requests | Bounded transient state, no growing retry storm, usable UI, durable database; report intentional history growth separately |

For every run report delivered unique messages / eligible messages, convergence
time, frames and bytes by type, duplicate frames, repair counts, maximum queue
depth, PTT seconds, and user-message latency. Compare signed/unsigned and
batched/unbatched runs using the same corpus and radio settings. Signed overhead
and broadcast collisions are measurable costs, not grounds to promise zero storms
or zero duplicates in every topology.

For initial loss-free RF runs use ten short messages per origin and require 100%
delivery. Predeclare the convergence deadline from measured payload airtime plus
configured relay/retry delays; do not move it after a failed run. Loss trials
report delivery at that deadline and time to eventual convergence. Reproduce
failures with saved traces and convert them into simulator regressions.

## 5. Release gate and execution order

1. Finish and visually verify the image/composer workflow.
2. Implement ARAP content contract, best-effort proof recovery and structured
   verification/key lifecycle; add authenticated receipts and packaging.
3. Pass the reproducible offline/transport suite with the real ARAP CLI.
4. Complete two-station bench/RF trials, then three-station relay/loss trials,
   followed by the always-on soak.
5. Publish a versioned candidate with setup instructions, supported modem/version
   matrix, recorded signing overhead, known limitations and RF evidence.

Do not call the application fully validated until the UI reports are reproduced
and resolved, all three transports have applicable evidence, and the signing and
RF gates above have recorded outcomes. Optional Internet key hosting and future
station challenge-response do not replace offline message verification.

## Source references

- `kisschat/tui.py`, `kisschat/app.py`, `kisschat/store.py`, `kisschat/auth.py`
- `kisschat/data2g.py`, `kisschat/protocol.py`, `kisschat/ax25.py`
- `scripts/simulate_mesh.py`, `fixtures/`, `README.md`
- `../arap/README.md`, `../arap/protocol/verify.go`, `../arap/identity/trust.go`
- `../arap/cmd/arap/main.go`, `../arap/docs/IMPLEMENTATION-PLAN.md`
- `../arap/docs/VALIDATION.md`, `../arap/spec/ARAP-profile-v1.cddl`
# 2026-10-21 checkpoint: narrow-mode sync airtime

- Implemented: all bandwidths use five-minute routine sync intervals with up
  to two minutes of jitter. Data2G broadcast modes at or below 500 Hz also use
  a 60-second base retry interval with bounded exponential backoff; wider modes
  keep the 12-second retry base. At 500 Hz, inventory announcements are capped
  at 16 frames, relayed inventories are spaced by 30 seconds, and image chunk
  retries wait two minutes; ACKMODE has at most four frames in flight. README
  documents the airtime/repair latency tradeoff.
- Validation: code inspection and `git diff --check`; no tests run.
- Unverified: sustained real modem airtime and retry behavior at 500 Hz.

# 2026-10-21 checkpoint: compact KC2 records

- Implemented: switched application records to KC2, replacing the JSON map
  fallback with compact typed maps, varint integers, short key codes, and raw
  128-bit station IDs/256-bit digests. Existing specialized message, image,
  proof, inventory-summary, repair, receipt, and page-end layouts remain binary.
  AX.25 record bundles now identify KC2 records.
- Validation: `git diff --check`; no tests run.
- Unverified: multi-station interoperability and measured airtime savings.
