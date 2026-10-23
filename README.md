# SUperChat

SUperChat is a terminal chat client for radio links. Its pane-based interface has
channel navigation, a message timeline, a station list, link/sync status,
mention completion, inline Markdown, clickable web links, and a quick channel
switcher. It supports Data2G's shared
broadcast/KISS transport, its VARA-compatible connected ARQ transport, and a
generic raw KISS connection carrying AX.25 UI frames. The modem/TNC host keeps
ownership of modem, audio, channel access, and PTT.
Messages render `# headings`, `> quotes`, ordered and bulleted lists, `**bold**`,
`*italic*`, `__underline__`, `~~strikethrough~~`, and `` `inline code` ``, plus
fenced code blocks with optional syntax highlighting. Web links and Markdown
links use terminal hyperlinks when the terminal supports them.

The client reconnects automatically after a radio transport TCP disconnect, with an
exponential retry delay capped at 30 seconds. Messages sent while the link is
down stay in local history; after reconnect the station advertises its
inventory so peers can fetch and relay them. An interactive broadcast client
does not advertise history on its first connection, so launching it remains
receive-only until `/sync`, the first periodic sync, or a later reconnect.

Messages have IRC style channels (`#general`, `#radio`, and so on), a display
nick, and a local SQLite history. By default, history sync advertises and serves
messages from the last 48 hours; `--sync-history-hours 24`, `4`, or `1` narrows
that window, and the value is saved in the database. `--sync-history-messages N`
sets a global cap across origins per sync exchange, up to the fixed maximum of
36 messages (`0` uses that maximum; later exchanges can continue catch-up). The
48-hour value is an age ceiling, not a request for 48 hours of traffic. These
settings limit radio sync only; other records remain in local history. A foreground broadcast client starts in
receive mode without an initial inventory burst. It periodically exchanges
history by default, which lets clients recover messages lost when stations
transmit at the same time. Use `/sync` to retry incomplete exchanges now or
`--no-auto-sync` to disable periodic radio traffic.
Headless relay mode enables periodic sync automatically. The first routine
sync and later syncs run after five minutes plus up to two minutes of jitter at
any bandwidth. Use `/sync` to request an immediate exchange.
Unanswered history requests on broadcast modes at or below 500 Hz retry from a
60-second base interval with bounded exponential backoff; wider modes retain
the 12-second retry base. These delays reduce control traffic on narrow modes;
they can make history repair take longer. At 500 Hz and below, each inventory
announcement is limited to 16 frames, relayed inventory announcements are
spaced at least 30 seconds apart, missing image chunks retry after two minutes,
and no more than four ACKMODE frames stay in flight. Routine inventories and history requests stay between directly heard
neighbors. Each cache re-announces history it stores, so messages cross several
hops without flooding every inventory or query back through the mesh.
History page fragments are public broadcasts: listeners store complete
messages and announce their inventory later, without immediately rebroadcasting
the fragments they just heard. When a peer has more history, clients
request it in small batches. Inventories include sparse sequence bounds, and
pages follow messages a station actually stores. Complete out-of-order messages
heard in another station's history response are stored immediately and omitted
from later sync requests. When sequence gaps remain on both sides of an already
heard message, one fixed-binary request can name up to 36 missing sequences,
so the separated gaps are fetched together rather than retransmitting the heard
row. Larger catch-ups continue in additional bounded batches. Each station stores
complete messages and can relay them later. A station that hears a message
queues an inventory announcement for that origin; repeated messages are
coalesced and the announcement is delayed randomly by up to 8 seconds to
stagger listeners. Bounded retries use independent jitter, but
shared radio airtime can still produce collisions. Always-on clients therefore act
as store-and-forward nodes: a message can cross several stations and reach a
client that was offline during the original conversation. Messages outside the
configured history window remain in local history but are not advertised or
requested by routine sync. No server is required.
Large origin inventories are announced in bounded rotating batches and use the
bulk transmit queue, so later history origins get airtime without delaying new
chat messages.
Each newly sent chat message is followed by a compact summary of up to eight
recent origin ranges. The ranges rotate across messages when a station knows
more than eight origins. A peer uses the summary to request missing history
from the station that directly sent it, reducing the wait for periodic sync.
Older clients ignore these summary frames and continue to use regular inventory
sync.

Data2G group names are routing/checking labels, not privacy or encryption.
SUperChat traffic is readable by any station that can decode the broadcast.

## Run

SUperChat installs a `superchat` command from the Python package. The old
`kisschat` command remains available as a compatibility alias. To install it on a
workstation with `pipx`:

```sh
pipx install .
superchat --host 192.0.2.10 --callsign N0CALL
```

This creates a regular executable command while keeping its Python dependencies
isolated. The Docker Compose setup below is optional and is useful for running
an always-on relay.

To build a self-contained executable for the current operating system, install
`uv` and run:

```sh
./scripts/build-binary.sh
./dist/SUperChat --host 192.0.2.10 --callsign N0CALL
```

The build produces a single-file executable in `dist/`. Build it on each target
operating system; PyInstaller does not cross-compile binaries.

## Simulate a radio mesh

Exercise three real SUperChat instances without a modem or RF connection:

```sh
uv run python scripts/simulate_mesh.py --topology line --loss 0.15 \
  --bitrate 1200 --propagation 0.05
```

The simulator starts stations A, B, and C with temporary SQLite databases. In
the `line` topology, A and C can only hear B. Use `send A text`, `sync C`,
`sync A C` / `sync C A` to fetch the other edge station's history through B,
`forget C A` to remove A's cached history from C, `history B`, `verify`,
`stats`, and `wait 5` at the prompt. `verify` compares the message sets stored
by all three stations and exits with an error if they do not converge. Run the
checked-in three-station conversation with simulated loss and timing using:

```sh
uv run python scripts/simulate_mesh.py --topology line --loss 0.05 \
  --contention-window 10 --time-scale 0.1 \
  --script fixtures/three_station_conversation.txt
```

To interact with three real clients through the same radio model, open one
terminal window per station (requires a graphical session and `gnome-terminal`,
`konsole`, or `xterm`). To run the clients in a tmux session instead, pass
`--terminal tmux` (requires tmux):

```sh
uv run python scripts/interactive_mesh.py --topology line --loss 0.10 \
  --loss-model bursty --seed 7
```

```sh
uv run python scripts/interactive_mesh.py --terminal tmux --topology line \
  --loss 0.10 --loss-model bursty --channel-busy-delay 0.5 --seed 7
```

`--channel-busy-delay` adds a configurable quiet period after each frame, during
which other stations that hear the transmitter wait before accessing the
simulated channel. It defaults to `0` and is fixture-only; `--time-scale`
scales this delay along with the rest of simulated radio timing.

Each window runs a normal client with a temporary database. Press Ctrl-C in the
launcher terminal to stop the bridge, close its client processes, and remove
those databases. With tmux, attach using the printed command; stopping the
launcher also closes the client session and removes those databases. The mesh
log records the bridge start and shutdown reason.

To capture a live trace, close the old client windows and run:

```sh
uv run python scripts/interactive_mesh.py --topology line --loss 0 \
  --contention-window 2 --debug-log /tmp/superchat-debug.log
```

This writes the mesh log and separate `superchat-debug-A.log`, `-B.log`, and
`-C.log` client logs beside it. The files are cleared at startup. Client events
distinguish queued records (`TX_QUEUE`), modem submissions (`TX_SUBMIT`), and
received records (`RX`). Raw KISS submission does not confirm reception on air.
Logs include message contents. Use `--debug-log PATH` with `superchat` directly
to capture a station outside the simulator.

Exercise automatic recovery without manual sync commands:

```sh
uv run python scripts/simulate_mesh.py --topology line --loss 0 \
  --contention-window 2 --auto-sync --script fixtures/automatic_sync_line.txt
```

The scripted simulator otherwise leaves automatic sync disabled. This fixture
sends three messages and checks that every station receives all three.

To exercise a one-way link where C hears A but A cannot hear C, run:

```sh
uv run python scripts/simulate_mesh.py --topology asymmetric --loss 0 \
  --contention-window 10 --time-scale 0.1 \
  --script fixtures/asymmetric_hearing.txt \
  --json-report /tmp/kisschat-asymmetric-report.json
```

The fixture verifies reception and non-reception by exact message identity; the
JSON report includes the directed transmit-to-listener map.

To run the repeatable 0%, 5%, 15% and 30% packet-loss matrix with a fixed seed
and capture sync traces in a combined report, use:

```sh
uv run python scripts/run_loss_matrix.py \
  --script fixtures/loss_matrix_smoke.txt \
  --seed 7 --output /tmp/kisschat-loss-matrix.json
```

Each rate is an independent three-station run with the same scenario and seed.
The combined report includes the simulator output and per-station database
snapshots; the runner exits nonzero if any run misses its convergence check.
The seed controls per-station backoff and per-link loss draws; host thread
scheduling can still change retry timing and exact traffic counters.

To simulate a temporary link outage, verify isolation, then rejoin the edge
stations and sync the retained history through B:

```sh
uv run python scripts/simulate_mesh.py --topology line --loss 0 \
  --contention-window 10 --time-scale 0.1 \
  --script fixtures/partition_rejoin.txt \
  --json-report /tmp/kisschat-partition-report.json
```

The script's `partition`, `verify-partition`, and `rejoin` commands update the
simulated radio links. `verify-partition` fails if any post-partition message
crosses the disconnected link; the fixture then requires all three stores to
converge after reconnection. The JSON report records the configuration, checks,
per-station counters, active links and exact message rows at each station. A
scripted report run exits nonzero if it runs no verification check or any check
fails.

To run the relay path through the raw KISS and AX.25 UI frame codec, use:

```sh
uv run python scripts/simulate_mesh.py --transport ax25 --topology line \
  --loss 0 --time-scale 0.1 --script fixtures/ax25_line_smoke.txt
```

This wraps application batches in AX.25 UI frames and KISS TCP frames before
the simulated stations receive them. It checks the codec and store-and-forward
path without opening a TNC connection or transmitting on air.

To confirm that starting an interactive client stays receive-only, run:

```sh
uv run python scripts/simulate_mesh.py --topology line --loss 0 \
  --time-scale 0.1 --script fixtures/startup_receive_only.txt
```

The `verify-quiet` command fails if startup queued or transmitted any KISS frames.

To confirm that a burst of short messages shares radio frames, run:

```sh
uv run python scripts/simulate_mesh.py --topology line --loss 0 \
  --contention-window 10 --time-scale 0.1 \
  --script fixtures/message_bundling.txt
```

The fixture's `verify-bundling` command requires at least one radio frame to
carry multiple chat messages; `verify` then checks delivery to all stations.

To verify that thumbnail bytes stay local until explicitly requested, run:

```sh
uv run python scripts/simulate_mesh.py --topology line --loss 0 \
  --contention-window 10 --time-scale 0.1 \
  --script fixtures/requested_thumbnail.txt
```

The fixture checks that C receives the attachment metadata without image
fragments, then requests the thumbnail and checks the completed bytes.

To check the 48-hour history boundary through the line relay, run:

```sh
uv run python scripts/simulate_mesh.py --topology line --loss 0 \
  --contention-window 1 --time-scale 0.1 \
  --script fixtures/history_window.txt
```

This leaves one message older than 48 hours in B's cache and verifies that C
receives the recent message only.

To verify a mention receipt from C returns to A through B, run:

```sh
uv run python scripts/simulate_mesh.py --topology line --loss 0 \
  --contention-window 1 --time-scale 0.1 \
  --script fixtures/mention_receipt.txt
```

To assert that C caches a broadcast history page and does not retransmit its
fragments, run:

```sh
uv run python scripts/simulate_mesh.py --topology line --loss 0 \
  --contention-window 10 --time-scale 0.1 \
  --script fixtures/no_echo.txt
```
The check requires C to receive actual message fragments and its page-end
before passing, so an empty response cannot pass as a successful case.

History page fragments are broadcast to nearby listeners so they can cache
the messages during someone else's sync. Cached listeners announce their
inventory later; they do not immediately retransmit the heard fragments.

For selective repair after a deliberately dropped fragment, run the focused
fixture:

```sh
uv run python scripts/simulate_mesh.py --topology line --loss 0 \
  --contention-window 10 --time-scale 0.1 --drop-first-history-fragment \
  --trace-sync --script fixtures/selective_repair.txt
```

Its final verifier requires exactly one repair request and confirms B sent
only the missing fragment after the original page; all three stations must
converge.
To check recovery when the entire page-end is lost, run:

```sh
uv run python scripts/simulate_mesh.py --topology line --loss 0 \
  --contention-window 10 --time-scale 0.1 --drop-first-history-page-end \
  --trace-sync --script fixtures/lost_page_end.txt
```

The simulator retries after 12 seconds so the fixture can verify the automatic
retry against its modeled radio timing; normal clients retry after 90 seconds,
with bounded exponential backoff. A page retry asks for the page manifest first;
it then repairs only fragments the receiver is still missing.

To verify real ARAP signing across the A—B—C simulator, build the sibling ARAP
CLI and run the disposable-key fixture:

```sh
make -C ../arap build
uv run python scripts/run_signed_mesh.py -- --topology line --loss 0 \
  --time-scale 0.1 --contention-window 10 --trace-sync \
  --json-report /tmp/kisschat-signed-history.json \
  --script fixtures/signed_history_relay.txt
```

The wrapper creates encrypted test-only keys and a local trust store in a
temporary directory, then removes them after the run. The report checks that
the signature verifies at each station and the detached proof bytes match.
This tests simulated transport and the real ARAP CLI; it does not test RF.
To confirm an incomplete signed proof stays unverified after a dropped fragment,
run:

```sh
uv run python scripts/run_signed_mesh.py -- --topology line --loss 0 \
  --time-scale 0.1 --contention-window 10 --drop-first-proof-fragment \
  --trace-sync --json-report /tmp/kisschat-signed-proof-loss.json \
  --script fixtures/signed_proof_loss_offline_origin.txt
```

The fixture verifies a trusted proof at B, stops A's chat process/database and
radio receiver, then drops one B-to-C proof fragment during C's history sync.
C keeps the message but leaves its incomplete proof unverified, with no
proof-only retry or message-body replay. A then restarts from its durable
database. This does not emulate rebooting a physical modem or transmitting
over RF.
`--loss` applies independent receive loss, while `--bitrate`, `--propagation`,
`--interframe`, and `--framing-bytes` control radio timing. `--time-scale 0.1`
runs simulated airtime ten times faster (script `wait` commands remain wall-clock
seconds); `--seed` sets the packet-loss random seed. `stats` reports frames,
records, bytes, estimated airtime, losses, collisions, and selective page repair
requests.
The optional `--drop-first-history-fragment` switch drops one A-origin history
fragment from B's response during the focused fixture's targeted sync. Set `--loss` above
zero to add independent receiver packet loss; those runs may need extra sync
rounds before `verify` passes.

Use a dedicated Data2G host connection for this app. Data2G's command interface
is single client for its command and ARQ services. Run SUperChat on a client
that can reach TCP ports 8300 and 8100 on the host for broadcast mode:

```sh
python -m kisschat --host 192.0.2.10 --callsign N0CALL
```

To limit broadcast transmissions, use `--max-bandwidth 500` (or another
maximum in Hz). SUperChat chooses the widest mode advertised by that Data2G
host within the limit. This setting applies to broadcast mode; `--bandwidth`
controls connected ARQ, while `--mode` explicitly selects a broadcast mode and
must fit under any configured maximum.

For VARA-compatible connected mode, also allow the data port (default 8301):

```sh
python -m kisschat --host 192.0.2.10 --callsign N0CALL --transport connected
```
Set `--bandwidth 500` when the ARQ link must stay within a 500 Hz segment;
the default cap is 2300 Hz. The option is ignored by broadcast/KISS transport.

For VHF packet radio, connect to a standard raw KISS TCP endpoint (for example,
Dire Wolf's default KISS server on port 8001):

```sh
python -m kisschat --host 192.0.2.10 --callsign N0CALL --transport ax25
```

This sends each SUperChat record as the information field of an AX.25 UI frame
with PID `0xF0`, destination `CQ`, and source `N0CALL`. Configure another
destination or a digipeater path with `--ax25-destination` and
`--digipeaters WIDE1-1,WIDE2-1`. `--kiss-tnc-port` selects a multiport TNC
channel. Raw KISS TCP defaults to port 8001 in this mode; override it with
`--kiss-port`. A serial KISS TNC can be exposed over TCP with a local KISS
bridge. This mode uses standard KISS data frames and does not use Data2G's
ACKMODE extension, so the modem does not report per-frame on-air confirmations
to SUperChat. AX.25 transmit submissions are paced using estimated frame
airtime; set `--ax25-baud` to the radio bit rate and adjust
`--ax25-interframe-delay` for extra spacing. This only limits the host queue;
the TNC still controls channel access and PTT. UI frames are best-effort;
digipeating and packet size depend on the configured radio path.
When several records are queued, SUperChat compresses and bundles them into one
radio frame when they fit. AX.25 bundles stay within the 256-byte information
field limit; Data2G broadcast bundles stay within SUperChat's 220-byte record
limit; connected ARQ bundles stay within its 255-byte stream-record limit.
Application records use the KC2 wire format: compact typed fields, variable
length integers, and binary station IDs replace JSON for control records.
Specialized message, image, proof, and sync records keep their compact layouts.
KC2 stations cannot exchange application records with KC1 stations, so update
all stations in a mesh together.
Sender-tagged message records are also a new KC2 subtype; upgrade every station
before sending them, since older KC2 clients do not decode that subtype.
Broadcast interactive traffic waits for its preceding ACKMODE receipt before
the next frame is submitted, so messages typed while one is on air can be
bundled together. Long records that do not fit remain separate. Receiving
clients unpack the bundle before processing each record. All stations that
should receive bundled traffic need a SUperChat version that understands this
format.

Install the locked dependencies and run it locally with `uv`:

```sh
uv sync --locked --no-dev
uv run superchat --host 192.0.2.10 --callsign N0CALL --nick Alice
```

The default history database is `~/.local/share/kisschat/chat.sqlite3`. Each
installation keeps its own stable node ID, outgoing sequence counter, selected
nick, and last selected channel there. The initial channel is the last selected
channel, or `#general` on first run; `--channel` overrides it. Give each station
a unique callsign and keep its database between runs.

Useful options:

```text
--version               print the SUperChat version and exit
--host HOST             Data2G or raw KISS TNC host/IP
--callsign CALL         station identity sent to Data2G
--nick NAME             display name (defaults to callsign)
--channel #NAME         initial channel (last selected, or #general on first run)
--mode MODE             optional Data2G broadcast transmit mode
--max-bandwidth HZ      broadcast mode maximum; choose widest advertised mode within it
--unattended            use a Data2G mode at or below 500 Hz for automatic traffic; explicit sends may use --mode or the widest host mode
--transport TRANSPORT   broadcast (default), connected, or ax25
--bandwidth HZ          connected ARQ cap: 500, 1200, 2300, or 2750
--auto-sync             periodically exchange history (default; transmits on air)
--no-auto-sync          disable periodic history exchange
--command-port PORT     default 8300
--kiss-port PORT        raw KISS TCP port; 8001 for ax25, 8100 for Data2G
--kiss-tnc-port PORT    raw KISS TNC channel, default 0
--ax25-destination CALL AX.25 UI destination, default CQ
--digipeaters PATH      comma-separated AX.25 digipeater path
--db PATH               SQLite database path
```

`--unattended` is for unattended Data2G broadcast relays. Startup reads the
host's `MODES` catalog and fails closed if it cannot find a mode at or below
500 Hz. Sync, relay, and other automatic traffic use that narrow mode. A
message you explicitly send (and its attachment metadata) temporarily uses
`--mode` when supplied, or the widest advertised mode, then switches back after
the modem acknowledges the frame. This option is currently limited to Data2G
broadcast; it does not change connected ARQ or raw AX.25 TNC configuration.
The same policy can be switched live with the **Unattended · auto TX ≤500 Hz**
checkbox in the TUI. Its setting is saved with the local chat database. When
unchecked, automatic traffic returns to the selected operator mode.

## Commands

Type a message and press Enter to send it to the current channel.
Use **↑ Load older** above the timeline to expand the visible history in
200-message steps; it is disabled once all locally stored channel history is shown.

```text
/join #channel          switch the displayed channel
/nick name              change the display name
/image PATH [caption]   send a compressed thumbnail; quote paths with spaces
/image-get ORIGIN:SEQ   request one shown thumbnail
/images                 pick a channel thumbnail to request
/search TEXT            search this channel's local and synced history
/history [count]        show recent local and synced messages
/channels               list channels in local history
/sync                   advertise local history now
/clear-history          confirm and erase this device's local history/cache
/cq                     send a Data2G discovery frame in connected mode
/connect CALLSIGN       call a station with connected transport
/disconnect             end the connected ARQ session
/help                   show command summary
/quit                   exit
```

`/clear-history` removes all locally stored messages, image data, receipts,
and partial transfers; it also discards frames still waiting in this client's
transmit queues. It preserves this station's identity and sequence counter.
It cannot erase copies already held by other stations, which can later sync
those messages back to this device.

`Ctrl+Shift+K` opens the fuzzy channel switcher; `Ctrl+Shift+F` searches the
current channel. Search is a literal, case-insensitive match across local and
synced message text. Use `/history` to return to the recent timeline.
Tab-complete an `@mention` using a recently heard station's callsign (including
portable `/` suffixes) or advertised nick; stations expire from the list and
suggestions after 15 minutes without an inventory. In connected mode, Tab also
completes recently heard callsigns after `/connect`. Nicks containing spaces
can be mentioned with quotes, for example `@"Alice Smith"`; Tab completes this
form too. The channel list marks unread messages and shows a separate `@` count
when a background-channel message mentions this station.
Those badges survive restarts and clear when you open the channel.

The app retains all channels in one KISS broadcast group. Channel selection is
application level; it does not make channel contents private. Joined channel
names persist locally, including channels that do not have messages yet.

For a directed, reliable Data2G ARQ link, start with
`--transport connected`. Each client listens for incoming sessions; call a
station with `/connect CALLSIGN`. Use `/cq` to transmit one Data2G discovery
frame while idle; stations that hear it appear in the station list and become
available for `/connect` completion. CQ transmission is explicit and does not
start a session. SUperChat enables Data2G's `CHAT ON` latency
profile and uses the VARA-compatible data stream for chat records. Only one
connected session can be active on a Data2G host at a time. Channel names remain
in the shared chat records, while the active ARQ peer is the only station that
receives them directly; relays can sync and forward history when they connect.

Messages containing `@callsign` or `@nick` show a waiting marker. When a client
whose current callsign or nick matches the mention receives and stores the full
message, it stores a receipt and sends it toward the author. Nearby stations
persist and relay pending receipts, retrying with increasing delays until the
author confirms receipt. On restart, clients reconstruct missing receipts from
stored messages before syncing. The author broadcasts that confirmation so relays can
stop retrying. A receipt names the station that stored the message; it does not
mean a person has read it. Receipts are app-level reports from the receiving
station and are not cryptographically authenticated.
If a message mentions the same station by both its callsign and nick, the
receiver acknowledges each alias separately.

## Inline thumbnails

Use the **＋ Image** button to browse for a file with the desktop chooser (when
`zenity` or `kdialog` is installed), or enter its path directly. You can also
use `/image PATH [caption]` to attach an image. Quote
the path if it contains spaces, for example `/image "/home/radio field/photo.jpg" portable setup`.
SUperChat corrects orientation and scales new thumbnails to fit within 80×72
pixels, twice the inline preview's default 40×36 pixel resolution. It encodes
a JPEG thumbnail capped at 8 KiB (at most about 59 radio fragments). The message
advertises only its digest and thumbnail dimensions; image bytes stay on the
sending station until a receiver requests that specific image. Downloaded
thumbnails are saved as JPEG blobs in the station's SQLite database at
`~/.local/share/kisschat/chat.sqlite3` by default, with a 64 MiB image cache cap.
The original full-size source file is not stored by SUperChat. The right panel
lists recent thumbnails in the
current channel; select one and press **Request selected**. The message also shows a request command, for example
`/image-get 0123abcd:42`. Any nearby station that has cached the thumbnail can
answer, so requests can use the same store-and-forward mesh as chat. A unique
request ID lets receivers retry if radio frames are lost. Caches exchange
response claims and converge on one station ID, with a brief settle period to
reduce duplicate thumbnail transmissions across multiple hops. Returned
chunks carry that request ID and a four-hop limit; each station forwards a chunk
only once for that request, preventing relay loops while allowing remote receivers
to fetch from an always-on cache. The requester fetches up to eight missing chunks
at a time, bounded by the request frame size, and advances when that window arrives. New chunks extend the repair
timeout, and retries request only missing chunks in the active window. The
requester defers background cache inventories while receiving a window and sends
them before requesting the next one. Local chat recovery announcements can pass
during a download. Unsigned local recovery hints use the compact history summary;
recent single-frame message repairs and their page ends use the normal chat queue.
Attachment metadata is re-announced independently of message text: origin hints
repeat the latest attachment, and periodic inventories rotate through one recent
attachment per cycle. This lets a caption recover its image indicator even when
the original metadata frame was lost after the caption arrived.
Every listener with the attachment metadata can assemble, display, and cache the
overheard image, including a station that never requested it. Passive listeners
do not start repair requests. Senders suppress returning copies of their own
chunks so relay traffic does not echo back into the transfer.
Each node retains up to 64 MiB of requested or overheard
thumbnail data in insertion order; message history and attachment metadata remain
available after an older cached image is evicted, so it can be fetched again from
another station.

To exercise a larger image through a line relay with only C requesting it and B
caching it from overhearing:

```sh
uv run python scripts/simulate_mesh.py --topology line --loss 0 \
  --contention-window 2 --time-scale 0.05 --image-pattern noise \
  --script fixtures/image_window_overhearing.txt
```

Received thumbnails appear inline as terminal half-block previews. Their visual
color fidelity depends on true-color terminal support. Original image files are
never transmitted. Attachments sync as metadata with message history, while the
thumbnail itself is fetched only on request.
In broadcast/KISS mode, requests are relayed up to four stations away. Nearby
nodes cache returned thumbnails they overhear and can serve them later. In
connected ARQ mode, a request is served by the current peer if it has the
thumbnail; connect through a cache node to request from it. Selecting an
incomplete thumbnail again asks caches only for the missing chunks. After an
explicit request, SUperChat also retries missing chunks up to three times when
radio frames are lost; completed thumbnails are never fetched automatically.

## PixelQSO cards

Use **＋ PixelQSO card** to choose a card from PixelQSO's `cards/` or
`sent-cards/` library. The default library is `~/.local/share/PixelQSO`; set
`PIXELQSO_DATA_DIR` to use another PixelQSO data folder. When the PixelQSO
source checkout is alongside this project, its `examples/starter-cards/` are
also listed; set `PIXELQSO_SOURCE_DIR` to locate another checkout. The picker
shows a terminal preview of the highlighted individual card, including a
linked frame's first-card ID and interval. Each card is a separate typed chat
log entry with a compact metadata reference; its raster is cached locally by
the sender and is transferred only when a receiver requests it from Recent
Attachments. Unrequested cards do not enter the PixelQSO received-card archive.
Linked animation frames remain separate cards/messages: the second card's link
identifies the first, and the terminal plays the animation after both payloads
are collected. Downloaded cards are saved as individual native PixelQSO
`qso-log/*.json` entries under `received_cards`. Cards up to 64×64 pixels and
32 colors are supported individually. Canvas editing is not supported. Older
SUperChat versions display the compact card reference as text.

## Optional ARAP message verification

SUperChat can optionally sign each outgoing message with an ARAP Ed25519 content proof.
The proof is stored locally and published to a configured ARAP keyserver; only its
19-byte `AR1` reference is sent over radio and relayed with synchronized history.
Verifiers fetch the proof from the configured server over the network, then verify
the signature, content, and local trust policy. The signature binds
the origin ID, sequence, timestamp, channel, nick, signing callsign, and body.
Image attachment metadata and signed card references bind digest, dimensions, and size; downloaded raster bytes are checked against their digests.
Clients display `✓ Verified` only when ARAP verifies both the proof and content
and that client's ARAP trust store authorizes the signing key for
`content-signature`. A cryptographically valid proof without local trust is
shown as signed but untrusted. If the public key or ARAP verifier is unavailable,
the signature is shown as unchecked rather than implying it was verified. A bad
proof is marked invalid. Saved untrusted or unchecked proofs are retried on a
later launch, so adding the key or trust binding can update their status.
Messages from clients without ARAP remain unsigned. Signature verification does
not encrypt radio traffic. Signing is best-effort: a configured client sends the
chat message immediately, then signs it asynchronously. If key access or signing
fails, the message remains sent and is marked unsigned while SUperChat retries
the proof later; sending never waits for ARAP.
The signed message fields and canonical bytes are defined by the
[KISSChat-message-v1 profile](docs/ARAP-CONTENT-PROFILE.md), shared with ARAP's
independent Go adapter.
When signing is enabled, a pending signing record is committed with each local
message; interrupted signing jobs resume at the next requested or automatic
history sync so an interactive client stays quiet on its first connection.

Build the `arap` CLI from the sibling `../arap` project and put its `bin`
directory on `PATH` (or set `ARAP_CLI` to the binary path). Create an encrypted
station key and put its public key in the local key directory:

```sh
make -C ../arap build
export PATH="$PWD/../arap/bin:$PATH"
mkdir -p ~/.config/arap ~/.config/kisschat/keys
arap keygen --private-key ~/.config/arap/N0CALL.age \
  --public-key ~/.config/kisschat/keys/N0CALL.pub.pem
arap trust add --store ~/.config/arap/trust.json \
  --public-key ~/.config/kisschat/keys/N0CALL.pub.pem --callsign N0CALL
```

Exchange public keys with stations you trust, place them in the same directory
as `<CALLSIGN>.pub.pem`, and add them to the ARAP trust store with
`arap trust add`. To accept `/auth` responses from a station, also trust its key
for `challenge-response` with `arap trust add --store ~/.config/arap/trust.json --public-key ~/.config/kisschat/keys/AG7SU.pub.pem --callsign AG7SU --purpose challenge-response`. Start SUperChat with `--arap-key ~/.config/arap/N0CALL.age`;
`--arap-cli`, `--arap-key-dir`, and `--arap-trust-store` select the CLI and
local verification configuration. Set `KISSCHAT_ARAP_PASSPHRASE` in the
process environment to sign with an encrypted key without an interactive
prompt. Set `ARAP_UPLOAD_TOKEN` for authenticated keyserver publishing, then add
`--arap-server https://your-keyserver` (repeat the option to configure multiple
resolvers). Enabling this online proof path sends proof uploads and lookup requests
out of band over the network; only the AR1 reference travels over radio. If the
server or network is unavailable, signing and chat delivery remain best effort,
and messages stay unchecked until their proofs can be fetched. ARAP callsigns must contain 3–20 ASCII letters, digits, and properly
placed slashes, with at least one letter and one digit. A Data2G callsign that
does not meet ARAP's grammar can still chat, but its messages cannot be signed
or shown as ARAP-verified. Signing is disabled when `--arap-key` is omitted. All participating
clients need this reference-capable version and network access to the configured
ARAP keyserver to show verified badges; older clients still display the message
but do not understand AR1 references. The default Docker image
does not bundle the separately maintained ARAP CLI; for Compose signing, build
a derived image that includes `arap` and mount the key and trust directories.

Use `/auth CALLSIGN` for a one-time fresh challenge-response over radio. The
target signs the nonce with its ARAP key; the requester verifies it under the
local `challenge-response` trust policy. A successful station-list indicator
means that key proved control. It remains until the local ARAP trust
policy changes, which clears saved station-auth indicators. This is separate
from each message's signature badge.

## Docker Compose

Set `DATA2G_HOST` to the reachable modem host and `CALLSIGN` to this station's
identity, then run `docker compose run --rm kisschat`. The Compose service uses
host networking so the configured TCP ports can reach the host or a remote
modem host without NAT mappings. The database lives in the `kisschat-data`
volume. Containers run the app as an unprivileged `kisschat` account; the entrypoint
repairs volume ownership before dropping privileges, including for an existing database.
The image installs application dependencies from the checked-in `uv.lock` and
fails the build if the lock no longer matches `pyproject.toml`.

```sh
DATA2G_HOST=192.0.2.10 CALLSIGN=N0CALL docker compose run --rm kisschat
```

Set `KISSCHAT_TRANSPORT=connected` to use Data2G's VARA-compatible connected
mode from Compose. That mode also needs TCP port 8301 reachable from the
container; the provided Compose services use host networking. Set
`KISSCHAT_BANDWIDTH=500` (or `1200`, `2300`, `2750`) to choose its ARQ cap.

For raw KISS over TCP, set `KISSCHAT_TRANSPORT=ax25` and point
`KISSCHAT_HOST` at the KISS server (Dire Wolf defaults to port 8001). Optional
`KISSCHAT_KISS_PORT`, `KISSCHAT_KISS_TNC_PORT`,
`KISSCHAT_AX25_DESTINATION`, `KISSCHAT_DIGIPEATERS`, `KISSCHAT_AX25_BAUD`,
and `KISSCHAT_AX25_INTERFRAME_DELAY` configure the endpoint, AX.25 UI
address/path, and airtime pacing.

```sh
KISSCHAT_TRANSPORT=ax25 KISSCHAT_HOST=192.0.2.20 CALLSIGN=N0CALL \
  KISSCHAT_AX25_DESTINATION=CQ KISSCHAT_DIGIPEATERS=WIDE1-1,WIDE2-1 \
  docker compose run --rm kisschat
```

For an always-on history relay on a Docker host, run the headless profile:

```sh
DATA2G_HOST=192.0.2.10 CALLSIGN=N0CALL docker compose --profile relay up -d kisschat-relay
```

Run only one SUperChat instance against a given Data2G command service because
Data2G gives that command interface to one client at a time. The relay profile
restarts after host/container restarts and keeps the same message database.

## Wire and sync notes

The app uses a compact binary message-fragment record inside KISS payloads.
Each frame is at most 220 bytes. Text fragments use the space left after the
actual channel and nick fields, reducing fragment count for typical short
names; longer messages are split and reassembled. History page data is
broadcast once so every listener can cache it; the listener advertises its
updated inventory later instead of echoing the same fragments. The page-end
record stays addressed to the requester and lists the message sequence numbers
it covered. The receiver asks for only missing fragment indices when it retained
a partial message; if no fragment arrived, it requests the whole message.
Page-end records and fragment-repair requests use compact binary forms.
Sparse history requests use a fixed binary sequence list (up to 36 IDs per
220-byte frame) rather than a JSON range query; the matching page-end carries
the same sequence list so listeners can verify exactly which records the batch
covered.
ARAP proof references use a compact record tied to the message origin and
sequence. Relays forward the reference with synchronized history; each receiver
fetches and caches the full proof out of band before verifying it. Messages
without a resolvable proof remain unchecked. Legacy proof-fragment records are
still accepted, but new signatures use online references.
The default maximum message length is 4096 UTF-8 bytes. Message history is
stored per origin with a monotonically increasing sequence number. A newly
heard peer advertises contiguous history, after which the clients exchange missing
records in batches of up to 8 messages. History fragments use the bulk transmit
queue so chat and control traffic can pass them. Imported records are re-advertised so
nearby clients can fetch and relay them. Mentions use durable application
receipt records that are sent only after the recipient has stored the complete message.
For each origin window, a client requests one directly heard cache at a time.
Inventories may include a signature-availability mask for compatible clients.
Old clients can still read the inventory and message records. The new
sparse-sequence `S` request subtype is
pre-1.0 wire format; synchronize with matching builds until the first stable
release.

The conversation stays in message timestamp order, including late history repairs.
New arrivals have a subtle background that fades over 12 seconds. Nickname colors
and symbols distinguish your messages (purple), directly heard messages (teal),
and relayed messages (amber, ↪). A neutral question mark means the path is unknown.
Raw AX.25 reception retains actual source and digipeater evidence. Data2G and
connected message records carry the transmitting node ID: the origin is direct,
and another node forwarding cached history is relayed. This is self-reported
application provenance, not proof of physical RF route; a transparent RF
repeater remains undetectable. Older untagged messages and transports without
source metadata remain unknown.
Reception colors describe the path, independently of signature verification.
KISS ACKMODE receipts pace the broadcast writer with at most 32 frames in
flight. A receipt confirms only that Data2G sent a frame on air; it does not
mean a remote station received the message. Dropped frames are released from
the local window and can be repaired by later history or image requests.

Broadcast/KISS remains the default transport for chat, mentions, and history
sync. The optional `--transport connected` mode uses Data2G's VARA-compatible
ARQ session and TCP data stream for reliable directed transfer and history
sync. Connected delivery still requires peer sessions for relaying between
stations; KISS ACKMODE and ARQ link acknowledgments are modem/link-layer signals,
not application-level proof that a person read a message.

The optional `--transport ax25` mode uses raw KISS and wraps chat records in
AX.25 UI frames. It does not require the Data2G command service. Application
store-and-forward and history sync work over the configured AX.25 path, within
the airtime and frame-size limits of that path.
