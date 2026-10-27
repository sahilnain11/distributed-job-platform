# One-minute illustrated walkthrough

**This is an offline illustrated explanation, not a live API, worker-crash test,
dashboard capture or benchmark recording.** It uses the cumulative teaching
snapshots already present in [the interactive explorer](../implementation-explorer.html).
No running services, saved jobs, credentials or customer data were used.

- [Watch/download the MP4](walkthrough.mp4): 60 seconds, 1280×720, H.264, no audio.
- [Open the local video player](index.html): controls, pause/seek, no autoplay.
- [Animated preview](preview.gif): 800×450, 12 frames, 60-second loop.
- [Full code-linked explanation](../implementation-review.md).

GitHub renders the GIF inline, but normally displays HTML source rather than
running the player. Open the player from a local checkout, download the MP4, or
use the README's raw video link. Playback/download behavior depends on your client.
The text transcript below is available without animation, video or JavaScript.

## Transcript / timestamps

Each scene lasts five seconds. Pause the MP4 to inspect the state. J1/A1/O1 are
teaching identities, not actual UUIDs. A **new example** resets the illustrative
scenario; the movie is not claiming that a completed job automatically returns
to RUNNING. Publication marking and worker execution may overlap in the real system.

| Time | Scene | Explanation |
| --- | --- | --- |
| 00:00–00:05 | Design | PostgreSQL owns state; RabbitMQ carries notifications. Logical roles can share one JAR or run separately |
| 00:05–00:10 | Acceptance | Job, payload, pending outbox and saved request outcome commit together before 202. Same scoped request/input replays the original response |
| 00:10–00:15 | Publication | Persistent reference, broker confirmation and routing check precede the outbox mark. No atomic DB/Rabbit transaction |
| 00:15–00:20 | Claim | Lock/recheck QUEUED, create fresh attempt/token/lease and increment counters. Claim commits before handler work |
| 00:20–00:25 | Completion | Recheck current attempt, fresh DB time, lease and runtime. Job/attempt/result commit atomically |
| 00:25–00:30 | ACK | Acknowledge after a known committed outcome or authoritative duplicate/stale check. Lost ACK after completion need not create another attempt |
| 00:30–00:35 | New duplicate example | Broker acceptance followed by DB-mark failure can resend. Second claim sees RUNNING and skips handler execution |
| 00:35–00:40 | New retry example | One accepted job can have FAILED, FAILED, COMPLETED attempts. Due retries create new outbox events; four total claims per cycle |
| 00:40–00:45 | New worker-loss example | After worker death, renewals stop. Expired ownership cannot complete even while stored state still says RUNNING |
| 00:45–00:50 | Recovery | Lock/recheck and record EXPIRED; if budget remains enter RETRYING, then use normal due retry/outbox/claim path |
| 00:50–00:55 | Replacement | A2 completes. Old A1 cannot overwrite platform state; external effects still require their own business idempotency |
| 00:55–01:00 | Evidence/limits | Tests and reports cover stated local conditions. No Kubernetes, HA, 1,000/s, exactly-once business effect or 99.9% availability guarantee |

Full captions and chapter order are in [storyboard.json](storyboard.json). This
movie adds no new execution behavior and does not change application defaults.

## Rebuild the media (optional development task)

Media rendering is **not** an application dependency or CI prerequisite. The
committed MP4/GIF work without Playwright or ffmpeg installed. For regeneration,
install Node.js, ffmpeg with `libx264`, and a local Playwright/Chromium toolchain.
Keep optional downloads in ignored build output rather than adding packages to
the Java project:

```sh
npm install --prefix target/demo-tools --no-save --package-lock=false playwright@1.62.1
target/demo-tools/node_modules/.bin/playwright install chromium
NODE_PATH="$PWD/target/demo-tools/node_modules" node tools/render_demo.cjs
```

The renderer reads the existing explorer's scenario data, creates twelve readable
frames, checks text overflow, then encodes MP4/GIF. It blocks external/network
page requests; there is no platform submission or live browser/session capture.
Work frames and snapshot diagnostics go under ignored build output. Encoder/font
versions may change binary hashes or sizes; review the output after rebuilding.

Generated public media must each remain below the repository's 1MB artifact
budget. Do not bypass the guard by committing raw screen captures, private data,
large rendering frames or recordings with credentials. Content checks cannot
automatically inspect every pixel; manually review the movie before publication.

Some editor-embedded file viewers do not support local video/range seeking. If
the video is blank or cannot seek there, open the MP4/player in a regular browser
or download it. The GIF/transcript remain alternatives; playback was verified in
ordinary Playwright Chromium with local files, including seeking to 00:36.

Validation for this addition: all twelve source snapshots were inspected, sample
frames visually reviewed, encoding checked for a 60-second H.264/no-audio stream
and a twelve-frame/60-second GIF. These are media checks, not new performance or
reliability experiments.