# KISSChat-message-v1 ARAP content profile

KISSChat signs the UTF-8 bytes produced by the `identity-v1` content
canonicalization. ARAP hashes these bytes as supplied. This document defines
the application content; it does not define a signature algorithm or trust
policy.

The ARAP media type is `application/vnd.kisschat.message+json` and the context
is `kisschat/message-v1`. The profile version is the JSON integer `1` in the
`version` member. The message schema is a JSON object with these required
members:

| Member | JSON type and bounds |
| --- | --- |
| `version` | integer, exactly `1` |
| `origin` | 32 lowercase hexadecimal characters representing the 128-bit station UUID |
| `sequence` | integer, 1 through 4,294,967,295 |
| `timestamp` | integer Unix seconds, 0 through 4,294,967,295 |
| `channel` | `#` followed by 1–31 ASCII letters, digits, `_` or `-` |
| `nick` | nonempty printable Unicode string, at most 16 UTF-8 bytes |
| `callsign` | uppercase ARAP signing identity, 3–20 ASCII letters/digits with optional internal `/` groups |
| `body` | Unicode text, at most 4,096 UTF-8 bytes; printable characters plus tab/newline |
| `image` | optional object described below |

If present, `image` has exactly these members: `sha256` is 64 lowercase hex
characters over the thumbnail bytes, `size` is 1–8,192, `width` and `height`
are each 1–160 pixels, and `media_type` is exactly `image/jpeg`. An absent
image member means there is no attachment. `null`, an empty object, and empty
strings are not aliases for absence. An image may have an empty body caption.

The canonical byte encoding is compact JSON with lexicographically sorted
object keys, no insignificant whitespace, decimal integers without leading
zeroes, and UTF-8 output. Non-ASCII characters are emitted as UTF-8 except
U+2028 and U+2029, which are escaped as lowercase `\\u2028` and `\\u2029` to
match Go `encoding/json`. JSON strings use JSON escaping for quotes, backslash
and control characters. No BOM or trailing newline is included. The optional
image object is also sorted by key. Python and Go implementations and golden
vectors live in KISSChat `kisschat/auth.py` and ARAP `adapters/kisschat`.
Strings are not Unicode-normalized; NFC/NFD distinctions remain signed. Empty
body is valid only when the image member is present.

`callsign` is the signing identity configured for ARAP. It is separate from the
RF transport callsign, including any AX.25 SSID. A signature binds the content
and this asserted signing identity under ARAP policy; it does not authenticate
the immediate RF peer, relay path, or arbitrary transport aliases. Relay TTL,
receive time, routing data, receipts, proof state and local UI metadata are not
part of the signed bytes. The profile does bind the message origin UUID and
sequence, timestamp, channel, nick, exact body, and optional thumbnail
metadata, so changing any of those values changes the content digest.
