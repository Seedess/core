# @seedess/core

The player-agnostic core of [Seedess](https://seedess.com): a WebRTC segment mesh, the
signaling client, and the SHA-256 verification primitive.

It does **not** know how a player fetches media. Use it directly only when building an
adapter for a player Seedess does not ship yet.

## Install

```bash
npm install @seedess/core
```

## Use

```js
import { SegmentMesh, Signaling, sha256Hex } from '@seedess/core';
```

| Export | Purpose |
| --- | --- |
| `SegmentMesh` | One mesh per stream: peer discovery, segment requests, verification, HTTP fallback. |
| `Signaling` | WebSocket client for the swarm protocol (`welcome`, `peer-join`, `signal`, `have`). |
| `sha256Hex` | Lowercase hex SHA-256 over an `ArrayBuffer`. |
| `DEFAULT_ICE` | Cloudflare STUN server. Add TURN for symmetric NAT. |

## Adapters

- [`@seedess/hls-p2p`](../hls-p2p) — hls.js fragment loader.
- [`@seedess/dash-p2p`](../dash-p2p) — dash.js Service Worker bridge.
- [`@seedess/p2p`](../p2p) — universal entry that picks an adapter.

Player adapters own the integration seam. The core owns the bytes.
