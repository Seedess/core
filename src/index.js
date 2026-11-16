/**
 * @seedess/core — the player-agnostic half of Seedess.
 *
 * Contains everything that does not know how a video player fetches bytes:
 * the WebRTC segment mesh, the signaling client and the verification hash.
 * Player adapters (@seedess/hls-p2p, @seedess/dash-p2p, ...) import these and
 * own the loader/bridge that plugs into a specific player.
 */

export { SegmentMesh } from './mesh.js';
export { Signaling } from './signaling.js';
export { sha256Hex } from './hash.js';

export const DEFAULT_ICE = [{ urls: ['stun:stun.cloudflare.com:3478'] }];
