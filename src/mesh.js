import { sha256Hex } from './hash.js';

const CHUNK = 16 * 1024;
const MAX_BUFFERED = 1024 * 1024;
const FRAME_HEADER = 11;
const FRAME_DATA = 1;

/**
 * One mesh per stream. Peers exchange HLS segments over WebRTC data channels.
 * The HTTP source is always the floor: no peer, timeout, nack or bad hash
 * falls through to it.
 *
 * The mesh is transport only. The caller supplies metadata per segment
 * (`size`, optional `sha256`) and the HTTP URL to fall back to.
 */
export class SegmentMesh {
  constructor({
    signaling,
    peerId,
    iceServers = [],
    role = 'peer',
    onStats = () => {},
    onPeers = () => {},
    requestTimeout = 30000,
    haveWaitMs = 800,
    hedgeDelay = 1000,
    verifyMode = 'auto',
    profile = false,
    offloadCap = null,
    meteredDefault = false,
  }) {
    this.signaling = signaling;
    this.peerId = peerId;
    this.iceServers = iceServers;
    this.role = role;
    this.onStats = onStats;
    this.onPeers = onPeers;
    this.requestTimeout = requestTimeout;
    this.haveWaitMs = haveWaitMs;
    this.hedgeDelay = hedgeDelay;
    this.verifyMode = verifyMode;
    this.profile = profile;
    this.offloadCap = offloadCap;
    this.meteredDefault = meteredDefault;

    this.meta = new Map();
    this.cache = new Map();
    this.have = new Set();
    this.peers = new Map();
    this.stopped = false;
    this.joined = false;
    this.peerWaiters = [];
    this.openWaiters = [];
    this.haveWaiters = new Map();

    this.stats = {
      p2pBytes: 0,
      httpBytes: 0,
      segmentsP2P: 0,
      segmentsHttp: 0,
      hashFailures: 0,
      unverified: 0,
      fallbacks: 0,
      error: null,
    };
  }

  start() {
    this.signaling.onMessage = (message) => this.handleSignalMessage(message);
    this.signaling.onOpen = () => this.emit();
    this.signaling.onClose = () => this.emit();
    this.signaling.onError = (error) => {
      this.stats.error = error.message;
      this.emit();
    };
    this.signaling.connect();
  }

  stop() {
    this.stopped = true;
    for (const peer of [...this.peers.values()]) this.removePeer(peer.id);
    this.signaling.close();
  }

  setMeta(seq, meta) {
    if (meta) this.meta.set(seq, meta);
    return this.meta.get(seq) ?? null;
  }

  /** True when the connection looks metered (saveData or a slow effective type). */
  isMeteredConnection() {
    try {
      const connection = navigator.connection;
      if (!connection) return false;
      return Boolean(connection.saveData) ||
        connection.effectiveType === 'slow-2g' ||
        connection.effectiveType === '2g';
    } catch {
      return false;
    }
  }

  /** Prefer CDN on metered connections when the stream asks for that default. */
  p2pDisabled() {
    return this.meteredDefault && this.isMeteredConnection();
  }

  /** Soft offload cap: stop pulling from peers once the running ratio hits it. */
  overOffloadCap() {
    if (this.offloadCap == null || this.offloadCap >= 100) return false;
    const total = this.stats.p2pBytes + this.stats.httpBytes;
    if (total <= 0) return false;
    return (this.stats.p2pBytes / total) * 100 >= this.offloadCap;
  }

  waitForPeers(timeoutMs = 2500) {
    if (this.peers.size > 0) return Promise.resolve(true);
    if (this.joined) return Promise.resolve(false);
    return this.waitOn(this.peerWaiters, timeoutMs, false);
  }

  waitForOpenPeer(timeoutMs = 4000) {
    if ([...this.peers.values()].some((peer) => peer.dc?.readyState === 'open')) {
      return Promise.resolve(true);
    }
    return this.waitOn(this.openWaiters, timeoutMs, false);
  }

  waitForHave(seq, timeoutMs = this.haveWaitMs) {
    if ([...this.peers.values()].some((peer) => peer.have.has(seq))) {
      return Promise.resolve(true);
    }
    if (this.peers.size === 0) return Promise.resolve(false);

    return new Promise((resolve) => {
      let settled = false;
      const done = (value) => {
        if (settled) return;
        settled = true;
        clearTimeout(timer);
        resolve(value);
      };
      const timer = setTimeout(() => done(false), timeoutMs);
      const waiters = this.haveWaiters.get(seq) ?? new Set();
      waiters.add(done);
      this.haveWaiters.set(seq, waiters);
    });
  }

  waitOn(list, timeoutMs, fallback) {
    return new Promise((resolve) => {
      let settled = false;
      const done = (value) => {
        if (settled) return;
        settled = true;
        clearTimeout(timer);
        resolve(value);
      };
      const timer = setTimeout(() => done(fallback), timeoutMs);
      list.push(done);
    });
  }

  settlePeerWaiters(value) {
    const waiters = this.peerWaiters.splice(0);
    for (const resolve of waiters) resolve(value);
  }

  settleOpenWaiters() {
    const waiters = this.openWaiters.splice(0);
    for (const resolve of waiters) resolve(true);
  }

  settleHaveWaiters(seq) {
    const waiters = this.haveWaiters.get(seq);
    if (!waiters) return;
    this.haveWaiters.delete(seq);
    for (const resolve of waiters) resolve(true);
  }

  handleSignalMessage(message) {
    if (!message || typeof message !== 'object') return;

    if (message.t === 'welcome') {
      this.joined = true;
      for (const peerId of message.peers ?? []) {
        if (peerId !== this.peerId) this.createPeer(peerId, true);
      }
      this.settlePeerWaiters(this.peers.size > 0);
      this.emitPeers();
      return;
    }

    if (message.t === 'peer-join') {
      this.settlePeerWaiters(true);
      this.emitPeers();
      return;
    }

    if (message.t === 'peer-leave') {
      this.removePeer(message.peerId);
      return;
    }

    if (message.t === 'have') {
      const peer = this.peers.get(message.peerId);
      if (peer && Number.isInteger(message.seq)) {
        peer.have.add(message.seq);
        this.settleHaveWaiters(message.seq);
        this.emitPeers();
      }
      return;
    }

    if (message.t === 'signal') {
      this.handleSignal(message.from, message.data).catch((error) => {
        console.warn('seedess: signal failed', message.from, error);
      });
    }
  }

  createPeer(remoteId, initiator) {
    const existing = this.peers.get(remoteId);
    if (existing) return existing;

    const pc = new RTCPeerConnection({ iceServers: this.iceServers });
    const peer = {
      id: remoteId,
      pc,
      dc: null,
      have: new Set(),
      pending: new Map(),
      incoming: new Map(),
      pendingCandidates: [],
      initiator,
    };
    this.peers.set(remoteId, peer);

    pc.addEventListener('icecandidate', (event) => {
      if (event.candidate) {
        this.signal(remoteId, {
          type: 'ice',
          candidate: event.candidate.toJSON(),
        });
      }
    });

    pc.addEventListener('connectionstatechange', () => {
      if (pc.connectionState === 'failed' || pc.connectionState === 'closed') {
        this.removePeer(remoteId);
      } else {
        this.emitPeers();
      }
    });

    pc.addEventListener('datachannel', (event) => {
      this.attachChannel(peer, event.channel);
    });

    if (initiator) {
      const dc = pc.createDataChannel('seedess', { ordered: true });
      this.attachChannel(peer, dc);
      this.makeOffer(peer).catch((error) => {
        console.warn('seedess: offer failed', remoteId, error);
      });
    }

    this.emitPeers();
    return peer;
  }

  async makeOffer(peer) {
    const offer = await peer.pc.createOffer();
    await peer.pc.setLocalDescription(offer);
    this.signal(peer.id, { type: 'offer', sdp: peer.pc.localDescription });
  }

  async handleSignal(from, data) {
    if (!from || !data) return;

    let peer = this.peers.get(from);
    if (!peer) peer = this.createPeer(from, false);

    if (data.type === 'offer') {
      await peer.pc.setRemoteDescription(data.sdp);
      await this.flushCandidates(peer);
      const answer = await peer.pc.createAnswer();
      await peer.pc.setLocalDescription(answer);
      this.signal(from, { type: 'answer', sdp: peer.pc.localDescription });
      return;
    }

    if (data.type === 'answer') {
      await peer.pc.setRemoteDescription(data.sdp);
      await this.flushCandidates(peer);
      return;
    }

    if (data.type === 'ice') {
      if (peer.pc.remoteDescription) {
        await peer.pc.addIceCandidate(data.candidate);
      } else {
        peer.pendingCandidates.push(data.candidate);
      }
    }
  }

  async flushCandidates(peer) {
    const queued = peer.pendingCandidates.splice(0);
    for (const candidate of queued) await peer.pc.addIceCandidate(candidate);
  }

  signal(to, data) {
    this.signaling.send({ t: 'signal', to, data });
  }

  attachChannel(peer, dc) {
    peer.dc = dc;
    dc.binaryType = 'arraybuffer';
    dc.bufferedAmountLowThreshold = 256 * 1024;

    // The answerer can receive a channel that is already open; in that case
    // 'open' never fires. Announce the current bitfield in both cases.
    const announce = () => {
      if (peer.announced) return;
      peer.announced = true;
      this.sendControl(peer, {
        t: 'hello',
        peerId: this.peerId,
        have: [...this.have],
      });
      this.settleOpenWaiters();
      this.emitPeers();
    };

    dc.addEventListener('open', announce);
    if (dc.readyState === 'open') announce();

    dc.addEventListener('message', (event) => {
      if (typeof event.data === 'string') this.handleControl(peer, event.data);
      else if (event.data instanceof ArrayBuffer) this.handleChunk(peer, event.data);
    });

    dc.addEventListener('close', () => this.removePeer(peer.id));
    dc.addEventListener('error', () => {});
  }

  handleControl(peer, raw) {
    let message;
    try {
      message = JSON.parse(raw);
    } catch {
      return;
    }

    if (message.t === 'hello') {
      peer.have = new Set(
        Array.isArray(message.have)
          ? message.have.filter((seq) => Number.isInteger(seq))
          : []
      );
      for (const seq of peer.have) this.settleHaveWaiters(seq);
      this.emitPeers();
      return;
    }

    if (message.t === 'req' && Number.isInteger(message.seq)) {
      this.sendSegment(peer, message.seq).catch((error) => {
        console.warn('seedess: send segment failed', peer.id, message.seq, error);
      });
      return;
    }

    if (message.t === 'end' && Number.isInteger(message.seq)) {
      this.finishIncoming(peer, message.seq, message.size);
      return;
    }

    if (message.t === 'nack' && Number.isInteger(message.seq)) {
      this.failPending(peer, message.seq, new Error('peer nack'));
      return;
    }

    if (message.t === 'bye') this.removePeer(peer.id);
  }

  incomingFor(peer, seq) {
    let incoming = peer.incoming.get(seq);
    if (!incoming) {
      incoming = { chunks: [], received: 0 };
      peer.incoming.set(seq, incoming);
    }
    return incoming;
  }

  handleChunk(peer, buffer) {
    if (buffer.byteLength < FRAME_HEADER) return;
    const view = new DataView(buffer);
    if (view.getUint8(0) !== FRAME_DATA) return;

    const seq = view.getUint32(1);
    const offset = view.getUint32(5);
    const length = view.getUint16(9);
    // The frame carries one chunk, so only the chunk has to fit. The absolute
    // segment offset can be larger than the frame.
    if (length > buffer.byteLength - FRAME_HEADER) return;

    const payload = new Uint8Array(buffer, FRAME_HEADER, length).slice();
    const incoming = this.incomingFor(peer, seq);
    incoming.chunks.push({ offset, payload });
    incoming.received += length;
  }

  finishIncoming(peer, seq, size) {
    const incoming = peer.incoming.get(seq);
    if (!incoming) return;
    peer.incoming.delete(seq);

    const total =
      Number.isInteger(size) && size > 0
        ? size
        : incoming.chunks.reduce(
            (max, chunk) => Math.max(max, chunk.offset + chunk.payload.length),
            0
          );

    if (incoming.received !== total) {
      this.failPending(peer, seq, new Error('incomplete peer segment'));
      return;
    }

    const bytes = new Uint8Array(total);
    for (const chunk of incoming.chunks) bytes.set(chunk.payload, chunk.offset);

    this.acceptPeerBytes(peer, seq, bytes.buffer);
  }

  acceptPeerBytes(peer, seq, buffer) {
    this.verifyBytes(buffer, this.meta.get(seq))
      .then((ok) => {
        if (!ok) {
          this.stats.hashFailures += 1;
          this.failPending(peer, seq, new Error('peer segment verification failed'));
          this.emit();
          return;
        }

        this.cache.set(seq, buffer);
        this.have.add(seq);
        this.broadcastHave(seq);
        this.stats.p2pBytes += buffer.byteLength;
        this.stats.segmentsP2P += 1;
        this.settlePending(peer, seq, buffer);
        this.emit();
      })
      .catch((error) => this.failPending(peer, seq, error));
  }

  async sendSegment(peer, seq) {
    const bytes = this.cache.get(seq);
    if (!bytes) {
      this.sendControl(peer, { t: 'nack', seq });
      return;
    }
    if (!peer.dc || peer.dc.readyState !== 'open') return;

    for (let offset = 0; offset < bytes.byteLength; offset += CHUNK) {
      if (!peer.dc || peer.dc.readyState !== 'open') return;
      await this.waitForDrain(peer.dc);

      const length = Math.min(CHUNK, bytes.byteLength - offset);
      const frame = new Uint8Array(FRAME_HEADER + length);
      const view = new DataView(frame.buffer);
      view.setUint8(0, FRAME_DATA);
      view.setUint32(1, seq);
      view.setUint32(5, offset);
      view.setUint16(9, length);
      frame.set(new Uint8Array(bytes, offset, length), FRAME_HEADER);

      try {
        peer.dc.send(frame.buffer);
      } catch (error) {
        console.warn('seedess: data channel send failed', peer.id, error);
        return;
      }
    }

    this.sendControl(peer, { t: 'end', seq, size: bytes.byteLength });
  }

  async waitForDrain(dc) {
    if (dc.bufferedAmount <= MAX_BUFFERED) return;

    await new Promise((resolve) => {
      let done = false;
      const finish = () => {
        if (done) return;
        done = true;
        dc.removeEventListener('bufferedamountlow', finish);
        clearTimeout(timer);
        resolve();
      };
      const timer = setTimeout(finish, 5000);
      dc.addEventListener('bufferedamountlow', finish);
    });
  }

  async getSegment(seq, meta = null) {
    const startedAt = performance.now();
    const log = (route, bytes) => {
      if (!this.profile) return;
      console.log(
        JSON.stringify({
          type: 'seedess-mesh',
          event: 'segment',
          seq,
          route,
          role: this.role,
          ms: Math.round(performance.now() - startedAt),
          bytes: bytes?.byteLength ?? null,
        })
      );
    };

    this.setMeta(seq, meta);

    if (this.cache.has(seq)) {
      const bytes = this.cache.get(seq);
      log('cache', bytes);
      return bytes;
    }

    if (this.role === 'seed') {
      const bytes = await this.fetchHttp(seq, meta);
      log('seed-http', bytes);
      return bytes;
    }

    if (this.p2pDisabled() || this.overOffloadCap()) {
      const bytes = await this.fetchHttp(seq, meta);
      log('http-cap', bytes);
      return bytes;
    }

    if (this.peers.size > 0) await this.waitForHave(seq, this.haveWaitMs);

    const candidates = [...this.peers.values()].filter(
      (peer) => peer.have.has(seq) && peer.dc?.readyState === 'open'
    );

    const peer = candidates[0];
    if (peer) {
      const result = await this.requestWithHedge(peer, seq, meta);
      if (result.source === 'http') this.stats.fallbacks += 1;
      log(result.source, result.bytes);
      return result.bytes;
    }

    const bytes = await this.fetchHttp(seq, meta);
    log('http', bytes);
    return bytes;
  }

  /**
   * Ask a peer, but start the origin fetch if the peer has not produced the
   * segment within `hedgeDelay`. Whichever finishes first wins, so a slow or
   * stalled peer never holds up playback.
   */
  requestWithHedge(peer, seq, meta) {
    return new Promise((resolve, reject) => {
      let settled = false;
      let timer = null;
      let httpStarted = false;

      const finish = (value) => {
        if (settled) return;
        settled = true;
        if (timer) clearTimeout(timer);
        resolve(value);
      };
      const fail = (error) => {
        if (settled) return;
        settled = true;
        if (timer) clearTimeout(timer);
        reject(error);
      };
      const startHttp = () => {
        if (settled || httpStarted) return;
        httpStarted = true;
        if (timer) {
          clearTimeout(timer);
          timer = null;
        }
        this.fetchHttp(seq, meta).then(
          (bytes) => finish({ source: 'http', bytes }),
          fail
        );
      };

      this.requestFromPeer(peer, seq).then(
        (bytes) => finish({ source: 'peer', bytes }),
        () => startHttp()
      );

      timer = setTimeout(startHttp, this.hedgeDelay);
    });
  }

  requestFromPeer(peer, seq) {
    if (peer.pending.has(seq)) {
      return Promise.reject(new Error('duplicate request'));
    }

    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        peer.pending.delete(seq);
        peer.incoming.delete(seq);
        reject(new Error('peer request timed out'));
      }, this.requestTimeout);

      peer.pending.set(seq, { resolve, reject, timer });

      if (!this.sendControl(peer, { t: 'req', seq })) {
        clearTimeout(timer);
        peer.pending.delete(seq);
        reject(new Error('peer channel closed'));
      }
    });
  }

  settlePending(peer, seq, bytes) {
    const pending = peer.pending.get(seq);
    if (!pending) return;
    clearTimeout(pending.timer);
    peer.pending.delete(seq);
    pending.resolve(bytes);
  }

  failPending(peer, seq, error) {
    const pending = peer.pending.get(seq);
    if (!pending) return;
    clearTimeout(pending.timer);
    peer.pending.delete(seq);
    pending.reject(error);
  }

  async fetchHttp(seq, meta = null) {
    const info = meta ?? this.meta.get(seq);
    const url = info?.url;
    if (!url) throw new Error(`No HTTP URL for segment ${seq}`);

    const response = await fetch(url);
    if (!response.ok) {
      throw new Error(`${response.status} ${response.statusText} for ${url}`);
    }

    const bytes = await response.arrayBuffer();
    if (!(await this.verifyBytes(bytes, info))) {
      this.stats.hashFailures += 1;
      this.emit();
      throw new Error(`HTTP segment verification failed for ${url}`);
    }

    this.cache.set(seq, bytes);
    this.have.add(seq);
    this.broadcastHave(seq);
    this.stats.httpBytes += bytes.byteLength;
    this.stats.segmentsHttp += 1;
    this.emit();
    return bytes;
  }

  async verifyBytes(buffer, meta) {
    if (!meta) return this.verifyMode !== 'sha256';
    if (meta.size && buffer.byteLength !== meta.size) return false;

    if (meta.sha256 && this.verifyMode !== 'size') {
      return (await sha256Hex(buffer)) === meta.sha256;
    }

    // No hash available. "auto" accepts it and counts it; "sha256" refuses.
    if (this.verifyMode === 'sha256') return false;
    this.stats.unverified += 1;
    this.emit();
    return true;
  }

  broadcastHave(seq) {
    this.signaling.send({ t: 'have', seq });
  }

  sendControl(peer, message) {
    if (!peer.dc || peer.dc.readyState !== 'open') return false;
    try {
      peer.dc.send(JSON.stringify(message));
      return true;
    } catch {
      return false;
    }
  }

  removePeer(peerId) {
    const peer = this.peers.get(peerId);
    if (!peer) return;

    for (const pending of peer.pending.values()) {
      clearTimeout(pending.timer);
      pending.reject(new Error('peer left'));
    }
    peer.pending.clear();

    try {
      peer.pc.close();
    } catch {
      // Already closed.
    }

    this.peers.delete(peerId);
    this.emitPeers();
    this.emit();
  }

  emit() {
    this.onStats({
      ...this.stats,
      peers: this.peers.size,
      connectedPeers: [...this.peers.values()].filter(
        (peer) => peer.dc?.readyState === 'open'
      ).length,
    });
  }

  emitPeers() {
    this.onPeers(
      [...this.peers.values()].map((peer) => ({
        id: peer.id,
        connected: peer.dc?.readyState === 'open',
        have: peer.have.size,
      }))
    );
  }
}
