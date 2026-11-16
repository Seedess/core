/**
 * Minimal WebSocket signaling client.
 *
 * The server is expected to speak the Seedess swarm protocol:
 *   welcome { peers: string[] }
 *   peer-join { peerId }
 *   peer-leave { peerId }
 *   signal { from, to, data }
 *   have { peerId, seq }
 *
 * The client sends `signal` and `have`, and the room is keyed by `?room=`.
 */
export class Signaling {
  constructor({ url = 'api/swarm', room, peerId, role = 'peer' }) {
    this.url = url;
    this.peerId = peerId;
    this.room = room;
    this.role = role;
    this.ws = null;
    this.closed = false;
    this.retry = 0;
    this.onMessage = () => {};
    this.onOpen = () => {};
    this.onClose = () => {};
    this.onError = () => {};
  }

  connect() {
    if (this.closed) return;

    const url = new URL(this.url, location.href);
    url.searchParams.set('room', this.room);
    url.searchParams.set('peer', this.peerId);
    url.searchParams.set('role', this.role);
    url.protocol = url.protocol === 'https:' ? 'wss:' : 'ws:';

    const ws = new WebSocket(url);
    this.ws = ws;

    ws.addEventListener('open', () => {
      this.retry = 0;
      this.onOpen();
    });

    ws.addEventListener('message', (event) => {
      let message;
      try {
        message = JSON.parse(event.data);
      } catch {
        return;
      }
      this.onMessage(message);
    });

    ws.addEventListener('close', (event) => {
      this.onClose();
      if (event.code === 4001) {
        this.closed = true;
        this.onError(new Error(event.reason || 'This room already has a broadcaster.'));
        return;
      }
      this.scheduleReconnect();
    });

    ws.addEventListener('error', () => {
      // `close` follows; avoid double handling.
    });
  }

  send(message) {
    if (this.ws?.readyState === WebSocket.OPEN) {
      this.ws.send(JSON.stringify(message));
      return true;
    }
    return false;
  }

  close() {
    this.closed = true;
    this.ws?.close();
  }

  scheduleReconnect() {
    if (this.closed) return;
    if (this.retry >= 5) {
      this.onError(new Error('Signaling disconnected after 5 retries.'));
      return;
    }
    const delay = Math.min(8000, 500 + this.retry * 700);
    this.retry += 1;
    setTimeout(() => this.connect(), delay);
  }
}
