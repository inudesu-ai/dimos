// Robot registry + subscription bookkeeping: robotId -> session, per-viewer
// watch/sub state, and routing of robot frames to exactly the viewers that
// asked for them. Transport-blind (sessions are peers behind small
// interfaces) so every transition is unit-testable without QUIC.
//
// Subscription flow: viewers sub/unsub channels of their watched robot; after
// every mutation the registry recomputes the active set (union over watchers)
// and pushes a `subs` snapshot upstream when it changed. Snapshots ride lossy
// datagrams, so server.ts also calls resendSnapshots() on an interval: any
// single delivery heals bridge state (the bridge ignores stale `n`).
//
// No app-level ping/pong pruning (deliberate T2 scope cut): the QUIC
// listener's maxIdleTimeout (30 s) + wt.closed already reap dead sessions,
// which is what turns a closed browser tab into an "encoding stopped" on the
// robot within a snapshot round-trip.
import {
  type ChannelSpec,
  type Delivery,
  type Msg,
  PROTOCOL_VERSION,
  type RobotInfo,
} from "@dimos/shared";
import {
  type ChannelPolicy,
  LatestChannel,
  parseRobotFrameHeader,
  ReliableChannel,
  type ViewerSink,
} from "./forward.ts";

/** What the registry needs from a robot session (fakeable in tests). */
export interface RobotPeer {
  /** Set by the session once a valid robot hello arrived. */
  readonly info: RobotInfo | null;
  readonly channels: ChannelSpec[];
  /** Control message upstream to the bridge (datagram: lossy, never blocks). */
  sendMsg(msg: Msg): void;
  close(reason: string): void;
}

/** What the registry needs from a viewer session (fakeable in tests). */
export interface ViewerPeer {
  readonly id: number;
  watched: string | null;
  readonly subs: Set<string>;
  readonly policies: Map<string, ChannelPolicy>;
  readonly sink: ViewerSink;
  /** True once a valid hello arrived; robots pushes skip un-greeted viewers. */
  greeted: boolean;
  /** Push channel chosen at hello time (bidi stream or datagrams). */
  sendMsg(msg: Msg): void;
}

interface ChannelInStats {
  delivery: Delivery;
  framesIn: number;
  bytesIn: number;
}

interface RobotEntry {
  peer: RobotPeer;
  /** Manifest delivery per channel; frame-header delivery is the fallback. */
  delivery: Map<string, Delivery>;
  /** Snapshot counter; survives takeover so the new session keeps ordering. */
  n: number;
  /** Last snapshot content, for change detection and periodic resend. */
  lastChs: string[];
  channelsIn: Map<string, ChannelInStats>;
}

export class Registry {
  #robots = new Map<string, RobotEntry>();
  #viewers = new Set<ViewerPeer>();
  #framesDropped = 0;
  #framesFromUnregistered = 0;

  addViewer(viewer: ViewerPeer): void {
    this.#viewers.add(viewer);
  }

  viewerClosed(viewer: ViewerPeer): void {
    if (!this.#viewers.delete(viewer)) return;
    if (viewer.watched !== null) this.#syncSubs(viewer.watched);
  }

  /**
   * Register a robot session after its valid hello. Idempotent under hello
   * resends; a second session with the same id is a takeover (the old one is
   * closed) so a restarted robot process reattaches without operator help.
   */
  registerRobot(peer: RobotPeer): void {
    const info = peer.info;
    if (info === null) return;
    const entry = this.#robots.get(info.id);
    if (entry !== undefined && entry.peer === peer) return; // hello resend
    const delivery = new Map(peer.channels.map((c) => [c.ch, c.delivery]));
    if (entry !== undefined) {
      console.log(`[relay] robot ${info.id} takeover: closing previous session`);
      const old = entry.peer;
      entry.peer = peer;
      entry.delivery = delivery;
      entry.channelsIn = new Map();
      old.close("replaced by new robot");
    } else {
      this.#robots.set(info.id, {
        peer,
        delivery,
        n: 0,
        lastChs: [],
        channelsIn: new Map(),
      });
      this.#pushRobots();
    }
    // Forced: gives a fresh bridge its baseline (possibly empty) and
    // reattaches surviving watchers after a robot restart.
    this.#syncSubs(info.id, true);
  }

  /** Session-closed hook; a no-op for a session already replaced by takeover. */
  robotClosed(peer: RobotPeer): void {
    const id = peer.info?.id;
    if (id === undefined) return;
    const entry = this.#robots.get(id);
    if (entry === undefined || entry.peer !== peer) return;
    this.#robots.delete(id);
    console.log(`[relay] robot ${id} disconnected`);
    // Viewers keep watched/subs: a returning robot reattaches seamlessly.
    this.#pushRobots();
  }

  /** Replies to viewer control messages; returns false if the session must close. */
  onViewerMsg(viewer: ViewerPeer, msg: Msg, reply: (msg: Msg) => void): boolean {
    switch (msg.t) {
      case "hello": {
        if (msg.v !== PROTOCOL_VERSION) {
          reply({
            t: "error",
            code: "version_mismatch",
            message: `protocol v${PROTOCOL_VERSION} required, got v${msg.v}`,
          });
          return false;
        }
        viewer.greeted = true;
        // Repeat hellos repeat both replies: the Python viewer's control
        // channel is datagrams, so this is its loss-healing path.
        reply({ t: "welcome", v: PROTOCOL_VERSION });
        reply(this.robotsMsg());
        break;
      }
      case "ping":
        reply({ t: "pong", n: msg.n, ts: msg.ts });
        break;
      case "watch": {
        const entry = this.#robots.get(msg.robotId);
        if (entry === undefined) {
          reply({ t: "error", code: "unknown_robot", message: `no robot ${msg.robotId}` });
          break;
        }
        const previous = viewer.watched;
        if (previous !== null && previous !== msg.robotId) {
          viewer.subs.clear();
          viewer.policies.clear();
        }
        viewer.watched = msg.robotId;
        if (previous !== null && previous !== msg.robotId) this.#syncSubs(previous);
        this.#syncSubs(msg.robotId);
        reply({ t: "manifest", robotId: msg.robotId, channels: entry.peer.channels });
        break;
      }
      case "sub":
      case "unsub": {
        if (viewer.watched === null) {
          reply({ t: "error", code: "no_watch", message: "watch a robot before sub/unsub" });
          break;
        }
        if (msg.t === "sub") viewer.subs.add(msg.ch);
        else viewer.subs.delete(msg.ch);
        this.#syncSubs(viewer.watched);
        break;
      }
    }
    return true;
  }

  /**
   * Route one robot data frame (raw bytes, already length-complete) to the
   * viewers watching this robot and subscribed to its channel.
   */
  onRobotFrame(peer: RobotPeer, bytes: Uint8Array): void {
    const id = peer.info?.id;
    const entry = id === undefined ? undefined : this.#robots.get(id);
    if (entry === undefined || entry.peer !== peer) {
      // Frames race registration (the stream loop starts at accept time).
      this.#framesFromUnregistered++;
      return;
    }
    const header = parseRobotFrameHeader(bytes);
    if (header === null) {
      this.#framesDropped++;
      console.log("[relay] dropping robot frame with invalid header");
      return;
    }
    const ch = header.ch;
    const delivery = entry.delivery.get(ch) ?? header.delivery;

    const stats = entry.channelsIn.get(ch) ?? { delivery, framesIn: 0, bytesIn: 0 };
    stats.delivery = delivery;
    stats.framesIn++;
    stats.bytesIn += bytes.byteLength;
    entry.channelsIn.set(ch, stats);

    for (const viewer of this.#viewers) {
      if (viewer.watched !== id || !viewer.subs.has(ch)) continue;
      let policy = viewer.policies.get(ch);
      if (policy === undefined || policy.delivery !== delivery) {
        policy = delivery === "reliable"
          ? new ReliableChannel(viewer.sink)
          : new LatestChannel(viewer.sink);
        viewer.policies.set(ch, policy);
      }
      policy.offer(bytes);
    }
  }

  /**
   * Re-push every robot's current snapshot with a fresh `n` (called on an
   * interval by server.ts). Content is unchanged, so a bridge that saw the
   * previous snapshot reconciles to a no-op; one that missed it heals.
   */
  resendSnapshots(): void {
    for (const entry of this.#robots.values()) {
      entry.peer.sendMsg({ t: "subs", chs: entry.lastChs, n: ++entry.n });
    }
  }

  robotsMsg(): Msg {
    return { t: "robots", robots: this.#robotInfos() };
  }

  #robotInfos(): RobotInfo[] {
    const robots: RobotInfo[] = [];
    for (const entry of this.#robots.values()) {
      if (entry.peer.info !== null) robots.push(entry.peer.info);
    }
    return robots;
  }

  stats(): unknown {
    return {
      robots: this.#robotInfos(),
      viewers: this.#viewers.size,
      framesDropped: this.#framesDropped,
      framesFromUnregistered: this.#framesFromUnregistered,
      perRobot: Object.fromEntries(
        [...this.#robots].map(([id, e]) => [id, {
          subs: e.lastChs,
          channels: Object.fromEntries(e.channelsIn),
        }]),
      ),
      perViewer: [...this.#viewers].map((v) => ({
        id: v.id,
        watched: v.watched,
        subs: [...v.subs].sort(),
        channels: Object.fromEntries(
          [...v.policies].map(([ch, p]) => [
            ch,
            { sent: p.sent, dropped: p.dropped, queued: p.queued() },
          ]),
        ),
      })),
    };
  }

  /** Sorted union of the subs of every viewer watching `robotId`. */
  #activeChs(robotId: string): string[] {
    const chs = new Set<string>();
    for (const viewer of this.#viewers) {
      if (viewer.watched !== robotId) continue;
      for (const ch of viewer.subs) chs.add(ch);
    }
    return [...chs].sort();
  }

  /**
   * Send a subs snapshot upstream iff the active set changed (or `force`).
   * Recomputed from scratch on every mutation: no incremental refcounts to
   * drift across watch switches, disconnects, and takeovers.
   */
  #syncSubs(robotId: string, force = false): void {
    const entry = this.#robots.get(robotId);
    if (entry === undefined) return;
    const chs = this.#activeChs(robotId);
    if (!force && chs.join("\n") === entry.lastChs.join("\n")) return;
    entry.lastChs = chs;
    entry.peer.sendMsg({ t: "subs", chs, n: ++entry.n });
    console.log(`[relay] robot ${robotId} active channels: [${chs.join(", ")}]`);
  }

  #pushRobots(): void {
    const msg = this.robotsMsg();
    for (const viewer of this.#viewers) {
      if (viewer.greeted) viewer.sendMsg(msg);
    }
  }
}
