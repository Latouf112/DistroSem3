#!/usr/bin/env python3
# Decentralized mutual exclusion with failures and message loss
# Ricart Agrawala with Lamport clocks, view changes, leases, reliability shim
# Human readable logs

import argparse
import asyncio
import json
import random
import socket
import sys
import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Set, Tuple


def now() -> float:
    return time.time()


# UDP with drop simulation
class LossyTransport:
    def __init__(self, loop, bind_host: str, bind_port: int, pdrop: float):
        self.loop = loop
        self.pdrop = pdrop
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.bind((bind_host, bind_port))
        self.sock.setblocking(False)

    async def recv(self) -> Tuple[bytes, Tuple[str, int]]:
        return await self.loop.sock_recvfrom(self.sock, 64 * 1024)

    async def sendto(self, data: bytes, addr: Tuple[str, int]):
        if random.random() < self.pdrop:
            return
        await self.loop.sock_sendto(self.sock, data, addr)


# Reliability shim with ACK and retries
@dataclass
class OutMsg:
    payload: dict
    addr: Tuple[str, int]
    next_send: float = field(default_factory=now)
    attempts: int = 0


class ReliableMessenger:
    def __init__(self, node_id: int, transport: LossyTransport, backoff_base=0.05, backoff_cap=0.8):
        self.node_id = node_id
        self.transport = transport
        self.backoff_base = backoff_base
        self.backoff_cap = backoff_cap
        self.pending: Dict[str, OutMsg] = {}
        self.msg_seq = 0

    def new_msg_id(self) -> str:
        self.msg_seq += 1
        return f"{self.node_id}-{self.msg_seq}-{int(now()*1000)}"

    async def send(self, msg: dict, addr: Tuple[str, int], reliable: bool = True) -> str:
        if reliable:
            if 'msg_id' not in msg:
                msg['msg_id'] = self.new_msg_id()
            out = OutMsg(payload=msg, addr=addr)
            self.pending[msg['msg_id']] = out
            await self._actually_send(out)
            return msg['msg_id']
        else:
            await self.transport.sendto(json.dumps(msg).encode(), addr)
            return ''

    async def _actually_send(self, out: OutMsg):
        out.attempts += 1
        out.next_send = now() + min(self.backoff_base * (2 ** (out.attempts - 1)), self.backoff_cap)
        await self.transport.sendto(json.dumps(out.payload).encode(), out.addr)

    async def ack(self, for_msg_id: str, addr: Tuple[str, int]):
        m = {'type': 'ACK', 'for': for_msg_id}
        await self.transport.sendto(json.dumps(m).encode(), addr)

    def handle_ack(self, for_id: str):
        if for_id in self.pending:
            del self.pending[for_id]

    async def tick(self):
        now_ts = now()
        to_resend = [m for m in self.pending.values() if m.next_send <= now_ts]
        for out in to_resend:
            await self._actually_send(out)


# Node logic
class Node:
    def __init__(self, node_id: int, bind: Tuple[str, int], peers: List[Tuple[str, int]],
                 pdrop: float, lease_s: int, hb_s: int, fail_lambda: float,
                 silent_min: float, silent_max: float):
        self.node_id = node_id
        self.bind = bind
        self.peers = peers
        self.loop = asyncio.get_event_loop()
        self.transport = LossyTransport(self.loop, bind[0], bind[1], pdrop)
        self.msg = ReliableMessenger(node_id, self.transport)

        self.members: Set[int] = set([node_id] + [i+1 for i in range(len(peers))])
        self.addr_by_id: Dict[int, Tuple[str, int]] = {node_id: bind}
        for idx, addr in enumerate(peers):
            self.addr_by_id[idx + 1] = addr

        self.lamport = 0
        self.view_id = 0
        self.state_in_cs = False
        self.state_want_cs = False
        self.req_ts: Optional[int] = None
        self.waiters: Set[int] = set()
        self.deferred: Set[int] = set()
        self.cs_lease = lease_s
        self.cs_expiry = 0.0
        self.hb_interval = hb_s
        self.last_hb_sent: Dict[int, float] = {}
        self.last_heard: Dict[int, float] = {i: now() for i in self.addr_by_id if i != self.node_id}
        self.suspected: Set[int] = set()
        self.down: Set[int] = set()

        self.fail_lambda = fail_lambda
        self.silent_min = silent_min
        self.silent_max = silent_max
        self.failed_until = 0.0
        self.permanent_down = False

        self.fencing_token = 0

    def log(self, event: str, **details):
        ts = round(now(), 3)
        info = ' '.join(f"{k}={v}" for k, v in details.items())
        print(f"[{ts}] node {self.node_id} view={self.view_id} event={event} {info}", flush=True)

    async def start(self):
        asyncio.create_task(self.receiver())
        asyncio.create_task(self.ticker())
        asyncio.create_task(self.failure_sim())
        asyncio.create_task(self.random_requester())

    def addr(self, nid: int) -> Tuple[str, int]:
        return self.addr_by_id[nid]

    def up_members(self) -> Set[int]:
        return set(i for i in self.members if i not in self.down)

    async def ticker(self):
        while True:
            await self.msg.tick()
            await self.send_heartbeats()
            self.update_suspicions()
            await self.maybe_view_change()
            if self.state_in_cs and now() + 1.0 >= self.cs_expiry:
                await self.renew_lease()
            await asyncio.sleep(0.05)

    async def send_heartbeats(self):
        for peer in self.addr_by_id:
            if peer == self.node_id:
                continue
            if peer in self.down:
                continue
            last = self.last_hb_sent.get(peer, 0)
            if now() - last >= self.hb_interval:
                m = {'type': 'HB', 'view': self.view_id, 'from': self.node_id, 'hb_seq': int(now()*1000)}
                await self.msg.send(m, self.addr(peer), reliable=False)
                self.last_hb_sent[peer] = now()

    def update_suspicions(self):
        for peer in list(self.addr_by_id.keys()):
            if peer == self.node_id:
                continue
            last = self.last_heard.get(peer, 0)
            gap = now() - last
            if gap > 5 and peer not in self.down:
                self.suspected.add(peer)
            if gap > 8 and peer not in self.down:
                self.down.add(peer)
                if peer in self.members:
                    self.members.remove(peer)
                self.log('peer_down', peer=peer)
                if self.state_want_cs and peer in self.waiters:
                    self.waiters.discard(peer)

    async def maybe_view_change(self):
        if not self.suspected:
            return
        self.view_id += 1
        new_members = sorted(list(self.up_members()))
        msg = {'type': 'VIEW', 'view': self.view_id, 'members': new_members, 'from': self.node_id}
        await self.broadcast(msg)
        self.log('view_change', members=new_members)
        self.suspected.clear()

    async def renew_lease(self):
        self.cs_expiry = now() + self.cs_lease
        msg = {'type': 'RENEW', 'view': self.view_id, 'from': self.node_id, 'token': self.fencing_token}
        await self.broadcast(msg)
        self.log('lease_renew', token=self.fencing_token, until=self.cs_expiry)

    async def broadcast(self, msg: dict):
        for nid in self.up_members():
            if nid == self.node_id:
                continue
            await self.msg.send(dict(msg), self.addr(nid), reliable=True)

    async def receiver(self):
        while True:
            data, addr = await self.transport.recv()
            try:
                m = json.loads(data.decode())
            except Exception:
                continue
            await self.handle_message(m, addr)

    async def handle_message(self, m: dict, addr: Tuple[str, int]):
        mtype = m.get('type')
        if mtype == 'ACK':
            self.msg.handle_ack(m.get('for', ''))
            return
        mid = m.get('msg_id')
        if mid:
            await self.msg.ack(mid, addr)
        src = m.get('from')
        if src is not None:
            self.last_heard[src] = now()
        if mtype == 'HB':
            return
        if mtype == 'VIEW':
            v = m.get('view', 0)
            if v > self.view_id:
                self.view_id = v
                mem = set(m.get('members', []))
                if mem:
                    self.members = mem
                self.down -= self.members
                self.log('view_install', members=sorted(list(self.members)))
                if self.state_want_cs:
                    await self.rebroadcast_request()
            return
        if mtype == 'JOIN':
            nid = m.get('from')
            self.members.add(nid)
            self.down.discard(nid)
            self.view_id = max(self.view_id, m.get('view', 0)) + 1
            msg = {'type': 'VIEW', 'view': self.view_id, 'members': sorted(list(self.up_members())), 'from': self.node_id}
            await self.broadcast(msg)
            return
        if mtype == 'REQ':
            t = m.get('ts', 0)
            j = m.get('from')
            self.lamport = max(self.lamport, t) + 1
            defer = False
            if self.state_in_cs:
                defer = True
            elif self.state_want_cs and self.req_ts is not None:
                my = (self.req_ts, self.node_id)
                other = (t, j)
                if my < other:
                    defer = True
            if defer:
                self.deferred.add(j)
                self.log('defer', from_node=j, ts=t)
            else:
                rep = {'type': 'REP', 'view': self.view_id, 'for_ts': t, 'from': self.node_id}
                await self.msg.send(rep, self.addr(j), reliable=True)
                self.log('reply', to=j, for_ts=t)
            return
        if mtype == 'REL':
            j = m.get('from')
            if j in self.deferred:
                rep = {'type': 'REP', 'view': self.view_id, 'for_ts': m.get('for_ts'), 'from': self.node_id}
                await self.msg.send(rep, self.addr(j), reliable=True)
                self.deferred.discard(j)
                self.log('release_reply', to=j)
            return
        if mtype == 'REP':
            j = m.get('from')
            if self.state_want_cs:
                self.waiters.discard(j)
                self.log('got_reply', from_node=j, waiters=list(self.waiters))
                if not self.waiters and not self.state_in_cs:
                    await self.enter_cs()
            return
        if mtype == 'RENEW':
            self.log('saw_renew', from_node=m.get('from'), token=m.get('token'))
            return

    async def request_cs(self):
        if self.state_want_cs or self.state_in_cs:
            return
        self.state_want_cs = True
        self.lamport += 1
        self.req_ts = self.lamport
        self.waiters = set(self.up_members())
        self.waiters.discard(self.node_id)
        msg = {'type': 'REQ', 'view': self.view_id, 'ts': self.req_ts, 'from': self.node_id}
        await self.broadcast(msg)
        self.log('broadcast_req', ts=self.req_ts, waiters=list(self.waiters))
        if not self.waiters:
            await self.enter_cs()

    async def rebroadcast_request(self):
        if not self.state_want_cs:
            return
        self.waiters = set(self.up_members())
        self.waiters.discard(self.node_id)
        msg = {'type': 'REQ', 'view': self.view_id, 'ts': self.req_ts, 'from': self.node_id}
        await self.broadcast(msg)
        self.log('rebroadcast_req', ts=self.req_ts, waiters=list(self.waiters))

    async def enter_cs(self):
        self.state_in_cs = True
        self.state_want_cs = False
        self.fencing_token = max(self.fencing_token, self.lamport)
        self.cs_expiry = now() + self.cs_lease
        self.log('enter_cs', token=self.fencing_token, until=self.cs_expiry)
        await asyncio.sleep(random.uniform(1.0, 3.0))
        await self.leave_cs()

    async def leave_cs(self):
        if not self.state_in_cs:
            return
        self.state_in_cs = False
        self.lamport += 1
        rel = {'type': 'REL', 'view': self.view_id, 'for_ts': self.req_ts, 'from': self.node_id}
        await self.broadcast(rel)
        for j in list(self.deferred):
            rep = {'type': 'REP', 'view': self.view_id, 'for_ts': self.req_ts, 'from': self.node_id}
            await self.msg.send(rep, self.addr(j), reliable=True)
            self.log('send_deferred_reply', to=j)
        self.deferred.clear()
        self.req_ts = None
        self.log('leave_cs')

    async def random_requester(self):
        await asyncio.sleep(random.uniform(1.0, 2.0))
        while True:
            gap = random.uniform(2.0, 6.0)
            await asyncio.sleep(gap)
            await self.request_cs()

    async def failure_sim(self):
        while True:
            wait = random.expovariate(self.fail_lambda) if self.fail_lambda > 0 else 1e9
            await asyncio.sleep(wait)
            silent = random.uniform(self.silent_min, self.silent_max)
            perm = random.random() < 0.02
            end = now() + silent
            self.log('fail_stop', silent=silent, permanent=perm)
            # while failed, drop all incoming by not running logic
            fail_until = end
            while now() < fail_until:
                await asyncio.sleep(0.2)
            if perm:
                self.log('permanent_down')
                return
            # rejoin
            self.view_id += 1
            j = {'type': 'JOIN', 'view': self.view_id, 'from': self.node_id}
            await self.broadcast(j)
            self.lamport += 1
            self.state_in_cs = False
            self.state_want_cs = False
            self.deferred.clear()
            self.waiters.clear()
            self.req_ts = None
            self.log('rejoin_after_failure')


async def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--id', type=int, required=True)
    ap.add_argument('--bind', type=str, required=True, help='host:port for this node')
    ap.add_argument('--peers', type=str, required=True, help='comma separated host:port for all nodes in the cluster, including this node, ordered by node id')
    ap.add_argument('--pdrop', type=float, default=0.05)
    ap.add_argument('--lease', type=int, default=5)
    ap.add_argument('--hb', type=int, default=1)
    ap.add_argument('--fail_lambda', type=float, default=0.00462)
    ap.add_argument('--silent_min', type=float, default=2.0)
    ap.add_argument('--silent_max', type=float, default=8.0)
    args = ap.parse_args()

    bind_host, bind_port = args.bind.split(':')
    bind = (bind_host, int(bind_port))

    peers = []
    for hp in args.peers.split(','):
        h, p = hp.strip().split(':')
        peers.append((h, int(p)))

    if args.id < 1 or args.id > len(peers):
        print('bad id')
        sys.exit(1)

    node = Node(
        node_id=args.id,
        bind=bind,
        peers=peers,
        pdrop=args.pdrop,
        lease_s=args.lease,
        hb_s=args.hb,
        fail_lambda=args.fail_lambda,
        silent_min=args.silent_min,
        silent_max=args.silent_max,
    )

    await node.start()
    try:
        while True:
            await asyncio.sleep(1)
    except KeyboardInterrupt:
        pass

if __name__ == '__main__':
    asyncio.run(main())
