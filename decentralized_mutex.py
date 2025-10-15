#!/usr/bin/env python3
import asyncio, aiohttp, random, time, argparse, json, signal
from aiohttp import web
from typing import Dict, Optional

############################################################
# CLI-argument
############################################################

parser = argparse.ArgumentParser(description="Decentralized Mutual Exclusion (majority voting)")
parser.add_argument("--id", type=int, required=True, help="My node id (0..N-1)")
parser.add_argument("--port", type=int, required=True, help="HTTP port to listen on")
parser.add_argument("--peers", type=str, required=True,
                    help='JSON list of peers like [{"id":0,"url":"http://hostA:8080"}, ...]')
parser.add_argument("--resource", type=str, default="res1")
parser.add_argument("--req-every", type=float, default=8.0, help="mean seconds between CS requests")
parser.add_argument("--cs-time", type=float, default=3.0, help="seconds spent inside CS")
parser.add_argument("--p-success", type=float, default=0.95, help="probability that a message is delivered")
parser.add_argument("--fail-prob", type=float, default=0.0, help="probability per second to fail-stop")
parser.add_argument("--fail-min", type=float, default=5.0)
parser.add_argument("--fail-max", type=float, default=15.0)
parser.add_argument("--permanent-fail-prob", type=float, default=0.02)
parser.add_argument("--retry", type=float, default=1.0, help="seconds between retries for missing grants")
parser.add_argument("--no-client", action="store_true", help="run only as coordinator (no CS requests)")
args = parser.parse_args()

MY_ID = args.id
RESOURCE = args.resource
P_SUCCESS = args.p_success

PEERS = json.loads(args.peers)
ALL_IDS = [p["id"] for p in PEERS]
N = len(PEERS)
MAJ = (N // 2) + 1

############################################################
# Globala tillstånd
############################################################

am_down: bool = False
permanently_down: bool = False

locks: Dict[str, Dict] = {
    RESOURCE: {
        "holder": None,
        "holder_req": None,
        "queue": [],
        "granted_to": set(),
        "seen_requests": set(),
        "seen_releases": set(),
    }
}

current_req_id: Optional[str] = None
current_ts: Optional[float] = None
grants_from: set[int] = set()
awaiting_from: set[int] = set()

stats = {
    "messages_sent": 0,
    "messages_attempted": 0,
    "cs_entries": 0,
    "cs_wait_times": [],
}

############################################################
# Hjälpfunktioner
############################################################

def now_ts() -> float:
    return time.time()

def new_req_id() -> str:
    return f"{MY_ID}-{int(time.time()*1000)}-{random.randint(0,9999)}"

def should_send() -> bool:
    return random.random() < P_SUCCESS

async def maybe_post(session: aiohttp.ClientSession, url: str, json_payload: dict, timeout=2.0):
    stats["messages_attempted"] += 1
    if not should_send():
        return None
    try:
        async with session.post(url, json=json_payload, timeout=timeout) as resp:
            stats["messages_sent"] += 1
            if resp.status == 200:
                return await resp.json()
            return None
    except Exception:
        return None

def majority_reached() -> bool:
    return len(grants_from) >= MAJ

def peer_url(peer_id: int, path: str) -> str:
    base = next(p["url"] for p in PEERS if p["id"] == peer_id)
    return f"{base}{path}"

############################################################
# HTTP-handlers (koordinator)
############################################################

async def require_up(request):
    if am_down or permanently_down:
        await asyncio.sleep(99999)
    return None

async def handle_request(req: web.Request):
    await require_up(req)
    data = await req.json()
    res = data["resource"]; requester = data["requester_id"]; ts = data["ts"]; req_id = data["req_id"]
    st = locks[res]

    if req_id in st["seen_requests"]:
        return web.json_response({"ok": True, "granted": req_id in st["granted_to"]})
    st["seen_requests"].add(req_id)

    if st["holder"] is None:
        st["holder"] = req_id
        st["holder_req"] = (requester, ts)
        st["granted_to"].add(req_id)
        return web.json_response({"ok": True, "granted": True})

    st["queue"].append((ts, requester, req_id))
    st["queue"].sort(key=lambda x: (x[0], x[1]))
    return web.json_response({"ok": True, "granted": False})

async def handle_release(req: web.Request):
    await require_up(req)
    data = await req.json()
    res = data["resource"]; req_id = data["req_id"]
    st = locks[res]

    if req_id in st["seen_releases"]:
        return web.json_response({"ok": True})
    st["seen_releases"].add(req_id)

    if st["holder"] == req_id:
        st["holder"] = None
        st["holder_req"] = None
        if st["queue"]:
            nxt_ts, nxt_req, nxt_reqid = st["queue"].pop(0)
            st["holder"] = nxt_reqid
            st["holder_req"] = (nxt_req, nxt_ts)
            st["granted_to"].add(nxt_reqid)
    return web.json_response({"ok": True})

async def handle_health(req: web.Request):
    return web.json_response({"id": MY_ID, "down": am_down or permanently_down})

app = web.Application()
app.router.add_post("/request", handle_request)
app.router.add_post("/release", handle_release)
app.router.add_get("/health", handle_health)

############################################################
# Failure simulation
############################################################

async def failure_daemon():
    global am_down, permanently_down
    while True:
        await asyncio.sleep(1.0)
        if permanently_down or am_down:
            continue
        if random.random() < args.fail_prob:
            am_down = True
            if random.random() < args.permanent_fail_prob:
                permanently_down = True
                print(f"[{MY_ID}] PERMANENT FAIL")
                continue
            tdown = random.uniform(args.fail_min, args.fail_max)
            print(f"[{MY_ID}] FAIL-STOP for {tdown:.1f}s")
            await asyncio.sleep(tdown)
            am_down = False
            print(f"[{MY_ID}] RECOVER")

############################################################
# Klientlogik
############################################################

async def client_loop():
    global current_req_id, current_ts, grants_from, awaiting_from
    if args.no_client:
        return
    async with aiohttp.ClientSession() as session:
        while True:
            wait = random.expovariate(1.0 / max(0.1, args.req_every))
            await asyncio.sleep(wait)
            if am_down or permanently_down:
                continue

            current_req_id = new_req_id()
            current_ts = now_ts()
            grants_from = set()
            awaiting_from = set(ALL_IDS)

            t0 = now_ts()
            print(f"[{MY_ID}] REQUEST CS req_id={current_req_id}")

            async def send_round():
                payload = {"resource": RESOURCE, "requester_id": MY_ID, "ts": current_ts, "req_id": current_req_id}
                targets = list(awaiting_from)
                tasks = [maybe_post(session, peer_url(pid, "/request"), payload, timeout=2.0)
                         for pid in targets]
                results = await asyncio.gather(*tasks, return_exceptions=True)
                for pid, resp in zip(targets, results):
                    if resp and resp.get("ok") and resp.get("granted"):
                        grants_from.add(pid)
                        awaiting_from.discard(pid)
                    # om granted=False eller inget svar → vi försöker igen senare

            while not majority_reached():
                await send_round()
                if majority_reached():
                    break
                await asyncio.sleep(args.retry)

            t_wait = now_ts() - t0
            stats["cs_wait_times"].append(t_wait)
            print(f"[{MY_ID}] ENTER CS (waited {t_wait:.2f}s) with {len(grants_from)} grants")
            stats["cs_entries"] += 1

            t_hold = random.uniform(0.5 * args.cs_time, 1.5 * args.cs_time)
            await asyncio.sleep(t_hold)

            payload = {"resource": RESOURCE, "req_id": current_req_id}
            t_release_end = now_ts() + max(2*args.retry, 2.0)
            while now_ts() < t_release_end:
                tasks = [maybe_post(session, p["url"] + "/release", payload, timeout=2.0) for p in PEERS]
                await asyncio.gather(*tasks, return_exceptions=True)
                await asyncio.sleep(args.retry/2)

            print(f"[{MY_ID}] LEAVE CS")
            current_req_id = None
            grants_from.clear()
            awaiting_from.clear()

############################################################
# Statistikutskrift
############################################################

async def stats_printer():
    while True:
        await asyncio.sleep(10)
        if permanently_down:
            continue
        avg_wait = (sum(stats["cs_wait_times"]) / len(stats["cs_wait_times"])
                    if stats["cs_wait_times"] else 0.0)
        print(f"[{MY_ID}] msgs: sent={stats['messages_sent']}/{stats['messages_attempted']}, "
              f"cs={stats['cs_entries']}, avg_wait={avg_wait:.2f}s")

############################################################
# Main
############################################################

async def main():
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, host="0.0.0.0", port=args.port)
    await site.start()
    print(f"[{MY_ID}] listening on 0.0.0.0:{args.port}, peers={[(p['id'], p['url']) for p in PEERS]}")
    await asyncio.gather(failure_daemon(), stats_printer(), client_loop())

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
