import os
import asyncio
import json
import requests
import websockets
import time
from collections import deque

# --- CONFIGURATION FROM DOCKER ---
MY_PUBKEY = os.environ.get("NOSTR_PUBKEY", "")
NTFY_URL = os.environ.get("NTFY_URL", "http://umbrel.nvpn:13199/nostr-events") 
BOOTSTRAP_RELAYS = ["wss://purplepag.es", "wss://relay.damus.io", "wss://nos.lol"]
MAX_RELAYS = 25
# --------------------------------

processed_events = deque(maxlen=500)

async def fetch_metadata():
    follows = set()
    inbox_relays = set()
    outbox_relays = set()
    group_ids = set() 
    
    print(f"Bootstrapping network topology for {MY_PUBKEY[:8]}...")
    
    for relay in BOOTSTRAP_RELAYS:
        try:
            async with websockets.connect(relay) as ws:
                req = ["REQ", f"boot-{int(time.time())}", 
                      {"authors": [MY_PUBKEY], "kinds": [3, 10002, 10009, 9021]}]
                await ws.send(json.dumps(req))
                
                while True:
                    try:
                        msg = await asyncio.wait_for(ws.recv(), timeout=3)
                        data = json.loads(msg)
                        if data[0] == "EOSE": break
                        if data[0] == "EVENT":
                            event = data[2]
                            kind = event["kind"]
                            
                            if kind == 3:
                                for tag in event.get("tags", []):
                                    if tag[0] == "p": follows.add(tag[1])
                            elif kind == 10002:
                                for tag in event.get("tags", []):
                                    if tag[0] == "r":
                                        if len(tag) == 2 or (len(tag) > 2 and tag[2] == "read"):
                                            inbox_relays.add(tag[1])
                            elif kind in [10009, 9021]:
                                for tag in event.get("tags", []):
                                    if tag[0] == "h":
                                        group_ids.add(tag[1])
                    except asyncio.TimeoutError:
                        break
        except Exception:
            continue

    final_relays = inbox_relays.union(set(BOOTSTRAP_RELAYS))
    return list(final_relays)[:MAX_RELAYS], list(group_ids)

async def listen_to_relay(relay_url, group_ids):
    while True:
        try:
            async with websockets.connect(relay_url) as ws:
                print(f"Listening on {relay_url} (Groups: {len(group_ids)})")
                current_time = int(time.time())
                
                filter_mentions = {"#p": [MY_PUBKEY], "since": current_time}
                sub_req = ["REQ", f"listen-{MY_PUBKEY[:6]}", filter_mentions]
                
                if group_ids:
                    filter_groups = {"#h": group_ids, "since": current_time}
                    sub_req.append(filter_groups)
                
                await ws.send(json.dumps(sub_req))
                
                while True:
                    msg = await ws.recv()
                    data = json.loads(msg)
                    if data[0] == "EVENT":
                        event = data[2]
                        evt_id = event["id"]
                        
                        if evt_id in processed_events: continue
                        processed_events.append(evt_id)
                        
                        is_group = any(tag[0] == "h" for tag in event.get("tags", []))
                        title = "NIP-29 Group Alert" if is_group else "Nostr Mention"
                        
                        content = event.get("content", "New notification!")
                        try:
                            requests.post(NTFY_URL, data=content.encode('utf-8'), headers={"Title": title})
                        except Exception as e:
                            print(f"ntfy error: {e}")
                            
        except Exception:
            await asyncio.sleep(30)

async def main():
    if not MY_PUBKEY:
        print("CRITICAL: NOSTR_PUBKEY environment variable not set. Exiting.")
        return
        
    relays, group_ids = await fetch_metadata()
    print(f"Engine starting: {len(relays)} relays | {len(group_ids)} groups")
    
    tasks = [listen_to_relay(r, group_ids) for r in relays]
    await asyncio.gather(*tasks)

if __name__ == "__main__":
    asyncio.run(main())