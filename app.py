import os
import sys
import asyncio
import json
import requests
import websockets
import time
import bech32
from aiohttp import web
from collections import deque

# --- GLOBAL CONFIG ---
DATA_DIR = data
CONFIG_FILE = os.path.join(DATA_DIR, config.json)
BOOTSTRAP_RELAYS = [wsspurplepag.es, wssrelay.damus.io, wssnos.lol]
MAX_RELAYS = 25
processed_events = deque(maxlen=500)

# --- UTILITIES ---
def load_config()
    if os.path.exists(CONFIG_FILE)
        with open(CONFIG_FILE, r) as f
            return json.load(f)
    return {pubkey , ntfy_url httpumbrel.nvpn13199nostr-events}

def save_config(config)
    os.makedirs(DATA_DIR, exist_ok=True)
    with open(CONFIG_FILE, w) as f
        json.dump(config, f)

def convert_npub_to_hex(npub)
    try
        hrp, data = bech32.bech32_decode(npub)
        decoded = bech32.convertbits(data, 5, 8, False)
        return bytes(decoded).hex()
    except Exception
        return 

# --- WEB UI (Settings Page) ---
async def handle_index(request)
    config = load_config()
    html = f
    !DOCTYPE html
    html
    head
        titleNostr-ntfy Settingstitle
        style
            body {{ font-family -apple-system, BlinkMacSystemFont, Segoe UI, Roboto, Helvetica, Arial, sans-serif; background-color #f3f4f6; color #1f2937; padding 40px; display flex; justify-content center; }}
            .card {{ background white; padding 30px; border-radius 12px; box-shadow 0 4px 6px rgba(0,0,0,0.1); width 100%; max-width 500px; }}
            input[type=text] {{ width 100%; padding 10px; margin 8px 0 20px 0; border 1px solid #d1d5db; border-radius 6px; box-sizing border-box; }}
            button {{ background-color #3b82f6; color white; border none; padding 12px 20px; border-radius 6px; cursor pointer; width 100%; font-size 16px; font-weight bold; }}
            buttonhover {{ background-color #2563eb; }}
        style
    head
    body
        div class=card
            h2Nostr Bridge Settingsh2
            form action=save method=post
                labelNostr Public Key (npub or hex)label
                input type=text name=pubkey value={config.get('pubkey', '')} placeholder=npub1... required
                
                labelLocal ntfy URLlabel
                input type=text name=ntfy_url value={config.get('ntfy_url', 'httpumbrel.nvpn13199nostr-events')} required
                
                button type=submitSave & Restart Listenerbutton
            form
        div
    body
    html
    
    return web.Response(text=html, content_type='texthtml')

async def handle_save(request)
    data = await request.post()
    raw_pubkey = data.get('pubkey', '').strip()
    ntfy_url = data.get('ntfy_url', '').strip()

    # Auto-convert npub if the user pastes one
    if raw_pubkey.startswith(npub1)
        hex_pubkey = convert_npub_to_hex(raw_pubkey)
    else
        hex_pubkey = raw_pubkey

    save_config({pubkey hex_pubkey, ntfy_url ntfy_url, raw_input raw_pubkey})
    
    # Trigger a Docker container restart to apply the new config cleanly
    asyncio.create_task(delayed_restart())
    
    return web.Response(text=scriptalert('Settings Saved! Restarting bridge...'); window.location='';script, content_type='texthtml')

async def delayed_restart()
    await asyncio.sleep(1)
    os._exit(0) # Exits Python. Docker's 'restart on-failure' will immediately bring it back up.

# --- NOSTR BRIDGE LOGIC ---
async def fetch_metadata(pubkey)
    follows, inbox_relays, outbox_relays, group_ids = set(), set(), set(), set()
    print(fBootstrapping network topology for {pubkey[8]}...)
    
    for relay in BOOTSTRAP_RELAYS
        try
            async with websockets.connect(relay) as ws
                req = [REQ, fboot-{int(time.time())}, {authors [pubkey], kinds [3, 10002, 10009, 9021]}]
                await ws.send(json.dumps(req))
                while True
                    try
                        msg = await asyncio.wait_for(ws.recv(), timeout=3)
                        data = json.loads(msg)
                        if data[0] == EOSE break
                        if data[0] == EVENT
                            kind = data[2][kind]
                            tags = data[2].get(tags, [])
                            if kind == 3
                                [follows.add(t[1]) for t in tags if t[0] == p]
                            elif kind == 10002
                                [inbox_relays.add(t[1]) for t in tags if t[0] == r and (len(t)==2 or t[2]==read)]
                            elif kind in [10009, 9021]
                                [group_ids.add(t[1]) for t in tags if t[0] == h]
                    except asyncio.TimeoutError
                        break
        except Exception
            continue
    return list(inbox_relays.union(set(BOOTSTRAP_RELAYS)))[MAX_RELAYS], list(group_ids)

async def listen_to_relay(relay_url, group_ids, pubkey, ntfy_url)
    while True
        try
            async with websockets.connect(relay_url) as ws
                print(fListening on {relay_url})
                current_time = int(time.time())
                sub_req = [REQ, flisten-{pubkey[6]}, {#p [pubkey], since current_time}]
                if group_ids sub_req.append({#h group_ids, since current_time})
                await ws.send(json.dumps(sub_req))
                
                while True
                    msg = await ws.recv()
                    data = json.loads(msg)
                    if data[0] == EVENT
                        evt = data[2]
                        if evt[id] in processed_events continue
                        processed_events.append(evt[id])
                        
                        is_group = any(t[0] == h for t in evt.get(tags, []))
                        title = NIP-29 Group Alert if is_group else Nostr Mention
                        
                        try
                            requests.post(ntfy_url, data=evt.get(content, Alert).encode('utf-8'), headers={Title title})
                        except Exception
                            pass
        except Exception
            await asyncio.sleep(30)

async def start_bridge(app)
    config = load_config()
    pubkey = config.get(pubkey)
    ntfy_url = config.get(ntfy_url)
    
    if pubkey
        relays, group_ids = await fetch_metadata(pubkey)
        print(fEngine starting {len(relays)} relays  {len(group_ids)} groups)
        tasks = [listen_to_relay(r, group_ids, pubkey, ntfy_url) for r in relays]
        app['bridge_tasks'] = asyncio.gather(tasks)
    else
        print(No pubkey configured yet. Waiting for user input via Web UI.)

# --- MAIN APP ENTRY ---
if __name__ == __main__
    app = web.Application()
    app.add_routes([web.get('', handle_index), web.post('save', handle_save)])
    app.on_startup.append(start_bridge)
    
    print(Starting Web UI on port 8080...)
    web.run_app(app, host='0.0.0.0', port=8080)