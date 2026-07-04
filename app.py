import os
import asyncio
import json
import requests
import websockets
import time
import bech32
from aiohttp import web
from collections import deque

# --- GLOBAL CONFIG ---
DATA_DIR = "/data"
CONFIG_FILE = os.path.join(DATA_DIR, "config.json")
BOOTSTRAP_RELAYS = ["wss://purplepag.es", "wss://relay.damus.io", "wss://nos.lol"]
MAX_RELAYS = 25
processed_events = deque(maxlen=10000)

# Bridge lifecycle: hex_pubkey -> asyncio.Task (the gather for that account's listeners)
running_accounts: dict[str, asyncio.Task] = {}
# Protect against concurrent restarts
_restart_lock = asyncio.Lock()


# --- UTILITIES ---
def load_config():
    """Load config, migrating old format if needed."""
    if not os.path.exists(CONFIG_FILE):
        return {"accounts": []}

    with open(CONFIG_FILE, "r") as f:
        config = json.load(f)

    # Migrate old single-pubkey format to new multi-account format
    if "pubkey" in config and "accounts" not in config:
        old_pubkey = config.get("pubkey", "")
        old_ntfy_url = config.get("ntfy_url", "")
        old_raw = config.get("raw_input", old_pubkey)

        if old_pubkey:
            if "/" in old_ntfy_url:
                parts = old_ntfy_url.rsplit("/", 1)
                base_url = parts[0]
                topic = parts[1] if len(parts) > 1 else "nostr-events"
            else:
                base_url = old_ntfy_url
                topic = "nostr-events"

            config = {
                "ntfy_base_url": base_url,
                "accounts": [
                    {
                        "npub": old_raw if old_raw.startswith("npub1") else "",
                        "hex_pubkey": old_pubkey,
                        "ntfy_topic": topic,
                    }
                ],
            }
            save_config(config)
            print("Migrated old config to multi-account format.")
        else:
            config = {"accounts": []}

    if "ntfy_base_url" not in config:
        config["ntfy_base_url"] = "http://ntfy_app_1:80"

    return config


def save_config(config):
    os.makedirs(DATA_DIR, exist_ok=True)
    with open(CONFIG_FILE, "w") as f:
        json.dump(config, f, indent=2)


def convert_npub_to_hex(npub):
    try:
        hrp, data = bech32.bech32_decode(npub)
        if data is None:
            return ""
        decoded = bech32.convertbits(data, 5, 8, False)
        if decoded is None:
            return ""
        return bytes(decoded).hex()
    except Exception as e:
        print(f"Error decoding npub: {e}")
        return ""


def hex_to_npub(hex_pubkey):
    try:
        data = bytes.fromhex(hex_pubkey)
        converted = bech32.convertbits(data, 8, 5, True)
        if converted is None:
            return ""
        return bech32.bech32_encode("npub", converted)
    except Exception:
        return ""


def validate_pubkey(hex_pubkey):
    return len(hex_pubkey) == 64 and all(
        c in "0123456789abcdef" for c in hex_pubkey.lower()
    )


# --- WEB UI ---
STYLE = """
<style>
    * { box-sizing: border-box; margin: 0; padding: 0; }
    body {
        font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif;
        background-color: #111827; color: #e5e7eb; padding: 40px 20px;
        display: flex; justify-content: center; min-height: 100vh;
    }
    .container { width: 100%; max-width: 600px; }
    h1 { font-size: 24px; font-weight: 700; margin-bottom: 8px; color: #f9fafb; }
    h2 { font-size: 16px; font-weight: 600; margin-bottom: 16px; color: #d1d5db; }
    .subtitle { font-size: 14px; color: #9ca3af; margin-bottom: 24px; }
    .card {
        background: #1f2937; padding: 24px; border-radius: 12px;
        border: 1px solid #374151; margin-bottom: 20px;
    }
    label { display: block; font-size: 13px; font-weight: 500; color: #9ca3af; margin-bottom: 6px; margin-top: 14px; }
    label:first-child { margin-top: 0; }
    input[type="text"] {
        width: 100%; padding: 10px 12px; border: 1px solid #374151; border-radius: 8px;
        background: #111827; color: #f9fafb; font-size: 14px; outline: none; transition: border-color 0.2s;
    }
    input[type="text"]:focus { border-color: #3b82f6; }
    input[type="text"]::placeholder { color: #6b7280; }
    .btn {
        display: inline-block; padding: 10px 20px; border: none; border-radius: 8px;
        font-size: 14px; font-weight: 600; cursor: pointer; transition: all 0.2s;
        text-decoration: none; text-align: center;
    }
    .btn-primary { background-color: #3b82f6; color: white; width: 100%; margin-top: 20px; }
    .btn-primary:hover { background-color: #2563eb; }
    .btn-danger { background-color: #dc2626; color: white; padding: 6px 12px; font-size: 12px; }
    .btn-danger:hover { background-color: #b91c1c; }
    .account-list { list-style: none; }
    .account-item {
        display: flex; align-items: center; justify-content: space-between;
        padding: 14px; margin-bottom: 10px; background: #111827; border-radius: 8px;
        border: 1px solid #374151;
    }
    .account-info { flex: 1; min-width: 0; }
    .account-npub { font-size: 14px; font-weight: 500; color: #f9fafb; word-break: break-all; }
    .account-topic { font-size: 12px; color: #6b7280; margin-top: 4px; }
    .account-relays { font-size: 11px; color: #4b5563; margin-top: 2px; }
    .account-status { font-size: 11px; margin-top: 4px; }
    .status-active { color: #34d399; }
    .status-inactive { color: #f87171; }
    .account-actions { margin-left: 12px; flex-shrink: 0; }
    .empty-state { text-align: center; padding: 40px 20px; color: #6b7280; font-size: 14px; }
    .toast {
        position: fixed; top: 20px; right: 20px; padding: 12px 20px; border-radius: 8px;
        font-size: 14px; font-weight: 500; z-index: 1000; animation: slideIn 0.3s ease;
        max-width: 400px;
    }
    .toast-success { background: #065f46; color: #a7f3d0; border: 1px solid #059669; }
    .toast-error { background: #7f1d1d; color: #fca5a5; border: 1px solid #dc2626; }
    @keyframes slideIn { from { transform: translateX(100%); opacity: 0; } to { transform: translateX(0); opacity: 1; } }
    .help { font-size: 12px; color: #6b7280; margin-top: 4px; }
</style>
"""


def render_index_page(config):
    accounts = config.get("accounts", [])
    ntfy_base = config.get("ntfy_base_url", "http://ntfy_app_1:80")

    if not accounts:
        account_html = '<div class="empty-state">No accounts configured yet. Add your first npub below.</div>'
    else:
        account_html = ""
        for i, acc in enumerate(accounts):
            hex_pk = acc.get("hex_pubkey", "")
            npub_display = acc.get("npub") or hex_pk[:16] + "..."
            topic = acc.get("ntfy_topic", "nostr-events")
            relays = acc.get("relay_count", 0)
            groups = acc.get("group_count", 0)
            relay_info = f"{relays} relays | {groups} groups" if relays else "Pending..."

            is_running = hex_pk in running_accounts
            if is_running:
                task = running_accounts[hex_pk]
                is_active = not task.done()
            else:
                is_active = False

            status_html = (
                '<div class="account-status status-active">● Listening</div>'
                if is_active
                else '<div class="account-status status-inactive">○ Stopped</div>'
            )

            account_html += f"""
            <li class="account-item">
                <div class="account-info">
                    <div class="account-npub">{npub_display}</div>
                    <div class="account-topic">ntfy topic: {ntfy_base}/{topic}</div>
                    <div class="account-relays">{relay_info}</div>
                    {status_html}
                </div>
                <div class="account-actions">
                    <form method="post" action="/delete" style="display:inline;">
                        <input type="hidden" name="index" value="{i}">
                        <button type="submit" class="btn btn-danger">Remove</button>
                    </form>
                </div>
            </li>"""

    return f"""<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>Nostr-to-ntfy Bridge</title>
    {STYLE}
</head>
<body>
    <div class="container">
        <h1>Nostr-to-ntfy Bridge</h1>
        <p class="subtitle">Route Nostr mentions and NIP-29 group messages to ntfy push notifications.</p>

        <div class="card">
            <h2>Configured Accounts</h2>
            <ul class="account-list">
                {account_html}
            </ul>
        </div>

        <div class="card">
            <h2>Add Account</h2>
            <form method="post" action="/add">
                <label for="npub">Nostr Public Key (npub or hex)</label>
                <input type="text" id="npub" name="npub" placeholder="npub1..." required>
                <label for="ntfy_topic">ntfy Topic</label>
                <input type="text" id="ntfy_topic" name="ntfy_topic" value="nostr-events-{len(accounts)}" required>
                <div class="help">Notifications for this key will be sent to: {ntfy_base}/&lt;topic&gt;</div>
                <button type="submit" class="btn btn-primary">Add Account</button>
            </form>
        </div>

        <div class="card">
            <h2>Settings</h2>
            <form method="post" action="/settings">
                <label for="ntfy_base_url">ntfy Base URL</label>
                <input type="text" id="ntfy_base_url" name="ntfy_base_url" value="{ntfy_base}" required>
                <div class="help">Base URL of your ntfy server. Topics are appended to this.</div>
                <button type="submit" class="btn btn-primary">Save Settings</button>
            </form>
        </div>
    </div>
</body>
</html>"""


def render_toast_redirect(message, is_error=False):
    css_class = "toast-error" if is_error else "toast-success"
    return web.Response(
        text=f"""<div id="toast" class="toast {css_class}">{message}</div>
<script>setTimeout(function() {{ window.location = '/'; }}, 1500);</script>""",
        content_type="text/html",
    )


# --- REQUEST HANDLERS ---
async def handle_index(request):
    config = load_config()
    return web.Response(text=render_index_page(config), content_type="text/html")


async def handle_add(request):
    data = await request.post()
    raw_pubkey = data.get("npub", "").strip()
    ntfy_topic = data.get("ntfy_topic", "nostr-events").strip()

    if not raw_pubkey:
        return render_toast_redirect("Error: Public key is required.", is_error=True)
    if not ntfy_topic:
        return render_toast_redirect("Error: ntfy topic is required.", is_error=True)

    if raw_pubkey.startswith("npub1"):
        hex_pubkey = convert_npub_to_hex(raw_pubkey)
        npub = raw_pubkey
    else:
        hex_pubkey = raw_pubkey.lower()
        npub = ""

    if not validate_pubkey(hex_pubkey):
        return render_toast_redirect(
            "Error: Invalid public key. Must be a valid npub or 64-char hex string.",
            is_error=True,
        )

    config = load_config()
    for acc in config.get("accounts", []):
        if acc.get("hex_pubkey") == hex_pubkey:
            return render_toast_redirect(
                "Error: This public key is already configured.", is_error=True
            )

    new_account = {
        "npub": npub,
        "hex_pubkey": hex_pubkey,
        "ntfy_topic": ntfy_topic.strip("/"),
        "relay_count": 0,
        "group_count": 0,
    }
    config.setdefault("accounts", []).append(new_account)
    save_config(config)

    # Hot-reload: start just this new account (no restart needed)
    asyncio.create_task(reload_bridge(request.app))
    return render_toast_redirect(f"Added account. Listener starting...")


async def handle_delete(request):
    data = await request.post()
    try:
        index = int(data.get("index", -1))
    except (ValueError, TypeError):
        return render_toast_redirect("Error: Invalid index.", is_error=True)

    config = load_config()
    accounts = config.get("accounts", [])
    if index < 0 or index >= len(accounts):
        return render_toast_redirect("Error: Account not found.", is_error=True)

    removed = accounts.pop(index)
    removed_name = removed.get("npub") or removed.get("hex_pubkey", "")[:16]
    save_config(config)

    # Hot-reload: stop just this account's listeners
    asyncio.create_task(reload_bridge(request.app))
    return render_toast_redirect(f"Removed {removed_name}. Listener stopped.")


async def handle_settings(request):
    data = await request.post()
    ntfy_base = data.get("ntfy_base_url", "").strip().rstrip("/")

    if not ntfy_base:
        return render_toast_redirect("Error: ntfy base URL is required.", is_error=True)

    config = load_config()
    old_base = config.get("ntfy_base_url", "")
    config["ntfy_base_url"] = ntfy_base
    save_config(config)

    if old_base != ntfy_base:
        asyncio.create_task(reload_bridge(request.app))
        return render_toast_redirect("Settings saved. Reloading all listeners...")
    return render_toast_redirect("Settings saved.")


# --- NOSTR BRIDGE LOGIC ---
async def fetch_metadata(pubkey_hex):
    """Discover relays and NIP-29 group IDs for a pubkey."""
    follows = set()
    inbox_relays = set()
    group_ids = set()

    print(f"  Bootstrapping network topology for {pubkey_hex[:8]}...")

    for relay in BOOTSTRAP_RELAYS:
        try:
            async with websockets.connect(relay, open_timeout=5, close_timeout=5) as ws:
                req = [
                    "REQ",
                    f"boot-{pubkey_hex[:6]}-{int(time.time())}",
                    {"authors": [pubkey_hex], "kinds": [3, 10002, 10009, 9021]},
                ]
                await ws.send(json.dumps(req))
                while True:
                    try:
                        msg = await asyncio.wait_for(ws.recv(), timeout=5)
                        data = json.loads(msg)
                        if data[0] == "EOSE":
                            break
                        if data[0] == "EVENT":
                            kind = data[2]["kind"]
                            tags = data[2].get("tags", [])
                            if kind == 3:
                                for t in tags:
                                    if t[0] == "p" and len(t) > 1:
                                        follows.add(t[1])
                            elif kind == 10002:
                                for t in tags:
                                    if t[0] == "r" and len(t) > 1:
                                        if len(t) == 2 or (len(t) > 2 and t[2] == "read"):
                                            inbox_relays.add(t[1])
                            elif kind in [10009, 9021]:
                                for t in tags:
                                    if t[0] == "h" and len(t) > 1:
                                        group_ids.add(t[1])
                    except asyncio.TimeoutError:
                        break
        except Exception as e:
            print(f"  Bootstrap relay {relay} failed: {e}")
            continue

    relays = list(inbox_relays.union(set(BOOTSTRAP_RELAYS)))[:MAX_RELAYS]
    print(f"  Found {len(relays)} relays, {len(group_ids)} groups, {len(follows)} follows.")
    return relays, list(group_ids)


async def listen_to_relay(relay_url, group_ids, pubkey_hex, ntfy_url, account_label):
    """Listen on a single relay. Exits cleanly on CancelledError."""
    try:
        while True:
            try:
                async with websockets.connect(
                    relay_url, open_timeout=10, close_timeout=10
                ) as ws:
                    print(f"  [{account_label}] Connected to {relay_url}")

                    current_time = int(time.time())
                    filters = {"#p": [pubkey_hex], "since": current_time, "limit": 0}
                    sub_id = f"listen-{pubkey_hex[:6]}-{relay_url[-8:]}"
                    await ws.send(json.dumps(["REQ", sub_id, filters]))

                    if group_ids:
                        group_sub_id = f"groups-{pubkey_hex[:6]}-{relay_url[-8:]}"
                        await ws.send(json.dumps([
                            "REQ", group_sub_id,
                            {"#h": group_ids, "since": current_time, "limit": 0},
                        ]))

                    while True:
                        msg = await ws.recv()
                        data = json.loads(msg)

                        if data[0] == "EOSE":
                            continue
                        if data[0] == "NOTICE":
                            print(f"  [{account_label}] NOTICE from {relay_url}: {data[1]}")
                            continue
                        if data[0] == "EVENT":
                            evt = data[2]
                            evt_id = evt.get("id", "")
                            if evt_id in processed_events:
                                continue
                            processed_events.append(evt_id)

                            is_group = any(t[0] == "h" for t in evt.get("tags", []))
                            title = "NIP-29 Group Alert" if is_group else "Nostr Mention"
                            author = evt.get("pubkey", "")[:12]
                            content = evt.get("content", "New notification")
                            if len(content) > 280:
                                content = content[:277] + "..."

                            try:
                                resp = requests.post(
                                    ntfy_url,
                                    data=content.encode("utf-8"),
                                    headers={
                                        "Title": title,
                                        "Tags": f"nostr,{'group' if is_group else 'mention'}",
                                        "Author": author,
                                    },
                                    timeout=10,
                                )
                                if resp.status_code == 200:
                                    print(f"  [{account_label}] Notification sent -> {ntfy_url}")
                                else:
                                    print(f"  [{account_label}] ntfy returned {resp.status_code}")
                            except Exception as e:
                                print(f"  [{account_label}] ntfy error: {e}")

            except asyncio.CancelledError:
                raise  # let the outer handler deal with it
            except (websockets.ConnectionClosed, ConnectionError, OSError) as e:
                print(f"  [{account_label}] {relay_url} disconnected: {e}")
            except Exception as e:
                print(f"  [{account_label}] Error on {relay_url}: {e}")

            print(f"  [{account_label}] Reconnecting to {relay_url} in 30s...")
            await asyncio.sleep(30)
    except asyncio.CancelledError:
        print(f"  [{account_label}] Listener for {relay_url} stopped.")
        return


async def start_account_bridge(account, ntfy_base_url):
    """Run all relay listeners for one account as a gather."""
    hex_pubkey = account["hex_pubkey"]
    ntfy_topic = account.get("ntfy_topic", "nostr-events")
    ntfy_url = f"{ntfy_base_url.rstrip('/')}/{ntfy_topic}"
    label = account.get("npub") or hex_pubkey[:12]

    print(f"[{label}] Starting bridge -> {ntfy_url}")

    relays, group_ids = await fetch_metadata(hex_pubkey)

    # Persist discovered relay/group counts back to config
    config = load_config()
    for acc in config.get("accounts", []):
        if acc["hex_pubkey"] == hex_pubkey:
            acc["relay_count"] = len(relays)
            acc["group_count"] = len(group_ids)
            break
    save_config(config)

    print(f"[{label}] Listening on {len(relays)} relays, {len(group_ids)} groups")

    tasks = [
        asyncio.create_task(listen_to_relay(r, group_ids, hex_pubkey, ntfy_url, label))
        for r in relays
    ]
    # gather returns when all tasks finish (i.e. all get cancelled)
    await asyncio.gather(*tasks, return_exceptions=True)


async def stop_account(hex_pubkey, label=""):
    """Cancel and wait for an account's bridge task to fully stop."""
    task = running_accounts.pop(hex_pubkey, None)
    if task is None:
        return
    display = label or hex_pubkey[:12]
    print(f"[{display}] Stopping listeners...")
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass
    print(f"[{display}] Stopped.")


async def reload_bridge(app):
    """Hot-reload: diff config against running accounts, stop removed, start new."""
    async with _restart_lock:
        config = load_config()
        accounts = config.get("accounts", [])
        ntfy_base = config.get("ntfy_base_url", "http://ntfy_app_1:80")

        wanted = {acc["hex_pubkey"]: acc for acc in accounts}
        current = set(running_accounts.keys())

        to_stop = current - set(wanted.keys())
        to_start = set(wanted.keys()) - current
        # We also restart changed accounts (ntfy_topic may have changed via settings)
        to_restart = current & set(wanted.keys()) if ntfy_base != app.get("_last_ntfy_base") else set()

        for hex_pk in to_stop | to_restart:
            label = wanted.get(hex_pk, {}).get("npub", hex_pk[:12])
            await stop_account(hex_pk, label)

        for hex_pk in (to_start | to_restart):
            acc = wanted[hex_pk]
            label = acc.get("npub") or hex_pk[:12]
            task = asyncio.create_task(
                _run_account_safely(acc, ntfy_base, label)
            )
            running_accounts[hex_pk] = task

        app["_last_ntfy_base"] = ntfy_base

        if not running_accounts:
            print("No accounts active. Waiting for user input via Web UI.")
        else:
            print(f"Bridge active: {len(running_accounts)} account(s).")


async def _run_account_safely(account, ntfy_base_url, label):
    """Wrapper that logs if an account bridge ever exits unexpectedly."""
    try:
        await start_account_bridge(account, ntfy_base_url)
    except asyncio.CancelledError:
        return
    except Exception as e:
        print(f"[{label}] Account bridge crashed: {e}")


async def start_bridge(app):
    """Called once on app startup."""
    await reload_bridge(app)


# --- MAIN APP ENTRY ---
if __name__ == "__main__":
    app = web.Application()
    app["_last_ntfy_base"] = ""
    app.add_routes([
        web.get("/", handle_index),
        web.post("/add", handle_add),
        web.post("/delete", handle_delete),
        web.post("/settings", handle_settings),
    ])
    app.on_startup.append(start_bridge)

    print("Starting Web UI on port 8181...")
    web.run_app(app, host="0.0.0.0", port=8181, print=None)
