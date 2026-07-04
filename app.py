import os
import sys
import asyncio
import json
import logging
import traceback
import requests
import websockets
import time
import bech32
from aiohttp import web
from collections import deque

# --- LOGGING SETUP ---
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s [%(levelname)-5s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    stream=sys.stdout,
)
log = logging.getLogger("nostr-ntfy")

# Silence noisy libraries
logging.getLogger("websockets").setLevel(logging.WARNING)
logging.getLogger("aiohttp").setLevel(logging.WARNING)

# --- GLOBAL CONFIG ---
DATA_DIR = "/data"
CONFIG_FILE = os.path.join(DATA_DIR, "config.json")
BOOTSTRAP_RELAYS = ["wss://purplepag.es", "wss://relay.damus.io", "wss://nos.lol"]
MAX_RELAYS = 25
processed_events = deque(maxlen=10000)
event_stats = {"received": 0, "deduped": 0, "sent": 0, "failed": 0}

# Bridge lifecycle: hex_pubkey -> asyncio.Task (the gather for that account's listeners)
running_accounts: dict[str, asyncio.Task] = {}
# Protect against concurrent restarts
_restart_lock = asyncio.Lock()


# --- UTILITIES ---
def load_config():
    """Load config, migrating old format if needed."""
    if not os.path.exists(CONFIG_FILE):
        log.info("No config file found at %s — returning empty config", CONFIG_FILE)
        return {"accounts": []}

    try:
        with open(CONFIG_FILE, "r") as f:
            config = json.load(f)
        log.debug("Loaded config from %s: %d account(s), ntfy_base=%s",
                  CONFIG_FILE, len(config.get("accounts", [])), config.get("ntfy_base_url", "(unset)"))
    except json.JSONDecodeError as e:
        log.error("Failed to parse config file %s: %s", CONFIG_FILE, e)
        return {"accounts": []}
    except Exception as e:
        log.error("Failed to read config file %s: %s", CONFIG_FILE, e)
        return {"accounts": []}

    # Migrate old single-pubkey format to new multi-account format
    if "pubkey" in config and "accounts" not in config:
        old_pubkey = config.get("pubkey", "")
        old_ntfy_url = config.get("ntfy_url", "")
        old_raw = config.get("raw_input", old_pubkey)

        log.info("Detected old single-pubkey config format. Migrating...")
        log.info("  Old pubkey: %s...", old_pubkey[:16] if old_pubkey else "(empty)")
        log.info("  Old ntfy_url: %s", old_ntfy_url)

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
            log.info("Migration complete: 1 account, base_url=%s, topic=%s", base_url, topic)
        else:
            config = {"accounts": []}
            log.info("Old config had empty pubkey. Starting fresh.")

    if "ntfy_base_url" not in config:
        config["ntfy_base_url"] = "http://ntfy_app_1:80"
        log.info("No ntfy_base_url in config, defaulting to %s", config["ntfy_base_url"])

    return config


def save_config(config):
    os.makedirs(DATA_DIR, exist_ok=True)
    try:
        with open(CONFIG_FILE, "w") as f:
            json.dump(config, f, indent=2)
        log.debug("Config saved: %d account(s), ntfy_base=%s",
                  len(config.get("accounts", [])), config.get("ntfy_base_url", "(unset)"))
    except Exception as e:
        log.error("Failed to save config to %s: %s", CONFIG_FILE, e)
        log.error(traceback.format_exc())


def convert_npub_to_hex(npub):
    try:
        hrp, data = bech32.bech32_decode(npub)
        if data is None:
            log.warning("bech32_decode returned None for npub: %s...", npub[:20])
            return ""
        decoded = bech32.convertbits(data, 5, 8, False)
        if decoded is None:
            log.warning("convertbits returned None for npub: %s...", npub[:20])
            return ""
        hex_val = bytes(decoded).hex()
        log.debug("Converted npub %s... -> hex %s...", npub[:16], hex_val[:16])
        return hex_val
    except Exception as e:
        log.error("Error decoding npub '%s...': %s", npub[:20], e)
        return ""


def hex_to_npub(hex_pubkey):
    try:
        data = bytes.fromhex(hex_pubkey)
        converted = bech32.convertbits(data, 8, 5, True)
        if converted is None:
            return ""
        return bech32.bech32_encode("npub", converted)
    except Exception as e:
        log.debug("hex_to_npub conversion failed for %s...: %s", hex_pubkey[:16], e)
        return ""


def validate_pubkey(hex_pubkey):
    if len(hex_pubkey) != 64:
        log.debug("Pubkey validation failed: length is %d, expected 64", len(hex_pubkey))
        return False
    if not all(c in "0123456789abcdef" for c in hex_pubkey.lower()):
        log.debug("Pubkey validation failed: contains non-hex characters")
        return False
    return True


def _log_environment():
    """Log useful environment info for debugging."""
    log.info("=== Nostr-to-ntfy Bridge Starting ===")
    log.info("Python: %s", sys.version.split()[0])
    log.info("Data dir: %s", DATA_DIR)
    log.info("Config file: %s", CONFIG_FILE)
    log.info("Config exists: %s", os.path.exists(CONFIG_FILE))
    log.info("Bootstrap relays: %s", ", ".join(BOOTSTRAP_RELAYS))
    log.info("Max relays per account: %d", MAX_RELAYS)
    log.info("Event dedup buffer size: %d", processed_events.maxlen)
    # Log a few common env vars that might matter in Docker/Umbrel
    for var in ["APP_DATA_DIR", "APP_IP", "DEVICE_DOMAIN_NAME"]:
        val = os.environ.get(var, "(not set)")
        log.info("  env %s=%s", var, val)


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
    log.debug("GET / — rendering index page")
    config = load_config()
    return web.Response(text=render_index_page(config), content_type="text/html")


async def handle_add(request):
    data = await request.post()
    raw_pubkey = data.get("npub", "").strip()
    ntfy_topic = data.get("ntfy_topic", "nostr-events").strip()
    log.info("POST /add — raw_pubkey='%s...', ntfy_topic='%s'", raw_pubkey[:24], ntfy_topic)

    if not raw_pubkey:
        log.warning("POST /add — rejected: empty pubkey")
        return render_toast_redirect("Error: Public key is required.", is_error=True)
    if not ntfy_topic:
        log.warning("POST /add — rejected: empty ntfy topic")
        return render_toast_redirect("Error: ntfy topic is required.", is_error=True)

    if raw_pubkey.startswith("npub1"):
        hex_pubkey = convert_npub_to_hex(raw_pubkey)
        npub = raw_pubkey
    else:
        hex_pubkey = raw_pubkey.lower()
        npub = ""

    if not validate_pubkey(hex_pubkey):
        log.warning("POST /add — rejected: invalid pubkey (hex='%s...')", hex_pubkey[:16])
        return render_toast_redirect(
            "Error: Invalid public key. Must be a valid npub or 64-char hex string.",
            is_error=True,
        )

    config = load_config()
    for acc in config.get("accounts", []):
        if acc.get("hex_pubkey") == hex_pubkey:
            log.warning("POST /add — rejected: pubkey %s... already configured", hex_pubkey[:16])
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
    log.info("POST /add — added account hex=%s..., npub=%s, topic=%s",
             hex_pubkey[:16], npub or "(hex-only)", ntfy_topic)

    asyncio.create_task(reload_bridge(request.app))
    return render_toast_redirect(f"Added account. Listener starting...")


async def handle_delete(request):
    data = await request.post()
    try:
        index = int(data.get("index", -1))
    except (ValueError, TypeError):
        log.warning("POST /delete — rejected: invalid index '%s'", data.get("index"))
        return render_toast_redirect("Error: Invalid index.", is_error=True)

    config = load_config()
    accounts = config.get("accounts", [])
    if index < 0 or index >= len(accounts):
        log.warning("POST /delete — rejected: index %d out of range (have %d accounts)", index, len(accounts))
        return render_toast_redirect("Error: Account not found.", is_error=True)

    removed = accounts.pop(index)
    removed_name = removed.get("npub") or removed.get("hex_pubkey", "")[:16]
    save_config(config)
    log.info("POST /delete — removed account #%d: %s (hex=%s...)",
             index, removed_name, removed.get("hex_pubkey", "")[:16])

    asyncio.create_task(reload_bridge(request.app))
    return render_toast_redirect(f"Removed {removed_name}. Listener stopped.")


async def handle_settings(request):
    data = await request.post()
    ntfy_base = data.get("ntfy_base_url", "").strip().rstrip("/")
    log.info("POST /settings — ntfy_base_url='%s'", ntfy_base)

    if not ntfy_base:
        log.warning("POST /settings — rejected: empty ntfy base URL")
        return render_toast_redirect("Error: ntfy base URL is required.", is_error=True)

    config = load_config()
    old_base = config.get("ntfy_base_url", "")
    config["ntfy_base_url"] = ntfy_base
    save_config(config)

    if old_base != ntfy_base:
        log.info("POST /settings — ntfy_base_url changed: '%s' -> '%s'. Reloading all listeners.", old_base, ntfy_base)
        asyncio.create_task(reload_bridge(request.app))
        return render_toast_redirect("Settings saved. Reloading all listeners...")
    else:
        log.info("POST /settings — ntfy_base_url unchanged. No reload needed.")
        return render_toast_redirect("Settings saved.")


# --- NOSTR BRIDGE LOGIC ---
async def fetch_metadata(pubkey_hex):
    """Discover relays and NIP-29 group IDs for a pubkey."""
    follows = set()
    inbox_relays = set()
    group_ids = set()
    label = pubkey_hex[:12]

    log.info("[%s] Discovering relays and groups from %d bootstrap relays...",
             label, len(BOOTSTRAP_RELAYS))

    for relay in BOOTSTRAP_RELAYS:
        try:
            log.debug("[%s] Querying bootstrap relay: %s", label, relay)
            async with websockets.connect(relay, open_timeout=5, close_timeout=5) as ws:
                req_id = f"boot-{pubkey_hex[:6]}-{int(time.time())}"
                req = [
                    "REQ",
                    req_id,
                    {"authors": [pubkey_hex], "kinds": [3, 10002, 10009, 9021]},
                ]
                log.debug("[%s] Sending bootstrap REQ %s kinds=[3,10002,10009,9021]", label, req_id)
                await ws.send(json.dumps(req))

                event_count = 0
                while True:
                    try:
                        msg = await asyncio.wait_for(ws.recv(), timeout=5)
                        data = json.loads(msg)
                        if data[0] == "EOSE":
                            log.debug("[%s] Bootstrap EOSE from %s (got %d events)", label, relay, event_count)
                            break
                        if data[0] == "EVENT":
                            event_count += 1
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
                                            log.debug("[%s]   + relay: %s", label, t[1])
                            elif kind in [10009, 9021]:
                                for t in tags:
                                    if t[0] == "h" and len(t) > 1:
                                        group_ids.add(t[1])
                                        log.debug("[%s]   + group: %s (kind %d)", label, t[1][:16], kind)
                    except asyncio.TimeoutError:
                        log.debug("[%s] Bootstrap timeout from %s after %d events", label, relay, event_count)
                        break
        except asyncio.TimeoutError:
            log.warning("[%s] Bootstrap relay %s — connection timeout", label, relay)
        except ConnectionRefusedError:
            log.warning("[%s] Bootstrap relay %s — connection refused", label, relay)
        except OSError as e:
            log.warning("[%s] Bootstrap relay %s — OS error: %s", label, relay, e)
        except Exception as e:
            log.warning("[%s] Bootstrap relay %s — unexpected error: %s", label, relay, e)
            log.debug(traceback.format_exc())
            continue

    all_relays = inbox_relays.union(set(BOOTSTRAP_RELAYS))
    relays = list(all_relays)[:MAX_RELAYS]

    log.info("[%s] Discovery complete:", label)
    log.info("[%s]   Relays:  %d total (%d from NIP-65 + %d bootstrap), using %d (capped at %d)",
             label, len(all_relays), len(inbox_relays), len(BOOTSTRAP_RELAYS), len(relays), MAX_RELAYS)
    log.info("[%s]   Groups:  %d NIP-29 group IDs (kinds 10009/9021)", label, len(group_ids))
    log.info("[%s]   Follows: %d (kind 3)", label, len(follows))

    if not relays:
        log.warning("[%s] No relays discovered! This account will not receive any events.", label)

    return relays, list(group_ids)


async def listen_to_relay(relay_url, group_ids, pubkey_hex, ntfy_url, account_label):
    """Listen on a single relay. Exits cleanly on CancelledError."""
    reconnect_delay = 30
    attempt = 0
    label = account_label

    try:
        while True:
            attempt += 1
            try:
                log.info("[%s] Relay %s — connecting (attempt %d)...", label, relay_url, attempt)
                t0 = time.monotonic()
                async with websockets.connect(
                    relay_url, open_timeout=10, close_timeout=10
                ) as ws:
                    connect_ms = int((time.monotonic() - t0) * 1000)
                    log.info("[%s] Relay %s — connected in %dms", label, relay_url, connect_ms)
                    attempt = 0  # reset on successful connect

                    current_time = int(time.time())
                    sub_id = f"listen-{pubkey_hex[:6]}-{relay_url[-8:]}"
                    filters = {"#p": [pubkey_hex], "since": current_time, "limit": 0}
                    sub_msg = ["REQ", sub_id, filters]
                    log.info("[%s] Relay %s — subscribing with id=%s filter=#p[%s...], since=%d",
                             label, relay_url, sub_id, pubkey_hex[:8], current_time)
                    await ws.send(json.dumps(sub_msg))

                    if group_ids:
                        group_sub_id = f"groups-{pubkey_hex[:6]}-{relay_url[-8:]}"
                        group_msg = [
                            "REQ", group_sub_id,
                            {"#h": group_ids, "since": current_time, "limit": 0},
                        ]
                        log.info("[%s] Relay %s — subscribing to %d groups with id=%s",
                                 label, relay_url, len(group_ids), group_sub_id)
                        await ws.send(json.dumps(group_msg))

                    msg_count = 0
                    last_stats_log = time.monotonic()

                    while True:
                        msg = await ws.recv()
                        msg_count += 1
                        try:
                            data = json.loads(msg)
                        except json.JSONDecodeError:
                            log.warning("[%s] Relay %s — received non-JSON message #%d: %s...",
                                        label, relay_url, msg_count, str(msg)[:100])
                            continue

                        msg_type = data[0] if isinstance(data, list) and len(data) > 0 else "unknown"

                        if msg_type == "EOSE":
                            log.debug("[%s] Relay %s — EOSE for sub '%s'",
                                     label, relay_url, data[1] if len(data) > 1 else "?")
                            continue

                        if msg_type == "NOTICE":
                            log.info("[%s] Relay %s — NOTICE: %s", label, relay_url, data[1])
                            continue

                        if msg_type == "EVENT":
                            evt = data[2] if len(data) > 2 else {}
                            evt_id = evt.get("id", "")[:16]
                            evt_kind = evt.get("kind", "?")
                            evt_pubkey = evt.get("pubkey", "")[:12]
                            sub_for = data[1] if len(data) > 1 else "?"

                            if evt_id in processed_events:
                                event_stats["deduped"] += 1
                                log.debug("[%s] Relay %s — EVENT %s (kind=%s, from=%s) DEDUPED",
                                         label, relay_url, evt_id, evt_kind, evt_pubkey)
                                continue

                            processed_events.append(evt_id)
                            event_stats["received"] += 1

                            is_group = any(t[0] == "h" for t in evt.get("tags", []))
                            title = "NIP-29 Group Alert" if is_group else "Nostr Mention"
                            author = evt.get("pubkey", "")[:12]
                            content = evt.get("content", "New notification")
                            content_preview = content[:80].replace("\n", " ")
                            if len(content) > 280:
                                content = content[:277] + "..."

                            log.info("[%s] EVENT from relay %s:", label, relay_url)
                            log.info("[%s]   id:      %s (kind=%d, group=%s)", label, evt_id, evt_kind, is_group)
                            log.info("[%s]   author:  %s", label, evt_pubkey)
                            log.info("[%s]   sub:     %s", label, sub_for)
                            log.info("[%s]   content: %s%s", label, content_preview,
                                     "..." if len(evt.get("content", "")) > 80 else "")
                            log.info("[%s]   -> sending to ntfy: %s", label, ntfy_url)

                            try:
                                t0 = time.monotonic()
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
                                ntfy_ms = int((time.monotonic() - t0) * 1000)
                                if resp.status_code == 200:
                                    event_stats["sent"] += 1
                                    log.info("[%s]   <- ntfy OK (%d, %dms)", label, resp.status_code, ntfy_ms)
                                else:
                                    event_stats["failed"] += 1
                                    log.error("[%s]   <- ntfy FAILED: HTTP %d (%dms) — body: %s",
                                              label, resp.status_code, ntfy_ms, resp.text[:200])
                            except requests.ConnectionError as e:
                                event_stats["failed"] += 1
                                log.error("[%s]   <- ntfy CONNECTION ERROR: %s", label, e)
                                log.error("[%s]   Is ntfy reachable at %s?", label, ntfy_url)
                            except requests.Timeout:
                                event_stats["failed"] += 1
                                log.error("[%s]   <- ntfy TIMEOUT after 10s", label)
                            except Exception as e:
                                event_stats["failed"] += 1
                                log.error("[%s]   <- ntfy error: %s", label, e)
                                log.debug(traceback.format_exc())
                        else:
                            log.debug("[%s] Relay %s — unknown message type: %s",
                                     label, relay_url, msg_type)

                        # Periodic stats log every 60s
                        now = time.monotonic()
                        if now - last_stats_log >= 60:
                            log.info("[%s] Stats for %s: %d msgs received, event buffer: %d/%d",
                                     label, relay_url, msg_count, len(processed_events),
                                     processed_events.maxlen)
                            log.info("[%s] Global stats: received=%d, deduped=%d, sent=%d, failed=%d",
                                     label, event_stats["received"], event_stats["deduped"],
                                     event_stats["sent"], event_stats["failed"])
                            last_stats_log = now

            except asyncio.CancelledError:
                raise
            except websockets.InvalidStatusCode as e:
                log.error("[%s] Relay %s — rejected connection (HTTP %d)", label, relay_url, e.status_code)
            except (websockets.ConnectionClosed, websockets.ConnectionClosedError) as e:
                log.warning("[%s] Relay %s — connection closed: code=%s reason='%s'",
                           label, relay_url, getattr(e, 'rcvd', '?'), getattr(e, 'reason', ''))
            except ConnectionRefusedError:
                log.warning("[%s] Relay %s — connection refused", label, relay_url)
            except OSError as e:
                log.warning("[%s] Relay %s — OS error: %s", label, relay_url, e)
            except Exception as e:
                log.error("[%s] Relay %s — unexpected error: %s", label, relay_url, e)
                log.error(traceback.format_exc())

            log.info("[%s] Relay %s — reconnecting in %ds...", label, relay_url, reconnect_delay)
            await asyncio.sleep(reconnect_delay)
    except asyncio.CancelledError:
        log.info("[%s] Relay %s — listener cancelled (account removed or reloading)", label, relay_url)
        return


async def start_account_bridge(account, ntfy_base_url):
    """Run all relay listeners for one account as a gather."""
    hex_pubkey = account["hex_pubkey"]
    ntfy_topic = account.get("ntfy_topic", "nostr-events")
    ntfy_url = f"{ntfy_base_url.rstrip('/')}/{ntfy_topic}"
    label = account.get("npub") or hex_pubkey[:12]

    log.info("=" * 60)
    log.info("[%s] === ACCOUNT BRIDGE STARTING ===", label)
    log.info("[%s]   hex pubkey:  %s", label, hex_pubkey)
    if account.get("npub"):
        log.info("[%s]   npub:        %s", label, account["npub"])
    log.info("[%s]   ntfy topic:  %s", label, ntfy_topic)
    log.info("[%s]   ntfy URL:    %s", label, ntfy_url)
    log.info("=" * 60)

    relays, group_ids = await fetch_metadata(hex_pubkey)

    # Persist discovered relay/group counts back to config
    config = load_config()
    for acc in config.get("accounts", []):
        if acc["hex_pubkey"] == hex_pubkey:
            acc["relay_count"] = len(relays)
            acc["group_count"] = len(group_ids)
            break
    save_config(config)

    if not relays:
        log.warning("[%s] No relays found — this account will be idle until config reload.", label)
        return

    log.info("[%s] Spawning %d relay listener(s)...", label, len(relays))
    for i, r in enumerate(relays):
        log.debug("[%s]   [%d/%d] %s", label, i + 1, len(relays), r)

    tasks = [
        asyncio.create_task(listen_to_relay(r, group_ids, hex_pubkey, ntfy_url, label))
        for r in relays
    ]
    await asyncio.gather(*tasks, return_exceptions=True)
    log.info("[%s] All relay listeners exited.", label)


async def stop_account(hex_pubkey, label=""):
    """Cancel and wait for an account's bridge task to fully stop."""
    task = running_accounts.pop(hex_pubkey, None)
    if task is None:
        log.debug("stop_account called for %s... but no running task found", hex_pubkey[:12])
        return
    display = label or hex_pubkey[:12]
    log.info("[%s] Cancelling all listeners...", display)
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass
    log.info("[%s] All listeners stopped.", display)


async def reload_bridge(app):
    """Hot-reload: diff config against running accounts, stop removed, start new."""
    async with _restart_lock:
        config = load_config()
        accounts = config.get("accounts", [])
        ntfy_base = config.get("ntfy_base_url", "http://ntfy_app_1:80")

        wanted = {acc["hex_pubkey"]: acc for acc in accounts}
        current_keys = set(running_accounts.keys())
        wanted_keys = set(wanted.keys())
        base_changed = ntfy_base != app.get("_last_ntfy_base", "")

        to_stop = current_keys - wanted_keys
        to_start = wanted_keys - current_keys
        to_restart = (current_keys & wanted_keys) if base_changed else set()

        log.info("--- Bridge reload ---")
        log.info("  Running: %d account(s), Config: %d account(s)", len(current_keys), len(wanted_keys))
        log.info("  ntfy_base changed: %s", base_changed)
        log.info("  To stop:    %s", [k[:12] for k in to_stop] or "(none)")
        log.info("  To start:   %s", [k[:12] for k in to_start] or "(none)")
        log.info("  To restart: %s", [k[:12] for k in to_restart] or "(none)")

        for hex_pk in to_stop | to_restart:
            label = wanted.get(hex_pk, {}).get("npub", hex_pk[:12])
            await stop_account(hex_pk, label)

        for hex_pk in (to_start | to_restart):
            acc = wanted[hex_pk]
            label = acc.get("npub") or hex_pk[:12]
            log.info("[%s] Starting account bridge...", label)
            task = asyncio.create_task(
                _run_account_safely(acc, ntfy_base, label)
            )
            running_accounts[hex_pk] = task

        app["_last_ntfy_base"] = ntfy_base

        if not running_accounts:
            log.info("No accounts active. Waiting for user input via Web UI.")
        else:
            log.info("Bridge active: %d account(s).", len(running_accounts))
            for pk, task in running_accounts.items():
                log.info("  %s... — %s", pk[:16], "running" if not task.done() else "DONE")
        log.info("--- Bridge reload complete ---")


async def _run_account_safely(account, ntfy_base_url, label):
    """Wrapper that logs if an account bridge ever exits unexpectedly."""
    try:
        await start_account_bridge(account, ntfy_base_url)
    except asyncio.CancelledError:
        log.debug("[%s] Account bridge task cancelled.", label)
        return
    except Exception as e:
        log.error("[%s] Account bridge CRASHED: %s", label, e)
        log.error(traceback.format_exc())


async def start_bridge(app):
    """Called once on app startup."""
    _log_environment()
    log.info("--- Initial bridge startup ---")
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

    log.info("Starting aiohttp web server on 0.0.0.0:8181...")
    web.run_app(app, host="0.0.0.0", port=8181, print=None)
