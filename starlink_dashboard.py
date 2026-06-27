#!/usr/bin/env python3
"""
╔══════════════════════════════════════════════════════════════╗
║              Starlink Local Dashboard                        ║
║                                                              ║
║  Connects to your dish's gRPC API and serves a live         ║
║  monitoring dashboard — fully offline after first run.       ║
║                                                              ║
║  Usage:  python starlink_dashboard.py                        ║
║  Open:   http://localhost:8889                               ║
║                                                              ║
║  Env vars:                                                   ║
║    STARLINK_HOST   dish address   (default: 192.168.100.1:9200)  ║
║    DASHBOARD_PORT  local port     (default: 8889)            ║
╚══════════════════════════════════════════════════════════════╝

Auto-installs:  grpcio  protobuf
Optional:       pip install grpcio-reflection  (enables auto-discovery
                for future firmware variants)
"""

import os, sys, json, math, time, struct, random, socket
import threading, webbrowser, collections, urllib.request
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse

# ── Config ─────────────────────────────────────────────────────────────────────
STARLINK_HOST        = os.environ.get("STARLINK_HOST", "192.168.100.1:9200")
STARLINK_ROUTER_HOST = os.environ.get("STARLINK_ROUTER_HOST", "192.168.2.1:9000")
PORT                 = int(os.environ.get("DASHBOARD_PORT", "8889"))
POLL_OPTIONS   = (0.25, 0.5, 1.0, 5.0, 10.0, 15.0, 30.0)
POLL_SEC       = 0.5      # default polling interval
HISTORY_MAX    = 2400     # enough headroom for faster/slower polling windows

# ── Auto-install grpcio ────────────────────────────────────────────────────────
def _pip(*pkgs):
    import subprocess
    subprocess.check_call([sys.executable, "-m", "pip", "install", *pkgs, "-q"])

try:
    import grpc
except ImportError:
    print("[setup] Installing grpcio…")
    _pip("grpcio")
    import grpc

try:
    import google.protobuf  # noqa: F401
except ImportError:
    print("[setup] Installing protobuf…")
    _pip("protobuf")

# ── Protobuf wire-format helpers ───────────────────────────────────────────────
# We speak directly to the dish's gRPC endpoint using the raw proto wire format,
# so we don't need to compile .proto files or install grpcio-tools.

def _vi_enc(n: int) -> bytes:
    """Encode an integer as a protobuf varint."""
    out = []
    while n > 0x7F:
        out.append((n & 0x7F) | 0x80)
        n >>= 7
    out.append(n)
    return bytes(out)

def _ld(field: int, body: bytes) -> bytes:
    """Return a length-delimited protobuf field."""
    return _vi_enc((field << 3) | 2) + _vi_enc(len(body)) + body

def _bool_field(field: int, value: bool) -> bytes:
    """Return a protobuf bool field."""
    return _vi_enc((field << 3) | 0) + _vi_enc(1 if value else 0)

# ── The one request we ever send ───────────────────────────────────────────────
# ToDevice { request(14): Request { get_status(1006): {} } }
# Field 14 → wrapper Request message
# Field 1006 → GetStatusRequest (empty)  — confirmed from sparky8512/starlink-grpc-tools
_STATUS_REQ: bytes = _ld(1004, b"")
_OBSTRUCTION_MAP_REQ: bytes = _ld(2008, b"")

_CONTROL_REQUESTS: dict[str, tuple[str, bytes]] = {
    "dish_reboot": ("Reboot dish", _ld(1001, b"")),
    "dish_factory_reset": ("Factory reset dish", _ld(2021, b"")),
    "dish_stow": ("Stow dish", _ld(2002, b"")),
    "dish_unstow": ("Unstow dish", _ld(2002, _bool_field(1, True))),
    "dish_clear_obstruction_map": ("Clear obstruction map", _ld(2017, b"")),
    "dish_self_test": ("Run dish self-test", _ld(1031, _bool_field(1, True))),
    "dish_get_device_info": ("Get dish device info", _ld(1008, b"")),
    "dish_get_context": ("Get dish context", _ld(2003, b"")),
    "dish_get_config": ("Get dish config", _ld(2011, b"")),
    "dish_get_data": ("Get dish data", _ld(2015, b"")),
    "dish_get_diagnostics": ("Get dish diagnostics", _ld(6000, b"")),
    "dish_get_location": ("Get dish location", _ld(1017, b"")),
    "dish_get_log": ("Get dish log", _ld(1012, b"")),
    "dish_get_network_interfaces": ("Get network interfaces", _ld(1015, b"")),
    "dish_get_persistent_stats": ("Get persistent stats", _ld(1022, b"")),
    "dish_get_connections": ("Get connections", _ld(1023, b"")),
    "dish_get_emc": ("Get EMC state", _ld(2009, b"")),
    "dish_get_rssi_scan_result": ("Get RSSI scan result", _ld(2020, b"")),
    "dish_get_radio_stats": ("Get radio stats", _ld(1036, b"")),
    "dish_get_goroutine_stack_traces": ("Get stack traces", _ld(1041, b"")),
    "dish_time": ("Get device time", _ld(1037, b"")),
    "dish_user_reported_issue": ("Send user issue marker", _ld(2029, b"")),
    "dish_inhibit_gps_on": ("Inhibit GPS on", _ld(2014, _bool_field(1, True))),
    "dish_inhibit_gps_off": ("Inhibit GPS off", _ld(2014, _bool_field(1, False))),
    "dish_inhibit_rf_on": ("Inhibit RF on", _ld(2026, _bool_field(1, True))),
    "dish_inhibit_rf_off": ("Inhibit RF off", _ld(2026, _bool_field(1, False))),
    "dish_max_power_test_on": ("Max power test on", _ld(2018, _bool_field(1, True))),
    "dish_max_power_test_off": ("Max power test off", _ld(2018, _bool_field(1, False))),
    "dish_reset_button_press": ("Simulate reset button press", _ld(2022, _bool_field(1, True))),
    "speedtest_start": ("Start speed test", _ld(1027, b"")),
    "speedtest_status": ("Get speed-test status", _ld(1028, b"")),
    "router_reboot": ("Reboot router", _ld(1001, b"")),
    "router_factory_reset": ("Factory reset router", _ld(1011, b"")),
    "wifi_clients": ("Get router clients", _ld(3002, b"")),
    "wifi_config": ("Get router config", _ld(3009, b"")),
    "wifi_status": ("Get router status", _ld(1004, b"")),
    "wifi_ping_metrics": ("Get Wi-Fi ping metrics", _ld(3007, b"")),
    "wifi_firewall": ("Get firewall rules", _ld(3024, b"")),
    "wifi_guest_info": ("Get guest info", _ld(3020, b"")),
    "wifi_backhaul_stats": ("Get backhaul stats", _ld(3029, b"")),
    "wifi_diagnostics": ("Get router diagnostics", _ld(6000, b"")),
    "wifi_self_test": ("Run router self-test", _ld(3018, b"")),
    "wifi_run_self_test": ("Run extended router self-test", _ld(3028, b"")),
    "wifi_calibration_mode": ("Enter Wi-Fi calibration mode", _ld(3019, b"")),
    "wifi_toggle_poe_on": ("PoE negotiation on", _ld(3025, _bool_field(1, True))),
    "wifi_toggle_poe_off": ("PoE negotiation off", _ld(3025, _bool_field(1, False))),
    "wifi_umbilical_on": ("Umbilical mode on", _ld(3030, _bool_field(1, True))),
    "wifi_umbilical_off": ("Umbilical mode off", _ld(3030, _bool_field(1, False))),
    "wifi_reset_eth_phy": ("Reset Ethernet PHY", _ld(3033, b"")),
    "wifi_flush_hardware_nat": ("Flush hardware NAT", _ld(3034, b"")),
    "wifi_run_debug_netsys": ("Run router net debug", _ld(3032, b"")),
}

_ROUTER_ACTIONS = {
    action for action in _CONTROL_REQUESTS
    if action.startswith("wifi_") or action.startswith("router_") or action.startswith("speedtest_")
}

_DANGEROUS_ACTIONS = {
    "dish_reboot", "dish_factory_reset", "dish_inhibit_gps_on", "dish_inhibit_rf_on",
    "dish_max_power_test_on", "dish_reset_button_press", "router_reboot",
    "router_factory_reset", "wifi_calibration_mode", "wifi_toggle_poe_on",
    "wifi_toggle_poe_off", "wifi_umbilical_on", "wifi_umbilical_off",
    "wifi_reset_eth_phy", "wifi_flush_hardware_nat", "wifi_run_debug_netsys",
}

# ── Generic protobuf parser ────────────────────────────────────────────────────
def _vi_dec(data: bytes, pos: int):
    """Decode a varint, return (value, new_pos)."""
    n = shift = 0
    while pos < len(data):
        b = data[pos]; pos += 1
        n |= (b & 0x7F) << shift
        if not (b & 0x80):
            return n, pos
        shift += 7
    return n, pos

def _parse(data: bytes) -> dict:
    """Parse proto bytes → {field_num: [value, …]}  (no schema required)."""
    out: dict = {}
    i = 0
    while i < len(data):
        try:
            tag, i = _vi_dec(data, i)
        except Exception:
            break
        fn, wt = tag >> 3, tag & 7
        try:
            if wt == 0:                                    # varint
                v, i = _vi_dec(data, i)
                out.setdefault(fn, []).append(v)
            elif wt == 1:                                  # 64-bit
                out.setdefault(fn, []).append(struct.unpack_from("<d", data, i)[0]); i += 8
            elif wt == 2:                                  # length-delimited
                l, i = _vi_dec(data, i)
                out.setdefault(fn, []).append(data[i : i + l]); i += l
            elif wt == 5:                                  # 32-bit float
                out.setdefault(fn, []).append(struct.unpack_from("<f", data, i)[0]); i += 4
            else:
                break
        except Exception:
            break
    return out

# ── Typed extractors ───────────────────────────────────────────────────────────
def _f(d: dict, *fields) -> float:
    for f in fields:
        for v in d.get(f, []):
            if isinstance(v, float) and math.isfinite(v):
                return v
    return 0.0

def _u(d: dict, *fields) -> int:
    for f in fields:
        for v in d.get(f, []):
            if isinstance(v, int):
                return v
    return 0

def _s(d: dict, *fields) -> str:
    for f in fields:
        for v in d.get(f, []):
            if isinstance(v, (bytes, bytearray)):
                s = v.decode("utf-8", errors="replace").strip("\x00").strip()
                if s:
                    return s
    return ""

def _sub(d: dict, field: int) -> dict:
    for v in d.get(field, []):
        if isinstance(v, (bytes, bytearray)) and v:
            return _parse(v)
    return {}

def _enum(value: int, mapping: dict[int, str], default: str = "UNKNOWN") -> str:
    return mapping.get(value, default if value == 0 else str(value))

def _mask_mac(mac: str) -> str:
    parts = mac.split(":")
    if len(parts) != 6:
        return mac
    return ":".join(parts[:3] + ["XX", "XX", "XX"])

# ── Starlink gRPC client ───────────────────────────────────────────────────────
class StarlinkClient:
    _SVC    = "SpaceX.API.Device.Device"
    _METHOD = "Handle"

    def __init__(self, host: str = STARLINK_HOST):
        self.host  = host
        self.error: str | None = None
        self._chan = None
        self._rpc  = None

    def connect(self) -> bool:
        try:
            opts = [
                ("grpc.max_receive_message_length", 8 * 1024 * 1024),
                ("grpc.keepalive_time_ms",          10_000),
                ("grpc.keepalive_timeout_ms",        5_000),
            ]
            self._chan = grpc.insecure_channel(self.host, options=opts)
            self._rpc  = self._chan.unary_unary(
                f"/{self._SVC}/{self._METHOD}",
                request_serializer=None,
                response_deserializer=None,
            )
            raw = self._call(timeout=6)
            if raw is None or len(raw) < 4:
                self.error = "Empty or no response from dish"
                return False
            self.error = None
            print(f"[starlink] Connected ({self.host})")
            return True
        except grpc.RpcError as e:
            details = e.details() or ""
            self.error = f"RPC {e.code()}: {details}"
            if self.host.endswith(":9201") and "Expected SETTINGS frame" in details:
                self.error += " (port 9201 speaks HTTP/1.1/gRPC-Web; use 192.168.100.1:9200 for grpcio)"
        except Exception as e:
            self.error = str(e)
        return False

    def _call(self, timeout: float = 8) -> bytes | None:
        return self._rpc(_STATUS_REQ, timeout=timeout)

    def _call_obstruction_map(self, timeout: float = 8) -> bytes | None:
        return self._rpc(_OBSTRUCTION_MAP_REQ, timeout=timeout)

    def _call_request(self, request: bytes, timeout: float = 12) -> bytes | None:
        return self._rpc(request, timeout=timeout)

    def get_status(self) -> dict | None:
        if self._rpc is None:
            return None
        try:
            raw = self._call()
            return _parse_dish_status(raw)
        except grpc.RpcError as e:
            self.error = str(e.code())
            self._rpc = None
            return None
        except Exception as e:
            self.error = str(e)
            return None

    def get_obstruction_map(self) -> dict | None:
        if self._rpc is None:
            return None
        try:
            raw = self._call_obstruction_map(timeout=8)
            return _parse_obstruction_map(raw)
        except grpc.RpcError as e:
            self.error = str(e.code())
            self._rpc = None
            return None
        except Exception as e:
            self.error = str(e)
            return None

    def run_control(self, action: str) -> dict:
        if action not in _CONTROL_REQUESTS:
            return {"ok": False, "action": action, "error": "Unsupported action"}
        if self._rpc is None and not self.connect():
            return {"ok": False, "action": action, "error": self.error or "Not connected"}

        label, request = _CONTROL_REQUESTS[action]
        try:
            raw = self._call_request(request)
            return _parse_control_response(action, label, raw)
        except grpc.RpcError as e:
            self.error = f"{e.code()}: {e.details()}"
            if e.code() == grpc.StatusCode.UNAVAILABLE:
                self._rpc = None
            return {"ok": False, "action": action, "label": label, "error": self.error}
        except Exception as e:
            self.error = str(e)
            return {"ok": False, "action": action, "label": label, "error": self.error}

# ── Response parser ────────────────────────────────────────────────────────────
# Field numbers sourced from the open-source sparky8512/starlink-grpc-tools project.
# DishGetStatusResponse key fields (all ~1000-range):
#   1001 state enum  1006 alerts msg  1007 obstruction_stats msg
#   1009 snr  1010 s2ff  1011 drop_rate  1012 dl_bps  1013 ul_bps  1014 latency_ms
#   1016 azimuth  1017 elevation

def _find_status_bytes(raw: bytes) -> bytes | None:
    """
    Navigate FromDevice → DishGetStatusResponse.
    Tries two common structures seen across firmware generations:
      A) FromDevice.response(14) → Response.dish_get_status(2006)
      B) FromDevice.dish_get_status(2006) directly
      C) Heuristic: largest deeply-nested sub-message
    """
    top = _parse(raw)

    # Strategy A
    for fld in (2004, 2006):
        for v in top.get(fld, []):
            if isinstance(v, (bytes, bytearray)) and len(v) > 8:
                return bytes(v)

    # Strategy B
    for wrapper in (1, 14):
        resp_map = _sub(top, wrapper)
        for fld in (2004, 2006):
            for v in resp_map.get(fld, []):
                if isinstance(v, (bytes, bytearray)) and len(v) > 8:
                    return bytes(v)

    # Strategy C — find the longest sub-message 2 levels deep
    best_len, best = 0, None
    for vals in top.values():
        for b1 in vals:
            if not isinstance(b1, (bytes, bytearray)):
                continue
            sub = _parse(b1)
            for vals2 in sub.values():
                for b2 in vals2:
                    if isinstance(b2, (bytes, bytearray)) and len(b2) > best_len:
                        best_len, best = len(b2), bytes(b2)
    return best

def _parse_status_message(top: dict) -> tuple[int, str]:
    status = _sub(top, 2)
    return _u(status, 1), _s(status, 2)

def _parse_wifi_clients(response: dict) -> list[dict]:
    out = []
    client_resp = _sub(response, 3002)
    for item in client_resp.get(1, []):
        if not isinstance(item, (bytes, bytearray)):
            continue
        c = _parse(item)
        name = _s(c, 31, 1) or "Unnamed"
        out.append({
            "name": name,
            "mac": _mask_mac(_s(c, 2)),
            "ip": _s(c, 3),
            "domain": _s(c, 22),
            "dhcp_active": bool(_u(c, 46)),
        })
    return out[:50]

def _parse_control_response(action: str, label: str, raw: bytes | None) -> dict:
    if not raw:
        return {"ok": False, "action": action, "label": label, "error": "Empty response"}

    top = _parse(raw)
    code, message = _parse_status_message(top)
    ok = code == 0
    result = {
        "ok": ok,
        "action": action,
        "label": label,
        "status_code": code,
        "message": message or ("OK" if ok else "Device returned an error"),
        "ts": time.time(),
    }

    if action == "wifi_clients":
        clients = _parse_wifi_clients(top)
        result["clients"] = clients
        result["message"] = f"{len(clients)} client(s) returned"
    elif action == "wifi_config":
        cfg_resp = _sub(top, 3009)
        cfg = _sub(cfg_resp, 1)
        result["config"] = {
            "country_code": _s(cfg, 3),
            "setup_complete": bool(_u(cfg, 7)),
            "version": _u(cfg, 9),
            "mac_wan": _s(cfg, 12),
            "mac_lan": _s(cfg, 13),
            "channel_2ghz": _u(cfg, 19),
        }
        result["message"] = "Router config read"
    elif action == "dish_self_test":
        resp = _sub(top, 1031)
        result["passed"] = bool(_u(resp, 1))
        report = _s(resp, 2)
        if report:
            result["report"] = report[:2000]
        result["message"] = "Dish self-test " + ("passed" if result["passed"] else "completed")
    elif action == "wifi_self_test":
        resp = _sub(top, 3016) or _sub(top, 3018)
        report = _s(resp, 2)
        if report:
            result["report"] = report[:2000]
        result["message"] = "Router self-test completed"

    return result

def _parse_dish_status(raw: bytes | None) -> dict | None:
    if not raw or len(raw) < 4:
        return None

    sb = _find_status_bytes(raw)
    if not sb:
        return None

    s     = _parse(sb)
    ds    = _sub(s, 2)     # DeviceState
    di    = _sub(s, 1)     # DeviceInfo
    obs   = _sub(s, 1004) or _sub(s, 1007)  # DishObstructionStats
    alrt  = _sub(s, 1005) or _sub(s, 1006)  # DishAlerts
    gps   = _sub(s, 1015)
    ready = _sub(s, 1019)
    swu   = _sub(s, 1026)
    align = _sub(s, 1027)
    init  = _sub(s, 1028)
    quat  = _sub(s, 1049)

    down    = _f(s, 1007, 1012)  # downlink_throughput_bps
    up      = _f(s, 1008, 1013)  # uplink_throughput_bps
    lat     = _f(s, 1009, 1014)  # pop_ping_latency_ms
    loss    = _f(s, 1003, 1011)  # pop_ping_drop_rate  (0-1)
    snr     = 0.0  # Current firmware does not expose raw status SNR here.
    s2ff    = _f(s, 1002, 1010)  # seconds_to_first_nonempty_slot
    az      = _f(s, 1011, 1016)  # boresight_azimuth_deg
    el      = _f(s, 1012, 1017)  # boresight_elevation_deg

    uptime  = _u(ds, 1)   # uptime_s
    hw      = _s(di, 2)   # hardware_version
    sw      = _s(di, 3)   # software_version
    boots   = _u(di, 8)   # bootcount
    if s2ff > 86400:
        s2ff = 0.0
    if boots > 100000:
        boots = 0

    obst_frac  = _f(obs, 2)  # fraction_obstructed
    cur_obst   = bool(_u(obs, 1))
    eth_speed   = _u(s, 1016)
    boot_ready  = bool(_u(s, 1030))
    sw_reboot_s = _u(s, 1031)
    if sw_reboot_s > 10_000_000:
        sw_reboot_s = 0

    mobility_map = {0: "STATIONARY", 1: "NOMADIC", 2: "MOBILE"}
    service_map = {
        1: "CONSUMER", 2: "BUSINESS", 3: "BUSINESS_PLUS",
        4: "COMMERCIAL_AVIATION",
    }
    update_map = {
        1: "IDLE", 2: "FETCHING", 3: "PRE_CHECK", 4: "WRITING",
        5: "POST_CHECK", 6: "REBOOT_REQUIRED", 7: "DISABLED", 8: "FAULTED",
    }
    actuator_map = {1: "YES", 2: "NO"}
    reboot_map = {
        0: "NONE", 1: "MANUAL", 2: "LOSS_OF_COMM", 3: "SWUPDATE_NOW",
        4: "SWUPDATE_SCHEDULED", 5: "APP", 6: "EMC",
        7: "FACTORY_RESET", 8: "TEST_CASE", 9: "THERMAL_POWER_CUT",
        10: "CRITICAL_PROCESS_DIED", 11: "NO_RF_READY",
        12: "POSTPONED_LOSS_OF_COMM", 13: "SWUPDATE_STATIONARY",
        14: "AAP_CRASH", 15: "XP70_SACS", 16: "INE_FAILED",
        17: "KERNEL_TAINTED",
    }
    rate_limit_map = {1: "NONE", 2: "UNKNOWN"}

    # Require at least one real metric to avoid silently returning zeros
    if down == 0 and up == 0 and uptime == 0 and lat == 0:
        return None

    state_map = {1: "CONNECTED", 2: "SEARCHING", 3: "BOOTING"}
    state_code = _u(s, 1001)
    state = state_map.get(state_code, "CONNECTED" if down > 0 or uptime > 0 else "UNKNOWN")

    alert_fields = {
        1: "motors stuck",    3: "thermal throttle",
        4: "thermal shutdown", 5: "mast not vertical",
        6: "unexpected location", 7: "slow ethernet",
        11: "roaming",        13: "heating",
    }
    active_alerts = [name for fld, name in alert_fields.items() if _u(alrt, fld)]

    return {
        "live": True, "ts": time.time(),
        "down":    round(down / 1e6, 1),
        "up":      round(up   / 1e6, 1),
        "latency": round(lat, 1),
        "loss":    round(min(100.0, max(0.0, loss * 100)), 3),
        "snr":     round(snr, 1),
        "s2ff":    round(s2ff, 2),
        "uptime":  uptime,
        "hardware": hw or "GEN3",
        "software": (sw[:12] if sw else ""),
        "software_full": sw,
        "boot_count": boots,
        "device_id": _s(di, 1),
        "country_code": _s(di, 4),
        "build_id": _s(di, 15),
        "hardware_index": _u(di, 16),
        "anti_rollback_version": _u(di, 9),
        "obstruction_pct": round(obst_frac * 100, 2),
        "currently_obstructed": cur_obst,
        "obstruction_valid_s": round(_f(obs, 4), 1),
        "obstruction_patches_valid": _u(obs, 10),
        "time_obstructed_s": round(_f(obs, 9), 1),
        "avg_obstruction_duration_s": round(_f(obs, 6), 1),
        "avg_obstruction_interval_s": round(_f(obs, 7), 1),
        "alerts": active_alerts,
        "state": state,
        "azimuth":   round(az, 1),
        "elevation": round(el, 1),
        "eth_speed_mbps": eth_speed,
        "mobility_class": _enum(_u(s, 1017), mobility_map),
        "class_of_service": _enum(_u(s, 1020), service_map),
        "software_update_state": _enum(_u(s, 1021), update_map),
        "software_update_progress": round(_f(swu, 2), 3),
        "software_update_requires_reboot": bool(_u(swu, 3) or boot_ready),
        "seconds_until_swupdate_reboot_possible": sw_reboot_s,
        "reboot_reason": _enum(_u(s, 1032), reboot_map),
        "has_actuators": _enum(_u(s, 1023), actuator_map),
        "has_signed_cals": bool(_u(s, 1025)),
        "cell_disabled": bool(_u(s, 1029)),
        "high_power_test_mode": bool(_u(s, 1033)),
        "moving_fast_persisted": bool(_u(s, 1042)),
        "treat_as_metered": bool(_u(s, 1056)),
        "user_debug_mode": bool(_u(s, 1055)),
        "account_shard": _u(s, 1051),
        "nat_flag": _u(s, 1053),
        "downlink_restriction": _enum(_u(s, 1044), rate_limit_map),
        "uplink_restriction": _enum(_u(s, 1045), rate_limit_map),
        "connected_routers": [_s({"v": [v]}, "v") for v in s.get(1040, []) if isinstance(v, (bytes, bytearray))],
        "gps": {
            "valid": bool(_u(gps, 1)),
            "satellites": _u(gps, 2),
            "no_sats_after_ttff": bool(_u(gps, 3)),
            "inhibit_gps": bool(_u(gps, 4)),
            "convergence_state": _u(gps, 5),
        },
        "ready_states": {
            "cady": bool(_u(ready, 1)),
            "scp": bool(_u(ready, 2)),
            "l1l2": bool(_u(ready, 3)),
            "xphy": bool(_u(ready, 4)),
            "aap": bool(_u(ready, 5)),
            "rf": bool(_u(ready, 6)),
        },
        "alignment": {
            "tilt_angle_deg": round(_f(align, 3), 1),
            "attitude_uncertainty_deg": round(_f(align, 7), 2),
            "desired_azimuth_deg": round(_f(align, 8), 1),
            "desired_elevation_deg": round(_f(align, 9), 1),
            "attitude_state": _u(align, 6),
        },
        "initialization": {
            "attitude_initialization": _u(init, 1),
            "burst_detected": _u(init, 2),
            "ekf_converged": _u(init, 3),
            "first_cplane": _u(init, 4),
            "first_pop_ping": _u(init, 5),
            "gps_valid": _u(init, 6),
            "initial_network_entry": _u(init, 7),
            "network_schedule": _u(init, 8),
            "rf_ready": _u(init, 9),
            "stable_connection": _u(init, 10),
        },
        "quaternion": {
            "q1": round(_f(quat, 1), 4),
            "q2": round(_f(quat, 2), 4),
            "q3": round(_f(quat, 3), 4),
            "q4": round(_f(quat, 4), 4),
        },
    }

def _parse_obstruction_map(raw: bytes | None) -> dict | None:
    if not raw or len(raw) < 4:
        return None

    top = _parse(raw)
    body = None
    for wrapper in (2008, 1, 14):
        for v in top.get(wrapper, []):
            if isinstance(v, (bytes, bytearray)) and len(v) > 16:
                body = bytes(v)
                break
        if body:
            break

    m = _parse(body or raw)
    rows = _u(m, 1)
    cols = _u(m, 2)
    if rows <= 0 or cols <= 0:
        return None

    snr = []
    for v in m.get(3, []):
        if isinstance(v, float) and math.isfinite(v):
            snr.append(v)
        elif isinstance(v, (bytes, bytearray)):
            usable = len(v) - (len(v) % 4)
            if usable:
                snr.extend(struct.unpack("<" + "f" * (usable // 4), v[:usable]))

    expected = rows * cols
    if len(snr) < expected:
        return None

    snr = snr[:expected]
    return {
        "live": True,
        "ts": time.time(),
        "rows": rows,
        "cols": cols,
        "min_elevation_deg": round(_f(m, 4), 1),
        "max_theta_deg": round(_f(m, 5), 1),
        "reference_frame": _u(m, 6),
        "snr": [round(max(-1.0, min(1.0, x)), 3) if math.isfinite(x) else -1.0 for x in snr],
    }

# ── Demo / fallback data ───────────────────────────────────────────────────────
_T0 = time.time()

def _demo() -> dict:
    t = time.time(); age = t - _T0
    return {
        "live": False, "ts": t,
        "down":    round(210 + 65 * math.sin(age * 0.04) + random.gauss(0, 12), 1),
        "up":      round(40  +  9 * math.sin(age * 0.03 + 1) + random.gauss(0, 3), 1),
        "latency": round(28  +  5 * math.sin(age * 0.06 + 2) + random.gauss(0, 2), 1),
        "loss":    round(max(0.0, 0.1 + random.gauss(0, 0.04)), 3),
        "snr": 9.2, "s2ff": 0.0,
        "uptime": int(age) + 892847,
        "hardware": "GEN3", "software": "demo-mode",
        "boot_count": 0,
        "obstruction_pct": 2.1, "currently_obstructed": False,
        "alerts": [], "state": "DEMO",
        "azimuth": 24.5, "elevation": 38.2,
    }

# ── Data collector ─────────────────────────────────────────────────────────────
class DataCollector:
    def __init__(self):
        self._lock    = threading.Lock()
        self._latest  = _demo()
        self._history: collections.deque = collections.deque(maxlen=HISTORY_MAX)
        self._obstruction_map: dict | None = None
        self._client  = StarlinkClient()
        self._router_client = StarlinkClient(STARLINK_ROUTER_HOST)
        self._live    = False
        self._poll_sec = POLL_SEC
        self._wake = threading.Event()

    def start(self):
        threading.Thread(target=self._loop, daemon=True).start()

    def _loop(self):
        connected = False
        last_try  = 0.0
        last_map  = 0.0
        retry_sec = 15.0

        while True:
            now = time.time()

            # Attempt (re)connect on schedule
            if not connected and now - last_try >= retry_sec:
                last_try  = now
                connected = self._client.connect()
                if not connected:
                    print(f"[starlink] Not reachable — {self._client.error}")
                    print("[starlink] Running in demo mode. Retrying every 15 s…")
                    retry_sec = 30.0  # back off after first failure

            snap = None
            if connected:
                snap = self._client.get_status()
                if snap is None:
                    connected = False
                    print(f"[starlink] Connection lost - {self._client.error}")
                elif now - last_map >= 30.0:
                    last_map = now
                    omap = self._client.get_obstruction_map()
                    if omap is not None:
                        with self._lock:
                            self._obstruction_map = omap

            if snap is None:
                snap = _demo()

            with self._lock:
                self._latest = snap
                self._history.append(dict(snap))
                self._live = snap.get("live", False)

            self._wake.wait(self.poll_sec)
            self._wake.clear()

    @property
    def latest(self) -> dict:
        with self._lock: return dict(self._latest)

    @property
    def history(self) -> list:
        with self._lock: return list(self._history)

    @property
    def poll_sec(self) -> float:
        with self._lock: return self._poll_sec

    def set_poll_sec(self, seconds: float) -> bool:
        if seconds not in POLL_OPTIONS:
            return False
        with self._lock:
            self._poll_sec = seconds
        self._wake.set()
        return True

    def run_control(self, action: str) -> dict:
        if action not in _CONTROL_REQUESTS:
            return {"ok": False, "action": action, "error": "Unsupported action"}

        if action in ("dish_stow", "dish_unstow"):
            with self._lock:
                has_actuators = self._latest.get("has_actuators")
            if has_actuators != "YES":
                label = _CONTROL_REQUESTS[action][0]
                return {
                    "ok": False,
                    "action": action,
                    "label": label,
                    "error": "This dish reports no adjustment motors, so stow/unstow is disabled for this hardware.",
                }

        client = self._router_client if action in _ROUTER_ACTIONS else self._client
        return client.run_control(action)

    @property
    def obstruction_map(self) -> dict:
        with self._lock:
            if self._obstruction_map is None:
                return {"live": False, "ts": time.time(), "rows": 0, "cols": 0, "snr": []}
            return dict(self._obstruction_map)

# ── HTTP handler ───────────────────────────────────────────────────────────────
_collector: DataCollector | None = None

class _Handler(BaseHTTPRequestHandler):
    def log_message(self, *_): pass   # silence access log

    def do_GET(self):
        p = urlparse(self.path).path
        if p in ("/", "/index.html"):
            self._send(200, "text/html; charset=utf-8", _get_html())
        elif p == "/api/status":
            self._send(200, "application/json", json.dumps(_collector.latest).encode())
        elif p == "/api/history":
            pts = _collector.history[-240:]
            self._send(200, "application/json", json.dumps(pts).encode())
        elif p == "/api/obstruction-map":
            self._send(200, "application/json", json.dumps(_collector.obstruction_map).encode())
        elif p == "/api/poll":
            self._send(200, "application/json", json.dumps({
                "poll_sec": _collector.poll_sec,
                "options": list(POLL_OPTIONS),
            }).encode())
        else:
            self.send_error(404)

    def do_POST(self):
        p = urlparse(self.path).path
        if p == "/api/control":
            try:
                length = int(self.headers.get("Content-Length", "0"))
                body = self.rfile.read(length).decode("utf-8") if length else "{}"
                action = str(json.loads(body).get("action", ""))
            except Exception:
                self.send_error(400, "Invalid control request")
                return
            result = _collector.run_control(action)
            self._send(200 if result.get("ok") else 400, "application/json", json.dumps(result).encode())
            return

        if p != "/api/poll":
            self.send_error(404)
            return
        try:
            length = int(self.headers.get("Content-Length", "0"))
            body = self.rfile.read(length).decode("utf-8") if length else "{}"
            payload = json.loads(body)
            seconds = float(payload.get("poll_sec"))
        except Exception:
            self.send_error(400, "Invalid poll interval")
            return
        if not _collector.set_poll_sec(seconds):
            self.send_error(400, "Unsupported poll interval")
            return
        self._send(200, "application/json", json.dumps({
            "poll_sec": _collector.poll_sec,
            "options": list(POLL_OPTIONS),
        }).encode())

    def _send(self, code: int, ctype: str, body):
        if isinstance(body, str): body = body.encode()
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Cache-Control", "no-cache")
        self.end_headers()
        self.wfile.write(body)

# ── Chart.js — download once, serve forever ────────────────────────────────────
_HERE        = os.path.dirname(os.path.abspath(__file__))
_CJS_CACHE   = os.path.join(_HERE, ".chartjs.cache.js")
_CJS_URL     = "https://cdnjs.cloudflare.com/ajax/libs/Chart.js/4.4.1/chart.umd.js"
_CJS_TAG     = None

def _chartjs_tag() -> str:
    global _CJS_TAG
    if _CJS_TAG: return _CJS_TAG
    if os.path.exists(_CJS_CACHE):
        try:
            js = open(_CJS_CACHE, encoding="utf-8").read()
            _CJS_TAG = f"<script>{js}</script>"
            print("[chart] Chart.js loaded from cache (offline OK)")
            return _CJS_TAG
        except Exception: pass
    try:
        print("[chart] Downloading Chart.js for offline caching…")
        with urllib.request.urlopen(_CJS_URL, timeout=15) as r:
            js = r.read().decode()
        open(_CJS_CACHE, "w", encoding="utf-8").write(js)
        _CJS_TAG = f"<script>{js}</script>"
        print("[chart] Chart.js cached — dashboard is now fully offline")
        return _CJS_TAG
    except Exception:
        print("[chart] Download failed; using CDN link (requires internet)")
        _CJS_TAG = f'<script src="{_CJS_URL}"></script>'
        return _CJS_TAG

_HTML_CACHE: str | None = None

def _get_html() -> bytes:
    global _HTML_CACHE
    if _HTML_CACHE is None:
        _HTML_CACHE = _HTML_TEMPLATE.replace("__CHARTJS__", _chartjs_tag())
    return _HTML_CACHE.encode()

# ── Dashboard HTML ─────────────────────────────────────────────────────────────
# Single-file page: all CSS, JS, and the sky-map canvas are inline.
# Fetches /api/status every 0.5 s and /api/history on load.
_HTML_TEMPLATE = '''<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Starlink Monitor</title>
<style>
:root{--bg:#050505;--surf:#101113;--surf2:#16181B;--bdr:#2B2E33;
  --blue:#7DA2FF;--teal:#FFFFFF;--warn:#FFB04D;--danger:#FF5D5D;--orange:#FF8A3D;
  --text:#F4F7FA;--muted:#8C939D;--faint:#25282D;}
*{box-sizing:border-box;margin:0;padding:0;}
html{scrollbar-width:none;-ms-overflow-style:none;}
html::-webkit-scrollbar,body::-webkit-scrollbar{display:none;}
body{background:var(--bg);color:var(--text);
  font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;min-height:100vh;
  scrollbar-width:none;-ms-overflow-style:none;}
.mono{font-family:"SF Mono","Cascadia Code",Consolas,"Courier New",monospace;}
/* header */
header{display:flex;align-items:center;justify-content:space-between;
  padding:16px 24px;border-bottom:1px solid var(--bdr);background:rgba(5,5,5,.86);}
.brand{display:flex;align-items:center;gap:10px;}
.brand-icon{width:36px;height:36px;background:#F4F7FA;color:#050505;border:1px solid #FFFFFF;
  border-radius:3px;display:flex;align-items:center;justify-content:center;font-size:12px;font-weight:800;letter-spacing:.06em;}
.brand-name{font-size:15px;font-weight:700;letter-spacing:.04em;text-transform:uppercase;}
.brand-sub{font-size:11px;color:var(--muted);font-family:monospace;margin-top:2px;}
.hdr-r{display:flex;align-items:center;gap:18px;}
.poll-ctl{display:flex;align-items:flex-end;gap:8px;}
.poll-ctl label{font-size:10px;font-weight:600;letter-spacing:.07em;color:var(--muted);text-transform:uppercase;}
.poll-ctl select{height:30px;background:#090A0B;color:#F4F7FA;border:1px solid var(--bdr);
  border-radius:4px;padding:0 8px;font-family:monospace;font-size:12px;outline:none;}
.poll-ctl select:focus{border-color:#D6DADE;}
.uptime-lbl{font-size:10px;font-weight:600;letter-spacing:.07em;
  color:var(--muted);text-transform:uppercase;}
.uptime-val{font-family:monospace;font-size:13px;color:var(--teal);margin-top:2px;}
.badge{display:flex;align-items:center;gap:7px;border-radius:20px;
  padding:6px 14px;font-family:monospace;font-size:12px;font-weight:600;}
.badge.live{background:#111;border:1px solid #D6DADE;color:#FFFFFF;}
.badge.demo{background:#1C1510;border:1px solid #5A3A1C;color:var(--warn);}
.badge.searching{background:#101624;border:1px solid #32466F;color:var(--blue);}
.dot{width:7px;height:7px;border-radius:50%;}
.dot.live{background:#FFFFFF;animation:pulse 2s infinite;}
.dot.demo{background:var(--warn);animation:pulse 2s infinite;}
.dot.searching{background:var(--blue);animation:pulse .8s infinite;}
/* main */
main{padding:18px 24px;}
.metrics{display:grid;grid-template-columns:repeat(4,1fr);gap:10px;margin-bottom:12px;}
.card{background:var(--surf);border:1px solid var(--bdr);border-radius:6px;padding:16px 18px;}
.clbl{display:flex;align-items:center;gap:6px;margin-bottom:10px;
  font-size:11px;font-weight:600;text-transform:uppercase;letter-spacing:.07em;color:var(--muted);}
.clbl .pip{width:7px;height:7px;border-radius:50%;}
.cval{font-family:monospace;font-size:34px;font-weight:500;
  color:var(--text);line-height:1;letter-spacing:0;}
.cunit{font-size:12px;color:var(--muted);margin-top:5px;}
/* chart */
.chart-card{padding:16px 20px;margin-bottom:12px;}
.chart-hdr{display:flex;justify-content:space-between;align-items:flex-start;margin-bottom:14px;}
.chart-title{font-size:14px;font-weight:500;}
.chart-sub{font-size:12px;color:var(--muted);margin-top:2px;}
.chart-legend{display:flex;gap:16px;font-size:12px;color:var(--muted);}
.lgnd{display:flex;align-items:center;gap:6px;}
.lgnd-line{width:18px;height:2px;border-radius:1px;}
.chart-wrap{position:relative;height:150px;}
/* bottom */
.bottom{display:grid;grid-template-columns:228px 1fr 1fr;gap:12px;}
.sky-card{display:flex;flex-direction:column;align-items:center;}
.sec-lbl{font-size:11px;font-weight:600;text-transform:uppercase;
  letter-spacing:.07em;color:var(--muted);margin-bottom:12px;align-self:flex-start;}
.sky-legend{display:flex;gap:10px;margin-top:10px;}
.sl-item{display:flex;align-items:center;gap:4px;font-size:10px;font-weight:600;
  text-transform:uppercase;letter-spacing:.05em;color:var(--muted);}
.sl-dot{width:7px;height:7px;border-radius:50%;display:inline-block;}
.detail-grid{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:12px;margin-top:12px;}
.detail-card{min-width:0;}
.detail-card .drow{padding:8px 0;}
.wide-val{max-width:58%;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;}
.ready-list{display:flex;flex-wrap:wrap;gap:6px;margin-top:4px;}
.ready-pill{border:1px solid var(--bdr);border-radius:4px;padding:4px 6px;font-size:10px;
  font-family:monospace;color:var(--muted);background:#090A0B;}
.ready-pill.on{color:#FFFFFF;border-color:#C9CED4;}
.controls-grid{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:12px;margin-top:12px;}
.ctrl-grid{display:flex;flex-wrap:wrap;gap:8px;}
.ctrl-btn{height:32px;background:#090A0B;color:#F4F7FA;border:1px solid var(--bdr);border-radius:4px;
  padding:0 10px;font-family:monospace;font-size:12px;cursor:pointer;}
.ctrl-btn:hover{border-color:#D6DADE;}
.ctrl-btn.warn{border-color:#73512C;color:#FFB04D;}
.ctrl-btn.danger{border-color:#8B2D2D;color:#FF6B6B;}
.ctrl-btn:disabled{opacity:.45;cursor:not-allowed;}
.control-output{margin-top:12px;border-top:1px solid var(--faint);padding-top:10px;
  font-family:monospace;font-size:12px;color:var(--muted);white-space:pre-wrap;max-height:180px;overflow:auto;}
.control-output.ok{color:#F4F7FA;}
.control-output.err{color:#FFB04D;}
/* detail rows */
.rows{display:flex;flex-direction:column;}
.drow{display:flex;justify-content:space-between;align-items:center;
  padding:9px 0;border-bottom:1px solid var(--faint);}
.drow:last-child{border-bottom:none;}
.drow-lbl{font-size:13px;color:var(--muted);}
.drow-val{font-family:monospace;font-size:13px;color:var(--text);}
/* alerts */
.alert-sep{margin-top:14px;padding-top:12px;border-top:1px solid var(--faint);}
.alert-hdr{font-size:11px;font-weight:600;text-transform:uppercase;
  letter-spacing:.07em;color:var(--muted);margin-bottom:8px;}
.alert-ok{font-size:13px;color:var(--teal);display:flex;align-items:center;gap:5px;}
.alert-item{font-size:13px;color:var(--warn);margin-bottom:3px;}
/* footer */
footer{text-align:center;padding:12px;font-size:11px;color:var(--faint);font-family:monospace;}
@media (max-width: 720px) {
  header{padding:14px 16px;gap:12px;align-items:flex-start;flex-wrap:wrap;}
  .brand{min-width:0;}
  .brand-sub{max-width:150px;white-space:normal;line-height:1.25;}
  .hdr-r{width:100%;gap:10px;align-items:flex-start;flex-wrap:wrap;justify-content:space-between;}
  .poll-ctl{gap:6px;}
  .poll-ctl select{height:28px;font-size:11px;padding:0 6px;}
  .badge{padding:6px 10px;font-size:11px;}
  main{padding:14px 12px;}
  .metrics{grid-template-columns:repeat(2,minmax(0,1fr));gap:8px;}
  .card{padding:14px 16px;}
  .cval{font-size:30px;}
  .chart-hdr{gap:10px;flex-wrap:wrap;}
  .chart-legend{gap:10px;flex-wrap:wrap;}
  .bottom{grid-template-columns:1fr;gap:10px;}
  .detail-grid{grid-template-columns:1fr;gap:10px;}
  .controls-grid{grid-template-columns:1fr;gap:10px;}
  .sky-card{align-items:center;}
  .drow{gap:12px;}
  .drow-lbl{min-width:0;}
  .drow-val{text-align:right;overflow-wrap:anywhere;}
}
@keyframes pulse{0%,100%{opacity:1}50%{opacity:.35}}
</style>
</head>
<body>

<header>
  <div class="brand">
    <div class="brand-icon">SL</div>
    <div>
      <div class="brand-name">Starlink Monitor</div>
      <div class="brand-sub mono" id="hw-sub">LOCAL TERMINAL</div>
    </div>
  </div>
  <div class="hdr-r">
    <div class="poll-ctl">
      <label for="poll-select">Poll</label>
      <select id="poll-select" aria-label="Polling interval">
        <option value="0.25">0.25 s</option>
        <option value="0.5" selected>0.5 s</option>
        <option value="1">1 s</option>
        <option value="5">5 s</option>
        <option value="10">10 s</option>
        <option value="15">15 s</option>
        <option value="30">30 s</option>
      </select>
    </div>
    <div>
      <div class="uptime-lbl">Uptime</div>
      <div class="uptime-val mono" id="uptime-val">—</div>
    </div>
    <div class="badge live" id="status-badge">
      <div class="dot live" id="status-dot"></div>
      <span id="status-txt">LIVE</span>
    </div>
  </div>
</header>

<main>
  <!-- 4 metric cards -->
  <div class="metrics">
    <div class="card">
      <div class="clbl"><div class="pip" style="background:#FFFFFF"></div>Download</div>
      <div class="cval" id="v-down">—</div>
      <div class="cunit">Mbps</div>
    </div>
    <div class="card">
      <div class="clbl"><div class="pip" style="background:#9DA4AF"></div>Upload</div>
      <div class="cval" id="v-up">—</div>
      <div class="cunit">Mbps</div>
    </div>
    <div class="card">
      <div class="clbl"><div class="pip" style="background:#FFFFFF"></div>Latency</div>
      <div class="cval" id="v-lat">—</div>
      <div class="cunit">ms</div>
    </div>
    <div class="card">
      <div class="clbl"><div class="pip" style="background:#9DA4AF"></div>Packet loss</div>
      <div class="cval" id="v-loss">—</div>
      <div class="cunit">%</div>
    </div>
  </div>

  <!-- Throughput chart -->
  <div class="card chart-card">
    <div class="chart-hdr">
      <div>
        <div class="chart-title">Throughput history</div>
        <div class="chart-sub" id="chart-sub">Live · 0.5-second samples</div>
      </div>
      <div class="chart-legend">
        <div class="lgnd"><div class="lgnd-line" style="background:#FFFFFF"></div>Download</div>
        <div class="lgnd"><div class="lgnd-line" style="background:#9DA4AF"></div>Upload</div>
      </div>
    </div>
    <div class="chart-wrap">
      <canvas id="chart" aria-label="Throughput history chart"></canvas>
    </div>
  </div>

  <!-- Bottom: sky map | signal | system -->
  <div class="bottom">
    <div class="card sky-card">
      <div class="sec-lbl">Sky view</div>
      <canvas id="sky" width="184" height="184"
        aria-label="Live Starlink obstruction map"></canvas>
      <div class="sky-legend">
        <div class="sl-item"><span class="sl-dot" style="background:#DDE5ED"></span>Clear</div>
        <div class="sl-item"><span class="sl-dot" style="background:#FF8A3D"></span>Weak</div>
        <div class="sl-item"><span class="sl-dot" style="background:#111315"></span>No data</div>
      </div>
    </div>

    <div class="card">
      <div class="sec-lbl">Signal</div>
      <div class="rows">
        <div class="drow"><span class="drow-lbl">Signal quality (SNR)</span>
          <span class="drow-val" id="r-snr" style="color:var(--teal)">—</span></div>
        <div class="drow"><span class="drow-lbl">Obstruction</span>
          <span class="drow-val" id="r-obst">—</span></div>
        <div class="drow"><span class="drow-lbl">Dish azimuth</span>
          <span class="drow-val" id="r-az">—</span></div>
        <div class="drow"><span class="drow-lbl">Dish elevation</span>
          <span class="drow-val" id="r-el">—</span></div>
        <div class="drow"><span class="drow-lbl">Time to first fix</span>
          <span class="drow-val" id="r-s2ff">—</span></div>
        <div class="drow"><span class="drow-lbl">State</span>
          <span class="drow-val" id="r-state" style="color:var(--blue)">—</span></div>
      </div>
    </div>

    <div class="card">
      <div class="sec-lbl">System</div>
      <div class="rows">
        <div class="drow"><span class="drow-lbl">Hardware</span>
          <span class="drow-val" id="r-hw">—</span></div>
        <div class="drow"><span class="drow-lbl">Software</span>
          <span class="drow-val" id="r-sw">—</span></div>
        <div class="drow"><span class="drow-lbl">Boot count</span>
          <span class="drow-val" id="r-boots">—</span></div>
      </div>
      <div class="alert-sep">
        <div class="alert-hdr">Alerts</div>
        <div id="alerts">
          <div class="alert-ok">✓ No active alerts</div>
        </div>
      </div>
    </div>
  </div>

  <div class="detail-grid">
    <div class="card detail-card">
      <div class="sec-lbl">Identity</div>
      <div class="rows">
        <div class="drow"><span class="drow-lbl">Terminal ID</span><span class="drow-val wide-val" id="r-device-id">—</span></div>
        <div class="drow"><span class="drow-lbl">Country</span><span class="drow-val" id="r-country">—</span></div>
        <div class="drow"><span class="drow-lbl">Service class</span><span class="drow-val" id="r-service">—</span></div>
        <div class="drow"><span class="drow-lbl">Mobility</span><span class="drow-val" id="r-mobility">—</span></div>
        <div class="drow"><span class="drow-lbl">Build ID</span><span class="drow-val wide-val" id="r-build">—</span></div>
      </div>
    </div>
    <div class="card detail-card">
      <div class="sec-lbl">Network</div>
      <div class="rows">
        <div class="drow"><span class="drow-lbl">Ethernet link</span><span class="drow-val" id="r-eth">—</span></div>
        <div class="drow"><span class="drow-lbl">Router</span><span class="drow-val wide-val" id="r-router">—</span></div>
        <div class="drow"><span class="drow-lbl">Downlink limit</span><span class="drow-val" id="r-dl-limit">—</span></div>
        <div class="drow"><span class="drow-lbl">Uplink limit</span><span class="drow-val" id="r-ul-limit">—</span></div>
        <div class="drow"><span class="drow-lbl">GPS satellites</span><span class="drow-val" id="r-gps">—</span></div>
      </div>
    </div>
    <div class="card detail-card">
      <div class="sec-lbl">Alignment</div>
      <div class="rows">
        <div class="drow"><span class="drow-lbl">Tilt angle</span><span class="drow-val" id="r-tilt">—</span></div>
        <div class="drow"><span class="drow-lbl">Desired azimuth</span><span class="drow-val" id="r-des-az">—</span></div>
        <div class="drow"><span class="drow-lbl">Desired elevation</span><span class="drow-val" id="r-des-el">—</span></div>
        <div class="drow"><span class="drow-lbl">Uncertainty</span><span class="drow-val" id="r-uncert">—</span></div>
        <div class="drow"><span class="drow-lbl">Actuators</span><span class="drow-val" id="r-actuators">—</span></div>
      </div>
    </div>
    <div class="card detail-card">
      <div class="sec-lbl">Readiness</div>
      <div class="rows">
        <div class="drow"><span class="drow-lbl">Software update</span><span class="drow-val" id="r-update">—</span></div>
        <div class="drow"><span class="drow-lbl">Reboot reason</span><span class="drow-val" id="r-reboot">—</span></div>
        <div class="drow"><span class="drow-lbl">Map valid</span><span class="drow-val" id="r-map-valid">—</span></div>
        <div class="drow"><span class="drow-lbl">Init stable</span><span class="drow-val" id="r-init-stable">—</span></div>
        <div class="ready-list" id="ready-list"></div>
      </div>
    </div>
  </div>

  <div class="controls-grid">
    <div class="card detail-card">
      <div class="sec-lbl">Dish controls</div>
      <div class="ctrl-grid">
        <button class="ctrl-btn warn" data-action="dish_stow" data-confirm="Stow the dish? This may interrupt service.">Stow</button>
        <button class="ctrl-btn warn" data-action="dish_unstow" data-confirm="Unstow the dish?">Unstow</button>
        <button class="ctrl-btn warn" data-action="dish_reboot" data-confirm="Reboot the dish? This will interrupt service temporarily.">Reboot</button>
        <button class="ctrl-btn danger" data-action="dish_factory_reset" data-confirm="Factory reset the dish? This can remove configuration and interrupt service." data-type-confirm="CONTROL">Factory reset</button>
        <button class="ctrl-btn" data-action="dish_clear_obstruction_map" data-confirm="Clear the obstruction map? The dish will rebuild obstruction history over time.">Clear map</button>
        <button class="ctrl-btn" data-action="dish_self_test">Self-test</button>
        <button class="ctrl-btn" data-action="dish_user_reported_issue">Issue marker</button>
      </div>
      <div class="ctrl-grid" style="margin-top:8px">
        <button class="ctrl-btn" data-action="dish_get_device_info">Device info</button>
        <button class="ctrl-btn" data-action="dish_get_context">Context</button>
        <button class="ctrl-btn" data-action="dish_get_config">Config</button>
        <button class="ctrl-btn" data-action="dish_get_data">Data</button>
        <button class="ctrl-btn" data-action="dish_get_diagnostics">Diagnostics</button>
        <button class="ctrl-btn" data-action="dish_get_location">Location</button>
        <button class="ctrl-btn" data-action="dish_get_log">Log</button>
        <button class="ctrl-btn" data-action="dish_get_network_interfaces">Interfaces</button>
        <button class="ctrl-btn" data-action="dish_get_persistent_stats">Persistent stats</button>
        <button class="ctrl-btn" data-action="dish_get_connections">Connections</button>
        <button class="ctrl-btn" data-action="dish_get_emc">EMC</button>
        <button class="ctrl-btn" data-action="dish_get_rssi_scan_result">RSSI scan</button>
        <button class="ctrl-btn" data-action="dish_get_radio_stats">Radio stats</button>
        <button class="ctrl-btn" data-action="dish_get_goroutine_stack_traces">Stacks</button>
        <button class="ctrl-btn" data-action="dish_time">Time</button>
      </div>
      <div class="ctrl-grid" style="margin-top:8px">
        <button class="ctrl-btn danger" data-action="dish_inhibit_gps_on" data-confirm="Inhibit dish GPS? This can affect positioning and service behavior." data-type-confirm="CONTROL">GPS inhibit on</button>
        <button class="ctrl-btn warn" data-action="dish_inhibit_gps_off" data-confirm="Turn dish GPS inhibit off?">GPS inhibit off</button>
        <button class="ctrl-btn danger" data-action="dish_inhibit_rf_on" data-confirm="Inhibit dish RF? This can interrupt Starlink service." data-type-confirm="CONTROL">RF inhibit on</button>
        <button class="ctrl-btn warn" data-action="dish_inhibit_rf_off" data-confirm="Turn dish RF inhibit off?">RF inhibit off</button>
        <button class="ctrl-btn danger" data-action="dish_max_power_test_on" data-confirm="Enable max power test mode? This is a hardware test mode." data-type-confirm="CONTROL">Max power on</button>
        <button class="ctrl-btn warn" data-action="dish_max_power_test_off" data-confirm="Disable max power test mode?">Max power off</button>
        <button class="ctrl-btn danger" data-action="dish_reset_button_press" data-confirm="Simulate a dish reset button press?" data-type-confirm="CONTROL">Reset button</button>
      </div>
      <div class="control-output" id="dish-control-output">No dish action run yet.</div>
    </div>

    <div class="card detail-card">
      <div class="sec-lbl">Router controls</div>
      <div class="ctrl-grid">
        <button class="ctrl-btn warn" data-action="router_reboot" data-confirm="Reboot the router? Wi-Fi will disconnect temporarily.">Reboot</button>
        <button class="ctrl-btn danger" data-action="router_factory_reset" data-confirm="Factory reset the router? This can remove Wi-Fi configuration." data-type-confirm="CONTROL">Factory reset</button>
        <button class="ctrl-btn" data-action="speedtest_start">Start speed test</button>
        <button class="ctrl-btn" data-action="speedtest_status">Speed status</button>
        <button class="ctrl-btn" data-action="wifi_clients">Clients</button>
        <button class="ctrl-btn" data-action="wifi_config">Config</button>
        <button class="ctrl-btn" data-action="wifi_status">Status</button>
        <button class="ctrl-btn" data-action="wifi_ping_metrics">Ping metrics</button>
        <button class="ctrl-btn" data-action="wifi_firewall">Firewall</button>
        <button class="ctrl-btn" data-action="wifi_guest_info">Guest info</button>
        <button class="ctrl-btn" data-action="wifi_backhaul_stats">Backhaul</button>
        <button class="ctrl-btn" data-action="wifi_diagnostics">Diagnostics</button>
        <button class="ctrl-btn" data-action="wifi_self_test">Self-test</button>
        <button class="ctrl-btn" data-action="wifi_run_self_test">Extended self-test</button>
      </div>
      <div class="ctrl-grid" style="margin-top:8px">
        <button class="ctrl-btn danger" data-action="wifi_calibration_mode" data-confirm="Enter Wi-Fi calibration mode? This is a factory/test behavior." data-type-confirm="CONTROL">Calibration mode</button>
        <button class="ctrl-btn danger" data-action="wifi_toggle_poe_on" data-confirm="Toggle PoE negotiation on?" data-type-confirm="CONTROL">PoE on</button>
        <button class="ctrl-btn danger" data-action="wifi_toggle_poe_off" data-confirm="Toggle PoE negotiation off?" data-type-confirm="CONTROL">PoE off</button>
        <button class="ctrl-btn danger" data-action="wifi_umbilical_on" data-confirm="Enable umbilical mode?" data-type-confirm="CONTROL">Umbilical on</button>
        <button class="ctrl-btn danger" data-action="wifi_umbilical_off" data-confirm="Disable umbilical mode?" data-type-confirm="CONTROL">Umbilical off</button>
        <button class="ctrl-btn danger" data-action="wifi_reset_eth_phy" data-confirm="Reset the router Ethernet PHY? Wired connectivity may drop briefly." data-type-confirm="CONTROL">Reset ETH PHY</button>
        <button class="ctrl-btn danger" data-action="wifi_flush_hardware_nat" data-confirm="Flush hardware NAT? Active connections may be interrupted." data-type-confirm="CONTROL">Flush NAT</button>
        <button class="ctrl-btn danger" data-action="wifi_run_debug_netsys" data-confirm="Run router network debug? This can take a moment." data-type-confirm="CONTROL">Net debug</button>
      </div>
      <div class="control-output" id="router-control-output">No router action run yet.</div>
    </div>
  </div>
</main>

<footer class="mono">
  Polling 192.168.100.1:9200 · <span id="last-ts">—</span>
</footer>

__CHARTJS__
<script>
(function () {
  "use strict";

  // ── Chart.js setup ──────────────────────────────────────────────────────────
  var BLU = "#FFFFFF", TEL = "#9DA4AF";
  var hLabels = [], hDown = [], hUp = [], MAX_PTS = 240;
  var chart;
  var pollSec = 0.5, statusTimer = null;

  function initChart() {
    chart = new Chart(document.getElementById("chart"), {
      type: "line",
      data: {
        labels: hLabels,
        datasets: [
          { data: hDown, borderColor: BLU, backgroundColor: "rgba(255,255,255,.08)",
            borderWidth: 2, fill: true, tension: 0.4, pointRadius: 0, pointHoverRadius: 4 },
          { data: hUp,   borderColor: TEL, backgroundColor: "rgba(157,164,175,.08)",
            borderWidth: 1.5, fill: true, tension: 0.4, pointRadius: 0, pointHoverRadius: 4 }
        ]
      },
      options: {
        responsive: true, maintainAspectRatio: false, animation: false,
        plugins: {
          legend: { display: false },
          tooltip: {
            backgroundColor: "#101113", borderColor: "#2B2E33", borderWidth: 1,
            titleColor: "#8C939D", bodyColor: "#F4F7FA", padding: 10,
            callbacks: {
              label: function(i) {
                return i.datasetIndex === 0
                  ? "↓ " + Math.round(i.raw) + " Mbps"
                  : "↑ " + Math.round(i.raw) + " Mbps";
              }
            }
          }
        },
        scales: {
          x: { grid: { display: false }, border: { display: false },
               ticks: { color: "#8C939D", font: { family: "monospace", size: 10 },
                        maxTicksLimit: 8, maxRotation: 0 } },
          y: { grid: { color: "#25282D" }, border: { display: false }, min: 0,
               ticks: { color: "#8C939D", font: { family: "monospace", size: 10 } } }
        },
        interaction: { mode: "index", intersect: false }
      }
    });
  }

  function pushPt(d) {
    var t = new Date(d.ts * 1000)
      .toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", second: "2-digit" });
    hLabels.push(t); hDown.push(d.down); hUp.push(d.up);
    if (hLabels.length > MAX_PTS) { hLabels.shift(); hDown.shift(); hUp.shift(); }
    if (chart) chart.update("none");
  }

  // ── Sky-map canvas ──────────────────────────────────────────────────────────
  var skyCanvas = document.getElementById("sky");
  var ctx       = skyCanvas.getContext("2d");
  var S = skyCanvas.width, cx = S / 2, cy = S / 2, R = S * 0.44;
  var latestMap = null, dishAz = 0, dishEl = 0;

  function drawSkyFrame() {
    ctx.clearRect(0, 0, S, S);
    ctx.beginPath(); ctx.arc(cx, cy, R, 0, Math.PI * 2);
    ctx.fillStyle = "#08090A"; ctx.fill();

    [.33, .66, 1].forEach(function(r) {
      ctx.beginPath(); ctx.arc(cx, cy, R * r, 0, Math.PI * 2);
      ctx.strokeStyle = "rgba(255,255,255," + (r === 1 ? ".1" : ".06") + ")";
      ctx.lineWidth = r === 1 ? 1 : .6; ctx.stroke();
    });
    for (var a = 0; a < 360; a += 45) {
      var rad = (a - 90) * Math.PI / 180;
      ctx.beginPath(); ctx.moveTo(cx, cy);
      ctx.lineTo(cx + Math.cos(rad) * R, cy + Math.sin(rad) * R);
      ctx.strokeStyle = "rgba(255,255,255,.04)"; ctx.lineWidth = .6; ctx.stroke();
    }
  }

  function mapColor(v) {
    if (v < 0) return [17, 19, 21, 255];
    if (v < 0.12) return [255, 138, 61, 230];
    if (v < 0.35) return [214, 171, 87, 220];
    var t = Math.min(1, (v - 0.35) / 0.65);
    return [
      Math.round(140 + 82 * t),
      Math.round(149 + 80 * t),
      Math.round(158 + 82 * t),
      220
    ];
  }

  function drawObstructionMap(map) {
    drawSkyFrame();
    if (!map || !map.live || !map.rows || !map.cols || !map.snr || !map.snr.length) {
      ctx.fillStyle = "#5A6A9A";
      ctx.font = "11px monospace";
      ctx.textAlign = "center";
      ctx.textBaseline = "middle";
      ctx.fillText("waiting for map", cx, cy);
      drawSkyLabels();
      return;
    }

    var cols = map.cols, rows = map.rows;
    var img = ctx.createImageData(cols, rows);
    for (var y = 0; y < rows; y++) {
      for (var x = 0; x < cols; x++) {
        var dx = (x + .5) / cols * 2 - 1;
        var dy = (y + .5) / rows * 2 - 1;
        var i = y * cols + x, p = i * 4;
        if (dx * dx + dy * dy > 1) {
          img.data[p + 3] = 0;
          continue;
        }
        var c = mapColor(map.snr[i]);
        img.data[p] = c[0]; img.data[p + 1] = c[1];
        img.data[p + 2] = c[2]; img.data[p + 3] = c[3];
      }
    }

    var tmp = document.createElement("canvas");
    tmp.width = cols; tmp.height = rows;
    tmp.getContext("2d").putImageData(img, 0, 0);
    ctx.save();
    ctx.beginPath(); ctx.arc(cx, cy, R, 0, Math.PI * 2); ctx.clip();
    ctx.imageSmoothingEnabled = false;
    ctx.drawImage(tmp, cx - R, cy - R, R * 2, R * 2);
    ctx.restore();
    drawSkyLabels();
    drawDishPointing();
  }

  function drawSkyLabels() {
    ctx.beginPath(); ctx.arc(cx, cy, R, 0, Math.PI * 2);
    ctx.strokeStyle = "rgba(255,255,255,.12)"; ctx.lineWidth = 1; ctx.stroke();

    ctx.font = "bold 9px monospace"; ctx.textAlign = "center"; ctx.textBaseline = "middle";
    [["N",0],["E",90],["S",180],["W",270]].forEach(function(item) {
      var r3 = (item[1] - 90) * Math.PI / 180;
      ctx.fillStyle = item[0] === "N" ? "#FFFFFF" : "#8C939D";
      ctx.fillText(item[0], cx + Math.cos(r3) * (R + 11), cy + Math.sin(r3) * (R + 11));
    });
  }

  function drawDishPointing() {
    var az = Number(dishAz), el = Number(dishEl);
    if (!isFinite(az) || !isFinite(el)) return;
    var radial = Math.max(0, Math.min(1, (90 - el) / 90));
    var sr = (az - 90) * Math.PI / 180;
    var sx = cx + Math.cos(sr) * R * radial, sy = cy + Math.sin(sr) * R * radial;
    ctx.beginPath(); ctx.arc(sx, sy, 9, 0, Math.PI * 2);
    ctx.strokeStyle = "rgba(255,255,255,.5)"; ctx.lineWidth = 1.5; ctx.stroke();
    ctx.beginPath(); ctx.arc(sx, sy, 3.5, 0, Math.PI * 2);
    ctx.fillStyle = "#FFFFFF"; ctx.fill();
    ctx.beginPath(); ctx.arc(cx, cy, 2, 0, Math.PI * 2);
    ctx.fillStyle = "rgba(255,255,255,.2)"; ctx.fill();
  }

  drawObstructionMap(null);

  // ── Uptime ticker ───────────────────────────────────────────────────────────
  var uptimeSec = 0;

  function fmtUptime(s) {
    var d  = Math.floor(s / 86400);
    var h  = String(Math.floor((s % 86400) / 3600)).padStart(2, "0");
    var m  = String(Math.floor((s % 3600) / 60)).padStart(2, "0");
    var sc = String(s % 60).padStart(2, "0");
    return d + "d " + h + ":" + m + ":" + sc;
  }

  setInterval(function() {
    if (uptimeSec > 0) {
      uptimeSec++;
      document.getElementById("uptime-val").textContent = fmtUptime(uptimeSec);
    }
  }, 1000);

  // ── DOM update ──────────────────────────────────────────────────────────────
  function setText(id, txt) {
    var el = document.getElementById(id);
    if (el) el.textContent = txt;
  }

  function val(v, suffix) {
    if (v === null || v === undefined || v === "") return "-";
    if (typeof v === "number" && !isFinite(v)) return "-";
    return String(v) + (suffix || "");
  }

  function boolVal(v) {
    return v ? "YES" : "NO";
  }

  function controlOutputFor(action) {
    return action.indexOf("wifi_") === 0 || action.indexOf("router_") === 0 || action.indexOf("speedtest_") === 0
      ? "router-control-output"
      : "dish-control-output";
  }

  function formatControlResult(r) {
    var lines = [(r.label || r.action || "Action") + ": " + (r.message || (r.ok ? "OK" : "Failed"))];
    if (r.clients) {
      if (!r.clients.length) lines.push("No clients returned.");
      r.clients.slice(0, 12).forEach(function(c) {
        lines.push("- " + (c.name || "Unnamed") + "  " + (c.ip || "") + "  " + (c.mac || ""));
      });
      if (r.clients.length > 12) lines.push("... " + (r.clients.length - 12) + " more");
    }
    if (r.config) {
      Object.keys(r.config).forEach(function(k) { lines.push(k + ": " + r.config[k]); });
    }
    if (r.report) lines.push(String(r.report).slice(0, 1000));
    if (r.error) lines.push("Error: " + r.error);
    return lines.join("\\n");
  }

  function runControl(action, btn) {
    var out = document.getElementById(controlOutputFor(action));
    if (out) {
      out.className = "control-output";
      out.textContent = "Running " + action + "...";
    }
    if (btn) btn.disabled = true;
    fetch("/api/control", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ action: action })
    }).then(function(r) {
      return r.json().then(function(body) { body.ok = r.ok && body.ok; return body; });
    }).then(function(body) {
      if (out) {
        out.className = "control-output " + (body.ok ? "ok" : "err");
        out.textContent = formatControlResult(body);
      }
      fetchStatus();
      if (action === "dish_clear_obstruction_map") fetchObstructionMap();
    }).catch(function(e) {
      if (out) {
        out.className = "control-output err";
        out.textContent = "Control request failed.";
      }
    }).finally(function() {
      if (btn) {
        btn.disabled = false;
        applyHardwareControlState();
      }
    });
  }

  function applyHardwareControlState(d) {
    var hasActuators = d ? d.has_actuators === "YES" : document.body.getAttribute("data-has-actuators") === "YES";
    document.querySelectorAll('[data-action="dish_stow"],[data-action="dish_unstow"]').forEach(function(btn) {
      btn.disabled = !hasActuators;
      btn.title = hasActuators ? "" : "This dish reports no adjustment motors; stow/unstow is disabled.";
    });
  }

  function update(d) {
    setText("v-down",  d.down);
    setText("v-up",    d.up);
    setText("v-lat",   d.latency);
    setText("v-loss",  d.loss.toFixed(2));

    uptimeSec = d.uptime || 0;
    setText("uptime-val", fmtUptime(d.uptime || 0));
    setText("hw-sub", (d.hardware || "GEN3") + " / " + (d.class_of_service || "SERVICE") + " / " + (d.mobility_class || "MOBILITY"));

    // Status badge
    var live  = d.live;
    var state = d.state || "";
    var badge = document.getElementById("status-badge");
    var dot   = document.getElementById("status-dot");
    var txt   = document.getElementById("status-txt");
    var cls   = live ? (state === "CONNECTED" || state === "" ? "live" : "searching") : "demo";
    badge.className = "badge " + cls;
    dot.className   = "dot "   + cls;
    txt.textContent = live ? state || "LIVE" : "DEMO";

    // Signal rows
    setText("r-snr",   d.snr ? d.snr + " dB" : "not reported");
    setText("r-obst",  d.obstruction_pct + "%" + (d.currently_obstructed ? " ⚠" : ""));
    setText("r-az",    d.azimuth + "°");
    setText("r-el",    d.elevation + "°");
    setText("r-s2ff",  d.s2ff + " s");
    setText("r-state", state);

    // System rows
    setText("r-hw",    d.hardware  || "—");
    setText("r-sw",    d.software  || "—");
    setText("r-boots", String(d.boot_count || 0));

    // Deep details
    setText("r-device-id", d.device_id || "-");
    setText("r-country", d.country_code || "-");
    setText("r-service", d.class_of_service || "-");
    setText("r-mobility", d.mobility_class || "-");
    setText("r-build", d.build_id || "-");
    setText("r-eth", d.eth_speed_mbps ? d.eth_speed_mbps + " Mbps" : "-");
    setText("r-router", d.connected_routers && d.connected_routers.length ? d.connected_routers.join(", ") : "-");
    setText("r-dl-limit", d.downlink_restriction || "-");
    setText("r-ul-limit", d.uplink_restriction || "-");
    setText("r-gps", d.gps ? (boolVal(d.gps.valid) + " / " + val(d.gps.satellites) + " sats") : "-");
    setText("r-tilt", d.alignment ? val(d.alignment.tilt_angle_deg, "°") : "-");
    setText("r-des-az", d.alignment ? val(d.alignment.desired_azimuth_deg, "°") : "-");
    setText("r-des-el", d.alignment ? val(d.alignment.desired_elevation_deg, "°") : "-");
    setText("r-uncert", d.alignment ? val(d.alignment.attitude_uncertainty_deg, "°") : "-");
    setText("r-actuators", d.has_actuators || "-");
    document.body.setAttribute("data-has-actuators", d.has_actuators || "");
    applyHardwareControlState(d);
    setText("r-update", (d.software_update_state || "-") + " / " + Math.round((d.software_update_progress || 0) * 100) + "%");
    setText("r-reboot", d.reboot_reason || "-");
    setText("r-map-valid", val(d.obstruction_valid_s, " s") + " / " + val(d.obstruction_patches_valid, " patches"));
    setText("r-init-stable", d.initialization ? val(d.initialization.stable_connection, " s") : "-");

    var rl = document.getElementById("ready-list");
    if (rl) {
      var rs = d.ready_states || {};
      rl.innerHTML = ["cady", "scp", "l1l2", "xphy", "aap", "rf"].map(function(k) {
        return '<span class="ready-pill ' + (rs[k] ? "on" : "") + '">' + k.toUpperCase() + '</span>';
      }).join("");
    }

    // Alerts
    var ac = document.getElementById("alerts");
    if (!d.alerts || d.alerts.length === 0) {
      ac.innerHTML = '<div class="alert-ok">✓ No active alerts</div>';
    } else {
      ac.innerHTML = d.alerts.map(function(a) {
        return '<div class="alert-item">⚠ ' + a + '</div>';
      }).join("");
    }

    // Sky map marker
    dishAz = d.azimuth || 0;
    dishEl = d.elevation || 0;
    if (latestMap) drawObstructionMap(latestMap);

    // Footer timestamp
    setText("last-ts", "updated " + new Date().toLocaleTimeString());
  }

  // ── Data fetching ───────────────────────────────────────────────────────────
  function fetchStatus() {
    fetch("/api/status")
      .then(function(r) { return r.ok ? r.json() : null; })
      .then(function(d) { if (d) { update(d); pushPt(d); } })
      .catch(function() {});
  }

  function setPollDisplay(sec) {
    pollSec = sec;
    var sel = document.getElementById("poll-select");
    if (sel) sel.value = String(sec);
    setText("chart-sub", "Live · " + sec + "-second samples");
    if (statusTimer) clearInterval(statusTimer);
    statusTimer = setInterval(fetchStatus, Math.max(250, sec * 1000));
  }

  function applyPollInterval(sec) {
    setPollDisplay(sec);
    fetch("/api/poll", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ poll_sec: sec })
    }).then(function(r) {
      return r.ok ? r.json() : Promise.reject();
    }).then(function(cfg) {
      if (cfg && cfg.poll_sec) setPollDisplay(Number(cfg.poll_sec));
    }).catch(function() {});
  }

  function loadPollInterval() {
    fetch("/api/poll")
      .then(function(r) { return r.ok ? r.json() : null; })
      .then(function(cfg) {
        if (cfg && cfg.poll_sec) setPollDisplay(Number(cfg.poll_sec));
      })
      .catch(function() { setPollDisplay(pollSec); });
  }

  function fetchHistory() {
    fetch("/api/history")
      .then(function(r) { return r.ok ? r.json() : null; })
      .then(function(pts) {
        if (!pts) return;
        hLabels.length = 0; hDown.length = 0; hUp.length = 0;
        pts.forEach(pushPt);
      })
      .catch(function() {});
  }

  function fetchObstructionMap() {
    fetch("/api/obstruction-map")
      .then(function(r) { return r.ok ? r.json() : null; })
      .then(function(m) {
        if (!m) return;
        latestMap = m;
        drawObstructionMap(latestMap);
      })
      .catch(function() {});
  }

  // Boot
  if (typeof Chart !== "undefined") { initChart(); } else {
    document.querySelector(".chart-wrap").innerHTML =
      '<p style="color:var(--muted);padding:50px;text-align:center;font-family:monospace">' +
      'Chart.js not loaded. Restart the script with an internet connection to cache it.</p>';
  }
  fetchHistory();
  fetchStatus();
  fetchObstructionMap();
  setPollDisplay(pollSec);
  loadPollInterval();
  var pollSelect = document.getElementById("poll-select");
  if (pollSelect) {
    pollSelect.addEventListener("change", function() {
      applyPollInterval(Number(pollSelect.value));
    });
  }
  document.querySelectorAll(".ctrl-btn[data-action]").forEach(function(btn) {
    btn.addEventListener("click", function() {
      var msg = btn.getAttribute("data-confirm");
      if (msg && !confirm(msg)) return;
      var typeConfirm = btn.getAttribute("data-type-confirm");
      if (typeConfirm) {
        var typed = prompt((msg || "Confirm this control.") + "\\n\\nType " + typeConfirm + " to continue.");
        if (typed !== typeConfirm) return;
      }
      runControl(btn.getAttribute("data-action"), btn);
    });
  });
  applyHardwareControlState();
  setInterval(fetchHistory, 30000);
  setInterval(fetchObstructionMap, 30000);
})();
</script>
</body>
</html>'''

# ── Main ───────────────────────────────────────────────────────────────────────
def main():
    global _collector

    print("=" * 62)
    print("  Starlink Local Dashboard")
    print("=" * 62)
    print(f"  Dish target  : {STARLINK_HOST}")
    print(f"  Router target: {STARLINK_ROUTER_HOST}")

    _collector = DataCollector()
    _collector.start()

    # Allow first poll to settle
    time.sleep(1.5)

    # Try to find a free port
    server = None
    for p in [PORT, PORT + 1, PORT + 2]:
        try:
            server = HTTPServer(("0.0.0.0", p), _Handler)
            used_port = p
            break
        except OSError:
            continue

    if server is None:
        print(f"  ERROR: could not bind to port {PORT} (or nearby ports).")
        sys.exit(1)

    # Print accessible URLs
    try:
        local_ip = socket.gethostbyname(socket.gethostname())
    except Exception:
        local_ip = "127.0.0.1"

    url = f"http://localhost:{used_port}"
    print(f"  Dashboard   : {url}")
    if local_ip != "127.0.0.1":
        print(f"  Network     : http://{local_ip}:{used_port}")
    print(f"  Mode        : {'LIVE (dish found)' if _collector._live else 'DEMO (dish not reachable yet)'}")
    print("-" * 62)
    print("  Press Ctrl+C to stop.")
    print("=" * 62)

    try:
        webbrowser.open(url)
    except Exception:
        pass

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n  Stopping…")
        server.shutdown()


if __name__ == "__main__":
    main()
