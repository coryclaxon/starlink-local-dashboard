# Starlink Local Dashboard

A single-file local dashboard for monitoring a Starlink terminal over the dish's local gRPC API.

The dashboard runs locally, talks to `192.168.100.1:9200`, and serves a browser UI at `http://localhost:8889`.

## Features

- Live dish status over local gRPC
- Configurable status polling: `0.25`, `0.5`, `1`, `5`, `10`, `15`, or `30` seconds
- Throughput, latency, packet loss, uptime, and system status
- Real Starlink obstruction-map rendering from `dish_get_obstruction_map`
- Terminal details including hardware, software, boot count, service class, mobility class, GPS status, Ethernet link speed, router ID, alignment, update state, and ready-state flags
- Local-only browser dashboard with no cloud service required
- Demo fallback mode when the dish is unreachable

## Requirements

- Python 3.10+
- Network access to the Starlink local dish endpoint
- The Starlink local route `192.168.100.1`

The script can auto-install `grpcio` and `protobuf` if they are missing. You can also install dependencies explicitly:

```powershell
python -m pip install -r requirements.txt
```

## Usage

```powershell
python starlink_dashboard.py
```

Then open:

```text
http://localhost:8889
```

## Configuration

Environment variables:

- `STARLINK_HOST`: dish address, default `192.168.100.1:9200`
- `DASHBOARD_PORT`: local web port, default `8889`

Example:

```powershell
$env:STARLINK_HOST = "192.168.100.1:9200"
$env:DASHBOARD_PORT = "8889"
python starlink_dashboard.py
```

## Notes

- The status call is lightweight enough for sub-second polling, but the dish may not update every metric that quickly.
- The obstruction map is intentionally polled more slowly because it returns a much larger grid.
- This project uses Starlink's local device API and may need updates if Starlink firmware changes field numbers or response formats.
