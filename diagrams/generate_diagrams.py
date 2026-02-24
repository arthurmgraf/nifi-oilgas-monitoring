"""Generate professional Excalidraw diagrams for NiFi Oil & Gas Platform."""
import json
import re
import os
import random
import hashlib

DIAGRAMS_DIR = os.path.dirname(os.path.abspath(__file__))
KB_DIR = os.path.join(DIAGRAMS_DIR, "..", ".claude", "kb", "diagram-generation", "logos")

# Global tracker for files used in the current diagram
_current_files = {}


def load_logos():
    """Load all logos from KB markdown files. Returns {key: {"hash": str, "dataURL": str}}."""
    logos = {}
    for fname in ["general.md", "custom.md"]:
        fpath = os.path.join(KB_DIR, fname)
        if not os.path.exists(fpath):
            continue
        with open(fpath, "r", encoding="utf-8") as f:
            content = f.read()
        file_entries = re.findall(
            r'"(\w+_logo)":\s*\{[^}]*"dataURL":\s*"(data:image/svg\+xml;base64,[^"]+)"',
            content,
            re.DOTALL,
        )
        for file_id, data_url in file_entries:
            file_hash = hashlib.sha256(data_url.encode()).hexdigest()[:40]
            logos[file_id] = {"hash": file_hash, "dataURL": data_url}
    return logos


def uid():
    """Generate a random Excalidraw-style ID."""
    return "".join(random.choices("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789", k=20))


def make_text(x, y, text, font_size=14, color="#000000", align="left", width=240, group=None):
    el = {
        "type": "text", "id": uid(), "x": x, "y": y, "width": width,
        "height": font_size * 1.2 * max(1, text.count("\n") + 1),
        "text": text, "fontSize": font_size, "fontFamily": 3,
        "textAlign": align, "verticalAlign": "top", "baseline": font_size,
        "strokeColor": color, "backgroundColor": "transparent",
        "fillStyle": "solid", "strokeWidth": 1, "roughness": 0, "opacity": 100,
        "groupIds": [group] if group else [], "roundness": None,
        "seed": random.randint(1, 999999999), "version": 1, "versionNonce": random.randint(1, 999999999),
        "isDeleted": False, "boundElements": None, "updated": 1, "link": None, "locked": False,
    }
    return el


def make_rect(x, y, w, h, stroke="#000000", fill="#ffffff", sw=1.5, group=None, dash=False):
    el = {
        "type": "rectangle", "id": uid(), "x": x, "y": y, "width": w, "height": h,
        "strokeColor": stroke, "backgroundColor": fill,
        "fillStyle": "solid", "strokeWidth": sw, "roughness": 0, "opacity": 100,
        "groupIds": [group] if group else [], "roundness": {"type": 3, "value": 8},
        "seed": random.randint(1, 999999999), "version": 1, "versionNonce": random.randint(1, 999999999),
        "isDeleted": False, "boundElements": None, "updated": 1, "link": None, "locked": False,
        "strokeStyle": "dashed" if dash else "solid",
    }
    return el


def make_image(x, y, logo_entry, w=48, h=48, group=None):
    """Create image element using hash-based fileId referencing the files object."""
    global _current_files
    file_hash = logo_entry["hash"]
    _current_files[file_hash] = {
        "mimeType": "image/svg+xml",
        "id": file_hash,
        "dataURL": logo_entry["dataURL"],
        "created": 1740000000000,
        "lastRetrieved": 1740000000000,
    }
    el = {
        "type": "image", "id": uid(), "x": x, "y": y, "width": w, "height": h,
        "strokeColor": "transparent", "backgroundColor": "transparent",
        "fillStyle": "solid", "strokeWidth": 1, "roughness": 0, "opacity": 100,
        "groupIds": [group] if group else [], "roundness": None,
        "seed": random.randint(1, 999999999), "version": 1, "versionNonce": random.randint(1, 999999999),
        "isDeleted": False, "boundElements": None, "updated": 1, "link": None, "locked": False,
        "status": "saved", "fileId": file_hash, "scale": [1, 1],
    }
    return el


def make_arrow(x1, y1, x2, y2, color="#000000", label=None):
    aid = uid()
    points = [[0, 0], [x2 - x1, y2 - y1]]
    el = {
        "type": "arrow", "id": aid, "x": x1, "y": y1, "width": abs(x2 - x1), "height": abs(y2 - y1),
        "strokeColor": color, "backgroundColor": "transparent",
        "fillStyle": "solid", "strokeWidth": 2, "roughness": 0, "opacity": 100,
        "groupIds": [], "roundness": {"type": 2},
        "seed": random.randint(1, 999999999), "version": 1, "versionNonce": random.randint(1, 999999999),
        "isDeleted": False, "boundElements": None, "updated": 1, "link": None, "locked": False,
        "points": points, "lastCommittedPoint": None, "startBinding": None, "endBinding": None,
        "startArrowhead": None, "endArrowhead": "arrow",
    }
    els = [el]
    if label:
        mx, my = (x1 + x2) / 2, (y1 + y2) / 2 - 20
        els.append(make_text(mx - 40, my, label, font_size=11, color="#4a5568", width=120, align="center"))
    return els


def make_line(x1, y1, x2, y2, color="#d1d5db"):
    return {
        "type": "line", "id": uid(), "x": x1, "y": y1, "width": abs(x2 - x1), "height": abs(y2 - y1),
        "strokeColor": color, "backgroundColor": "transparent",
        "fillStyle": "solid", "strokeWidth": 1, "roughness": 0, "opacity": 100,
        "groupIds": [], "roundness": {"type": 2},
        "seed": random.randint(1, 999999999), "version": 1, "versionNonce": random.randint(1, 999999999),
        "isDeleted": False, "boundElements": None, "updated": 1, "link": None, "locked": False,
        "points": [[0, 0], [x2 - x1, y2 - y1]], "lastCommittedPoint": None,
        "startBinding": None, "endBinding": None, "startArrowhead": None, "endArrowhead": None,
    }


def save_excalidraw(filename, elements):
    global _current_files
    data = {
        "type": "excalidraw", "version": 2, "source": "https://excalidraw.com",
        "elements": elements, "appState": {"gridSize": None, "viewBackgroundColor": "#ffffff"},
        "files": dict(_current_files),
    }
    path = os.path.join(DIAGRAMS_DIR, filename)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)
    n_files = len(_current_files)
    _current_files = {}
    print(f"  Created: {filename} ({len(elements)} elements, {n_files} images)")


# ─── SERVICE DATA ────────────────────────────────────────────────
SERVICES = [
    {"name": "Eclipse Mosquitto", "ver": "v2.0.20", "logo": "eclipsemosquitto_logo",
     "port": ":1883 (MQTT+Auth) ✓", "features": ["TLS-ready encryption", "Password credentials", "Persistent sessions"]},
    {"name": "Data Generator", "ver": "Python 3.11+", "logo": "python_logo",
     "port": "Internal service", "features": ["Faker synthetic data", "250 sensors, 5 platforms", "Auto-inject to MQTT"]},
    {"name": "Apache NiFi", "ver": "v2.8.0", "logo": "apachenifi_logo",
     "port": ":9443 (HTTPS+Auth) ✓", "features": ["10 Process Groups", "Flow Registry integrated", "Single-User Auth"]},
    {"name": "NiFi Registry", "ver": "v2.0.0", "logo": "apachenifi_logo",
     "port": ":18443 (HTTPS)", "features": ["Git-backed storage", "Flow version control", "CI/CD integration"]},
    {"name": "Apache Kafka", "ver": "v3.9.0 (KRaft)", "logo": "apachekafka_logo",
     "port": ":9092 (SASL) ✓", "features": ["SCRAM-SHA-512 auth", "7 topics configured", "Exactly-once semantics"]},
    {"name": "Schema Registry", "ver": "v7.9.0", "logo": "apachekafka_logo",
     "port": ":8081 (HTTP)", "features": ["Avro schema mgmt", "Schema evolution", "Compatibility checks"]},
    {"name": "TimescaleDB", "ver": "v2.17.2-pg17", "logo": "timescale_logo",
     "port": ":5432 (PostgreSQL)", "features": ["Hypertables", "Compression policies", "Continuous aggregates"]},
    {"name": "PostgreSQL", "ver": "v16", "logo": "postgresql_logo",
     "port": ":5433 (PostgreSQL)", "features": ["Equipment metadata", "Alert history", "Reference data"]},
    {"name": "MinIO", "ver": "2025-02-18", "logo": "minio_logo",
     "port": ":9000 (S3 API)", "features": ["Flow backups", "Object versioning", "S3-compatible API"]},
    {"name": "Prometheus", "ver": "v2.54.1", "logo": "prometheus_logo",
     "port": ":9090 (HTTP)", "features": ["7 alert rules", "120s scrape interval", "Multi-target scrape"]},
    {"name": "Grafana", "ver": "v11.4.0", "logo": "grafana_logo",
     "port": ":3000 (HTTP)", "features": ["4 dashboards", "35 panels total", "Real-time refresh"]},
    {"name": "Alertmanager", "ver": "v0.27.0", "logo": "prometheus_logo",
     "port": ":9093 (HTTP)", "features": ["Email notifications", "Alert grouping", "Silence & inhibit"]},
    {"name": "Kafka Exporter", "ver": "v1.8.0", "logo": "apachekafka_logo",
     "port": ":9308 (HTTP)", "features": ["Consumer lag metrics", "Topic offset tracking", "Prometheus format"]},
    {"name": "Python Scripts", "ver": "3.11+", "logo": "python_logo",
     "port": "CLI tools", "features": ["create-flow.py", "create-processors.py", "CSV data generators"]},
    {"name": "Docker Compose", "ver": "Latest", "logo": "docker_logo",
     "port": "Container runtime", "features": ["16 containers", "docker-compose.yml", "Health checks"]},
    {"name": "Security Layer", "ver": "Grade: B+", "logo": None,
     "port": "Platform-wide", "features": ["30 vulns remediated", "HTTPS + SASL + Auth", "4 exposed ports only"]},
]


def gen_architecture_overview(logos):
    """Diagram 1: Architecture Overview - 16 services in 4x4 grid."""
    elements = []

    # Title
    elements.append(make_text(80, 30, "NiFi Oil & Gas Upstream Monitoring Platform", font_size=30, color="#000000", width=1500))
    elements.append(make_text(80, 75, "Production Architecture Overview  |  16 Services  |  Server: 15.235.61.251", font_size=16, color="#4a5568", width=1500))
    elements.append(make_text(80, 105, "Security Grade: B+  |  30 Vulnerabilities Remediated  |  HTTPS + SASL + Auth", font_size=13, color="#6b7280", width=1500))

    # Row labels
    row_labels = ["DATA INGESTION", "EVENT STREAMING & STORAGE", "OBSERVABILITY & STORAGE", "INFRASTRUCTURE"]
    BOX_W, BOX_H = 280, 290
    GAP_X, GAP_Y = 40, 50
    START_X, START_Y = 80, 180

    for row in range(4):
        ry = START_Y + row * (BOX_H + GAP_Y)
        elements.append(make_text(START_X - 5, ry - 25, row_labels[row], font_size=11, color="#9ca3af", width=1300))
        if row > 0:
            elements.append(make_line(START_X, ry - 15, START_X + 4 * BOX_W + 3 * GAP_X, ry - 15, color="#e5e7eb"))

        for col in range(4):
            idx = row * 4 + col
            svc = SERVICES[idx]
            gid = uid()
            bx = START_X + col * (BOX_W + GAP_X)
            by = ry

            elements.append(make_rect(bx, by, BOX_W, BOX_H, group=gid))

            # Logo - pass data URL directly as fileId
            if svc["logo"] and svc["logo"] in logos:
                elements.append(make_image(bx + BOX_W / 2 - 24, by + 18, logos[svc["logo"]], group=gid))
            else:
                elements.append(make_text(bx + BOX_W / 2 - 15, by + 22, "SEC", font_size=18, color="#374151", width=40, align="center", group=gid))

            elements.append(make_text(bx + 10, by + 78, svc["name"], font_size=15, color="#000000", width=BOX_W - 20, align="center", group=gid))
            elements.append(make_text(bx + 10, by + 100, svc["ver"], font_size=12, color="#6b7280", width=BOX_W - 20, align="center", group=gid))
            elements.append(make_line(bx + 20, by + 122, bx + BOX_W - 20, by + 122, color="#e5e7eb"))
            elements.append(make_text(bx + 15, by + 132, svc["port"], font_size=12, color="#374151", width=BOX_W - 30, group=gid))
            feat_text = "\n".join(f"  {f}" for f in svc["features"])
            elements.append(make_text(bx + 15, by + 160, feat_text, font_size=12, color="#4a5568", width=BOX_W - 30, group=gid))

    footer_y = START_Y + 4 * (BOX_H + GAP_Y) + 10
    elements.append(make_line(START_X, footer_y, START_X + 4 * BOX_W + 3 * GAP_X, footer_y, color="#d1d5db"))
    elements.append(make_text(START_X, footer_y + 15, "Security: HTTPS + Single-User Auth  |  Kafka SASL/PLAIN + SCRAM-SHA-512  |  MQTT Password Auth  |  TLS-Ready  |  Network: 4 exposed ports", font_size=12, color="#374151", width=1400))

    save_excalidraw("architecture-overview.excalidraw", elements)


def gen_data_flow(logos):
    """Diagram 2: Data Flow - horizontal pipeline."""
    elements = []

    elements.append(make_text(80, 30, "Data Flow Pipeline", font_size=28, color="#000000", width=1200))
    elements.append(make_text(80, 70, "End-to-end data flow from IoT sensors to real-time dashboards", font_size=15, color="#4a5568", width=1200))

    stages = [
        {"name": "IoT Sensors\n(250 sensors)", "logo": "eclipsemosquitto_logo", "x": 80, "desc": "MQTT Publish\n5 platforms"},
        {"name": "Eclipse\nMosquitto", "logo": "eclipsemosquitto_logo", "x": 350, "desc": "MQTT Broker\n:1883 (Auth)"},
        {"name": "Apache\nNiFi", "logo": "apachenifi_logo", "x": 620, "desc": "10 Process Groups\n:9443 (HTTPS)"},
        {"name": "Apache\nKafka", "logo": "apachekafka_logo", "x": 890, "desc": "7 Topics\n:9092 (SASL)"},
        {"name": "TimescaleDB", "logo": "timescale_logo", "x": 1160, "desc": "Hypertables\n:5432"},
        {"name": "Grafana", "logo": "grafana_logo", "x": 1430, "desc": "4 Dashboards\n:3000"},
    ]

    BOX_W, BOX_H = 200, 200
    BY = 150

    for i, stage in enumerate(stages):
        gid = uid()
        sx = stage["x"]
        elements.append(make_rect(sx, BY, BOX_W, BOX_H, group=gid))
        if stage["logo"] in logos:
            elements.append(make_image(sx + BOX_W / 2 - 24, BY + 15, logos[stage["logo"]], group=gid))
        elements.append(make_text(sx + 10, BY + 75, stage["name"], font_size=14, color="#000000", width=BOX_W - 20, align="center", group=gid))
        elements.append(make_line(sx + 20, BY + 115, sx + BOX_W - 20, BY + 115, color="#e5e7eb"))
        elements.append(make_text(sx + 10, BY + 125, stage["desc"], font_size=12, color="#4a5568", width=BOX_W - 20, align="center", group=gid))
        if i < len(stages) - 1:
            next_x = stages[i + 1]["x"]
            elements.extend(make_arrow(sx + BOX_W + 5, BY + BOX_H / 2, next_x - 5, BY + BOX_H / 2))

    sec_y = BY + BOX_H + 80
    elements.append(make_text(80, sec_y, "Secondary Flows", font_size=16, color="#000000", width=500))

    sec_flows = [
        {"from": "NiFi", "to": "Schema Registry", "logo": "apachekafka_logo", "desc": "Avro schema validation before Kafka publish"},
        {"from": "NiFi", "to": "MinIO", "logo": "minio_logo", "desc": "Flow backups and raw data archival"},
        {"from": "NiFi", "to": "PostgreSQL", "logo": "postgresql_logo", "desc": "Equipment metadata enrichment via DBCP"},
        {"from": "Kafka", "to": "Kafka Exporter", "logo": "apachekafka_logo", "desc": "Consumer lag and offset metrics"},
        {"from": "Prometheus", "to": "Alertmanager", "logo": "prometheus_logo", "desc": "Alert routing when thresholds exceeded"},
    ]

    for i, flow in enumerate(sec_flows):
        fy = sec_y + 35 + i * 30
        if flow["logo"] in logos:
            elements.append(make_image(80, fy - 5, logos[flow["logo"]], w=20, h=20))
        elements.append(make_text(110, fy, f"{flow['from']}  ->  {flow['to']}:  {flow['desc']}", font_size=12, color="#4a5568", width=1200))

    save_excalidraw("data-flow.excalidraw", elements)


def gen_nifi_process_groups(logos):
    """Diagram 3: NiFi Process Groups - 10 PGs with flow."""
    elements = []

    elements.append(make_text(80, 30, "Apache NiFi Process Groups", font_size=28, color="#000000", width=1200))
    elements.append(make_text(80, 70, "10 Process Groups  |  31 Processors  |  :9443 (HTTPS + Single-User Auth)", font_size=15, color="#4a5568", width=1200))

    if "apachenifi_logo" in logos:
        elements.append(make_image(80, 110, logos["apachenifi_logo"]))

    pgs = [
        {"id": "PG-01", "name": "MQTT Ingestion", "procs": "ConsumeMQTT\nUpdateAttribute\nLogAttribute", "out": "raw-data"},
        {"id": "PG-02", "name": "Schema Validation", "procs": "ValidateRecord\nRouteOnAttribute\nUpdateAttribute", "out": "validated-data"},
        {"id": "PG-03", "name": "Data Enrichment", "procs": "LookupRecord\nUpdateRecord", "out": "enriched-data"},
        {"id": "PG-04", "name": "Kafka Publishing", "procs": "PublishKafkaRecord\nUpdateAttribute", "out": "kafka-ack"},
        {"id": "PG-05", "name": "Anomaly Detection", "procs": "ExecutePythonProcessor\nRouteOnAttribute\nEvaluateJsonPath\nUpdateAttribute", "out": "alerts"},
        {"id": "PG-06", "name": "Alert Routing", "procs": "RouteOnAttribute\nUpdateRecord\nPublishKafkaRecord\nLogAttribute", "out": "routed-alerts"},
        {"id": "PG-07", "name": "TimescaleDB Storage", "procs": "ConvertRecord\nPutDatabaseRecord\nUpdateAttribute", "out": "stored"},
        {"id": "PG-08", "name": "Kafka Anomalies", "procs": "PublishKafkaRecord\nUpdateAttribute", "out": "kafka-alerts"},
        {"id": "PG-09", "name": "Compliance Logging", "procs": "MergeContent\nPutFile\nUpdateAttribute\nLogAttribute", "out": "audit-log"},
        {"id": "PG-10", "name": "Dead Letter Queue", "procs": "UpdateAttribute\nPutFile\nLogAttribute\nNotify", "out": "dlq-stored"},
    ]

    BOX_W, BOX_H = 250, 200
    GAP = 40
    START_X, START_Y = 80, 180

    for i, pg in enumerate(pgs):
        col = i % 4
        row = i // 4
        gid = uid()
        bx = START_X + col * (BOX_W + GAP)
        by = START_Y + row * (BOX_H + GAP)

        elements.append(make_rect(bx, by, BOX_W, BOX_H, group=gid))
        elements.append(make_text(bx + 10, by + 10, pg["id"], font_size=11, color="#6b7280", width=BOX_W - 20, group=gid))
        elements.append(make_text(bx + 10, by + 28, pg["name"], font_size=15, color="#000000", width=BOX_W - 20, group=gid))
        elements.append(make_line(bx + 15, by + 52, bx + BOX_W - 15, by + 52, color="#e5e7eb"))
        elements.append(make_text(bx + 15, by + 60, "Processors:", font_size=11, color="#374151", width=BOX_W - 30, group=gid))
        elements.append(make_text(bx + 15, by + 78, pg["procs"], font_size=11, color="#4a5568", width=BOX_W - 30, group=gid))
        elements.append(make_text(bx + 15, by + BOX_H - 25, f"Output: {pg['out']}", font_size=11, color="#6b7280", width=BOX_W - 30, group=gid))

        # Arrow to next PG in flow
        if i < 9:
            next_col = (i + 1) % 4
            next_row = (i + 1) // 4
            nx = START_X + next_col * (BOX_W + GAP)
            ny = START_Y + next_row * (BOX_H + GAP)
            if next_row == row:
                elements.extend(make_arrow(bx + BOX_W + 2, by + BOX_H / 2, nx - 2, ny + BOX_H / 2))

    # Flow description
    flow_y = START_Y + 3 * (BOX_H + GAP) + 20
    elements.append(make_text(START_X, flow_y, "Flow: PG-01 -> PG-02 -> PG-03 -> PG-04 (Kafka) + PG-05 (Anomaly) -> PG-06 -> PG-07 (Storage) + PG-08 (Kafka Alerts) -> PG-09 (Compliance) -> PG-10 (DLQ)", font_size=12, color="#4a5568", width=1200))

    save_excalidraw("nifi-process-groups.excalidraw", elements)


def gen_observability(logos):
    """Diagram 4: Observability Stack."""
    elements = []

    elements.append(make_text(80, 30, "Observability Stack", font_size=28, color="#000000", width=1200))
    elements.append(make_text(80, 70, "Metrics collection, alerting, and visualization", font_size=15, color="#4a5568", width=1200))

    PX, PY, PW, PH = 500, 200, 280, 220
    gid = uid()
    elements.append(make_rect(PX, PY, PW, PH, group=gid))
    if "prometheus_logo" in logos:
        elements.append(make_image(PX + PW / 2 - 24, PY + 15, logos["prometheus_logo"], group=gid))
    elements.append(make_text(PX + 10, PY + 75, "Prometheus", font_size=18, color="#000000", width=PW - 20, align="center", group=gid))
    elements.append(make_text(PX + 10, PY + 100, "v2.54.1  |  :9090", font_size=13, color="#6b7280", width=PW - 20, align="center", group=gid))
    elements.append(make_line(PX + 20, PY + 122, PX + PW - 20, PY + 122, color="#e5e7eb"))
    elements.append(make_text(PX + 15, PY + 130, "  7 alert rules\n  120s scrape interval\n  Multi-target config\n  TSDB retention: 15d", font_size=12, color="#4a5568", width=PW - 30, group=gid))

    # Scrape targets (left side)
    targets = [
        {"name": "NiFi", "logo": "apachenifi_logo", "port": ":9443/metrics"},
        {"name": "Kafka Exporter", "logo": "apachekafka_logo", "port": ":9308/metrics"},
        {"name": "TimescaleDB", "logo": "timescale_logo", "port": ":9187/metrics"},
        {"name": "MinIO", "logo": "minio_logo", "port": ":9000/minio/v2/metrics"},
    ]

    for i, t in enumerate(targets):
        tx, ty = 80, 180 + i * 70
        gid2 = uid()
        elements.append(make_rect(tx, ty, 200, 55, group=gid2))
        if t["logo"] in logos:
            elements.append(make_image(tx + 10, ty + 8, logos[t["logo"]], w=24, h=24, group=gid2))
        elements.append(make_text(tx + 42, ty + 8, t["name"], font_size=13, color="#000000", width=150, group=gid2))
        elements.append(make_text(tx + 42, ty + 28, t["port"], font_size=11, color="#6b7280", width=150, group=gid2))
        # Arrow to Prometheus
        elements.extend(make_arrow(tx + 200 + 2, ty + 27, PX - 2, PY + PH / 2, label="scrape" if i == 0 else None))

    # Grafana (right)
    GX, GY, GW, GH = 900, 200, 280, 220
    gid3 = uid()
    elements.append(make_rect(GX, GY, GW, GH, group=gid3))
    if "grafana_logo" in logos:
        elements.append(make_image(GX + GW / 2 - 24, GY + 15, logos["grafana_logo"], group=gid3))
    elements.append(make_text(GX + 10, GY + 75, "Grafana", font_size=18, color="#000000", width=GW - 20, align="center", group=gid3))
    elements.append(make_text(GX + 10, GY + 100, "v11.4.0  |  :3000", font_size=13, color="#6b7280", width=GW - 20, align="center", group=gid3))
    elements.append(make_line(GX + 20, GY + 122, GX + GW - 20, GY + 122, color="#e5e7eb"))
    elements.append(make_text(GX + 15, GY + 130, "  Platform Overview (10s)\n  Sensor Deep Dive (5s)\n  Alerts & Incidents (15s)\n  Pipeline Health (10s)", font_size=12, color="#4a5568", width=GW - 30, group=gid3))

    # Arrow Prometheus -> Grafana
    elements.extend(make_arrow(PX + PW + 5, PY + PH / 2, GX - 5, GY + GH / 2, label="query"))

    # Alertmanager (below Prometheus)
    AX, AY, AW, AH = 500, 480, 280, 160
    gid4 = uid()
    elements.append(make_rect(AX, AY, AW, AH, group=gid4))
    if "prometheus_logo" in logos:
        elements.append(make_image(AX + AW / 2 - 24, AY + 12, logos["prometheus_logo"], group=gid4))
    elements.append(make_text(AX + 10, AY + 68, "Alertmanager", font_size=18, color="#000000", width=AW - 20, align="center", group=gid4))
    elements.append(make_text(AX + 10, AY + 92, "v0.27.0  |  :9093", font_size=13, color="#6b7280", width=AW - 20, align="center", group=gid4))
    elements.append(make_text(AX + 15, AY + 115, "  Email notifications\n  Alert grouping & dedup", font_size=12, color="#4a5568", width=AW - 30, group=gid4))

    # Arrow Prometheus -> Alertmanager
    elements.extend(make_arrow(PX + PW / 2, PY + PH + 5, AX + AW / 2, AY - 5, label="alerts"))

    save_excalidraw("observability-stack.excalidraw", elements)


def gen_security(logos):
    """Diagram 5: Security Architecture."""
    elements = []

    elements.append(make_text(80, 30, "Security Architecture", font_size=28, color="#000000", width=1200))
    elements.append(make_text(80, 70, "Platform Hardening  |  Grade: D -> B+  |  30 Vulnerabilities Remediated", font_size=15, color="#4a5568", width=1200))

    # Security layers (horizontal bands)
    layers = [
        {"name": "Network Layer", "color": "#f3f4f6", "items": [
            {"icon": "docker_logo", "text": "Docker Network: oilgas-net (bridge)"},
            {"icon": None, "text": "4 exposed ports: 9443, 3000, 9000, 9001"},
            {"icon": None, "text": "Internal-only: 1883, 5432, 9090, 9093, 8081"},
        ]},
        {"name": "Authentication Layer", "color": "#ffffff", "items": [
            {"icon": "apachenifi_logo", "text": "NiFi: HTTPS :9443 + Single-User Auth"},
            {"icon": "apachekafka_logo", "text": "Kafka: SASL/PLAIN + SCRAM-SHA-512"},
            {"icon": "eclipsemosquitto_logo", "text": "MQTT: Password-based auth + TLS-ready"},
            {"icon": "grafana_logo", "text": "Grafana: Parameterized admin/reader passwords"},
        ]},
        {"name": "Encryption Layer", "color": "#f3f4f6", "items": [
            {"icon": "apachenifi_logo", "text": "NiFi: Self-signed TLS certificate"},
            {"icon": "postgresql_logo", "text": "Database: SSL connections (when enabled)"},
            {"icon": None, "text": "Secrets: Environment variables, no hardcoded values"},
        ]},
        {"name": "Monitoring Layer", "color": "#ffffff", "items": [
            {"icon": "prometheus_logo", "text": "Prometheus: 7 alert rules (KafkaConsumerLag, NiFiBackPressure, etc.)"},
            {"icon": "grafana_logo", "text": "Grafana: 4 dashboards, 35 panels"},
            {"icon": None, "text": "Alertmanager: Email routing, deduplication"},
        ]},
    ]

    LAYER_W = 1100
    START_X, START_Y = 80, 130
    LAYER_H_BASE = 50

    for i, layer in enumerate(layers):
        item_count = len(layer["items"])
        layer_h = LAYER_H_BASE + item_count * 30
        ly = START_Y + sum(LAYER_H_BASE + len(layers[j]["items"]) * 30 + 20 for j in range(i))

        elements.append(make_rect(START_X, ly, LAYER_W, layer_h, stroke="#d1d5db", fill=layer["color"]))
        elements.append(make_text(START_X + 15, ly + 10, layer["name"], font_size=15, color="#000000", width=300))

        for j, item in enumerate(layer["items"]):
            iy = ly + 38 + j * 30
            if item["icon"] and item["icon"] in logos:
                elements.append(make_image(START_X + 30, iy - 2, logos[item["icon"]], w=20, h=20))
            elements.append(make_text(START_X + 60, iy, item["text"], font_size=12, color="#374151", width=LAYER_W - 80))

    # Audit summary box
    summary_y = START_Y + sum(LAYER_H_BASE + len(l["items"]) * 30 + 20 for l in layers) + 20
    elements.append(make_rect(START_X, summary_y, LAYER_W, 120, stroke="#000000"))
    elements.append(make_text(START_X + 15, summary_y + 10, "Security Audit Summary", font_size=15, color="#000000", width=400))
    elements.append(make_text(START_X + 15, summary_y + 35,
        "Initial (2026-02-23):  Grade D  |  13 CRITICAL  |  8 HIGH  |  7 MEDIUM  |  2 LOW",
        font_size=12, color="#4a5568", width=LAYER_W - 30))
    elements.append(make_text(START_X + 15, summary_y + 55,
        "Final   (2026-02-24):  Grade B+ |   0 CRITICAL  |  0 HIGH  |  0 MEDIUM  |  0 LOW",
        font_size=12, color="#000000", width=LAYER_W - 30))
    elements.append(make_text(START_X + 15, summary_y + 80,
        "8 sprints  |  20 files modified  |  16 services hardened  |  Pre-commit: Gitleaks + Ruff",
        font_size=12, color="#6b7280", width=LAYER_W - 30))

    save_excalidraw("security-architecture.excalidraw", elements)


def main():
    print("Loading logos from KB...")
    logos = load_logos()
    print(f"  Found {len(logos)} logos: {', '.join(logos.keys())}")
    print()

    print("Generating diagrams...")
    gen_architecture_overview(logos)
    gen_data_flow(logos)
    gen_nifi_process_groups(logos)
    gen_observability(logos)
    gen_security(logos)

    print()
    print("Done! Generated 5 diagrams in diagrams/")


if __name__ == "__main__":
    main()
