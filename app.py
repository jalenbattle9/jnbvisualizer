import os
import re
import uuid
import json
import csv
import sqlite3
import shutil
import zipfile
from datetime import datetime, timezone
from typing import List
from io import BytesIO

from fastapi import FastAPI, HTTPException, Form
from fastapi.responses import HTMLResponse, FileResponse, Response
from pyembroidery import read, write
from PIL import Image, ImageDraw, ImageFont


# ============================================================
# CONFIG
# ============================================================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

DATA_DIR = os.environ.get("JNB_DATA_DIR", "").strip() or BASE_DIR

# Put your ORIGINAL PES files here (these can stay in the repo):
MASTER_DIR = os.path.join(BASE_DIR, "designs", "master")

# Anything you CREATE should go on the persistent disk:
GENERATED_DIR = os.path.join(DATA_DIR, "designs", "generated")
BACKUP_DIR = os.path.join(DATA_DIR, "backups")

DB_PATH = os.path.join(DATA_DIR, "proofs.db")
LOG_CSV_PATH = os.path.join(DATA_DIR, "proofs_log.csv")

DESIGN_MAP_PATH = os.path.join(BASE_DIR, "design_map.json")

# Set in Render Environment Variables:
#   JNB_ADMIN_PASSWORD=your-strong-password
ADMIN_PASSWORD = os.environ.get("JNB_ADMIN_PASSWORD", "change-this-now")

# Optional mirror backup folder (recommended if you want a second copy somewhere safe)
#   JNB_MIRROR_BACKUP_DIR=/some/path
MIRROR_BACKUP_DIR = os.environ.get("JNB_MIRROR_BACKUP_DIR", "").strip() or None

MAX_BLOCKS = 20

# Clean preview: treat big stitch moves as "jump" and do NOT draw them
JUMP_DISTANCE_THRESHOLD = float(os.environ.get("JNB_JUMP_THRESHOLD", "45.0"))

os.makedirs(MASTER_DIR, exist_ok=True)
os.makedirs(GENERATED_DIR, exist_ok=True)
os.makedirs(BACKUP_DIR, exist_ok=True)
if MIRROR_BACKUP_DIR:
    os.makedirs(MIRROR_BACKUP_DIR, exist_ok=True)

app = FastAPI(title="jnbvisualizer")


# ============================================================
# DATABASE
# ============================================================
def db():
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    return con


def init_db():
    con = db()
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS proofs (
            proof_id TEXT PRIMARY KEY,
            design_file TEXT NOT NULL,
            client_tag TEXT NOT NULL,
            bg_hex TEXT NOT NULL,
            colors_csv TEXT NOT NULL,
            created_utc TEXT NOT NULL,
            generated_pes_path TEXT NOT NULL
        )
        """
    )
    con.commit()
    con.close()


init_db()


# ============================================================
# HELPERS
# ============================================================
def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def list_pes_files() -> List[str]:
    if not os.path.isdir(MASTER_DIR):
        return []
    return sorted([f for f in os.listdir(MASTER_DIR) if f.lower().endswith(".pes")])


def safe_tag(s: str, max_len: int = 28) -> str:
    s = (s or "").strip().lower()
    s = re.sub(r"[^a-z0-9_-]+", "_", s)
    s = re.sub(r"_+", "_", s).strip("_")
    return (s or "client")[:max_len]


def require_admin(pw: str):
    if pw != ADMIN_PASSWORD:
        raise HTTPException(status_code=401, detail="Unauthorized")


def new_proof_id() -> str:
    return "JNB-" + uuid.uuid4().hex[:8].upper()


def hex_to_rgb(hex_color: str):
    h = (hex_color or "").strip().lstrip("#")
    if len(h) == 3:
        h = "".join([c * 2 for c in h])
    if len(h) != 6 or not re.fullmatch(r"[0-9a-fA-F]{6}", h):
        raise HTTPException(status_code=400, detail=f"Invalid color: {hex_color}")
    return (int(h[0:2], 16), int(h[2:4], 16), int(h[4:6], 16))


def hex_to_rgb_int(hex_color: str) -> int:
    h = (hex_color or "").strip().lstrip("#")
    if len(h) == 3:
        h = "".join([c * 2 for c in h])
    if len(h) != 6 or not re.fullmatch(r"[0-9a-fA-F]{6}", h):
        raise HTTPException(status_code=400, detail=f"Invalid color: {hex_color}")
    return int(h, 16)  # 0xRRGGBB


def validate_design_file(design_file: str) -> str:
    files = set(list_pes_files())
    if design_file not in files:
        raise HTTPException(status_code=404, detail="Design file not found in designs/master.")
    return os.path.join(MASTER_DIR, design_file)


def parse_colors_csv(colors_csv: str) -> List[str]:
    arr = [c.strip() for c in (colors_csv or "").split(",") if c.strip()]
    if not arr:
        raise HTTPException(status_code=400, detail="No colors provided.")
    arr = arr[:MAX_BLOCKS]
    for c in arr:
        _ = hex_to_rgb_int(c)
    return arr


def ensure_csv_header():
    if os.path.exists(LOG_CSV_PATH):
        return
    with open(LOG_CSV_PATH, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["created_utc", "proof_id", "design_file", "client_tag", "bg_hex", "colors_csv", "generated_pes_filename"])


def append_csv_log(created_utc, proof_id, design_file, client_tag, bg_hex, colors_csv, generated_path):
    ensure_csv_header()
    with open(LOG_CSV_PATH, "a", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow([created_utc, proof_id, design_file, client_tag, bg_hex, colors_csv, os.path.basename(generated_path)])


def write_json_snapshot(payload: dict, proof_id: str) -> str:
    snap_path = os.path.join(BACKUP_DIR, f"{proof_id}.json")
    with open(snap_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)
    return snap_path


def mirror_file_if_enabled(path: str):
    if not MIRROR_BACKUP_DIR:
        return
    try:
        shutil.copy2(path, os.path.join(MIRROR_BACKUP_DIR, os.path.basename(path)))
    except Exception:
        pass


# ============================================================
# OPTIONAL SLUG LOCK ( /w/{slug} ) using design_map.json
# ============================================================
def load_design_map() -> dict:
    if not os.path.exists(DESIGN_MAP_PATH):
        return {}
    with open(DESIGN_MAP_PATH, "r", encoding="utf-8") as f:
        try:
            return json.load(f)
        except Exception:
            return {}


# ============================================================
# PES -> BLOCKS -> RENDER (CLEAN: NO JUMPS/TRAVEL)
# ============================================================
def pattern_to_blocks_clean(pattern):
    blocks = []
    current = []
    last = None

    for x, y, cmd in pattern.stitches:
        # 0=STITCH, 1=JUMP, 2=TRIM, 3=STOP, 4=END, 5=COLOR_CHANGE
        if cmd == 0:  # STITCH
            if last is not None:
                x0, y0 = last
                dx = x - x0
                dy = y - y0
                dist = (dx * dx + dy * dy) ** 0.5

                # Treat long moves as jumps (do NOT draw)
                if dist > JUMP_DISTANCE_THRESHOLD:
                    last = (x, y)
                    continue

                current.append((x0, y0, x, y))
            last = (x, y)

        elif cmd == 5:  # COLOR_CHANGE
            if current:
                blocks.append(current)
                current = []
            last = None

        else:
            last = None

    if current:
        blocks.append(current)

    return blocks


def normalize_blocks(blocks, padding=40, canvas=900):
    pts = []
    for b in blocks:
        for x1, y1, x2, y2 in b:
            pts.append((x1, y1))
            pts.append((x2, y2))

    if not pts:
        return blocks, canvas

    xs = [p[0] for p in pts]
    ys = [p[1] for p in pts]
    minx, maxx = min(xs), max(xs)
    miny, maxy = min(ys), max(ys)
    w = (maxx - minx) or 1
    h = (maxy - miny) or 1

    scale = (canvas - 2 * padding) / max(w, h)

    out = []
    for b in blocks:
        nb = []
        for x1, y1, x2, y2 in b:
            nx1 = (x1 - minx) * scale + padding
            ny1 = (y1 - miny) * scale + padding
            nx2 = (x2 - minx) * scale + padding
            ny2 = (y2 - miny) * scale + padding
            nb.append((nx1, ny1, nx2, ny2))
        out.append(nb)

    return out, canvas


def extract_thread_colors(pattern) -> List[str]:
    colors = []
    for t in getattr(pattern, "threadlist", []) or []:
        try:
            c = int(getattr(t, "color", 0)) & 0xFFFFFF
            colors.append(f"#{c:06x}")
        except Exception:
            colors.append("#000000")
    return colors[:MAX_BLOCKS]


def get_block_count(pattern) -> int:
    blocks = pattern_to_blocks_clean(pattern)
    return min(len(blocks), MAX_BLOCKS)


def render_preview_png(design_path: str, bg_hex: str, colors_hex: List[str]) -> bytes:
    pattern = read(design_path)
    blocks = pattern_to_blocks_clean(pattern)
    blocks = blocks[:MAX_BLOCKS]
    blocks, canvas = normalize_blocks(blocks, padding=40, canvas=900)

    fallback = extract_thread_colors(pattern)

    img = Image.new("RGB", (canvas, canvas), hex_to_rgb(bg_hex))
    draw = ImageDraw.Draw(img)

    line_width = 2

    for i, block in enumerate(blocks):
        if i < len(colors_hex):
            col = hex_to_rgb(colors_hex[i])
        elif i < len(fallback):
            col = hex_to_rgb(fallback[i])
        else:
            col = (0, 0, 0)

        for x1, y1, x2, y2 in block:
            draw.line((x1, y1, x2, y2), fill=col, width=line_width)

    # watermark
    try:
        font = ImageFont.load_default()
    except Exception:
        font = None
    draw.rectangle((0, canvas - 26, canvas, canvas), fill=(0, 0, 0))
    draw.text((10, canvas - 20), "jnbvisualizer proof (preview only)", fill=(255, 255, 255), font=font)

    buf = BytesIO()
    img.save(buf, format="PNG")
    return buf.getvalue()


def generate_recolored_pes(master_path: str, colors_hex: List[str], proof_id: str, client_tag: str, design_file: str) -> str:
    pattern = read(master_path)

    if not pattern.threadlist:
        raise HTTPException(status_code=500, detail="Master PES has no thread list.")

    n = min(len(pattern.threadlist), len(colors_hex))
    for i in range(n):
        pattern.threadlist[i].color = hex_to_rgb_int(colors_hex[i])

    out_name = f"{os.path.splitext(design_file)[0]}__{safe_tag(client_tag)}__{proof_id}.pes"
    out_path = os.path.join(GENERATED_DIR, out_name)

    write(pattern, out_path)
    return out_path


# ============================================================
# ROUTES
# ============================================================
@app.get("/", response_class=HTMLResponse)
def home():
    return (
        "<h2>jnbvisualizer</h2>"
        "<p>Widget: <b>/widget</b></p>"
        "<p>Locked widget: <b>/widget?design=FILE.pes&lock=1</b></p>"
        "<p>Optional slug lock: <b>/w/slug</b> (design_map.json)</p>"
        "<p>Admin: <b>/admin?pw=YOUR_PASSWORD</b></p>"
    )


@app.get("/design-info")
def design_info(design: str):
    design_path = validate_design_file(design)
    pattern = read(design_path)
    colors = extract_thread_colors(pattern)
    blocks = get_block_count(pattern)
    return {"design": design, "colors": colors, "block_count": blocks}


@app.get("/w/{slug}", response_class=HTMLResponse)
def widget_locked_by_slug(slug: str):
    mapping = load_design_map()
    if slug not in mapping:
        raise HTTPException(status_code=404, detail="Unknown design link.")
    design_file = mapping[slug]
    _ = validate_design_file(design_file)
    return widget(design=design_file, lock=1)


@app.get("/widget", response_class=HTMLResponse)
def widget(design: str = "", lock: int = 0):
    pes_files = list_pes_files()
    if not pes_files:
        return "<h3>No .pes files found in designs/master</h3><p>Put your PES files in designs/master</p>"

    locked = bool(lock)

    if locked:
        if not design:
            raise HTTPException(status_code=400, detail="Locked widget requires a design.")
        design = os.path.basename(design)
        validate_design_file(design)
        selected = design
    else:
        selected = design if (design and design in pes_files) else pes_files[0]

    # Dropdown only if not locked
    if not locked:
        opts = []
        for f in pes_files:
            sel = " selected" if f == selected else ""
            opts.append(f"<option value='{f}'{sel}>{f}</option>")
        design_html = (
            "<label>Design</label>"
            "<select id='design' onchange='onDesignChange()'>"
            + "".join(opts) +
            "</select>"
        )
    else:
        design_html = (
            f"<input type='hidden' id='design' value='{selected}' />"
            f"<div class='locked'>Design locked</div>"
        )

    color_inputs = []
    for i in range(1, MAX_BLOCKS + 1):
        color_inputs.append(
            f"""
            <div class="block" id="blkwrap{i}">
              <label id="blklab{i}">Block {i}</label>
              <div class="row2">
                <input id="c{i}" type="color" value="#000000" style="width:90px;height:34px;" />
                <span id="na{i}" class="na"> </span>
              </div>
            </div>
            """
        )

    html = f"""
<!doctype html>
<html>
<head>
  <meta charset="utf-8"/>
  <title>jnbvisualizer</title>
  <style>
    body {{ font-family: Arial, sans-serif; margin: 16px; }}
    .row {{ display:flex; gap:16px; align-items:flex-start; flex-wrap:wrap; }}
    .panel {{ flex:1; min-width: 360px; }}
    .box {{ border:1px solid #ddd; border-radius:12px; padding:14px; background:#fff; }}
    label {{ display:block; margin-top:10px; font-size: 13px; }}
    input[type="text"] {{ width:100%; padding:8px; }}
    select {{ width:100%; padding:8px; }}
    button {{ margin-top: 12px; padding: 10px 12px; cursor:pointer; }}
    .grid {{ display:flex; flex-wrap:wrap; gap:14px; margin-top:8px; }}
    .block {{ width:180px; }}
    .row2 {{ display:flex; gap:10px; align-items:center; }}
    .na {{ font-size:12px; color:#666; }}
    img {{ width:100%; max-width:900px; min-height:420px; object-fit:contain; background:#fff; display:block; border-radius:12px; border:1px solid #eee; }}
    .small {{ font-size:12px; color:#444; margin-top:8px; }}
    .locked {{ font-size:12px; color:#222; margin:10px 0; padding:8px 10px; background:#f6f6f6; border-radius:8px; border:1px solid #e5e5e5; display:inline-block; }}
    #debug {{ margin-top:8px; font-size:12px; color:#666; word-break: break-all; }}
  </style>
</head>
<body>

<div class="row">
  <div class="panel box">
    <h3>Preview Thread Colors</h3>
    <div class="small">This is a proof image only. Original design files are never downloadable to customers.</div>

    {design_html}

    <label>Your name / tag</label>
    <input id="clientTag" type="text" placeholder="ex: maniz" />

    <label>Background (garment color)</label>
    <input id="bg" type="color" value="#ffffff" onchange="refresh()" />

    <label style="margin-top:14px;">Color blocks (up to {MAX_BLOCKS})</label>
    <div class="grid">
      {''.join(color_inputs)}
    </div>

    <button onclick="refresh()">Update Preview</button>
    <button onclick="saveProof()">Save Proof</button>

    <!-- ✅ ONLY ONE result div -->
    <div id="result" style="margin-top:10px;"></div>
  </div>

  <div class="panel box">
    <h3>Live Preview</h3>
    <div class="small">Clean preview: no jump/travel lines. Final embroidery may vary slightly.</div>
    <img id="preview" alt="preview"/>
    <div id="debug"></div>
  </div>
</div>

<script>
const MAX_BLOCKS = {MAX_BLOCKS};

function getColors() {{
  const arr = [];
  for (let i = 1; i <= MAX_BLOCKS; i++) {{
    const el = document.getElementById("c" + i);
    if (el) arr.push(el.value);
  }}
  return arr;
}}

function getDesign() {{
  const el = document.getElementById("design");
  return el ? el.value : "{selected}";
}}

function setNA(i, isNA) {{
  const picker = document.getElementById("c" + i);
  const lab = document.getElementById("blklab" + i);
  const na = document.getElementById("na" + i);
  if (!picker || !lab || !na) return;

  if (isNA) {{
    picker.disabled = true;
    picker.value = "#000000";
    lab.textContent = "Block " + i + " (N/A)";
    na.textContent = "N/A";
  }} else {{
    picker.disabled = false;
    lab.textContent = "Block " + i;
    na.textContent = "";
  }}
}}

async function loadDesignColors() {{
  const design = getDesign();
  const res = await fetch("/design-info?design=" + encodeURIComponent(design));
  const data = await res.json();

  const blockCount = (data.block_count || 0);

  for (let i = 1; i <= MAX_BLOCKS; i++) {{
    setNA(i, i > blockCount);
  }}

  if (data.colors && data.colors.length) {{
    for (let i = 1; i <= Math.min(data.colors.length, MAX_BLOCKS); i++) {{
      const el = document.getElementById("c" + i);
      if (el && !el.disabled) el.value = data.colors[i - 1];
    }}
  }}
}}

function refresh() {{
  const design = getDesign();
  const bg = document.getElementById("bg").value;
  const colors = getColors().join(",");

  const url = "/preview.png?design=" + encodeURIComponent(design)
            + "&bg=" + encodeURIComponent(bg)
            + "&colors=" + encodeURIComponent(colors);

  const img = document.getElementById("preview");
  img.src = url + "&t=" + Date.now();

  const dbg = document.getElementById("debug");
  if (dbg) dbg.textContent = "Preview URL: " + img.src;
}}

async function saveProof() {{
  const design = getDesign();
  const clientTag = document.getElementById("clientTag").value || "client";
  const bg = document.getElementById("bg").value;
  const colors = getColors().join(",");

  const form = new FormData();
  form.append("design_file", design);
  form.append("client_tag", clientTag);
  form.append("bg_hex", bg);
  form.append("colors_csv", colors);

  const result = document.getElementById("result");
  result.innerHTML = "";

  let res, data;
  try {{
    res = await fetch("/save-proof", {{ method: "POST", body: form }});
    data = await res.json();
  }} catch (e) {{
    result.innerHTML = "<span style='color:red;'>Error: Could not reach server.</span>";
    return;
  }}

  if (!res.ok) {{
    result.innerHTML = "<span style='color:red;'>Error: " + (data.detail || "Unknown") + "</span>";
    return;
  }}

  result.innerHTML = `
    <div style="padding:10px; margin-top:10px; background:#e6ffe6; border:1px solid #2ecc71; color:#145a32; font-weight:bold;">
      ✅ Proof Saved Successfully
    </div>
  `;
}}  // ✅ IMPORTANT: closes saveProof()

async function onDesignChange() {{
  await loadDesignColors();
  refresh();
}}

loadDesignColors().then(refresh);
</script>

</body>
</html>
"""
    return HTMLResponse(content=html)


@app.get("/preview.png")
def preview_png(design: str, bg: str, colors: str):
    design_path = validate_design_file(design)
    _ = hex_to_rgb_int(bg)
    colors_list = parse_colors_csv(colors)

    png_bytes = render_preview_png(design_path, bg, colors_list)
    return Response(content=png_bytes, media_type="image/png")


@app.post("/save-proof")
def save_proof(
    design_file: str = Form(...),
    client_tag: str = Form(...),
    bg_hex: str = Form(...),
    colors_csv: str = Form(...),
):
    master_path = validate_design_file(design_file)
    client_tag_clean = safe_tag(client_tag)

    _ = hex_to_rgb_int(bg_hex)
    colors_list = parse_colors_csv(colors_csv)

    proof_id = new_proof_id()
    created = utc_now_iso()

    out_path = generate_recolored_pes(master_path, colors_list, proof_id, client_tag_clean, design_file)

    con = db()
    con.execute(
        "INSERT INTO proofs (proof_id, design_file, client_tag, bg_hex, colors_csv, created_utc, generated_pes_path) VALUES (?,?,?,?,?,?,?)",
        (proof_id, design_file, client_tag_clean, bg_hex, ",".join(colors_list), created, out_path),
    )
    con.commit()
    con.close()

    append_csv_log(created, proof_id, design_file, client_tag_clean, bg_hex, ",".join(colors_list), out_path)

    snap_payload = {
        "created_utc": created,
        "proof_id": proof_id,
        "design_file": design_file,
        "client_tag": client_tag_clean,
        "bg_hex": bg_hex,
        "colors": colors_list,
        "generated_pes_filename": os.path.basename(out_path),
    }
    snap_path = write_json_snapshot(snap_payload, proof_id)

    mirror_file_if_enabled(DB_PATH)
    mirror_file_if_enabled(LOG_CSV_PATH)
    mirror_file_if_enabled(snap_path)

    return {"proof_id": proof_id}


@app.get("/admin", response_class=HTMLResponse)
def admin(pw: str):
    require_admin(pw)
    con = db()
    rows = con.execute(
        "SELECT proof_id, design_file, client_tag, created_utc FROM proofs ORDER BY created_utc DESC LIMIT 200"
    ).fetchall()
    con.close()

    items = []
    for r in rows:
        items.append(
            f"<li>{r['created_utc']} — <b>{r['proof_id']}</b> — {r['design_file']} — {r['client_tag']} — "
            f"<a href='/admin/download/{r['proof_id']}?pw={pw}'>Download</a></li>"
        )

    mirror_note = ""
    if MIRROR_BACKUP_DIR:
        mirror_note = f"<p><b>Mirror Backup Folder:</b> {MIRROR_BACKUP_DIR}</p>"

    return (
        "<h2>jnbvisualizer admin</h2>"
        f"<p><a href='/admin/backup.zip?pw={pw}'>Download Backup ZIP</a></p>"
        + mirror_note +
        "<p>Recent proofs:</p>"
        "<ul>" + "".join(items) + "</ul>"
    )


@app.get("/admin/download/{proof_id}")
def admin_download(proof_id: str, pw: str):
    require_admin(pw)
    con = db()
    r = con.execute(
        "SELECT generated_pes_path FROM proofs WHERE proof_id=?", (proof_id.strip(),)
    ).fetchone()
    con.close()

    if not r:
        raise HTTPException(status_code=404, detail="Proof not found.")

    path = r["generated_pes_path"]
    if not os.path.exists(path):
        raise HTTPException(status_code=404, detail="Generated file missing.")

    return FileResponse(path, media_type="application/octet-stream", filename=os.path.basename(path))


@app.get("/admin/backup.zip")
def admin_backup_zip(pw: str):
    require_admin(pw)

    mem = BytesIO()
    with zipfile.ZipFile(mem, "w", compression=zipfile.ZIP_DEFLATED) as z:
        if os.path.exists(DB_PATH):
            z.write(DB_PATH, arcname="proofs.db")
        if os.path.exists(LOG_CSV_PATH):
            z.write(LOG_CSV_PATH, arcname="proofs_log.csv")

        if os.path.isdir(BACKUP_DIR):
            for name in os.listdir(BACKUP_DIR):
                if name.lower().endswith(".json"):
                    z.write(os.path.join(BACKUP_DIR, name), arcname=f"backups/{name}")

        if os.path.isdir(GENERATED_DIR):
            for name in os.listdir(GENERATED_DIR):
                if name.lower().endswith(".pes"):
                    z.write(os.path.join(GENERATED_DIR, name), arcname=f"generated/{name}")

        if os.path.exists(DESIGN_MAP_PATH):
            z.write(DESIGN_MAP_PATH, arcname="design_map.json")

    mem.seek(0)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"JNB_BACKUP_{stamp}.zip"
    return Response(
        mem.read(),
        media_type="application/zip",
        headers={"Content-Disposition": f"attachment; filename={filename}"}
    )
