#!/usr/bin/env python3
import os, re, datetime
from flask import Flask, request, jsonify, render_template, abort
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv

load_dotenv()

DB = dict(
    host=os.getenv("DATABASE_HOST","localhost"),
    port=int(os.getenv("DATABASE_PORT","5432")),
    dbname=os.getenv("DATABASE_NAME","warehouse_pricing"),
    user=os.getenv("DATABASE_USER","postgres"),
    password=os.getenv("DATABASE_PASSWORD","")
)
APPROVER = os.getenv("APP_APPROVER","approver")
BATCH_PREFIX = os.getenv("BATCH_PREFIX","legacy_review")
API_TOKEN = os.getenv("API_TOKEN","change-me")

app = Flask(__name__)

def get_conn():
    return psycopg2.connect(**DB)

def require_token():
    token = request.headers.get("X-API-Token") or request.args.get("token")
    if not token or token != API_TOKEN:
        abort(401, description="Unauthorized")

def today_batch_id(seq=1):
    return f"{BATCH_PREFIX}_{datetime.date.today().strftime('%Y%m%d')}_{seq}"

@app.route("/")
def index():
    return render_template("index.html", api_token=API_TOKEN)

def fetch_next(packmatch_only=False):
    sql = """
    WITH cand AS (
      SELECT r.*,
             NULLIF(regexp_replace(COALESCE(r.product_pack::text,''), '[^0-9\\.]', '', 'g'), '')::numeric AS p_sz,
             NULLIF(regexp_replace(COALESCE(r.dmd_pack::text,''),     '[^0-9\\.]', '', 'g'), '')::numeric AS d_sz
      FROM v_mapping_review r
      WHERE NOT already_mapped
        AND dmd_item_id IS NOT NULL
    )
    SELECT stage_id FROM cand
    WHERE %s OR p_sz IS NULL OR d_sz IS NULL OR p_sz = d_sz
    ORDER BY stage_id
    LIMIT 1;
    """
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql, (packmatch_only,))
        row = cur.fetchone()
        if not row:
            return None
        stage_id = row[0]
    return fetch_stage(stage_id)

def fetch_stage(stage_id):
    sql = """
    SELECT stage_id, pip, product_id, product_name, product_pack,
           dmd_item_id, dmd_level, vmpp_id, ampp_id, dmd_pack, dmd_price, dmd_cat,
           pay_cat_name, zero_discount, already_mapped,
           (SELECT dn.dm_name FROM dmd_name dn WHERE (dn.vmpp_id = vmpp_id OR dn.ampp_id = ampp_id) LIMIT 1) AS dmd_name
    FROM v_mapping_review WHERE stage_id = %s;
    """
    with get_conn() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(sql, (stage_id,))
        return cur.fetchone()

@app.get("/api/next")
def api_next():
    require_token()
    packmatch = request.args.get("packmatch","0") in ("1","true","yes","on")
    data = fetch_next(packmatch_only=packmatch)
    return jsonify({"ok": True, "stage": data})

@app.post("/api/approve")
def api_approve():
    require_token()
    payload = request.get_json(force=True)
    sid = payload.get("stage_id")
    if not sid:
        abort(400, description="stage_id required")
    # insert mapping and mark approved
    with get_conn() as conn, conn.cursor() as cur:
        # already mapped?
        cur.execute("SELECT already_mapped FROM v_mapping_review WHERE stage_id=%s;", (sid,))
        row = cur.fetchone()
        if not row:
            abort(404, description="stage_id not found")
        if row[0]:
            return jsonify({"ok": True, "message": "Already mapped"})
        # choose batch id for today
        seq=1
        batch_id = f"{BATCH_PREFIX}_{datetime.date.today().strftime('%Y%m%d')}_{seq}"
        while True:
            cur.execute("SELECT 1 FROM product_mapping WHERE batch_id=%s LIMIT 1;", (batch_id,))
            if cur.fetchone():
                seq += 1
                batch_id = f"{BATCH_PREFIX}_{datetime.date.today().strftime('%Y%m%d')}_{seq}"
            else:
                break
        cur.execute("""
          INSERT INTO product_mapping (product_id, dmd_item_id, confidence_score, approved_by, approved_at, batch_id)
          SELECT product_id, dmd_item_id, 1.0, %s, now(), %s
          FROM v_mapping_review WHERE stage_id=%s
          ON CONFLICT DO NOTHING;
        """, (APPROVER, batch_id, sid))
        cur.execute("UPDATE mapping_stage SET approved=TRUE, reviewed=TRUE, reviewer=%s WHERE id=%s;", (APPROVER, sid))
        conn.commit()
    return jsonify({"ok": True, "approved": sid, "batch_id": batch_id})

@app.post("/api/skip")
def api_skip():
    require_token()
    payload = request.get_json(force=True)
    sid = payload.get("stage_id")
    note = payload.get("note","")
    if not sid:
        abort(400, description="stage_id required")
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("UPDATE mapping_stage SET reviewed=TRUE, review_notes=%s, reviewer=%s WHERE id=%s;", (note, APPROVER, sid))
        conn.commit()
    return jsonify({"ok": True, "skipped": sid})

@app.get("/api/stats")
def api_stats():
    require_token()
    sql = """
    SELECT
      (SELECT COUNT(*) FROM mapping_stage) AS staged,
      (SELECT COUNT(*) FROM mapping_stage WHERE approved) AS approved_stage,
      (SELECT COUNT(*) FROM product_mapping) AS mapped_products,
      (SELECT COUNT(*) FROM product) AS total_products;
    """
    with get_conn() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(sql)
        stats = cur.fetchone()
    return jsonify({"ok": True, "stats": stats})

if __name__ == "__main__":
    app.run(host="127.0.0.1", port=5000, debug=True)
