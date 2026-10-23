"""เว็บ dashboard ราคาน้ำมัน อ่านข้อมูลจาก PostgreSQL โดยตรงทุก request."""

import os
from datetime import date, timedelta

import psycopg2
import psycopg2.extras
from flask import Flask, jsonify, render_template, request

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://fuel:fuel@localhost:5433/fueldb")
app = Flask(__name__)


def query(sql: str, params: tuple = ()) -> list[dict]:
    with psycopg2.connect(DATABASE_URL) as conn, conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(sql, params)
        return [dict(r) for r in cur.fetchall()]


def jsonable(rows: list[dict]) -> list[dict]:
    for row in rows:
        for key, value in row.items():
            if hasattr(value, "isoformat"):
                row[key] = value.isoformat()
            elif value is not None and type(value).__name__ == "Decimal":
                row[key] = float(value)
    return rows


def filters() -> tuple[str, list]:
    """แปลง query string เป็น WHERE: fuel, brand (หลายค่าได้), start, end."""
    where, params = ["true"], []
    if fuel := request.args.get("fuel"):
        where.append("p.fuel_code = %s")
        params.append(fuel)
    if brands := [b for b in request.args.getlist("brand") if b]:
        where.append("p.brand = ANY(%s)")
        params.append(brands)
    if start := request.args.get("start"):
        where.append("p.price_date >= %s")
        params.append(start)
    if end := request.args.get("end"):
        where.append("p.price_date <= %s")
        params.append(end)
    return " AND ".join(where), params


@app.route("/")
def index():
    return render_template("index.html", page="dashboard")


@app.route("/status")
def status():
    return render_template("status.html", page="status")


@app.route("/api/meta")
def meta():
    fuels = query("SELECT fuel_code, fuel_name, fuel_group FROM dim_fuel ORDER BY fuel_group, fuel_code")
    brands = query("SELECT DISTINCT brand FROM fuel_prices ORDER BY brand")
    span = query("SELECT min(price_date) AS min_date, max(price_date) AS max_date FROM fuel_prices")[0]
    return jsonify(fuels=fuels, brands=[b["brand"] for b in brands], **jsonable([span])[0])


@app.route("/api/kpi")
def kpi():
    where, params = filters()
    summary = query(
        f"""
        SELECT count(*) AS rows, count(DISTINCT p.brand) AS brands, count(DISTINCT p.fuel_code) AS fuels,
               max(p.price_date) AS latest, round(avg(p.price), 2) AS avg_price,
               count(*) FILTER (WHERE p.source_count > 1) AS cross_checked
        FROM fuel_prices p WHERE {where}
        """,
        tuple(params),
    )[0]
    latest = query(
        f"""
        SELECT p.brand, p.fuel_code, f.fuel_name, p.price FROM fuel_prices p JOIN dim_fuel f USING (fuel_code)
        WHERE {where} AND p.price_date = (SELECT max(price_date) FROM fuel_prices p WHERE {where})
        ORDER BY p.price ASC LIMIT 1
        """,
        tuple(params) * 2,
    )
    change = query(
        f"""
        WITH d AS (SELECT p.price_date, avg(p.price) AS avg FROM fuel_prices p WHERE {where} GROUP BY 1)
        SELECT (SELECT avg FROM d ORDER BY price_date DESC LIMIT 1) - (SELECT avg FROM d ORDER BY price_date LIMIT 1)
               AS change
        """,
        tuple(params),
    )[0]
    return jsonify(summary=jsonable([summary])[0], cheapest=jsonable(latest)[0] if latest else None,
                   change=float(change["change"]) if change["change"] is not None else None)


@app.route("/api/series")
def series():
    where, params = filters()
    rows = query(
        f"SELECT p.price_date, p.brand, avg(p.price) AS price FROM fuel_prices p WHERE {where} "
        "GROUP BY 1, 2 ORDER BY 1, 2",
        tuple(params),
    )
    return jsonify(jsonable(rows))


@app.route("/api/brand-compare")
def brand_compare():
    where, params = filters()
    rows = query(
        f"""
        SELECT p.brand, round(avg(p.price), 2) AS price FROM fuel_prices p
        WHERE {where} AND p.price_date = (SELECT max(price_date) FROM fuel_prices p WHERE {where})
        GROUP BY 1 ORDER BY 2
        """,
        tuple(params) * 2,
    )
    return jsonify(jsonable(rows))


@app.route("/api/prices")
def prices():
    where, params = filters()
    page = max(int(request.args.get("page", 1)), 1)
    size = 25
    total = query(f"SELECT count(*) AS n FROM fuel_prices p WHERE {where}", tuple(params))[0]["n"]
    rows = query(
        f"""
        SELECT p.price_date, p.brand, f.fuel_name, p.price, p.source, p.sources
        FROM fuel_prices p JOIN dim_fuel f USING (fuel_code)
        WHERE {where} ORDER BY p.price_date DESC, f.fuel_name, p.brand LIMIT %s OFFSET %s
        """,
        tuple(params) + (size, (page - 1) * size),
    )
    return jsonify(total=total, page=page, size=size, rows=jsonable(rows))


@app.route("/api/status")
def pipeline_status():
    runs = query(
        """
        SELECT run_id, started_at, finished_at, status, rows_candidate, rows_rejected, rows_loaded, total_rows,
               dq_passed, note, extract(epoch FROM finished_at - started_at)::int AS seconds
        FROM pipeline_runs ORDER BY started_at DESC LIMIT 15
        """
    )
    latest = runs[0]["run_id"] if runs else None
    sources = query(
        """
        SELECT split_part(source, ':', 1) AS source, count(*) AS calls,
               count(*) FILTER (WHERE status = 'success') AS ok, sum(row_count) AS rows,
               max(error) AS error, max(logged_at) AS logged_at
        FROM source_runs WHERE run_id = %s GROUP BY 1 ORDER BY 1
        """,
        (latest,),
    )
    rejects = query(
        """
        SELECT reason, brand, count(*) AS rows, min(price_date) AS first_date, max(price_date) AS last_date
        FROM dq_rejects WHERE run_id = %s GROUP BY 1, 2 ORDER BY 3 DESC
        """,
        (latest,),
    )
    by_source = query("SELECT source, count(*) AS rows FROM stg_fuel_prices GROUP BY 1 ORDER BY 1")
    return jsonify(runs=jsonable(runs), sources=jsonable(sources), rejects=jsonable(rejects),
                   staged=jsonable(by_source), latest=latest)


@app.errorhandler(psycopg2.Error)
def db_error(exc):
    return jsonify(error="ยังไม่มีข้อมูล หรือเชื่อมต่อฐานข้อมูลไม่ได้ — รัน DAG fuel_price_pipeline ก่อน", detail=str(exc)), 503


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000, debug=True)
