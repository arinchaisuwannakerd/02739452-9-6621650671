"""
เว็บ dashboard ราคาผัก-ผลไม้ขายส่ง 3 ตลาด (Flask)
อ่านข้อมูลจาก PostgreSQL (postgres_target) โดยตรงทุกครั้งที่โหลดหน้า

  /        dashboard: KPI, ตัวกรอง, กราฟ, ตารางข้อมูล
  /status  หน้าสถานะ pipeline (อ่านจากตาราง pipeline_runs ที่ DAG บันทึก)
"""

import os

import psycopg2
import psycopg2.extras
from flask import Flask, render_template, request

app = Flask(__name__)


def get_conn():
    return psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=os.getenv("DB_PORT", "5433"),
        dbname=os.getenv("DB_NAME", "etl_db"),
        user=os.getenv("DB_USER", "etluser"),
        password=os.getenv("DB_PASSWORD", "etlpass"),
    )


def query(sql, params=None):
    with get_conn() as conn, conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(sql, params or ())
        return cur.fetchall()


def table_exists():
    return query("SELECT to_regclass('public.agri_prices') IS NOT NULL AS ok")[0]["ok"]


@app.route("/")
def dashboard():
    if not table_exists():
        return render_template("empty.html")

    markets = [r["market"] for r in query("SELECT DISTINCT market FROM agri_prices ORDER BY 1")]
    categories = [r["category"] for r in query("SELECT DISTINCT category FROM agri_prices WHERE category IS NOT NULL ORDER BY 1")]
    dates = [r["d"] for r in query("SELECT DISTINCT price_date::text AS d FROM agri_prices ORDER BY 1 DESC")]

    f = {
        "market": request.args.get("market", ""),
        "category": request.args.get("category", ""),
        "date": request.args.get("date", ""),
        "q": request.args.get("q", "").strip(),
        "product": request.args.get("product", "").strip(),
    }

    # สร้าง WHERE จากตัวกรอง (ใช้ parameter กัน SQL injection)
    where, params = ["1=1"], []
    if f["market"]:
        where.append("market = %s"); params.append(f["market"])
    if f["category"]:
        where.append("category = %s"); params.append(f["category"])
    if f["date"]:
        where.append("price_date = %s"); params.append(f["date"])
    else:
        # ค่าเริ่มต้น: ราคาล่าสุดของแต่ละตลาด
        where.append("price_date = (SELECT MAX(price_date) FROM agri_prices p2 WHERE p2.source = agri_prices.source)")
    if f["q"]:
        where.append("product_name ILIKE %s"); params.append(f"%{f['q']}%")
    w = " AND ".join(where)

    kpi = query(f"""
        SELECT COUNT(*) AS rows, COUNT(DISTINCT product_key) AS products,
               COUNT(DISTINCT market) AS markets, ROUND(AVG(price_avg), 2) AS avg_price,
               MAX(price_date)::text AS latest
        FROM agri_prices WHERE {w}""", params)[0]
    kpi_total = query("""
        SELECT COUNT(*) AS rows, COUNT(DISTINCT price_date) AS days,
               (SELECT COUNT(*) FROM product_master WHERE market_count > 1) AS cross_products
        FROM agri_prices""")[0]

    rows = query(f"""
        SELECT market, product_name, category, grade, unit, price_min, price_max, price_avg, price_date::text AS price_date
        FROM agri_prices WHERE {w}
        ORDER BY product_name, market, grade LIMIT 500""", params)

    by_market = query(f"""
        SELECT market, COUNT(*) AS n FROM agri_prices WHERE {w} GROUP BY market ORDER BY market""", params)
    by_category = query(f"""
        SELECT category, COUNT(*) AS n FROM agri_prices WHERE {w} GROUP BY category ORDER BY n DESC""", params)

    # เปรียบเทียบราคาเฉลี่ย (ต่อกิโลกรัม) ของสินค้าที่มีขายหลายตลาด ณ ข้อมูลล่าสุด
    compare = query("""
        WITH latest AS (
            SELECT DISTINCT ON (source, product_key) source, market, product_key, price_avg
            FROM agri_prices WHERE unit = 'กิโลกรัม' AND price_avg > 0
            ORDER BY source, product_key, price_date DESC, grade
        )
        SELECT l.product_key, pm.display_name, l.market, l.price_avg
        FROM latest l JOIN product_master pm USING (product_key)
        WHERE l.product_key IN (
            SELECT product_key FROM latest GROUP BY product_key HAVING COUNT(DISTINCT market) >= 2
            ORDER BY COUNT(*) DESC, product_key LIMIT 12)
        ORDER BY l.product_key, l.market""")

    product_options = query("""
        SELECT product_key, display_name, market_count FROM product_master
        ORDER BY market_count DESC, display_name LIMIT 400""")
    product = f["product"] or (compare[0]["product_key"] if compare else "")
    trend = query("""
        SELECT market, price_date::text AS d, ROUND(AVG(price_avg), 2) AS price
        FROM agri_prices WHERE product_key = %s
        GROUP BY market, price_date ORDER BY price_date""", (product,)) if product else []

    return render_template(
        "index.html", f=f, markets=markets, categories=categories, dates=dates,
        kpi=kpi, kpi_total=kpi_total, rows=rows, by_market=by_market, by_category=by_category,
        compare=compare, trend=trend, product=product, product_options=product_options,
    )


@app.route("/status")
def status():
    if not table_exists():
        return render_template("empty.html")
    runs = query("""
        SELECT run_id, started_at, finished_at, source_status, rows_extracted, rows_after_dedupe,
               rows_invalid, rows_loaded, quality_passed, quality_message, total_rows_in_db
        FROM pipeline_runs ORDER BY finished_at DESC LIMIT 30""")
    freshness = query("""
        SELECT market, MAX(price_date)::text AS latest, COUNT(*) AS rows, MAX(updated_at) AS updated
        FROM agri_prices GROUP BY market ORDER BY market""")
    rejected = query("""
        SELECT source, reason, COUNT(*) AS n FROM agri_prices_rejected
        GROUP BY source, reason ORDER BY n DESC LIMIT 20""")
    return render_template("status.html", runs=runs, freshness=freshness, rejected=rejected)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8050, debug=False)
