"""
agri_price_pipeline.py
================
ระบบรวบรวมราคาขายส่งผัก-ผลไม้จากตลาดกลาง 3 แห่ง -> PostgreSQL -> เว็บ dashboard

โครงสร้าง DAG (ดู Graph view):

  start ─┬─ create_tables
         └─ [TaskGroup extract]  extract_talaadthai | extract_simummuang | extract_srimuang  (รันขนาน)
                     │  (trigger_rule=all_done: แหล่งใดล่มก็ยังไปต่อได้)
              transform_dedupe      รวมเป็น schema เดียว + ตัดรายการซ้ำ (ภายใน/ข้ามแหล่ง)
                     │
              quality_check         BranchPythonOperator ตรวจคุณภาพข้อมูล
               ├── load_prices      ผ่าน -> UPSERT ลง agri_prices + ตารางจับคู่สินค้าข้ามตลาด
               └── quarantine_batch ไม่ผ่าน -> เก็บลง agri_prices_rejected (ไม่แตะตารางหลัก)
                     │
              record_pipeline_run   บันทึกสถานะ run ลง pipeline_runs (ใช้แสดงหน้า /status บนเว็บ)

ระดับที่ทำ:
  ระดับ 1: extract -> transform -> load -> PostgreSQL
  ระดับ 2: 3 แหล่งรันขนานด้วย TaskGroup, schema เดียวกัน, UPSERT (ON CONFLICT) รันซ้ำแถวไม่เพิ่ม
  ระดับ 3: แหล่งล่มยังรวมต่อได้, ตัดรายการซ้ำข้ามแหล่ง, data quality check + branch
  โบนัส : ตาราง pipeline_runs + หน้าสถานะ pipeline บนเว็บ

Connection "postgres_target" ถูกสร้างอัตโนมัติจาก environment variable
AIRFLOW_CONN_POSTGRES_TARGET ใน docker-compose.yml (ไม่ต้องตั้งค่าใน UI)
"""

import json
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule

from sources import EXTRACTORS, SOURCES

POSTGRES_CONN_ID = "postgres_target"
SRIMUANG_DAYS_BACK = 7          # ศรีเมืองดึงย้อนหลังได้ -> backfill 7 วันล่าสุด
MIN_TOTAL_ROWS = 100            # ข้อกำหนดข้อสอบ: ข้อมูลรวม >= 100 รายการ
MIN_VALID_RATIO = 0.8           # แถวที่ผ่านเกณฑ์ต้องไม่น้อยกว่า 80%
MAX_REASONABLE_PRICE = 100_000  # ราคาเกินนี้ถือว่าผิดปกติ (บาท)
MAX_PRICE_AGE_DAYS = 30         # ราคาเก่ากว่านี้ถือว่าค้าง (เช่น ตลาดไทบางสินค้า prev เป็นปี 2023)

default_args = {
    "owner": "agri-price",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

CREATE_TABLES_SQL = """
CREATE TABLE IF NOT EXISTS agri_prices (
    id            SERIAL PRIMARY KEY,
    source        VARCHAR(30)  NOT NULL,
    market        VARCHAR(100) NOT NULL,
    product_id    VARCHAR(100) NOT NULL,
    product_name  VARCHAR(255) NOT NULL,
    product_key   VARCHAR(255) NOT NULL,
    category      VARCHAR(100),
    unit          VARCHAR(50),
    grade         VARCHAR(30)  NOT NULL DEFAULT '-',
    price_min     NUMERIC(12,2),
    price_max     NUMERIC(12,2),
    price_avg     NUMERIC(12,2),
    price_date    DATE         NOT NULL,
    ingested_at   TIMESTAMPTZ  DEFAULT NOW(),
    updated_at    TIMESTAMPTZ  DEFAULT NOW(),
    -- คีย์ธรรมชาติสำหรับ UPSERT: รันซ้ำวันเดิมจะ update ไม่ insert เพิ่ม
    CONSTRAINT uq_agri_price UNIQUE (source, product_id, grade, price_date)
);
CREATE INDEX IF NOT EXISTS idx_agri_prices_date ON agri_prices (price_date);
CREATE INDEX IF NOT EXISTS idx_agri_prices_key  ON agri_prices (product_key);

-- สินค้าเดียวกันข้ามตลาด (ผลจากการตัดรายการซ้ำข้ามแหล่ง): 1 แถวต่อ product_key
CREATE TABLE IF NOT EXISTS product_master (
    product_key   VARCHAR(255) PRIMARY KEY,
    display_name  VARCHAR(255) NOT NULL,
    category      VARCHAR(100),
    markets       TEXT[]       NOT NULL,
    market_count  INT          NOT NULL,
    updated_at    TIMESTAMPTZ  DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS agri_prices_rejected (
    id            SERIAL PRIMARY KEY,
    run_id        VARCHAR(255),
    source        VARCHAR(30),
    payload       JSONB,
    reason        TEXT,
    rejected_at   TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id          VARCHAR(255) PRIMARY KEY,
    started_at      TIMESTAMPTZ,
    finished_at     TIMESTAMPTZ DEFAULT NOW(),
    source_status   JSONB,
    rows_extracted  INT,
    rows_after_dedupe INT,
    rows_invalid    INT,
    rows_loaded     INT,
    quality_passed  BOOLEAN,
    quality_message TEXT,
    total_rows_in_db INT
);
"""

# หมวดหมู่สำหรับแหล่งที่ไม่มีหมวด (ศรีเมือง) / หมวดที่เป็นชื่อโซนตลาด (ตลาดไท)
FRUIT_WORDS = ["มะม่วง", "ส้ม", "กล้วย", "ทุเรียน", "มังคุด", "เงาะ", "ลำไย", "ลิ้นจี่", "ฝรั่ง", "ชมพู่",
               "แตงโม", "สับปะรด", "มะละกอ", "องุ่น", "แอปเปิ", "สาลี่", "แก้วมังกร", "อะโวคาโด", "อโวกาโด",
               "มะพร้าว", "ละมุด", "น้อยหน่า", "ขนุน", "ลองกอง", "เมล่อน", "สตรอว์", "กีวี", "พุทรา", "อินทผาลัม",
               "ลูกพลับ", "มะขาม", "ระกำ", "สละ", "ส้มโอ", "มะยงชิด", "เชอร์รี่"]
MEAT_WORDS = ["หมู", "ไก่", "เนื้อ", "ปลา", "กุ้ง", "หมึก", "ปู", "หอย", "กบ", "ไข่"]


def infer_category(name, category):
    if category in ("ผัก", "ผลไม้"):
        return category
    n = name or ""
    if any(w in n for w in MEAT_WORDS):
        return "เนื้อสัตว์/อาหารทะเล"
    if any(w in n for w in FRUIT_WORDS) or (category and "ผลไม้" in category):
        return "ผลไม้"
    if category and category not in ("ผัก",) and "ผัก" not in category and "สมุนไพร" not in category:
        return category
    return "ผัก"


# -----------------------------------------------------------------
# Extract: 1 task ต่อแหล่ง ใน TaskGroup (รันขนาน)
# คืนค่า dict ที่บอกสถานะเสมอ -> ถ้าแหล่งล่ม task จะ fail (เห็นสีแดงใน Grid)
# แต่ task ถัดไปใช้ trigger_rule=all_done จึงรวมข้อมูลจากแหล่งที่เหลือต่อได้
# -----------------------------------------------------------------
def extract_source(source, **context):
    # ใช้ทดสอบระดับ 3: Trigger DAG w/ config {"simulate_fail": ["talaadthai"]} เพื่อจำลองแหล่งล่ม
    if source in (context["params"].get("simulate_fail") or []):
        raise ConnectionError(f"จำลองแหล่ง {source} ล่ม (simulate_fail)")
    extract_fn, normalize_fn = EXTRACTORS[source]
    if source == "srimuang":
        raw = extract_fn(days_back=SRIMUANG_DAYS_BACK)
    else:
        raw = extract_fn()
    rows = normalize_fn(raw)
    if not rows:
        raise ValueError(f"{source}: ดึงข้อมูลได้ 0 แถว (โครงสร้างต้นทางอาจเปลี่ยน)")
    print(f"[{source}] raw={len(raw)} -> normalized={len(rows)} แถว")
    return rows  # ส่งผ่าน XCom (return_value)


# -----------------------------------------------------------------
# Transform: รวมทุกแหล่ง + ทำความสะอาด + ตัดรายการซ้ำ
# -----------------------------------------------------------------
def transform_dedupe(**context):
    ti = context["ti"]
    all_rows, source_status = [], {}
    for src in SOURCES:
        rows = ti.xcom_pull(task_ids=f"extract.extract_{src}")
        if rows:
            source_status[src] = {"status": "success", "rows": len(rows)}
            all_rows.extend(rows)
        else:
            source_status[src] = {"status": "failed", "rows": 0}
            print(f"!! แหล่ง {src} ล่ม/ไม่มีข้อมูล -> ข้ามไป รวมจากแหล่งที่เหลือ")

    # ทำความสะอาด + คำนวณ field ใหม่
    cleaned = []
    for r in all_rows:
        r = dict(r)
        r["product_name"] = (r.get("product_name") or "").strip()
        r["unit"] = (r.get("unit") or "").strip() or None
        r["category"] = infer_category(r["product_name"], (r.get("category") or "").strip() or None)
        try:
            pmin = float(r["price_min"]) if r.get("price_min") is not None else None
            pmax = float(r["price_max"]) if r.get("price_max") is not None else None
        except (TypeError, ValueError):
            pmin = pmax = None
        if pmin is not None and pmax is not None and pmin > pmax:
            pmin, pmax = pmax, pmin  # ต้นทางบางแถวกรอกสลับกัน
        if pmin in (None, 0) and pmax:
            pmin = pmax
        r["price_min"], r["price_max"] = pmin, pmax
        r["price_avg"] = round((pmin + pmax) / 2, 2) if pmin is not None and pmax is not None else None
        cleaned.append(r)

    # ตัดรายการซ้ำ (1) ภายในแหล่ง: คีย์ (source, product_id, grade, price_date)
    #   เช่น ตลาดไทแถว 'prev' ของวันนี้ซ้ำกับรอบก่อน / API คืนรายการซ้ำข้ามหน้า
    seen, deduped = {}, []
    for r in cleaned:
        key = (r["source"], r["product_id"], r["grade"], r["price_date"])
        if key not in seen:
            seen[key] = r
            deduped.append(r)
    dup_within = len(cleaned) - len(deduped)

    # ตัดรายการซ้ำ (2) ข้ามแหล่ง: สินค้าเดียวกันจากหลายตลาด (product_key เดียวกัน)
    #   ยุบเป็น 1 แถวใน product_master พร้อมรายชื่อตลาดที่มีขาย
    master = {}
    for r in deduped:
        m = master.setdefault(r["product_key"], {
            "product_key": r["product_key"], "display_name": r["product_name"],
            "category": r["category"], "markets": set()})
        m["markets"].add(r["market"])
        if len(r["product_name"]) < len(m["display_name"]):
            m["display_name"] = r["product_name"]  # ใช้ชื่อสั้นสุดเป็นชื่อกลาง
    master_rows = [{**m, "markets": sorted(m["markets"])} for m in master.values()]
    cross = sum(1 for m in master_rows if len(m["markets"]) > 1)

    print(f"รวม {len(cleaned)} แถว | ซ้ำภายในแหล่ง {dup_within} | เหลือ {len(deduped)} แถว")
    print(f"product_master {len(master_rows)} สินค้า | มีขายมากกว่า 1 ตลาด {cross} สินค้า")
    return {
        "rows": deduped,
        "master": master_rows,
        "source_status": source_status,
        "rows_extracted": len(cleaned),
        "dup_within": dup_within,
        "cross_market_products": cross,
    }


# -----------------------------------------------------------------
# Quality check: ตรวจคุณภาพแล้วแตกเส้นทาง (branch)
# -----------------------------------------------------------------
def validate_row(r):
    if not r.get("product_name"):
        return "ไม่มีชื่อสินค้า"
    if not r.get("price_date"):
        return "ไม่มีวันที่"
    if r.get("price_max") is None or r.get("price_min") is None:
        return "ไม่มีราคา"
    if r["price_max"] <= 0:
        return "ราคา <= 0"
    if r["price_max"] > MAX_REASONABLE_PRICE:
        return f"ราคาเกิน {MAX_REASONABLE_PRICE:,} บาท"
    age = (datetime.now().date() - datetime.fromisoformat(r["price_date"]).date()).days
    if age > MAX_PRICE_AGE_DAYS:
        return f"ราคาค้าง (เก่ากว่า {MAX_PRICE_AGE_DAYS} วัน)"
    if age < -1:
        return "วันที่อยู่ในอนาคต"
    return None


def quality_check(**context):
    ti = context["ti"]
    data = ti.xcom_pull(task_ids="transform_dedupe")
    rows = data["rows"]
    valid, invalid = [], []
    for r in rows:
        reason = validate_row(r)
        (invalid if reason else valid).append({**r, "_reason": reason} if reason else r)

    ok_sources = [s for s, v in data["source_status"].items() if v["status"] == "success"]
    ratio = len(valid) / len(rows) if rows else 0
    checks = {
        "มีอย่างน้อย 1 แหล่งที่สำเร็จ": len(ok_sources) >= 1,
        f"แถวที่ใช้ได้ >= {MIN_TOTAL_ROWS}": len(valid) >= MIN_TOTAL_ROWS,
        f"สัดส่วนแถวที่ผ่าน >= {MIN_VALID_RATIO:.0%}": ratio >= MIN_VALID_RATIO,
    }
    passed = all(checks.values())
    message = "; ".join(f"{k}: {'ผ่าน' if v else 'ไม่ผ่าน'}" for k, v in checks.items())
    message += f" (valid={len(valid)}, invalid={len(invalid)}, ratio={ratio:.1%})"
    print(message)

    ti.xcom_push(key="valid_rows", value=valid)
    ti.xcom_push(key="invalid_rows", value=invalid)
    ti.xcom_push(key="quality", value={"passed": passed, "message": message})
    return "load_prices" if passed else "quarantine_batch"


# -----------------------------------------------------------------
# Load (ผ่าน QC): UPSERT -> รันซ้ำกี่ครั้งจำนวนแถวไม่เพิ่ม
# -----------------------------------------------------------------
UPSERT_SQL = """
INSERT INTO agri_prices
    (source, market, product_id, product_name, product_key, category, unit, grade,
     price_min, price_max, price_avg, price_date)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (source, product_id, grade, price_date) DO UPDATE SET
    product_name = EXCLUDED.product_name,
    product_key  = EXCLUDED.product_key,
    category     = EXCLUDED.category,
    unit         = EXCLUDED.unit,
    price_min    = EXCLUDED.price_min,
    price_max    = EXCLUDED.price_max,
    price_avg    = EXCLUDED.price_avg,
    updated_at   = NOW();
"""

UPSERT_MASTER_SQL = """
INSERT INTO product_master (product_key, display_name, category, markets, market_count)
VALUES (%s, %s, %s, %s, %s)
ON CONFLICT (product_key) DO UPDATE SET
    display_name = EXCLUDED.display_name,
    category     = EXCLUDED.category,
    markets      = (SELECT ARRAY(SELECT DISTINCT unnest(product_master.markets || EXCLUDED.markets) ORDER BY 1)),
    market_count = (SELECT COUNT(DISTINCT m) FROM unnest(product_master.markets || EXCLUDED.markets) m),
    updated_at   = NOW();
"""


def _save_rejects(cur, run_id, rows):
    for r in rows:
        reason = r.pop("_reason", None) or "batch ไม่ผ่าน quality check"
        cur.execute(
            "INSERT INTO agri_prices_rejected (run_id, source, payload, reason) VALUES (%s, %s, %s, %s)",
            (run_id, r.get("source"), json.dumps(r, ensure_ascii=False, default=str), reason),
        )


def load_prices(**context):
    ti = context["ti"]
    valid = ti.xcom_pull(task_ids="quality_check", key="valid_rows")
    invalid = ti.xcom_pull(task_ids="quality_check", key="invalid_rows") or []
    master = ti.xcom_pull(task_ids="transform_dedupe")["master"]
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    conn = hook.get_conn()
    with conn, conn.cursor() as cur:
        cur.executemany(UPSERT_SQL, [
            (r["source"], r["market"], r["product_id"], r["product_name"], r["product_key"],
             r["category"], r["unit"], r["grade"], r["price_min"], r["price_max"], r["price_avg"],
             r["price_date"]) for r in valid])
        cur.executemany(UPSERT_MASTER_SQL, [
            (m["product_key"], m["display_name"], m["category"], m["markets"], len(m["markets"]))
            for m in master])
        _save_rejects(cur, context["run_id"], invalid)  # แถวเสียรายตัว แยกเก็บไว้ตรวจ
    total = hook.get_first("SELECT COUNT(*) FROM agri_prices")[0]
    print(f"UPSERT {len(valid)} แถว | rejected {len(invalid)} แถว | ทั้งตารางมี {total} แถว")
    return len(valid)


def quarantine_batch(**context):
    ti = context["ti"]
    valid = ti.xcom_pull(task_ids="quality_check", key="valid_rows") or []
    invalid = ti.xcom_pull(task_ids="quality_check", key="invalid_rows") or []
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    conn = hook.get_conn()
    with conn, conn.cursor() as cur:
        _save_rejects(cur, context["run_id"], valid + invalid)
    print(f"!! QC ไม่ผ่าน: กัก {len(valid) + len(invalid)} แถวไว้ใน agri_prices_rejected ไม่โหลดเข้าตารางหลัก")
    return 0


# -----------------------------------------------------------------
# บันทึกสถานะ run (หน้า /status บนเว็บอ่านจากตารางนี้)
# -----------------------------------------------------------------
def record_pipeline_run(**context):
    ti = context["ti"]
    data = ti.xcom_pull(task_ids="transform_dedupe") or {}
    quality = ti.xcom_pull(task_ids="quality_check", key="quality") or {"passed": False, "message": "ไม่ได้ตรวจ"}
    invalid = ti.xcom_pull(task_ids="quality_check", key="invalid_rows") or []
    loaded = ti.xcom_pull(task_ids="load_prices") or 0

    # สถานะจริงของ extract task แต่ละตัว (success / failed / upstream_failed)
    dag_run = context["dag_run"]
    source_status = data.get("source_status", {})
    for src in SOURCES:
        t = dag_run.get_task_instance(f"extract.extract_{src}")
        source_status.setdefault(src, {"rows": 0})
        source_status[src]["state"] = t.state if t else None

    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    total = hook.get_first("SELECT COUNT(*) FROM agri_prices")[0]
    hook.run(
        """
        INSERT INTO pipeline_runs (run_id, started_at, finished_at, source_status, rows_extracted,
            rows_after_dedupe, rows_invalid, rows_loaded, quality_passed, quality_message, total_rows_in_db)
        VALUES (%s, %s, NOW(), %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (run_id) DO UPDATE SET finished_at = NOW(), source_status = EXCLUDED.source_status,
            rows_extracted = EXCLUDED.rows_extracted, rows_after_dedupe = EXCLUDED.rows_after_dedupe,
            rows_invalid = EXCLUDED.rows_invalid, rows_loaded = EXCLUDED.rows_loaded,
            quality_passed = EXCLUDED.quality_passed, quality_message = EXCLUDED.quality_message,
            total_rows_in_db = EXCLUDED.total_rows_in_db;
        """,
        parameters=(
            context["run_id"], dag_run.start_date, json.dumps(source_status, ensure_ascii=False),
            data.get("rows_extracted", 0), len(data.get("rows", [])), len(invalid), loaded,
            quality["passed"], quality["message"], total,
        ),
    )
    print(f"บันทึก pipeline_runs: {context['run_id']} | ตารางหลักมี {total} แถว")


with DAG(
    dag_id="agri_price_pipeline",
    default_args=default_args,
    description="รวมราคาผัก-ผลไม้ขายส่งจาก ตลาดไท / สี่มุมเมือง / ศรีเมือง -> PostgreSQL",
    schedule="0 9 * * *",            # ทุกวัน 09:00 UTC (16:00 เวลาไทย) หลังตลาดประกาศราคา
    start_date=datetime(2026, 10, 1),
    catchup=False,
    max_active_runs=1,
    params={"simulate_fail": Param([], type="array", description="รายชื่อแหล่งที่จะจำลองว่าล่ม เช่น [\"talaadthai\"]")},
    tags=["final-exam", "agri-price"],
) as dag:

    start = EmptyOperator(task_id="start")

    create_tables = PostgresOperator(
        task_id="create_tables",
        postgres_conn_id=POSTGRES_CONN_ID,
        sql=CREATE_TABLES_SQL,
    )

    with TaskGroup(group_id="extract", tooltip="ดึงข้อมูล 3 ตลาดพร้อมกัน") as extract_group:
        for src in SOURCES:
            PythonOperator(
                task_id=f"extract_{src}",
                python_callable=extract_source,
                op_kwargs={"source": src},
                execution_timeout=timedelta(minutes=5),
            )

    transform = PythonOperator(
        task_id="transform_dedupe",
        python_callable=transform_dedupe,
        trigger_rule=TriggerRule.ALL_DONE,   # แหล่งใดล่ม ก็ยังรวมข้อมูลจากแหล่งที่เหลือ
    )

    qc = BranchPythonOperator(
        task_id="quality_check",
        python_callable=quality_check,
    )

    load = PythonOperator(task_id="load_prices", python_callable=load_prices)
    quarantine = PythonOperator(task_id="quarantine_batch", python_callable=quarantine_batch)

    record = PythonOperator(
        task_id="record_pipeline_run",
        python_callable=record_pipeline_run,
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,  # รอสาขาที่ถูกเลือก (อีกสาขา skipped)
    )

    start >> [create_tables, extract_group]
    [create_tables, extract_group] >> transform >> qc >> [load, quarantine] >> record
