# ระบบรวบรวมราคาขายส่งผัก-ผลไม้จาก 3 ตลาดกลาง

ข้อสอบปลายภาค 02739452 แพลตฟอร์มข้อมูลขนาดใหญ่และการวิเคราะห์ (ภาคต้น 2569)

- **หมวด:** ราคาสินค้าเกษตร
- **ขอบเขต:** ราคาขายส่งผักและผลไม้รายวันจากตลาดกลางค้าส่ง 3 แห่ง (ปทุมธานี 2 แห่ง และราชบุรี 1 แห่ง) เพื่อเปรียบเทียบราคาสินค้าเดียวกันข้ามตลาด
- **Pipeline:** Apache Airflow DAG → PostgreSQL → เว็บ dashboard (Flask + Chart.js)

## แหล่งข้อมูล

| # | แหล่ง | วิธีดึง | ข้อมูลที่ได้ |
|---|---|---|---|
| 1 | [ตลาดไท](https://talaadthai.com/products) | **Web scraping**: อ่านรายชื่อหน้าสินค้าจาก `products-sitemap.xml` แล้วดึงหน้า HTML `/products/<slug>` **ทุกหน้า (~1,500 หน้า)** ด้วย `requests` + `BeautifulSoup` (พร้อมกันไม่เกิน 4 หน้า) แกะข้อมูลจาก `<script id="__NEXT_DATA__">` ใช้เวลาประมาณ 5 นาที | ราคาวันล่าสุด + วันก่อนหน้า ของทุกสินค้า ตัดสินค้าที่ราคาไม่อัปเดตเกิน 30 วัน (~390 แถว) |
| 2 | [ตลาดสี่มุมเมือง](https://www.simummuangmarket.com/pricing) | JSON API ที่หน้าเว็บเรียกใช้ (`api.simummuangmarket.com/api/app/products`) แบ่งหน้า 100 รายการ | ราคาประมาณ 967 สินค้า แยกตามขนาด เล็ก/กลาง/ใหญ่ |
| 3 | [ตลาดศรีเมือง ราชบุรี](https://www.taladsrimuang.com/app/) | JSON API (`taladsrimuang.com:5100/price-by-filter`) | ราคารายวันประมาณ 246 สินค้า ย้อนหลัง 7 วัน |

รวมประมาณ **3,100 แถวต่อการรัน 1 ครั้ง** (ข้อกำหนดขั้นต่ำคือ 100 แถว)

**robots.txt และ PDPA**
- ตลาดไทห้ามเฉพาะไฟล์ `*.json` และ `*.js` ระบบนี้ดึงเฉพาะ sitemap `.xml` และหน้า HTML `/products/...` ซึ่งได้รับอนุญาต
- ตลาดสี่มุมเมืองและศรีเมืองไม่มี robots.txt ที่ห้ามไว้
- ระบบหน่วงเวลา 0.5 วินาทีระหว่าง request และระบุ User-Agent ของโปรเจกต์
- ไม่ใช้ Selenium
- เก็บเฉพาะชื่อสินค้า ราคา หน่วย และวันที่ **ไม่เก็บข้อมูลส่วนบุคคล** (เช่น ชื่อผู้ค้าหรือเบอร์โทร)

## โครงสร้าง DAG `agri_price_pipeline`

```
start ─┬─ create_tables
       └─ [TaskGroup extract] extract_talaadthai | extract_simummuang | extract_srimuang  (รันขนาน)
                │ trigger_rule = all_done  → ถ้าแหล่งใดล่ม ก็ยังรวมข้อมูลจากแหล่งที่เหลือต่อได้
          transform_dedupe     รวมเป็น schema เดียว + ทำความสะอาด + ตัดรายการซ้ำ
                │
          quality_check        (BranchPythonOperator)
           ├─ load_prices       ผ่าน   → UPSERT agri_prices + product_master
           └─ quarantine_batch  ไม่ผ่าน → เก็บลง agri_prices_rejected
                │
          record_pipeline_run  บันทึกสถานะลง pipeline_runs (ใช้แสดงในหน้า /status)
```

| ระดับ | สิ่งที่ทำ |
|---|---|
| 1 | ETL ครบ: extract → transform → load ลง PostgreSQL และเว็บแสดงตารางกับกราฟ |
| 2 | 3 แหล่งรันขนานใน `TaskGroup` และใช้ schema เดียวกัน `UPSERT` ใช้ `ON CONFLICT (source, product_id, grade, price_date)` จึงรันซ้ำแล้วจำนวนแถวไม่เพิ่ม เว็บมี KPI 5 ตัวและตัวกรอง 5 แบบ |
| 3 | `trigger_rule=all_done` ทำให้แหล่งที่ล่มไม่หยุดทั้ง DAG มีการตัดรายการซ้ำทั้งภายในแหล่งและข้ามแหล่ง (`product_key` → ตาราง `product_master`) และตรวจคุณภาพข้อมูลแล้วแตกเส้นทางด้วย `BranchPythonOperator` |
| โบนัส | หน้าสถานะ pipeline บนเว็บ (`/status`) |

**การตัดรายการซ้ำ**
1. **ภายในแหล่ง:** ใช้คีย์ `(source, product_id, grade, price_date)`
2. **ข้ามแหล่ง:** แปลงชื่อสินค้าเป็น `product_key` โดยตัดวงเล็บ เบอร์/ขนาด ตัวเลข และช่องว่าง เช่น `มะนาว 500 ลูก/กระสอบ` → `มะนาว` สินค้าที่มี key เดียวกันจากหลายตลาดจะถูกรวมเป็น 1 แถวใน `product_master` พร้อมรายชื่อตลาดที่มีขาย แล้วใช้ตารางนี้ทำกราฟเปรียบเทียบราคาข้ามตลาด

**เกณฑ์ quality check** (ไม่ผ่านข้อใดข้อหนึ่ง → `quarantine_batch`)
- มีอย่างน้อย 1 แหล่งที่ดึงสำเร็จ
- แถวที่ใช้ได้ไม่น้อยกว่า 100 แถว
- แถวที่ผ่านเกณฑ์ไม่น้อยกว่า 80%

แถวที่ไม่ผ่านรายตัว (ไม่มีชื่อ ไม่มีราคา ราคา ≤ 0 ราคาเกิน 100,000 หรือราคาค้างเก่ากว่า 30 วัน) จะถูกแยกไปเก็บที่ `agri_prices_rejected`

**ทดสอบกรณีแหล่งล่ม:** ใน Airflow UI ให้กด Trigger DAG w/ config แล้วใส่ `{"simulate_fail": ["talaadthai"]}`

## วิธีรัน

ต้องมี Docker Desktop

```bash
cp .env.example .env              # Windows: copy .env.example .env  แล้วแก้รหัสผ่านใน .env
docker compose up airflow-init    # ครั้งแรกครั้งเดียว: สร้าง metadata DB + user
docker compose up -d --build
```

1. เปิด Airflow ที่ http://localhost:8080 (user และ password ตามที่ตั้งใน `.env`)
2. Unpause DAG `agri_price_pipeline` แล้วกด Trigger
3. เปิด Dashboard ที่ http://localhost:8050 และหน้าสถานะที่ http://localhost:8050/status
4. ต่อ PostgreSQL จากเครื่อง host ได้ที่ `localhost:5433` (db, user และ password ตาม `.env`)

ไม่ต้องตั้ง Airflow Connection เอง เพราะ `docker-compose.yml` สร้าง `postgres_target` ให้อัตโนมัติผ่าน `AIRFLOW_CONN_POSTGRES_TARGET`

**ตรวจว่า UPSERT ทำงาน:** Trigger ซ้ำ 2 ครั้ง แล้วดูว่า `total_rows_in_db` ในหน้า `/status` ไม่เพิ่มขึ้น (หากวันที่ราคายังเป็นวันเดิม)

## โครงสร้างไฟล์

```
dags/
  agri_price_pipeline.py   DAG หลัก
  sources.py               ฟังก์ชัน extract + normalize ของแต่ละแหล่ง (ทดสอบนอก Airflow ได้)
  certs/                   intermediate CA ของ RapidSSL (ใช้กับ API ศรีเมือง)
web/
  app.py, templates/       Flask dashboard (อ่าน PostgreSQL โดยตรง)
  Dockerfile, requirements.txt
docker-compose.yml         Airflow (CeleryExecutor) + Postgres ปลายทาง + web
Dockerfile                 image ของ Airflow + requirements.txt
requirements.txt           library ที่ DAG ใช้
.env.example               ตัวอย่างค่า environment (ไม่ commit .env จริง)
```
