"""
sources.py
================
ฟังก์ชันดึงข้อมูล (extract) และแปลงเป็น schema กลาง (normalize) ของแต่ละแหล่ง
แยกออกจากไฟล์ DAG เพื่อให้ทดสอบนอก Airflow ได้ (python dags/sources.py)

แหล่งข้อมูล (ราคาขายส่งผัก-ผลไม้ จากตลาดกลาง 3 แห่ง):
  1) ตลาดไท (talaadthai.com)              -> web scraping หน้า HTML /products
  2) ตลาดสี่มุมเมือง (simummuangmarket.com) -> JSON API ที่หน้าเว็บ /pricing เรียกใช้
  3) ตลาดศรีเมือง ราชบุรี (taladsrimuang.com) -> JSON API ที่หน้าเว็บ /app เรียกใช้

schema กลางของทุกแหล่ง (1 dict = 1 แถว):
  source, market, product_id, product_name, product_key, category, unit, grade,
  price_min, price_max, price_date (YYYY-MM-DD)
ไม่มีข้อมูลส่วนบุคคลใดๆ (ไม่เก็บชื่อผู้ค้า/เบอร์โทร) ตาม PDPA
"""

import json
import os
import re
import tempfile
import time
from datetime import datetime, timedelta, timezone

import certifi
import requests
from bs4 import BeautifulSoup

USER_AGENT = "Mozilla/5.0 (compatible; KU-02739452-student-project; agri-price-pipeline)"
HEADERS = {"User-Agent": USER_AGENT, "Accept-Language": "th,en;q=0.8"}
TIMEOUT = 30
POLITE_DELAY = 0.5  # หน่วงระหว่าง request เพื่อไม่ให้ภาระกับเว็บต้นทาง

BKK = timezone(timedelta(hours=7))

SOURCES = {
    "talaadthai": "ตลาดไท",
    "simummuang": "ตลาดสี่มุมเมือง",
    "srimuang": "ตลาดศรีเมือง",
}


def normalize_name(name):
    """
    ทำ product_key ไว้จับคู่สินค้าเดียวกันข้ามตลาด / ตัดรายการซ้ำ
    เช่น 'มะนาว 500 ลูก/กระสอบ' -> 'มะนาว', 'ผักบุ้งจีน ' -> 'ผักบุ้งจีน'
    """
    s = (name or "").strip().lower()
    s = re.sub(r"\(.*?\)", " ", s)          # ตัดวงเล็บ (เกรด/สายพันธุ์)
    s = re.split(r"\s[–-]\s|เบอร์|\d", s)[0]  # ตัดส่วนขยาย เช่น ' – เบอร์ใหญ่', ตัวเลขขนาด
    s = re.sub(r"\s+", "", s)
    return s or (name or "").strip()


def _bkk_date(iso_str):
    """แปลงเวลา ISO (UTC) เป็นวันที่ตามเวลาไทย"""
    dt = datetime.fromisoformat(iso_str.replace("Z", "+00:00"))
    return dt.astimezone(BKK).date().isoformat()


# -----------------------------------------------------------------
# 1) ตลาดไท: web scraping (BeautifulSoup) จากหน้า HTML ทีละหน้าสินค้า
#    robots.txt ห้ามเฉพาะ *.json / *.js -> หน้า /products/... และ sitemap .xml ดึงได้
#    รายชื่อหน้าสินค้าได้จาก products-sitemap.xml (~1,500 หน้า)
#    ข้อมูลราคาฝังอยู่ใน <script id="__NEXT_DATA__"> ของแต่ละหน้า HTML
# -----------------------------------------------------------------
TALAADTHAI = "https://talaadthai.com"
TALAADTHAI_WORKERS = 4        # ดึงพร้อมกันไม่เกิน 4 หน้า
MAX_PRICE_AGE_DAYS = 30       # ราคาที่ไม่อัปเดตเกินนี้ถือว่าสินค้าไม่มีขายแล้ว ไม่นำเข้า


def _next_data(html):
    soup = BeautifulSoup(html, "html.parser")
    tag = soup.find("script", id="__NEXT_DATA__")
    if tag is None:
        raise ValueError("ไม่พบ __NEXT_DATA__ (โครงสร้างหน้าเว็บอาจเปลี่ยน)")
    return json.loads(tag.string)["props"]["pageProps"]


def talaadthai_product_urls():
    """อ่าน sitemap index -> sitemap ย่อย -> URL หน้าสินค้าทั้งหมด"""
    index = requests.get(f"{TALAADTHAI}/products-sitemap.xml", headers=HEADERS, timeout=TIMEOUT)
    index.raise_for_status()
    urls = []
    for sm in re.findall(r"<loc>\s*([^<]+?)\s*</loc>", index.text):
        r = requests.get(sm, headers=HEADERS, timeout=TIMEOUT)
        r.raise_for_status()
        urls += re.findall(r"<loc>\s*([^<]+?)\s*</loc>", r.text)
        time.sleep(POLITE_DELAY / 5)
    return sorted(set(urls))


def _fetch_product(url):
    """ดึง 1 หน้าสินค้า; หน้าที่ error ข้ามไป (ไม่ทำให้ทั้งแหล่งล้ม)"""
    time.sleep(POLITE_DELAY / 2)
    try:
        r = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
        if r.status_code != 200:
            return None
        return _next_data(r.text).get("product")
    except Exception as e:  # noqa: BLE001
        print(f"[talaadthai] ข้าม {url}: {e}")
        return None


def extract_talaadthai(max_products=None):
    from concurrent.futures import ThreadPoolExecutor

    urls = talaadthai_product_urls()
    if max_products:
        urls = urls[:max_products]
    with ThreadPoolExecutor(max_workers=TALAADTHAI_WORKERS) as ex:
        products = [p for p in ex.map(_fetch_product, urls) if p]
    print(f"[talaadthai] sitemap {len(urls)} หน้า -> ดึงสำเร็จ {len(products)} หน้า")
    if not products:
        raise ValueError("talaadthai: ดึงหน้าสินค้าไม่สำเร็จเลย")
    return products


def normalize_talaadthai(raw_items):
    rows, stale = [], 0
    today = datetime.now(BKK).date()
    for it in raw_items:
        name = (it.get("title") or {}).get("th")
        cats = it.get("categories") or []
        category = (cats[0].get("title") or {}).get("th", "").strip() if cats else None
        snap = (((it.get("pricingData") or {}).get("latestPriceDiffProductSnapShot") or {}).get("data")) or {}
        base = {
            "source": "talaadthai",
            "market": SOURCES["talaadthai"],
            "product_id": str(it.get("slug") or it.get("id")),
            "product_name": name,
            "product_key": normalize_name(name),
            "category": category,
            "unit": it.get("unit"),
            "grade": "-",
        }
        # ราคาวันล่าสุด + วันก่อนหน้า ต้นทางใช้ 'YYYY-MM-DDT23:59:59Z' แทนวันที่ จึงตัดเฉพาะส่วนวันที่
        for key in ("current", "prev"):
            p = snap.get(key)
            if not (p and p.get("date")) or p.get("maxPrice") is None:
                continue
            d = p["date"][:10]
            if (today - datetime.fromisoformat(d).date()).days > MAX_PRICE_AGE_DAYS:
                stale += 1  # สินค้าที่เลิกอัปเดตราคา (บางตัวค้างตั้งแต่ปี 2023)
                continue
            rows.append({**base, "price_min": p.get("minPrice"), "price_max": p.get("maxPrice"), "price_date": d})
    print(f"[talaadthai] ตัดราคาค้างเกิน {MAX_PRICE_AGE_DAYS} วัน {stale} แถว")
    return rows


# -----------------------------------------------------------------
# 2) ตลาดสี่มุมเมือง: JSON API (แบ่งหน้า สูงสุด 100 รายการ/หน้า)
# -----------------------------------------------------------------
def extract_simummuang(max_pages=20):
    base = "https://api.simummuangmarket.com/api/app/products"
    items, page = [], 1
    while page <= max_pages:
        resp = requests.get(base, params={"lang": "th", "page": page, "limit": 100},
                            headers=HEADERS, timeout=TIMEOUT)
        resp.raise_for_status()
        data = resp.json()["data"]
        items.extend(data["data"])
        if page >= data.get("totalPages", 1):
            break
        page += 1
        time.sleep(POLITE_DELAY)
    return items


def normalize_simummuang(raw_items):
    rows = []
    size_th = {"small": "เล็ก", "medium": "กลาง", "large": "ใหญ่"}
    for it in raw_items:
        name = (it.get("th") or {}).get("name")
        price = it.get("price") or {}
        if not price.get("action_date"):
            continue
        base = {
            "source": "simummuang",
            "market": SOURCES["simummuang"],
            "product_id": str(it.get("code") or it.get("_id")),
            "product_name": name,
            "product_key": normalize_name(name),
            "category": ((it.get("prod_category_id") or {}).get("th") or {}).get("name"),
            "unit": ((it.get("prod_unit_id") or {}).get("th") or {}).get("name"),
            "price_date": _bkk_date(price["action_date"]),
        }
        # 1 สินค้ามีราคาได้ 3 ขนาด -> แตกเป็นแถวละ grade (เฉพาะขนาดที่มีราคา)
        for size, label in size_th.items():
            p = price.get(size) or {}
            if (p.get("max") or 0) > 0 or (p.get("min") or 0) > 0:
                rows.append({**base, "grade": label, "price_min": p.get("min"), "price_max": p.get("max")})
    return rows


# -----------------------------------------------------------------
# 3) ตลาดศรีเมือง ราชบุรี: JSON API (ราคารายวัน ย้อนหลังได้ตาม price-dates)
# -----------------------------------------------------------------
SRIMUANG_API = "https://taladsrimuang.com:5100"
# เซิร์ฟเวอร์ศรีเมืองส่ง certificate มาไม่ครบ chain (ขาด intermediate CA)
# จึงรวม CA ของ certifi + RapidSSL intermediate (ดาวน์โหลดจาก AIA ของ DigiCert)
# เป็น bundle ใหม่ แทนการปิดการตรวจ SSL (verify=False)
_INTERMEDIATE = os.path.join(os.path.dirname(__file__), "certs", "rapidssl_tls_rsa_ca_g1.pem")


def _srimuang_ca_bundle():
    bundle = os.path.join(tempfile.gettempdir(), "srimuang_ca_bundle.pem")
    with open(bundle, "w", encoding="utf-8") as out:
        out.write(open(certifi.where(), encoding="utf-8").read())
        out.write("\n")
        out.write(open(_INTERMEDIATE, encoding="utf-8").read())
    return bundle


def extract_srimuang(days_back=7):
    ca = _srimuang_ca_bundle()
    resp = requests.get(f"{SRIMUANG_API}/price-dates", headers=HEADERS, timeout=TIMEOUT, verify=ca)
    resp.raise_for_status()
    dates = [d["date"] for d in resp.json()][:days_back]
    items = []
    for d in dates:
        r = requests.get(f"{SRIMUANG_API}/price-by-filter",
                         params={"date": d, "building": "", "search": ""},
                         headers=HEADERS, timeout=TIMEOUT, verify=ca)
        r.raise_for_status()
        day_items = r.json()
        if isinstance(day_items, list):
            for x in day_items:
                x["_date"] = d
            items.extend(day_items)
        time.sleep(POLITE_DELAY)
    return items


def normalize_srimuang(raw_items):
    rows = []
    for it in raw_items:
        name = (it.get("name_pro") or "").strip()
        price = it.get("price_pro")  # price_pro = ราคาวันนั้น, price_pro2 = ราคาวันก่อนหน้า
        rows.append({
            "source": "srimuang",
            "market": SOURCES["srimuang"],
            "product_id": str(it.get("id")),
            "product_name": name,
            "product_key": normalize_name(name),
            "category": None,  # API ไม่มีหมวดหมู่ -> เติมใน transform
            "unit": it.get("unitname"),
            "grade": "-",
            "price_min": price,
            "price_max": price,
            "price_date": it.get("_date"),
        })
    return rows


EXTRACTORS = {
    "talaadthai": (extract_talaadthai, normalize_talaadthai),
    "simummuang": (extract_simummuang, normalize_simummuang),
    "srimuang": (extract_srimuang, normalize_srimuang),
}


if __name__ == "__main__":
    # ทดสอบนอก Airflow: python dags/sources.py
    for src, (ext, norm) in EXTRACTORS.items():
        rows = norm(ext())
        print(src, len(rows), rows[:1])
