"""ค่าตั้งต้น, ตัวแปลงชื่อน้ำมันให้เป็นรหัสเดียวกัน และฟังก์ชันฐานข้อมูลที่ทุก task ใช้ร่วมกัน."""

import os
import re
from datetime import date, datetime, timedelta, timezone

from airflow.providers.postgres.hooks.postgres import PostgresHook

POSTGRES_CONN_ID = "fuel_db"
LOOKBACK_DAYS = int(os.getenv("FUEL_LOOKBACK_DAYS", "30"))
HTTP_TIMEOUT = 30
REQUEST_PAUSE_SECONDS = 0.5
USER_AGENT = "KU-02739452-student-project/1.0 (+educational use)"
BANGKOK_TZ = timezone(timedelta(hours=7))

PTT_SOAP_URL = "https://orapiweb.pttor.com/oilservice/OilPrice.asmx"
BANGCHAK_API_URL = "https://oil-price.bangchak.co.th/ApiOilPrice2/th"
EPPO_FORM_URL = "https://www2.eppo.go.th/epporop/entryreport/ropbydaypublic.aspx"

# แหล่งที่เป็นเจ้าของแบรนด์ถือว่าน่าเชื่อถือที่สุด เมื่อราคาซ้ำกันข้ามแหล่งจะเลือกแหล่งที่ลำดับต่ำกว่า
SOURCE_PRIORITY = {"ptt_soap": 1, "bangchak_api": 1, "eppo_web": 2}

# รหัสน้ำมันกลางที่ทุกแหล่งแปลงเข้ามา
FUELS = {
    "gsh95": ("แก๊สโซฮอล์ 95 (E10)", "เบนซิน"),
    "gsh91": ("แก๊สโซฮอล์ 91 (E10)", "เบนซิน"),
    "e20": ("แก๊สโซฮอล์ E20", "เบนซิน"),
    "e85": ("แก๊สโซฮอล์ E85", "เบนซิน"),
    "gsh95_premium": ("แก๊สโซฮอล์ 95 พรีเมียม", "เบนซิน"),
    "gsh99_premium": ("แก๊สโซฮอล์ 99 พรีเมียม", "เบนซิน"),
    "ulg95": ("เบนซิน 95", "เบนซิน"),
    "diesel_b7": ("ดีเซล B7", "ดีเซล"),
    "diesel_b10": ("ดีเซล B10", "ดีเซล"),
    "diesel_b20": ("ดีเซล B20", "ดีเซล"),
    "diesel_premium": ("ดีเซลพรีเมียม", "ดีเซล"),
}

BRANDS = ["PTT", "BCP", "SHELL", "ESSO", "CHEVRON", "IRPC", "PTG", "SUSCO", "PURE", "SUSCO_DEALER"]

# กฎแปลงชื่อน้ำมันของแต่ละแหล่ง (เช็กตามลำดับ ตัวแรกที่ตรงชนะ)
FUEL_RULES = {
    "ptt_soap": [
        (r"^Diesel B20$", "diesel_b20"),
        (r"^Diesel B10$", "diesel_b10"),
        (r"^Premium Diesel", "diesel_premium"),
        (r"^Diesel$", "diesel_b7"),
        (r"E85", "e85"),
        (r"E20", "e20"),
        (r"^Gasohol 91", "gsh91"),
        (r"^Gasohol 95", "gsh95"),
        (r"^Gasoline 95", "ulg95"),
        (r"GSH95", "gsh95_premium"),
        (r"X99", "gsh99_premium"),
    ],
    "bangchak_api": [
        (r"ดีเซล B20", "diesel_b20"),
        (r"พรีเมียม ดีเซล", "diesel_premium"),
        (r"ไฮดีเซล", "diesel_b7"),
        (r"พรีเมียม 9[5-8]", "gsh95_premium"),
        (r"E85", "e85"),
        (r"E20", "e20"),
        (r"91", "gsh91"),
        (r"95", "gsh95"),
    ],
    "eppo_web": [
        (r"95-E10", "gsh95"),
        (r"95-E20", "e20"),
        (r"95-E85", "e85"),
        (r"91-E10", "gsh91"),
        (r"^แก๊สโซฮอล ออกเทน 95 พรีเมียม", "gsh95_premium"),
        (r"^แก๊สโซฮอล ออกเทน 99 พรีเมียม", "gsh99_premium"),
        (r"ULG 95", "ulg95"),
        (r"HSD B7", "diesel_b7"),
        (r"HSD B10", "diesel_b10"),
        (r"HSD B20", "diesel_b20"),
        (r"^ดีเซลหมุนเร็ว พรีเมียม", "diesel_premium"),
    ],
}


def to_fuel_code(source: str, raw_name: str) -> str | None:
    for pattern, code in FUEL_RULES[source]:
        if re.search(pattern, raw_name.strip()):
            return code
    return None


def today_bkk() -> date:
    return datetime.now(BANGKOK_TZ).date()


def lookback_days() -> list[date]:
    today = today_bkk()
    return [today - timedelta(days=i) for i in range(LOOKBACK_DAYS - 1, -1, -1)]


def hook() -> PostgresHook:
    return PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)


def upsert_staging(source: str, rows: list[tuple]) -> int:
    """rows = (price_date, brand, fuel_code, raw_name, price). รันซ้ำแล้วจำนวนแถวไม่เพิ่ม."""
    if not rows:
        return 0
    conn = hook().get_conn()
    with conn, conn.cursor() as cur:
        cur.executemany(
            """
            INSERT INTO stg_fuel_prices (source, price_date, brand, fuel_code, fuel_name_raw, price, fetched_at)
            VALUES (%s, %s, %s, %s, %s, %s, now())
            ON CONFLICT (source, price_date, brand, fuel_code) DO UPDATE SET
                price = EXCLUDED.price, fuel_name_raw = EXCLUDED.fuel_name_raw, fetched_at = now();
            """,
            [(source, *row) for row in rows],
        )
    conn.close()
    return len(rows)


def log_source(run_id: str, source: str, status: str, rows: int, error: str | None = None) -> None:
    hook().run(
        """
        INSERT INTO source_runs (run_id, source, status, row_count, error, logged_at)
        VALUES (%s, %s, %s, %s, %s, now())
        ON CONFLICT (run_id, source) DO UPDATE SET
            status = EXCLUDED.status, row_count = EXCLUDED.row_count,
            error = EXCLUDED.error, logged_at = now();
        """,
        parameters=(run_id, source, status, rows, (error or "")[:500] or None),
    )
