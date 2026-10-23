# ราคาน้ำมันขายปลีกรายวัน กรุงเทพฯ จากหลายแหล่ง

ข้อสอบปลายภาค วิชา 02739452 แพลตฟอร์มข้อมูลขนาดใหญ่และการวิเคราะห์ (ภาคต้น 2569)

- **หมวด:** ราคาพลังงาน
- **ขอบเขต:** ราคาน้ำมันขายปลีกรายวัน (ดีเซล / แก๊สโซฮอล์ 91, 95 / E20 ฯลฯ) ในกรุงเทพฯ และปริมณฑล เปรียบเทียบระหว่างผู้ค้า
- **ผู้จัดทำ:** รหัสนิสิต 6621653808 (ลำดับที่ 17) · สรุปภาพรวมอยู่ที่ [docs/17_6621653808_summary.pdf](docs/17_6621653808_summary.pdf)
- **ระดับที่ทำได้:** ระดับ 1 + 2 + 3 + โบนัส (หน้าสถานะ pipeline และ Dynamic Task Mapping)

## แหล่งข้อมูล

| # | แหล่ง | วิธีดึง | ได้ข้อมูลอะไร |
|---|---|---|---|
| 1 | ปตท. — [OilPrice Web Service](https://orapiweb.pttor.com/oilservice/OilPrice.asmx) | SOAP `GetOilPrice` ทีละวัน | ราคาแบรนด์ PTT ย้อนหลัง 30 วัน |
| 2 | บางจาก — [Oil Price API](https://oil-price.bangchak.co.th/ApiOilPrice2/th) | REST JSON | ราคาแบรนด์ BCP วันนี้ และราคาก่อนปรับครั้งล่าสุด |
| 3 | สำนักงานนโยบายและแผนพลังงาน (สนพ.) — [หน้ารายงานราคาขายปลีกรายวัน](https://www2.eppo.go.th/epporop/entryreport/ropbydaypublic.aspx) | **Web scraping** ด้วย `requests` + BeautifulSoup: GET หน้าเว็บ อ่าน hidden field ของฟอร์ม ASP.NET (`__VIEWSTATE` ฯลฯ) แล้ว POST วันที่ ได้รายงานราคาของวันนั้น (ไม่ใช้ Selenium) | ราคาของทุกผู้ค้า 10 ราย (PTT, BCP, Shell, Esso, Chevron, IRPC, PTG, Susco, Pure, Susco Dealer) ย้อนหลัง 30 วัน |

การปฏิบัติตามข้อกำหนด
- **robots.txt:** `www.bangchak.co.th` และ `www.eppo.go.th` ไม่ได้ห้าม path ที่ใช้ ส่วน `www2.eppo.go.th` ไม่มี robots.txt (404) ทุก request เว้นระยะ 0.5 วินาที, จำกัด scraping ขนานไม่เกิน 4 task และระบุ User-Agent ของโปรเจกต์
- **PDPA:** เก็บเฉพาะราคาสินค้า ผู้ค้า และวันที่ ไม่มีข้อมูลส่วนบุคคล
- **จำนวนข้อมูล:** ชุดข้อมูลรวมประมาณ 1,800+ รายการ (30 วัน × ผู้ค้า × ชนิดน้ำมัน)

## สถาปัตยกรรม

```text
create_schema → start_run → extract (TaskGroup, รันขนาน)
                              ├─ extract_ptt          (SOAP)
                              ├─ extract_bangchak     (JSON API)
                              └─ list_eppo_days → scrape_eppo_day ×30 (Dynamic Task Mapping, web scraping)
                                        ↓ แต่ละแหล่ง UPSERT ลง stg_fuel_prices
            merge_sources  (trigger_rule=all_done → แหล่งไหนล่ม ก็รวมจากแหล่งที่เหลือ)
                                        ↓ ตัดรายการซ้ำข้ามแหล่ง → fuel_prices_candidate
            quality_check  (branch) ──→ dq_passed ───────────────┐
                                  └──→ quarantine_bad_rows ──────┤ (ย้ายแถวไม่ผ่านไป dq_rejects)
            load_fuel_prices (UPSERT ON CONFLICT → fuel_prices) ←┘
            finalize_run (บันทึกสถานะลง pipeline_runs ให้หน้า /status)
```

- **Schema เดียวกัน:** ทุกแหล่งแปลงเป็น `(price_date, brand, fuel_code, price)` โดยใช้กฎ regex แปลงชื่อน้ำมันของแต่ละแหล่งให้เป็นรหัสกลาง (`dags/fuel_common.py`)
- **UPSERT:** `stg_fuel_prices` มี PK `(source, price_date, brand, fuel_code)` และ `fuel_prices` มี PK `(price_date, brand, fuel_code)` ใช้ `INSERT ... ON CONFLICT DO UPDATE` รันซ้ำแล้วจำนวนแถวไม่เพิ่ม
- **ตัดรายการซ้ำข้ามแหล่ง:** ราคา PTT มีทั้งจาก ปตท. และ สนพ. ส่วนราคา BCP มีทั้งจากบางจากและ สนพ. จึงเลือก 1 แถวต่อ (วัน, ผู้ค้า, น้ำมัน) โดยให้แหล่งของเจ้าของแบรนด์ชนะ และเก็บคอลัมน์ `sources` ไว้ว่าพบในแหล่งใดบ้าง
- **Data Quality:** ราคาต้องอยู่ในช่วง 10–100 บาท, ไม่เป็นวันที่ในอนาคต และต่างจากค่ามัธยฐานของผู้ค้าอื่นในวันเดียวกันไม่เกิน 15% ถ้ามีแถวไม่ผ่าน DAG จะแตกไปทาง `quarantine_bad_rows`
- **ทดสอบแหล่งล่ม:** ตั้ง Airflow Variable `simulate_down` เป็นชื่อแหล่ง เช่น `bangchak_api` หรือ `ptt_soap,eppo_web` แล้วรัน DAG งานของแหล่งนั้นจะ fail แต่ DAG ยังรวมข้อมูลจากแหล่งที่เหลือต่อได้ (สถานะรอบ = `partial`)

## โครงสร้างไฟล์

```text
dags/
  fuel_price_pipeline.py   DAG หลัก
  fuel_sources.py          ฟังก์ชันดึงข้อมูล 3 แหล่ง
  fuel_common.py           ค่าตั้งต้น, กฎแปลงชื่อน้ำมัน, helper ฐานข้อมูล
  sql/schema.sql           ตารางทั้งหมด
web/
  app.py                   Flask อ่าน PostgreSQL โดยตรง (psycopg2)
  templates/               หน้า Dashboard (/) และสถานะ Pipeline (/status)
db/init/                   สคริปต์สร้างฐานข้อมูล fueldb
docker-compose.yml, Dockerfile, requirements.txt, .env.example
```

## วิธีรัน

ต้องมี Docker Desktop

```bash
cp .env.example .env          # แก้รหัสผ่านใน .env ได้ (ไม่แก้ก็รันได้)
docker compose up -d --build
```

1. เปิด Airflow ที่ http://localhost:8080 (user / password ตาม `AIRFLOW_ADMIN_USER` / `AIRFLOW_ADMIN_PASSWORD` ใน `.env`)
2. DAG `fuel_price_pipeline` ไม่ถูก pause ตั้งแต่สร้าง จึงรันรอบแรกเองทันทีที่ระบบขึ้น (ใช้เวลาประมาณ 1 นาที) จะกด Trigger ซ้ำก็ได้
3. เปิด dashboard ที่ http://localhost:8000 และหน้าสถานะ pipeline ที่ http://localhost:8000/status

PostgreSQL เปิดที่ `localhost:5433` (ฐานข้อมูล `fueldb`) ใช้ตรวจผลได้ เช่น

```bash
docker compose exec postgres psql -U fuel -d fueldb -c "select count(*) from fuel_prices;"
```

ทดสอบกรณีแหล่งข้อมูลล่ม:

```bash
docker compose exec airflow-scheduler airflow variables set simulate_down bangchak_api
docker compose exec airflow-scheduler airflow dags trigger fuel_price_pipeline
docker compose exec airflow-scheduler airflow variables delete simulate_down
```
