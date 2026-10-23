"""ดึงราคาน้ำมันจาก 3 แหล่ง แล้วคืนเป็นแถวรูปแบบเดียวกัน (price_date, brand, fuel_code, raw_name, price).

1) ปตท.   : SOAP Web Service GetOilPrice ขอได้ทีละวัน มีเฉพาะแบรนด์ PTT
2) บางจาก : JSON API คืนราคาเมื่อวานและวันนี้ มีเฉพาะแบรนด์ BCP
3) สนพ.   : web scraping หน้า ASP.NET ของ สนพ. (อ่าน hidden field ด้วย BeautifulSoup แล้วส่งฟอร์ม)
            ได้รายงานราคาของทุกผู้ค้าในวันที่ขอ ไม่ใช้ Selenium
ไม่มีแหล่งใดมีข้อมูลส่วนบุคคล เก็บเฉพาะราคาสินค้า
"""

import html
import io
import json
import re
import time
from datetime import date, datetime

import requests
import xlrd
from bs4 import BeautifulSoup

from fuel_common import (
    BANGCHAK_API_URL,
    EPPO_FORM_URL,
    HTTP_TIMEOUT,
    PTT_SOAP_URL,
    REQUEST_PAUSE_SECONDS,
    USER_AGENT,
    to_fuel_code,
)

PTT_SOAP_BODY = (
    '<?xml version="1.0" encoding="utf-8"?>'
    '<soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/"><soap:Body>'
    '<GetOilPrice xmlns="http://www.pttor.com"><Language>en</Language>'
    "<DD>{d}</DD><MM>{m}</MM><YYYY>{y}</YYYY></GetOilPrice>"
    "</soap:Body></soap:Envelope>"
)
PTT_PATTERN = re.compile(r"<PRODUCT>(.*?)</PRODUCT>\s*<PRICE>(.*?)</PRICE>", re.S)

# คอลัมน์ในรายงานของ สนพ. เรียงตามหัวตาราง PTT | BCP | Shell | Esso | Chevron | IRPC | PTG | Susco | Pure | SUSCO Dealers
EPPO_BRAND_COLUMNS = ["PTT", "BCP", "SHELL", "ESSO", "CHEVRON", "IRPC", "PTG", "SUSCO", "PURE", "SUSCO_DEALER"]


def _session() -> requests.Session:
    session = requests.Session()
    session.headers["User-Agent"] = USER_AGENT
    return session


def _price(value) -> float | None:
    try:
        price = float(str(value).replace(",", "").strip())
    except ValueError:
        return None
    return price if price > 0 else None


def fetch_ptt(days: list[date]) -> list[tuple]:
    session = _session()
    rows = []
    for day in days:
        time.sleep(REQUEST_PAUSE_SECONDS)
        response = session.post(
            PTT_SOAP_URL,
            data=PTT_SOAP_BODY.format(d=day.day, m=day.month, y=day.year).encode("utf-8"),
            headers={"Content-Type": "text/xml; charset=utf-8", "SOAPAction": '"http://www.pttor.com/GetOilPrice"'},
            timeout=HTTP_TIMEOUT,
        )
        response.raise_for_status()
        # ผลลัพธ์เป็น XML ที่ถูก escape ซ้อนอยู่ใน SOAP body
        for name, raw_price in PTT_PATTERN.findall(html.unescape(response.text)):
            code, price = to_fuel_code("ptt_soap", name), _price(raw_price)
            if code and price is not None:
                rows.append((day, "PTT", code, name.strip(), price))
    return rows


def _thai_date(value: str) -> date:
    d, m, y = (int(x) for x in value.split("/"))
    return date(y - 543 if y > 2400 else y, m, d)


def fetch_bangchak() -> list[tuple]:
    response = _session().get(BANGCHAK_API_URL, timeout=HTTP_TIMEOUT)
    response.raise_for_status()
    payload = response.json()[0]
    # วันที่เป็น dd/mm/พ.ศ. PriceToday ใช้กับ OilDateNow ส่วน PriceYesterday คือราคาก่อนการปรับครั้งล่าสุด
    # จึงใช้กับ OilPriceDate (วันประกาศ ซึ่งยังเป็นราคาเดิม) ไม่ใช่ "วันนี้ - 1"
    today, announced = _thai_date(payload["OilDateNow"]), _thai_date(payload["OilPriceDate"])
    rows = []
    for item in json.loads(payload["OilList"]):
        code = to_fuel_code("bangchak_api", item["OilName"])
        if not code:
            continue
        for day, key in ((announced, "PriceYesterday"), (today, "PriceToday")):
            if key == "PriceYesterday" and announced >= today:
                continue
            price = _price(item.get(key))
            if price is not None:
                rows.append((day, "BCP", code, item["OilName"].strip(), price))
    return rows


def _with_retry(send, attempts: int = 3) -> requests.Response:
    """เซิร์ฟเวอร์ สนพ. ตัดการเชื่อมต่อกลางทางบ่อย (IncompleteRead) ซึ่ง Retry ของ urllib3 ไม่ครอบคลุม จึงลองใหม่เอง."""
    for attempt in range(1, attempts + 1):
        try:
            response = send()
            response.raise_for_status()
            return response
        except requests.RequestException:
            if attempt == attempts:
                raise
            time.sleep(2 * attempt)


def fetch_eppo_day(day: date) -> list[tuple]:
    """Scrape หน้าฟอร์ม: GET เพื่ออ่าน __VIEWSTATE แล้ว POST วันที่ ได้รายงานราคาทุกผู้ค้าของวันนั้น."""
    session = _session()
    page = _with_retry(lambda: session.get(EPPO_FORM_URL, timeout=HTTP_TIMEOUT))
    soup = BeautifulSoup(page.text, "html.parser")
    form = {field["name"]: field.get("value", "") for field in soup.select("input[type=hidden]") if field.get("name")}
    if "__VIEWSTATE" not in form or not soup.find("input", {"name": "TbxToDate"}):
        raise ValueError("EPPO page layout changed: date form not found")
    form.update({"TbxToDate": day.strftime("%d/%m/%Y"), "BtnGenerate.x": "10", "BtnGenerate.y": "10"})
    time.sleep(REQUEST_PAUSE_SECONDS)
    report = _with_retry(lambda: session.post(EPPO_FORM_URL, data=form, timeout=HTTP_TIMEOUT))
    if "excel" not in report.headers.get("content-type", ""):
        raise ValueError(f"EPPO returned {report.headers.get('content-type')} instead of the price report")

    sheet = xlrd.open_workbook(file_contents=report.content).sheet_by_index(0)
    rows = []
    for r in range(sheet.nrows):
        label = str(sheet.cell_value(r, 0)).strip()
        code = to_fuel_code("eppo_web", label) if label else None
        if not code:
            continue
        for offset, brand in enumerate(EPPO_BRAND_COLUMNS, start=1):
            if offset < sheet.ncols:
                price = _price(sheet.cell_value(r, offset))
                if price is not None:
                    rows.append((day, brand, code, label, price))
    return rows


def parse_iso(value: str) -> date:
    return datetime.strptime(value[:10], "%Y-%m-%d").date()
