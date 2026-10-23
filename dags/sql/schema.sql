-- ตารางพักข้อมูลดิบ: 1 แถวต่อ แหล่ง x วัน x ผู้ค้า x ชนิดน้ำมัน
CREATE TABLE IF NOT EXISTS stg_fuel_prices (
    source        TEXT NOT NULL,
    price_date    DATE NOT NULL,
    brand         TEXT NOT NULL,
    fuel_code     TEXT NOT NULL,
    fuel_name_raw TEXT,
    price         NUMERIC(8, 2) NOT NULL,
    fetched_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (source, price_date, brand, fuel_code)
);

CREATE TABLE IF NOT EXISTS dim_fuel (
    fuel_code  TEXT PRIMARY KEY,
    fuel_name  TEXT NOT NULL,
    fuel_group TEXT NOT NULL
);

-- ชุดข้อมูลรวม (ตัดรายการซ้ำข้ามแหล่งแล้ว): 1 แถวต่อ วัน x ผู้ค้า x ชนิดน้ำมัน
CREATE TABLE IF NOT EXISTS fuel_prices (
    price_date   DATE NOT NULL,
    brand        TEXT NOT NULL,
    fuel_code    TEXT NOT NULL REFERENCES dim_fuel (fuel_code),
    price        NUMERIC(8, 2) NOT NULL,
    source       TEXT NOT NULL,
    source_count INT NOT NULL,
    sources      TEXT NOT NULL,
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (price_date, brand, fuel_code)
);

-- ผลการรวมของรอบล่าสุด ก่อนผ่าน Data Quality
CREATE TABLE IF NOT EXISTS fuel_prices_candidate (LIKE fuel_prices INCLUDING DEFAULTS);
ALTER TABLE fuel_prices_candidate ADD COLUMN IF NOT EXISTS reject_reason TEXT;

CREATE TABLE IF NOT EXISTS dq_rejects (
    run_id     TEXT NOT NULL,
    price_date DATE NOT NULL,
    brand      TEXT NOT NULL,
    fuel_code  TEXT NOT NULL,
    price      NUMERIC(8, 2),
    source     TEXT,
    reason     TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (run_id, price_date, brand, fuel_code)
);

CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id         TEXT PRIMARY KEY,
    started_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at    TIMESTAMPTZ,
    status         TEXT NOT NULL DEFAULT 'running',
    rows_candidate INT,
    rows_rejected  INT,
    rows_loaded    INT,
    total_rows     INT,
    dq_passed      BOOLEAN,
    note           TEXT
);

CREATE TABLE IF NOT EXISTS source_runs (
    run_id    TEXT NOT NULL,
    source    TEXT NOT NULL,
    status    TEXT NOT NULL,
    row_count INT NOT NULL DEFAULT 0,
    error     TEXT,
    logged_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (run_id, source)
);
