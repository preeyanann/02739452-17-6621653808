"""DAG รวมราคาน้ำมันขายปลีกรายวันในกรุงเทพฯ จาก 3 แหล่ง (ปตท., บางจาก, สนพ.) ลง PostgreSQL.

create_schema -> start_run -> [extract: ptt | bangchak | eppo (Dynamic Task Mapping ทีละวัน)]
             -> merge_sources (all_done: แหล่งไหนล่มก็รวมจากแหล่งที่เหลือ, ตัดรายการซ้ำข้ามแหล่ง)
             -> quality_check (branch) -> dq_passed | quarantine_bad_rows
             -> load_fuel_prices (UPSERT) -> finalize_run (บันทึกสถานะให้หน้า status บนเว็บ)
"""

from datetime import datetime, timedelta
from pathlib import Path

import pendulum
from airflow.decorators import dag, task, task_group
from airflow.exceptions import AirflowFailException
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule

from fuel_common import FUELS, SOURCE_PRIORITY, hook, log_source, lookback_days, today_bkk, upsert_staging

SCHEMA_SQL = Path(__file__).parent / "sql" / "schema.sql"
OUTLIER_RATIO = 0.15


def _simulate_down(source: str) -> None:
    """ตั้ง Airflow Variable `simulate_down` เช่น "bangchak_api" เพื่อทดสอบกรณีแหล่งข้อมูลล่ม."""
    down = {s.strip() for s in Variable.get("simulate_down", default_var="").split(",") if s.strip()}
    if source in down:
        raise ConnectionError(f"simulated outage of {source} (Variable simulate_down)")


def _run_source(source: str, run_id: str, fetch) -> int:
    try:
        _simulate_down(source)
        rows = fetch()
        if not rows:
            raise ValueError("source returned 0 rows")
        count = upsert_staging(source.split(":")[0], rows)
    except Exception as exc:
        log_source(run_id, source, "failed", 0, f"{type(exc).__name__}: {exc}")
        raise
    log_source(run_id, source, "success", count)
    return count


@dag(
    dag_id="fuel_price_pipeline",
    description="รวมราคาน้ำมันขายปลีกรายวัน กทม. จาก ปตท. / บางจาก / สนพ.",
    schedule="0 7 * * *",
    start_date=pendulum.datetime(2026, 10, 1, tz="Asia/Bangkok"),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": timedelta(seconds=20)},
    tags=["02739452", "fuel", "final-exam"],
)
def fuel_price_pipeline():
    @task
    def create_schema() -> None:
        db = hook()
        db.run(SCHEMA_SQL.read_text(encoding="utf-8"))
        db.insert_rows(
            "dim_fuel",
            [(code, name, group) for code, (name, group) in FUELS.items()],
            target_fields=["fuel_code", "fuel_name", "fuel_group"],
            replace=True,
            replace_index="fuel_code",
        )

    @task
    def start_run(run_id: str | None = None) -> None:
        hook().run(
            "INSERT INTO pipeline_runs (run_id) VALUES (%s) "
            "ON CONFLICT (run_id) DO UPDATE SET started_at = now(), status = 'running', finished_at = NULL",
            parameters=(run_id,),
        )

    @task_group(group_id="extract")
    def extract():
        @task
        def extract_ptt(run_id: str | None = None) -> int:
            from fuel_sources import fetch_ptt

            return _run_source("ptt_soap", run_id, lambda: fetch_ptt(lookback_days()))

        @task
        def extract_bangchak(run_id: str | None = None) -> int:
            from fuel_sources import fetch_bangchak

            return _run_source("bangchak_api", run_id, fetch_bangchak)

        @task
        def list_eppo_days() -> list[str]:
            return [d.isoformat() for d in lookback_days()]

        @task(max_active_tis_per_dag=4)
        def scrape_eppo_day(day: str, run_id: str | None = None) -> int:
            from fuel_sources import fetch_eppo_day, parse_iso

            return _run_source(f"eppo_web:{day}", run_id, lambda: fetch_eppo_day(parse_iso(day)))

        extract_ptt()
        extract_bangchak()
        scrape_eppo_day.expand(day=list_eppo_days())

    @task(trigger_rule=TriggerRule.ALL_DONE)
    def merge_sources(run_id: str | None = None) -> dict:
        """รวมทุกแหล่งเป็น schema เดียว และเลือก 1 แถวต่อ (วัน, ผู้ค้า, น้ำมัน) ตามลำดับความน่าเชื่อถือ."""
        db = hook()
        ok = db.get_records(
            "SELECT split_part(source, ':', 1), count(*) FROM source_runs "
            "WHERE run_id = %s AND status = 'success' GROUP BY 1",
            parameters=(run_id,),
        )
        if not ok:
            raise AirflowFailException("ทุกแหล่งล้มเหลว ไม่มีข้อมูลให้รวม")
        priority = " ".join(f"WHEN '{src}' THEN {rank}" for src, rank in SOURCE_PRIORITY.items())
        db.run(
            f"""
            TRUNCATE fuel_prices_candidate;
            INSERT INTO fuel_prices_candidate
                (price_date, brand, fuel_code, price, source, source_count, sources, updated_at)
            SELECT r.price_date, r.brand, r.fuel_code, r.price, r.source, a.n, a.srcs, now()
            FROM (
                SELECT s.*, row_number() OVER (
                    PARTITION BY price_date, brand, fuel_code
                    ORDER BY CASE source {priority} ELSE 9 END, fetched_at DESC) AS rn
                FROM stg_fuel_prices s
            ) r
            JOIN (
                SELECT price_date, brand, fuel_code, count(*) AS n, string_agg(source, ',' ORDER BY source) AS srcs
                FROM stg_fuel_prices GROUP BY 1, 2, 3
            ) a USING (price_date, brand, fuel_code)
            WHERE r.rn = 1;
            """
        )
        staged, merged = db.get_first(
            "SELECT (SELECT count(*) FROM stg_fuel_prices), (SELECT count(*) FROM fuel_prices_candidate)"
        )
        hook().run("UPDATE pipeline_runs SET rows_candidate = %s WHERE run_id = %s", parameters=(merged, run_id))
        result = {"sources_ok": dict(ok), "staged_rows": staged, "merged_rows": merged, "duplicates_removed": staged - merged}
        print(f"merge result: {result}")
        return result

    @task.branch
    def quality_check() -> str:
        """ตรวจ: ราคาอยู่ในช่วงสมเหตุสมผล, ไม่ใช่วันในอนาคต, ไม่ต่างจากราคากลางของผู้ค้าอื่นเกิน 15%."""
        db = hook()
        db.run(
            f"""
            UPDATE fuel_prices_candidate SET reject_reason = NULL;
            UPDATE fuel_prices_candidate SET reject_reason = 'price_out_of_range'
             WHERE price < 10 OR price > 100;
            UPDATE fuel_prices_candidate SET reject_reason = 'future_date'
             WHERE reject_reason IS NULL AND price_date > %s;
            UPDATE fuel_prices_candidate c SET reject_reason = 'outlier_vs_market'
              FROM (
                SELECT price_date, fuel_code,
                       percentile_cont(0.5) WITHIN GROUP (ORDER BY price) AS median, count(*) AS n
                FROM fuel_prices_candidate GROUP BY 1, 2
              ) m
             WHERE c.reject_reason IS NULL AND m.price_date = c.price_date AND m.fuel_code = c.fuel_code
               AND m.n >= 3 AND abs(c.price - m.median) / m.median > {OUTLIER_RATIO};
            """,
            parameters=(today_bkk() + timedelta(days=1),),
        )
        bad, total = db.get_first(
            "SELECT count(*) FILTER (WHERE reject_reason IS NOT NULL), count(*) FROM fuel_prices_candidate"
        )
        print(f"quality check: {bad} of {total} rows failed")
        if total == 0:
            raise AirflowFailException("ไม่มีข้อมูลหลังรวม")
        return "dq_passed" if bad == 0 else "quarantine_bad_rows"

    dq_passed = EmptyOperator(task_id="dq_passed")

    @task
    def quarantine_bad_rows(run_id: str | None = None) -> int:
        db = hook()
        db.run(
            """
            INSERT INTO dq_rejects (run_id, price_date, brand, fuel_code, price, source, reason)
            SELECT %s, price_date, brand, fuel_code, price, source, reject_reason
            FROM fuel_prices_candidate WHERE reject_reason IS NOT NULL
            ON CONFLICT (run_id, price_date, brand, fuel_code) DO UPDATE SET reason = EXCLUDED.reason;
            """,
            parameters=(run_id,),
        )
        bad = db.get_first("SELECT count(*) FROM fuel_prices_candidate WHERE reject_reason IS NOT NULL")[0]
        print(f"WARN data quality: quarantined {bad} rows into dq_rejects")
        return bad

    @task(trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
    def load_fuel_prices(run_id: str | None = None) -> int:
        """UPSERT เฉพาะแถวที่ผ่าน DQ รันซ้ำกี่รอบจำนวนแถวก็ไม่เพิ่ม และลบแถวเดิมที่รอบนี้ตรวจแล้วไม่ผ่าน."""
        db = hook()
        db.run(
            """
            INSERT INTO fuel_prices (price_date, brand, fuel_code, price, source, source_count, sources, updated_at)
            SELECT price_date, brand, fuel_code, price, source, source_count, sources, now()
            FROM fuel_prices_candidate WHERE reject_reason IS NULL
            ON CONFLICT (price_date, brand, fuel_code) DO UPDATE SET
                price = EXCLUDED.price, source = EXCLUDED.source, source_count = EXCLUDED.source_count,
                sources = EXCLUDED.sources, updated_at = now();
            DELETE FROM fuel_prices f USING fuel_prices_candidate c
             WHERE c.reject_reason IS NOT NULL
               AND (f.price_date, f.brand, f.fuel_code) = (c.price_date, c.brand, c.fuel_code);
            """
        )
        loaded, rejected = db.get_first(
            "SELECT count(*) FILTER (WHERE reject_reason IS NULL), count(*) FILTER (WHERE reject_reason IS NOT NULL) "
            "FROM fuel_prices_candidate"
        )
        total = db.get_first("SELECT count(*) FROM fuel_prices")[0]
        db.run(
            "UPDATE pipeline_runs SET rows_loaded = %s, rows_rejected = %s, total_rows = %s, dq_passed = %s "
            "WHERE run_id = %s",
            parameters=(loaded, rejected, total, rejected == 0, run_id),
        )
        print(f"loaded {loaded} rows, fuel_prices now has {total} rows")
        return loaded

    @task(trigger_rule=TriggerRule.ALL_DONE)
    def finalize_run(run_id: str | None = None) -> str:
        db = hook()
        failed = [r[0] for r in db.get_records(
            "SELECT source FROM source_runs WHERE run_id = %s AND status = 'failed' ORDER BY source",
            parameters=(run_id,),
        )]
        loaded = db.get_first("SELECT rows_loaded FROM pipeline_runs WHERE run_id = %s", parameters=(run_id,))
        status = "failed" if not loaded or loaded[0] is None else "partial" if failed else "success"
        note = f"แหล่งที่ล้มเหลว: {', '.join(failed)}" if failed else None
        db.run(
            "UPDATE pipeline_runs SET status = %s, finished_at = now(), note = %s WHERE run_id = %s",
            parameters=(status, note, run_id),
        )
        return status

    schema = create_schema()
    started = start_run()
    extracted = extract()
    merged = merge_sources()
    branch = quality_check()
    quarantined = quarantine_bad_rows()
    loaded = load_fuel_prices()
    finished = finalize_run()

    schema >> started >> extracted >> merged >> branch >> [dq_passed, quarantined]
    [dq_passed, quarantined] >> loaded >> finished


fuel_price_pipeline()
