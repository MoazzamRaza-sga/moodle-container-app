import json
import sys
import time
from datetime import datetime

from dotenv import load_dotenv

from storage_io import CSVSink, WatermarkRegistry, _get
from mysql_conn import (
    make_ssh_tunnel_if_needed,
    mysql_connect,
    extract_in_chunks_by_pk,
    rows_to_table,
)

# --------- Table config (PK + watermark column if available) ---------
TABLE_SPECS = [
    {"name": "mdl_cohort",                          "pk": "id", "wm": "timemodified"},
    {"name": "mdl_cohort_members",                  "pk": "id", "wm": "timeadded"},
    {"name": "mdl_context",                         "pk": "id", "wm": None},
    {"name": "mdl_course",                          "pk": "id", "wm": "timemodified"},
    {"name": "mdl_course_categories",               "pk": "id", "wm": "timemodified"},
    {"name": "mdl_course_completion_crit_compl",    "pk": "id", "wm": "timecompleted"},
    {"name": "mdl_course_completions",              "pk": "id", "wm": "timeenrolled"},
    {"name": "mdl_customfield_data",                "pk": "id", "wm": "timecreated"},
    {"name": "mdl_customfield_field",               "pk": "id", "wm": "timecreated"},
    {"name": "mdl_logstore_standard_log",           "pk": "id", "wm": "timecreated"},
    {"name": "mdl_role_assignments",                "pk": "id", "wm": "timemodified"},
    {"name": "mdl_tag",                             "pk": "id", "wm": "timemodified"},
    {"name": "mdl_tag_instance",                    "pk": "id", "wm": "timecreated"},
    {"name": "mdl_user_lastaccess",                 "pk": "id", "wm": "timeaccess"},
]

def _safe_int(x):
    if x is None:
        return None
    try:
        return int(x)
    except Exception:
        return None

def _max_col(batch, col):
    if not batch:
        return None
    vals = [row.get(col) for row in batch if row.get(col) is not None]
    if not vals:
        return None
    ints = [v for v in (_safe_int(v) for v in vals) if v is not None]
    if ints:
        return max(ints)
    try:
        return max(vals)
    except Exception:
        return None


def run_job() -> int:
    load_dotenv()

    chunk_size = int(_get("CHUNK_SIZE", "20000"))
    max_rows = _get("MAX_ROWS")
    max_rows = int(max_rows) if max_rows else None
    per_table_pause = float(_get("PER_TABLE_PAUSE_SEC", "0"))  # gentle throttle across tables

    print(json.dumps({
        "connect_via": _get("CONNECT_VIA", "SSH").upper(),
        "storage_kind": _get("STORAGE_KIND", "adls").lower(),
        "output_base_path": _get("OUTPUT_BASE_PATH"),
        "watermark_dir": _get("WATERMARK_DIR", "watermarks"),
        "format": "csv",
        "tables": [t["name"] for t in TABLE_SPECS]
    }, indent=2))

    sink = CSVSink()
    registry = WatermarkRegistry()

    tunnel = None
    key_ctx = None

    try:
        tunnel, key_ctx = make_ssh_tunnel_if_needed()
        conn = mysql_connect(tunnel)

        for spec in TABLE_SPECS:
            name = spec["name"]
            pk_col = spec["pk"]
            wm_col = spec["wm"]

            # read last watermark (or last pk for PK-only tables)
            last_meta = registry.load(name) or {}
            last_value = last_meta.get("value")
            last_value_int = _safe_int(last_value)

            print(f"\n=== Table: {name} (pk={pk_col}, wm={wm_col or 'PK'}) last_value={last_value} ===")

            total_rows = 0
            parts = 0
            max_seen = last_value_int  # carry forward

            started = datetime.utcnow()

            # choose starting point
            last_pk_start = (last_value_int if wm_col is None else None)

            # iterate in chunks
            chunk_iter = extract_in_chunks_by_pk(
                conn, name, pk_col, chunk_size,
                watermark_col=wm_col,
                last_watermark=last_value_int if wm_col else None,
                max_rows=max_rows,
                last_pk_start=last_pk_start
            )

            for i, rows in enumerate(chunk_iter, start=1):
                tbl = rows_to_table(rows)
                if tbl is None or tbl.num_rows == 0:
                    continue

                col_for_wm = (wm_col or pk_col)
                batch_max = _max_col(rows, col_for_wm)
                if batch_max is not None:
                    if max_seen is None or batch_max > max_seen:
                        max_seen = batch_max

                sink.write_table(tbl, part_idx=i - 1, dataset_name=name)
                total_rows += tbl.num_rows
                parts += 1
                print(f"[{name}] part {i-1}, rows={tbl.num_rows}, total_rows={total_rows}, max_seen={max_seen}")

            duration_s = (datetime.utcnow() - started).total_seconds()
            sink.write_success_marker(
                total_rows=total_rows, parts=parts, extras={"duration_seconds": duration_s}, dataset_name=name
            )
            print(f"[{name}] done total_rows={total_rows}, parts={parts}, duration_s={duration_s:.2f}")

            if parts > 0 and max_seen is not None:
                registry.save(name, column=(wm_col or pk_col), value=max_seen)

            if per_table_pause > 0:
                time.sleep(per_table_pause)

        return 0

    except Exception as e:
        print(f"[error] {e}", file=sys.stderr)
        return 1

    finally:
        try:
            if tunnel: tunnel.stop()
        except Exception:
            pass
        try:
            if key_ctx: key_ctx.__exit__(None, None, None)
        except Exception:
            pass


if __name__ == "__main__":
    sys.exit(run_job())
