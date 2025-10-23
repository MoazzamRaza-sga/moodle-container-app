import json
import sys
from datetime import datetime

from dotenv import load_dotenv

from storage_io import CSVSink, _get
from mysql_conn import (
    make_ssh_tunnel_if_needed,
    mysql_connect,
    extract_in_chunks_by_pk,
    extract_in_chunks_by_offset,
    rows_to_table,
)


def run_job() -> int:
    load_dotenv()

    table = _get("COURSE_TABLE", "mdl_course")
    pk_col = _get("COURSE_PK")
    watermark_col = _get("WATERMARK_COLUMN")
    last_watermark = _get("LAST_WATERMARK")
    chunk_size = int(_get("CHUNK_SIZE", "200000"))
    max_rows = _get("MAX_ROWS")
    max_rows = int(max_rows) if max_rows else None

    print(json.dumps({
        "table": table, "pk_col": pk_col, "watermark_col": watermark_col,
        "last_watermark": last_watermark, "chunk_size": chunk_size, "max_rows": max_rows,
        "connect_via": _get("CONNECT_VIA", "SSH").upper(),
        "storage_kind": _get("STORAGE_KIND", "adls").lower(),
        "output_dir": _get("OUTPUT_DIR"),  # e.g., 2025/09/24
        "format": "csv"
    }, indent=2))

    sink = CSVSink()

    tunnel = None
    key_ctx = None
    total_rows = 0
    parts = 0
    started = datetime.utcnow()

    try:
        tunnel, key_ctx = make_ssh_tunnel_if_needed()
        conn = mysql_connect(tunnel)

        if pk_col:
            chunk_iter = extract_in_chunks_by_pk(
                conn, table, pk_col, chunk_size,
                watermark_col=watermark_col, last_watermark=last_watermark, max_rows=max_rows
            )
        else:
            print("[warn] COURSE_PK not set; using OFFSET paging (slower).")
            chunk_iter = extract_in_chunks_by_offset(
                conn, table, chunk_size,
                watermark_col=watermark_col, last_watermark=last_watermark, max_rows=max_rows
            )

        for i, rows in enumerate(chunk_iter, start=1):
            tbl = rows_to_table(rows)
            if tbl is None or tbl.num_rows == 0:
                continue
            sink.write_table(tbl, part_idx=i - 1)
            total_rows += tbl.num_rows
            parts += 1
            print(f"[batch] wrote part {i-1}, rows={tbl.num_rows}, total_rows={total_rows}")

        duration_s = (datetime.utcnow() - started).total_seconds()
        sink.write_success_marker(
            total_rows=total_rows, parts=parts, extras={"duration_seconds": duration_s}
        )
        print(f"[done] total_rows={total_rows}, parts={parts}, duration_s={duration_s:.2f}")
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
