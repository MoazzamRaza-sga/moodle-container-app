import os, sys, io, json, tempfile, platform, math
from datetime import datetime, timezone
from contextlib import contextmanager

from dotenv import load_dotenv
from sshtunnel import SSHTunnelForwarder
import pymysql
from pymysql.cursors import SSCursor, SSDictCursor  # server-side (streaming) cursors

# Storage (ADLS Gen2)
from azure.identity import DefaultAzureCredential
from azure.storage.filedatalake import DataLakeServiceClient
import pyarrow as pa
import pyarrow.parquet as pq

load_dotenv()

# ---------- Helpers & Config ----------

def _require(name):
    v = os.getenv(name)
    if not v:
        raise RuntimeError(f"Missing required env: {name}")
    return v

def _get(name, default=None):
    v = os.getenv(name)
    return v if v is not None and v != "" else default

@contextmanager
def temp_pem_from_env():
    """
    Accepts either:
      - SSH_PRIVATE_KEY (PEM text), or
      - SSH_PRIVATE_KEY_PATH (path to .pem file)
    Returns a temp file path usable by ssh.
    """
    pem_text = os.getenv("SSH_PRIVATE_KEY")
    pem_path = os.getenv("SSH_PRIVATE_KEY_PATH")

    if pem_text:
        fd, path = tempfile.mkstemp(prefix="id_", suffix=".pem")
        with os.fdopen(fd, "w") as f:
            f.write(pem_text)
        try:
            # chmod 600 is not enforced on Windows; ignore if it fails there.
            os.chmod(path, 0o600)
        except Exception:
            if platform.system().lower() != "windows":
                raise
        try:
            yield path
        finally:
            try:
                os.remove(path)
            except Exception:
                pass
    elif pem_path:
        yield pem_path
    else:
        raise RuntimeError("Provide SSH_PRIVATE_KEY or SSH_PRIVATE_KEY_PATH")

# ---------- Storage (ADLS/local) ----------

class ParquetSink:
    def __init__(self):
        self.local_out = _get("LOCAL_OUTPUT_DIR")
        self.account = _get("STORAGE_ACCOUNT")
        self.fs_name = _get("FILE_SYSTEM")
        self.base_path = _get("OUTPUT_BASE_PATH", "bronze/course")
        self.compression = _get("PARQUET_COMPRESSION", "snappy").lower()
        now = datetime.utcnow()
        self.run_ts = now.strftime("%Y%m%dT%H%M%SZ")
        self.ingestion_date = now.strftime("%Y-%m-%d")

        if self.local_out:
            os.makedirs(self.local_out, exist_ok=True)
            self.mode = "local"
            print(f"[sink] Using local output: {self.local_out}")
        else:
            if not self.account or not self.fs_name:
                raise RuntimeError("For ADLS, set STORAGE_ACCOUNT and FILE_SYSTEM.")
            self.mode = "adls"
            acct_url = f"https://{self.account}.dfs.core.windows.net"
            cred = DefaultAzureCredential(exclude_shared_token_cache_credential=True)
            self.dls = DataLakeServiceClient(account_url=acct_url, credential=cred)
            self.fs = self.dls.get_file_system_client(self.fs_name)
            print(f"[sink] Using ADLS: {acct_url}/{self.fs_name}")

    def _build_rel_path(self, part_idx: int):
        return (
            f"{self.base_path}/"
            f"ingestion_date={self.ingestion_date}/"
            f"run_ts={self.run_ts}/"
            f"part-{part_idx:05d}.parquet"
        )

    def write_table(self, table: pa.Table, part_idx: int):
        # to in-memory parquet
        buf = pa.BufferOutputStream()
        pq.write_table(table, buf, compression=self.compression)
        data = buf.getvalue().to_pybytes()

        rel = self._build_rel_path(part_idx)

        if self.mode == "local":
            out_path = os.path.join(self.local_out, rel.replace("/", os.sep))
            os.makedirs(os.path.dirname(out_path), exist_ok=True)
            with open(out_path, "wb") as f:
                f.write(data)
            print(f"[sink] wrote {len(data)} bytes → {out_path}")
            return out_path

        # ADLS
        file = self.fs.get_file_client(rel)
        file.upload_data(data, overwrite=True)
        print(f"[sink] wrote {len(data)} bytes → abfss://{self.fs_name}@{self.account}.dfs.core.windows.net/{rel}")
        return rel

    def write_success_marker(self, total_rows: int, parts: int, extras: dict):
        marker = {
            "table": "course",
            "total_rows": total_rows,
            "parts": parts,
            "compression": self.compression,
            "ingestion_date": self.ingestion_date,
            "run_ts": self.run_ts,
            "extras": extras,
            "created_utc": datetime.utcnow().isoformat() + "Z",
        }
        payload = (json.dumps(marker, indent=2) + "\n").encode("utf-8")

        rel = (
            f"{self.base_path}/"
            f"ingestion_date={self.ingestion_date}/"
            f"run_ts={self.run_ts}/_SUCCESS.json"
        )
        if self.mode == "local":
            path = os.path.join(self.local_out, rel.replace("/", os.sep))
            os.makedirs(os.path.dirname(path), exist_ok=True)
            with open(path, "wb") as f:
                f.write(payload)
            print(f"[sink] wrote marker → {path}")
            return

        file = self.fs.get_file_client(rel)
        file.upload_data(payload, overwrite=True)
        print(f"[sink] wrote marker → abfss://{self.fs_name}@{self.account}.dfs.core.windows.net/{rel}")

# ---------- MySQL over SSH ----------

def make_ssh_tunnel():
    ssh_host = _require("SSH_HOST")
    ssh_port = int(_get("SSH_PORT", "22"))
    ssh_user = _require("SSH_USERNAME")

    db_host = _get("DB_HOST", "127.0.0.1")
    db_port = int(_get("DB_PORT", "3306"))

    key_path_ctx = temp_pem_from_env()

    key_path = key_path_ctx.__enter__()  # manual so we can close later
    tunnel = SSHTunnelForwarder(
        (ssh_host, ssh_port),
        ssh_username=ssh_user,
        ssh_pkey=key_path,
        remote_bind_address=(db_host, db_port),
    )
    tunnel.start()
    print(f"[ssh] tunnel on localhost:{tunnel.local_bind_port}")
    return tunnel, key_path_ctx

def mysql_connect(tunnel):
    db_name = _require("DB_NAME")
    db_user = _require("DB_USERNAME")
    db_pass = _require("DB_PASSWORD")

    conn = pymysql.connect(
        host="127.0.0.1",
        port=tunnel.local_bind_port,
        user=db_user,
        password=db_pass,
        database=db_name,
        cursorclass=SSDictCursor,  # streaming dict cursor
        connect_timeout=10,
        read_timeout=120,
        write_timeout=120,
        charset=_get("DB_CHARSET", "utf8mb4"),
        autocommit=False,
    )
    return conn

# ---------- Extract & Write ----------

def fetch_pk_bounds(conn, table, pk_col):
    with conn.cursor() as cur:
        cur.execute(f"SELECT MIN(`{pk_col}`) AS lo, MAX(`{pk_col}`) AS hi FROM `{table}`")
        row = cur.fetchone() or {}
    return row.get("lo"), row.get("hi")

def build_where_clause(pk_col, watermark_col, last_watermark):
    clauses = []
    params = []
    if last_watermark and watermark_col:
        clauses.append(f"`{watermark_col}` > %s")
        params.append(last_watermark)
    where_sql = ("WHERE " + " AND ".join(clauses)) if clauses else ""
    return where_sql, params

def extract_in_chunks_by_pk(conn, table, pk_col, chunk_size, watermark_col=None, last_watermark=None, max_rows=None):
    lo, hi = fetch_pk_bounds(conn, table, pk_col)
    if lo is None:
        return  # empty table

    where_sql, params = build_where_clause(pk_col, watermark_col, last_watermark)

    rows_read = 0
    last_pk = lo - 1
    while True:
        limit = chunk_size
        if max_rows is not None:
            remaining = max_rows - rows_read
            if remaining <= 0:
                break
            limit = min(limit, remaining)

        sql = (
            f"SELECT * FROM `{table}` "
            f"{where_sql} {'AND' if where_sql else 'WHERE'} `{pk_col}` > %s "
            f"ORDER BY `{pk_col}` ASC LIMIT %s"
        )
        qparams = params + [last_pk, limit]
        with conn.cursor() as cur:
            cur.execute(sql, qparams)
            batch = cur.fetchall()

        if not batch:
            break

        rows_read += len(batch)
        last_pk = batch[-1][pk_col]
        yield batch

def extract_in_chunks_by_offset(conn, table, chunk_size, watermark_col=None, last_watermark=None, max_rows=None):
    where_sql, params = build_where_clause(None, watermark_col, last_watermark)
    # Note: OFFSET can be slow on huge tables; PK paging is preferred.
    offset = 0
    rows_read = 0

    while True:
        limit = chunk_size
        if max_rows is not None:
            remaining = max_rows - rows_read
            if remaining <= 0:
                break
            limit = min(limit, remaining)

        sql = f"SELECT * FROM `{table}` {where_sql} LIMIT %s OFFSET %s"
        qparams = params + [limit, offset]
        with conn.cursor() as cur:
            cur.execute(sql, qparams)
            batch = cur.fetchall()

        if not batch:
            break

        rows_read += len(batch)
        offset += len(batch)
        yield batch

def rows_to_table(rows):
    if not rows:
        return None
    # Let Arrow infer schema; for DECIMAL heavy schemas you might want to cast explicitly.
    return pa.Table.from_pylist(rows)

def run_job():
    # ---- Config from env ----
    table = _get("COURSE_TABLE", "tbl_course")
    pk_col = _get("COURSE_PK")  # e.g., "id"; if missing, falls back to OFFSET paging
    watermark_col = _get("WATERMARK_COLUMN")        # e.g., "updated_at"
    last_watermark = _get("LAST_WATERMARK")         # ISO or MySQL datetime string
    chunk_size = int(_get("CHUNK_SIZE", "200000"))  # tune based on width
    max_rows = _get("MAX_ROWS")
    max_rows = int(max_rows) if max_rows else None

    print(json.dumps({
        "table": table, "pk_col": pk_col, "watermark_col": watermark_col,
        "last_watermark": last_watermark, "chunk_size": chunk_size, "max_rows": max_rows
    }, indent=2))

    sink = ParquetSink()

    tunnel = None
    key_ctx = None
    total_rows = 0
    parts = 0
    started = datetime.utcnow()

    try:
        tunnel, key_ctx = make_ssh_tunnel()
        conn = mysql_connect(tunnel)

        # Choose strategy
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

        # Marker + metrics
        duration_s = (datetime.utcnow() - started).total_seconds()
        sink.write_success_marker(
            total_rows=total_rows,
            parts=parts,
            extras={"duration_seconds": duration_s}
        )

        print(f"[done] total_rows={total_rows}, parts={parts}, duration_s={duration_s:.2f}")
        return 0

    except Exception as e:
        print(f"[error] {e}", file=sys.stderr)
        return 1

    finally:
        # Cleanup
        try:
            if tunnel:
                tunnel.stop()
        except Exception:
            pass
        try:
            if key_ctx:
                key_ctx.__exit__(None, None, None)
        except Exception:
            pass

if __name__ == "__main__":
    sys.exit(run_job())
