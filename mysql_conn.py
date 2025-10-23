import os
import platform
import tempfile
from contextlib import contextmanager
from typing import Dict, Generator, Iterable, List, Optional, Tuple

import pymysql
from pymysql.cursors import SSDictCursor
from sshtunnel import SSHTunnelForwarder

import pyarrow as pa

# Reuse storage helpers to pull keys from ADLS/Blob if needed
from storage_io import (
    _get,
    _require,
    download_adls_path_to_bytes,
    download_blob_url_to_bytes,
)


# -------------------- SSH key sourcing --------------------

@contextmanager
def temp_pem_from_env():
    """
    Sources (priority order):
      1) SSH_PRIVATE_KEY (PEM text in env)
      2) SSH_PRIVATE_KEY_PATH (path on disk)
      3) SSH_KEY_PATH in ADLS Gen2 (account/container/path via env)
      4) SSH_KEY_BLOB_URL (full https Blob URL; MI or SAS)
    Yields a temp file path (or None) and cleans up on exit.
    """
    pem_text = os.getenv("SSH_PRIVATE_KEY")
    pem_path = os.getenv("SSH_PRIVATE_KEY_PATH")  # <-- fix: read this env
    adls_rel = os.getenv("SSH_KEY_PATH")          # e.g. secrets/razamo-key.pem
    blob_url = os.getenv("SSH_KEY_BLOB_URL")

    # 1) raw PEM in env
    if pem_text:
        fd, path = tempfile.mkstemp(prefix="id_", suffix=".pem")
        with os.fdopen(fd, "w") as f:
            f.write(pem_text)
        try:
            os.chmod(path, 0o600)
        except Exception:
            if platform.system().lower() != "windows":
                raise
        try:
            print("[ssh] using private key from SSH_PRIVATE_KEY")
            yield path
        finally:
            try:
                os.remove(path)
            except Exception:
                pass
        return

    # 2) direct local path
    if pem_path:
        print(f"[ssh] using private key from SSH_PRIVATE_KEY_PATH: {pem_path}")
        yield pem_path
        return

    # 3) ADLS Gen2 (dfs) by account/container/path
    if adls_rel:
        # If not explicitly provided, reuse STORAGE_ACCOUNT/FILE_SYSTEM
        account = os.getenv("SSH_KEY_ACCOUNT") or os.getenv("STORAGE_ACCOUNT")
        fs_name = os.getenv("SSH_KEY_FILE_SYSTEM") or os.getenv("FILE_SYSTEM")
        if not account or not fs_name:
            raise RuntimeError(
                "For ADLS key, set SSH_KEY_ACCOUNT (or STORAGE_ACCOUNT) and "
                "SSH_KEY_FILE_SYSTEM (or FILE_SYSTEM)."
            )
        key_bytes = download_adls_path_to_bytes(adls_rel, account=account, filesystem=fs_name)
        fd, path = tempfile.mkstemp(prefix="id_", suffix=".pem")
        with os.fdopen(fd, "wb") as f:
            f.write(key_bytes)
        try:
            os.chmod(path, 0o600)
        except Exception:
            if platform.system().lower() != "windows":
                raise
        try:
            print(
                f"[ssh] downloaded key from ADLS: "
                f"abfss://{fs_name}@{account}.dfs.core.windows.net/{adls_rel}"
            )
            yield path
        finally:
            try:
                os.remove(path)
            except Exception:
                pass
        return

    # 4) Blob URL (blob.core.windows.net) with MI or SAS
    if blob_url:
        key_bytes = download_blob_url_to_bytes(blob_url)
        fd, path = tempfile.mkstemp(prefix="id_", suffix=".pem")
        with os.fdopen(fd, "wb") as f:
            f.write(key_bytes)
        try:
            os.chmod(path, 0o600)
        except Exception:
            if platform.system().lower() != "windows":
                raise
        try:
            print(f"[ssh] downloaded key from Blob URL")
            yield path
        finally:
            try:
                os.remove(path)
            except Exception:
                pass
        return

    print("[ssh] no key provided via env/path/storage; continuing without key")
    yield None


# -------------------- MySQL connectivity --------------------

def make_ssh_tunnel_if_needed() -> Tuple[Optional[SSHTunnelForwarder], Optional[contextmanager]]:
    connect_via = _get("CONNECT_VIA", "SSH").upper()
    if connect_via != "SSH":
        return None, None

    ssh_host = _require("SSH_HOST")
    ssh_port = int(_get("SSH_PORT", "22"))
    ssh_user = _require("SSH_USERNAME")

    db_host = _get("DB_HOST", "127.0.0.1")
    db_port = int(_get("DB_PORT", "3306"))

    ssh_password = _get("SSH_PASSWORD")       # may unlock encrypted key
    ssh_passphrase = _get("SSH_PASSPHRASE")   # explicit passphrase

    key_ctx = temp_pem_from_env()
    key_path = key_ctx.__enter__()

    kwargs = {
        "ssh_address_or_host": (ssh_host, ssh_port),
        "ssh_username": ssh_user,
        "remote_bind_address": (db_host, db_port),
    }

    if key_path:
        kwargs["ssh_pkey"] = key_path
        if ssh_passphrase:
            kwargs["ssh_private_key_password"] = ssh_passphrase
        elif ssh_password:
            kwargs["ssh_private_key_password"] = ssh_password
        print("[ssh] authenticating with private key")
    elif ssh_password:
        kwargs["ssh_password"] = ssh_password
        print("[ssh] authenticating with password")
    else:
        raise RuntimeError(
            "No SSH key or password provided. Set SSH_PRIVATE_KEY / SSH_PRIVATE_KEY_PATH / "
            "SSH_KEY_PATH / SSH_KEY_BLOB_URL, or SSH_PASSWORD."
        )

    tunnel = SSHTunnelForwarder(**kwargs)
    tunnel.start()
    print(f"[ssh] tunnel on localhost:{tunnel.local_bind_port}")
    return tunnel, key_ctx

def mysql_ssl_kwargs() -> Dict:
    mode = (_get("DB_SSL_MODE") or "").lower()
    ca_path = _get("DB_SSL_CA_PATH", "/etc/ssl/certs/ca-certificates.crt")
    if mode == "disabled":
        return {}
    if mode == "verify_ca":
        return {"ssl": {"ca": ca_path}}
    return {"ssl": {}}

def mysql_connect(tunnel: Optional[SSHTunnelForwarder]) -> pymysql.connections.Connection:
    db_name = _require("DB_NAME")
    db_user = _require("DB_USERNAME")
    db_pass = _require("DB_PASSWORD")

    connect_via = _get("CONNECT_VIA", "SSH").upper()
    if tunnel and connect_via == "SSH":
        host = "127.0.0.1"
        port = tunnel.local_bind_port
        ssl_kwargs = {}
    else:
        host = _require("DB_HOST")
        port = int(_get("DB_PORT", "3306"))
        ssl_kwargs = mysql_ssl_kwargs()

    return pymysql.connect(
        host=host,
        port=port,
        user=db_user,
        password=db_pass,
        database=db_name,
        cursorclass=SSDictCursor,
        connect_timeout=10,
        read_timeout=120,
        write_timeout=120,
        charset=_get("DB_CHARSET", "utf8mb4"),
        autocommit=False,
        **ssl_kwargs
    )


# -------------------- Extraction helpers --------------------

def fetch_pk_bounds(conn, table: str, pk_col: str) -> Tuple[Optional[int], Optional[int]]:
    with conn.cursor() as cur:
        cur.execute(f"SELECT MIN(`{pk_col}`) AS lo, MAX(`{pk_col}`) AS hi FROM `{table}`")
        row = cur.fetchone() or {}
    return row.get("lo"), row.get("hi")

def build_where_clause(watermark_col: Optional[str], last_watermark: Optional[str]) -> Tuple[str, List]:
    clauses, params = [], []
    if last_watermark and watermark_col:
        clauses.append(f"`{watermark_col}` > %s")
        params.append(last_watermark)
    where_sql = ("WHERE " + " AND ".join(clauses)) if clauses else ""
    return where_sql, params

def extract_in_chunks_by_pk(conn,
                            table: str,
                            pk_col: str,
                            chunk_size: int,
                            watermark_col: Optional[str] = None,
                            last_watermark: Optional[str] = None,
                            max_rows: Optional[int] = None) -> Generator[list, None, None]:
    lo, hi = fetch_pk_bounds(conn, table, pk_col)
    if lo is None:
        return
    where_sql, params = build_where_clause(watermark_col, last_watermark)
    rows_read, last_pk = 0, lo - 1
    while True:
        limit = chunk_size if max_rows is None else min(chunk_size, max(0, max_rows - rows_read))
        if limit == 0:
            break
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

def extract_in_chunks_by_offset(conn,
                                table: str,
                                chunk_size: int,
                                watermark_col: Optional[str] = None,
                                last_watermark: Optional[str] = None,
                                max_rows: Optional[int] = None) -> Generator[list, None, None]:
    where_sql, params = build_where_clause(watermark_col, last_watermark)
    offset, rows_read = 0, 0
    while True:
        limit = chunk_size if max_rows is None else min(chunk_size, max(0, max_rows - rows_read))
        if limit == 0:
            break
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

def rows_to_table(rows: list) -> Optional[pa.Table]:
    if not rows:
        return None
    return pa.Table.from_pylist(rows)
