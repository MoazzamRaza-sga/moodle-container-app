import os
import json
import tempfile
from datetime import datetime
from typing import Optional, Dict, Any

import pyarrow as pa
import pyarrow.csv as pacsv  # CSV writer

# Azure SDKs
from azure.identity import DefaultAzureCredential
from azure.storage.filedatalake import DataLakeServiceClient
from azure.storage.blob import BlobServiceClient, BlobClient
from azure.core.exceptions import ResourceNotFoundError

try:
    from zoneinfo import ZoneInfo  # Python 3.9+
except Exception:  # pragma: no cover
    ZoneInfo = None


# -------------------- Env helpers --------------------

def _require(name: str) -> str:
    v = os.getenv(name)
    if not v:
        raise RuntimeError(f"Missing required env: {name}")
    return v

def _get(name: str, default: Optional[str] = None) -> Optional[str]:
    v = os.getenv(name)
    return v if v is not None and v != "" else default

def _default_azure_credential():
    mi_client_id = _get("AZURE_CLIENT_ID")
    if mi_client_id:
        return DefaultAzureCredential(managed_identity_client_id=mi_client_id)
    return DefaultAzureCredential()


# -------------------- Low-level storage helpers (used by mysql_conn) --------------------

def _get_adls_clients(account: str, filesystem: str):
    cred = _default_azure_credential()
    acct_url = f"https://{account}.dfs.core.windows.net"
    dls = DataLakeServiceClient(account_url=acct_url, credential=cred)
    fs = dls.get_file_system_client(filesystem)
    return dls, fs

def download_adls_path_to_bytes(rel_path: str,
                                account: Optional[str] = None,
                                filesystem: Optional[str] = None) -> bytes:
    account = account or _require("STORAGE_ACCOUNT")
    filesystem = filesystem or _require("FILE_SYSTEM")
    _, fs = _get_adls_clients(account, filesystem)
    file_client = fs.get_file_client(rel_path)
    return file_client.download_file().readall()

def download_blob_url_to_bytes(blob_url: str) -> bytes:
    cred = _default_azure_credential()
    bc = BlobClient.from_blob_url(blob_url, credential=cred)
    # keep concurrency to 1 to avoid throttling
    return bc.download_blob(max_concurrency=1).readall()

# -------------------- CSV sanitizers --------------------

_ALLOWED_CTRL = {9, 10, 13}  # \t, \n, \r

def _to_text(value) -> str:
    """Coerce any value to a sanitized UTF-8 string."""
    if value is None:
        return ""
    if isinstance(value, bytes):
        s = value.decode("utf-8", "replace")
    else:
        s = str(value)

    # Normalize newlines and strip nasty control characters (keep \t \n \r)
    s = s.replace("\r\n", "\n").replace("\r", "\n")
    s = "".join(ch if (ord(ch) >= 32 or ord(ch) in _ALLOWED_CTRL) else " " for ch in s)
    return s

def _sanitize_table_to_strings(table: pa.Table) -> pa.Table:
    """
    Convert every column to string and sanitize each value.
    Works chunk-by-chunk to keep memory usage predictable.
    """
    names = table.schema.names
    cols = []
    for name in names:
        # Convert one column at a time
        pylist = table.column(name).to_pylist()
        san = [_to_text(v) for v in pylist]
        cols.append(pa.array(san, type=pa.string()))
    return pa.table(cols, names=names)




# -------------------- CSV sink (single file per table per run) --------------------

class CSVSink:
    """
    Writes each table to ONE CSV file named <table>.csv.

    Data file:
      OUTPUT_BASE_PATH/<table>/<YYYY>/<MM>/<DD>/<table>.csv

    Success JSON:
      OUTPUT_BASE_PATH/json logs/<table>/<YYYY>/<MM>/<DD>/success.json

    LOCAL mode: append directly to final file.
    ADLS/Blob modes: buffer to a local temp file, then upload once per table.
    """

    def __init__(self):
        self.base_path = _require("OUTPUT_BASE_PATH").strip().strip("/")

        self.local_out = _get("LOCAL_OUTPUT_DIR")
        self.account = _get("STORAGE_ACCOUNT")
        self.container = _get("FILE_SYSTEM")  # ADLS filesystem or Blob container
        self.storage_kind = _get("STORAGE_KIND", "adls").lower()  # 'adls' or 'blob'

        # Dynamic date (YYYY/MM/DD) in OUTPUT_TZ (fallback UTC)
        tz_name = _get("OUTPUT_TZ", "Asia/Karachi")
        now = datetime.utcnow()
        self.y = f"{now.year:04d}"
        self.m = f"{now.month:02d}"
        self.d = f"{now.day:02d}"

        # For success marker payload
        self.run_ts = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
        self.ingestion_date = datetime.utcnow().strftime("%Y-%m-%d")

        # Optional cleanup of sibling CSVs (e.g., table_01.csv created previously)
        self.cleanup_siblings = (_get("CLEANUP_SIBLINGS", "false").lower() == "true")

        # Init backends
        if self.local_out:
            os.makedirs(self.local_out, exist_ok=True)
            self.mode = "local"
            print(f"[sink] Using local output: {self.local_out}")
        else:
            if not self.account or not self.container:
                raise RuntimeError("For cloud sink, set STORAGE_ACCOUNT and FILE_SYSTEM (container).")
            cred = _default_azure_credential()
            if self.storage_kind == "adls":
                self.mode = "adls"
                acct_url = f"https://{self.account}.dfs.core.windows.net"
                self.dls = DataLakeServiceClient(account_url=acct_url, credential=cred)
                self.fs = self.dls.get_file_system_client(self.container)
                print(f"[sink] Using ADLS (dfs): {acct_url}/{self.container}")
            elif self.storage_kind == "blob":
                self.mode = "blob"
                acct_url = f"https://{self.account}.blob.core.windows.net"
                self.bsc = BlobServiceClient(account_url=acct_url, credential=cred)
                self.cc = self.bsc.get_container_client(self.container)
                print(f"[sink] Using Blob (blob): {acct_url}/{self.container}")
            else:
                raise RuntimeError("STORAGE_KIND must be 'adls' or 'blob'.")

        # per-table state:
        # { table: { "header_written": bool,
        #            "rel": str, "success_rel": str,
        #            "temp_path": str or None } }
        self._state: Dict[str, Dict[str, Any]] = {}

    # ---------- Path helpers ----------
    def _data_dir_for(self, table: str) -> str:
        return f"{self.base_path}/{table}/{self.y}/{self.m}/{self.d}"

    def _data_relpath_for(self, table: str) -> str:
        return f"{self._data_dir_for(table)}/{table}.csv"

    def _success_relpath_for(self, table: str) -> str:
        # folder name is literally "json logs"
        return f"{self.base_path}/json logs/{table}/{self.y}/{self.m}/{self.d}/success.json"

    # ---------- Cleanup helpers ----------
    def _delete_remote(self, rel: str):
        try:
            if self.mode == "adls":
                self.fs.delete_file(rel)
            elif self.mode == "blob":
                self.cc.delete_blob(rel, delete_snapshots="include")
        except Exception:
            pass  # ok if it doesn't exist

    def _cleanup_previous_csvs(self, table: str, final_rel: str):
        if not self.cleanup_siblings:
            return
        prefix = f"{self._data_dir_for(table)}/"
        try:
            if self.mode == "adls":
                for p in self.fs.get_paths(path=prefix, recursive=False):
                    name = p.name
                    if name.endswith(".csv") and name != final_rel:
                        try:
                            self.fs.delete_file(name)
                        except Exception:
                            pass
            elif self.mode == "blob":
                for b in self.cc.list_blobs(name_starts_with=prefix):
                    name = b.name
                    if name.endswith(".csv") and name != final_rel:
                        try:
                            self.cc.delete_blob(name, delete_snapshots="include")
                        except Exception:
                            pass
        except Exception:
            pass

    # ---------- File lifecycle per table ----------
    def _prepare_table(self, table: str):
        if table in self._state:
            return

        rel = self._data_relpath_for(table)
        success_rel = self._success_relpath_for(table)

        if self.mode == "local":
            path = os.path.join(self.local_out, rel.replace("/", os.sep))
            os.makedirs(os.path.dirname(path), exist_ok=True)
            if os.path.exists(path):
                os.remove(path)  # fresh file each run
            self._state[table] = {
                "header_written": False,
                "rel": rel,
                "success_rel": success_rel,
                "temp_path": None,   # not needed in local mode
            }
            print(f"[sink] Writing → {path}")
            return

        # Cloud modes: buffer to a temporary local file first
        fd, tmp = tempfile.mkstemp(prefix=f"{table}_", suffix=".csv")
        os.close(fd)
        self._state[table] = {
            "header_written": False,
            "rel": rel,
            "success_rel": success_rel,
            "temp_path": tmp,
        }
        print(f"[sink] Buffering → {tmp}  (final: {rel})")

    # ---------- Append a chunk ----------
    def write_table(self, table_pa: pa.Table, part_idx: int, dataset_name: str) -> str:
        tbl = dataset_name
        self._prepare_table(tbl)

        # Sanitize: force every value to a UTF-8 string and clean control chars
        # table_pa = _sanitize_table_to_strings(table_pa)

        # Include header only for the first chunk of this dataset
        # include_header = not self._state[tbl]["header_written"]
        # write_opts = pacsv.WriteOptions(include_header=include_header)

        opts = pacsv.WriteOptions(
                    delimiter="|",
                    quoting_style="none",      # never add quotes
                    include_header=True        # keep header (see note below)
                )
        buf = pa.BufferOutputStream()
        pacsv.write_csv(table_pa, buf,write_options=opts)
        chunk_bytes = buf.getvalue().to_pybytes()

        # Mark header as written after first chunk
        self._state[tbl]["header_written"] = True
        rel = self._state[tbl]["rel"]

        if self.mode == "local":
            path = os.path.join(self.local_out, rel.replace("/", os.sep))
            with open(path, "ab") as f:
                # Bytes are already UTF-8 from Arrow; we just append them.
                f.write(chunk_bytes)
            return rel

        tmp = self._state[tbl]["temp_path"]
        with open(tmp, "ab") as f:
            f.write(chunk_bytes)
        return rel

    # ---------- Upload temp file (cloud) + success marker ----------
    def write_success_marker(self, total_rows: int, parts: int, extras: dict, dataset_name: str):
        st = self._state.get(dataset_name)
        if st and self.mode in ("adls", "blob") and st["temp_path"]:
            tmp = st["temp_path"]
            rel = st["rel"]

            # ensure no stale file or siblings
            self._cleanup_previous_csvs(dataset_name, rel)
            self._delete_remote(rel)

            if self.mode == "adls":
                fc = self.fs.get_file_client(rel)
                with open(tmp, "rb") as f:
                    fc.upload_data(f, overwrite=True, max_concurrency=1)
                print(f"[sink] uploaded → abfss://{self.container}@{self.account}.dfs.core.windows.net/{rel}")
            else:
                bc = self.cc.get_blob_client(rel)
                with open(tmp, "rb") as f:
                    bc.upload_blob(f, overwrite=True, max_concurrency=1)
                print(f"[sink] uploaded → https://{self.account}.blob.core.windows.net/{self.container}/{rel}")

            try:
                os.remove(tmp)
            except Exception:
                pass

        rel_success = self._success_relpath_for(dataset_name)
        marker = {
            "dataset": dataset_name,
            "total_rows": total_rows,
            "parts": parts,
            "format": "csv",
            "data_path": self._data_relpath_for(dataset_name),
            "ingestion_date": self.ingestion_date,
            "run_ts": self.run_ts,
            "extras": extras,
            "created_utc": datetime.utcnow().isoformat() + "Z",
        }
        payload = (json.dumps(marker, indent=2) + "\n").encode("utf-8")

        if self.mode == "local":
            path = os.path.join(self.local_out, rel_success.replace("/", os.sep))
            os.makedirs(os.path.dirname(path), exist_ok=True)
            with open(path, "wb") as f:
                f.write(payload)
            print(f"[sink] wrote success → {path}")
            return

        if self.mode == "adls":
            fc = self.fs.get_file_client(rel_success)
            fc.upload_data(payload, overwrite=True, max_concurrency=1)
            print(f"[sink] wrote success → abfss://{self.container}@{self.account}.dfs.core.windows.net/{rel_success}")
            return

        bc = self.cc.get_blob_client(rel_success)
        bc.upload_blob(payload, overwrite=True, max_concurrency=1)
        print(f"[sink] wrote success → https://{self.account}.blob.core.windows.net/{self.container}/{rel_success}")


# -------------------- Watermark registry --------------------

class run_registry:
    """
    Persists per-table watermark JSON at WATERMARK_DIR/<table>.json
    (You can set WATERMARK_DIR to e.g. `${OUTPUT_BASE_PATH}/json logs/watermarks`.)
    """
    def __init__(self):
        self.local_out = _get("LOCAL_OUTPUT_DIR")
        self.account = _get("STORAGE_ACCOUNT")
        self.container = _get("FILE_SYSTEM")
        self.storage_kind = _get("STORAGE_KIND", "adls").lower()
        self.dir = (_get("WATERMARK_DIR", "moodle/prod/run_registry")).strip().strip("/")

        if self.local_out:
            self.mode = "local"
            os.makedirs(os.path.join(self.local_out, self.dir.replace("/", os.sep)), exist_ok=True)
            return

        if not self.account or not self.container:
            raise RuntimeError("For cloud registry, set STORAGE_ACCOUNT and FILE_SYSTEM (container).")

        cred = _default_azure_credential()
        if self.storage_kind == "adls":
            self.mode = "adls"
            acct_url = f"https://{self.account}.dfs.core.windows.net"
            self.dls = DataLakeServiceClient(account_url=acct_url, credential=cred)
            self.fs = self.dls.get_file_system_client(self.container)
        elif self.storage_kind == "blob":
            self.mode = "blob"
            acct_url = f"https://{self.account}.blob.core.windows.net"
            self.bsc = BlobServiceClient(account_url=acct_url, credential=cred)
            self.cc = self.bsc.get_container_client(self.container)
        else:
            raise RuntimeError("STORAGE_KIND must be 'adls' or 'blob'.")

    def _rel(self, table: str) -> str:
        return f"{self.dir}/{table}.json"

    def load(self, table: str) -> Optional[Dict[str, Any]]:
        rel = self._rel(table)

        try:
            if self.mode == "adls":
                file = self.fs.get_file_client(rel)
                data = file.download_file().readall()
                return json.loads(data.decode("utf-8"))

            # blob
            bc = self.cc.get_blob_client(rel)
            data = bc.download_blob(max_concurrency=1).readall()
            return json.loads(data.decode("utf-8"))

        except ResourceNotFoundError:
            return None
        except FileNotFoundError:
            return None

    def save(self, table: str, column: str, value: Any):
        rel = self._rel(table)
        payload = {
            "table": table,
            "column": column,
            "value": value,
            "updated_utc": datetime.utcnow().isoformat() + "Z",
        }
        data = (json.dumps(payload, indent=2) + "\n").encode("utf-8")

        if self.mode == "adls":
            file = self.fs.get_file_client(rel)
            file.upload_data(data, overwrite=True, max_concurrency=1)
            print(f"[wm] saved → abfss://{self.container}@{self.account}.dfs.core.windows.net/{rel}")
            return

        # blob
        bc = self.cc.get_blob_client(rel)
        bc.upload_blob(data, overwrite=True, max_concurrency=1)
        print(f"[wm] saved → https://{self.account}.blob.core.windows.net/{self.container}/{rel}")
