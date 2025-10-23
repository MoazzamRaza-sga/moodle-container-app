import os
import json
from datetime import datetime
from typing import Optional, Dict, Any

import pyarrow as pa
import pyarrow.csv as pacsv  # CSV writer

# Azure SDKs
from azure.identity import DefaultAzureCredential
from azure.storage.filedatalake import DataLakeServiceClient
from azure.storage.blob import BlobServiceClient, BlobClient, AppendBlobClient
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


# -------------------- Low-level storage helpers --------------------

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
    return bc.download_blob().readall()


# -------------------- CSV sink (single file per table per run) --------------------

class CSVSink:
    """
    Writes each table to ONE CSV file named <table>.csv.

    Data file:
      OUTPUT_BASE_PATH/<table>/<YYYY>/<MM>/<DD>/<table>.csv

    Success JSON:
      OUTPUT_BASE_PATH/json logs/<table>/<YYYY>/<MM>/<DD>/success.json

    Supports local, ADLS Gen2 (dfs), and Blob (append blob).
    """

    def __init__(self):
        # Required base path for final layout
        self.base_path = _require("OUTPUT_BASE_PATH").strip().strip("/")

        self.local_out = _get("LOCAL_OUTPUT_DIR")
        self.account = _get("STORAGE_ACCOUNT")
        self.container = _get("FILE_SYSTEM")  # ADLS filesystem or Blob container
        self.storage_kind = _get("STORAGE_KIND", "adls").lower()  # 'adls' or 'blob'

        # Dynamic date (YYYY/MM/DD) in OUTPUT_TZ
        tz_name = _get("OUTPUT_TZ", "Asia/Karachi")
        if ZoneInfo:
            now = datetime.now(ZoneInfo(tz_name))
        else:
            now = datetime.utcnow()
        self.y = f"{now.year:04d}"
        self.m = f"{now.month:02d}"
        self.d = f"{now.day:02d}"

        # For success marker payload
        self.run_ts = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
        self.ingestion_date = datetime.utcnow().strftime("%Y-%m-%d")

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

        # per-table state: { table: { "header_written": bool, "offset": int, "rel": str, "success_rel": str, ... } }
        self._state: Dict[str, Dict[str, Any]] = {}

    # ---------- Path helpers ----------
    def _data_dir_for(self, table: str) -> str:
        return f"{self.base_path}/{table}/{self.y}/{self.m}/{self.d}"

    def _data_relpath_for(self, table: str) -> str:
        return f"{self._data_dir_for(table)}/{table}.csv"

    def _success_relpath_for(self, table: str) -> str:
        # note: the folder name must literally be "json logs" (with space)
        return f"{self.base_path}/json logs/{table}/{self.y}/{self.m}/{self.d}/success.json"

    # ---------- File lifecycle per table ----------
    def _delete_if_exists(self, rel: str):
        try:
            if self.mode == "local":
                path = os.path.join(self.local_out, rel.replace("/", os.sep))
                if os.path.exists(path):
                    os.remove(path)
                return
            if self.mode == "adls":
                fc = self.fs.get_file_client(rel)
                fc.delete_file()
                return
            # blob
            bc = self.cc.get_blob_client(rel)
            bc.delete_blob(delete_snapshots="include")
        except ResourceNotFoundError:
            pass
        except FileNotFoundError:
            pass

    def _prepare_table(self, table: str):
        if table in self._state:
            return  # already prepared

        rel = self._data_relpath_for(table)
        success_rel = self._success_relpath_for(table)
        self._delete_if_exists(rel)

        if self.mode == "local":
            # Create parent dirs and empty file
            path = os.path.join(self.local_out, rel.replace("/", os.sep))
            os.makedirs(os.path.dirname(path), exist_ok=True)
            open(path, "wb").close()
            self._state[table] = {"header_written": False, "offset": 0, "rel": rel, "success_rel": success_rel}
            print(f"[sink] Writing → {path}")
            return

        if self.mode == "adls":
            fc = self.fs.get_file_client(rel)
            fc.create_file()  # create empty file
            self._state[table] = {"header_written": False, "offset": 0, "rel": rel, "success_rel": success_rel, "fc": fc}
            print(f"[sink] Writing → abfss://{self.container}@{self.account}.dfs.core.windows.net/{rel}")
            return

        # blob (Append Blob)
        abc = AppendBlobClient(account_url=f"https://{self.account}.blob.core.windows.net",
                               container_name=self.container,
                               blob_name=rel,
                               credential=_default_azure_credential())
        try:
            abc.create_append_blob()
        except Exception:
            # If it already exists and couldn't be deleted, try delete+recreate
            try:
                abc.delete_blob(delete_snapshots="include")
                abc.create_append_blob()
            except Exception:
                pass
        self._state[table] = {"header_written": False, "offset": 0, "rel": rel, "success_rel": success_rel, "abc": abc}
        print(f"[sink] Writing → https://{self.account}.blob.core.windows.net/{self.container}/{rel}")

    # ---------- Append a chunk ----------
    def write_table(self, table_pa: pa.Table, part_idx: int, dataset_name: str) -> str:
        """Keep external signature; dataset_name is the MySQL table name."""
        tbl = dataset_name
        self._prepare_table(tbl)

        # header on first chunk only
        include_header = not self._state[tbl]["header_written"]
        write_opts = pacsv.WriteOptions(include_header=include_header)

        buf = pa.BufferOutputStream()
        pacsv.write_csv(table_pa, buf, options=write_opts)
        data = buf.getvalue().to_pybytes()

        # Update state
        self._state[tbl]["header_written"] = True

        rel = self._state[tbl]["rel"]

        if self.mode == "local":
            path = os.path.join(self.local_out, rel.replace("/", os.sep))
            with open(path, "ab") as f:
                f.write(data)
            return rel

        if self.mode == "adls":
            fc = self._state[tbl]["fc"]
            offset = self._state[tbl]["offset"]
            fc.append_data(data=data, offset=offset, length=len(data))
            fc.flush_data(offset + len(data))
            self._state[tbl]["offset"] = offset + len(data)
            return rel

        # blob append
        abc: AppendBlobClient = self._state[tbl]["abc"]
        abc.append_block(data)
        return rel

    # ---------- Success marker ----------
    def write_success_marker(self, total_rows: int, parts: int, extras: dict, dataset_name: str):
        rel = self._success_relpath_for(dataset_name)
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
            path = os.path.join(self.local_out, rel.replace("/", os.sep))
            os.makedirs(os.path.dirname(path), exist_ok=True)
            with open(path, "wb") as f:
                f.write(payload)
            print(f"[sink] wrote success → {path}")
            return

        if self.mode == "adls":
            fc = self.fs.get_file_client(rel)
            # ensure parent path exists implicitly; upload will create
            fc.upload_data(payload, overwrite=True)
            print(f"[sink] wrote success → abfss://{self.container}@{self.account}.dfs.core.windows.net/{rel}")
            return

        # blob
        bc = self.cc.get_blob_client(rel)
        bc.upload_blob(payload, overwrite=True)
        print(f"[sink] wrote success → https://{self.account}.blob.core.windows.net/{self.container}/{rel}")


# -------------------- Watermark registry (unchanged behavior) --------------------

class WatermarkRegistry:
    """
    Persists per-table watermark JSON at WATERMARK_DIR/<table>.json
    You can point WATERMARK_DIR anywhere (e.g., under OUTPUT_BASE_PATH) if you want.
    """
    def __init__(self):
        self.local_out = _get("LOCAL_OUTPUT_DIR")
        self.account = _get("STORAGE_ACCOUNT")
        self.container = _get("FILE_SYSTEM")
        self.storage_kind = _get("STORAGE_KIND", "adls").lower()
        self.dir = (_get("WATERMARK_DIR", "watermarks")).strip().strip("/")

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
            if self.mode == "local":
                path = os.path.join(self.local_out, rel.replace("/", os.sep))
                if not os.path.exists(path):
                    return None
                with open(path, "rb") as f:
                    return json.loads(f.read().decode("utf-8"))

            if self.mode == "adls":
                file = self.fs.get_file_client(rel)
                data = file.download_file().readall()
                return json.loads(data.decode("utf-8"))

            # blob
            bc = self.cc.get_blob_client(rel)
            data = bc.download_blob().readall()
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

        if self.mode == "local":
            path = os.path.join(self.local_out, rel.replace("/", os.sep))
            os.makedirs(os.path.dirname(path), exist_ok=True)
            with open(path, "wb") as f:
                f.write(data)
            print(f"[wm] saved → {path}")
            return

        if self.mode == "adls":
            file = self.fs.get_file_client(rel)
            file.upload_data(data, overwrite=True)
            print(f"[wm] saved → abfss://{self.container}@{self.account}.dfs.core.windows.net/{rel}")
            return

        # blob
        bc = self.cc.get_blob_client(rel)
        bc.upload_blob(data, overwrite=True)
        print(f"[wm] saved → https://{self.account}.blob.core.windows.net/{self.container}/{rel}")
