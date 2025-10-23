import os
import json
from datetime import datetime
from typing import Optional

import pyarrow as pa
import pyarrow.csv as pacsv  # CSV writer

# Azure SDKs
from azure.identity import DefaultAzureCredential
from azure.storage.filedatalake import DataLakeServiceClient
from azure.storage.blob import BlobServiceClient, BlobClient

try:
    # Python 3.9+
    from zoneinfo import ZoneInfo  # type: ignore
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
    """
    Uses DefaultAzureCredential; honors managed identity client id via AZURE_CLIENT_ID.
    """
    mi_client_id = _get("AZURE_CLIENT_ID")
    if mi_client_id:
        return DefaultAzureCredential(managed_identity_client_id=mi_client_id)
    return DefaultAzureCredential()


# -------------------- Lightweight storage helpers --------------------

def _get_adls_clients(account: str, filesystem: str):
    cred = _default_azure_credential()
    acct_url = f"https://{account}.dfs.core.windows.net"
    dls = DataLakeServiceClient(account_url=acct_url, credential=cred)
    fs = dls.get_file_system_client(filesystem)
    return dls, fs

def download_adls_path_to_bytes(rel_path: str,
                                account: Optional[str] = None,
                                filesystem: Optional[str] = None) -> bytes:
    """
    Download a file from ADLS Gen2 (DFS endpoint) to memory.
    If account/filesystem are not provided, falls back to STORAGE_ACCOUNT/FILE_SYSTEM.
    """
    account = account or _require("STORAGE_ACCOUNT")
    filesystem = filesystem or _require("FILE_SYSTEM")
    _, fs = _get_adls_clients(account, filesystem)
    file_client = fs.get_file_client(rel_path)
    return file_client.download_file().readall()

def download_blob_url_to_bytes(blob_url: str) -> bytes:
    """
    Download from a full Blob URL. Works with SAS or MI (no SAS).
    """
    cred = _default_azure_credential()
    bc = BlobClient.from_blob_url(blob_url, credential=cred)
    return bc.download_blob().readall()


# -------------------- CSV sink --------------------

class CSVSink:
    """
    Writes pyarrow Tables to:
      - Local directory (if LOCAL_OUTPUT_DIR set), OR
      - ADLS Gen2 (dfs endpoint), OR
      - Blob (blob endpoint).
    Also writes a _SUCCESS.json marker.

    Directory strategy:
      - If OUTPUT_DIR is set, use it exactly (e.g. "2025/09/24").
      - Otherwise, default to "YYYY/MM/DD" for the current date in OUTPUT_TZ (default Asia/Karachi).
    """
    def __init__(self):
        self.local_out = _get("LOCAL_OUTPUT_DIR")
        self.account = _get("STORAGE_ACCOUNT")
        self.container = _get("FILE_SYSTEM")  # ADLS filesystem or Blob container
        self.storage_kind = _get("STORAGE_KIND", "adls").lower()  # 'adls' or 'blob'

        now = datetime.utcnow()
        self.today_dir = f"{now.year}/{now.month}/{now.day}"

        # If OUTPUT_DIR is set, use it verbatim; else use dynamic today_dir
        self.output_dir = (_get("OUTPUT_DIR") or self.today_dir).strip().strip("/")

        # Keep these for the success marker only (informational)
        self.run_ts = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
        self.ingestion_date = datetime.utcnow().strftime("%Y-%m-%d")

        if self.local_out:
            os.makedirs(self.local_out, exist_ok=True)
            self.mode = "local"
            print(f"[sink] Using local output: {self.local_out}")
            print(f"[sink] Writing under: {self._rel_dir()}")
            return

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

        print(f"[sink] Writing under: {self._rel_dir()}")

    def _rel_dir(self) -> str:
        # Always prefer explicit/dynamic directory
        return self.output_dir

    def _build_rel_path(self, part_idx: int) -> str:
        return f"{self._rel_dir()}/part-{part_idx:05d}.csv"

    def write_table(self, table: pa.Table, part_idx: int) -> str:
        # Write Arrow table to CSV bytes
        buf = pa.BufferOutputStream()
        pacsv.write_csv(table, buf)  # no compression by default
        data = buf.getvalue().to_pybytes()
        rel = self._build_rel_path(part_idx)

        if self.mode == "local":
            out_path = os.path.join(self.local_out, rel.replace("/", os.sep))
            os.makedirs(os.path.dirname(out_path), exist_ok=True)
            with open(out_path, "wb") as f:
                f.write(data)
            print(f"[sink] wrote {len(data)} bytes → {out_path}")
            return out_path

        if self.mode == "adls":
            file = self.fs.get_file_client(rel)
            file.upload_data(data, overwrite=True)
            print(f"[sink] wrote {len(data)} bytes → abfss://{self.container}@{self.account}.dfs.core.windows.net/{rel}")
            return rel

        # blob
        bc = self.cc.get_blob_client(rel)
        bc.upload_blob(data, overwrite=True)
        print(f"[sink] wrote {len(data)} bytes → https://{self.account}.blob.core.windows.net/{self.container}/{rel}")
        return rel

    def write_success_marker(self, total_rows: int, parts: int, extras: dict):
        marker = {
            "table": "course",
            "total_rows": total_rows,
            "parts": parts,
            "format": "csv",
            "output_dir": self._rel_dir(),
            "ingestion_date": self.ingestion_date,
            "run_ts": self.run_ts,
            "extras": extras,
            "created_utc": datetime.utcnow().isoformat() + "Z",
        }
        payload = (json.dumps(marker, indent=2) + "\n").encode("utf-8")
        rel = f"{self._rel_dir()}/_SUCCESS.json"

        if self.mode == "local":
            path = os.path.join(self.local_out, rel.replace("/", os.sep))
            os.makedirs(os.path.dirname(path), exist_ok=True)
            with open(path, "wb") as f:
                f.write(payload)
            print(f"[sink] wrote marker → {path}")
            return

        if self.mode == "adls":
            file = self.fs.get_file_client(rel)
            file.upload_data(payload, overwrite=True)
            print(f"[sink] wrote marker → abfss://{self.container}@{self.account}.dfs.core.windows.net/{rel}")
            return

        # blob
        bc = self.cc.get_blob_client(rel)
        bc.upload_blob(payload, overwrite=True)
        print(f"[sink] wrote marker → https://{self.account}.blob.core.windows.net/{self.container}/{rel}")
