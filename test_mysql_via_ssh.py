import os, tempfile, json, platform
from datetime import datetime, timezone
from contextlib import contextmanager

from dotenv import load_dotenv
from sshtunnel import SSHTunnelForwarder
import pymysql
# from azure.storage.blob import BlobServiceClient, ResourceExistsError

load_dotenv()

def _require(name):
    v = os.getenv(name)
    if not v:
        raise RuntimeError(f"Missing required env: {name}")
    return v

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

def test_mysql_over_ssh():
    print("== Testing SSH tunnel + MySQL on Windows ==")
    ssh_host = _require("SSH_HOST")
    ssh_port = int(os.getenv("SSH_PORT", "22"))
    ssh_user = _require("SSH_USERNAME")
    # ssh_passphrase = os.getenv("SSH_PASSPHRASE") or None

    db_host = os.getenv("DB_HOST", "127.0.0.1")
    db_port = int(os.getenv("DB_PORT", "3306"))
    # db_name = _require("DB_NAME")
    db_user = _require("DB_USERNAME")
    db_pass = _require("DB_PASSWORD")

    with temp_pem_from_env() as key_path:
        with SSHTunnelForwarder(
            (ssh_host, ssh_port),
            ssh_username=ssh_user,
            ssh_pkey=key_path,
            # ssh_private_key_password=ssh_passphrase,
            remote_bind_address=(db_host, db_port),
        ) as tunnel:
            tunnel.start()
            print(f"SSH tunnel established on localhost:{tunnel.local_bind_port}")

            conn = pymysql.connect(
                host="127.0.0.1",
                port=tunnel.local_bind_port,
                user=db_user,
                password=db_pass,
                # database=db_name,
                cursorclass=pymysql.cursors.DictCursor,
                connect_timeout=10,
                read_timeout=30,
                write_timeout=30,
            )
            with conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT NOW() AS server_time, DATABASE() AS db, VERSION() AS mysql_version;")
                    row = cur.fetchone()
            print("MySQL OK:", row)
            return row

if __name__ == "__main__":
    info = test_mysql_over_ssh()
    # optional_blob_write({"checked": datetime.now(timezone.utc).isoformat(), "mysql_info": info})
    print("All tests completed.")
