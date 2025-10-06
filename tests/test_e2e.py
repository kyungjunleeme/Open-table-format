import os, subprocess, sys, pytest

WAREHOUSE = os.getenv("WAREHOUSE", "s3://iceberg/warehouse")
DATAPATH  = os.getenv("DATAPATH",  "s3://iceberg/data")

@pytest.mark.e2e
def test_flow():
    assert subprocess.run([sys.executable, "-c", "import open_table_format"], check=False).returncode == 0
    # gen/upload/append/add_files/inspect 를 별도 스크립트 대신 UI/CLI 조합으로도 가능
