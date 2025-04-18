import os
from typing import Any

import pytest

TLM_BUCKET: str | None = os.environ.get("JP2IO_TLM_BUCKET")

try:
    import boto3

    assert TLM_BUCKET is not None
    _s3_client = boto3.client("s3")
    s = _s3_client.list_objects_v2(
        Bucket=TLM_BUCKET,
        Prefix="/",
        MaxKeys=0,
    )
except Exception:
    _s3_client = None


@pytest.fixture(scope="session")
def maybe_skip_s3() -> None:
    if _s3_client is None:
        pytest.skip("skipped because AWS S3 credentials are not found")


@pytest.fixture(scope="session")
def s3_client(maybe_skip_s3: None) -> Any:
    return _s3_client
