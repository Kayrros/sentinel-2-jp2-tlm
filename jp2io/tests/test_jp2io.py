import os
import time
from typing import Any

import numpy as np
import pytest
import rasterio

from jp2io import JP2IOException, ParquetTLMProvider, S3TLMProvider, TLMIndex, TLMProvider
from jp2io.exception import TLMIndexNotFound

uri = "https://storage.googleapis.com/gcp-public-data-sentinel-2/L2/tiles/31/U/DQ/S2A_MSIL2A_20241016T105031_N0511_R051_T31UDQ_20241016T151206.SAFE/GRANULE/L2A_T31UDQ_A048668_20241016T105303/IMG_DATA/R10m/T31UDQ_20241016T105031_B03_10m.jp2"

product_id = "S2A_MSIL2A_20241016T105031_N0511_R051_T31UDQ_20241016T151206"
mgrs_tile = "31UDQ"
band_id = "B03"

window = rasterio.windows.Window(8800, 8800, 100, 100)

# GCP seems to return a lot of 429 lately
open_options = {
    "GDAL_HTTP_MAX_RETRY": 2,
    "GDAL_HTTP_RETRY_DELAY": 1,
}


@pytest.fixture
def tlm_provider() -> TLMProvider:
    base = os.path.dirname(os.path.realpath(__file__))
    return ParquetTLMProvider.from_local_file(f"{base}/{mgrs_tile}.parquet")


@pytest.fixture
def s3_tlm_provider(s3_client: Any) -> TLMProvider:
    from conftest import TLM_BUCKET

    return S3TLMProvider(
        s3_path_pattern=f"s3://{TLM_BUCKET}/v1/{{level}}-{{mgrs_tile}}.parquet",
        s3_client=s3_client,
    )


@pytest.fixture
def tlm_index(tlm_provider: TLMProvider) -> TLMIndex:
    tlm_index = tlm_provider.get_tlm(product_id, band_id)
    return tlm_index


def test_uri_invalid(tlm_index: TLMIndex) -> None:
    from jp2io.index import VirtualTLMIndex

    assert isinstance(tlm_index, VirtualTLMIndex)
    tlm_index.make_vsi_file_for_uri(f"/vsis3/{uri}").close()
    with pytest.raises(JP2IOException):
        tlm_index.make_vsi_file_for_uri(f"{uri}").close()


def test_crop_from_gcp(tlm_index: TLMIndex) -> None:
    with tlm_index.open(f"/vsicurl/{uri}", open_options) as src:
        a = src.read(1, window=window)
        assert a.shape == (100, 100)
        assert a.dtype == np.uint16


def test_crop_using_s3provider(s3_tlm_provider: TLMProvider) -> None:
    tlm_index = s3_tlm_provider.get_tlm(product_id, band_id)
    with tlm_index.open(f"/vsicurl/{uri}", open_options) as src:
        a = src.read(1, window=window)
        assert a.shape == (100, 100)
        assert a.dtype == np.uint16


def test_s3provider_is_cached(s3_tlm_provider: TLMProvider) -> None:
    t1 = time.time()
    s3_tlm_provider.get_tlm(product_id, band_id)
    t2 = time.time()
    s3_tlm_provider.get_tlm(product_id, band_id)
    t3 = time.time()
    assert t3 - t2 < (t2 - t1) * 0.1


def test_localprovider_product_not_found(tlm_provider: TLMProvider) -> None:
    with pytest.raises(TLMIndexNotFound):
        # this product is too recent compared to the parquet in ./tests/
        tlm_provider.get_tlm("S2C_MSIL2A_20250305T104951_N0511_R051_T31UDQ_20250305T144913", "B02")


def test_s3provider_product_not_found(s3_tlm_provider: TLMProvider) -> None:
    with pytest.raises(TLMIndexNotFound):
        # this product does not exist, but the MGRS tile might exist
        s3_tlm_provider.get_tlm("S2B_MSIL1C_20170730T111111_N9999_R051_T31TCN_20170730T111111", "B02")


def test_s3provider_parquet_not_found(s3_tlm_provider: TLMProvider) -> None:
    with pytest.raises(TLMIndexNotFound):
        # this MGRS tile does not exist
        s3_tlm_provider.get_tlm("S2B_MSIL1C_20170730T111111_N9999_R051_T99AAA_20170730T111111", "B02")
