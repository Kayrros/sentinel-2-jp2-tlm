import concurrent.futures
import os
import time

import numpy as np
import rasterio.windows
from numpy._typing import NDArray

from jp2io.index import TLMIndex
from jp2io.provider import ParquetTLMProvider

PRODUCTS = (
    "S2B_MSIL2A_20230403T105629_N0509_R094_T31UDQ_20230403T123134",
    "S2B_MSIL2A_20230503T105619_N0509_R094_T31UDQ_20230503T143028",
    "S2B_MSIL2A_20230513T105619_N0509_R094_T31UDQ_20230513T122244",
    "S2A_MSIL2A_20230525T105031_N0509_R051_T31UDQ_20230525T162958",
    "S2A_MSIL2A_20230528T105621_N0509_R094_T31UDQ_20230528T165402",
    "S2B_MSIL2A_20230602T105629_N0509_R094_T31UDQ_20230602T123043",
    "S2A_MSIL2A_20230604T104621_N0509_R051_T31UDQ_20230604T165401",
    "S2A_MSIL2A_20230607T105621_N0509_R094_T31UDQ_20230607T171312",
    "S2A_MSIL2A_20230614T105031_N0509_R051_T31UDQ_20230614T163601",
    "S2A_MSIL2A_20230624T104621_N0509_R051_T31UDQ_20230624T170454",
    "S2B_MSIL2A_20230821T105629_N0509_R094_T31UDQ_20230821T140250",
    "S2B_MSIL2A_20230907T104629_N0509_R051_T31UDQ_20230907T153138",
    "S2B_MSIL2A_20230910T105629_N0509_R094_T31UDQ_20230910T141644",
    "S2A_MSIL2A_20230915T105701_N0509_R094_T31UDQ_20230915T152500",
    "S2B_MSIL2A_20231007T104829_N0509_R051_T31UDQ_20231007T135731",
    "S2B_MSIL2A_20231216T105349_N0510_R051_T31UDQ_20231216T123152",
    "S2B_MSIL2A_20240805T105619_N0511_R094_T31UDQ_20240805T143427",
    "S2A_MSIL2A_20240810T105621_N0511_R094_T31UDQ_20240810T152055",
    "S2B_MSIL2A_20240822T104619_N0511_R051_T31UDQ_20240822T151147",
    "S2A_MSIL2A_20240919T105731_N0511_R094_T31UDQ_20240919T171547",
)

BASE_URI = "https://storage.googleapis.com/gcp-public-data-sentinel-2/L2/tiles/31/U/DQ"


def main(use_tlm: bool, output_npy: str | None = None) -> None:
    mgrs_tile = PRODUCTS[0].split("_")[5][1:]
    base = os.path.dirname(os.path.realpath(__file__))
    provider = ParquetTLMProvider.from_local_file(f"{base}/tests/{mgrs_tile}.parquet")

    window = rasterio.windows.Window(5500, 5500, 512, 512)
    env_options = {
        "GDAL_HTTP_MAX_RETRY": 4,
        "GDAL_HTTP_RETRY_DELAY": 1,
    }

    def info_of_product(product_id: str) -> tuple[TLMIndex, str]:
        tlm = provider.get_tlm(product_id, "B02")

        # construct the uri using the base GCP path and the granule info
        granule_path = tlm.path.split("/", maxsplit=7)[-1]
        uri = f"{BASE_URI}/{granule_path}"

        return tlm, uri

    if use_tlm:

        def read_window(product_id: str) -> NDArray[np.uint16]:
            tlm, uri = info_of_product(product_id)

            with tlm.open(f"/vsicurl/{uri}", env_options=env_options) as src:
                return src.read(1, window=window)
    else:

        def read_window(product_id: str) -> NDArray[np.uint16]:
            _, uri = info_of_product(product_id)

            with rasterio.Env(
                GDAL_DISABLE_READDIR_ON_OPEN="EMPTY_DIR",
                CPL_VSIL_CURL_ALLOWED_EXTENSIONS="jp2",
                **env_options,
            ):
                with rasterio.open(uri) as src:
                    return src.read(1, window=window)

    t0 = time.time()
    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
        arrays = list(pool.map(read_window, PRODUCTS))
    t1 = time.time()

    print(f"{len(arrays)} rasters read in {t1 - t0:.3} seconds")

    if output_npy:
        arrays = np.stack(arrays, axis=0)
        np.save(output_npy, arrays)
        print(f"Rasters saved as {output_npy}")


if __name__ == "__main__":
    import fire

    fire.Fire(main)
