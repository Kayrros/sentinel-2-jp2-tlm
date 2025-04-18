# JP2IO: Inject pre-indexed TLMs into Sentinel-2 JPEG2000 for efficient remote cropping

This package injects TLM indexed by [../s2tlm-indexer/](../s2tlm-indexer/) on the fly, in order to make the current Sentinel-2 collections "cloud-ready" (efficient remote cropping) without reprocessing.

## Installation

```bash
pip install .

# from a venv, or inside a Dockerfile:
# this is required for as long as https://github.com/rasterio/rasterio-wheels/issues/138 is opened.
# (cmake and a C toolchain are required)
jp2io-update-openjpeg
```

(The package is not yet published on pypi.)

## Usage

```python
from jp2io import ParquetTLMProvider

product_id = ...
band_id = ...
mgrs_tile = ...
uri = ...

provider = ParquetTLMProvider.from_local_file(f"{mgrs_tile}.parquet")
tlm_index = tlm_provider.get_tlm(product_id, band_id)

# vsicurl or vsis3, see GDAL documentation
with tlm_index.open(f"/vsicurl/{uri}") as src:
    array = src.read(1, window=window)
```

## Demonstration

The following commands demonstrate how injecting TLM on the fly when cropping reduces a lot the time to access the data:

```
$ uv sync
$ jp2io-update-openjpeg   # make sure to run this!
$ uv run demo.py --use-tlm=True --output-npy=rasters.npy
$ uv run demo.py --use-tlm=False --output-npy=rasters.npy
```

The actual timing results depends on network conditions and throttling from Google Cloud Storage, but according to our tests one can expect a consistent x10 improvement.
