# JP2IO: Inject pre-indexed TLMs into Sentinel-2 JPEG2000 for efficient remote cropping

This package injects TLM indexed by [../s2tlm-indexer/](../s2tlm-indexer/) on the fly, in order to make the current Sentinel-2 collections "cloud-ready" (efficient remote cropping) without reprocessing.

`jp2io` is made for:

- `rasterio` users, with the method `jp2io.index.TLMIndex.open`
- `xarray/zarr` users, with the `jp2io.zarr` module

## Installation

```bash
pip install .
```

## Usage for rasterio

See also `./demo.py`.

```bash
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

### Demonstration

The following commands demonstrate how injecting TLM on the fly when cropping reduces a lot the time to access the data:

```bash
$ uv sync
$ jp2io-update-openjpeg   # make sure to run this!
$ uv run demo.py --use-tlm=True --output-npy=rasters.npy
$ uv run demo.py --use-tlm=False --output-npy=rasters.npy
```

The actual timing results depends on network conditions and throttling from Google Cloud Storage, but according to our tests one can expect a consistent x10 improvement.

## Usage for xarray

The library offers a codec and means to create a kerchunk file from the TLM indexes.

For now it relies on unreleased VirtualiZarr features to create the kerchunk file.

```bash
pip install jp2io[zarr]
```

```bash
jp2io-make-virtual-cube tests/31UDQ.parquet cube-L2A-31UDQ.json
```

```python
import jp2io.zarr.codec  # register the codec

cdse_storage_options = dict(
    remote_protocol="s3",
    remote_options=dict(
        key=os.environ["CDSE_ACCESS_KEY_ID"],
        secret=os.environ["CDSE_SECRET_ACCESS_KEY"],
        endpoint_url="https://eodata.dataspace.copernicus.eu",
    ),
)

ds = xr.open_dataset("./cube-L2A-31UDQ.json", engine="kerchunk", backend_kwargs={"storage_options": cdse_storage_options})
```
