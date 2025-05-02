import datetime
import json
import os
from dataclasses import dataclass

import numpy as np
import xarray as xr
from obstore.store import S3Store
from virtualizarr import ChunkManifest, ManifestArray
from virtualizarr.manifests.group import ManifestGroup
from virtualizarr.manifests.store import ManifestStore, ObjectStoreRegistry
from virtualizarr.manifests.utils import create_v3_array_metadata
from virtualizarr.parallel import get_executor

import jp2io
from jp2io.index import TilesRange


def make_manifest_group(path: str, ranges: TilesRange, group_name: str = "data") -> ManifestGroup:
    tile_shape = (11, 11)

    tile_offsets = np.array(ranges.tiles_position, dtype=np.uint64).reshape(tile_shape)
    tile_lengths = np.array(ranges.tiles_length, dtype=np.uint64).reshape(tile_shape)
    paths = np.full_like(tile_offsets, path, dtype=np.dtypes.StringDType)

    chunkmanifest = ChunkManifest.from_arrays(
        paths=paths,
        offsets=tile_offsets,
        lengths=tile_lengths,
        validate_paths=False,
    )

    arraymetadata = create_v3_array_metadata(
        shape=(10980, 10980),
        data_type=np.dtype(np.uint16),
        chunk_shape=(1024, 1024),
        fill_value=0,
        codecs=[
            {
                # TODO: are both needed?...
                "name": "jp2io.zarr.Sentinel2Jpeg2000Codec",
                "id": "jp2io.zarr.Sentinel2Jpeg2000Codec",
                # TODO: add tile specification of this band (it depends on the resolution etc)
            }
        ],
        dimension_names=("y", "x"),
    )
    manifest = ManifestArray(metadata=arraymetadata, chunkmanifest=chunkmanifest)

    manifest_group = ManifestGroup(arrays={group_name: manifest})
    return manifest_group


def extract_time(ds: xr.Dataset, pid: str) -> xr.Dataset:
    date_format = "%Y%m%dT%H%M%S"
    date_str = pid.split("_")[2]
    time = datetime.datetime.strptime(date_str, date_format)
    return ds.assign_coords(time=time)


@dataclass(frozen=True)
class Sentinel2Datacube:
    store_registry: ObjectStoreRegistry
    tlm_provider: jp2io.ParquetTLMProvider

    def open_band(self, pid: str, bid: str) -> xr.Dataset:
        tlm = self.tlm_provider.get_tlm(pid, bid)
        assert isinstance(tlm, jp2io.index.VirtualTLMIndex)
        ranges = tlm.into_tiles_range()

        path = f"s3://DIAS{tlm.path}"  # assumes CDSE S3
        manifest_group = make_manifest_group(path, ranges, group_name=bid)

        ms = ManifestStore(group=manifest_group, store_registry=self.store_registry)
        ms = ms.to_virtual_dataset()
        ms = extract_time(ms, pid)
        return ms

    def open_r10m(self, pid: str) -> xr.Dataset:
        b02 = self.open_band(pid, "B02")
        b03 = self.open_band(pid, "B03")
        b04 = self.open_band(pid, "B04")
        b08 = self.open_band(pid, "B08")
        return xr.merge([b02, b03, b04, b08])

    def open_all(self) -> xr.Dataset:
        def dateof(pid: str) -> str:
            return pid.split("_")[2]

        # we can get the list of existing products from the parquet of TLM
        # but in practice this information would come from a catalog (eg STAC)
        product_ids = set(self.tlm_provider.table["product_id"])

        # remove duplicate dates as I don't know how to deal with them yet
        product_per_date = {dateof(pid): pid for pid in product_ids}
        product_ids = list(product_per_date.values())

        # sort by ascending date
        product_ids = sorted(product_ids, key=dateof)

        executor = get_executor(parallel="dask")
        with executor() as exec:
            ds = list(exec.map(self.open_r10m, product_ids))

        # 'minimal' to have lazy loading
        return xr.concat(ds, dim="time", coords="minimal", data_vars="minimal")


def get_credentials():
    return {
        "access_key_id": os.environ["CDSE_ACCESS_KEY_ID"],
        "secret_access_key": os.environ["CDSE_SECRET_ACCESS_KEY"],
        "token": None,
        "expires_at": None,
    }


def main_export_to_kerchunk(tlm_index_path: str, output_path: str):
    store = S3Store(
        bucket="DIAS",
        config={"endpoint": "https://eodata.dataspace.copernicus.eu"},
        credential_provider=get_credentials,  # type: ignore
    )
    store_registry = ObjectStoreRegistry({"s3://DIAS": store})

    tlm_provider = jp2io.ParquetTLMProvider.from_local_file(tlm_index_path)

    datacube = Sentinel2Datacube(store_registry=store_registry, tlm_provider=tlm_provider)

    ds = datacube.open_all()

    if output_path.endswith(".json"):
        kerchunk = ds.virtualize.to_kerchunk(format="json")
        with open(output_path, "w") as f:
            json.dump(kerchunk, f, indent=2)
    elif output_path.endswith(".kerchunk"):
        ds.virtualize.to_kerchunk(output_path, format="parquet")
    elif output_path.endswith(".icechunk"):
        import icechunk

        storage = icechunk.local_filesystem_storage(output_path)
        config = icechunk.RepositoryConfig.default()
        config.set_virtual_chunk_container(
            icechunk.VirtualChunkContainer("s3", "s3://", icechunk.s3_store(endpoint_url=store.config["endpoint"]))
        )
        credentials = icechunk.containers_credentials(
            s3=icechunk.s3_credentials(
                access_key_id=os.environ["CDSE_ACCESS_KEY_ID"],
                secret_access_key=os.environ["CDSE_SECRET_ACCESS_KEY"],
            )
        )

        repo = icechunk.Repository.create(storage, config, credentials)
        session = repo.writable_session("main")
        ds.virtualize.to_icechunk(session.store)
        session.commit("init")
    else:
        raise ValueError("unsupported output path extension (use .json or .kerchunk")

    print("dataset exported as kerchunk to", output_path)


def cli_export_to_kerchunk():
    import fire

    fire.Fire(main_export_to_kerchunk)
