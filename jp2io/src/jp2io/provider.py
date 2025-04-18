from __future__ import annotations

import abc
import functools
import io
import weakref
from dataclasses import dataclass
from typing import Any, TypedDict

import pyarrow.parquet as pq
from typing_extensions import override

from jp2io.exception import TLMIndexNotFound
from jp2io.index import TLMIndex, TLMMetadata


class TLMProvider(abc.ABC):
    @abc.abstractmethod
    def get_tlm(self, product_id: str, band_id: str) -> TLMIndex:
        """
        Returns a TLMIndex object for the given product_id and band_id.

        Raises:
            TLMIndexNotFound: if no TLMIndex object is found.
        """


class _ParquetTLMTable(TypedDict):
    product_id: list[str]
    band_id: list[str]
    path: list[str]
    index: list[bytes]


@dataclass(frozen=True)
class ParquetTLMProvider(TLMProvider):
    table: _ParquetTLMTable

    @staticmethod
    def from_pyarray(table: Any) -> ParquetTLMProvider:
        table = table.to_pydict()
        return ParquetTLMProvider(table=table)

    @staticmethod
    def from_local_file(path: str) -> ParquetTLMProvider:
        db = pq.read_table(path)
        return ParquetTLMProvider.from_pyarray(db)

    @override
    def get_tlm(self, product_id: str, band_id: str) -> TLMIndex:
        table = self.table
        for pid, bid, path, index in zip(table["product_id"], table["band_id"], table["path"], table["index"]):
            if pid == product_id and bid == band_id:
                meta = TLMMetadata(
                    product_id=product_id,
                    band_id=band_id,
                    path=path,
                )
                tlm_index = TLMIndex.from_bytes(index, meta)
                return tlm_index

        raise TLMIndexNotFound(f"Could not find TLM index for product {product_id} and band {band_id}")


# from https://stackoverflow.com/a/68052994
def weak_lru(maxsize: int = 128, typed: bool = False):  # type: ignore
    'LRU Cache decorator that keeps a weak reference to "self"'

    def wrapper(func):  # type: ignore
        @functools.lru_cache(maxsize, typed)
        def _func(_self, *args, **kwargs):  # type: ignore
            return func(_self(), *args, **kwargs)

        @functools.wraps(func)
        def inner(self, *args, **kwargs):  # type: ignore
            return _func(weakref.ref(self), *args, **kwargs)

        return inner

    return wrapper


@dataclass(frozen=True)
class S3TLMProvider(TLMProvider):
    s3_path_pattern: str
    """ must contain {level} and {mgrs_tile} as a placeholder """
    s3_client: Any

    @override
    def get_tlm(self, product_id: str, band_id: str) -> TLMIndex:
        mgrs_tile = product_id.split("_")[5][1:]
        level = product_id.split("_")[1][3:]
        provider: ParquetTLMProvider = self._get_provider_for(level, mgrs_tile)
        return provider.get_tlm(product_id, band_id)

    @weak_lru(maxsize=32)  # type: ignore
    def _get_provider_for(self, level: str, mgrs_tile: str) -> ParquetTLMProvider:
        s3_path = self.s3_path_pattern.format(level=level, mgrs_tile=mgrs_tile)
        bucket, key = s3_path.removeprefix("s3://").split("/", maxsplit=1)

        try:
            object = self.s3_client.get_object(Bucket=bucket, Key=key)
        except self.s3_client.exceptions.NoSuchKey:
            raise TLMIndexNotFound(f"Could not find TLM Parquet file for collection {level} and tile {mgrs_tile}")

        body = io.BytesIO(object["Body"].read())
        table = pq.read_table(body)
        return ParquetTLMProvider.from_pyarray(table)
