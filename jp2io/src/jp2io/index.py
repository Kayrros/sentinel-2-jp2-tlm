from __future__ import annotations

import abc
import contextlib
import struct
from dataclasses import dataclass
from typing import Any, Generator

import rasterio
from rasterio.io import MemoryFile
from typing_extensions import override

from jp2io.exception import JP2IOException
from jp2io.parsefile import JP2WithTLMSparseFile


@dataclass(frozen=True, slots=True)
class TLMMetadata(abc.ABC):
    product_id: str
    band_id: str
    path: str
    """ Path of the JP2 on CDSE S3. Does not include the s3://<bucket> prefix. """


class TLMIndex(abc.ABC):
    @property
    def product_id(self) -> str:
        return self._get_tlmmetadata().product_id

    @property
    def band_id(self) -> str:
        return self._get_tlmmetadata().band_id

    @property
    def path(self) -> str:
        return self._get_tlmmetadata().path

    @abc.abstractmethod
    def _get_tlmmetadata(self) -> TLMMetadata: ...

    @abc.abstractmethod
    @contextlib.contextmanager
    def open(self, uri: str, env_options: dict[str, Any] = {}) -> Generator[rasterio.DatasetReader]:
        """
        Parameters
        ----------
        uri
            Path to the raster.
            It should be in a format accepted by GDAL, for example starting with /vsicurl/ or /vsis3/ for remote access.
        env_options
            rasterio.open will happen in a rasterio.Env with some default options.
            Use this parameter to override/add options.
        """

    @staticmethod
    def from_bytes(buf: bytes, meta: TLMMetadata) -> TLMIndex:
        if len(buf) == 0:
            # empty index means that the TLM marker is already present
            return NoopTLMIndex(meta=meta)

        # u64       u64                 u32                 = (20 bytes)
        (file_size, position_first_sot, tlm_segment_length) = struct.unpack(">QQL", buf[:20])
        tlm_segment = buf[20 : 20 + tlm_segment_length]
        return VirtualTLMIndex(
            file_size=file_size,
            position_first_sot=position_first_sot,
            tlm_segment=tlm_segment,
            meta=meta,
        )


class UnsupportedJP2Exception(Exception):
    pass


@dataclass(frozen=True)
class TilesRange:
    tiles_position: list[int]
    tiles_length: list[int]


@dataclass(frozen=True)
class VirtualTLMIndex(TLMIndex):
    file_size: int
    position_first_sot: int
    tlm_segment: bytes
    meta: TLMMetadata

    @override
    def _get_tlmmetadata(self) -> TLMMetadata:
        return self.meta

    @override
    @contextlib.contextmanager
    def open(self, uri: str, env_options: dict[str, Any] = {}) -> Generator[rasterio.DatasetReader]:
        sparsefile = self.make_vsi_file_for_uri(uri)
        try:
            env = self.recommended_env_vars().copy()
            env |= env_options
            with rasterio.Env(**env):
                with rasterio.open(sparsefile.name) as src:
                    yield src
        finally:
            sparsefile.close()

    def recommended_env_vars(self) -> dict[str, Any]:
        return {
            "GDAL_DISABLE_READDIR_ON_OPEN": "EMPTY_DIR",
            "GDAL_INGESTED_BYTES_AT_OPEN": self.position_first_sot,
            # not trivial to tweak the chunk size, maybe it should be tuned from a benchmark
            # because it will affect the "ingested bytes at open" too, even though we know how much we want to read
            # a solution could be a custom reader...
            # "CPL_VSIL_CURL_CHUNK_SIZE": "500000",
        }

    def make_vsi_file_for_uri(self, uri: str) -> JP2WithTLMSparseFile:
        """
        Parameters
        ----------
        uri
            Path to the raster.
            It should be in a format accepted by GDAL, for example starting with /vsicurl/ or /vsis3/ for remote access.
        """
        if uri.startswith("s3://") or uri.startswith("https://"):
            raise JP2IOException(f"Uri unsupported ('{uri}'): make sure to use /vsis3/ or /vsicurl/")

        tlm_mem = MemoryFile(self.tlm_segment, ext=".tlm")

        def make_content() -> bytes:
            tlm_segment_length = len(self.tlm_segment)
            content = "<VSISparseFile>"

            # JP2 until the end of the main header
            content += f"""
            <SubfileRegion>
                <Filename>{uri}</Filename>
                <DestinationOffset>0</DestinationOffset>
                <SourceOffset>0</SourceOffset>
                <RegionLength>{self.position_first_sot}</RegionLength>
            </SubfileRegion>
            """

            # TLM marker, concatenated to the main header
            content += f"""
            <SubfileRegion>
                <Filename>{tlm_mem.name}</Filename>
                <DestinationOffset>{self.position_first_sot}</DestinationOffset>
                <SourceOffset>0</SourceOffset>
                <RegionLength>{tlm_segment_length}</RegionLength>
            </SubfileRegion>
            """

            # JP2 after the main header
            content += f"""
            <SubfileRegion>
                <Filename>{uri}</Filename>
                <DestinationOffset>{self.position_first_sot + tlm_segment_length}</DestinationOffset>
                <SourceOffset>{self.position_first_sot}</SourceOffset>
                <RegionLength>{self.file_size - self.position_first_sot}</RegionLength>
            </SubfileRegion>
            """

            content += "</VSISparseFile>"
            content = content.encode("utf-8")
            return content

        content = make_content()
        jp2_mem = MemoryFile(content, ext=".xml")

        return JP2WithTLMSparseFile(
            name=f"/vsisparse/{jp2_mem.name}",
            _children=[jp2_mem, tlm_mem],
            _content=content,
        )

    def into_tiles_range(self) -> TilesRange:
        """
        See https://web.archive.org/web/20250209200219/https://ics.uci.edu/~dhirschb/class/267/papers/jpeg2000.pdf
        """
        tlm_marker = self.tlm_segment

        tlm, ltlm, ztlm, stlm = struct.unpack_from(">HHBB", tlm_marker)
        if tlm != 0xFF55:
            # if the TLM index is not valid
            raise UnsupportedJP2Exception()
        if ltlm != len(tlm_marker) - 2:
            # if the TLM index is not valid
            raise UnsupportedJP2Exception()
        if ztlm != 0:
            raise UnsupportedJP2Exception()

        if stlm & 0b00_11_0000 == 0b00_00_0000:
            ttlm_size = 0
        elif stlm & 0b00_11_0000 == 0b00_01_0000:
            ttlm_size = 8
        elif stlm & 0b00_11_0000 == 0b00_10_0000:
            ttlm_size = 16
        else:
            # not possible according to the spec
            raise UnsupportedJP2Exception()

        if stlm & 0b01000000 != 0:
            ptlm_size = 32
        else:
            ptlm_size = 16

        tiles_position = []
        tiles_length = []
        tile_index = 0
        cumulative_position = self.position_first_sot

        offset = 6
        while offset < len(tlm_marker):
            if ttlm_size == 0:
                ttlm = tile_index
            elif ttlm_size == 8:
                ttlm = struct.unpack_from(">B", tlm_marker, offset)[0]
                offset += 1
            elif ttlm_size == 16:
                ttlm = struct.unpack_from(">H", tlm_marker, offset)[0]
                offset += 2
            else:
                # not possible according to the spec
                raise UnsupportedJP2Exception()

            # assuming sorted tiles
            if ttlm != tile_index:
                raise UnsupportedJP2Exception()

            if ptlm_size == 0:
                ptlm = 0
            elif ptlm_size == 16:
                ptlm = struct.unpack_from(">H", tlm_marker, offset)[0]
                offset += 2
            elif ptlm_size == 32:
                ptlm = struct.unpack_from(">I", tlm_marker, offset)[0]
                offset += 4
            else:
                # not possible according to the spec
                raise UnsupportedJP2Exception()

            tiles_position.append(+cumulative_position)
            tiles_length.append(ptlm)

            cumulative_position += ptlm
            tile_index += 1

        return TilesRange(tiles_position, tiles_length)


@dataclass(frozen=True)
class NoopTLMIndex(TLMIndex):
    """
    Used when the target JP2 file already contains the TLM.
    """

    meta: TLMMetadata

    @override
    def _get_tlmmetadata(self) -> TLMMetadata:
        return self.meta

    @override
    @contextlib.contextmanager
    def open(self, uri: str, env_options: dict[str, Any] = {}) -> Generator[rasterio.DatasetReader]:
        env = self.recommended_env_vars().copy()
        env |= env_options
        with rasterio.Env(**env):
            with rasterio.open(uri) as src:
                yield src

    def recommended_env_vars(self) -> dict[str, Any]:
        return {
            "GDAL_DISABLE_READDIR_ON_OPEN": "EMPTY_DIR",
            "CPL_VSIL_CURL_ALLOWED_EXTENSIONS": "jp2",
        }
