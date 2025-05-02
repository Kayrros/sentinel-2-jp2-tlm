from __future__ import annotations

import asyncio
import math
import struct
from dataclasses import dataclass

import imagecodecs
import numcodecs.abc
import numcodecs.registry
import numpy as np
from typing_extensions import override
from zarr.abc.codec import BytesBytesCodec
from zarr.core.array_spec import ArraySpec
from zarr.core.buffer import Buffer, NDBuffer


# TODO: expose all parameters in the codec config
def build_sentinel2_jp2_codestream(tile_data: bytes, tile_size: int, raster_size_x: int, raster_size_y: int) -> bytes:
    """
    See https://web.archive.org/web/20250209200219/https://ics.uci.edu/~dhirschb/class/267/papers/jpeg2000.pdf
    Current values taken GDAL's dump_jp2 tool on a Sentinel-2 JP2 file.
    """
    # from the SOT marker, read the Isot to know which tile we are considering
    Isot = struct.unpack_from(">H", tile_data, 4)[0]
    tile_x = Isot % 11
    tile_y = Isot // 11

    n_tile_x = math.ceil(raster_size_x / tile_size)
    n_tile_y = math.ceil(raster_size_y / tile_size)
    xsize = tile_size if tile_x < n_tile_x - 1 else (raster_size_x - (n_tile_x - 1) * tile_size)
    ysize = tile_size if tile_y < n_tile_y - 1 else (raster_size_y - (n_tile_y - 1) * tile_size)

    soc_marker = struct.pack(">H", 0xFF4F)  # SOC marker (2 bytes)
    siz_marker = struct.pack(
        ">HHHIIIIIIIIHBBB",
        0xFF51,  # SIZ marker (2 bytes)
        41,  # Lsiz (uint16)
        0,  # Rsiz (uint16)
        xsize,  # Xsiz (uint32)
        ysize,  # Ysiz (uint32)
        0,  # XOsiz (uint32)
        0,  # YOsiz (uint32)
        tile_size,  # XTsiz (uint32)
        tile_size,  # YTsiz (uint32)
        0,  # XTOSiz (uint32)
        0,  # YTOSiz (uint32)
        1,  # Csiz (uint16)
        14,  # Ssiz0 (uint8)
        1,  # XRsiz0 (uint8)
        1,  # YRsiz0 (uint8)
    )
    cod_marker = struct.pack(
        ">HHBBHBBBBBBBBBBB",
        0xFF52,  # COD marker (2 bytes)
        17,  # Lcod (2 bytes)
        1,  # Scod (uint8)
        0,  # SGcod_Progress (uint8)
        1,  # SGcod_NumLayers (uint16)
        0,  # SGcod_MCT (uint8)
        4,  # SPcod_NumDecompositions (uint8)
        4,  # SPcod_xcb_minus_2 (uint8)
        4,  # SPcod_ycb_minus_2 (uint8)
        0,  # SPcod_cbstyle (uint8)
        1,  # SPcod_transformation (uint8)
        136,  # SPcod_Precincts0 (uint8)
        136,  # SPcod_Precincts1 (uint8)
        136,  # SPcod_Precincts2 (uint8)
        136,  # SPcod_Precincts3 (uint8)
        136,  # SPcod_Precincts4 (uint8)
    )
    qcd_marker = struct.pack(
        ">HHBBBBBBBBBBBBBB",
        0xFF5C,  # QCD marker (2 bytes)
        16,  # Lqcd (uint16)
        32,  # Sqcd (uint8)
        128,  # SPqcd0 (uint8)
        136,  # SPqcd1 (uint8)
        136,  # SPqcd2 (uint8)
        144,  # SPqcd3 (uint8)
        136,  # SPqcd4 (uint8)
        136,  # SPqcd5 (uint8)
        144,  # SPqcd6 (uint8)
        136,  # SPqcd7 (uint8)
        136,  # SPqcd8 (uint8)
        136,  # SPqcd9 (uint8)
        128,  # SPqcd10 (uint8)
        128,  # SPqcd11 (uint8)
        136,  # SPqcd12 (uint8)
    )
    eoc_marker = struct.pack(">H", 0xFFD9)  # EOC marker (2 bytes)

    # overwrite the Isot (tile index) to be 0
    mut_tile_data = bytearray(tile_data)
    mut_tile_data[4:6] = struct.pack(">H", 0)
    tile_data = bytes(mut_tile_data)

    codestream = soc_marker + siz_marker + cod_marker + qcd_marker + tile_data + eoc_marker
    return codestream


def _decode_jp2_tile(chunk_data: Buffer, chunk_spec: ArraySpec) -> Buffer:
    codestream = build_sentinel2_jp2_codestream(
        chunk_data.to_bytes(), tile_size=1024, raster_size_x=10980, raster_size_y=10980
    )
    array = imagecodecs.jpeg2k_decode(codestream)

    # needed because a BytesCodec will take care of decoding to uint16
    array = array.view(np.uint8)
    array = array.ravel()

    return chunk_spec.prototype.buffer.from_array_like(array)


def _decode_jp2_tile_npy(chunk_data: np.ndarray) -> np.ndarray:
    codestream = build_sentinel2_jp2_codestream(
        chunk_data.tobytes(), tile_size=1024, raster_size_x=10980, raster_size_y=10980
    )
    array = imagecodecs.jpeg2k_decode(codestream)
    return array


@dataclass(frozen=True)
class Sentinel2Jpeg2000NumCodec(numcodecs.abc.Codec):
    """
    Warning: this codec only supports 10m bands of Sentinel-2 right now.
    """

    codec_id: str | None = "jp2io.zarr.Sentinel2Jpeg2000Codec"
    # TODO: 'raster_id' attribute, r10m_b02 / etc

    # id: str = ""  # to fix some zarr v2 / v3 issues

    @override  # for numcodecs
    def decode(self, buf, out=None):
        tile = _decode_jp2_tile_npy(buf)
        if tile.shape[0] < 1024:
            tile = np.pad(tile, ((0, 1024 - tile.shape[0]), (0, 0)))
        if tile.shape[1] < 1024:
            tile = np.pad(tile, ((0, 0), (0, 1024 - tile.shape[1])))
        if out is not None:
            out[:] = tile[:]
        return tile

    @override  # for numcodecs, unsupported
    def encode(self, buf):
        raise NotImplementedError

    @classmethod
    def from_config(cls, config) -> Sentinel2Jpeg2000NumCodec:
        return Sentinel2Jpeg2000NumCodec()

    def get_config(self) -> dict:
        return {"id": self.codec_id, "name": self.codec_id}


@dataclass(frozen=True)
class Sentinel2Jpeg2000Codec(BytesBytesCodec):
    """
    Warning: this codec only supports 10m bands of Sentinel-2 right now.
    """

    name: str = "jp2io.zarr.Sentinel2Jpeg2000Codec"
    id: str = "jp2io.zarr.Sentinel2Jpeg2000Codec"

    # TODO: 'raster_id' attribute, r10m_b02 / etc

    # for zarr v3
    @override
    async def _decode_single(self, chunk_data: Buffer, chunk_spec: ArraySpec) -> NDBuffer:
        return await asyncio.to_thread(_decode_jp2_tile, chunk_data, chunk_spec)


numcodecs.registry.register_codec(Sentinel2Jpeg2000NumCodec, codec_id="jp2io.zarr.Sentinel2Jpeg2000Codec")
assert numcodecs.get_codec({"id": "jp2io.zarr.Sentinel2Jpeg2000Codec"})
