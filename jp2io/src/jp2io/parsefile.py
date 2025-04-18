from __future__ import annotations

from dataclasses import dataclass

from rasterio.io import MemoryFile


@dataclass(frozen=True)
class JP2WithTLMSparseFile:
    name: str
    _children: list[MemoryFile]
    _content: bytes
    """ for debugging purposes """

    def close(self) -> None:
        for c in self._children:
            c.close()
