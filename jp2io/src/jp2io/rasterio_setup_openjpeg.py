import importlib.resources
import os
import shutil
import subprocess
import tempfile


def setup_openjpeg() -> None:
    with importlib.resources.as_file(importlib.resources.files("rasterio")) as f:
        dir = f.as_posix() + ".libs"
        if not os.path.isdir(dir):
            dir = os.path.join(f.as_posix(), ".dylibs")
        libs = os.listdir(dir)
        jp2 = next(lib for lib in libs if lib.startswith("libopenjp2"))
        lib_dst = os.path.join(dir, jp2)

    if os.path.exists(lib_dst + ".copied"):
        print("openjpeg v2.5.3 already installed")
        return

    _root = tempfile.TemporaryDirectory()
    root = _root.name

    subprocess.run(
        f"git clone --branch v2.5.3 https://github.com/uclouvain/openjpeg {root}/openjpeg",
        check=True,
        shell=True,
    )
    subprocess.run(
        f"cd {root}/openjpeg && mkdir -p build && cd build && cmake .. -DCMAKE_BUILD_TYPE=Release",
        check=True,
        shell=True,
    )
    subprocess.run(f"cd {root}/openjpeg/build && make -j", check=True, shell=True)

    dir = f"{root}/openjpeg/build/bin/"
    libs = os.listdir(dir)
    jp2 = next(lib for lib in libs if lib.startswith("libopenjp2") and "2.5.3" in lib)
    lib_src = os.path.join(dir, jp2)

    os.unlink(lib_dst)
    shutil.copy(lib_src, lib_dst)
    with open(lib_dst + ".copied", "w") as f:
        pass

    print("openjpeg v2.5.3 successfully installed")


if __name__ == "__main__":
    setup_openjpeg()
