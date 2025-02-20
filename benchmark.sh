#!/bin/bash

set -eu

baseurl=/vsicurl/https://media.githubusercontent.com/media/Kayrros/sentinel-2-jp2-tlm/refs/heads/main

# download openjpeg v2.5.3, which supports TLM markers
case "$(uname -s -m)" in
    "Linux x86_64")
        if [ ! -d openjpeg-v2.5.3-linux-x86_64/lib ]; then
            curl -o openjpeg-v2.5.3-linux-x86_64.tar.gz --location https://github.com/uclouvain/openjpeg/releases/download/v2.5.3/openjpeg-v2.5.3-linux-x86_64.tar.gz
            tar xf openjpeg-v2.5.3-linux-x86_64.tar.gz
        fi
        export LD_LIBRARY_PATH=openjpeg-v2.5.3-linux-x86_64/lib
        ;;
    "Darwin x86_64")
        if [ ! -d openjpeg-v2.5.3-osx-x86_64/lib ]; then
            curl -o openjpeg-v2.5.3-osx-x86_64.zip --location https://github.com/uclouvain/openjpeg/releases/download/v2.5.3/openjpeg-v2.5.3-osx-x86_64.zip
            unzip openjpeg-v2.5.3-osx-x86_64.zip 
        fi
        export DYLD_LIBRARY_PATH=openjpeg-v2.5.3-osx-x86_64/lib
        ;;
    "Darwin arm64")
        if [ ! -d openjpeg-v2.5.3-osx-arm64/lib ]; then
            curl -o openjpeg-v2.5.3-osx-arm64.zip --location https://github.com/uclouvain/openjpeg/releases/download/v2.5.3/openjpeg-v2.5.3-osx-arm64.zip
            unzip openjpeg-v2.5.3-osx-arm64.zip
        fi
        export DYLD_LIBRARY_PATH=openjpeg-v2.5.3-osx-arm64/lib
        ;;
    *)
        echo Unsupported OS/architecture "$(uname -s -m)"
        exit 1
        ;;
esac


# extract a crop of size 500x500 pixels, from a Sentinel-2 raster hosted on Github
export GDAL_DISABLE_READDIR_ON_OPEN=EMPTY_DIR
export CPL_VSIL_CURL_ALLOWED_EXTENSIONS=jp2,tiff
export GDAL_NUM_THREADS=1
export CPL_CURL_VERBOSE=YES  # needed to count the number of requests

run() {
    echo -e "\nJP2 without TLM"
    echo -e "===============\n"
    # - time taken: 4325ms
    # - bandwidth used: ~3MB
    # - number of requests: 110
    echo Number of requests:
    time bash -c "gdal_translate -srcwin 9500 9500 500 500 $baseurl/T32TQM_20241115T100159_B03_10m.jp2 crop1.tif 2>&1 | grep -i '^Range: bytes=' | wc -l"

    echo -e "\nJP2 with TLM"
    echo -e "============\n"
    # - time taken: 391ms
    # - bandwidth used: ~1MB
    # - number of requests: 3
    echo Number of requests:
    time bash -c "gdal_translate -srcwin 9500 9500 500 500 $baseurl/T32TQM_20241115T100159_B03_10m_with_TLM.jp2 crop2.tif 2>&1 | grep -i '^Range: bytes=' | wc -l"

    echo -e "\nCOG"
    echo -e "===\n"
    # - time taken: 389ms
    # - bandwidth used: ~1.5MB
    # - number of requests: 2
    echo Number of requests:
    time bash -c "gdal_translate -srcwin 9500 9500 500 500 $baseurl/T32TQM_20241115T100159_B03_10m.tiff crop3.tif 2>&1 | grep -i '^Range: bytes=' | wc -l"

    # crops from JP2 with and without TLM are strictly the same
    diff crop1.tif crop2.tif
}

# The first run warms-up Github CDN cache. This is typically not necessary when using an object store.
echo -n "warm-up... "
run >/dev/null 2>/dev/null
echo "OK"

# run the real benchmark
run
