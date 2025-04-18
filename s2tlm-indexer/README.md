# Sentinel-2 TLM indexer

## Single file indexing

To generate a .tlm file, which can be injected on the fly during cropping:

```
cargo run --package indexer-singlejp2 ../T32TQM_20241115T100159_B03_10m.jp2 index.tlm
```

To add the TLM marker into a JP2:

```
cargo run --package indexer-singlejp2 ../T32TQM_20241115T100159_B03_10m.jp2 T32TQM_20241115T100159_B03_10m_with_TLM.jp2 --full-jp2
```

## File format

### Index file

Given a single JP2 file, the corresponding index file contains the following information:

- u64, big endian: length of the original JP2 file 
- u64, big endian: position of the first SOT marker in the original file (= length of the JP2+J2C main header)
- u32, big endian: length of the TLM segment (including the marker)
- array of u8: TLM segment (including the marker) (directly injectable in the JP2 file)

### Parquet file

The bulk indexer generates parquet files, one per MGRS tile and per collection.
The parquet contains 4 columns:

- `product_id`
- `band_id`
- `path`: path to the asset jp2 on CDSE S3, without the s3://<bucketname> prefix. This column is not necessary but can be convenient to have.
- `index`: bytes (see below)

A parquet file for all products of an MGRS tile from the start of the mission to 2025 weights around 5MB.
