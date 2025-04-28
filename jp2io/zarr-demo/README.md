```bash
# (takes a few seconds but could be made instant)
uv run jp2io-make-virtual-cube L2A-32UPC.parquet ./L2A-32UPC.json

# (load env vars CDSE_ACCESS_KEY_ID and CDSE_SECRET_ACCESS_KEY)
uv run jupyter lab kerchunk.ipynb
```

(inspired by https://github.com/EOPF-Sample-Service/eopf-sample-webinar2/)
