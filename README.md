# NOMAD STAC Converter

This is a repository to host a STAC catalog version of data samples from ExoMars's NOMAD Instrument.

To obtain a smooth run of the catalog, use the following command:

```shell
$ uv run python -m src.cli create-stac-catalog --id "lno-10-days" -d "Data from NOMAD's LNO" -b "lno" -O ./catalog_output  --clean

```

To download the original data, use this command:

```shell
uv run python -m src.cli download-from-file path/to/lno/data/ten_days_LNO.zip
```
