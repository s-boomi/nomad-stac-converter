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


# Notes

Do not make fields required in the extension or else you might have None floating around like so:

```python
    @property
    def targets(self) -> list[str] | None:
        """Allows to have one or more targets listed within an array of strings.
        This can happen, for example, if several moons are in the same view.

        As an example, this scene has both of Ganymede and Jupiter in the same image
        as taken by the NASA mission Cassini `PIA02862<https://photojournal.jpl.nasa.gov/catalog/PIA02862>`_.

        Returns:
            list[str] or None
        """
        return get_required(
            self._get_property(TARGETS_PROPS, list[str]), self, TARGETS_PROPS
        )

```

Don't forget to enable `pop_if_none` in the setters also.
