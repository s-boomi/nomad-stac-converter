import json
import time
from itertools import chain
from pathlib import Path
from typing import Literal, NamedTuple

import geopandas as gpd
import pandas as pd
import pyogrio
import pystac
from pystac.extensions.eo import Band, EOExtension
from pystac.extensions.projection import ProjectionExtension
from rich.console import Console
from rich.table import Table
from shapely import bounds, to_geojson
from tqdm.rich import tqdm

from src.io import IoHandler
from src.settings import DEFAULT_DATA_FOLDER, create_logger
from src.stac_extra.ssys_extension import SolSysExtension, SolSysTargetClass

log = create_logger(__name__)


class RawDataAnalysis:
    """
    Primarily serves as a concatenator for sparse JSON data files.

    Can serve as a good middleman for those who can handle GeoDataframes.
    """

    def __init__(self, folder: Path | None = None, dest_folder: Path | None = None):
        if dest_folder is None:
            dest_folder = DEFAULT_DATA_FOLDER / "analysis"

        self.io_handler = IoHandler(folder, dest_folder)

    @staticmethod
    def read_geojson(fn: Path) -> gpd.GeoDataFrame:
        """
        Reads a GeoJSON file and converts it in a Geopandas GeoDataFrame.
        """
        gdf = gpd.read_file(fn)
        gdf["utc_start_time"] = gdf["utc_start_time"].apply(pd.to_datetime)
        gdf["utc_end_time"] = gdf["utc_end_time"].apply(pd.to_datetime)
        return gdf

    def folder_as_geopandas(self) -> gpd.GeoDataFrame:
        """Converts the folder in Geopandas Dataframe"""
        try:
            file_iterator = chain(
                self.io_handler.all_input_files_from_ext("geojson"),
                self.io_handler.all_input_files_from_ext("json"),
            )
            log.info("Converting files")
            _dfs = [self.read_geojson(_fn) for _fn in file_iterator]
            return pd.concat(_dfs)
        except Exception as e:
            log.error(
                f"Couldn't parse {self.io_handler.input_folder} as unified GeoDataFrame"
            )
            log.error(f"Reason: {e.__class__.__name__}: {e}")
            raise e

    @staticmethod
    def show_writable_formats():
        table = Table(title="Available formats")

        table.add_column("Format", style="bold")
        table.add_column("Permission")

        for driver_name, driver_permissions in pyogrio.list_drivers().items():
            if driver_permissions == "r":
                permission = "[yellow]READ ONLY"
            elif driver_permissions == "rw":
                permission = "[bold green]READ/WRITE"

            table.add_row(driver_name, permission)

        console = Console()
        console.print(table)

    def save_to_format(
        self,
        filename: str,
        fmt_name: Literal["shp", "geosjon", "gpkg", "other"] = "other",
    ) -> gpd.GeoDataFrame:
        output_file = self.io_handler.output_folder / filename
        gdf = self.folder_as_geopandas()

        if fmt_name in ["shp", "other"]:
            gdf.to_file(output_file)
        elif fmt_name == "geojson":
            gdf.to_file(output_file, driver="GeoJSON")
        elif fmt_name == "geopackage":
            gdf.to_file(output_file, layer="data", driver="GPKG")

        return gdf


class CatalogCreator:
    """
    Util module used to create the catalog.
    """

    def __init__(
        self,
        catalog_id: str,
        catalog_descr: str,
        bands: list[Band],
        folder: Path | None = None,
        dest_folder: Path | None = None,
        interm_folder: Path | None = None,
    ):
        self.catalog_id = catalog_id
        self.catalog_descr = catalog_descr
        self.io_handler = IoHandler(folder, dest_folder)
        self.analyzer = RawDataAnalysis(folder, interm_folder)
        self.bands = bands

    @staticmethod
    def create_collection_from_slice(
        df_slice: gpd.GeoDataFrame, collection_id: str
    ) -> pystac.Collection:
        # Create collection
        spatial_extent = pystac.SpatialExtent(
            bboxes=[df_slice.geometry.total_bounds.tolist()]
        )
        temporal_extent = pystac.TemporalExtent(
            intervals=[
                [
                    df_slice.utc_start_time.sort_values()
                    .head(1)
                    .item()
                    .to_pydatetime(),
                    df_slice.utc_end_time.sort_values(ascending=False)
                    .head(1)
                    .item()
                    .to_pydatetime(),
                ]
            ]
        )
        collection_extent = pystac.Extent(
            spatial=spatial_extent, temporal=temporal_extent
        )
        return pystac.Collection(
            id=collection_id,
            description="Nomad LNO Samples over 2018",
            extent=collection_extent,
            license="CC-BY-SA-4.0",
        )

    def create_catalog(
        self, self_contained: bool = True, clean_previous_output: bool = False
    ) -> pystac.Catalog:
        """
        Creates a STAC catalog using the data provided in the `folder`, into the `dest_folder`.

        # TODO: probably avoid using Geopandas dataframe in case it gets too big.

        """
        start_time = time.time()
        if self.io_handler.is_input_folder_empty():
            raise FileNotFoundError("Your folder is empty! Download the data first.")

        if not self.io_handler.is_output_folder_empty() and not clean_previous_output:
            raise ValueError(
                "The output folder is not empty. Please clean it first or set clean_previous_output` to True"
            )
        elif not self.io_handler.is_output_folder_empty() and clean_previous_output:
            self.io_handler.clean_output_folder()

        catalog = pystac.Catalog(id=self.catalog_id, description=self.catalog_descr)

        df = self.analyzer.save_to_format("lno_10_days.csv", fmt_name="csv")
        _nrows, _ = df.shape

        # Create main collection
        collection = self.create_collection_from_slice(df, collection_id="10-days-lno")

        eo = EOExtension.summaries(collection, add_if_missing=True)
        eo.bands = self.bands

        # Adding SSYS extension
        ssys = SolSysExtension.summaries(collection, add_if_missing=True)
        ssys.targets = ["mars"]
        ssys.target_class = SolSysTargetClass.PLANET

        # .. And Projection extension
        proj = ProjectionExtension.summaries(collection, add_if_missing=True)
        # proj.apply(wkt2=pyproj.CRS(default_wkt).to_wkt())
        proj.epsg = "IAU:2015:49986"

        for diff_order in df["diffraction_order"].unique():
            sliced_df = df[df["diffraction_order"] == diff_order]
            sub_collection = self.create_collection_from_slice(
                sliced_df, collection_id=f"diffraction-order-{diff_order}"
            )

            subrows, _ = sliced_df.shape

            # Save it for sub-collections
            for row in tqdm(
                sliced_df.itertuples(),
                total=subrows,
                desc=f"Collection (DF={diff_order})",
            ):
                item = self.gpd_line_to_item(row)
                item = self.add_asset(item, row)
                item = self.add_extensions(item, row)
                sub_collection.add_item(item)

            collection.add_child(sub_collection)
            log.info(
                f"Sub-collection {sub_collection.id} added to master collection {collection.id}!"
            )

        # Add collection to the catalog
        catalog.add_child(collection)
        log.info(f"{collection.id} added to the catalog")

        # Save catalog (ie. in the STAC folder)
        log.info(f"Normalizing hrefs to {self.io_handler.output_folder}")
        catalog.normalize_hrefs(self.io_handler.output_folder.as_posix())

        log.info(
            f"""Saving catalog as {"self-contained" if self_contained else "absolute published"}"""
        )
        if self_contained:
            catalog.save(catalog_type=pystac.CatalogType.SELF_CONTAINED)
        else:
            catalog.save(catalog_type=pystac.CatalogType.ABSOLUTE_PUBLISHED)

        exec_time = time.time() - start_time
        log.info(
            f"Catalog created in {exec_time // 60} minutes and {exec_time % 60} seconds!"
        )

        return catalog

    @staticmethod
    def gpd_line_to_item(df_line: NamedTuple) -> pystac.Item:
        """
        Converts a line from a geopandas dataframe into a pystac item, while taking account
        of the arrangement of said line.
        """
        item_id = df_line.psa_lid
        _line_geom = df_line.geometry
        footprint = json.loads(to_geojson(_line_geom))
        bbox = bounds(_line_geom).tolist()
        start_datetime_utc = df_line.utc_start_time.to_pydatetime()
        end_datetime_utc = df_line.utc_end_time.to_pydatetime()

        # Use this object as a throwaway (not recommended)
        # If you can find related extensions, use them
        properties = {}

        # Place anything that doesn't fit in an extension
        properties["spec_ix"] = df_line.spec_ix
        properties["incidence_angle"] = df_line.incidence_angle
        properties["emergence_angle"] = df_line.emergence_angle
        properties["phase_angle"] = df_line.phase_angle
        properties["centre_latitude"] = df_line.centre_latitude
        properties["centre_longitude"] = df_line.centre_longitude
        properties["channel_temperature"] = df_line.channel_temperature

        item = pystac.Item(
            id=item_id,
            geometry=footprint,
            bbox=bbox,
            datetime=start_datetime_utc,
            start_datetime=start_datetime_utc,
            end_datetime=end_datetime_utc,
            properties=properties,
        )

        # Add common metadata here
        item.common_metadata.platform = "exomars-trace-gas-orbiter"
        item.common_metadata.instruments = ["nomad"]
        item.common_metadata.constellation = "exomars"

        return item

    @staticmethod
    def add_asset(item: pystac.Item, data_row: NamedTuple) -> pystac.Item:
        """
        Takes an item and adds assets like imgs or data onto it
        """
        item.add_asset(
            key="dataformat",
            asset=pystac.Asset(
                href=data_row.hdf5_filename, media_type=pystac.MediaType.HDF5
            ),
        )
        return item

    def add_extensions(self, item: pystac.Item, data_row: NamedTuple) -> pystac.Item:
        """
        Calls the extensions and add them on the current object
        """
        # Adding SSYS extension
        ssys = SolSysExtension.ext(item, add_if_missing=True)

        # find local time on mars
        mars_local_time = (
            f"{data_row.martian_year}:{data_row.ls}:{data_row.local_solar_time}"
        )

        ssys.apply(
            local_time=mars_local_time,
        )

        # Add common metadata here
        item.common_metadata.mission = "ExoMars"
        item.common_metadata.instruments = ["NOMAD"]
        return item
