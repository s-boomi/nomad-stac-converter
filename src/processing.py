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

        # Create collection
        spatial_extent = pystac.SpatialExtent(
            bboxes=[df.geometry.total_bounds.tolist()]
        )
        temporal_extent = pystac.TemporalExtent(
            intervals=[
                [
                    df.utc_start_time.sort_values().head(1).item().to_pydatetime(),
                    df.utc_end_time.sort_values(ascending=False)
                    .head(1)
                    .item()
                    .to_pydatetime(),
                ]
            ]
        )
        collection_extent = pystac.Extent(
            spatial=spatial_extent, temporal=temporal_extent
        )
        collection = pystac.Collection(
            id="lno-10-days-2018",
            description="Nomad LNO Samples over 2018",
            extent=collection_extent,
            license="CC-BY-SA-4.0",
        )

        for row in tqdm(df.itertuples(), total=_nrows, desc="Creating Catalog"):
            item = self.gpd_line_to_item(row)
            item = self.add_asset(item, row)
            item = self.add_extensions(item, row)
            collection.add_item(item)

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

        log.info(f"Catalog created in {time.time() - start_time}s!")

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
        properties = {}
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
        # adding EO extension
        eo = EOExtension.ext(item, add_if_missing=True)
        eo.apply(bands=self.bands)

        # Adding SSYS extension
        ssys = SolSysExtension.ext(item, add_if_missing=True)

        # find local time on mars
        mars_local_time = (
            f"{data_row.martian_year}:{data_row.ls}:{data_row.local_solar_time}"
        )

        ssys.apply(
            targets=["mars"],
            local_time=mars_local_time,
            target_class=SolSysTargetClass.PLANET,
        )

        # .. And Projection extension
        default_wkt = """
 PROJCRS["Mars (2015) / Ographic / Lambert Conic Conformal",                                                                                    │
     BASEGEOGCRS["Mars (2015) / Ographic",                                                                                                      │
         DATUM["Mars (2015)",                                                                                                                   │
             ELLIPSOID["Mars (2015)",3396190,169.894447223612,                                                                                  │
                 LENGTHUNIT["metre",1]],                                                                                                        │
             ANCHOR["Viking 1 lander : 47.95137 W"]],                                                                                           │
         PRIMEM["Reference Meridian",0,                                                                                                         │
             ANGLEUNIT["degree",0.0174532925199433]],                                                                                           │
         ID["IAU",49901,2015]],                                                                                                                 │
     CONVERSION["Lambert Conic Conformal",                                                                                                      │
         METHOD["Lambert Conic Conformal (2SP)",                                                                                                │
             ID["EPSG",9802]],                                                                                                                  │
         PARAMETER["Latitude of false origin",40,                                                                                               │
             ANGLEUNIT["degree",0.0174532925199433],                                                                                            │
             ID["EPSG",8821]],                                                                                                                  │
         PARAMETER["Longitude of false origin",0,                                                                                               │
             ANGLEUNIT["degree",0.0174532925199433],                                                                                            │
             ID["EPSG",8822]],                                                                                                                  │
         PARAMETER["Latitude of 1st standard parallel",20,                                                                                      │
             ANGLEUNIT["degree",0.0174532925199433],                                                                                            │
             ID["EPSG",8823]],                                                                                                                  │
         PARAMETER["Latitude of 2nd standard parallel",60,                                                                                      │
             ANGLEUNIT["degree",0.0174532925199433],                                                                                            │
             ID["EPSG",8824]],                                                                                                                  │
         PARAMETER["Easting at false origin",0,                                                                                                 │
             LENGTHUNIT["metre",1],                                                                                                             │
             ID["EPSG",8826]],                                                                                                                  │
         PARAMETER["Northing at false origin",0,                                                                                                │
             LENGTHUNIT["metre",1],                                                                                                             │
             ID["EPSG",8827]]],                                                                                                                 │
     CS[Cartesian,2],                                                                                                                           │
         AXIS["westing (W)",west,                                                                                                               │
             ORDER[1],                                                                                                                          │
             LENGTHUNIT["metre",1]],                                                                                                            │
         AXIS["(N)",north,                                                                                                                      │
             ORDER[2],                                                                                                                          │
             LENGTHUNIT["metre",1]],                                                                                                            │
     ID["IAU",49976,2015]]

"""
        proj = ProjectionExtension.ext(item, add_if_missing=True)
        # proj.apply(wkt2=pyproj.CRS(default_wkt).to_wkt())
        proj.apply(wkt2=default_wkt)

        # Add common metadata here
        item.common_metadata.mission = "ExoMars"
        item.common_metadata.instruments = ["NOMAD"]
        return item
