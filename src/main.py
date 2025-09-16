import re
from pathlib import Path
from typing import Literal

import pandas as pd
import pyproj
from rich.console import Console
from rich.panel import Panel

from src.downloader import WktDownloader
from src.instrument import Nomad
from src.io import IoHandler
from src.processing import CatalogCreator, RawDataAnalysis


def create_stac_catalog(
    catalog_id: str,
    catalog_description: str,
    target_bands: list[Literal["so", "lno", "uvis"]],
    clean_previous_output: bool = False,
    input_folder: Path | None = None,
    output_folder: Path | None = None,
):
    bands = [Nomad().bands[target_band] for target_band in target_bands]

    catalog_creator = CatalogCreator(
        catalog_id=catalog_id,
        catalog_descr=catalog_description,
        bands=bands,
        folder=input_folder,
        dest_folder=output_folder,
    )
    catalog_creator.create_catalog(clean_previous_output=clean_previous_output)


def download_from_file(file_name: str, output_folder: Path | None = None):
    io_handler = IoHandler(input_folder=output_folder)
    io_handler.download_data(file_name)


def format_data_for_analysis(
    file_name: str, fmt: Literal["shp", "geosjon", "gpkg", "other"]
):
    """
    ie. "lno_10_days.shp", "shp"
    """
    rda = RawDataAnalysis()
    rda.save_to_format(file_name, fmt)


def show_possible_formats():
    rda = RawDataAnalysis()
    rda.show_writable_formats()


def download_wkt_files(output_file: Path):
    wkt_dl = WktDownloader()
    wkt_dl.local_download(output_file)


def show_wkt_projections(
    summary_file: Path,
    solar_body: str | None = None,
    proj_keywords: list[str] | None = None,
):
    df = pd.read_csv(summary_file)

    if solar_body:
        df = df[df["solar_body"].str.contains(solar_body, flags=re.IGNORECASE)]

    if proj_keywords:
        df = df[
            df["projection_name"].str.contains(
                "".join([f"(?=.*{kw})" for kw in proj_keywords]),
                regex=True,
                flags=re.IGNORECASE,
            )
        ]

    console = Console()
    for row in df.itertuples():
        crs = pyproj.CRS(row.wkt)

        panel = Panel(
            crs.to_wkt(pretty=True),
            title=f"""[bold]{row.id}""",
            subtitle=f"[bold]{row.solar_body} - {row.projection_name}",
        )
        console.print(panel)
