from enum import Enum
from pathlib import Path
from typing import Annotated, List, Optional

import typer

from src import main

app = typer.Typer(pretty_exceptions_show_locals=False)


class FileFormat(str, Enum):
    SHP = "shp"
    GEOJSON = "geosjon"
    GPKG = "gpkg"
    OTHER = "other"


class TargetBands(str, Enum):
    SO = "so"
    LNO = "lno"
    UVIS = "uvis"


@app.callback()
def callback():
    """
    CLI utility for the STAC converter
    """


@app.command()
def create_stac_catalog(
    catalog_id: Annotated[str, typer.Option("--id", help="The ID of the catalog")],
    catalog_description: Annotated[
        str, typer.Option("--desc", "-d", help="A short description of the catalog")
    ],
    target_bands: Annotated[
        List[TargetBands], typer.Option("--bands", "-b", help="The bands from NOMAD")
    ],
    clean_previous_output: Annotated[bool, typer.Option("--clean/--no-clean")] = False,
    input_folder: Annotated[
        Optional[Path],
        typer.Option(
            "--input-folder",
            "-I",
            exists=True,
            file_okay=False,
            dir_okay=True,
            writable=False,
            readable=True,
            resolve_path=True,
            help="The folder where the raw data is located",
        ),
    ] = None,
    output_folder: Annotated[
        Optional[Path],
        typer.Option(
            "--outpit-folder",
            "-O",
            exists=True,
            file_okay=False,
            dir_okay=True,
            writable=False,
            readable=True,
            resolve_path=True,
            help="The folder to put your STAC catalog in data in",
        ),
    ] = None,
):
    """
    Creates a catalog from an input folder
    """
    main.create_stac_catalog(
        catalog_id=catalog_id,
        catalog_description=catalog_description,
        target_bands=target_bands,
        clean_previous_output=clean_previous_output,
        input_folder=input_folder,
        output_folder=output_folder,
    )


@app.command()
def download_from_file(
    file_name: Annotated[str, typer.Argument(help="The name of the file to download")],
    output_folder: Annotated[
        Optional[Path],
        typer.Option(
            exists=True,
            file_okay=False,
            dir_okay=True,
            writable=False,
            readable=True,
            resolve_path=True,
            help="The folder to put your downloaded data in",
        ),
    ] = None,
):
    """
    Downloads and unzip data from FILE_NAME to OUTPUT_FOLDER.
    """
    main.download_from_file(file_name, output_folder=output_folder)


@app.command()
def format_data_for_analysis(
    file_name: Annotated[str, typer.Argument(help="The name of the intermediate file")],
    fmt: Annotated[
        FileFormat,
        typer.Option("--format", "-f", help="The format of the intermediate file"),
    ],
):
    main.format_data_for_analysis(file_name, fmt)


@app.command()
def show_possible_formats():
    """Shows the available formats for saving"""
    main.show_possible_formats()


@app.command()
def download_wkt_files(
    file_name: Annotated[
        Path,
        typer.Argument(
            exists=False,
            file_okay=True,
            dir_okay=False,
            writable=False,
            readable=True,
            help="The target file. Must be a CSV.",
        ),
    ],
):
    """Downloads WKT files from the solar system in FILE_NAME"""
    main.download_wkt_files(file_name)


@app.command()
def show_wkt_projections(
    file_name: Annotated[
        Path,
        typer.Argument(
            exists=True,
            file_okay=True,
            dir_okay=False,
            writable=False,
            readable=True,
            help="The target file. Must be a CSV.",
        ),
    ],
    solar_body: Annotated[str, typer.Option("--solar-body", "-sb")] = None,
    proj_keywords: Annotated[list[str], typer.Option("--keywords", "-k")] = None,
):
    main.show_wkt_projections(
        file_name, solar_body=solar_body, proj_keywords=proj_keywords
    )


if __name__ == "__main__":
    app()
