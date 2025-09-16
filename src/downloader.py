import tempfile
import time
import zipfile
from pathlib import Path
from typing import Literal
from urllib.parse import urlparse

import httpx
import pandas as pd
from bs4 import BeautifulSoup
from tqdm.rich import tqdm

from src.settings import create_logger

log = create_logger(__name__)


class Downloader:
    def _analyze_file_path(self):
        parsed_url = urlparse(self.file_name)
        if not parsed_url.netloc:
            self.url_type = "local"
            true_path = Path(self.file_name)
            if not true_path.exists():
                raise FileNotFoundError(f"Couldn't find {true_path}")
        else:
            self.url_type = "url"
            true_path = Path(parsed_url.path)
        self.extension = true_path.suffix

    def __init__(self, file_name: str):
        self.file_name = file_name
        self.url_type: Literal["local", "url", "unknown"] = "unknown"
        self.extension: str = ""
        self._analyze_file_path()
        self.local_path: Path | None = (
            self.file_name if self.url_type == "local" else None
        )

    def _unzip_archive(self, output_folder: Path):
        with zipfile.ZipFile(self.local_path, mode="r") as archive:
            for _file in archive.namelist():
                archive.extract(_file, output_folder)

    def _download_remote_file(self, output_directory: Path, filename: str):
        """Bulk downloads the file if on remote"""
        self.local_path = output_directory / filename
        with httpx.stream("GET", self.file_name) as response:
            response.raise_for_status()
            with open(self.local_path, "wb") as f:
                for chunk in response.iter_bytes():
                    f.write(chunk)

    def local_download(self, output_folder: Path):
        if self.url_type != "local":
            tmp_dir = tempfile.TemporaryDirectory()
            self._download_remote_file(Path(tmp_dir))

        if self.extension.endswith("zip"):
            self._unzip_archive(output_folder)

        if self.url_type != "local":
            tmp_dir.close()


class WktDownloader:
    BASE_URL = "https://voparis-vespa-crs.obspm.fr/web/{planet}.html"
    PLANETS = [
        "mercury",
        "venus",
        "earth",
        "mars",
        "jupiter",
        "saturn",
        "uranus",
        "neptune",
    ]

    @staticmethod
    def download_html_contents(url: str) -> BeautifulSoup:
        response = httpx.get(url)
        response.raise_for_status()
        return BeautifulSoup(response.content, "html.parser")

    def local_download(self, output_file: Path):
        big_df = []

        for planet in tqdm(self.PLANETS):
            soup = self.download_html_contents(self.BASE_URL.format(planet=planet))
            planet_wkts = soup.find("table")
            if planet_wkts is not None:
                df = pd.read_html(planet_wkts.prettify())[0]
                df["created_at"] = df["created_at"].apply(pd.to_datetime)
                big_df.append(df)
            else:
                log.error(
                    f"Couldn't download WKT2 info for {planet} at {self.BASE_URL.format(planet=planet)}"
                )
            time.sleep(2)

        big_df = pd.concat(big_df)
        big_df.to_csv(output_file, index=None)
