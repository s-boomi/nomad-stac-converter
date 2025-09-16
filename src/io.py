from pathlib import Path
from typing import Iterator

from src.downloader import Downloader
from src.settings import DEFAULT_INPUT_FOLDER, DEFAULT_OUTPUT_FOLDER, create_logger

log = create_logger(__name__)


class IoHandler:
    def __init__(
        self, input_folder: Path | None = None, output_folder: Path | None = None
    ):
        if input_folder is None:
            input_folder = DEFAULT_INPUT_FOLDER
        else:
            log.warning(f"Make sure you don't commit {input_folder} to version control")
        if output_folder is None:
            output_folder = DEFAULT_OUTPUT_FOLDER
        else:
            log.warning(
                f"Make sure you don't commit {output_folder} to version control"
            )

        self.input_folder = input_folder
        self.output_folder = output_folder

    def count_input_elements(self) -> int:
        count = 0
        for _, dirs, files in self.input_folder.walk(on_error=print):
            count += len(dirs)
            count += len(files)
        return count

    def count_output_elements(self) -> int:
        count = 0
        for _, dirs, files in self.output_folder.walk(on_error=print):
            count += len(dirs)
            count += len(files)
        return count

    def is_input_folder_empty(self) -> bool:
        return self.count_input_elements() == 0

    def is_output_folder_empty(self) -> bool:
        return self.count_output_elements() == 0

    def show_input_folder(self):
        for root, dirs, files in self.input_folder.walk(on_error=print):
            log.info(f"""Root: {root}, dirs: {dirs}, files: {files}""")

    def show_output_folder(self):
        for root, dirs, files in self.output_folder.walk(on_error=print):
            log.info(f"""Root: {root}, dirs: {dirs}, files: {files}""")

    def download_data(self, file_path: str):
        if not self.is_input_folder_empty():
            raise FileExistsError("The input folder is not empty!")
        downloader = Downloader(file_path)
        downloader.local_download(output_folder=self.input_folder)

    def all_input_files_from_ext(self, extension: str) -> Iterator[Path]:
        return self.input_folder.rglob(f"*.{extension}")

    def all_output_files_from_ext(self, extension: str) -> Iterator[Path]:
        return self.output_folder.rglob(f"*.{extension}")

    def clean_output_folder(self):
        log.warning(f"This action will remove the contents of {self.output_folder}")
        for root, dirs, files in self.output_folder.walk(top_down=False):
            for name in files:
                (root / name).unlink()
            for name in dirs:
                (root / name).rmdir()
