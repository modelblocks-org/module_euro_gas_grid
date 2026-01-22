"""Generic file unzipper."""

import sys
import zipfile
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    snakemake: Any
sys.stderr = open(snakemake.log[0], "w")


def unzip_to_path(input_path: str, output_path: str, file: str | None = None) -> None:
    """Unzip files from a zip archive."""
    with zipfile.ZipFile(input_path, "r") as zfile:
        if file is None:
            zfile.extractall(output_path)
        else:
            try:
                data = zfile.read(file)
            except KeyError as e:
                raise FileNotFoundError(
                    f"File {file!r} not found in zip archive"
                ) from e
            # Write the contents to the exact path given in output_path
            with open(output_path, "wb") as i:
                i.write(data)


if __name__ == "__main__":
    unzip_to_path(
        input_path=snakemake.input.zip_file,
        output_path=snakemake.output[0],
        file=snakemake.params.get("file", None),
    )
