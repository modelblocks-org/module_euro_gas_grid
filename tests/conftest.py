"""Shared test fixtures."""

import shutil
import zipfile
from pathlib import Path
from urllib.request import urlretrieve

import pytest

TEST_FILES = "https://zenodo.org/records/18153129/files/test_suite.zip?download=1"


@pytest.fixture(scope="module")
def module_path():
    """Parent directory of the project."""
    return Path(__file__).parent.parent


@pytest.fixture(scope="session")
def user_path() -> Path:
    """Download and unzip test files."""
    dir = Path("resources/")
    # If test suite has been downloaded, assume everything is OK.
    # Otherwise, cleanup and re-download.
    if not Path(dir / "test_suite.zip").exists():
        shutil.rmtree(dir, ignore_errors=True)
        Path(dir).mkdir(parents=True, exist_ok=True)
        test_zip = Path(dir / "test_suite.zip")
        urlretrieve(TEST_FILES, test_zip)
        with zipfile.ZipFile(test_zip, "r") as zfile:
            zfile.extractall(dir)
    return dir / "user"


@pytest.fixture(scope="module")
def integration_path(user_path: Path, module_path: Path):
    """Ensures the minimal integration test is ready."""
    integration_dir = Path(module_path / "tests/integration")
    if integration_dir.exists():
        shutil.rmtree(
            integration_dir / "results/", ignore_errors=True
        )  # clean everything
    user_integ_dir = integration_dir / "resources/inputs/"
    files_to_copy = ["BALK/shapes.parquet"]
    for file in files_to_copy:
        destination_file = Path(user_integ_dir / file)
        destination_file.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy(user_path / file, destination_file)
    return integration_dir
