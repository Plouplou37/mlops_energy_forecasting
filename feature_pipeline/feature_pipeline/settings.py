import os
from pathlib import Path
from typing import Union

from dotenv import load_dotenv


def load_env_vars(root_dir: Union[str, Path]) -> dict:
    """
    Load environment variables from .env.default and .env files.

    Args:
        root_dir: Root directory of the .env files. Must be either of str or Path type

    Returns:
        Dictionary with the environment variables.
    """
    if isinstance(root_dir, str):
        # if root_dir is an str type then convert it into Path type
        root_dir = Path(root_dir)

    load_dotenv(dotenv_path=root_dir / ".env.default")
    # Load the the ".env" into envrionement variables
    load_dotenv(dotenv_path=root_dir / ".env", override=True)

    # use os library to get the load environment variables into a dictionnary
    return dict(os.environ)


def get_root_dir(default_value: str = ".") -> Path:
    """
    Get the root directory of the project either from the environment variable ML_PIPELINE_ROOT_DIR or "." (current directory)

    Args:
        default_value: Default value to use if the environment variable is not set. The default value is the current folder "."

    Returns:
        Path to the root directory of the project.
    """

    # Get the value of the root directory, if not set as default_value "."
    return Path(os.getenv("ML_PIPELINE_ROOT_DIR", default_value))


ML_PIPELINE_ROOT_DIR = get_root_dir()
OUTPUT_DIR = ML_PIPELINE_ROOT_DIR / "output"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

SETTINGS = load_env_vars(root_dir=ML_PIPELINE_ROOT_DIR)
