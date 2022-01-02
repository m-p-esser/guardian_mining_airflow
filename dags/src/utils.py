""" Misc Utility Functions  """

import pathlib
import yaml


def load_parameters(file_name):
    """Load parameters from .yaml file"""
    root_dir = pathlib.Path.cwd()
    dags_dir = root_dir / "dags"
    matching_filepaths = list(dags_dir.rglob(pattern=file_name))
    if len(matching_filepaths) > 1:
        raise Exception(
            f"Too many parameter files. There are {len(matching_filepaths)} \
        files matching pattern {file_name}. The following files match the pattern: {matching_filepaths}"
        )
    else:
        parameters_file_path = matching_filepaths[0]
        with open(parameters_file_path) as yaml_file:
            parameters = yaml.safe_load(yaml_file)
            return parameters
