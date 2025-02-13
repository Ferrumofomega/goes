# pylint: disable=line-too-long
"""S3 interface to interact with NASA/NOAA GOES-R satellite data.

GOES-17: https://s3.console.aws.amazon.com/s3/buckets/noaa-goes17/?region=us-east-1
GOES-16: https://s3.console.aws.amazon.com/s3/buckets/noaa-goes16/?region=us-east-1

This module uses the s3fs library to interact with Amazon S3. s3fs requires the user to
supply their access key id and secret access key. To provide boto3 with the necessary
credentials the user must either have a `~/.aws/credentials` file, or the environment
variables `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` set. See boto3's documentation at
https://boto3.amazonaws.com/v1/documentation/api/latest/guide/configuration.html#shared-credentials-file for more information.
"""
from collections import namedtuple
import logging
import os

import s3fs

from wildfire import multiprocessing
from . import utilities

LOCAL_FILEPATH_FORMAT = "{local_directory}/{s3_key}"

DownloadFileArgs = namedtuple(
    "DownloadFileArgs", ("s3_filepath", "local_directory", "s3_filesystem")
)
_logger = logging.getLogger(__name__)


def list_s3_files(satellite, region, start_time, end_time=None, channel=None):
    """List the NOAA GOES-R level 1 series files in Amazon S3 matching parameters.

    Parameters
    ----------
    satellite : str
        Must be in set (noaa-goes16, noaa-goes17).
    region : str
        Must be in set (M1, M2, C, F).
    channel : int, optional
        Must be between 1 and 16 inclusive. By default `None` which will list all data for
        all channels.
    start_time : datetime.datetime
    end_time : datetime.datetime, optional
        By default `None`, which will list all files whose scan start time matches
        `start_time`.

    Returns
    -------
    list of str
    """
    s3 = s3fs.S3FileSystem(anon=True, use_ssl=False)
    glob_patterns = utilities.decide_fastest_glob_patterns(
        directory=satellite,
        satellite=satellite,
        region=region,
        start_time=start_time,
        end_time=end_time,
        channel=channel,
        s3=True,
    )
    _logger.info("Listing files in S3 using glob patterns: %s", glob_patterns)
    filepaths = multiprocessing.map_function(  # only parallel across local hardware
        function=s3.glob, function_args=glob_patterns
    )
    filepaths = multiprocessing.flatten_array(filepaths)
    if end_time is None:
        return filepaths
    return utilities.filter_filepaths(
        filepaths=filepaths, start_time=start_time, end_time=end_time,
    )


def s3_filepath_to_local(s3_filepath, local_directory):
    """Translate s3fs filepath to local filesystem filepath."""
    _, key = s3fs.core.split_path(s3_filepath)
    return LOCAL_FILEPATH_FORMAT.format(local_directory=local_directory, s3_key=key)


def download_file(s3_filepath, local_directory, s3_filesystem=None):
    """Download file to disk.

    Local filepath will be of the form: {local_direcory}/{s3_key}

    Returns
    -------
    str
        Local filepath to the downloaded file.
    """
    s3_filesystem = (
        s3_filesystem if s3_filesystem else s3fs.S3FileSystem(anon=True, use_ssl=False)
    )
    local_path = s3_filepath_to_local(
        s3_filepath=s3_filepath, local_directory=local_directory
    )
    os.makedirs(name=os.path.dirname(local_path), exist_ok=True)
    s3_filesystem.get(rpath=s3_filepath, lpath=local_path)
    return local_path


def download_files(local_directory, satellite, region, start_time, end_time=None):
    """Download files matching parameters to disk in parallel.

    Parameters
    ----------
    local_directory : str
    satellite : str
        Must be in set (noaa-goes16, noaa-goes17).
    region : str
        Must be in set (M1, M2, C, F).
    start_time : datetime.datetime
    end_time : datetime.datetime, optional
        By default `None`, which will list all files whose scan start time matches
        `start_time`.

    Returns
    -------
    list of str
        Local filepaths to downloaded files.
    """
    s3_filepaths = list_s3_files(
        satellite=satellite, region=region, start_time=start_time, end_time=end_time
    )
    already_local_filepaths = utilities.list_local_files(
        local_directory=local_directory,
        satellite=satellite,
        region=region,
        start_time=start_time,
        end_time=end_time,
    )

    filepath_mapping = {  # local -> s3 filepath
        s3_filepath_to_local(s3_filepath, local_directory=local_directory): s3_filepath
        for s3_filepath in s3_filepaths
    }
    to_download = set(filepath_mapping.keys()) - set(already_local_filepaths)

    _logger.info(
        "Downloading %d files using %d workers...", len(to_download), os.cpu_count(),
    )
    downloaded_filepaths = multiprocessing.map_function(
        function=download_file,
        function_args=[
            [filepath_mapping[filepath] for filepath in to_download],
            [local_directory] * len(to_download),
        ],
    )
    _logger.info(
        "Downloaded %.5f GB of satellite data.",
        sum(os.path.getsize(f) for f in downloaded_filepaths) / 1e9,
    )
    return list(filepath_mapping.keys())
