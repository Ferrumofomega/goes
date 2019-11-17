import boto3
import datetime
import pandas as pd


def get_matching_s3_objects(bucket, prefix=None):
    """
    Generate objects in an S3 bucket.

    :param bucket: Name of the S3 bucket.
    :param prefix: Only fetch objects whose key starts with
        this prefix (optional).
    """
    s3 = boto3.client("s3", AWS_ACCESS_KEY_ID=AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY=AWS_SECRET_ACCESS_KEY)
    paginator = s3.get_paginator("list_objects_v2")

    kwargs = {'Bucket': bucket}

    # Pass the prefix directly to the S3 API.  If you pass
    # a tuple or list of prefixes, go through them one by one.
    if isinstance(prefix, str):
        prefixes = (prefix, )
    else:
        prefixes = prefix

    for key_prefix in prefixes:
        kwargs["Prefix"] = key_prefix

        for page in paginator.paginate(**kwargs):
            try:
                contents = page["Contents"]
                print(contents)
            except KeyError:
                return

            for obj in contents:
                yield obj


def get_matching_s3_keys(bucket, prefix=None):
    """
    Generate the keys in an S3 bucket.

    :param bucket: Name of the S3 bucket.
    :param prefix: Only fetch keys that start with this prefix (optional).
    """
    for obj in get_matching_s3_objects(bucket, prefix):

        yield obj["Key"]


def make_prefixes(product, date_range=None, date_time=None, bands=None):
    """
    Generate the prefixes for GOES files contained via s3 keys for a given date or date range, bands, and product.

    :param product: The GOES product we are targeting; accepts a string.
    :param date_range: A list of dates we want to download GOES data from; accepts lists of datetimes.
    :param date_time: A date we want to download GOES data from; accepts datetime instances.
    :param bands: The bands from the GOES data we're expecting; accepts instances of ints and lists of ints.
    """

    # Validate
    if bands is not None:
        if isinstance(bands, int):
            bands = [bands]
        elif isinstance(bands, list) and isinstance(bands[0], int):
                bands = bands
        else:
            raise ValueError("You need to pass either type list or int for param bands.")
    else:
        bands = list(range(0, 17))

    if date_time is None and date_range is None:
        raise ValueError("You need to pass either a date_range or date_time; please pass one through.")

    if date_time is not None and date_range is not None:
        raise ValueError("You need to pass either a date_range OR date_time; please pass only one through.")

    if date_range is not None:
        if isinstance(date_range[0], datetime.datetime) and isinstance(date_range, list):
            date_range = date_range
        else:
            raise ValueError("The date_range is not a list of type datetimes; please reformat param.")

    if date_time is not None:
        if isinstance(date_time, datetime.datetime):
            date_range = [date_time]
        else:
            raise ValueError("The date_time is not of type datetime; please reformat param.")

    # We construct prefixes from arguments to the function.
    prefixes = []
    for date in date_range:
        day_of_year = date.timetuple().tm_yday
        year = date.year
        hour = date.hour
        for r in bands:
            file_prefix = product + '/' + str(year) + '/' + str(day_of_year).zfill(3) + '/' + str(hour).zfill(
            2) + '/OR_' + product_name + '-M3C' + str(r).zfill(2)
            prefixes.append(file_prefix)
    print(prefixes)
    return prefixes



AWS_ACCESS_KEY_ID = ""
AWS_SECRET_ACCESS_KEY = ""
bucket_name = 'noaa-goes17'
product_name = 'ABI-L1b-RadC'

# We can handle a single date.
# date = datetime.datetime(2019, 11, 1, 1, 00)

# We can handle a list of dates.
start_date = datetime.datetime(2019, 10, 1, 1, 00)
end_date = datetime.datetime(2019, 10, 31, 1, 00)
delta = end_date - start_date
date_list = pd.date_range(start_date, end_date).tolist()

# Do we want any more granular dates/time handling?

prefixes = make_prefixes(product_name, date_range=date_list)