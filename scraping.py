import os
from asyncio import sleep
from functools import partial
from tempfile import NamedTemporaryFile
from numpy import format_float_positional
from typing import Any, List
from json import dumps
from aiohttp import ClientSession, ClientConnectorError, ClientError
from metadata import RestMetadata
from csv import writer, QUOTE_MINIMAL


def convert_json_value(x: Any) -> str:
    """
    Function used to transform elements of a DataFrame to string based upon the type of object

    This is the default implementation of the function that a FileLoader will use if the user does
    not supply their own function

    Parameters
    ----------
    x : Any
        an object of Any type stored in a DataFrame
    Returns
    -------
     string conversion of the values based upon it's type
    """
    if isinstance(x, str):
        return x
    elif isinstance(x, int):
        return str(x)
    # Note that for JSON numbers, some truncation might occur during json load into python dict
    elif isinstance(x, float):
        return format_float_positional(x).rstrip(".")
    elif isinstance(x, bool):
        return "TRUE" if x else "FALSE"
    elif x is None:
        return ""
    elif x is list:
        return dumps(x)
    else:
        return str(x)


def handle_record(geo_type: str, feature: dict) -> List[str]:
    """
    Parameters
    ----------
    geo_type : str
        geometry type from the RestMetadata object
    feature : str
        json object from the query's feature json array
    Return
    ------
    feature object converted to List[str] with geometry is applicable
    """
    # collect all values from the attributes key and convert them to string
    record = [
        convert_json_value(value).strip()
        for value in feature["attributes"].values()
    ]
    # If geometry is point, get X and Y and add to the record. If no geometry present, default to a
    # blank X and Y
    if geo_type == "esriGeometryPoint":
        record += [
            convert_json_value(point).strip()
            for point in feature.get("geometry", {"x": "", "y": ""}).values()
        ]
    # If geometry is multi point, join coordinates into a list of points using json list notation
    # and add to the record
    elif geo_type == "esriGeometryMultipoint":
        record += [convert_json_value(feature["geometry"]["points"]).strip()]
    # If geometry is Polygon get the rings and add the value to the record
    elif geo_type == "esriGeometryPolygon":
        record += [convert_json_value(feature["geometry"]["rings"]).strip()]
    # Other geometries could exist but are not currently handled
    return record


async def fetch_query(query: str,
                      rest_metadata: RestMetadata,
                      max_tries: int = 10) -> NamedTemporaryFile:
    temp_file = NamedTemporaryFile(
        mode="w",
        encoding="utf8",
        suffix=".csv",
        delete=False,
        newline="\n"
    )
    temp_file.close()
    async with ClientSession() as session:
        try:
            invalid_response = True
            json_response = dict()
            try_number = 1
            while invalid_response:
                async with session.get(query) as response:
                    try:
                        invalid_response = response.status != 200
                        if invalid_response:
                            print(f"Error: {query} got this response:\n{await response.text()}")
                        json_response = await response.json()
                        # Check to make sure JSON response has features
                        if "features" not in json_response.keys():
                            # No features in response and JSON has an error code, retry query
                            if "error" in json_response.keys():
                                print("Request had an error... trying again")
                                invalid_response = True
                                # Sleep to give the server sometime to handle the request again
                                await sleep(10)
                                try_number += 1
                            # No features in response and no error code. Raise error which
                            # terminates all operations
                            else:
                                raise KeyError("Response was not an error but no features found")
                    except ClientConnectorError:
                        await sleep(10)
                        invalid_response = True
                        try_number += 1
                if try_number > max_tries:
                    raise Exception(f"Too many tries to fetch query ({query})")

            # Once query is successful, Map features from response using handle_record and geo_type
            data = map(
                partial(handle_record, rest_metadata.geo_type),
                json_response["features"]
            )
            # Create Dataframe using records and fields
            with open(temp_file.name, "w", newline="", encoding="utf8") as csv_file:
                csv_writer = writer(csv_file, delimiter=",", quotechar='"', quoting=QUOTE_MINIMAL)
                csv_writer.writerows(data)
        except ClientError as ex:
            os.remove(temp_file.name)
            raise ex
        return temp_file
