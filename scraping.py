from os import remove as os_remove
from asyncio import sleep
from tempfile import NamedTemporaryFile
from geopandas import GeoDataFrame
from numpy import format_float_positional
from typing import Any, List
from json import dumps
from aiohttp import ClientSession, ClientConnectorError, ClientError, ClientSSLError
from metadata import RestField, RestFieldType
from tqdm import tqdm
from datetime import datetime


def epoch_to_utc_timestamp(epoch: float) -> str:
    return datetime.utcfromtimestamp(epoch/1000).strftime("%Y-%m-%d %H:%M:%S%z")


class Feature:

    def __init__(self, feature_dict: dict, fields: List[RestField], dates: bool) -> None:
        self.type = feature_dict["type"]
        self.id = feature_dict["id"]
        self.geometry = feature_dict["geometry"]
        self.properties = {
            key: convert_json_value(value)
            for key, value in feature_dict["properties"].items()
        }
        for field in fields:
            if field.type == RestFieldType.Date and dates:
                value = self.properties[field.name]
                if value == "":
                    self.properties[f"{field.name}_DT"] = ""
                else:
                    self.properties[f"{field.name}_DT"] = epoch_to_utc_timestamp(float(value))
            elif field.is_code:
                value = self.properties[field.name]
                self.properties[f"{field.name}_DESC"] = field.codes.get(value, '').strip()
        self.__geo_interface__ = {
            "type": self.type,
            "id": self.id,
            "geometry": self.geometry,
            "properties": self.properties,
        }


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
    if x is None:
        return ""
    if isinstance(x, int):
        return str(x)
    # Note that for JSON numbers, some truncation might occur during json load into python dict
    if isinstance(x, float):
        return format_float_positional(x).rstrip(".")
    if isinstance(x, bool):
        return "TRUE" if x else "FALSE"
    if x is list:
        return dumps(x)
    return str(x)


async def check_json_response(response: dict) -> bool:
    """
    Parameters
    ----------
    response : dict
        json response from an HTTP request
    Return
    ------
    A boolean value indicating if the response is valid. Raises an error if the issue with the
    response cannot be recovered from
    """
    if "features" not in response.keys():
        # No features in response and JSON has an error code, retry query
        if "error" in response.keys():
            print("Request had an error... sleeping for 5sec to try again")
            # Sleep to give the server sometime to handle the request again
            await sleep(5)
            return False
        # No features in response and no error code. Raise error which
        # terminates all operations
        else:
            raise KeyError("Response was not an error but no features found")
    return True


def handle_csv_value(value: str) -> str:
    if any((char == ',' or char == '"' or char == '\r' or char == '\n' for char in value)):
        new_value = value.replace("\"", "\"\"")
        return f'"{new_value}"'
    return value


async def fetch_query(t: tqdm,
                      query: str,
                      params: dict,
                      options: dict) -> NamedTemporaryFile:
    temp_file = NamedTemporaryFile(
        mode="w",
        encoding="utf8",
        suffix=".feather",
        dir="temp_files",
        delete=False,
        newline="\n"
    )
    temp_file.close()
    async with ClientSession() as session:
        try:
            invalid_response = True
            json_response = dict()
            try_number = 0
            while invalid_response:
                async with session.get(query, params=params, ssl=options["ssl"]) as response:
                    try_number += 1
                    try:
                        invalid_response = response.status != 200
                        if invalid_response:
                            response_text = await response.text()
                            t.write(f"Error: {query}, {params} got this response:\n{response_text}")
                        else:
                            json_response = await response.json(content_type=response.content_type)
                            # Check to make sure JSON response has features
                            invalid_response = not await check_json_response(json_response)
                    except ClientConnectorError:
                        t.write("Client connection error... sleeping for 5sec")
                        await sleep(5)
                        invalid_response = True
                if try_number > options["tries"]:
                    raise Exception(f"Too many tries to fetch query ({query}, {params})")
            df = GeoDataFrame.from_features(
                features=(
                    Feature(feature, options["fields"], options["dates"])
                    for feature in json_response["features"]
                ),
                crs=json_response["crs"]["properties"]["name"],
            )
            df.to_feather(temp_file.name, index=False)
        except ClientSSLError as ex:
            t.write("Client error raised. Issue with the service's SSL certification")
            t.write("To avoid this error you can provide '--ssl false' as a command line argument")
            t.write("THIS IS VERY RISKY!! Only use this argument if you are sure the site is legit")
            t.write(ex)
            os_remove(temp_file.name)
            raise ex
        except ClientError as ex:
            t.write(ex)
            os_remove(temp_file.name)
            raise ex
    return temp_file
