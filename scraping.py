from os import remove as os_remove
from asyncio import sleep
from tempfile import NamedTemporaryFile
from numpy import format_float_positional
from typing import Any, List
from json import dumps
from aiohttp import ClientSession, ClientConnectorError, ClientError, ClientSSLError
from metadata import RestField, RestFieldType, RestGeometryType
from functools import reduce
from operator import iconcat
from tqdm import tqdm


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


def convert_json_field(x: Any, field: RestField) -> List[str]:
    if field.is_code:
        code = convert_json_value(x)
        return [code.strip(), field.codes.get(code, '').strip()]
    return [convert_json_value(x).strip()]


def handle_record(fields: List[RestField], geo_type: RestGeometryType, feature: dict) -> List[str]:
    """
    Parameters
    ----------
    fields : List[RestField]
        service fields to collect feature attributes
    geo_type : RestGeometryType
        geometry type from the RestMetadata object
    feature : str
        json object from the query's feature json array
    Return
    ------
    feature object converted to List[str] with geometry is applicable
    """
    attributes = feature["attributes"]
    # collect all values from the attributes key and convert them to string
    record = [
        convert_json_field(attributes[field.name], field=field)
        for field in fields
        if field.type != RestFieldType.Geometry
    ]
    # If geometry is point, get X and Y and add to the record. If no geometry present, default to a
    # blank X and Y
    match geo_type:
        case RestGeometryType.Point:
            record += [
                [convert_json_value(point).strip()]
                for point in feature.get("geometry", {"x": "", "y": ""}).values()
            ]
        # If geometry is multipoint, join coordinates into a list of points using json list notation
        # and add to the record
        case RestGeometryType.Multipoint:
            record += [[convert_json_value(feature["geometry"]["points"]).strip()]]
        # If geometry is Polyline get the paths and add the value to the record
        case RestGeometryType.Polyline:
            record += [[convert_json_value(feature["geometry"]["paths"]).strip()]]
        # If geometry is Polygon get the rings and add the value to the record
        case RestGeometryType.Polygon:
            record += [[convert_json_value(feature["geometry"]["rings"]).strip()]]
        # If geometry is Envelope get each bound and add the dict to the record
        case RestGeometryType.Envelope:
            geometry = feature["geometry"]
            bounds_map = {}
            if "xmin" in geometry:
                bounds_map["xmin"] = geometry["xmin"]
            if "ymin" in geometry:
                bounds_map["ymin"] = geometry["ymin"]
            if "xmax" in geometry:
                bounds_map["xmax"] = geometry["xmax"]
            if "ymax" in geometry:
                bounds_map["ymax"] = geometry["ymax"]
            if "zmin" in geometry:
                bounds_map["zmin"] = geometry["zmin"]
            if "zmax" in geometry:
                bounds_map["zmax"] = geometry["zmax"]
            if "mmin" in geometry:
                bounds_map["mmin"] = geometry["mmin"]
            if "mmax" in geometry:
                bounds_map["mmax"] = geometry["mmax"]
            record += [[convert_json_value(bounds_map).strip()]]
    return list(reduce(
        iconcat,
        record,
    ))


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
        suffix=".csv",
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
                            t.write(f"Error: {query} got this response:\n{response_text}")
                        else:
                            json_response = await response.json(content_type=response.content_type)
                            # Check to make sure JSON response has features
                            invalid_response = not await check_json_response(json_response)
                    except ClientConnectorError:
                        t.write("Client connection error... sleeping for 5sec")
                        await sleep(5)
                        invalid_response = True
                if try_number > options["tries"]:
                    raise Exception(f"Too many tries to fetch query ({query})")
            # write all rows to temp csv file using a mapping generator
            with open(temp_file.name, "w", newline="", encoding="utf8") as csv_file:
                for feature in json_response["features"]:
                    record = handle_record(options["fields"], options["geo_type"], feature)
                    line = ",".join([handle_csv_value(value) for value in record]) + "\n"
                    csv_file.write(line)
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
