from textwrap import dedent
from dataclasses import dataclass
from enum import Enum, unique
from typing import List, Optional, Tuple
from math import ceil
from json import dumps
from aiohttp import ClientSession
from pandas import DataFrame


def max_min_query_params(oid_field: str) -> dict:
    """ Returns the query params for calculating the max and min OID values """
    max_stat = {
        "statisticType": "max",
        "onStatisticField": oid_field,
        "outStatisticFieldName": "MAX_VALUE",
    }
    min_stat = {
        "statisticType": "min",
        "onStatisticField": oid_field,
        "outStatisticFieldName": "MIN_VALUE"
    }
    return {
        "outStatistics": dumps([max_stat, min_stat]),
        "f": "json",
    }


@unique
class RestGeometryType(Enum):
    Point = "esriGeometryPoint"
    Multipoint = "esriGeometryMultipoint"
    Polyline = "esriGeometryPolyline"
    Polygon = "esriGeometryPolygon"
    Envelope = "esriGeometryEnvelope"
    None_ = "esriGeometryNone"


@unique
class RestFieldType(Enum):
    Blob = "esriFieldTypeBlob"
    Date = "esriFieldTypeDate"
    Double = "esriFieldTypeDouble"
    Float = "esriFieldTypeFloat"
    Geometry = "esriFieldTypeGeometry"
    GlobalID = "esriFieldTypeGlobalID"
    GUID = "esriFieldTypeGUID"
    Integer = "esriFieldTypeInteger"
    OID = "esriFieldTypeOID"
    Raster = "esriFieldTypeRaster"
    Single = "esriFieldTypeSingle"
    SmallInteger = "esriFieldTypeSmallInteger"
    String = "esriFieldTypeString"
    XML = "esriFieldTypeXML"


class RestField:

    def __init__(self, field: dict) -> None:
        self.name = field["name"]
        self.type = RestFieldType(field["type"])
        self.alias = field["alias"]
        if "domain" in field and field["domain"] and field["domain"]["type"] == "codedValue":
            domain = field["domain"]
            self.is_code = True
            self.codes = {
                str(codedValue["code"]): codedValue["name"]
                for codedValue in domain["codedValues"]
            }
        else:
            self.is_code = False
            self.codes = {}

    @staticmethod
    def for_geometry(name: str):
        return RestField(field={
            "name": name,
            "alias": name,
            "type": RestFieldType.Geometry.value,
        })

    @property
    def as_dict(self) -> dict:
        return {
            "name": self.name,
            "type": self.type.value,
            "alias": self.alias,
            "is_code": self.is_code,
        }


@dataclass
class RestMetadata:
    url: str
    name: str
    source_count: int
    max_record_count: int
    pagination: bool
    stats: bool
    server_type: str
    geo_type: RestGeometryType
    fields: List[RestField]
    oid_field: Optional[RestField]
    max_min_oid: Tuple[int, int]
    inc_oid: bool
    source_spatial_reference: Optional[int]
    output_spatial_reference: Optional[int]
    """
    Data class for the backing information for an ArcGIS REST server and how to query the service

    Parameters
    ----------
    url : str
        Base url of the service. Used to collect data and generate queries
    name: str
        Name of the REST service. Used to generate the file name of the output
    source_count : int
        Number of records found within the service
    max_record_count : int
        Max number of records the service allows to be scraped in a single query. This is only used
        for generating queries if it's less than 10000. A property of the class, scrape_count,
        provides the true count that is use for generating queries
    pagination: bool
        Does the source provide the ability to page results. Easiest query generation
    stats: bool
        Does the source provide the ability to query statistics about the data. Used to get the max
        and min Oid field values to generate queries
    server_type : str
        Property of the server that denotes if geometry is available for each feature
    geo_type : str
        Geometry type for each feature. Guides how geometry is stored in CSV
    fields : List[str]
        Field names for each feature
    oid_field : str
        Name of unique identifier field for each feature. Used when pagination is not provided
    max_min_oid: Tuple[int, int]
        max and min Oid field values if that method is required to query all features. Defaults to
        -1 values if not required
    inc_oid : bool
        Is the Oid fields a sequential number. Checked using source_count and max Oid values
    """

    @staticmethod
    async def from_url(url: str, ssl: bool, output_spatial_reference: Optional[int] = None):
        count_query_params = {
            "where": "1=1",
            "returnCountOnly": "true",
            "f": "json",
        }
        field_query_params = {
            "f": "json"
        }
        ids_only_params = {
            "where": "1=1",
            "returnIdsOnly": "true",
            "f": "json",
        }
        source_count = -1
        server_type = ""
        name = ""
        max_record_count = -1
        pagination = False
        stats = False
        geo_type = RestGeometryType.None_
        fields = []
        oid_field = ""
        max_min_oid = (-1, -1)
        inc_oid = False
        query_url = f"{url}/query"
        source_spatial_reference = None

        async with ClientSession() as session:
            async with session.get(query_url,
                                   params=count_query_params,
                                   ssl=ssl) as response:
                if response.status == 200:
                    json = await response.json(content_type=response.content_type)
                    source_count = json.get("count", -1)

            async with session.get(url, params=field_query_params, ssl=ssl) as response:
                if response.status == 200:
                    json = await response.json(content_type=response.content_type)
                    advanced_query = json.get("advancedQueryCapabilities", dict())
                    server_type = json["type"]
                    name = json["name"]
                    max_record_count = int(json["maxRecordCount"])
                    spatial_reference_obj = json.get("sourceSpatialReference", dict())
                    source_spatial_reference = spatial_reference_obj.get("wkid", None)
                    if source_spatial_reference is not None:
                        source_spatial_reference = int(source_spatial_reference)
                    if advanced_query:
                        pagination = advanced_query.get("supportsPagination", False)
                    else:
                        pagination = json.get("supportsPagination", False)
                    if advanced_query:
                        stats = advanced_query.get("supportsStatistics", False)
                    else:
                        stats = json.get("supportsStatistics", False)
                    geo_type = RestGeometryType(json.get("geometryType", ""))
                    fields = [
                        RestField(field) for field in json["fields"]
                        if field["name"] != "Shape" and field["type"] != "esriFieldTypeGeometry"
                    ]
                    match geo_type:
                        case RestGeometryType.Point:
                            fields += [RestField.for_geometry("X"), RestField.for_geometry("Y")]
                        case RestGeometryType.Multipoint:
                            fields += [RestField.for_geometry("POINTS")]
                        case RestGeometryType.Polygon:
                            fields += [RestField.for_geometry("RINGS")]
                        case RestGeometryType.Polyline:
                            fields += [RestField.for_geometry("PATHS")]
                        case RestGeometryType.Point:
                            fields += [RestField.for_geometry("ENVELOPE")]
                    oid_fields = [
                        field for field in fields
                        if field.type == RestFieldType.OID
                    ]
                    if oid_fields:
                        oid_field = oid_fields[0]
            if not pagination and stats and oid_field:
                async with session.get(query_url,
                                       params=max_min_query_params(oid_field),
                                       ssl=ssl) as response:
                    if response.status == 200:
                        json = await response.json(content_type=response.content_type)
                        attributes = json["features"][0]["attributes"]
                        max_min_oid = (attributes["MAX_VALUE"], attributes["MIN_VALUE"])
                        diff = max_min_oid[0] - max_min_oid[1] + 1
                        inc_oid = diff == source_count
            elif not pagination and not stats and oid_field:
                async with session.get(query_url,
                                       params=ids_only_params,
                                       ssl=ssl) as response:
                    if response.status == 200:
                        json = await response.json(content_type=response.content_type)
                        oid_values = json["objectIds"]
                        max_min_oid = (max(oid_values), min(oid_values))
                        diff = max_min_oid[0] - max_min_oid[1] + 1
                        inc_oid = diff == source_count
        return RestMetadata(
            url,
            name,
            source_count,
            max_record_count,
            pagination,
            stats,
            server_type,
            geo_type,
            fields,
            oid_field,
            max_min_oid,
            inc_oid,
            source_spatial_reference,
            output_spatial_reference,
        )

    @property
    def query_url(self) -> str:
        return f"{self.url}/query"

    @property
    def scrape_count(self) -> int:
        """ Used for generating queries. Caps feature count per query to 10000 """
        return self.max_record_count if self.max_record_count <= 10000 else 10000

    @property
    def oid_query_count(self) -> int:
        """ Number of queries needed if Oid field used """
        return ceil((self.max_min_oid[0] - self.max_min_oid[1] + 1) / self.scrape_count)

    @property
    def pagination_query_count(self) -> int:
        """ Number of queries needed if pagination used """
        return ceil(self.source_count / self.scrape_count)

    @property
    def is_table(self) -> bool:
        """ Checks if the service is a Table type (ie no geometry provided) """
        return self.server_type == "TABLE"

    @property
    def geo_params(self) -> dict:
        """
        Returns the request params for the feature's geometry. Empty dict if the server is a table
        """
        if self.is_table:
            return {}
        if self.output_spatial_reference is not None:
            return {
                "geometryType": self.geo_type.value,
                "outSR": self.output_spatial_reference,
            }
        elif self.source_spatial_reference is not None:
            return {
                "geometryType": self.geo_type.value,
                "outSR": self.source_spatial_reference,
            }
        else:
            raise AttributeError("No spatial reference provided to service with geometry")

    def print_formatted(self):
        """ Prints class attributes to the console """
        print("Metadata")
        print("--------")
        temp_str = f"""\
            URL: {self.url}
            Name: {self.name}
            Feature Count: {self.source_count}
            Max Scrape Chunk Count: {self.max_record_count}
            Server Type: {self.server_type}"""
        print(dedent(temp_str))
        if not self.is_table:
            print(f"Geometry Type: {self.geo_type.value}")
        print("Fields:")
        df_fields = DataFrame(data=(field.as_dict for field in self.fields))
        print(df_fields.to_string(index=False))
        if self.oid_field is not None:
            print(f"OID Field: {self.oid_field.name}")
        if self.source_spatial_reference is not None:
            print(f"Source Spatial Reference: {self.source_spatial_reference}")
        if self.output_spatial_reference is not None:
            print(f"Output Spatial Reference: {self.output_spatial_reference}")

    @property
    def queries(self) -> List[Tuple[str, dict]]:
        """
        Get all the queries for this service. Returns empty list when no query method available

        TO-DO
        ----
        - find other query methods when current methods exhausted
        """
        if self.pagination:
            return [
                (self.query_url, self.get_pagination_query_params(i))
                for i in range(self.pagination_query_count)
            ]
        elif self.oid_field:
            return [
                (self.query_url, self.get_oid_query_params(i))
                for i in range(self.oid_query_count)
            ]
        else:
            return []

    def get_pagination_query_params(self, query_num: int) -> dict:
        """
        Generate query params for service when pagination is supported using query_num to get offset
        """
        return {
            "where": "1=1",
            "resultOffset": query_num * self.scrape_count,
            "resultRecordCount": self.scrape_count,
            "outFields": "*",
            "f": "json",
        } | self.geo_params

    def get_oid_query_params(self, index: int) -> dict:
        """
        Generate query params for service when Oid is available using a starting Oid number and an
        offset
        """
        min_oid = self.max_min_oid[1] + (index * self.scrape_count)
        max_oid = min_oid + self.scrape_count - 1
        return {
            "where": f"{self.oid_field} > {min_oid} and {self.oid_field} < {max_oid}",
            "outFields": "*",
            "f": "json",
        } | self.geo_params
