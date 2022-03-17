from dataclasses import dataclass
from typing import List, Tuple
from math import ceil
from json import dumps
import aiohttp


def max_min_query(oid_field: str) -> str:
    """
    Helper query postfix to get max and min Oid values. Given an Oid field name, can be added to the
    end of a base url
    """
    return f'/query?outStatistics=%5B%0D%0A+%7B%0D%0A++++"statisticType"%3A+"max"%2C%0D%0A' \
           f'++++"onStatisticField' \
           f'"%3A+"{oid_field}"%2C+++++%0D%0A++++"outStatisticFieldName"%3A+"MAX_VALUE"%0D%0A' \
           f'++%7D%2C%0D%0A++%7B%0D%0A++++"statisticType"%3A+"min"%2C%0D%0A++++"onStatisticField' \
           f'"%3A+"{oid_field}"%2C+++++%0D%0A++++"outStatisticFieldName"%3A+"MIN_VALUE"%0D%0A' \
           f'++%7D%0D%0A%5D&f=json'


@dataclass
class RestMetadata:
    url: str
    name: str
    source_count: int
    max_record_count: int
    pagination: bool
    stats: bool
    server_type: str
    geo_type: str
    fields: List[str]
    oid_field: str
    max_min_oid: Tuple[int, int]
    inc_oid: bool
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
    async def from_url(url: str):
        count_query = "/query?where=1%3D1&returnCountOnly=true&f=json"
        field_query = "?f=json"
        source_count = -1
        server_type = ""
        name = ""
        max_record_count = -1
        pagination = False
        stats = False
        geo_type = ""
        fields = []
        oid_field = ""
        max_min_oid = (-1, -1)
        inc_oid = False

        async with aiohttp.ClientSession() as session:
            async with session.get(url + count_query) as response:
                if response.status == 200:
                    json = await response.json(content_type=response.content_type)
                    source_count = json.get("count", -1)

            async with session.get(url + field_query) as response:
                if response.status == 200:
                    json = await response.json(content_type=response.content_type)
                    advanced_query = json.get("advancedQueryCapabilities", dict())
                    server_type = json["type"]
                    name = json["name"]
                    max_record_count = int(json["maxRecordCount"])
                    if advanced_query:
                        pagination = advanced_query.get("supportsPagination", False)
                    else:
                        pagination = json.get("supportsPagination", False)
                    if advanced_query:
                        stats = advanced_query.get("supportsStatistics", False)
                    else:
                        stats = json.get("supportsStatistics", False)
                    geo_type = json.get("geometryType", "")
                    fields = [
                        field["name"] for field in json["fields"]
                        if field["name"] != "Shape" and field["type"] != "esriFieldTypeGeometry"
                    ]
                    if geo_type == "esriGeometryPoint":
                        fields += ["X", "Y"]
                    elif geo_type == "esriGeometryMultipoint":
                        fields += ["POINTS"]
                    elif geo_type == "esriGeometryPolygon":
                        fields += ["RINGS"]
                    oid_fields = [
                        field["name"] for field in json["fields"]
                        if field["type"] == "esriFieldTypeOID"
                    ]
                    if oid_fields:
                        oid_field = oid_fields[0]
            if not pagination and stats and oid_field:
                async with session.get(url + max_min_query(oid_field), ssl=False) as response:
                    if response.status == 200:
                        json = await response.json(content_type=response.content_type)
                        attributes = json["features"][0]["attributes"]
                        max_min_oid = (attributes["MAX_VALUE"], attributes["MIN_VALUE"])
                        diff = max_min_oid[0] - max_min_oid[1] + 1
                        inc_oid = diff == source_count
            elif not pagination and not stats and oid_field:
                async with session.get(url + "/query?where=1%3D1&returnIdsOnly=true&f=json", ssl=False) as response:
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
            inc_oid
        )

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
    def geo_text(self) -> str:
        """
        String added to the queries for geometry. If service is a Table then empty string.
        Adds an out spatial reference for geometry to NAD83. Might need to be changed in the future

        TO-DO
        ----
        - add ability to provide spatial reference override for non-NA services
        """
        return "" if self.is_table else f"&geometryType={self.geo_type}&outSR=4269"

    @property
    def json_text(self) -> str:
        """ Converts class attributes to a dict for displaying details as JSON text """
        return dumps(
            {
                "URL": self.url,
                "Name": self.name,
                "Source Count": self.source_count,
                "Max Record Count": self.max_record_count,
                "Pagination": self.pagination,
                "Stats": self.stats,
                "Server Type": self.server_type,
                "Geometry Type": self.geo_type,
                "Fields": self.fields,
                "OID Fields": self.oid_field,
                "Max Min OID": self.max_min_oid,
                "Incremental OID": self.inc_oid
            },
            indent=4
        )

    @property
    def queries(self) -> List[str]:
        """
        Get all the queries for this service. Returns empty list when no query method available

        TO-DO
        ----
        - find other query methods when current methods exhausted
        """
        if self.pagination:
            return [
                self.url + self.get_pagination_query(i)
                for i in range(self.pagination_query_count)
            ]
        elif self.oid_field:
            return [
                self.url + self.get_oid_query(self.max_min_oid[1] + (i * self.scrape_count))
                for i in range(self.oid_query_count)
            ]
        else:
            return []

    def get_pagination_query(self, query_num: int) -> str:
        """ Generate query for service when pagination is supported using query_num to get offset """
        return f"/query?where=1+%3D+1&resultOffset={query_num * self.scrape_count}" \
               f"&resultRecordCount={self.scrape_count}{self.geo_text}&outFields=*&f=json"

    def get_oid_query(self, min_oid: int) -> str:
        """ Generate query for service when Oid is available using a starting Oid number and an offset """
        return f"/query?where={self.oid_field}+>%3D+{min_oid}+and+" \
               f"{self.oid_field}+<%3D+{min_oid + self.scrape_count - 1}" \
               f"{self.geo_text}&outFields=*&f=json"
