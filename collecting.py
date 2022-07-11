from typing import Optional, Any
from geopandas import GeoDataFrame, read_feather, read_parquet, read_file
from pathlib import Path
from os.path import abspath
from pyarrow import Table
from pyarrow.parquet import ParquetWriter, read_table as read_table_pq
from pyarrow.feather import read_table as read_table_ft
from json import loads as json_loads, dumps as json_dumps


class OutputWriter:

    __supported_output_types = (".CSV", ".GEOJSON", ".SHP", ".PARQUET")

    def __init__(self, output_path: Path) -> None:
        self.output_path = output_path
        self.output_type = output_path.suffix.upper()
        if self.output_type not in self.__supported_output_types:
            raise Exception(f"Provided an unsupported file type to write, '{self.output_type}'")
        self.schema: Optional[Any] = None
        self.writer: Optional[ParquetWriter] = None

    def _read_data(self, data: str | Path) -> GeoDataFrame | Table:
        path = data if isinstance(data, Path) else Path(data)
        read_as_table = self.output_type == ".PARQUET" and self.writer is not None
        match path.suffix.upper():
            case ".FEATHER":
                return read_table_ft(path) if read_as_table else read_feather(path)
            case ".PARQUET":
                return read_table_pq(path) if read_as_table else read_parquet(path)
            case ".GEOJSON":
                return read_file(data, driver="GeoJSON", mode="r")
            case _:
                raise Exception(f"Tried to read an unsupported file type, {data}")

    def write_data(self, data: str | Path | GeoDataFrame) -> None:
        if isinstance(data, str) or isinstance(data, Path):
            df = self._read_data(data)
        elif isinstance(data, GeoDataFrame):
            df = data
        else:
            input_type = type(data)
            raise Exception(f"Expected an input of str, Path or GeoDataFrame but got {input_type}")
        match self.output_type:
            case ".CSV":
                df.to_wkt().to_csv(
                    self.output_path,
                    index=False,
                    mode="a",
                )
            case ".GEOJSON":
                df.to_file(
                    abspath(self.output_path),
                    driver="GeoJSON",
                    index=False,
                    mode="a",
                )
            case ".SHP":
                df.to_file(
                    abspath(self.output_path),
                    driver="ESRI Shapefile",
                    index=False,
                    mode="a",
                )
            case ".PARQUET":
                # noinspection PyArgumentList
                table = Table.from_pandas(df.to_wkb()) if isinstance(df, GeoDataFrame) else df
                if self.writer is None:
                    metadata = {
                        "version": "0.4.0",
                        "primary_column": "geometry",
                        "columns": {
                            "geometry": {
                                "encoding": "WKB",
                                "geometry_type": [df.geom_type.iloc[0]],
                                "crs": json_loads(df.crs.to_json()),
                                "bbox": [round(x, 4) for x in df.total_bounds]
                            }
                        }
                    }
                    schema = table.schema.with_metadata({"geo": json_dumps(metadata)})
                    self.writer = ParquetWriter(self.output_path, schema)
                self.writer.write_table(table)