import asyncio
import os
import traceback
import sys
from metadata import RestMetadata
from scraping import fetch_query
from csv import reader, writer, QUOTE_MINIMAL


async def main():
    metadata = await RestMetadata.from_url(
        "https://x-23.env.nm.gov/arcgis/rest/services/pstb/leaking_petroleum_storage_tank_sites"
        "/FeatureServer/0"
    )
    print(metadata.json_text)
    if input("Proceed with scrape? (y/n)").upper() == "Y":
        tasks = (fetch_query(query, metadata) for query in metadata.queries)
        with open(f"{metadata.name}.csv", encoding="utf8", mode="w", newline="") as output_file:
            csv_writer = writer(output_file, delimiter=",", quotechar='"', quoting=QUOTE_MINIMAL)
            csv_writer.writerow(metadata.fields)
            for result in await asyncio.gather(*tasks):
                if isinstance(result, BaseException):
                    traceback.print_exc()
                    continue
                with open(result.name, newline="", encoding="utf8") as csv_file:
                    csv_writer.writerows(
                        reader(
                            csv_file,
                            delimiter=",",
                            quotechar='"'
                        )
                    )
                os.remove(result.name)

if __name__ == "__main__":
    if sys.platform in ("win32", "cygwin"):
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())
