import asyncio
from metadata import RestMetadata
from scraping import fetch_query
import sys
from csv import reader, writer, QUOTE_MINIMAL


async def main():
    metadata = await RestMetadata.from_url(
        "https://x-23.env.nm.gov/arcgis/rest/services/pstb/leaking_petroleum_storage_tank_sites"
        "/FeatureServer/0"
    )
    print(metadata.json_text)
    if input("Proceed with scrape? (y/n)").upper() == "Y":
        tasks = (fetch_query(query, metadata) for query in metadata.queries)
        with open(f"{metadata.name}.csv", newline="") as output_file:
            csv_writer = writer(output_file, delimter=",", quotechar='"', quoting=QUOTE_MINIMAL)
            csv_writer.writerow(metadata.fields)
            for file in await asyncio.gather(*tasks):
                with open(file, newline="") as csv_file:
                    csv_writer.writerows(
                        reader(
                            csv_file,
                            delimiter=",",
                            quotechar='"'
                        )
                    )

if __name__ == "__main__":
    if sys.platform in ("win32", "cygwin"):
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())
