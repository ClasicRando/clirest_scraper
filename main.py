import asyncio
from metadata import RestMetadata
import sys


async def main():
    metadata = await RestMetadata.from_url(
        "https://x-23.env.nm.gov/arcgis/rest/services/pstb/leaking_petroleum_storage_tank_sites"
        "/FeatureServer/0"
    )
    print(metadata.json_text)


if __name__ == "__main__":
    if sys.platform in ("win32", "cygwin"):
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())
