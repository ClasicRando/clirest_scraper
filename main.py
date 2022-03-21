import asyncio
import os
import traceback
import sys
import time
from metadata import RestMetadata
from scraping import fetch_query
from csv import reader, writer, QUOTE_MINIMAL
from argparse import ArgumentParser, Namespace
from tqdm import tqdm


async def fetch_worker(queue: asyncio.Queue, done_queue: asyncio.Queue, ssl: bool):
    while True:
        query, params, geo_type, max_tries = await queue.get()
        result = await fetch_query(query, params, geo_type, max_tries, ssl)
        await done_queue.put(result)
        queue.task_done()


async def csv_writer_worker(queue: asyncio.Queue, metadata: RestMetadata):
    with open(f"{metadata.name}.csv", encoding="utf8", mode="w", newline="") as output_file:
        csv_writer = writer(output_file, delimiter=",", quotechar='"', quoting=QUOTE_MINIMAL)
        csv_writer.writerow(metadata.fields)
        results_handled = 0
        total_results = len(metadata.queries)
        t = tqdm(total=total_results)
        while True:
            result = await queue.get()
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
            results_handled += 1
            queue.task_done()
            t.update(1)


async def main(args: Namespace):
    metadata = await RestMetadata.from_url(args.url, args.ssl, args.sr)
    proceed = "Y" if args.yes is None else args.yes
    print(metadata.json_text)
    if proceed == "N":
        proceed = input("Proceed with scrape? (y/n)").upper()
    if proceed == "Y":
        fetch_worker_queue = asyncio.Queue(args.workers)
        writer_queue = asyncio.Queue(args.workers)
        start = time.time()
        workers = [asyncio.create_task(fetch_worker(fetch_worker_queue, writer_queue, args.ssl))]
        writer_task = asyncio.create_task(csv_writer_worker(writer_queue, metadata))

        for (query, params) in metadata.queries:
            await fetch_worker_queue.put((query, params, metadata.geo_type, args.tries))

        await fetch_worker_queue.join()

        for worker in workers:
            worker.cancel()

        await writer_queue.join()
        writer_task.cancel()

        print(f"Scraping done. Took {time.time() - start} seconds")
    print("Exiting Program")

if __name__ == "__main__":
    if sys.platform in ("win32", "cygwin"):
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    parser = ArgumentParser()
    parser.add_argument(
        "--url",
        "-u",
        help="base url of the arcgis rest service",
        required=True
    )
    parser.add_argument(
        "--yes",
        "-y",
        help="accept scrape without confirmation of details",
        default="N",
        required=False,
        nargs="?"
    )
    parser.add_argument(
        "--tries",
        "-t",
        help="max number of tries for a scraping query before operation is cancelled (Default: 10)",
        type=int,
        default=10,
        required=False
    )
    parser.add_argument(
        "--ssl",
        help="synonymous with ssl option for requests/aiohttp library GET request (Default: True)",
        type=bool,
        default=True,
        required=False
    )
    parser.add_argument(
        "--workers",
        "-w",
        help="number of workers spawned to perform the HTTP requests (Default: 10)",
        type=int,
        default=10,
        required=False
    )
    parser.add_argument(
        "--sr",
        help="spatial reference code (epsg) to project the geometry (Default: 4269)",
        type=int,
        default=4269,
        required=False
    )
    asyncio.run(main(parser.parse_args()))
