import asyncio
from metadata import RestMetadata
from scraping import fetch_query
from argparse import ArgumentParser, Namespace
from tqdm import tqdm
from pathlib import Path
from time import time as now
from sys import platform as sys_platform
from os import remove as os_remove, mkdir
from asyncio import Queue, create_task, set_event_loop_policy, run as asyncio_run
from collecting import OutputWriter, shapefile_extensions
from zipfile import ZipFile, ZIP_DEFLATED


async def fetch_worker(t: tqdm, queue: Queue, done_queue: Queue, options: dict):
    try:
        while True:
            query, params = await queue.get()
            result = await fetch_query(t, query, params, options)
            await done_queue.put(result)
            queue.task_done()
    except Exception as ex:
        t.write(f"Encountered an error in the fetch worker\n{ex}")
        return


async def csv_writer_worker(t: tqdm, queue: Queue, metadata: RestMetadata, out_type: str):
    try:
        output_file = Path("output_files", f"{metadata.name}.{out_type}")
        if not output_file.exists():
            output_file.touch()
        writer = OutputWriter(output_file)
        results_handled = 0
        while True:
            result = await queue.get()
            if isinstance(result, BaseException):
                raise result
            writer.write_data(result.name)
            os_remove(result.name)
            results_handled += 1
            queue.task_done()
            t.update(1)
    except Exception as ex:
        t.write(f"Encountered an error in the writer worker\n{ex}")
        return


async def main(args: Namespace):
    metadata = await RestMetadata.from_url(args.url, args.ssl, args.sr)
    proceed = "Y" if args.yes is None else args.yes
    metadata.print_formatted()
    if args.out == "shp":
        print("Warning: Column names longer than 10 characters will be truncated when saved to "
              "ESRI Shapefile.")
    if proceed == "N":
        proceed = input("Proceed with scrape? (y/n)").upper()
    if proceed == "Y":
        if not Path("output_files").exists():
            mkdir("output_files")
        if not Path("temp_files").exists():
            mkdir("temp_files")
        queries = await metadata.queries(args.ssl)
        total_results = len(queries)
        t = tqdm(total=total_results, leave=False)
        if args.workers <= 0 or args.workers > 10:
            args.workers = 10
        fetch_worker_count = args.workers if args.workers <= total_results else total_results
        fetch_worker_queue = Queue(fetch_worker_count)
        writer_queue = Queue(args.workers)
        start = now()
        options = {
            "ssl": args.ssl,
            "dates": args.dates,
            "tries": args.tries,
            "fields": metadata.fields,
            "geo_type": metadata.geo_type,
        }
        workers = [
            create_task(fetch_worker(t, fetch_worker_queue, writer_queue, options))
            for _ in range(fetch_worker_count)
        ]
        writer_task = create_task(csv_writer_worker(t, writer_queue, metadata, args.out))

        for (query, params) in queries:
            await fetch_worker_queue.put((query, params))

        await fetch_worker_queue.join()

        for worker in workers:
            worker.cancel()

        await writer_queue.join()
        writer_task.cancel()

        t.write(f"Scraping done. Took {round(now() - start, 2)} seconds")

        if args.out == "shp":
            t.write("Zipping Shapefile")
            zip_path = Path("output_files", f"{metadata.name}.zip")
            with ZipFile(zip_path, mode="w", compression=ZIP_DEFLATED) as zip_file:
                for extension in shapefile_extensions:
                    name = f"{metadata.name}.{extension}"
                    file_path = Path("output_files", name)
                    zip_file.write(filename=file_path, arcname=name)
                    os_remove(file_path)

        t.close()
    print("Exiting Program")

if __name__ == "__main__":
    if sys_platform in ("win32", "cygwin"):
        set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
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
        help="number of workers spawned to perform the HTTP requests (Default/Max: 10)",
        type=int,
        default=10,
        required=False
    )
    parser.add_argument(
        "--out",
        help="output file format",
        type=str,
        default="csv",
        choices=["csv", "geojson", "shp", "parquet"],
        required=False
    )
    parser.add_argument(
        "--sr",
        help="spatial reference code (epsg) to project the geometry",
        type=int,
        required=False
    )
    parser.add_argument(
        "--dates",
        "--d",
        help="convert date fields to UTC epoch",
        type=bool,
        default=False,
        required=False
    )
    asyncio_run(main(parser.parse_args()))
