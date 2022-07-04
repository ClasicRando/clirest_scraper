import asyncio
from metadata import RestMetadata
from scraping import fetch_query, handle_csv_value
from argparse import ArgumentParser, Namespace
from tqdm import tqdm
from functools import reduce
from operator import iconcat
from pathlib import Path
from time import time as now
from sys import platform as sys_platform
from os import remove as os_remove, mkdir
from asyncio import Queue, create_task, set_event_loop_policy, run as asyncio_run


async def fetch_worker(t: tqdm, queue: Queue, done_queue: Queue, ssl: bool):
    try:
        while True:
            query, params, fields, geo_type, max_tries = await queue.get()
            result = await fetch_query(t, query, params, fields, geo_type, max_tries, ssl)
            await done_queue.put(result)
            queue.task_done()
    except Exception as ex:
        t.write(f"Encountered an error in the fetch worker\n{ex}")


async def csv_writer_worker(t: tqdm, queue: Queue, metadata: RestMetadata):
    output_file = None
    try:
        output_files_directory = Path("output_files")
        output_file_path = Path(f"{metadata.name}.csv")
        output_file = open(
            output_files_directory.joinpath(output_file_path),
            encoding="utf8",
            mode="w",
            newline="",
        )
        header_cols = list(reduce(
            iconcat,
            [
                [field.name, f"{field.name}_DESC"] if field.is_code else [field.name]
                for field in metadata.fields
            ],
        ))
        header_line = ",".join((handle_csv_value(column) for column in header_cols))
        output_file.write(header_line)
        results_handled = 0
        while True:
            result = await queue.get()
            if isinstance(result, BaseException):
                raise result
            with open(result.name, newline="", encoding="utf8") as csv_file:
                for line in csv_file:
                    output_file.write(line)
            os_remove(result.name)
            results_handled += 1
            queue.task_done()
            t.update(1)
    except Exception as ex:
        t.write(f"Encountered an error in the writer worker\n{ex}")
    finally:
        if output_file:
            output_file.close()


async def main(args: Namespace):
    metadata = await RestMetadata.from_url(args.url, args.ssl, args.sr)
    proceed = "Y" if args.yes is None else args.yes
    metadata.print_formatted()
    if proceed == "N":
        proceed = input("Proceed with scrape? (y/n)").upper()
    if proceed == "Y":
        if not Path("output_files").exists():
            mkdir("output_files")
        if not Path("temp_files").exists():
            mkdir("temp_files")
        total_results = len(metadata.queries)
        t = tqdm(total=total_results)
        fetch_worker_queue = Queue(args.workers)
        writer_queue = Queue(args.workers)
        start = now()
        workers = [
            create_task(fetch_worker(t, fetch_worker_queue, writer_queue, args.ssl))
            for _ in range(args.workers)
        ]
        writer_task = create_task(csv_writer_worker(t, writer_queue, metadata))

        for (query, params) in metadata.queries:
            await fetch_worker_queue.put(
                (query, params, metadata.fields, metadata.geo_type, args.tries)
            )

        await fetch_worker_queue.join()

        for worker in workers:
            worker.cancel()

        await writer_queue.join()
        writer_task.cancel()

        t.write(f"Scraping done. Took {round(now() - start, 2)} seconds")
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
        help="number of workers spawned to perform the HTTP requests (Default: 10)",
        type=int,
        default=10,
        required=False
    )
    parser.add_argument(
        "--sr",
        help="spatial reference code (epsg) to project the geometry",
        type=int,
        required=False
    )
    asyncio_run(main(parser.parse_args()))
