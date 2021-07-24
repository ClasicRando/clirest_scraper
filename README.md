<h1>CliRest Scaper</h1>
This project is a CLI version of another application I made to scrape ArcGIS Rest services. The main differences are in
how concurrent processes are handled, what libraries are used and the method of running the application (desktop GUI vs
command line).

<h3>Concurrency</h3>
In this application asyncio handles the currency with a little help from aiohttp (the only third party library used).

<h3>Requirements</h3>
The python version I used to make this is 3.8.2, however it should run in most python3 environments. The only third
party package I used is aiohttp which can be installed using pip:

`pip install aiohttp`

After that the project should run for your python environment.

<h3>App Run</h3>
PyQt5 is a nice framework, but who doesn't love a CLI application. I wanted to find a better way to make the
application easier to run and require less third party libraries.

From the command line and using the python command you can call the script which accepts 2 parameters:

- `--url base url of the rest service (string)`
- `--yes (or -y) no value needed and auto accepts scrape start after metadata is obtained`

<h3>Notes</h3>

- during the scraping the application creates temp files then consolidates them into 1 output file in the application
  directory. While running the application DO NOT work with or delete temp files
- Not all scraping methods are made equal. If the service makes it difficult to collect features easily, the process
  might take a while and also take a lot of resources