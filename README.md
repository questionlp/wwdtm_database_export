# Wait Wait Don't Tell Me! Stats Page Database Export

## Overview

Python application that exports each of the tables in an instance of the [Wait Wait Don't Tell Me Stats Page Database](https://github.com/questionlp/wwdtm_database) as individual JSON files. Each of the JSON files is named after the database table.

## Requirements

- Python 3.10 or newer (Any earlier version of Python is not supported)
- Wait Wait Stats Database v4.6 or higher (Earlier versions are no longer supported) running on MySQL 8.0 or newer

## Setup

Before the application can be used, a copy of the `config.json.dist` file needs to be created and named `config.json`. The `config.json` file contains the required database connection information to connect and log into a MySQL server.

It is highly recommended to use Python virtual environments to install the required packages and isolate the application from other Python programs. All of the package requirements are stored in `requirements.txt`.

In case you are working on developing or troubleshooting the program, the development environment requirements are stored in `requirements-dev.txt`.

## Usage

```text
usage: database_export.py [-h] [--date] [output]

Export Wait Wait Stats database tables in JSON format.

positional arguments:
  output      Output directory

optional arguments:
  -h, --help  show this help message and exit
  --date      Create a subdirectory under the output directory with date/timestamp
```

## Code of Conduct

This projects follows version 2.1 of the [Contributor Covenant's](https://www.contributor-covenant.org/). A copy of the [Code of Conduct](https://github.com/questionlp/wwdtm_database_export/blob/main/CODE_OF_CONDUCT.md) document is included in this repository.

## License

This library is licensed under the terms of the [Apache License 2.0](http://www.apache.org/licenses/LICENSE-2.0).
