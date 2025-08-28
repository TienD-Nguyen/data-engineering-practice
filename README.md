## Data Engineering Practice Problems ##

This repository contains my attempts to resolve the amazing data engineer exercises posted by Daniel Beach [danielbeach/data-engineering-practice](https://github.com/danielbeach/data-engineering-practice/).

To execute the solutions contained within this directory, each respective folder will either:

- Utilise Docker Compose.
- Run the "main.py" module from a Python virtual environment.

The objective of solving these exercises is to guide and help me developing technical skills, that are required on a daily basis of a Data Engineer. It is also a pursuit of mine to break into the field. There are multiple high level topics was covered through the exercises and guided by Daniel, these include:

- Python data processing.
- csv, flat-file, parquet, json, etc.
- SQL database table design.
- Python + Postgres, data ingestion and retrieval.
- PySpark
- Data cleansing / dirty data.

### Exercises Details

### Beginner Exercises

#### Exercise 1 - Downloading files.
The [first exercise](https://github.com/danielbeach/data-engineering-practice/tree/main/Exercises/Exercise-1) tests the ability to download a number of files
from an `HTTP` source and unzip them, storing them locally with `Python`.

#### Exercise 2 - Web Scraping + Downloading + Pandas
The [second exercise](https://github.com/danielbeach/data-engineering-practice/tree/main/Exercises/Exercise-2) tests the ability perform web scraping, build uris, download files, and use Pandas to do some simple cumulative actions.

#### Exercise 3 - Boto3 AWS + s3 + Python.
The [third exercise](https://github.com/danielbeach/data-engineering-practice/tree/main/Exercises/Exercise-3) tests a few skills with a popular `aws` package called `boto3` to try to perform a multi-step actions to download some open source `s3` data files.

#### Exercise 4 - Convert JSON to CSV + Ragged Directories.
The [fourth exercise](https://github.com/danielbeach/data-engineering-practice/tree/main/Exercises/Exercise-4) focuses on file types `json` and `csv`, and working with them in `Python`, where a ragged directory structure will be traversed to find `json` files and convert them to `csv`.

#### Exercise 5 - Data Modeling for Postgres + Python.
The [fifth exercise](https://github.com/danielbeach/data-engineering-practice/tree/main/Exercises/Exercise-5) is slightly different than the rest. In this problem you will a number of `csv` files were given. A data model / schema will be created to hold these data sets, including indexes, then insert all the tables into `Postgres` by connecting to the database with `Python`.


### Intermediate Exercises

#### Exercise 6 - Ingestion and Aggregation with PySpark.
The [sixth exercise](https://github.com/danielbeach/data-engineering-practice/tree/main/Exercises/Exercise-6) moves onto more popular tools, and the task is to load some files using `PySpark` and then be asked to do some basic aggregation.

#### Exercise 7 - Using Various PySpark Functions
The [seventh exercise](https://github.com/danielbeach/data-engineering-practice/tree/main/Exercises/Exercise-7) takes a page out of the previous exercise, and focuses on using a few of the more common build in PySpark functions `pyspark.sql.functions` and applying their usage to real-life problems. Many times, in order to solve simple problems we have to find and use multiple functions available from libraries. This will test the ability to do that.

#### Exercise 8 - Using DuckDB for Analytics and Transforms.
The [eighth exercise](https://github.com/danielbeach/data-engineering-practice/tree/main/Exercises/Exercise-8) introduces new tools, which are imperative to growing as a Data Engineer. DuckDB is one of those new tools. In this exercise, the task to to complete a number of analytical and transformation tasks using DuckDB. This will require an understanding of the functions and documenation of DuckDB.

#### Exercise 9 - Using Polars lazy computation.
The [ninth exercise](https://github.com/danielbeach/data-engineering-practice/tree/main/Exercises/Exercise-9) introduces Polars, which is a new Rust based tool with a wonderful Python package that has taken Data Engineering by storm. It's better than Pandas because it has both SQL Context and supports Lazy evalutation for larger than memory data sets! Show your Lazy skills!


### Advanced Exercises

#### Exercise 10 - Data Quality with Great Expectations
The [tenth exercise](https://github.com/danielbeach/data-engineering-practice/tree/main/Exercises/Exercise-10) guides through Data Quality, specially with the use of Great Expectations. A dataset was given in CSV format, as well as an existing pipeline. However, there is a data quality issue and the task is to implement Data Quality checks to catch some of these issues.
