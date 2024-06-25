import os
from time import time
import dotenv
import argparse
import pyarrow.parquet as pq
from sqlalchemy import create_engine


def main(args):
    dotenv.load_dotenv()
    dialect = os.getenv("DATABASE_DIALECT")
    driver = os.getenv("DATABASE_DRIVER")
    username = os.getenv("DATABASE_USERNAME")
    password = os.getenv("DATABASE_PASSWORD")
    host = os.getenv("DATABASE_HOST")
    port = os.getenv("DATABASE_PORT")
    database = os.getenv("DATABASE_DATABASE")
    table = os.getenv("DATABASE_TABLE")
    file = args.filename

    engine = create_engine(
        f"{dialect}+{driver}://{username}:{password}@{host}:{port}/{database}"
    )

    parquet_file = pq.ParquetFile(file)
    print(f"Loading {file} into database.")
    file_start = time()
    for i, batch in enumerate(parquet_file.iter_batches()):
        print(f"Loading batch {i} into database.")
        batch_start = time()

        df = batch.to_pandas()
        df.to_sql(table, engine, if_exists="append")

        batch_end = time()
        print(
            f"Finished loading batch {i} into database. Elasped seconds: {batch_end - batch_start}."
        )
    file_end = time()
    print(
        f"Finished loading {file} into database. Elasped seconds: {file_end - file_start}. Number of batches: {i}."
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Load data from Parquet file to a SQLAlcehmy-supported database."
    )
    parser.add_argument("--filename", help="Parquet file to load into database.")
    args = parser.parse_args()
    main(args)
