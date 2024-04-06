import requests
import re
from datetime import datetime, timedelta
import polars as pl
import os
import shutil
import sqlite3

def clean_column_name(name):
    """Cleans up the column names and returns the cleaned name as a string."""
    return re.sub(r"[ \-&]", "_", name).replace("___", "_").replace("__","_").lower()

def get_data(ses):
    """
    Checks the final available date from the API and gets the previous two weeks of data.
    We set the limit to 1_500_000 because everyday has approximately 70_000 rows so this
    will accomodate for the 14 days of data. Since we only update the ridership table daily,
    we can filter the columns.
    
    :returns: Polars dataframe of the 2 latest weeks of data of the ridership table.
    """
    url = "https://data.ny.gov/resource/wujg-7c2s.csv"

    params = {
        "$limit":1,
        "$order":"transit_timestamp DESC"
    }
    response = ses.get(url, params=params)

    last_update_string = pl.read_csv(response.content)["transit_timestamp"][0]
    last_update_datetime = datetime.fromisoformat(last_update_string)

    two_weeks_prior = last_update_datetime - timedelta(days=14)
    rounded_two_weeks = datetime(year=two_weeks_prior.year, month=two_weeks_prior.month, day=two_weeks_prior.day)

    print(f"Getting data from {rounded_two_weeks.isoformat()} and {last_update_datetime.isoformat()}...")
    params = {
        "$where": f"transit_timestamp between'{rounded_two_weeks.isoformat()}' and '{last_update_datetime.isoformat()}'",
        "$limit": 1_500_000
    }
    response = ses.get(url, params=params)
    
    columns_to_keep = ["transit_timestamp", "station_complex_id", "fare_class_category", "ridership"]
    df = pl.read_csv(response.content, infer_schema_length=0, columns=columns_to_keep)
    return df

def format_df(df):
    """
    Transforms the ridership table to fit the database schema. Adds a total_ridership column
    so we can easily query total ridership instead of summing in the SQL query.
    
    :returns: Formatted ridership polars dataframe.
    """
    ridership_wide = df.with_columns(
        [pl.col("transit_timestamp").map_elements(lambda x: datetime.fromisoformat(x), return_dtype=pl.Datetime("us")),
        pl.col("ridership").cast(pl.Float64).floor().cast(pl.Int16)]
    ).pivot(
        index=["transit_timestamp", "station_complex_id"],
        columns="fare_class_category",
        values="ridership",
        aggregate_function="sum",
        sort_columns=True
    ).sort(
        ["transit_timestamp", "station_complex_id"], descending=[False, False]
    ).fill_null(0)

    ridership_columns = [col for col in ridership_wide.columns if "Metrocard" in col or "OMNY" in col]
    ridership = ridership_wide.with_columns(
        total_ridership=pl.sum_horizontal(col for col in ridership_columns)
    )

    rename_mapping = {col: clean_column_name(col) for col in ridership.columns}
    ridership = ridership.rename(rename_mapping)
    return ridership

def upsert_data(conn, df):
    """
    Connects to the database and updates the data. We perform a REPLACE for any revisions
    that are picked up. Since the transit_timestamp column is of type datetime, we transform
    to ISO8061 string so SQLite3 can properly update the database.
    """
    print("Updating ridership table...")
    columns = [col for col in df.columns if col != 'entry_id']
    sql_columns = ', '.join(columns)
    sql_placeholders = ', '.join(['?'] * len(columns))
    
    upsert_sql = f"""
    REPLACE INTO ridership ({sql_columns})
    VALUES ({sql_placeholders});
    """

    for row in df.to_pandas().itertuples(index=False, name=None):
        row_list = list(row)
        for i, elem in enumerate(row_list):
            if isinstance(elem, datetime):
                row_list[i] = elem.isoformat(' ')
        conn.execute(upsert_sql, tuple(row_list))
    conn.commit()

def main():
    with requests.session() as ses:
        df = get_data(ses)
    ridership = format_df(df)
    
    db_path = "data/subway.db"
    backup_path = "backup/subway_backup.db"
    
    if not os.path.exists("backup"):
        os.makedirs("backup")
    
    if os.path.exists(db_path):
        print("Backing up the current database...")
        shutil.copyfile(db_path, backup_path)
        
    with sqlite3.connect("data/subway.db") as conn:
        upsert_data(conn, ridership)
    print("Done updating ridership data!")
        
if __name__ == "__main__":
    main()