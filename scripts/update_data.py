import requests
import re
import os
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text
import polars as pl


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

    days_of_history = 14
    two_weeks_prior = last_update_datetime - timedelta(days=days_of_history)
    rounded_two_weeks = datetime(year=two_weeks_prior.year, month=two_weeks_prior.month, day=two_weeks_prior.day)

    print(f"Getting data from {rounded_two_weeks.isoformat()} and {last_update_datetime.isoformat()}...")
    params = {
        "$where": f"transit_timestamp between'{rounded_two_weeks.isoformat()}' and '{last_update_datetime.isoformat()}'",
        "$limit": days_of_history * 100_000
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

    metrocard_columns = [col for col in ridership_wide.columns if "Metrocard" in col]
    omny_columns = [col for col in ridership_wide.columns if "OMNY" in col]
    ridership_columns = [col for col in ridership_wide.columns if "Metrocard" in col or "OMNY" in col]

    ridership = ridership_wide.with_columns(
        total_metrocard_ridership=pl.sum_horizontal(col for col in metrocard_columns),
        total_omny_ridership=pl.sum_horizontal(col for col in omny_columns),
        total_ridership=pl.sum_horizontal(col for col in ridership_columns),
    )

    rename_mapping = {col: clean_column_name(col) for col in ridership.columns}
    ridership = ridership.rename(rename_mapping)
    
    #Since we got rid of the shuttle and TRAM lines, we filter them out here too.
    ridership = ridership.filter(~pl.col("station_complex_id").str.contains("TRAM")).filter(~pl.col("station_complex_id").str.contains("141"))
    return ridership

def upsert_data(conn, df):
    """
    Connects to the database and updates the data. We perform an UPSERT for any revisions
    that are picked up.
    """
    print("Updating ridership table...")
    columns = [col for col in df.columns if col != 'entry_id']
    sql_columns = ', '.join(columns)
    sql_placeholders = ', '.join([f":{col}" for col in columns])

    on_conflict_columns = ["transit_timestamp", "station_complex_id"]
    conflict_target = ', '.join(on_conflict_columns)
    update_expressions = ', '.join([f"{col}=EXCLUDED.{col}" for col in columns if col not in on_conflict_columns])
    
    upsert_sql = f"""
    INSERT INTO ridership ({sql_columns})
    VALUES ({sql_placeholders})
    ON CONFLICT ({conflict_target})
    DO UPDATE SET {update_expressions};
    """

    statement = text(upsert_sql)
    dict_list = df.to_pandas().to_dict(orient="records")

    for row_dict in dict_list:
        conn.execute(statement, row_dict)


def main():
    with requests.session() as ses:
        df = get_data(ses)
    ridership = format_df(df)
        
    db_name = os.environ["SUBWAYDB_N"]
    db_user = os.environ["SUBWAYDB_U"]
    db_pass = os.environ["SUBWAYDB_P"]
    engine = create_engine(f'postgresql://{db_user}:{db_pass}@localhost/{db_name}')

    with engine.connect() as conn:
        upsert_data(conn, ridership)
        conn.commit()
        
    print("Done updating ridership data!")
        
if __name__ == "__main__":
    main()