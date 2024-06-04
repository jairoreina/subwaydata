import polars as pl
from sqlalchemy import create_engine, text
import re
import os


def add_dec_prec(row):
    """
    Helper function to add the decimal places in the longitude and latitude columns
    so we can find the rows with the longest/most accurate decimal precision as sometimes
    the entries have different decimal precision for the same station. 

    :returns: Return the length of decimal precision in longitude and latitude as a string.
    """
    latitude = row["latitude"]
    longitude = row["longitude"]
    # Assuming latitude and longitude are strings; extract the precision part
    lat_precision = len(latitude.split(".")[-1]) if "." in latitude else 0
    long_precision = len(longitude.split(".")[-1]) if "." in longitude else 0
    # Return the total precision as the sum of both
    return lat_precision + long_precision

def clean_column_name(name):
    """Cleans up the column names and returns the cleaned name as a string."""
    return re.sub(r"[ \-&]", "_", name).replace("___", "_").replace("__","_").lower()

def get_ridership():
    """
    Reads the required columns from the history parquet file (data until 2024-03-16) and
    formats and pivots it so that it can fit into the ridership table in the database.
    Since we have multiple types of fare classes, we pivot the df to put those as columns
    to have allow for unique rows.
    
    :returns: Returns a polars dataframe of the ridership table.
    """
    columns_to_keep = ["transit_timestamp", "station_complex_id", "fare_class_category", "ridership"]
    original_ridership = pl.read_parquet("data/hist.parquet", columns=columns_to_keep, low_memory=True)

    ridership_wide = original_ridership.with_columns(
        [pl.col("transit_timestamp").str.strptime(pl.Datetime, format="%m/%d/%Y %I:%M:%S %p"),
         pl.col("ridership").cast(pl.Int16)]
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

def get_stations(subset_df):
    """
    Takes a subset of the history data and formats and filters so we keep a datafame of
    station information for every station in the dataset. Only need a subset because they 
    will all appear at least once if we take 10 million rows for example.

    :param subset_df: Polars dataframe of the history data subset.
    :returns: Returns a polars dataframe of the stations table.
    """
    stations = subset_df.select(["station_complex_id", "station_complex", "borough", "latitude", "longitude"]).unique()

    df_with_precision = stations.with_columns(
        [(pl.struct(["latitude", "longitude"]).map_batches(
            lambda batch: batch.map_elements(add_dec_prec, return_dtype=pl.Int64)
        )).alias("total_precision"),
        pl.col("latitude").cast(pl.Float64),
        pl.col("longitude").cast(pl.Float64)]
    )
    df_with_precision = df_with_precision.sort(['station_complex_id', 'total_precision'], descending=[False, True]).unique(subset=["station_complex_id"])

    stations = df_with_precision.select(["station_complex_id", "station_complex", "borough", "latitude", "longitude"]).unique().sort("station_complex_id")
    stations = stations.with_columns(
        pl.col("station_complex")
        .str.replace_all(r"\,S", "")
        .str.replace_all(r"\(110 St\)", "- 110 St")
        .str.replace_all(r"\/Botanic Garden \(S\)", "")
        .str.strip_chars()
        ).filter(~pl.col("station_complex_id").str.contains("TRAM")
        ).filter(~pl.col("station_complex").str.contains(r"\(S\)")
        ).sort("station_complex_id")
    return stations

def get_routes(stations):
    """
    Finds all the train lines in the stations table.
    
    :returns: Returns a polars dataframe of the routes table.
    """
    station_list = stations["station_complex"].to_list()
    regex_pattern = r"\(([^)]+)\)"

    unique_train_lines = set()

    for station in station_list:
        matches = re.findall(regex_pattern, station)
        if matches:
            for line in matches[0].split(','):
                unique_train_lines.add(line.strip())

    routes = pl.DataFrame({
        "route_name": sorted(list(unique_train_lines))
    })
    return routes

def get_station_routes(stations):
    """
    Gets the mapping of the station_complex_id's to the train lines that service that
    station.
    
    :returns: Returns a polars dataframe of the station_routes table.
    """
    station_routes = stations.with_columns(
        pl.col("station_complex")
        .str.extract_all(r"\((.*?)\)")
        .map_elements(lambda groups: ','.join(groups), return_dtype=str)
        .str.replace_all(r"\(", "")
        .str.replace_all(r"\)", "")
        .str.split(",")
        .alias("route_list"))

    # Step 3: Explode the list into separate rows
    stations_exploded = station_routes.explode("route_list")

    # Step 4: Select and rename columns to fit the SQL schema, remove duplicates
    station_routes = stations_exploded.select([
        pl.col("station_complex_id"),
        pl.col("route_list").alias("route_name")
    ]).unique()

    station_routes = station_routes.sort("station_complex_id")
    return station_routes

def main():
    print("Creating the database...")
    db_name = os.environ["SUBWAYDB_N"]
    db_user = os.environ["SUBWAYDB_U"]
    db_pass = os.environ["SUBWAYDB_P"]
    engine = create_engine(f'postgresql://{db_user}:{db_pass}@localhost/{db_name}')

    with engine.connect() as conn:
        with open("sql/tables.sql", "r") as file:
            query = text(file.read())
            conn.execute(query)
        
        print("Creating the subset dataframes...")
        subset_df = pl.read_parquet("data/hist.parquet", n_rows=30_000_000, low_memory=True)
        
        stations = get_stations(subset_df)
        stations_clean = stations.with_columns(pl.col("station_complex").str.replace_all(r"\([^)]*\)", "").str.strip_chars())
        
        routes = get_routes(stations)
        station_routes = get_station_routes(stations)
        
        print("Creating the main ridership dataframe...")
        ridership = get_ridership()

        print("Populating the tables with data...")
        stations_clean.to_pandas().to_sql('stations', conn, if_exists='append', index=False)
        routes.to_pandas().to_sql('routes', conn, if_exists='append', index=False)
        station_routes.to_pandas().to_sql('station_routes', conn, if_exists='append', index=False)
        ridership.to_pandas().to_sql('ridership', conn, if_exists='append', index=False)
    
        conn.commit()
        
    print("Done creating the subway database!")
    
    
if __name__ == "__main__":
    main()