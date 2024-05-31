import os
import json
import datetime
import pandas as pd
from sqlalchemy import create_engine, text, insert
from openai import OpenAI
from thefuzz import fuzz, process

client = OpenAI()
client.api_key = os.environ["OPENAI_API_KEY"]
db_name = os.environ["SUBWAYDB_N"]
db_user = os.environ["SUBWAYDB_U"]
db_pass = os.environ["SUBWAYDB_P"]
engine = create_engine(f'postgresql://{db_user}:{db_pass}@localhost/{db_name}')

def get_engine():
    return create_engine(f'postgresql://{db_user}:{db_pass}@localhost/{db_name}')


def log_query(engine, user_query, initial_response_json, corrected_response_json, query_result, query_timestamp):
    log_entry = {
        "user_query": user_query,
        "initial_response_json": json.dumps(initial_response_json),
        "corrected_response_json": json.dumps(corrected_response_json) if corrected_response_json else None,
        "query_result": json.dumps(query_result, default=str),
        "created_at": query_timestamp
    }
    
    insert_stmt = text("""
        INSERT INTO query_logs (user_query, initial_response_json, corrected_response_json, query_result, created_at)
        VALUES (:user_query, :initial_response_json, :corrected_response_json, :query_result, :created_at)
    """)

    with engine.connect() as conn:
        conn.execute(insert_stmt, log_entry)
        conn.commit()
        
def log_error(engine, user_query, error_message, query_timestamp):
    error_log_entry = {
        "user_query": user_query,
        "error_message": error_message,
        "created_at": query_timestamp
    }
    insert_stmt = text("""
        INSERT INTO error_logs (user_query, error_message, created_at)
        VALUES (:user_query, :error_message, :created_at)
    """)
    with engine.connect() as conn:
        conn.execute(insert_stmt, error_log_entry)
        conn.commit()

def make_initial_query(client, query):
    with open("initial_prompt.txt", "r") as file:
        prompt = file.read()
        
    response = client.chat.completions.create(
        model="gpt-4o",
        response_format={"type": "json_object"},
        messages=[
            {"role": "system", "content": prompt},
            {"role": "user", "content": query}
        ],
        temperature=1,
        max_tokens=600,
        top_p=1,
        frequency_penalty=0,
        presence_penalty=0
    )
    response_json = json.loads(response.choices[0].message.content)
    return response_json

def correct_station_names(engine, response_json):
    for station in response_json["stations"]:
        if station[1]:
            query = f"SELECT sr.*, s.station_complex FROM stations s JOIN station_routes sr ON s.station_complex_id = sr.station_complex_id WHERE route_name ILIKE '%%{station[1]}%%'"
        else:
            query = f"SELECT station_complex FROM stations"
            
        station_list = pd.read_sql(query, con=engine.connect())["station_complex"].tolist()
        top_matches = process.extract(station[0], station_list, limit=3, scorer=fuzz.partial_token_sort_ratio)
        #print(f"The best matches between '{station[0]}' and all stations is: {top_matches[0]}")
        response_json["sql"] = response_json["sql"].replace(station[0], top_matches[0][0])
    return response_json

def make_full_query(query):
    initial_response_json = make_initial_query(client, query)
    corrected_response_json = None
    if initial_response_json["stations"]:
        corrected_response_json = correct_station_names(engine, initial_response_json)
    return initial_response_json, corrected_response_json


def get_html(response_json):
    with engine.connect() as conn:
        results = conn.execute(text(response_json["sql"]))
    query_result = results.mappings().all()
    query_df = pd.DataFrame(query_result)
    html_table = query_df.to_html(index=False)
    query_df_as_dict = query_df.to_dict()
    return html_table, query_df_as_dict