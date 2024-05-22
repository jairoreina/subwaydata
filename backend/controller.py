import os
import json
import pandas as pd
from sqlalchemy import create_engine, text
from openai import OpenAI
from thefuzz import fuzz, process

client = OpenAI()
client.api_key = os.environ["OPENAI_API_KEY"]
db_name = os.environ["SUBWAYDB_N"]
db_user = os.environ["SUBWAYDB_U"]
db_pass = os.environ["SUBWAYDB_P"]
engine = create_engine(f'postgresql://{db_user}:{db_pass}@localhost/{db_name}')

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
    response_json = make_initial_query(client, query)
    if response_json["stations"]:
        response_json = correct_station_names(engine, response_json)
    return response_json

def get_html(response_json):
    with engine.connect() as conn:
        results = conn.execute(text(response_json["sql"]))
    query_result = results.mappings().all()
    return pd.DataFrame(query_result).to_html()