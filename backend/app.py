from flask import Flask, request, jsonify, render_template, make_response
from backend.controller import make_full_query, get_html, log_query, log_error, get_engine, is_query_safe
import datetime

app = Flask(__name__, template_folder='../templates', static_folder='../static')

@app.route("/")
def index():
    return render_template("index.html")

@app.route("/about", methods=["GET"])
def get_about():
    return render_template('about.html')

@app.route("/query", methods=["POST"])
def get_query_response():
    try:
        read_only_engine = get_engine(read_only=True)
        read_write_engine = get_engine(read_only=False)
        
        query = request.json["query"]
        print(query)
        is_sql_only = request.json.get("is_sql_only", False)
        query_timestamp = datetime.datetime.now(datetime.UTC).isoformat()
        
        if is_sql_only:
            generated_sql = query
            initial_response_json = "SQL only mode"
            corrected_response_json = "SQL only mode"
        else:
            initial_response_json, corrected_response_json = make_full_query(read_only_engine, query)
            generated_sql = corrected_response_json["sql"] if corrected_response_json else initial_response_json["sql"]

        if not is_query_safe(generated_sql):
            raise ValueError("Generated query is not safe to execute.")
        
        table_html, query_result_as_dict = get_html(read_only_engine, generated_sql)

        log_query(read_write_engine, query, initial_response_json, corrected_response_json, query_result_as_dict, query_timestamp)
        
        if not query_result_as_dict:
            raise ValueError("The query did not return any results.")
        
        resp_data = {
            "table_html": table_html,
            "query": generated_sql,
        }
        return jsonify(resp_data)
    except ValueError as ve:
        error_message = str(ve)
        log_error(read_write_engine, query, error_message, query_timestamp)
        return make_response(jsonify({"error": error_message}), 400)
    except Exception as e:
        error_message = str(e)
        log_error(read_write_engine, query, error_message, query_timestamp)
        return make_response(jsonify({"error": "An unexpected error occurred"}), 500)


if __name__ == '__main__':
    app.run(debug=True)
