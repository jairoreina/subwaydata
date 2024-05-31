from flask import Flask, request, jsonify, render_template, make_response
from backend.controller import make_full_query, get_html, log_query, log_error, get_engine
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
        engine = get_engine()
        query = request.json["query"]
        query_timestamp = datetime.datetime.now(datetime.UTC).isoformat()
        initial_response_json, corrected_response_json = make_full_query(query)
        table_html, query_result_as_dict = get_html(corrected_response_json if corrected_response_json else initial_response_json)

        log_query(engine, query, initial_response_json, corrected_response_json, query_result_as_dict, query_timestamp)
        
        if not query_result_as_dict:
            raise ValueError("The query did not return any results.")
        
        resp_data = {
            "table_html": table_html,
            "query": corrected_response_json["sql"] if corrected_response_json else initial_response_json["sql"],
        }
        return jsonify(resp_data)
    except Exception as e:
        error_message = str(e)
        print({"Error": error_message})
        log_error(engine, query, error_message, query_timestamp)
        return make_response(jsonify(error_message), 500)

if __name__ == '__main__':
    app.run(debug=True)
