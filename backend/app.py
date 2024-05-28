from flask import Flask, request, jsonify, render_template, make_response
from backend.controller import make_full_query, get_html

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
        query = request.json["query"]
        response_json = make_full_query(query)
        table_html = get_html(response_json)
        resp_data = {
            "table_html": table_html,
            "query": response_json["sql"],
        }
        return jsonify(resp_data)
    except Exception as e:
        # Handle other exceptions
        error_message = {"Error": str(e)}
        return make_response(jsonify(error_message), 500)

if __name__ == '__main__':
    app.run(debug=True)
