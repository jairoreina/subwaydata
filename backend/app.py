from flask import Flask, request, jsonify, render_template
from backend.controller import make_full_query, get_html

app = Flask(__name__, template_folder='../templates', static_folder='../static')

@app.route("/")
def index():
    return render_template("index.html")

@app.route("/query", methods=["POST"])
def get_query_response():
    query = request.json["query"]
    response_json = make_full_query(query)
    table_html = get_html(response_json)
    resp_data = {
        "table_html": table_html,
        "query": response_json["sql"],
    }
    return jsonify(resp_data)

if __name__ == '__main__':
    app.run(debug=True)
