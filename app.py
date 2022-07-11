from flask import Flask, request
from exceptions import RequestError
from utils import read, get_query

app = Flask(__name__)


@app.route("/perform_query/")
def perform_query():
    data = request.args

    try:
        file_name = read(data.get('filename'))
        query = get_query(file_name, data.get('cmd1'), data.get('value1'))
        if data.get('cmd2'):
            query = get_query(query, data.get('cmd2'), data.get('value2'))

        return app.response_class('\n'.join(query), content_type="text/plain")

    except (FileNotFoundError, RequestError) as e:
        return f'{e}', 400


if __name__ == '__main__':
    app.run(debug=True)
