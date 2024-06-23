import sys
from main import getLM
import json
from flask import Flask
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address

app = Flask(__name__)
limiter = Limiter(
    get_remote_address,
    app=app,
    default_limits=["2000 per day", "300 per hour"],
    storage_uri="memory://",
)


@app.route('/update')
@limiter.limit("2 per minute")
def update():
    getLM.run()
    return "The update is complete"


@app.errorhandler(500)
def internal_error(error):
    return "Server error", 500

@app.errorhandler(429)
def tooManyReq(error):
    return "Limit exceed - 5 request per minute. Wait a one minute", 429

@app.after_request
def complete(response):
    ids_var = getLM.ids()
    res_data = response.data
    res_data = str(res_data, "UTF-8")
    response.data = f"{res_data}\n for {ids_var}"
    print(response)
    return response
