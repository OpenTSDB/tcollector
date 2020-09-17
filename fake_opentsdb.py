"""
A fake OpenTSDB server using Flask.

You can modify the response code using the FAKE_OPENTSDB_RESPONSE environment
variable.
"""

import os

from flask import Flask
app = Flask(__name__)

RESPONSE_CODE = int(os.environ.get("FAKE_OPENTSDB_RESPONSE", 204))


@app.route('/api/put', methods=["PUT", "POST"])
def put_message():
    # This could be extended to write the messages to disk, for use by
    # additional tests...
    return "", RESPONSE_CODE
