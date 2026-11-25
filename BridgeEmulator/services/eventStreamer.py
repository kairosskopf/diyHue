import json

from flask import Blueprint, Response, request, stream_with_context
from HueObjects import event_sequence, events_after

stream = Blueprint('stream', __name__)


@stream.route('/eventstream/clip/v2')
def streamV2Events():
    from flaskUI.v2restapi import authorizeV2
    if "user" not in authorizeV2(request.headers):
        return "", 403

    current = event_sequence()
    cursor = request.headers.get("Last-Event-ID", type=int)
    if cursor is None or not 0 <= cursor <= current:
        cursor = current

    def generate(cursor):
        yield ": hi\n\n"
        while True:
            events = events_after(cursor)
            if not events:
                yield ": keepalive\n\n"
            for cursor, message in events:
                batch = message if isinstance(message, list) else [message]
                yield f"id: {cursor}\ndata: {json.dumps(batch, separators=(',', ':'))}\n\n"

    return Response(stream_with_context(generate(cursor)), mimetype='text/event-stream')
