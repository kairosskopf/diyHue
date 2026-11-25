import json

from flask import Blueprint, Response, request
from HueObjects import stream_events_after, stream_start
import logManager

logging = logManager.logger.get_logger(__name__)
stream = Blueprint("stream", __name__)


def frame(wire_id, message):
    batch = message if isinstance(message, list) else [message]
    return f"id: {wire_id}\ndata: {json.dumps(batch, separators=(',', ':'))}\n\n"


@stream.route("/eventstream/clip/v2")
def streamV2Events():
    from flaskUI.v2restapi import authorizeV2
    auth = authorizeV2(request.headers)
    if "user" not in auth:
        return "", 403

    cursor, recovery, mode = stream_start(request.headers.get("Last-Event-ID"), auth["user"])
    logging.info("Hue event stream opened (%s, %d scene deletions to replay)", mode, len(recovery))

    def generate(cursor):
        yield ": hi\n\n"
        for _, wire_id, message in recovery:
            yield frame(wire_id, message)
        while True:
            events = stream_events_after(cursor)
            if not events:
                yield ": keepalive\n\n"
            for cursor, wire_id, message in events:
                yield frame(wire_id, message)

    return Response(generate(cursor), mimetype="text/event-stream",
                    headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})
