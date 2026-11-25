import json
from datetime import datetime, timezone
from uuid import uuid4

from flask import Blueprint, Response, request
from HueObjects import stream_events_after, stream_start
import logManager

logging = logManager.logger.get_logger(__name__)
stream = Blueprint("stream", __name__)


def frame(wire_id, message):
    batch = message if isinstance(message, list) else [message]
    header = f"id: {wire_id}\n" if wire_id is not None else ""
    return f"{header}data: {json.dumps(batch, separators=(',', ':'))}\n\n"


def current_light_levels(bridge):
    resources = []
    for light in list(bridge.get("lights", {}).values()):
        try:
            resources.append(light.getV2Api())
        except (KeyError, TypeError, ValueError, IndexError):
            logging.exception("Could not read light %s for Hue reconnect", light.id_v1)
    for group in list(bridge.get("groups", {}).values()):
        if group.type in ("Room", "Zone") or group.id_v1 == "0":
            resources.append(group.getV2GroupedLight())
    keys = ("id", "id_v1", "type", "owner", "service_id", "on", "dimming")
    return {"id": str(uuid4()), "type": "update",
            "creationtime": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "data": [{key: resource[key] for key in keys if key in resource} for resource in resources]}


@stream.route("/eventstream/clip/v2")
def streamV2Events():
    from flaskUI.v2restapi import authorizeV2, bridgeConfig
    auth = authorizeV2(request.headers)
    if "user" not in auth:
        return "", 403

    cursor, recovery, mode, wire_id = stream_start(request.headers.get("Last-Event-ID"), auth["user"])
    logging.info("Hue event stream opened (%s, %d scene deletions to replay)", mode, len(recovery))

    # Send the checkpoint only with the complete recovery batch. Events
    # published while we read current levels follow it on the live stream.
    initial = [message for _, _, message in recovery]
    if mode != "resume":
        snapshot = current_light_levels(bridgeConfig)
        if snapshot["data"]:
            initial.append(snapshot)

    def generate(cursor):
        yield ": hi\n\n"
        if initial:
            yield frame(wire_id, initial)
        while True:
            events = stream_events_after(cursor)
            if not events:
                yield ": keepalive\n\n"
            for cursor, live_id, message in events:
                yield frame(live_id, message)

    return Response(generate(cursor), mimetype="text/event-stream",
                    headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})
