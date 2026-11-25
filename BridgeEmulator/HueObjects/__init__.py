import uuid
import logManager
import random
from collections import deque
from copy import deepcopy
from itertools import islice
from threading import Condition
from time import monotonic, time

logging = logManager.logger.get_logger(__name__)

eventstream = deque(maxlen=2000)
_event_condition = Condition()
_event_sequence = 0
_event_headers = {}
_event_second = 0
_index_in_second = 0
_SCENE_REPLAY_SECONDS = 300


def StreamEvent(message, *, deleted_by=None):
    global _event_sequence, _event_second, _index_in_second
    with _event_condition:
        eventstream.append(deepcopy(message))
        _event_sequence += 1
        # Hue event IDs use Unix seconds and an index within that second.
        # Keep IDs increasing if the wall clock is corrected backwards.
        second = max(int(time()), _event_second)
        _index_in_second = _index_in_second + 1 if second == _event_second else 0
        _event_second = second
        expires = monotonic() + _SCENE_REPLAY_SECONDS if deleted_by is not None else 0
        _event_headers[_event_sequence] = (f"{second}:{_index_in_second}", deleted_by, expires)
        _event_headers.pop(_event_sequence - eventstream.maxlen, None)
        _event_condition.notify_all()


def event_sequence():
    with _event_condition:
        return _event_sequence


def events_after(cursor, timeout=15):
    with _event_condition:
        _event_condition.wait_for(lambda: _event_sequence > cursor, timeout=timeout)
        first = _event_sequence - len(eventstream) + 1
        index = max(0, cursor + 1 - first)
        return list(enumerate(islice(eventstream, index, None), start=first + index))


def stream_events_after(cursor, timeout=15):
    with _event_condition:
        return [(seq, _event_headers[seq][0], message)
                for seq, message in events_after(cursor, timeout)]


def stream_start(last_id, user):
    """Take the replay snapshot and the live cursor under the publisher's lock."""
    with _event_condition:
        current = _event_sequence
        first = current - len(eventstream) + 1
        if last_id:
            # Accept numeric IDs sent by previous diyHue builds as well.
            if last_id.isascii() and last_id.isdecimal() and len(last_id) < 20:
                seq = int(last_id)
                if first - 1 <= seq <= current:
                    return seq, [], "resume"
            for seq in range(current, first - 1, -1):
                if _event_headers[seq][0] == last_id:
                    return seq, [], "resume"

        # Without a checkpoint we cannot safely repeat button presses or older
        # light states. Re-send only this app's recent scene deletions. A later
        # add for the same UUID cancels the deletion, including after a restore.
        pending = {}
        now = monotonic()
        for seq, message in enumerate(eventstream, first):
            wire_id, deleted_by, expires = _event_headers[seq]
            if deleted_by is user and user is not None and expires > now:
                resource = message["data"][0]
                pending[resource["id"]] = (seq, wire_id, message)
            elif pending:
                for event in message if isinstance(message, list) else (message,):
                    if event.get("type") == "add":
                        for resource in event.get("data", []):
                            pending.pop(resource.get("id"), None)
        return current, list(pending.values()), "unknown checkpoint" if last_id else "no checkpoint"


def v1StateToV2(v1State):
    v2State = {}
    if "on" in v1State:
        v2State["on"] = {"on": v1State["on"]}
    if "bri" in v1State:
        v2State["dimming"] = {"brightness": round(v1State["bri"] / 2.54, 2)}
    if "ct" in v1State:
        v2State["color_temperature"] = {"mirek": v1State["ct"], "color_temperature_delta": {}}
    if "xy" in v1State:
        v2State["color"] = {
            "xy": {"x": v1State["xy"][0], "y": v1State["xy"][1]}}
    return v2State


def v2StateToV1(v2State):
    v1State = {}
    if "dimming" in v2State:
        v1State["bri"] = int(v2State["dimming"]["brightness"] * 2.54)
    if "on" in v2State:
        v1State["on"] = v2State["on"]["on"]
    if "color_temperature" in v2State:
        v1State["ct"] = v2State["color_temperature"]["mirek"]
    if "color" in v2State:
        if "xy" in v2State["color"]:
            v1State["xy"] = [v2State["color"]["xy"]
                             ["x"], v2State["color"]["xy"]["y"]]
    if "gradient" in v2State:
        v1State["gradient"] = v2State["gradient"]
    if "dynamics" in v2State and "duration" in v2State["dynamics"]:
        # v2 gives the transition in milliseconds, v1 in steps of 100 ms.
        v1State["transitiontime"] = round(v2State["dynamics"]["duration"] / 100)
    if "transitiontime" in v2State:  # to be replaced once api will be public
        v1State["transitiontime"] = v2State["transitiontime"]
    return v1State

def genV2Uuid():
    return str(uuid.uuid4())

def generate_unique_id():
    rand_bytes = [random.randrange(0, 256) for _ in range(3)]
    return "00:17:88:01:00:%02x:%02x:%02x-0b" % (rand_bytes[0], rand_bytes[1], rand_bytes[2])


def setGroupAction(group, state, scene=None):
    lightsState = {}
    if scene != None:
        sceneStates = list(scene.lightstates.items())
        for light, state in sceneStates:
            lightsState[light.id_v1] = state
            if "on" in state and state["on"] == True:
                group.state["any_on"] = True

    else:
        state = incProcess(group.action, state)
        for light in group.lights:
            if light():
                # Group 0 means "all lights". Sockets report as Hue on/off
                # plug-in units (model ids LOM001/LOM004/...); switching them
                # with the lights would cut power to whatever is plugged in.
                if group.id_v1 == "0" and (light().config.get("archetype") == "plug" or str(light().modelid).startswith("LOM")):
                    continue
                lightsState[light().id_v1] = state
        if "xy" in state:
            group.action["colormode"] = "xy"
        elif "ct" in state:
            group.action["colormode"] = "ct"
        elif "hue" in state or "sat" in state:
            group.action["colormode"] = "hs"

        if "on" in state:
            group.state["any_on"] = state["on"]
            group.state["all_on"] = state["on"]
        group.action.update(state)

    queueState = {}
    for light in group.lights:
        if light() and light().id_v1 in lightsState:  # apply only if the light belong to this group
            for key, value in lightsState[light().id_v1].items():
                if key in light().state:
                    light().state[key] = value
            light().updateLightState(lightsState[light().id_v1])
            # apply max and min brightness limis
            if "bri" in lightsState[light().id_v1]:
                if "min_bri" in light().protocol_cfg and light().protocol_cfg["min_bri"] > lightsState[light().id_v1]["bri"]:
                    lightsState[light().id_v1]["bri"] = light().protocol_cfg["min_bri"]
                if "max_bri" in light().protocol_cfg and light().protocol_cfg["max_bri"] < lightsState[light().id_v1]["bri"]:
                    lightsState[light().id_v1]["bri"] = light().protocol_cfg["max_bri"]
                if  light().protocol == "mqtt" and not light().state["on"]:
                    continue
            # end limits
            if light().protocol in ["native_multi", "mqtt"]:
                light().genStreamEvent(v1StateToV2(lightsState[light().id_v1]))
                if light().protocol_cfg["ip"] not in queueState:
                    queueState[light().protocol_cfg["ip"]] = {
                        "object": light(), "lights": {}}
                if light().protocol == "native_multi":
                    queueState[light().protocol_cfg["ip"]]["lights"][light(
                    ).protocol_cfg["light_nr"]] = lightsState[light().id_v1]
                elif light().protocol == "mqtt":
                    queueState[light().protocol_cfg["ip"]]["lights"][light(
                    ).protocol_cfg["command_topic"]] = lightsState[light().id_v1]
            else:
                light().setV1State(lightsState[light().id_v1])
    for device, state in queueState.items():
        state["object"].setV1State(state)

    group.state = group.update_state()


def incProcess(state, data):
    if "bri_inc" in data:
        state["bri"] += data["bri_inc"]
        if state["bri"] > 254:
            state["bri"] = 254
        elif state["bri"] < 1:
            state["bri"] = 1
        del data["bri_inc"]
        data["bri"] = state["bri"]
    elif "ct_inc" in data:
        state["ct"] += data["ct_inc"]
        if state["ct"] > 500:
            state["ct"] = 500
        elif state["ct"] < 153:
            state["ct"] = 153
        del data["ct_inc"]
        data["ct"] = state["ct"]
    elif "hue_inc" in data:
        state["hue"] += data["hue_inc"]
        if state["hue"] > 65535:
            state["hue"] -= 65535
        elif state["hue"] < 0:
            state["hue"] += 65535
        del data["hue_inc"]
        data["hue"] = state["hue"]
    elif "sat_inc" in data:
        state["sat"] += data["sat_inc"]
        if state["sat"] > 254:
            state["sat"] = 254
        elif state["sat"] < 1:
            state["sat"] = 1
        del data["sat_inc"]
        data["sat"] = state["sat"]

    return data
