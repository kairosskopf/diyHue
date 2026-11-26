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
                    return seq, [], "resume", None
            for seq in range(current, first - 1, -1):
                if _event_headers[seq][0] == last_id:
                    return seq, [], "resume", None

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
        mode = "unknown checkpoint" if last_id else "no checkpoint"
        wire_id = _event_headers[current][0] if current in _event_headers else None
        return current, list(pending.values()), mode, wire_id


def brightness_percent(level):
    # Older persisted states and some integrations use 255 for full brightness.
    if level is None or level == "null":
        level = 1
    return round(max(0, min(254, float(level))) / 2.54, 2)


def v1StateToV2(v1State):
    v2State = {}
    if "on" in v1State:
        v2State["on"] = {"on": v1State["on"]}
    if "bri" in v1State:
        v2State["dimming"] = {"brightness": brightness_percent(v1State["bri"])}
    if "ct" in v1State:
        v2State["color_temperature"] = {"mirek": v1State["ct"], "color_temperature_delta": {}}
    if "xy" in v1State:
        v2State["color"] = {
            "xy": {"x": v1State["xy"][0], "y": v1State["xy"][1]}}
    return v2State


def v2StateToV1(v2State):
    v1State = {}
    if "dimming" in v2State:
        v1State["bri"] = round(v2State["dimming"]["brightness"] * 2.54)
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

def resolve_dimming_delta(light, command, delta):
    """Resolve a v2 step against this lamp, preserving differences within a room."""
    state = command.copy()
    if "bri" in state or "bri" not in light.state or not state.get("on", light.state["on"]):
        return state
    level = light.state["bri"]
    if not isinstance(level, (int, float)):
        return state
    action = delta.get("action")
    if action == "stop":
        state.update(bri=max(1, min(254, level)), transitiontime=0)
    elif action in ("up", "down"):
        magnitude = delta.get("brightness_delta", 100)
        if isinstance(magnitude, (int, float)) and 0 < magnitude <= 100:
            step = round(magnitude * 2.54)
            state["bri"] = max(1, min(254, level + (step if action == "up" else -step)))
    return state


def genV2Uuid():
    return str(uuid.uuid4())

def generate_unique_id():
    rand_bytes = [random.randrange(0, 256) for _ in range(3)]
    return "00:17:88:01:00:%02x:%02x:%02x-0b" % (rand_bytes[0], rand_bytes[1], rand_bytes[2])


def publish_group_brightness(lights, groups=None, exclude=None):
    if not lights:
        return
    if groups is None:
        import configManager
        groups = configManager.bridgeConfig.yaml_config.get("groups", {})
    for group in list(groups.values()):
        if group is exclude or (group.type not in ("Room", "Zone") and group.id_v1 != "0"):
            continue
        if any(ref() in lights for ref in group.lights):
            group.genStreamEvent({"dimming": {}})


def limit_brightness(state, config):
    if "bri" in state:
        brightness = state["bri"]
        if "min_bri" in config:
            brightness = max(brightness, config["min_bri"])
        if "max_bri" in config:
            brightness = min(brightness, config["max_bri"])
        state["bri"] = brightness


def send_light_batch(batch, changed):
    sender = next(iter(batch.values()))[0]
    command = {"lights": {address: state for address, (_, state) in batch.items()}}
    if sender.protocol == "mqtt":
        command["members"] = {address: light for address, (light, _) in batch.items()}
    tickets = {light: light.begin_write(state) for light, state in batch.values()}
    sent = False
    try:
        sent = sender.send_protocol_command(command)
    finally:
        for light, state in batch.values():
            light.finish_write(state, tickets[light], sent)
    for light, state in batch.values():
        fields = v1StateToV2(state)
        if not sent:
            current = light.getV2Api()
            fields = {key: current[key] for key in fields if key in current}
        light.genStreamEvent(fields, groups={})
        if "bri" in state or "on" in state:
            changed.add(light)
    return sent


def set_light_actions(actions, *, group=None):
    """Dispatch per-lamp states, retaining one network write per batch-capable device."""
    batches = {}
    changed = set()
    applied = set()
    try:
        for light, state in actions:
            state = state.copy()
            if light.protocol not in ("native_multi", "mqtt"):
                if light.setV1State(state, group_changes=changed) is not False:
                    applied.add(light)
                continue

            # A brightness-only group command must not wake an off MQTT light.
            if group is not None and light.protocol == "mqtt" and "bri" in state and not state.get("on", light.state["on"]):
                if "on" not in state:
                    continue
                del state["bri"]
            limit_brightness(state, light.protocol_cfg)
            address = light.protocol_cfg["light_nr" if light.protocol == "native_multi" else "command_topic"]
            batch = batches.setdefault((light.protocol, light.protocol_cfg["ip"]), {})
            batch[address] = (light, state)

        for batch in batches.values():
            if send_light_batch(batch, changed):
                applied.update(light for light, _ in batch.values())
    finally:
        publish_group_brightness(changed, exclude=group)
    return applied


def setGroupAction(group, state, scene=None, *, dimming_delta=None):
    if scene is not None:
        scene.activate({}, group=group)
        return
    lightsState = {}
    if "bri_inc" in state:
        members = (ref() for ref in group.lights)
        levels = [light.state["bri"] for light in members if light is not None
                  and light.state.get("on") and isinstance(light.state.get("bri"), (int, float))]
        if levels:
            group.action["bri"] = round(sum(levels) / len(levels))
    state = incProcess(group.action, state)
    for light in group.lights:
        if light():
            # Group 0 means "all lights". Sockets report as Hue on/off
            # plug-in units (model ids LOM001/LOM004/...); switching them
            # with the lights would cut power to whatever is plugged in.
            if group.id_v1 == "0" and (light().config.get("archetype") == "plug" or str(light().modelid).startswith("LOM")):
                continue
            member_state = state
            if dimming_delta is not None:
                member_state = resolve_dimming_delta(light(), state, dimming_delta)
                if not member_state.keys() - {"transitiontime"}:
                    continue
            lightsState[light().id_v1] = member_state
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

    members = (ref() for ref in group.lights)
    actions = [(light, lightsState[light.id_v1]) for light in members
               if light is not None and light.id_v1 in lightsState]
    set_light_actions(actions, group=group)

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
