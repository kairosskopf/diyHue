import uuid
import logManager
import weakref
from threading import Thread
from flaskUI import scene_write_lock
from datetime import datetime, timezone
from HueObjects import genV2Uuid, StreamEvent, set_light_actions, brightness_percent

logging = logManager.logger.get_logger(__name__)

def _scene_event(kind, resource, *, deleted_by=None):
    StreamEvent({"creationtime": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                 "id": genV2Uuid(), "type": kind, "data": [resource]}, deleted_by=deleted_by)


def remove_scene(scenes, key, resource_type="scene", *, deleted_by=None):
    """Remove the registered resource before notifying open Hue clients."""
    scene = scenes.pop(key)
    collection = "scenes" if resource_type == "scene" else "smart_scene"
    resource = {"id": scene.id_v2, "id_v1": f"/{collection}/{key}", "type": resource_type}
    _scene_event("delete", resource, deleted_by=deleted_by)


class Scene():

    DEFAULT_SPEED = 0.6269841194152832

    def __init__(self, data):
        self.name = data["name"]
        self.id_v1 = data["id_v1"]
        self.id_v2 = data["id_v2"] if "id_v2" in data else genV2Uuid()
        self.owner = data["owner"]
        self.appdata = data["appdata"] if "appdata" in data else {}
        self.type = data["type"] if "type" in data else "LightScene"
        self.picture = data["picture"] if "picture" in data else ""
        self.image = data["image"] if "image" in data else None
        self.recycle = data["recycle"] if "recycle" in data else False
        self.lastupdated = data["lastupdated"] if "lastupdated" in data else datetime.now(timezone.utc
        ).strftime("%Y-%m-%dT%H:%M:%S")
        self.lightstates = weakref.WeakKeyDictionary()
        self.palette = data["palette"] if "palette" in data else {}
        self.speed = data["speed"] if "speed" in data else self.DEFAULT_SPEED
        self.group = data["group"] if "group" in data else None
        self.lights = data["lights"] if "lights" in data else []
        self._palette_run = None
        self.status = data["status"] if "status" in data else "inactive"
        if "group" in data:
            self.lights = self.group().lights
            if "lightstates" not in data and "actions" not in data:
                self.storelightstate()

    def publish_event(self, kind):
        _scene_event(kind, self.getV2Api())

    def add_light(self, light):
        self.lights.append(light)

    def _stop_palette(self):
        if self._palette_run is not None:
            for light in list(self.lightstates):
                if light.palette_run is self._palette_run:
                    light.palette_run = None
                    light.dynamics["status"] = "none"
            self._palette_run = None

    def activate(self, data, *, group=None):
        import configManager
        with scene_write_lock:
            scenes = configManager.bridgeConfig.yaml_config.get("scenes", {})
            action = data.get("recall", {}).get("action", "active")
            targets = dict(self.lightstates.items())
            if group is not None:  # v1 recalls are restricted to the addressed group
                members = {ref() for ref in group.lights}
                targets = {light: state for light, state in targets.items() if light in members}
            if action == "deactivate":
                self._stop_palette()
                self.status = "inactive"
                self.publish_event("update")
                return
            if action == "dynamic_palette":
                self._stop_palette()
                applied = self._start_palette(targets)
            else:
                applied = self._recall_static(data, targets, group)
                if applied:
                    self._stop_palette()
            for previous in list(scenes.values()):
                if previous is self or previous.status == "inactive":
                    continue
                if not applied.isdisjoint(previous.lightstates):
                    previous._stop_palette()
                    previous.status = "inactive"
                    previous.publish_event("update")
            if applied:
                self.status = action if applied == targets.keys() else "inactive"
            self.publish_event("update")
            return len(applied) == len(targets)

    def _start_palette(self, targets):
        self._palette_run = object()
        applied = set()
        index = 0
        for ref in self.lights:
            light = ref()
            if light not in targets:
                continue
            if targets[light].get("on") is False:
                if light.setV1State({"on": False}) is not False:
                    applied.add(light)
                continue
            if not light.state.get("on") and light.setV1State({"on": True}) is False:
                continue
            light.dynamics["speed"] = self.speed
            light.palette_run = self._palette_run
            Thread(target=light.dynamicScenePlay, args=(self.palette, index, self._palette_run)).start()
            applied.add(light)
            index += 1
        return applied

    def _recall_static(self, data, targets, group):
        actions = []
        for light, saved_state in targets.items():
            state = saved_state.copy()
            if len(data) > 0:
                transitiontime = 0
                if "seconds" in data:
                    transitiontime += data["seconds"] * 10
                if "minutes" in data:
                    transitiontime += data["minutes"] * 600
                if transitiontime > 0:
                    state["transitiontime"] = transitiontime
                if "recall" in data and "duration" in data["recall"]:
                    state["transitiontime"] = int(
                        data["recall"]["duration"] / 100)

            actions.append((light, state))
        applied = set_light_actions(actions, group=group)
        for light in applied:
            if light.dynamics["status"] == "dynamic_palette":
                light.palette_run = None
                light.dynamics["status"] = "none"
        if group is not None:
            group.state = group.update_state()
            group.genStreamEvent({"dimming": {}})
        elif self.type == "GroupScene":
            self.group().state = self.group().update_state()
        return applied

    def getV1Api(self):
        result = {}
        result["name"] = self.name
        result["type"] = self.type
        result["lights"] = []
        result["lightstates"] = {}
        if self.type == "LightScene":
            for light in self.lights:
                if light():
                    result["lights"].append(light().id_v1)
        elif self.type == "GroupScene":
            result["group"] = self.group().id_v1
            for light in self.group().lights:
                if light():
                    result["lights"].append(light().id_v1)

        lightstates = list(self.lightstates.items())
        for light, state in lightstates:
            if light.id_v1 in result["lights"] and "gradient" not in state:
                result["lightstates"][light.id_v1] = state
        result["owner"] = self.owner.username
        result["recycle"] = self.recycle
        # must be fuction to check the presece in rules or schedules
        result["locked"] = True
        result["appdata"] = self.appdata
        if self.image != None:
            result["image"] = self.image
        result["picture"] = self.picture
        result["lastupdated"] = self.lastupdated
        return result

    def getV2Api(self):
        result = {"actions": []}
        lightstates = list(self.lightstates.items())

        for light, state in lightstates:
            v2State = {}
            if "on" in state:
                v2State["on"] = {"on": state["on"]}
            if "bri" in state:
                v2State["dimming"] = {"brightness": brightness_percent(state["bri"])}
                #v2State["dimming_delta"] = {}

            if "xy" in state:
                v2State["color"] = {
                    "xy": {"x": state["xy"][0], "y": state["xy"][1]}}
            if "ct" in state:
                v2State["color_temperature"] = {
                    "mirek": state["ct"]}
            if "gradient" in state:
                v2State["gradient"] = state["gradient"]
            result["actions"].append(
                {
                    "action": v2State,
                    "target": {
                        "rid": light.id_v2,
                        "rtype": "light",
                    },
                }
            )

        if self.type == "GroupScene":
            if self.group():
                result["group"] = {
                    "rid": str(uuid.uuid5(uuid.NAMESPACE_URL, self.group().id_v2 + self.group().type.lower())),
                    "rtype": self.group().type.lower()
                }
        result["metadata"] = {}
        if self.image != None:
            result["metadata"]["image"] = {"rid": self.image,
                                           "rtype": "public_image"}
        result["metadata"]["name"] = self.name
        result["id"] = self.id_v2
        result["id_v1"] = "/scenes/" + self.id_v1
        result["type"] = "scene"
        if self.palette:
            result["palette"] = self.palette
        result["speed"] = self.speed
        result["auto_dynamic"] = False
        result["status"] = {"active": self.status}
        result["recall"] = {}
        return result

    def storelightstate(self):
        lights = self.group().lights if self.type == "GroupScene" else self.lights
        for light in lights:
            if light() is None:
                continue
            if "on" not in light().state:
                continue
            state = {}
            state["on"] = light().state["on"]
            if "colormode" in light().state:
                if light().state["colormode"] == "xy":
                    state["xy"] = light().state["xy"]
                elif light().state["colormode"] == "ct":
                    state["ct"] = light().state["ct"]
                elif light().state["colormode"] == "hs":
                    state["hue"] = light().state["hue"]
                    state["sat"] = light().state["sat"]
            if "bri" in light().state:
                state["bri"] = light().state["bri"]
            self.lightstates[light()] = state

    def update_attr(self, newdata):
        self.lastupdated = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
        if "storelightstate" in newdata and newdata["storelightstate"]:
            self.storelightstate()
            self.publish_event("update")
            return
        for key, value in newdata.items():
            if key == "lightstates" and isinstance(value, dict):
                for light, changes in value.items():
                    self.lightstates.setdefault(light, {}).update(changes)
                continue
            updateAttribute = getattr(self, key)
            if isinstance(updateAttribute, dict):
                updateAttribute.update(value)
                setattr(self, key, updateAttribute)
            else:
                setattr(self, key, value)

        self.publish_event("update")

    def getObjectPath(self):
        return {"resource": "scenes", "id": self.id_v1}

    def save(self):
        result = {"id_v2": self.id_v2, "name": self.name, "appdata": self.appdata, "owner": self.owner.username, "type": self.type, "picture": self.picture,
                  "image": self.image, "recycle": self.recycle, "lastupdated": self.lastupdated, "lights": [], "lightstates": {}}
        if self.type == "GroupScene":
            if self.group():
                result["group"] = self.group().id_v1
            else:
                return False
        if self.palette != None:
            result["palette"] = self.palette
        result["speed"] = self.speed if self.speed is not None else self.DEFAULT_SPEED
        for light in self.lights:
            if light():
                result["lights"].append(light().id_v1)
        lightstates = list(self.lightstates.items())
        for light, state in lightstates:
            result["lightstates"][light.id_v1] = state

        return result
