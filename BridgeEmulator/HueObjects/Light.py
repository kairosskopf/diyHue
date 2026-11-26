import uuid
import logManager
from flaskUI import scene_write_lock
from lights.light_types import lightTypes, archetype
from lights.protocols import protocols, wled
from HueObjects import resolve_dimming_delta, genV2Uuid, incProcess, v1StateToV2, generate_unique_id, v2StateToV1, StreamEvent, limit_brightness, publish_group_brightness, brightness_percent
from datetime import datetime, timezone
from copy import deepcopy
from time import sleep
from threading import RLock

logging = logManager.logger.get_logger(__name__)

class Light():
    def __init__(self, data):
        self.name = data["name"]
        self.modelid = data["modelid"]
        self.id_v1 = data["id_v1"]
        self.id_v2 = data["id_v2"] if "id_v2" in data else genV2Uuid()
        self.uniqueid = data["uniqueid"] if "uniqueid" in data else generate_unique_id()
        # Saved lights pass their own state in, and new lights pass the catalog
        # template. Either way this object has to own the dicts, including nested
        # lists such as xy and gradient points.
        self.state = deepcopy(data["state"]) if "state" in data else deepcopy(
            lightTypes[self.modelid]["state"])
        self.protocol = data["protocol"] if "protocol" in data else "dummy"
        self.config = deepcopy(data["config"]) if "config" in data else deepcopy(
            lightTypes[self.modelid]["config"])
        self.protocol_cfg = data["protocol_cfg"] if "protocol_cfg" in data else {}
        self.state_lock = RLock()
        self.pending_fields = {}
        self.palette_run = None
        self.streaming = False
        self.dynamics = deepcopy(lightTypes[self.modelid]["dynamics"])
        self.effect = "no_effect"
        self.function = data["function"] if "function" in data else "mixed"

        # entertainment
        streamMessage = {"creationtime": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                         "data": [{"id": str(uuid.uuid5(
                             uuid.NAMESPACE_URL, self.id_v2 + 'entertainment')), "type": "entertainent"}],
                         "id": str(uuid.uuid4()),
                         "type": "add"
                         }
        streamMessage["id_v1"] = "/lights/" + self.id_v1
        streamMessage["data"][0].update(self.getV2Entertainment())
        StreamEvent(streamMessage)

        # zigbee_connectivity
        streamMessage = {"creationtime": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                         "data": [self.getZigBee()],
                         "id": str(uuid.uuid4()),
                         "type": "add"
                         }
        StreamEvent(streamMessage)

        # light
        streamMessage = {"creationtime": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                         "data": [self.getV2Api()],
                         "id": str(uuid.uuid4()),
                         "type": "add"
                         }
        StreamEvent(streamMessage)

        # device
        streamMessage = {"creationtime": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                         "data": [self.getDevice()],
                         "id": str(uuid.uuid4()),
                         "type": "add"
                         }
        streamMessage["data"][0].update(self.getDevice())
        StreamEvent(streamMessage)

    def __del__(self):
        ## light ##
        streamMessage = {"creationtime": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                         "data": [{"id": self.id_v2, "type": "light"}],
                         "id": str(uuid.uuid4()),
                         "type": "delete"
                         }
        streamMessage["id_v1"] = "/lights/" + self.id_v1
        StreamEvent(streamMessage)

        ## device ##
        streamMessage = {"creationtime": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                         "data": [{"id": self.getDevice()["id"], "type": "device"}],
                         "id": str(uuid.uuid4()),
                         "type": "delete"
                         }
        streamMessage["id_v1"] = "/lights/" + self.id_v1
        StreamEvent(streamMessage)

        # Zigbee Connectivity
        streamMessage = {"creationtime": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                         "data": [{"id": self.getZigBee()["id"], "type": "zigbee_connectivity"}],
                         "id": str(uuid.uuid4()),
                         "type": "delete"
                         }
        streamMessage["id_v1"] = "/lights/" + self.id_v1
        StreamEvent(streamMessage)

        # Entertainment
        streamMessage = {"creationtime": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                         "data": [{"id": self.getV2Entertainment()["id"], "type": "entertainment"}],
                         "id": str(uuid.uuid4()),
                         "type": "delete"
                         }
        streamMessage["id_v1"] = "/lights/" + self.id_v1
        StreamEvent(streamMessage)

        logging.info(self.name + " light was destroyed.")

    def update_attr(self, newdata):
        for key, value in newdata.items():
            updateAttribute = getattr(self, key)
            if isinstance(updateAttribute, dict):
                updateAttribute.update(value)
                setattr(self, key, updateAttribute)
            else:
                setattr(self, key, value)
        streamMessage = {"creationtime": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                         "data": [self.getDevice()],
                         "id": str(uuid.uuid4()),
                         "type": "update"
                         }
        StreamEvent(streamMessage)

    def getV1Api(self):
        result = deepcopy(lightTypes[self.modelid]["v1_static"])
        result["config"] = self.config
        result["state"] = {"on": self.state["on"]}
        if "bri" in self.state and self.modelid not in ["LOM001", "LOM004", "LOM010"]:
            result["state"]["bri"] = int(self.state["bri"]) if self.state["bri"] is not None else 1
        if "ct" in self.state and self.modelid not in ["LOM001", "LOM004", "LOM010", "LTW001", "LLC010"]:
            result["state"]["ct"] = self.state["ct"]
            result["state"]["colormode"] = self.state["colormode"]
        if "xy" in self.state and self.modelid not in ["LOM001", "LOM004", "LOM010", "LTW001", "LWB010"]:
            result["state"]["xy"] = self.state["xy"]
            result["state"]["hue"] = self.state["hue"]
            result["state"]["sat"] = self.state["sat"]
            result["state"]["colormode"] = self.state["colormode"]
        result["state"]["alert"] = self.state["alert"]
        if "mode" in self.state:
            result["state"]["mode"] = self.state["mode"]
        result["state"]["reachable"] = self.state["reachable"]
        # Smart plugs report their measurements; pass them through so apps and
        # the WebUI can display them.
        for measurement in ("power", "voltage", "current", "energy", "childlock"):
            if measurement in self.state:
                result["state"][measurement] = self.state[measurement]
        result["modelid"] = self.modelid
        result["name"] = self.name
        result["uniqueid"] = self.uniqueid
        return result

    def updateLightState(self, state):

        if "xy" in state and "xy" in self.state:
            self.state["colormode"] = "xy"
        elif "ct" in state and "ct" in self.state:
            self.state["colormode"] = "ct"
        elif ("hue" in state or "sat" in state) and "hue" in self.state:
            self.state["colormode"] = "hs"

    def setV1State(self, state, advertise=True, *, group_changes=None):
        if self.protocol == "homeassistant_ws":
            from services import homeAssistantWS
            if homeAssistantWS.event_session is not None:
                with homeAssistantWS.event_session.reducer.lock:
                    homeAssistantWS.assert_command_allowed(self)
                    return self._setV1State(state, advertise, group_changes)
        return self._setV1State(state, advertise, group_changes)

    def _cache_command(self, state, *, update_mode=True):
        if state.get("on") is False:
            self.dynamics["status"] = "none"
        if update_mode:
            self.updateLightState(state)
        for key, value in state.items():
            if key in self.state:
                self.state[key] = value
            if key in self.config:
                self.config[key] = value.replace("_", "") if key == "archetype" else value
            if key == "name":
                self.name = value
            if key == "function":
                self.function = value

    def begin_write(self, state):
        # A device can report a value even before the transport returns. A
        # report (including an unchanged one) takes ownership of that field.
        ticket = object()
        with self.state_lock:
            keys = (state.keys() & self.state.keys()) | {"reachable"}
            if state.keys() & {"xy", "ct", "hue", "sat"}:
                keys.add("colormode")
            self.pending_fields.update(dict.fromkeys(keys, ticket))
        return ticket

    def finish_write(self, state, ticket, success):
        with self.state_lock:
            owned = {k for k, owner in self.pending_fields.items() if owner is ticket}
            for key in owned:
                del self.pending_fields[key]
            if "reachable" in owned:
                self.state["reachable"] = success
            if success:
                accepted = {k: v for k, v in state.items() if k not in self.state or k in owned}
                self._cache_command(accepted, update_mode="colormode" in owned)

    def send_protocol_command(self, state):
        from services.homeAssistantWS import MappingUnavailable
        for adapter in protocols:
            if adapter.__name__ == "lights.protocols." + self.protocol:
                try:
                    adapter.set_light(self, state)
                except MappingUnavailable:
                    raise
                except Exception as error:
                    logging.warning("%s light error: %s", self.name, error)
                    return False
                break
        return True

    def _setV1State(self, state, advertise, group_changes):
        if "lights" in state:
            return self.send_protocol_command(state)
        state = incProcess(self.state.copy(), state)
        limit_brightness(state, self.protocol_cfg)
        if self.protocol in ("mqtt", "homeassistant_ws"):
            ticket = self.begin_write(state)
            sent = False
            try:
                sent = self.send_protocol_command(state)
            finally:
                self.finish_write(state, ticket, sent)
            if not sent:
                return False
        else:
            self._cache_command(state)
            if self.protocol != "dummy":
                self.state["reachable"] = self.send_protocol_command(state)
                if not self.state["reachable"]:
                    return False
        if advertise:
            v2State = v1StateToV2(state)
            self.genStreamEvent(v2State, groups=None if group_changes is None else {})
            if group_changes is not None and ("on" in state or "bri" in state):
                group_changes.add(self)

    def setV2State(self, state):
        v1State = v2StateToV1(state)
        if "effects_v2" in state and "action" in state["effects_v2"]:
            v1State["effect"] = state["effects_v2"]["action"]["effect"]
            self.effect = v1State["effect"]
        if "effects" in state:
            v1State["effect"] = state["effects"]["effect"]
            self.effect = v1State["effect"]
        if "dynamics" in state and "speed" in state["dynamics"]:
            self.dynamics["speed"] = state["dynamics"]["speed"]
        if "metadata" in state:
            if "archetype" in state["metadata"]:
                v1State["archetype"] = state["metadata"]["archetype"]
            if "name" in state["metadata"]:
                v1State["name"] = state["metadata"]["name"]
            if "function" in state["metadata"]:
                v1State["function"] = state["metadata"]["function"]
        if "dimming_delta" in state:
            state = state.copy()
            v1State = resolve_dimming_delta(self, v1State, state.pop("dimming_delta"))
            if "bri" in v1State:
                state["dimming"] = {}
            elif not v1State.keys() - {"transitiontime"}:
                return
        if self.setV1State(v1State, advertise=False) is False:
            return False
        self.genStreamEvent(state)

    def genStreamEvent(self, v2State, groups=None):
        with self.state_lock:
            self._publish_state(v2State, groups)

    def _publish_state(self, v2State, groups):
        # Commands may be quantized, limited per lamp, or corrected by a device
        # report. Publish the same brightness and power that a subsequent GET sees.
        if "on" in v2State or "dimming" in v2State:
            current = self.getV2Api()
            v2State = v2State.copy()
            for key in ("on", "dimming"):
                if key in v2State:
                    if key in current:
                        v2State[key] = current[key]
                    else:
                        del v2State[key]
        streamMessage = {"creationtime": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                         "data": [{"id": self.id_v2,"id_v1": "/lights/" + self.id_v1, "type": "light"}],
                         "id": str(uuid.uuid4()),
                         "type": "update"
                         }
        streamMessage["data"][0].update(v2State)
        streamMessage["data"][0].update({"owner": {"rid": self.getDevice()["id"], "rtype": "device"}})
        streamMessage["data"][0].update({"service_id": self.protocol_cfg["light_nr"]-1 if "light_nr" in self.protocol_cfg else 0})
        StreamEvent(streamMessage)
        streamMessage = {"creationtime": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                         "data": [self.getDevice()],
                         "id": str(uuid.uuid4()),
                         "type": "update"
                         }
        StreamEvent(streamMessage)
        if "on" in v2State or "dimming" in v2State:
            publish_group_brightness({self}, groups)

    def getDevice(self):
        result = {"id": str(uuid.uuid5(
            uuid.NAMESPACE_URL, self.id_v2 + 'device'))}
        result["id_v1"] = "/lights/" + self.id_v1
        result["identify"] = {}
        result["metadata"] = {
            "archetype": archetype[self.config["archetype"]],
            "name": self.name
        }
        result["product_data"] = deepcopy(lightTypes[self.modelid]["device"])
        result["product_data"]["model_id"] = self.modelid
        result["service_id"] = self.protocol_cfg["light_nr"]-1 if "light_nr" in self.protocol_cfg else 0
        result["services"] = [
            {
                "rid": self.id_v2,
                "rtype": "light"
            },
            {
                "rid": str(uuid.uuid5(uuid.NAMESPACE_URL, self.id_v2 + 'zigbee_connectivity')),
                "rtype": "zigbee_connectivity"
            },
            {
                "rid": str(uuid.uuid5(uuid.NAMESPACE_URL, self.id_v2 + 'entertainment')),
                "rtype": "entertainment"
            }
        ]
        result["type"] = "device"
        return result

    def getZigBee(self):
        result = {}
        result["id"] = str(uuid.uuid5(uuid.NAMESPACE_URL,
                                      self.id_v2 + 'zigbee_connectivity'))
        result["id_v1"] = "/lights/" + self.id_v1
        result["mac_address"] = self.uniqueid[:23]
        result["owner"] = {
            "rid": self.getDevice()["id"],
            "rtype": "device"
        }
        result["status"] = "connected" if self.state["reachable"] else "connectivity_issue"
        result["type"] = "zigbee_connectivity"
        return result

    def getBridgeHome(self):
        return {
            "rid": self.id_v2,
            "rtype": "light"
        }

    def getV2Api(self):
        result = {}
        result["alert"] = {"action_values": ["breathe"]}
        # gradient lights
        if self.modelid in ["LCX002", "915005987201", "LCX004", "LCX006"]:
            result["effects"] = {
                "effect_values": [
                    "no_effect",
                    "candle",
                    "fire"
                ],
                "status": self.effect,
                "status_values": [
                    "no_effect",
                    "candle",
                    "fire"
                ]
            }
            result["gradient"] = {"points": self.state["gradient"]["points"],
                                  "points_capable": self.protocol_cfg.get("points_capable", 5)}

        # color lights only
        if self.modelid in ["LST002", "LCT001", "LCT015", "LCX002", "915005987201", "LCX004", "LCX006", "LCA005", "LLC010", "LCG001"]:
            colorgamut = lightTypes[self.modelid]["v1_static"]["capabilities"]["control"]["colorgamut"]
            result["color"] = {
                "gamut": {
                    "blue":  {"x": colorgamut[2][0], "y": colorgamut[2][1]},
                    "green": {"x": colorgamut[1][0], "y": colorgamut[1][1]},
                    "red":   {"x": colorgamut[0][0], "y": colorgamut[0][1]}
                },
                "gamut_type": lightTypes[self.modelid]["v1_static"]["capabilities"]["control"]["colorgamuttype"],
                "xy": {
                    "x": self.state["xy"][0],
                    "y": self.state["xy"][1]
                }
            }
        if "ct" in self.state:
            result["color_temperature"] = {
                "mirek": self.state["ct"] if self.state["colormode"] == "ct" else None,
                "mirek_schema": {
                    "mirek_maximum": 500,
                    "mirek_minimum": 153
                }
            }
            result["color_temperature"]["mirek_valid"] = True if self.state[
                "ct"] != None and self.state["ct"] < 500 and self.state["ct"] > 153 else False
            result["color_temperature_delta"] = {}
        if "bri" in self.state:
            result["dimming"] = {
                "brightness": brightness_percent(self.state["bri"]),
                "min_dim_level": 0.1  # Adjust this value as needed
            }
            result["dimming_delta"] = {}
        result["dynamics"] = self.dynamics
        result["effects"] = {
            "effect_values": [
                "no_effect",
                "candle",
                "fire",
                "colorloop"
            ],
            "status": self.effect,
            "status_values": [
                "no_effect",
                "candle",
                "fire",
                "colorloop"
            ]
        }
        if self.protocol == "wled":
            # "none" is a v1-only internal alias for "no_effect"; "sunbeam" is a
            # silent input alias for "sunrise" (see wled.py) -- neither is advertised.
            wled_effect_values = [e for e in wled.HUE_EFFECT_TO_WLED_FX if e not in ("none", "sunbeam")]
            result["effects_v2"] = {
                "action": {"effect_values": wled_effect_values},
                "status": {
                    "effect": self.effect,
                    "effect_values": wled_effect_values
                }
            }
        result["timed_effects"] = {}
        result["identify"] = {}
        result["id"] = self.id_v2
        result["id_v1"] = "/lights/" + self.id_v1
        result["metadata"] = {"name": self.name, "function": self.function,
                              "archetype": archetype[self.config["archetype"]]}
        result["mode"] = "normal"
        if "mode" in self.state and self.state["mode"] == "streaming":
            result["mode"] = "streaming"
        result["on"] = {
            "on": self.state["on"]
        }
        result["owner"] = {
            "rid": str(uuid.uuid5(uuid.NAMESPACE_URL, self.id_v2 + 'device')),
            "rtype": "device"
        }
        result["product_data"] = {"function": "mixed"}
        result["signaling"] = {"signal_values": [
            "no_signal",
            "on_off"]}
        result["powerup"] = {
            "preset": "last_on_state",
            "configured": True,
            "on": {
                 "mode": "on",
                 "on": {
                      "on": True
                }
            },
            "dimming": {
                "mode": "previous"
            }
        }
        result["service_id"] = self.protocol_cfg["light_nr"]-1 if "light_nr" in self.protocol_cfg else 0
        result["type"] = "light"
        return result

    def getV2Entertainment(self):
        entertainmenUuid = str(uuid.uuid5(
            uuid.NAMESPACE_URL, self.id_v2 + 'entertainment'))
        result = {
            "equalizer": True,
            "id": entertainmenUuid,
            "id_v1": "/lights/" + self.id_v1,
            "proxy": lightTypes[self.modelid]["v1_static"]["capabilities"]["streaming"]["proxy"],
            "renderer": lightTypes[self.modelid]["v1_static"]["capabilities"]["streaming"]["renderer"],
            "renderer_reference": {
                "rid": self.id_v2,
                "rtype": "light"
            }
        }
        result["owner"] = {
            "rid": self.getDevice()["id"], "rtype": "device"}
        result["segments"] = {
            "configurable": False
        }
        if self.modelid == "LCX002":
            result["segments"]["max_segments"] = 7
            result["segments"]["segments"] = [
                {
                    "length": 2,
                    "start": 0
                },
                {
                    "length": 2,
                    "start": 2
                },
                {
                    "length": 4,
                    "start": 4
                },
                {
                    "length": 4,
                    "start": 8
                },
                {
                    "length": 4,
                    "start": 12
                },
                {
                    "length": 2,
                    "start": 16
                },
                {
                    "length": 2,
                    "start": 18
                }]
        elif self.modelid in ["915005987201", "LCX004", "LCX006"]:
            result["segments"]["max_segments"] = 10
            result["segments"]["segments"] = [
                {
                    "length": 3,
                    "start": 0
                },
                {
                    "length": 4,
                    "start": 3
                },
                {
                    "length": 3,
                    "start": 7
                }
            ]
        else:
            result["segments"]["max_segments"] = 1
            result["segments"]["segments"] = [{
                "length": 1,
                "start": 0
            }]
        result["type"] = "entertainment"
        return result

    def getObjectPath(self):
        return {"resource": "lights", "id": self.id_v1}

    def dynamicScenePlay(self, palette, index, run=None):
        logging.debug("Start Dynamic scene play for " + self.name)
        with scene_write_lock:
            if self.palette_run is not run:
                return
            if "dynamic_palette" in self.dynamics["status_values"]:
                self.dynamics["status"] = "dynamic_palette"
        while (
            self.palette_run is run
            and self.dynamics["status"] == "dynamic_palette"
            and self.state.get("on", True)
        ):
            try:
                current_speed = float(self.dynamics["speed"])
            except (TypeError, ValueError):
                current_speed = 0.6269841194152832

            # diyHue's existing Hue-like timing model.
            # Avoid division by zero but otherwise preserve Hue's
            # incoming dynamics.speed value unchanged.
            current_speed = max(0.01, current_speed)
            transition = max(1, int(30 / current_speed))

            logging.debug(
                "Dynamic scene %s speed %.4f transition %.1fs",
                self.name,
                current_speed,
                transition / 10
            )
            if self.modelid in ["LCT001", "LCT015", "LST002", "LCX002", "915005987201", "LCX004", "LCX006", "LCA005"]:
                if index >= len(palette["color"]):
                    index = 0
                points = []
                if self.modelid in ["LCX002", "915005987201", "LCX004", "LCX006"]:
                    # for gradient lights
                    gradientIndex = index
                    for x in range(self.protocol_cfg.get("points_capable", 5)):
                        points.append(palette["color"][gradientIndex])
                        gradientIndex += 1
                        if gradientIndex == len(palette["color"]):
                            gradientIndex = 0
                    lightState = {"gradient": {"points": points}, "transitiontime": transition}
                else:
                    lightState = deepcopy(palette["color"][index])
                    # based on youtube videos, the transition is slow
                    lightState["transitiontime"] = transition
            elif self.modelid == "LTW001":
                if index >= len(palette["color_temperature"]):
                    index = 0
                lightState = deepcopy(palette["color_temperature"][index])
                lightState["transitiontime"] = transition
            else:
                if index >= len(palette["dimming"]):
                    index = 0
                lightState = deepcopy(palette["dimming"][index])
                lightState["transitiontime"] = transition
            with scene_write_lock:
                if self.palette_run is not run or self.dynamics["status"] != "dynamic_palette":
                    return
                self.setV2State(lightState)
            # Wait in short slices instead of one long sleep.
            # This lets Hue speed changes and OFF commands take effect
            # immediately instead of after the current transition.
            remaining = transition / 10

            while (
                remaining > 0
                and self.palette_run is run
                and self.dynamics["status"] == "dynamic_palette"
                and self.state.get("on", True)
            ):
                try:
                    new_speed = max(
                        0.01,
                        float(self.dynamics["speed"])
                    )
                except (TypeError, ValueError):
                    new_speed = current_speed

                # Hue changed the live speed: recalculate immediately.
                if abs(new_speed - current_speed) > 0.0001:
                    break

                chunk = min(0.1, remaining)
                sleep(chunk)
                remaining -= chunk

            if self.palette_run is not run:
                return
            # OFF or scene deactivation means the worker is finished.
            if (
                self.dynamics["status"] != "dynamic_palette"
                or not self.state.get("on", True)
            ):
                self.dynamics["status"] = "none"
                break

            index += 1
            logging.debug("Step forward dynamic scene " + self.name)
        logging.debug("Dynamic Scene " + self.name + " stopped.")

    def save(self):
        result = {"id_v2": self.id_v2, "name": self.name, "modelid": self.modelid, "uniqueid": self.uniqueid, "function": self.function,
                  "state": self.state, "config": self.config, "protocol": self.protocol, "protocol_cfg": self.protocol_cfg}
        return result
