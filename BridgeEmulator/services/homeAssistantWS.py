import logManager
import json
import time
import threading
from services.haReflection import ha_brightness
from ws4py.client.threadedclient import WebSocketClient

logging = logManager.logger.get_logger(__name__)


discovery_timeout_seconds = 60
auth_timeout_seconds = 10
discovery_result = threading.Event()
next_connection_error_log = 0
logging_backoff = 2  # 2 Second back off
homeassistant_token = ''
homeassistant_url = 'ws://127.0.0.1:8123/api/websocket'
homeassistant_ws_client = None
event_session = None
reconfiguration_lock = threading.RLock()
include_by_default = False
use_https = False

# This is Home Assistant States so looks like this:
# {
#   'entity_id': 'light.my_light',
#   'state': 'on',
#   'attributes': {
#        'min_mireds': 153,
#        'max_mireds': 500,
#        'effect_list': ['colorloop', 'random'],
#        'brightness': 254,
#        'hs_color': [291.687, 65.098],
#        'rgb_color': [232, 89, 255],
#        'xy_color': [0.348, 0.168],
#        'is_hue_group': True,
#        'friendly_name': 'My Light',
#        'supported_features': 63
#   },
#   'last_changed': '2019-01-09T10:35:39.148462+00:00',
#    'last_updated': '2019-01-09T10:35:39.148462+00:00',
#    'context': {'id': 'X', 'parent_id': None, 'user_id': None}
# }
latest_states = {}


class HomeAssistantClient(WebSocketClient):


    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Home Assistant rejects anything but the auth message until it has
        # answered auth_ok, so commands have to wait for this.
        self.authenticated = threading.Event()
        self.message_id = 1
        self.id_to_type = {}
        self.send_lock = threading.RLock()
        self.snapshot_due = 0
        self.snapshot_deadline = time.monotonic() + auth_timeout_seconds
        self.snapshot_id = None
        self.snapshot_events = {}

    def opened(self):
        logging.info("Home Assistant WebSocket Connection Opened")

    def closed(self, code, reason=None):
        logging.info(
            "Home Assistant WebSocket Connection Closed. Code: {} Reason {}".format(code, reason))
        self.authenticated.clear()
        with reconfiguration_lock:
            if self is homeassistant_ws_client and not reflection_enabled():
                for entity in list(latest_states):
                    latest_states[entity] = dict(latest_states[entity], state="unavailable")
                    publish_entity(entity, latest_states[entity])

    def received_message(self, m):
        # logging.debug("Received message: {}".format(m))
        message_text = m.data.decode(m.encoding)
        message = json.loads(message_text)
        if message.get('type', None) == "auth_required":
            self.do_auth_required(message)
        elif message.get('type', None) == "auth_ok":
            self.do_auth_complete()
        elif message.get('type', None) == "auth_invalid":
            self.do_auth_invalid(message)
        elif message.get('type', None) == "result":
            self.do_result(message)
        elif message.get('type', None) == "event":
            self.do_event(message)
        elif message.get('type', None) == "pong":
            self.do_pong(message)
        else:
            logging.warning("Unexpected message: %s", message)

    def do_auth_required(self, m):
        logging.info("Home Assistant Web Socket Authorisation required")
        payload = {
            'type': 'auth',
            'access_token': homeassistant_token
        }
        self._send(payload)

    def do_auth_invalid(self, message):
        logging.error(
            "Home Assistant Web Socket Authorisation invalid: {}".format(message))

    def do_auth_complete(self):
        logging.info("Home Assistant Web Socket Authorisation complete")
        self.authenticated.set()
        self.subscribe_for_updates()
        self.get_all_lights()

    def get_all_lights(self):
        with reconfiguration_lock, self.send_lock:
            if self.snapshot_id is not None:
                return
            discovery_result.clear()
            self.snapshot_deadline = time.monotonic() + 15
            self.snapshot_events.clear()
            self.snapshot_id = self.message_id
            self._send_with_id({"type": "get_states"}, "getstates")

    def subscribe_for_updates(self):
        payload = {
            "type": "subscribe_events",
            "event_type": "state_changed"
        }
        self._send_with_id(payload, "subscribe")

    def change_light(self, light, data):
        service_data = {}
        service_data['entity_id'] = light.protocol_cfg['entity_id']
        if light.protocol_cfg['entity_id'].startswith("light."):
            payload = {
                "type": "call_service",
                "domain": "light",
                "service_data": service_data
            }
        elif light.protocol_cfg['entity_id'].startswith("switch."):
            payload = {
                "type": "call_service",
                "domain": "switch",
                "service_data": service_data
            }
        payload["service"] = "turn_on"
        if 'on' in data:
            if not data['on']:
                payload["service"] = "turn_off"

        color_from_hsv = False
        for key, value in data.items():
            if key == "ct":
                service_data['color_temp_kelvin'] = 1000000 / value
            if key == "bri":
                service_data['brightness'] = ha_brightness(value)
            if key == "xy":
                service_data['xy_color'] = [value[0], value[1]]
            if key == "hue":
                color_from_hsv = True
            if key == "sat":
                color_from_hsv = True
            if key == "on":
                if value:
                    payload["service"] = "turn_on"
                else:
                    payload["service"] = "turn_off"
            if key == "alert":
                service_data['flash'] = "long"
            if key == "transitiontime":
                service_data['transition'] = value / 10

        if color_from_hsv:
            # Hue uses hue 0-65535 and sat 0-254; Home Assistant's hs_color is
            # [0-360, 0-100]. A request can carry only one of the two.
            hue = data.get('hue', light.state.get('hue', 0))
            sat = data.get('sat', light.state.get('sat', 0))
            service_data['hs_color'] = [round(hue * 360 / 65535, 2), round(sat * 100 / 254, 2)]

        self._send_with_id(payload, "service")

    def do_result(self, message):
        with self.send_lock:
            kind = self.id_to_type.pop(message.get("id"), None)
        if kind == "service" and not message.get("success"):
            self.snapshot_due = 0
        if kind == "subscribe" and not message.get("success"):
            self.client_terminated = True
            self.authenticated.clear()
            self.close_connection()
        if kind != "getstates":
            return
        with reconfiguration_lock:
            if self is not homeassistant_ws_client or self.client_terminated or reflection_enabled():
                return
            self.snapshot_id = None
            rows = message.get("result")
            if not message.get("success") or not isinstance(rows, list):
                self.snapshot_deadline = 0
                return
            snapshot = {row["entity_id"]: row for row in rows if self._should_include(row)}
            # Events received while this read was in flight can be newer
            # than the returned snapshot. HA timestamps order competing rows.
            from services.haReflection import revision
            for entity, event_row in self.snapshot_events.items():
                if event_row is None:
                    snapshot.pop(entity, None)
                    continue
                candidate = snapshot.get(entity)
                if candidate is not None:
                    try:
                        if revision(candidate) > revision(event_row):
                            continue
                    except (AttributeError, KeyError, ValueError, TypeError):
                        pass
                snapshot[entity] = event_row
            self.snapshot_events.clear()
            import configManager
            mapped = {light.protocol_cfg.get("entity_id")
                      for light in list(configManager.bridgeConfig.yaml_config.get("lights", {}).values())
                      if light.protocol == "homeassistant_ws"}
            removed = (set(latest_states) | mapped) - snapshot.keys()
            latest_states.clear()
            latest_states.update(snapshot)
            for entity in removed:
                publish_entity(entity, None)
            for entity, row in snapshot.items():
                publish_entity(entity, row)
            self.snapshot_deadline = float("inf")
            self.snapshot_due = time.monotonic() + 45
            discovery_result.set()

    def do_event(self, message):
        try:
            event_type = message['event']['event_type']
            if event_type == 'state_changed':
                self.do_state_changed(message)
        except KeyError:
            logging.exception("No event_type  in event")

    def do_state_changed(self, message):
        data = message.get("event", {}).get("data", {})
        entity = data.get("entity_id")
        if not isinstance(entity, str) or not entity.startswith(("light.", "switch.")):
            return
        row = data.get("new_state")
        with reconfiguration_lock:
            if self is not homeassistant_ws_client or self.client_terminated or reflection_enabled():
                return
            included = self._should_include(row)
            if self.snapshot_id is not None:
                self.snapshot_events[entity] = row if included else None
            if included:
                latest_states[entity] = row
                publish_entity(entity, row)
            elif entity in latest_states:
                del latest_states[entity]
                publish_entity(entity, None)

    def _should_include(self, ha_state):
        if not isinstance(ha_state, dict) or not isinstance(ha_state.get("entity_id"), str):
            return False
        should_include = False
        diy_hue_flag = None
        entity_id = ha_state.get('entity_id', None)
        if entity_id.startswith("light.") or entity_id.startswith("switch."):
            if 'attributes' in ha_state and 'diyhue' in ha_state['attributes']:
                diy_hue_flag = ha_state['attributes']['diyhue']

            if include_by_default:
                if diy_hue_flag is not None and diy_hue_flag == "exclude":
                    should_include = False
                else:
                    should_include = True
            else:
                if diy_hue_flag is not None and diy_hue_flag == "include":
                    should_include = True
                else:
                    should_include = False
#        logging.debug("Home Asssitant Web Socket should include? {} - Include By Default? {}, Attribute: {} - State {}".format(should_include, include_by_default, diy_hue_flag, new_state))
        return should_include

    def _send_with_id(self, payload, type_of_call):
        with self.send_lock:
            payload['id'] = self.message_id
            self.id_to_type[self.message_id] = type_of_call
            self.message_id += 1
            self._send(payload)

    def _send(self, payload):
        json_payload = json.dumps(payload)
        self.send(json_payload)


def reflection_enabled():
    return event_session is not None


class MappingUnavailable(ValueError):
    pass


def assert_command_allowed(light):
    if event_session is not None and not event_session.reducer.command_allowed(light):
        raise MappingUnavailable("HA mapping is not authoritative")


def update_mapped_light(light, row, bridge):
    from services.haReflection import translate
    from services.lightReports import apply_report
    report = translate(light.state, row) if row else {"reachable": False}
    apply_report(light, report, bridge.get("groups", {}))


def publish_entity(entity, row):
    import configManager
    bridge = configManager.bridgeConfig.yaml_config
    for light in list(bridge.get("lights", {}).values()):
        if light.protocol == "homeassistant_ws" and light.protocol_cfg.get("entity_id") == entity:
            try:
                update_mapped_light(light, row, bridge)
            except (KeyError, TypeError, ValueError, IndexError, ArithmeticError):
                logging.exception("Invalid Home Assistant state for %s", entity)


def maintain_legacy_connection(bridge):
    while True:
        time.sleep(5)
        if not bridge["config"]["homeassistant"].get("enabled"):
            continue
        try:
            with reconfiguration_lock:
                if reflection_enabled():
                    continue
                client = connect_if_required()
                if client is None:
                    continue
                now = time.monotonic()
                if now >= client.snapshot_deadline:
                    logging.warning("Home Assistant state read timed out; reconnecting")
                    client.client_terminated = True
                    client.authenticated.clear()
                    client.close_connection()
                elif client.authenticated.is_set() and client.snapshot_id is None and now >= client.snapshot_due:
                    client.get_all_lights()
        except (OSError, ValueError, RuntimeError):
            logging.exception("Home Assistant connection check failed")


def connect_if_required():
    # A command may already hold the event owner's reducer lock. Reading its
    # published client here avoids acquiring the lifecycle locks in reverse.
    owner = event_session
    if owner is not None:
        return owner.client
    with reconfiguration_lock:
        if event_session is not None:
            return event_session.client
        if homeassistant_ws_client is None or homeassistant_ws_client.client_terminated:
            create_websocket_client()
        return homeassistant_ws_client


def connect_and_authenticate():
    """Return a connected client once Home Assistant has accepted its token, or None."""
    client = connect_if_required()
    if client is None or not client.authenticated.wait(timeout=auth_timeout_seconds):
        return None
    return client


def create_websocket_client():
    global homeassistant_ws_client
    global next_connection_error_log
    global logging_backoff
    if time.time() >= next_connection_error_log:
        logging.warning(
            "Home Assistant Web Socket Client disconnected trying to (re)connect")

    try:
        homeassistant_ws_client = HomeAssistantClient(
            homeassistant_url, protocols=['http-only', 'chat'])
        homeassistant_ws_client.sock.settimeout(auth_timeout_seconds)
        homeassistant_ws_client.connect()
        logging.info("Home Assistant Web Socket Client connected")
        logging_backoff = 2
    except Exception:
        if homeassistant_ws_client is not None:
            homeassistant_ws_client.close_connection()
        if time.time() >= next_connection_error_log:
            logging.exception("Error connecting to Home Assistant WebSocket")
            next_connection_error_log = time.time() + logging_backoff
            logging_backoff = min(300, logging_backoff * 2)
        homeassistant_ws_client = None


def disable_event_session(owner):
    global event_session, homeassistant_ws_client
    with reconfiguration_lock:
        if event_session is owner:
            owner.retire()
            event_session = None
            homeassistant_ws_client = None
            create_ws_client(owner.bridge)


def create_ws_client(bridgeConfig):
    with reconfiguration_lock:
        return _create_ws_client(bridgeConfig)


def _create_ws_client(bridgeConfig):
    global event_session, homeassistant_ws_client
    if event_session is not None:
        previous = event_session
        previous.retire()
        event_session = None
    cfg = bridgeConfig["config"]["homeassistant"]
    if cfg.get("homeAssistantEventReflection", False):
        previous_legacy, homeassistant_ws_client = homeassistant_ws_client, None
        if previous_legacy is not None:
            # Close the transport first so a legacy close-frame write cannot block.
            previous_legacy.close_connection()
            try:
                previous_legacy.close()
            except Exception:
                pass
        from services.haEventClient import EventSession, make_client_class
        def included(row):
            if row is None or not row.get('entity_id', '').startswith(('light.', 'switch.')):
                return False
            flag = row.get('attributes', {}).get('diyhue')
            current_cfg = bridgeConfig['config']['homeassistant']
            return flag != 'exclude' if current_cfg.get('homeAssistantIncludeByDefault', False) else flag == 'include'
        event_session = EventSession(bridgeConfig, make_client_class(HomeAssistantClient), included)
        event_session.settings()
        event_session.start()
        return
    global homeassistant_token
    global homeassistant_url
    global include_by_default
    if 'homeAssistantIp' in bridgeConfig["config"]["homeassistant"]:
        homeassistant_ip = bridgeConfig["config"]["homeassistant"]['homeAssistantIp']
    if 'homeAssistantPort' in bridgeConfig["config"]["homeassistant"]:
        homeAssistant_port = bridgeConfig["config"]["homeassistant"]['homeAssistantPort']
    if 'homeAssistantToken' in bridgeConfig["config"]["homeassistant"]:
        homeassistant_token = bridgeConfig["config"]["homeassistant"]['homeAssistantToken']
    if 'homeAssistantIncludeByDefault' in bridgeConfig["config"]["homeassistant"]:
        include_by_default = bridgeConfig["config"]["homeassistant"]['homeAssistantIncludeByDefault']
    if 'homeAssistantUseHttps' in bridgeConfig["config"]["homeassistant"]:
        use_https = bridgeConfig["config"]["homeassistant"]['homeAssistantUseHttps']

    ws_prefix = "ws"
    if use_https:
        ws_prefix = "wss"

    homeassistant_url = f'{ws_prefix}://{homeassistant_ip}:{homeAssistant_port}/api/websocket'
    connect_if_required()


def discover(detectedLights):
    logging.info("HomeAssistant WebSocket discovery called")
    client = connect_and_authenticate()
    if client is None:
        logging.warning("HomeAssistant WebSocket discovery skipped, not connected or not authorised")
        return
    client.get_all_lights()
    logging.info("HomeAssistant WebSocket discovery waiting for devices")
    completed = (event_session.reducer.ready if event_session is not None else
                 discovery_result.wait(timeout=discovery_timeout_seconds))
    logging.info("HomeAssistant WebSocket discovery devices received, timeout? {}".format(
        (not completed)))
    # This only loops over discovered devices so we have already filtered out what we don't want
    with reconfiguration_lock:
        states = dict(latest_states)
    if event_session is not None:
        with event_session.reducer.lock:
            states = dict(event_session.reducer.rows)
    for entity_id in states.keys():
        ha_state = states[entity_id]
        device_new = True
        lightName = ha_state["attributes"]["friendly_name"] if "friendly_name" in ha_state["attributes"] else entity_id

        logging.info("HomeAssistant_ws: found light {}".format(lightName))
        # From Home Assistant lights/__init.py__
        UNKNOWN = "unknown"  # Ambiguous color mode
        ONOFF = "onoff"  # Must be the only supported mode
        BRIGHTNESS = "brightness"  # Must be the only supported mode
        COLOR_TEMP = "color_temp"
        HS = "hs"
        XY = "xy"
        RGB = "rgb"
        RGBW = "rgbw"
        RGBWW = "rgbww"
        WHITE = "white"  # Must *NOT* be the only supported mode

        supported_colourmodes = ha_state.get('attributes', {}).get('supported_color_modes', [])

        model_id = None
        if HS in supported_colourmodes or XY in supported_colourmodes or RGB in supported_colourmodes or RGBW in supported_colourmodes or RGBWW in supported_colourmodes and COLOR_TEMP in supported_colourmodes:
            model_id = "LCT015"
        elif COLOR_TEMP in supported_colourmodes:
            model_id = "LTW001"
        elif XY in supported_colourmodes:
            model_id = "LLC010"
        elif BRIGHTNESS in supported_colourmodes:
            model_id = "LWB010"
        elif ONOFF in supported_colourmodes and not BRIGHTNESS in supported_colourmodes:
            model_id = "LOM001"
        else:
            logging.info("unknown model id " + str(supported_colourmodes))
            continue

        protocol_cfg = {"entity_id": entity_id,
                        "ip": "none"}

        detectedLights.append({"protocol": "homeassistant_ws", "name": lightName, "modelid": model_id, "protocol_cfg": protocol_cfg})

    logging.info("HomeAssistant WebSocket discovery complete")
