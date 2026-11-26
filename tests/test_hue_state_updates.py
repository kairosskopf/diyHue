"""Exercise Hue notifications and HA recovery through the runtime entry points."""
import json
from concurrent.futures import ThreadPoolExecutor
import sys
import unittest
from types import SimpleNamespace
from unittest.mock import Mock, patch

from flask import Flask

sys.modules.setdefault("configManager", SimpleNamespace(bridgeConfig=SimpleNamespace(yaml_config={})))
import configManager
from flaskUI import v2restapi
from functions.colors import hsv_to_rgb
from HueObjects import StreamEvent, event_sequence, events_after, stream_events_after
from HueObjects.EntertainmentConfiguration import EntertainmentConfiguration
from HueObjects.Group import Group
from HueObjects.Light import Light
from HueObjects.Scene import Scene
from services import eventStreamer, homeAssistantWS as ha, stateFetch
from services.lightReports import apply_report


def lamp(number, protocol="dummy"):
    return Light({"id_v1": str(number), "name": f"Lamp {number}", "modelid": "LCT015",
                  "protocol": protocol, "protocol_cfg": {"entity_id": f"light.test{number}"}})


def row(value, second):
    return {"entity_id": "light.test1", "state": value, "attributes": {"diyhue": "include"},
            "last_updated": f"2026-01-01T00:00:{second:02d}+00:00"}


class HueStateUpdates(unittest.TestCase):
    def setUp(self):
        self.first = lamp(1, "homeassistant_ws")
        self.second = lamp(2)
        self.room = Group({"id_v1": "1", "type": "Room"})
        self.room.add_light(self.first)
        self.room.add_light(self.second)
        self.bridge = {"lights": {"1": self.first, "2": self.second}, "groups": {"1": self.room},
                       "apiUsers": {"key": SimpleNamespace()}}
        self.enterContext(patch.object(configManager.bridgeConfig, "yaml_config", self.bridge))
        # Production resolves the current configManager at callback time.
        self.enterContext(patch.dict(sys.modules, {"configManager": configManager}))
        with patch("ws4py.client.threadedclient.WebSocketClient.__init__", return_value=None):
            self.client = ha.HomeAssistantClient("ws://example.invalid")
        self.client._send = Mock()
        self.client.client_terminated = False
        self.enterContext(patch.object(ha, "homeassistant_ws_client", self.client))
        self.enterContext(patch.object(ha, "event_session", None))
        self.enterContext(patch.object(ha, "latest_states", {}))

    def change(self, value, second):
        self.client.do_state_changed({"event": {"data": {"entity_id": "light.test1",
                                                         "new_state": row(value, second)}}})

    def test_external_change_updates_light_and_room_and_skips_sibling(self):
        cursor = event_sequence()
        self.change("on", 4)
        events = [resource for _, event in events_after(cursor, 0) for resource in event["data"]]
        self.assertEqual([(r["type"], r["id"]) for r in events],
                         [("light", self.first.id_v2), ("device", self.first.getDevice()["id"]),
                          ("grouped_light", self.room.id_v2)])
        self.assertTrue(self.first.getV2Api()["on"]["on"])
        self.assertTrue(self.room.getV2GroupedLight()["on"]["on"])
        self.assertFalse(self.second.getV2Api()["on"]["on"])
        cursor = event_sequence()
        self.change("on", 5)
        self.assertEqual(events_after(cursor, 0), [])

    def brightness_events(self, cursor):
        return {resource["id"]: resource["dimming"]["brightness"]
                for _, event in events_after(cursor, 0) for resource in event["data"]
                if "dimming" in resource}

    def test_individual_brightness_command_updates_room_even_with_identical_echo(self):
        self.second.state["on"] = True
        self.second.protocol_cfg["max_bri"] = 127
        cursor = event_sequence()
        self.second.setV2State({"dimming": {"brightness": 90}})
        apply_report(self.second, {"bri": 127}, self.bridge["groups"])
        self.assertEqual(self.brightness_events(cursor),
                         {self.second.id_v2: 50, self.room.id_v2: 50})

    def test_ha_report_during_command_wins_even_if_brightness_was_unchanged(self):
        for original in (26, 255):
            initial = row("on", 1)
            initial["attributes"]["brightness"] = original
            ha.update_mapped_light(self.first, initial, self.bridge)
            report = row("on", 2)
            report["attributes"]["brightness"] = 255
            cursor = event_sequence()
            with ThreadPoolExecutor(max_workers=1) as worker:
                def confirm(*args):
                    event = {"event": {"data": {"entity_id": "light.test1", "new_state": report}}}
                    worker.submit(self.client.do_state_changed, event).result(timeout=2)
                with patch("lights.protocols.homeassistant_ws.set_light", side_effect=confirm):
                    self.first.setV2State({"dimming": {"brightness": 20}})
            self.assertEqual(self.first.state["bri"], 254)
            self.assertEqual(self.brightness_events(cursor)[self.first.id_v2], 100)

    def test_failed_single_mqtt_write_preserves_last_reported_room_and_lamp(self):
        self.second.protocol = "mqtt"
        self.second.state.update(on=True, bri=127)
        with patch("lights.protocols.mqtt.set_light", side_effect=ConnectionError):
            self.assertIs(self.second.setV2State({"dimming": {"brightness": 10}}), False)
        self.assertEqual(self.second.state["bri"], 127)
        self.assertEqual(self.room.getV2GroupedLight()["dimming"]["brightness"], 50)

    def test_partial_report_keeps_unreported_command_fields_and_survives_send_failure(self):
        self.second.protocol = "mqtt"
        self.second.state.update(on=False, bri=127)
        def report_during_send(*args):
            apply_report(self.second, {"bri": 254}, self.bridge["groups"])
        with patch("lights.protocols.mqtt.set_light", side_effect=report_during_send):
            self.second.setV1State({"on": True, "bri": 25})
        self.assertIs(self.second.state["on"], True)
        self.assertEqual(self.second.state["bri"], 254)
        def report_then_fail(*args):
            apply_report(self.second, {"bri": 50}, self.bridge["groups"])
            raise ConnectionError()
        with patch("lights.protocols.mqtt.set_light", side_effect=report_then_fail):
            self.assertIs(self.second.setV1State({"bri": 100}), False)
        self.assertEqual(self.second.state["bri"], 50)
        self.assertFalse(self.second.pending_fields)

    def test_v2_relative_dimming_for_lamp_and_room(self):
        self.first.state.update(on=True, bri=127)
        self.second.state.update(on=True, bri=200)
        self.second.protocol_cfg["max_bri"] = 210
        increase = {"dimming_delta": {"action": "up", "brightness_delta": 10}}
        with patch("lights.protocols.homeassistant_ws.set_light"):
            cursor = event_sequence()
            self.first.setV2State(increase)
            self.assertEqual(self.first.state["bri"], 152)
            self.assertEqual(self.brightness_events(cursor)[self.first.id_v2], round(152/2.54, 2))
            cursor = event_sequence()
            self.room.setV2Action(increase)
            self.assertEqual([self.first.state["bri"], self.second.state["bri"]], [177, 210])
            self.assertEqual(self.brightness_events(cursor)[self.room.id_v2],
                             self.room.getV2GroupedLight()["dimming"]["brightness"])
            self.room.setV2Action({"dimming_delta": {"action": "down", "brightness_delta": 100}})
            self.assertEqual([self.first.state["bri"], self.second.state["bri"]], [1, 1])

    def test_group_brightness_limit_is_per_lamp(self):
        self.first.protocol_cfg["max_bri"] = 127
        cursor = event_sequence()
        with patch("lights.protocols.homeassistant_ws.set_light"):
            self.room.setV2Action({"on": {"on": True}, "dimming": {"brightness": 100}})
        self.assertEqual([self.first.state["bri"], self.second.state["bri"]], [127, 254])
        self.assertEqual(self.brightness_events(cursor),
                         {self.first.id_v2: 50, self.second.id_v2: 100, self.room.id_v2: 75})

    def test_mqtt_scene_recall_publishes_brightness_before_device_echo(self):
        self.second.protocol = "mqtt"
        self.second.protocol_cfg.update(ip="broker", command_topic="z2m/lamp/set")
        scene = Scene({"id_v1": "4", "name": "Reading", "owner": None})
        scene.lightstates[self.second] = {"on": True, "bri": 127}
        cursor = event_sequence()
        with patch("lights.protocols.mqtt.set_light") as send:
            scene.activate({"recall": {"action": "active"}})
        send.assert_called_once()
        self.assertEqual(self.brightness_events(cursor),
                         {self.second.id_v2: 50, self.room.id_v2: 50})
        cursor = event_sequence()
        apply_report(self.second, {"on": True, "bri": 127}, self.bridge["groups"])
        self.assertEqual(events_after(cursor, 0), [])

    def test_mqtt_scene_sends_each_lamps_brightness_and_color_in_one_batch(self):
        scene = Scene({"id_v1": "4", "name": "Reading", "owner": None})
        broker = {"mqttUser": "", "mqttPassword": "", "mqttServer": "broker", "mqttPort": 1883}
        for light, hue, brightness in ((self.first, 0, 64), (self.second, 32000, 240)):
            light.protocol = "mqtt"
            light.protocol_cfg.update(ip="mqtt", command_topic=f"z2m/{light.id_v1}/set", mqtt_server=broker)
            light.state.update(hue=hue, sat=254)
            scene.lightstates[light] = {"on": True, "sat": 200, "bri": brightness}
        self.second.protocol_cfg["max_bri"] = 127
        with patch("lights.protocols.mqtt.publish.multiple") as send:
            scene.activate({"recall": {"action": "active"}})
        send.assert_called_once()
        payloads = {m["topic"]: json.loads(m["payload"]) for m in send.call_args.args[0]}
        for light, hue, brightness in ((self.first, 0, 64), (self.second, 32000, 127)):
            payload = payloads[light.protocol_cfg["command_topic"]]
            self.assertEqual(payload["brightness"], brightness)
            self.assertEqual(payload["color"], dict(zip(("r", "g", "b"), hsv_to_rgb(hue, 200, brightness))))
            self.assertEqual(light.state["bri"], brightness)
        self.assertEqual(scene.lightstates[self.second]["bri"], 240)

    def test_reconnect_with_legacy_full_brightness_is_within_hue_range(self):
        self.first.state.update(on=True, bri=255)
        self.second.state.update(on=True, bri=127)
        snapshot = eventStreamer.current_light_levels(self.bridge)
        levels = {r["id"]: r["dimming"]["brightness"] for r in snapshot["data"]}
        self.assertEqual(levels, {self.first.id_v2: 100, self.second.id_v2: 50, self.room.id_v2: 75})

    def test_room_with_one_lit_lamp_reports_its_exact_brightness(self):
        cursor = event_sequence()
        self.second.setV1State({"on": True, "bri": 1})
        brightness = self.second.getV2Api()["dimming"]["brightness"]
        self.assertEqual(self.room.getV2GroupedLight()["dimming"]["brightness"], brightness)
        self.assertEqual(self.brightness_events(cursor)[self.room.id_v2], brightness)

    def test_report_during_mqtt_group_write_wins_over_command(self):
        self.second.protocol = "mqtt"
        self.second.protocol_cfg.update(ip="broker", command_topic="z2m/lamp/set")
        cursor = event_sequence()
        def device_correction(*args):
            apply_report(self.second, {"bri": 127}, self.bridge["groups"])
        with patch("lights.protocols.homeassistant_ws.set_light"), \
                patch("lights.protocols.mqtt.set_light", side_effect=device_correction):
            self.room.setV2Action({"on": {"on": True}, "dimming": {"brightness": 100}})
        self.assertEqual(self.brightness_events(cursor),
                         {self.first.id_v2: 100, self.second.id_v2: 50, self.room.id_v2: 75})

    def test_failed_mqtt_scene_write_keeps_previous_brightness(self):
        self.second.protocol = "mqtt"
        self.second.protocol_cfg.update(ip="broker", command_topic="z2m/lamp/set")
        self.second.state.update(on=True, bri=127)
        scene = Scene({"id_v1": "4", "name": "Reading", "owner": None})
        scene.lightstates[self.second] = {"on": True, "bri": 254}
        cursor = event_sequence()
        with patch("lights.protocols.mqtt.set_light", side_effect=ConnectionError):
            scene.activate({"recall": {"action": "active"}})
        self.assertEqual(self.brightness_events(cursor), {self.second.id_v2: 50, self.room.id_v2: 50})
        self.assertEqual(self.second.state["bri"], 127)

    def test_home_assistant_maximum_brightness_round_trip(self):
        change = row("on", 4)
        change["attributes"]["brightness"] = 255
        cursor = event_sequence()
        self.client.do_state_changed({"event": {"data": {"entity_id": "light.test1", "new_state": change}}})
        self.assertEqual(self.brightness_events(cursor), {self.first.id_v2: 100, self.room.id_v2: 100})
        self.client.change_light(self.first, {"bri": self.first.state["bri"]})
        self.assertEqual(self.client._send.call_args.args[0]["service_data"]["brightness"], 255)

    def test_room_dimmer_steps_from_level_reported_by_home_assistant(self):
        change = row("on", 4)
        change["attributes"]["brightness"] = 51
        self.client.do_state_changed({"event": {"data": {"entity_id": "light.test1", "new_state": change}}})
        with patch("lights.protocols.homeassistant_ws.set_light"):
            self.room.setV1Action({"bri_inc": 30})
        self.assertEqual(self.first.state["bri"], 81)

    def test_grouped_light_owner_matches_events_in_rooms_and_zones(self):
        zone = Group({"id_v1": "2", "type": "Zone"})
        zone.add_light(self.second)
        self.bridge["groups"]["2"] = zone
        cursor = event_sequence()
        self.second.setV1State({"on": True, "bri": 127})
        owners = {r["id"]: r["owner"] for _, event in events_after(cursor, 0)
                  for r in event["data"] if r["type"] == "grouped_light"}
        for group, resource in ((self.room, self.room.getV2Room()), (zone, zone.getV2Zone())):
            expected = {"rid": resource["id"], "rtype": resource["type"]}
            self.assertEqual(owners[group.id_v2], expected)
            self.assertEqual(group.getV2GroupedLight()["owner"], expected)

    def test_failed_ha_command_preserves_on_state_and_reports_failure(self):
        with patch("lights.protocols.homeassistant_ws.set_light", side_effect=ConnectionError):
            self.assertIs(self.first.setV2State({"on": {"on": True}}), False)
        self.assertFalse(self.first.state["on"])
        self.assertFalse(self.first.state["reachable"])

    def test_snapshot_cannot_replace_a_newer_event(self):
        self.client.get_all_lights()
        self.change("on", 4)
        self.client.do_result({"id": 1, "success": True, "result": [row("off", 2)]})
        self.assertTrue(self.first.state["on"])
        self.client.get_all_lights()
        self.client.do_result({"id": 2, "success": True, "result": []})
        self.assertFalse(self.first.state["reachable"])

    def test_silent_connection_is_retired_and_replaced(self):
        self.client.client_terminated = False
        self.client.authenticated.set()
        self.client.snapshot_deadline = 100
        self.client.close_connection = Mock()
        replacement = Mock(client_terminated=False, snapshot_deadline=200)
        def reconnect():
            ha.homeassistant_ws_client = replacement
        with patch.object(ha.time, "monotonic", return_value=101), \
                patch.object(ha.time, "sleep", side_effect=[None, None, StopIteration]), \
                patch.object(ha, "create_websocket_client", side_effect=reconnect) as create:
            with self.assertRaises(StopIteration):
                ha.maintain_legacy_connection({"config": {"homeassistant": {"enabled": True}}})
        self.client.close_connection.assert_called_once()
        self.assertFalse(self.client.authenticated.is_set())
        create.assert_called_once()
        self.assertIs(ha.homeassistant_ws_client, replacement)

    def test_external_brightness_change_reaches_connected_and_reconnecting_apps(self):
        app = Flask(__name__)
        app.register_blueprint(eventStreamer.stream)
        client = app.test_client()
        self.enterContext(patch.object(v2restapi, "bridgeConfig", self.bridge))
        self.enterContext(patch.object(eventStreamer, "stream_events_after",
                          side_effect=lambda cursor: stream_events_after(cursor, timeout=0)))

        def connect(last=None):
            headers = {"hue-application-key": "key"}
            if last is not None:
                headers["Last-Event-ID"] = last
            response = client.get("/eventstream/clip/v2", headers=headers, buffered=False)
            self.addCleanup(response.close)
            frames = iter(response.response)
            self.assertIn(b": hi", next(frames))
            return frames

        def read_levels(frame):
            data = next((line[6:] for line in frame.decode().splitlines() if line.startswith("data: ")), "[]")
            return {r["id"]: r["dimming"]["brightness"] for event in json.loads(data)
                    for r in event["data"] if "dimming" in r}

        def ha_event(brightness, second):
            state = row("on", second)
            state["attributes"]["brightness"] = brightness
            event = {"type": "event", "event": {"event_type": "state_changed", "data": {
                "entity_id": "light.test1", "new_state": state}}}
            self.client.received_message(SimpleNamespace(data=json.dumps(event).encode(), encoding="utf-8"))

        ha_event(26, 1)
        live = connect(str(event_sequence()))
        ha_event(255, 2)  # The automation changes brightness while the light stays on.
        self.assertEqual(read_levels(next(live)), {self.first.id_v2: 100})
        next(live)  # device metadata
        self.assertEqual(read_levels(next(live)), {self.room.id_v2: 100})
        for checkpoint in (None, "0:999999"):
            levels = read_levels(next(connect(checkpoint)))
            self.assertEqual(levels.get(self.first.id_v2), 100)
            self.assertEqual(levels.get(self.room.id_v2), 100)

    def test_event_stream_clients_receive_identical_events_and_resume(self):
        app = Flask(__name__)
        app.register_blueprint(eventStreamer.stream)
        cursor = event_sequence()
        StreamEvent({"type": "update", "data": [{"id": "first"}]})
        StreamEvent({"type": "update", "data": [{"id": "second"}]})
        def read(last, count):
            with patch.object(v2restapi, "bridgeConfig", self.bridge):
                response = app.test_client().get("/eventstream/clip/v2",
                    headers={"hue-application-key": "key", "Last-Event-ID": str(last)}, buffered=False)
                try:
                    self.assertEqual(response.status_code, 200)
                    stream = iter(response.response)
                    self.assertIn(b": hi", next(stream))
                    messages = []
                    for _ in range(count):
                        seq, data = next(stream).decode().strip().splitlines()
                        messages.append((seq[4:], json.loads(data[6:])[0]["data"][0]["id"]))
                    return messages
                finally:
                    response.close()
        expected = read(cursor, 2)
        self.assertEqual([value for _, value in expected], ["first", "second"])
        self.assertRegex(expected[0][0], r"^[0-9]+:[0-9]+$")
        self.assertNotEqual(expected[0][0], expected[1][0])
        self.assertEqual(read(cursor, 2), expected)
        self.assertEqual(read(expected[0][0], 1), expected[1:])

    def test_light_added_during_poll_does_not_kill_worker(self):
        self.first.protocol = "native"
        def get_state(light):
            self.bridge["lights"]["3"] = lamp(3)
            return {"on": True}
        adapter = SimpleNamespace(__name__="lights.protocols.native", get_light_state=get_state)
        with patch.object(stateFetch, "bridgeConfig", self.bridge), \
                patch.object(stateFetch, "protocols", [adapter]), \
                patch.object(stateFetch, "sleep", side_effect=StopIteration):
            with self.assertRaises(StopIteration):
                stateFetch.syncWithLights(False)
        self.assertTrue(self.first.state["on"])

    def test_off_report_stops_running_palette(self):
        self.first.dynamics["status"] = "dynamic_palette"
        apply_report(self.first, {"on": False}, {})
        self.assertEqual(self.first.dynamics["status"], "none")

    def test_entertainment_positions_are_returned_and_saved(self):
        area = EntertainmentConfiguration({"id_v1": "3", "name": "TV"})
        point = {"x": -0.5, "y": 0.25, "z": 0.75}
        rid = self.first.getV2Entertainment()["id"]
        change = {"locations": {"service_locations": [{"service": {"rid": rid, "rtype": "entertainment"},
                                                       "positions": [point]}]}}
        with patch.object(v2restapi, "bridgeConfig", self.bridge):
            self.assertTrue(area.update_configuration(change, v2restapi.getObject))
        self.assertEqual(area.getV2Api()["channels"][0]["position"], point)
        self.assertEqual(area.save()["locations"]["1"], [point])
