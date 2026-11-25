"""Exercise Hue notifications and HA recovery through the runtime entry points."""
import json
import sys
import unittest
from types import SimpleNamespace
from unittest.mock import Mock, patch

from flask import Flask

sys.modules.setdefault("configManager", SimpleNamespace(bridgeConfig=SimpleNamespace(yaml_config={})))
import configManager
from flaskUI import v2restapi
from HueObjects import StreamEvent, event_sequence, events_after
from HueObjects.EntertainmentConfiguration import EntertainmentConfiguration
from HueObjects.Group import Group
from HueObjects.Light import Light
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
