from concurrent.futures import ThreadPoolExecutor
from threading import Event
import gc
import json
import sys
import unittest
from types import SimpleNamespace
from unittest.mock import Mock, patch

from flask import Flask
from flask_restful import Api

if not isinstance(sys.modules.get("configManager"), SimpleNamespace):
    sys.modules["configManager"] = SimpleNamespace(bridgeConfig=SimpleNamespace(yaml_config={}))
import logManager
logManager.logger.configure_logger("CRITICAL")
from flaskUI import restful, v2restapi
from HueObjects import event_sequence, events_after
from HueObjects.Group import Group
from HueObjects.Light import Light
from services import eventStreamer


class SceneEvents(unittest.TestCase):
    def setUp(self):
        self.lights = [Light({"id_v1": str(i), "name": f"Lamp {i}", "modelid": "LCT015"})
                       for i in (1, 2)]
        self.room = Group({"id_v1": "1", "type": "Room"})
        for light in self.lights:
            self.room.add_light(light)
        config = {"apiUsers": {"key": SimpleNamespace(username="key")}, "scenes": {}, "smart_scene": {},
                  "lights": {l.id_v1: l for l in self.lights}, "groups": {"1": self.room}, "sensors": {}}
        store = SimpleNamespace(bridgeConfig=SimpleNamespace(yaml_config=config, mark_dirty=Mock(), save_config=Mock()))
        self.scenes = config["scenes"]
        self.save_config = store.bridgeConfig.save_config
        for module in (restful, v2restapi):
            self.enterContext(patch.object(module, "bridgeConfig", config))
            self.enterContext(patch.object(module, "configManager", store))
        app = Flask(__name__)
        app.register_blueprint(eventStreamer.stream)
        api = Api(app)
        self.path = "/clip/v2/resource/scene"
        api.add_resource(v2restapi.ClipV2Resource, "/clip/v2/resource/<resource>")
        api.add_resource(v2restapi.ClipV2ResourceId, "/clip/v2/resource/<resource>/<resourceid>")
        api.add_resource(restful.ResourceElements, "/api/<username>/<resource>")
        api.add_resource(restful.Element, "/api/<username>/<resource>/<resourceid>")
        self.app = app
        self.client = app.test_client()
        self.client.environ_base["HTTP_HUE_APPLICATION_KEY"] = "key"
        self.payload = {"metadata": {"name": "Evening"},
                        "group": {"rid": self.room.getV2Room()["id"], "rtype": "room"},
                        "actions": [{"target": {"rid": self.lights[1].id_v2, "rtype": "light"},
                                     "action": {"on": {"on": True}}}]}

    def create(self):
        response = self.client.post(self.path, json=self.payload)
        self.assertEqual(response.status_code, 200)
        return self.path + "/" + response.json["data"][0]["rid"]

    def subscribe(self):
        # Keep the real Flask stream open across writes. A missing event returns
        # a keepalive immediately so regressions fail without hanging the tests.
        self.enterContext(patch.object(eventStreamer, "events_after",
                                      side_effect=lambda cursor: events_after(cursor, timeout=0)))
        response = self.client.get("/eventstream/clip/v2", buffered=False)
        self.addCleanup(response.close)
        frames = iter(response.response)
        self.assertIn(b": hi", next(frames))
        return frames

    def next_event(self, frames, kind):
        frame = next(frames).decode()
        self.assertIn("data: ", frame, f"Missing {kind} event")
        event, = json.loads(frame.split("data: ", 1)[1])
        self.assertEqual(event["type"], kind)
        resource, = event["data"]
        return resource

    def test_delete_reaches_open_client_while_another_reader_holds_the_scene(self):
        path = self.create()
        held = self.scenes["1"]
        self.assertEqual(self.client.get(path).json["data"][0]["id"], held.id_v2)
        frames = self.subscribe()
        response = self.client.delete(path)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(self.client.get(self.path).json["data"], [])
        self.assertEqual(self.next_event(frames, "delete"),
                         {"id": held.id_v2, "id_v1": "/scenes/1", "type": "scene"})
        self.assertEqual(self.client.get(path).json["data"], [])
        self.assertEqual(response.json["errors"], [])
        cursor = event_sequence()
        del held
        gc.collect()
        self.assertFalse(any(e["type"] == "delete" and r["type"] == "scene"
                             for _, e in events_after(cursor, 0) for r in e["data"]))

    def test_create_streams_only_requested_actions_and_matches_get(self):
        frames = self.subscribe()
        path = self.create()
        current, = self.client.get(path).json["data"]
        self.assertEqual(self.next_event(frames, "add"), current)
        self.assertEqual(current["actions"], self.payload["actions"])

    def test_edit_sends_changed_name_and_actions_to_open_client(self):
        path = self.create()
        frames = self.subscribe()
        edit = {"metadata": {"name": "Night"}, "actions": []}
        response = self.client.put(path, json=edit)
        self.assertEqual(response.status_code, 200)
        event = self.next_event(frames, "update")
        self.assertEqual(event, self.client.get(path).json["data"][0])
        self.assertEqual((event["metadata"]["name"], event["actions"]), ("Night", []))
        self.assertEqual(response.json["errors"], [])

    def test_overlapping_edit_and_delete_arrive_in_order(self):
        path = self.create()
        edited, release_save, deleting = Event(), Event(), Event()
        def pause_save(**kwargs):
            if not edited.is_set():
                edited.set()
                self.assertTrue(release_save.wait(2))
        def delete():
            deleting.set()
            return self.app.test_client().delete(path, headers={"hue-application-key": "key"})
        self.save_config.side_effect = pause_save
        cursor = event_sequence()
        with ThreadPoolExecutor(max_workers=2) as executor:
            edit = executor.submit(self.app.test_client().put, path, json={"metadata": {"name": "Night"}},
                                   headers={"hue-application-key": "key"})
            try:
                self.assertTrue(edited.wait(1))
                removal = executor.submit(delete)
                self.assertTrue(deleting.wait(1))
            finally:
                release_save.set()
            self.assertEqual((edit.result().status_code, removal.result().status_code), (200, 200))
        self.assertEqual([ev["type"] for _, ev in events_after(cursor, 0)], ["update", "delete"])
        self.assertEqual(self.client.get(path).json["data"], [])

    def test_image_only_edit_and_zero_speed_round_trip(self):
        path = self.create()
        frames = self.subscribe()
        image = {"rid": "public-image", "rtype": "public_image"}
        response = self.client.put(path, json={"metadata": {"image": image}, "speed": 0})
        self.assertEqual(response.status_code, 200)
        event = self.next_event(frames, "update")
        self.assertEqual(event["metadata"], {"name": "Evening", "image": image})
        self.assertEqual(self.scenes["1"].save()["speed"], 0)

    def test_v1_writes_notify_v2_clients_once_and_invalidate_scene_lookup(self):
        cursor = event_sequence()
        created = self.client.post("/api/key/scenes", json={"name": "Desk", "lights": ["1"]})
        key = created.json[0]["success"]["id"]
        held = self.scenes[key]
        path = self.path + "/" + held.id_v2
        self.assertEqual(self.client.get(path).json["data"][0]["metadata"]["name"], "Desk")
        self.assertEqual(self.client.delete("/api/key/scenes/" + key).status_code, 200)
        events = [(ev["type"], r) for _, ev in events_after(cursor, 0)
                  for r in ev["data"] if r.get("id") == held.id_v2]
        self.assertEqual([kind for kind, _ in events], ["add", "delete"])
        self.assertEqual(self.client.get(path).json["data"], [])
