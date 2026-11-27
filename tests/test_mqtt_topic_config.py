import json
from pathlib import Path
import sys
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, call, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "BridgeEmulator"))

# Import the real MQTT service without loading a user's bridge configuration or
# parsing the test runner's CLI arguments (same approach as the other tests).
# The guard keeps the shared configManager stub of other test modules intact:
# whichever module is imported first provides it.
if not isinstance(sys.modules.get("configManager"), SimpleNamespace):
    sys.modules["configManager"] = SimpleNamespace(bridgeConfig=SimpleNamespace(
        yaml_config={}, mark_dirty=Mock(), save_config=Mock()))

import logManager  # noqa: E402
logManager.logger.configure_logger("CRITICAL")
from services import mqtt  # noqa: E402


def device(model_id, friendly_name, ieee_address):
    return {
        "model_id": model_id,
        "friendly_name": friendly_name,
        "ieee_address": ieee_address,
        "definition": {"model": model_id},
    }


class MqttBaseTopicTests(unittest.TestCase):
    def setUp(self):
        self.config = {"config": {"mqtt": {"enabled": True}}, "sensors": {}, "lights": {}, "groups": {}}
        self.sensors = self.config["sensors"]
        # The service binds the bridge configuration at import time, so it has
        # to be pointed at this test's own configuration.
        self.bridge_config = patch.object(mqtt, "bridgeConfig", self.config)
        self.bridge_config.start()
        self.addCleanup(self.bridge_config.stop)

    def discover(self, topic, devices):
        msg = SimpleNamespace(topic=topic, payload=json.dumps(devices).encode())
        mqtt.on_message(None, None, msg)

    def sensor_names(self):
        return [sensor.name for sensor in self.sensors.values()]

    def test_default_base_topic_is_zigbee2mqtt(self):
        self.assertEqual(mqtt.getBaseTopic(), "zigbee2mqtt")

    def test_devices_on_default_topic_are_discovered(self):
        self.discover("zigbee2mqtt/bridge/devices", [
            device("RWL021", "hallway dimmer", "0x0017880100000002"),
        ])
        self.assertIn("hallway dimmer", self.sensor_names())

    def test_configured_base_topic_is_used_for_device_list(self):
        self.config["config"]["mqtt"]["baseTopic"] = "hue"
        self.assertEqual(mqtt.getBaseTopic(), "hue")
        self.discover("hue/bridge/devices", [
            device("RWL021", "hallway dimmer", "0x0017880100000002"),
        ])
        self.assertIn("hallway dimmer", self.sensor_names())

    def test_default_topic_is_ignored_when_base_topic_is_configured(self):
        self.config["config"]["mqtt"]["baseTopic"] = "hue"
        self.discover("zigbee2mqtt/bridge/devices", [
            device("RWL021", "hallway dimmer", "0x0017880100000002"),
        ])
        self.assertNotIn("hallway dimmer", self.sensor_names())

    def test_base_topic_is_stripped_of_slashes(self):
        self.config["config"]["mqtt"]["baseTopic"] = "/hue/"
        self.assertEqual(mqtt.getBaseTopic(), "hue")

    def test_worker_retries_from_startup_and_restores_configured_subscriptions(self):
        self.config["config"]["mqtt"].update(
            mqttServer="test-broker", mqttPort=8883, mqttUser="bridge", mqttPassword="test",
            discoveryPrefix="ha-lab", baseTopic="house/z2m", mqttTls=True,
            mqttCaCerts="test-ca.pem", mqttCertfile="test-client.pem", mqttKeyfile="test-client.key",
        )
        with patch.object(mqtt, "client", autospec=True) as client, \
                patch.object(mqtt, "discoveryPrefix", "homeassistant"):
            mqtt.mqttServer()
            self.assertEqual(client.method_calls, [
                call.username_pw_set("bridge", "test"),
                call.tls_set(ca_certs="test-ca.pem", certfile="test-client.pem",
                             keyfile="test-client.key", tls_version=mqtt.ssl.PROTOCOL_TLS),
                call.reconnect_delay_set(min_delay=1, max_delay=30),
                call.connect_async("test-broker", 8883),
                call.loop_forever(retry_first_connection=True),
            ])
            self.assertIs(client.on_message, mqtt.on_message)
            for _ in range(2):
                client.reset_mock()
                client.on_connect(client, None, {}, 0)
                self.assertCountEqual([c.args[0] for c in client.subscribe.call_args_list],
                                      ["ha-lab/light/+/light/config", "ha-lab/switch/+/+/config",
                                       "house/z2m/+", "house/z2m/bridge/devices", "house/z2m/bridge/log"])

    def test_subscriptions_use_configured_base_topic(self):
        self.config["config"]["mqtt"]["baseTopic"] = "hue"
        client = Mock()
        mqtt.on_connect(client, None, None, 0)
        topics = [call.args[0] for call in client.subscribe.call_args_list]
        self.assertIn("hue/+", topics)
        self.assertIn("hue/bridge/devices", topics)
        self.assertIn("hue/bridge/log", topics)
        self.assertNotIn("zigbee2mqtt/bridge/devices", topics)

    def test_dimmer_telemetry_does_not_replay_button_release(self):
        name = "office remote"
        topic = "zigbee2mqtt/" + name
        self.discover("zigbee2mqtt/bridge/devices", [
            device("RWL021", name, "0x0017880100000011"),
        ])
        sensor = mqtt.getObject(name)
        with patch.object(mqtt, "rulesProcessor") as rules, patch.object(mqtt, "checkBehaviorInstances") as behaviors:
            self.discover(topic, {"action": "on_press_release"})
            self.assertEqual(sensor.state["buttonevent"], 1002)
            for payload in ({"battery": 87, "linkquality": 156}, {"action": "unknown"}, {"action": ""}):
                self.discover(topic, payload)
            self.assertEqual(sensor.config["battery"], 87)
            rules.assert_called_once()
            behaviors.assert_called_once_with(sensor)
            self.discover(topic, {"action": "on_press_release"})
            self.assertEqual(behaviors.call_count, 2)

    def test_dimmer_discovery_without_lamps_or_plugs(self):
        self.config["config"]["mqtt"]["discoverLights"] = False
        client = Mock()
        mqtt.on_connect(client, None, None, 0)
        topics = [call.args[0] for call in client.subscribe.call_args_list]
        self.assertCountEqual(topics, ["zigbee2mqtt/+", "zigbee2mqtt/bridge/devices",
                                       "zigbee2mqtt/bridge/log"])
        with patch.object(mqtt, "on_autodiscovery_light") as import_light:
            self.discover("homeassistant/light/bulb/light/config", {})
            self.discover("homeassistant/switch/plug/switch/config", {})
            import_light.assert_not_called()
        self.discover("zigbee2mqtt/bridge/devices", [
            device("RWL021", "office dimmer", "0x0017880100000011"),
        ])
        self.assertIn("office dimmer", self.sensor_names())

    def test_nested_device_and_base_topics_deliver_state_but_ignore_commands(self):
        from paho.mqtt.client import topic_matches_sub
        from HueObjects.Light import Light
        for base in ("zigbee2mqtt", "home/z2m"):
            for friendly in ("lamp", "office/lamp"):
                with self.subTest(base=base, friendly=friendly), patch.object(mqtt, "devices_ids", {}):
                    topic = base + "/" + friendly
                    light = Light({"id_v1": "1", "modelid": "LCT015", "name": friendly,
                                   "protocol": "mqtt", "protocol_cfg": {"state_topic": topic}})
                    self.config["config"]["mqtt"]["baseTopic"] = base
                    self.config["lights"]["1"] = light
                    client = Mock()
                    mqtt.on_connect(client, None, None, 0)
                    topics = [c.args[0] for c in client.subscribe.call_args_list]
                    self.assertTrue(any(topic_matches_sub(t, topic) for t in topics))
                    self.discover(topic, {"state": "ON", "brightness": 254})
                    self.discover(topic + "/set", {"brightness": 10})
                    self.assertEqual(light.state["bri"], 254)

    def test_discovery_subscribes_to_a_renamed_nested_topic_before_reconnect(self):
        from HueObjects.Light import Light
        broker = Mock()
        old = "zigbee2mqtt/old/desk"
        new = "zigbee2mqtt/office/desk"
        light = Light({"id_v1": "1", "modelid": "LCT015", "name": "desk", "protocol": "mqtt",
                       "protocol_cfg": {"uid": "desk-1", "state_topic": old}})
        self.config["lights"]["1"] = light
        data = {"unique_id": "desk-1", "state_topic": new, "command_topic": new + "/set"}
        msg = SimpleNamespace(topic="homeassistant/light/desk/light/config", payload=json.dumps(data).encode())
        with patch.object(mqtt, "devices_ids", {}), patch.object(mqtt, "discoveredDevices", {}):
            mqtt.on_connect(broker, None, None, 0)
            self.discover(old, {"brightness": 25})
            mqtt.on_message(broker, None, msg)
            broker.unsubscribe.assert_called_with(old)
            broker.subscribe.assert_any_call(new)
            self.discover(new, {"brightness": 254})
            self.discover(old, {"brightness": 25})
            self.assertEqual(light.state["bri"], 254)

    def test_light_discovery_publishes_to_configured_base_topic(self):
        from lights.protocols import mqtt as light_mqtt
        mqtt_config = {
            "enabled": True,
            "mqttUser": "",
            "mqttPassword": "",
            "mqttServer": "127.0.0.1",
            "mqttPort": 1883,
            "baseTopic": "hue",
        }
        with patch.object(light_mqtt.publish, "single") as publish_single:
            light_mqtt.discover(mqtt_config)
        published = [call.args[0] for call in publish_single.call_args_list]
        self.assertIn("hue/bridge/request/permit_join", published)
        self.assertIn("hue/bridge/config/devices/get", published)


if __name__ == "__main__":
    unittest.main()
