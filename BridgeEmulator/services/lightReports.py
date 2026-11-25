"""Reflect a device report into Hue resources without sending a device command."""
from copy import deepcopy
from HueObjects import StreamEvent
from datetime import datetime, timezone
from uuid import uuid4


def apply_report(light, report, groups):
    if all(light.state.get(key) == value for key, value in report.items()) and not (
            report.get("on") is False and light.dynamics["status"] == "dynamic_palette"):
        return
    before = deepcopy(light.getV2Api())
    reachable = light.state.get("reachable")
    light.state.update(report)
    if report.get("on") is False:
        light.dynamics["status"] = "none"
    after = light.getV2Api()
    changes = {key: value for key, value in after.items() if before.get(key) != value}
    if changes:
        light.genStreamEvent(changes, groups=groups)
    if light.state.get("reachable") != reachable:
        StreamEvent({"creationtime": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                     "id": str(uuid4()), "type": "update", "data": [light.getZigBee()]})
