import logManager
import configManager
from lights.protocols import protocols
from services.lightReports import apply_report
from time import sleep
from datetime import datetime, timedelta, timezone

logging = logManager.logger.get_logger(__name__)
bridgeConfig = configManager.bridgeConfig.yaml_config

def syncWithLights(off_if_unreachable): #update Hue Bridge lights states
    while True:
        logging.info("start lights sync")
        for key, light in list(bridgeConfig["lights"].items()):
            name = light.protocol
            # Both HA clients apply state reports directly from their WebSocket.
            if name in ("mqtt", "flex", "mi_box", "dummy", "homeassistant_ws"):
                continue
            for protocol in protocols:
                if protocol.__name__ != "lights.protocols." + name:
                    continue
                try:
                    report = dict(protocol.get_light_state(light))
                    report.setdefault("reachable", True)
                except Exception as error:
                    logging.warning("%s is unreachable: %s", light.name, error)
                    report = {"reachable": False}
                    if off_if_unreachable:
                        report["on"] = False
                if bridgeConfig["lights"].get(key) is light:
                    try:
                        apply_report(light, report, bridgeConfig["groups"])
                    except (TypeError, ValueError, KeyError, IndexError):
                        logging.exception("Invalid state for %s", light.name)
                break

        sleep(10) #wait at last 10 seconds before next sync
        i = 0
        while i < 300: #sync with lights every 300 seconds or instant if one user is connected
            for key, user in list(bridgeConfig["apiUsers"].items()):
                lu = user.last_use_date
                try: #in case if last use is not a proper datetime
                    lu = datetime.strptime(lu, "%Y-%m-%dT%H:%M:%S")
                    if abs(datetime.now(timezone.utc).replace(tzinfo=None) - lu) <= timedelta(seconds = 2):
                        i = 300
                        break
                except Exception as e:
                    logging.debug("Invalid last-use date %r: %s", lu, e)
            i += 1
            sleep(1)
