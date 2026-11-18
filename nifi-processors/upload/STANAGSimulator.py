import json
import random
import datetime
import os
import sys
from pathlib import Path

# NiFi loads this file from python/extensions but runs the processor in an isolated
# venv; sibling modules are only importable if the extensions dir is on sys.path.
_EXTENSIONS_DIR = Path(__file__).resolve().parent
if _EXTENSIONS_DIR.as_posix() not in sys.path:
    sys.path.insert(0, _EXTENSIONS_DIR.as_posix())

from nifiapi.flowfiletransform import FlowFileTransform, FlowFileTransformResult
from nifiapi.properties import PropertyDescriptor, StandardValidators

from marine_fleet_registry import (
    GERMAN_NAVAL_FLEET,
    MARINE_POSITION_DECIMALS,
    MARINESHIP_STATE_FILE,
    load_simulated_ship_positions,
    parse_marine_simulator_ndjson,
)

class StanagMessageGenerator(FlowFileTransform):
    """
    NiFi Python-Prozessor zum Generieren von STANAG-ähnlichen Nachrichten.
    """

    class Java:
        implements = ['org.apache.nifi.python.processor.FlowFileTransform']

    class ProcessorDetails:
        version = '1.7.0'
        description = (
            'STANAG-style naval surveillance reports. POSITION/LAT-LON matches '
            'MarineShipSimulator for the same MMSI (incoming NDJSON preferred, '
            'else /tmp/marineship_state.json). Wire MarineShipSimulator success '
            'into this processor instead of a separate GenerateFlowFile.'
        )
        dependencies = []

    NUM_MESSAGES = PropertyDescriptor(
        name="Number of Messages",
        description="The number of STANAG messages to generate per incoming FlowFile.",
        validators=[StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR],
        default_value="5",
        required=True
    )

    REQUIRE_MARINE_POSITIONS = PropertyDescriptor(
        name="Require Marine Ship Positions",
        description=(
            "When true, do not invent random Baltic positions; use only marine "
            "simulator data (FlowFile NDJSON or /tmp state). On failure, route to failure."
        ),
        allowable_values=["true", "false"],
        default_value="true",
        required=True,
    )

    PREFER_INCOMING_NDJSON = PropertyDescriptor(
        name="Prefer Incoming Marine NDJSON",
        description="When the FlowFile body is MarineShipSimulator NDJSON, use those positions.",
        allowable_values=["true", "false"],
        default_value="true",
        required=True,
    )
    
    def __init__(self, **kwargs):
        kwargs.pop("jvm", None)
        super().__init__(**kwargs)
        self.descriptors = [
            self.NUM_MESSAGES,
            self.REQUIRE_MARINE_POSITIONS,
            self.PREFER_INCOMING_NDJSON,
        ]
        
        self.activities = ["patrolling", "shadowing", "intercepting", "escorting", "monitoring"]
        self.targets = ["unidentified vessel", "fishing trawler", "merchant ship", "submarine contact", "fast attack craft"]
        self.outcomes = [
            "no hostile intent detected",
            "contact maintained",
            "vessel diverted",
            "boarding party deployed",
            "submarine submerged and lost contact"
        ]
        self.subjects = ["MARITIME SURVEILLANCE REPORT", "SICK REPORT"]
        self.illnesses = ["fever", "stomach flu", "respiratory infection", "sea sickness", "minor injury"]

    def getPropertyDescriptors(self):
        return self.descriptors

    def _format_position(self, lat: float, lon: float) -> str:
        lat_r = round(lat, MARINE_POSITION_DECIMALS)
        lon_r = round(lon, MARINE_POSITION_DECIMALS)
        return f"LAT {lat_r}°N, LON {lon_r}°E"

    def _resolve_reporting_ships(self, content: bytes, prefer_ndjson: bool) -> list[dict]:
        if prefer_ndjson and content:
            text = content.decode("utf-8", errors="replace")
            from_ndjson = parse_marine_simulator_ndjson(text)
            if from_ndjson:
                return from_ndjson
        from_state = load_simulated_ship_positions()
        if from_state:
            return from_state
        return []

    def _generate_stanag_message(self, unit_name: str, lat: float, lon: float, mmsi: str) -> str:
        location = self._format_position(lat, lon)
        dtg = datetime.datetime.utcnow().strftime("%d%H%MZ%b%y").upper()
        subject = random.choice(self.subjects)
        unit_key = unit_name.replace(" ", "")

        if subject == "MARITIME SURVEILLANCE REPORT":
            activity = random.choice(self.activities)
            target = random.choice(self.targets)
            outcome = random.choice(self.outcomes)
            message_text = f"{unit_name} at {location} is {activity} {target}. Result: {outcome}."
        elif subject == "SICK REPORT":
            illness = random.choice(self.illnesses)
            message_text = (
                f"SICK REPORT: ONE CREWMEMBER ONBOARD {unit_name} HAS BEEN DIAGNOSED WITH "
                f"{illness}. CONDITION IS STABLE. REQUESTING MEDICAL ADVICE."
            )
        else:
            message_text = "UNKNOWN REPORT TYPE: No content generated."

        return f"""ZCZC
MSGID/SITREP/{unit_key}/0001
DTG/{dtg}
FROM/{unit_name}
TO/NAVAL COMMAND
SUBJ/{subject}

POSITION/{location}

TXT/{message_text}

NNNN""".strip()

    def transform(self, context, flowFile):
        try:
            num_messages_str = context.getProperty(self.NUM_MESSAGES.name).getValue()
            num_messages = int(num_messages_str) if num_messages_str.isdigit() else 5
            require_marine = (
                context.getProperty(self.REQUIRE_MARINE_POSITIONS.name).getValue() == "true"
            )
            prefer_ndjson = (
                context.getProperty(self.PREFER_INCOMING_NDJSON.name).getValue() == "true"
            )

            content = flowFile.getContentsAsBytes()
            ships = self._resolve_reporting_ships(content, prefer_ndjson)

            if not ships:
                if require_marine:
                    hint = (
                        f"No marine positions (connect MarineShipSimulator success here or run "
                        f"{MARINESHIP_STATE_FILE} on this host)."
                    )
                    self.logger.error(hint)
                    return FlowFileTransformResult(
                        contents=hint.encode("utf-8"),
                        attributes={"error": "marine_positions_missing"},
                        relationship="failure",
                    )
                ships = [
                    {
                        "name": u["name"],
                        "mmsi": u["mmsi"],
                        "lat": 54.3233,
                        "lon": 10.1228,
                    }
                    for u in GERMAN_NAVAL_FLEET[: max(1, num_messages)]
                ]

            pool = list(ships)
            random.shuffle(pool)

            position_source = "registry_fallback"
            if prefer_ndjson and content:
                text = content.decode("utf-8", errors="replace")
                if parse_marine_simulator_ndjson(text):
                    position_source = "ndjson"
                elif os.path.exists(MARINESHIP_STATE_FILE):
                    position_source = "state_file"
            elif os.path.exists(MARINESHIP_STATE_FILE):
                position_source = "state_file"

            output_messages = []
            mmsi_list = []
            for i in range(num_messages):
                ship = pool[i % len(pool)]
                output_messages.append(
                    self._generate_stanag_message(
                        ship["name"], float(ship["lat"]), float(ship["lon"]), str(ship["mmsi"])
                    )
                )
                mmsi_list.append(str(ship["mmsi"]))

            output_content = ("\n\n" + ("=" * 60) + "\n\n").join(output_messages)

            attrs = {
                "format": "text/plain",
                "message.type": "stanag_report",
                "marine.position.source": position_source,
                "marine.mmsi.list": ",".join(mmsi_list),
            }

            return FlowFileTransformResult(
                contents=output_content.encode("utf-8"),
                attributes=attrs,
                relationship="success",
            )

        except Exception as e:
            self.logger.error(f"Failed to generate STANAG messages: {e}")
            return FlowFileTransformResult(
                contents=f"Error generating STANAG messages: {e}".encode("utf-8"),
                attributes={"error": str(e)},
                relationship="failure",
            )