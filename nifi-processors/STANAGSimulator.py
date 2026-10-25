import sys
from pathlib import Path

# NiFi loads this file from python/extensions but runs the processor in an isolated
# venv; sibling modules are only importable if the extensions dir is on sys.path.
_EXTENSIONS_DIR = Path(__file__).resolve().parent
if _EXTENSIONS_DIR.as_posix() not in sys.path:
    sys.path.insert(0, _EXTENSIONS_DIR.as_posix())

from nifiapi.flowfiletransform import FlowFileTransform, FlowFileTransformResult
from nifiapi.properties import PropertyDescriptor, StandardValidators
import json
import random
import string
import datetime

from baltic_water_placement import sample_baltic_water_position
from marine_fleet_registry import GERMAN_NAVAL_FLEET, load_simulated_ship_positions

class StanagMessageGenerator(FlowFileTransform):
    """
    NiFi Python-Prozessor zum Generieren von STANAG-ähnlichen Nachrichten.
    """

    class Java:
        implements = ['org.apache.nifi.python.processor.FlowFileTransform']

    class ProcessorDetails:
        version = '1.6.0'
        description = (
            'STANAG-style naval surveillance reports; POSITION matches the '
            'MarineShipSimulator ship named in FROM (shared /tmp state).'
        )
        dependencies = ['numpy', 'pyproj>=3.6.0']

    # --- Property Descriptors ---
    NUM_MESSAGES = PropertyDescriptor(
        name="Number of Messages",
        description="The number of STANAG messages to generate per incoming FlowFile.",
        validators=[StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR],
        default_value="5",
        required=True
    )
    
    def __init__(self, **kwargs):
        kwargs.pop("jvm", None)
        super().__init__(**kwargs)
        # NEU: Die Eigenschaft für Naval Units wurde entfernt.
        self.descriptors = [self.NUM_MESSAGES]
        
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

    # Die Methode onPropertyModified() wurde entfernt, da keine dynamische Eigenschaft mehr vorhanden ist.

    def _format_position(self, lat: float, lon: float) -> str:
        return f"LAT {round(lat, 4)}°N, LON {round(lon, 4)}°E"

    def _pick_reporting_unit(self):
        """
        Prefer a ship currently simulated by MarineShipSimulator so FROM and POSITION align.
        """
        active = load_simulated_ship_positions()
        if active:
            ship = random.choice(active)
            return ship["name"], ship["lat"], ship["lon"]
        unit_data = random.choice(GERMAN_NAVAL_FLEET)
        lon, lat = sample_baltic_water_position()
        return unit_data["name"], lat, lon

    def _generate_stanag_message(self):
        unit_name, lat, lon = self._pick_reporting_unit()
        location = self._format_position(lat, lon)

        dtg = datetime.datetime.utcnow().strftime("%d%H%MZ%b%y").upper()
        
        subject = random.choice(self.subjects)

        if subject == "MARITIME SURVEILLANCE REPORT":
            activity = random.choice(self.activities)
            target = random.choice(self.targets)
            outcome = random.choice(self.outcomes)
            message_text = f"{unit_name} at {location} is {activity} {target}. Result: {outcome}."
            message_body = f"""POSITION/{location}\n\nTXT/{message_text}"""
        
        elif subject == "SICK REPORT":
            illness = random.choice(self.illnesses)
            message_text = f"SICK REPORT: ONE CREWMEMBER ONBOARD {unit_name} HAS BEEN DIAGNOSED WITH {illness}. CONDITION IS STABLE. REQUESTING MEDICAL ADVICE."
            message_body = f"""POSITION/{location}\n\nTXT/{message_text}"""

        else:
            message_text = f"UNKNOWN REPORT TYPE: No content generated."
            message_body = f"""TXT/{message_text}"""

        message = f"""
ZCZC
MSGID/SITREP/{unit_name.replace(" ", "")}/0001
DTG/{dtg}
FROM/{unit_name}
TO/NAVAL COMMAND
SUBJ/{subject}

{message_body}

NNNN
"""
        return message.strip()

    def transform(self, context, flowFile):
        try:
            num_messages_str = context.getProperty(self.NUM_MESSAGES.name).getValue()
            num_messages = int(num_messages_str) if num_messages_str.isdigit() else 5
            
            output_messages = []
            for _ in range(num_messages):
                output_messages.append(self._generate_stanag_message())
                
            output_content = "\n\n" + ("="*60) + "\n\n".join(output_messages)
            
            return FlowFileTransformResult(
                contents=output_content.encode("utf-8"),
                attributes={"format": "text/plain", "message.type": "stanag_report"},
                relationship="success"
            )

        except Exception as e:
            self.logger.error(f"Failed to generate STANAG messages: {e}")
            return FlowFileTransformResult(
                contents=f"Error generating STANAG messages: {e}".encode("utf-8"),
                attributes={"error": str(e)},
                relationship="failure"
            )
