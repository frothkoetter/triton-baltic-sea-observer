from nifiapi.flowfiletransform import FlowFileTransform, FlowFileTransformResult
from nifiapi.properties import PropertyDescriptor, StandardValidators
import json
import random
import string
import datetime

class StanagMessageGenerator(FlowFileTransform):
    """
    NiFi Python-Prozessor zum Generieren von STANAG-ähnlichen Nachrichten.
    """

    class Java:
        implements = ['org.apache.nifi.python.processor.FlowFileTransform']

    class ProcessorDetails:
        version = '1.5.1'
        description = 'Generates simulated STANAG-style naval surveillance reports and other report types with units randomly selected from the entire German Navy fleet list.'
        dependencies = []

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
        
        # Vollständige Liste der Schiffe mit allen Details
        # HINWEIS: MMSI-Nummern in diesem Beispiel sind fiktiv.
        self.full_fleet = [
            {'class': 'Fregatten', 'name': 'FGS Brandenburg', 'mmsi': 'MAR123400'},
            {'class': 'Fregatten', 'name': 'FGS Schleswig-Holstein', 'mmsi': 'MAR123401'},
            {'class': 'Fregatten', 'name': 'FGS Bayern', 'mmsi': 'MAR123402'},
            {'class': 'Fregatten', 'name': 'FGS Mecklenburg-Vorpommern', 'mmsi': 'MAR123403'},
            {'class': 'Fregatten', 'name': 'FGS Sachsen', 'mmsi': 'MAR123404'},
            {'class': 'Fregatten', 'name': 'FGS Hamburg', 'mmsi': 'MAR123405'},
            {'class': 'Fregatten', 'name': 'FGS Hessen', 'mmsi': 'MAR123406'},
            {'class': 'Fregatten', 'name': 'FGS Baden-Württemberg', 'mmsi': 'MAR123407'},
            {'class': 'Fregatten', 'name': 'FGS Nordrhein-Westfalen', 'mmsi': 'MAR123408'},
            {'class': 'Fregatten', 'name': 'FGS Sachsen-Anhalt', 'mmsi': 'MAR123409'},
            {'class': 'Fregatten', 'name': 'FGS Rheinland-Pfalz', 'mmsi': 'MAR123410'},
            {'class': 'Korvetten', 'name': 'FGS Braunschweig', 'mmsi': 'MAR123411'},
            {'class': 'Korvetten', 'name': 'FGS Magdeburg', 'mmsi': 'MAR123412'},
            {'class': 'Korvetten', 'name': 'FGS Erfurt', 'mmsi': 'MAR123413'},
            {'class': 'Korvetten', 'name': 'FGS Oldenburg', 'mmsi': 'MAR123414'},
            {'class': 'Korvetten', 'name': 'FGS Ludwigshafen am Rhein', 'mmsi': 'MAR123415'},
            {'class': 'U-Boote', 'name': 'U-31', 'mmsi': 'MAR123416'},
            {'class': 'U-Boote', 'name': 'U-32', 'mmsi': 'MAR123417'},
            {'class': 'U-Boote', 'name': 'U-33', 'mmsi': 'MAR123418'},
            {'class': 'U-Boote', 'name': 'U-34', 'mmsi': 'MAR123419'},
            {'class': 'U-Boote', 'name': 'U-35', 'mmsi': 'MAR123420'},
            {'class': 'U-Boote', 'name': 'U-36', 'mmsi': 'MAR123421'},
            {'class': 'Minenabwehreinheiten', 'name': 'FGS Fulda', 'mmsi': 'MAR123422'},
            {'class': 'Minenabwehreinheiten', 'name': 'FGS Weilheim', 'mmsi': 'MAR123423'},
            {'class': 'Minenabwehreinheiten', 'name': 'FGS Sulzbach-Rosenberg', 'mmsi': 'MAR123424'},
            {'class': 'Minenabwehreinheiten', 'name': 'FGS Dillingen', 'mmsi': 'MAR123425'},
            {'class': 'Minenabwehreinheiten', 'name': 'FGS Homburg', 'mmsi': 'MAR123426'},
            {'class': 'Minenabwehreinheiten', 'name': 'FGS Siegburg', 'mmsi': 'MAR123427'},
            {'class': 'Minenabwehreinheiten', 'name': 'FGS Auerbach/Oberpfalz', 'mmsi': 'MAR123428'},
            {'class': 'Minenabwehreinheiten', 'name': 'FGS Pegnitz', 'mmsi': 'MAR123429'},
            {'class': 'Minenabwehreinheiten', 'name': 'FGS Passau', 'mmsi': 'MAR123430'},
            {'class': 'Minenabwehreinheiten', 'name': 'FGS Weiden', 'mmsi': 'MAR123431'},
            {'class': 'Minenabwehreinheiten', 'name': 'FGS Ensdorf', 'mmsi': 'MAR123432'},
            {'class': 'Minenabwehreinheiten', 'name': 'FGS Kühlungsborn', 'mmsi': 'MAR123433'},
            {'class': 'Flottendienstboote', 'name': 'A52 Oste', 'mmsi': 'MAR123434'},
            {'class': 'Flottendienstboote', 'name': 'A50 Oker', 'mmsi': 'MAR123435'},
            {'class': 'Flottendienstboote', 'name': 'A53 Alster', 'mmsi': 'MAR123436'},
            {'class': 'Versorgungsschiffe/Tender', 'name': 'A1411 Berlin', 'mmsi': 'MAR123437'},
            {'class': 'Versorgungsschiffe/Tender', 'name': 'A1412 Frankfurt am Main', 'mmsi': 'MAR123438'},
            {'class': 'Versorgungsschiffe/Tender', 'name': 'A1413 Bonn', 'mmsi': 'MAR123439'},
            {'class': 'Versorgungsschiffe/Tender', 'name': 'A511 Elbe', 'mmsi': 'MAR123440'},
            {'class': 'Versorgungsschiffe/Tender', 'name': 'A512 Mosel', 'mmsi': 'MAR123441'},
            {'class': 'Versorgungsschiffe/Tender', 'name': 'A513 Rhein', 'mmsi': 'MAR123442'},
            {'class': 'Versorgungsschiffe/Tender', 'name': 'A514 Werra', 'mmsi': 'MAR123443'},
            {'class': 'Versorgungsschiffe/Tender', 'name': 'A515 Main', 'mmsi': 'MAR123444'},
            {'class': 'Versorgungsschiffe/Tender', 'name': 'A516 Donau', 'mmsi': 'MAR123445'},
            {'class': 'Versorgungsschiffe/Tender', 'name': 'A1442 Spessart', 'mmsi': 'MAR123446'},
            {'class': 'Versorgungsschiffe/Tender', 'name': 'A1443 Rhön', 'mmsi': 'MAR123447'},
            {'class': 'Sonstige', 'name': 'FGS Gorch Fock', 'mmsi': 'MAR123448'},
            {'class': 'Sonstige', 'name': 'FGS Planet', 'mmsi': 'MAR123449'},
            {'class': 'Sonstige', 'name': 'FGS Wangerooge', 'mmsi': 'MAR123450'},
            {'class': 'Sonstige', 'name': 'FGS Baltrum', 'mmsi': 'MAR123451'},
            {'class': 'Sonstige', 'name': 'FGS Borkum', 'mmsi': 'MAR123452'},
            {'class': 'Sonstige', 'name': 'FGS Norderney', 'mmsi': 'MAR123453'},
            {'class': 'Sonstige', 'name': 'FGS Juist', 'mmsi': 'MAR123454'},
            {'class': 'Sonstige', 'name': 'FGS Langeoog', 'mmsi': 'MAR123455'}
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

    # Die Methode onPropertyModified() wurde entfernt, da keine dynamische Eigenschaft mehr vorhanden ist.

    def _generate_baltic_coordinates(self):
        """Generiert zufällige Breitengrad- und Längengrad-Koordinaten in der Ostseeregion."""
        min_lat, max_lat = 53.5, 65.8
        min_lon, max_lon = 9.5, 30.2
        
        latitude = round(random.uniform(min_lat, max_lat), 4)
        longitude = round(random.uniform(min_lon, max_lon), 4)
        
        return f"LAT {latitude}°N, LON {longitude}°E"

    def _generate_stanag_message(self):
        # NEU: Wähle eine Einheit zufällig aus der gesamten Flottenliste
        unit_data = random.choice(self.full_fleet)
        unit_name = unit_data['name']
        
        dtg = datetime.datetime.utcnow().strftime("%d%H%MZ%b%y").upper()
        
        subject = random.choice(self.subjects)

        if subject == "MARITIME SURVEILLANCE REPORT":
            location = self._generate_baltic_coordinates()
            activity = random.choice(self.activities)
            target = random.choice(self.targets)
            outcome = random.choice(self.outcomes)
            message_text = f"{unit_name} at {location} is {activity} {target}. Result: {outcome}."
            message_body = f"""POSITION/{location}\n\nTXT/{message_text}"""
        
        elif subject == "SICK REPORT":
            location = self._generate_baltic_coordinates()
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
