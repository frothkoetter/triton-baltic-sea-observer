from nifiapi.flowfiletransform import FlowFileTransform, FlowFileTransformResult
from nifiapi.properties import PropertyDescriptor, StandardValidators
import json
import random
import string
from datetime import datetime, timezone
from faker import Faker # Importiert die Faker-Bibliothek

# --- Liste der NATO Marine Häfen aus der Quelle [12-14] ---
HARBORS = [
    {"name": "Kiel", "Latitude": 54.3233, "Longitude": 10.1228},
    {"name": "Rostock", "Latitude": 54.0887, "Longitude": 12.1405},
    {"name": "Karlskrona", "Latitude": 56.1612, "Longitude": 15.5869},
    {"name": "Gdynia", "Latitude": 54.5189, "Longitude": 18.5305},
    {"name": "Świnoujście", "Latitude": 53.9106, "Longitude": 14.2478},
    {"name": "Klaipėda", "Latitude": 55.7033, "Longitude": 21.1443},
    {"name": "Riga", "Latitude": 56.9496, "Longitude": 24.1052},
    {"name": "Tallinn", "Latitude": 59.4370, "Longitude": 24.7536},
    {"name": "Helsinki", "Latitude": 60.1695, "Longitude": 24.9354},
    {"name": "Rønne", "Latitude": 55.1037, "Longitude": 14.7065},
    {"name": "Stockholm", "Latitude": 59.3293, "Longitude": 18.0686},
    {"name": "Turku", "Latitude": 60.4518, "Longitude": 22.2666},
    {"name": "Paldiski", "Latitude": 59.3567, "Longitude": 24.0539},
    {"name": "Liepāja", "Latitude": 56.5110, "Longitude": 21.0136},
    {"name": "Ventspils", "Latitude": 57.3890, "Longitude": 21.5610},
    {"name": "Wismar", "Latitude": 53.8934, "Longitude": 11.4536},
    {"name": "Stralsund", "Latitude": 54.3091, "Longitude": 13.0810},
    {"name": "Sassnitz", "Latitude": 54.5183, "Longitude": 13.6414},
    {"name": "Gdańsk", "Latitude": 54.3520, "Longitude": 18.6466},
    {"name": "Ustka", "Latitude": 54.5801, "Longitude": 16.8596}
]

# --- Beispielhafte Objekttypen und deren Merkmale aus der Quelle [1-10] ---
# Die Lambdas ermöglichen die Generierung von zufälligen Werten im spezifizierten Bereich
OBJECT_TYPES_EXAMPLES = [
    # Beispiel 1: Großes U-Boot (SSK)
    {
        "type": "SUBMARINE",
        "classification": "LARGE_DIESEL_ELECTRIC",
        "confidence": lambda: random.randint(85, 98),
        "estimatedDepth": lambda: random.randint(20, 50),
        "motion": "MOVING",
        "extent": None,
        "notes": None,
        "orientation": None,
        "correlationId": None,
        "gradient": "VERY_STEEP",
        "detectionConfidence": "VERY_HIGH"
    },
    # Beispiel 2: Kleines U-Boot (midget sub)
    {
        "type": "SUBMARINE",
        "classification": "MIDGET_SUBMARINE",
        "confidence": lambda: random.randint(75, 90),
        "estimatedDepth": lambda: random.randint(10, 30),
        "motion": "MOVING",
        "extent": None,
        "notes": None,
        "orientation": None,
        "correlationId": None,
        "gradient": "MODERATE",
        "detectionConfidence": "HIGH"
    },
    # Beispiel 3: U-Boot in großer Tiefe (System unsicher)
    {
        "type": "SUBMARINE",
        "classification": "POSSIBLE_SUBMARINE",
        "confidence": lambda: random.randint(50, 70),
        "estimatedDepth": lambda: random.randint(150, 300),
        "motion": "MOVING",
        "extent": None,
        "notes": None,
        "orientation": None,
        "correlationId": None,
        "gradient": "SHALLOW",
        "detectionConfidence": "MEDIUM"
    },
    # Beispiel 4: Wrack eines großen Schiffs
    {
        "type": "SURFACE_VESSEL",
        "classification": "SHIPWRECK",
        "confidence": lambda: random.randint(90, 99),
        "estimatedDepth": lambda: random.randint(70, 100),
        "motion": "FIXED",
        "extent": None,
        "notes": None,
        "orientation": None,
        "correlationId": None,
        "gradient": "STEADY",
        "detectionConfidence": "HIGH"
    },
    # Beispiel 5: Kein Kontakt (Hintergrundrauschen)
    {
        "type": "NATURAL_PHENOMENON",
        "classification": "BACKGROUND_NOISE",
        "confidence": lambda: random.randint(95, 100),
        "estimatedDepth": None,
        "motion": "FIXED",
        "extent": None,
        "notes": "No significant contact",
        "orientation": None,
        "correlationId": None,
        "gradient": "FLAT",
        "detectionConfidence": "LOW"
    },
    # Beispiel 6: Magnetit-Gesteinsformation
    {
        "type": "GEOLOGICAL",
        "classification": "MAGNETIC_ORE_DEPOSIT",
        "confidence": lambda: random.randint(80, 95),
        "estimatedDepth": None,
        "motion": "FIXED",
        "extent": "LARGE_AREA",
        "notes": None,
        "orientation": None,
        "correlationId": None,
        "gradient": "BROAD",
        "detectionConfidence": "HIGH"
    },
    # Beispiel 7: Unterwasserkabel oder Pipeline
    {
        "type": "MANMADE_STRUCTURE",
        "classification": "PIPELINE_OR_CABLE",
        "confidence": lambda: random.randint(80, 95),
        "estimatedDepth": None,
        "motion": "FIXED",
        "extent": "LINEAR",
        "notes": None,
        "orientation": lambda: random.randint(0, 359),
        "correlationId": None,
        "gradient": "LINEAR",
        "detectionConfidence": "HIGH"
    },
    # Beispiel 8: Torpedo in Bewegung
    {
        "type": "ORDNANCE",
        "classification": "POSSIBLE_TORPEDO",
        "confidence": lambda: random.randint(30, 60),
        "estimatedDepth": lambda: random.randint(10, 50),
        "motion": "FAST_MOVING",
        "extent": None,
        "notes": None,
        "orientation": lambda: random.randint(0, 359),
        "correlationId": None,
        "gradient": "VERY_STEEP",
        "detectionConfidence": "MEDIUM"
    },
    # Beispiel 9: Meeressäuger-Schwarm (False Positive)
    {
        "type": "BIOLOGICAL",
        "classification": "MARINE_FAUNA_SWARM",
        "confidence": lambda: random.randint(65, 85),
        "estimatedDepth": lambda: random.randint(5, 40),
        "motion": "ERRATIC",
        "extent": "WIDE",
        "notes": "Signal pattern inconsistent with man-made object",
        "orientation": None,
        "correlationId": None,
        "gradient": "ERRATIC",
        "detectionConfidence": "LOW"
    },
    # Beispiel 10: Bestätigungsmessung nach Sonarkontakt
    {
        "type": "SUBMARINE",
        "classification": "CONFIRMED_SUBMARINE",
        "confidence": lambda: random.randint(95, 99),
        "estimatedDepth": lambda: random.randint(10, 25),
        "motion": "MOVING",
        "extent": None,
        "notes": None,
        "orientation": None,
        "correlationId": lambda: f"SONAR_CONTACT_ALPHA_{random.randint(1, 99)}",
        "gradient": "VERY_STEEP",
        "detectionConfidence": "VERY_HIGH"
    }
]


class BuoySensorSimulator(FlowFileTransform):
    """
    Ein NiFi-Python-Prozessor, der MAD-Bojen-Sensordaten im JSON-Format generiert.
    Die generierten Daten enthalten geografische Positionen in der Nähe von NATO-Häfen
    und klassifizieren erkannte Objekte basierend auf den bereitgestellten Beispielen.
    """

    class Java:
        implements = ['org.apache.nifi.python.processor.FlowFileTransform']

    class ProcessorDetails:
        version = '1.2.0'
        description = 'Generiert simulierte MAD-Bojen-Sensordaten mit Objekterkennung und Positionen nahe NATO-Häfen. Das Ausgabeformat ist eine flache JSON-Struktur.'
        dependencies = ['Faker==25.0.0']

    # Definiert eine Eigenschaft, um die Anzahl der zu generierenden Datensätze festzulegen.
    NUM_RECORDS = PropertyDescriptor(
        name="Number of Records per FlowFile",
        description="Die Anzahl der simulierten MAD-Bojen-Datensätze, die pro eingehendem FlowFile generiert werden sollen.",
        validators=[StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR],
        default_value="1", # Standardwert für die Anzahl der Datensätze
        required=False
    )

    def __init__(self, **kwargs):
        kwargs.pop("jvm", None)
        super().__init__(**kwargs)
        self.descriptors = [self.NUM_RECORDS]
        self.fake = Faker()
        self.buoy_counter = 0

    def getPropertyDescriptors(self):
        return self.descriptors

    def _generate_mad_buoy_id(self):
        """Generiert eine eindeutige Bojen-ID im Format MAD-XX."""
        self.buoy_counter += 1
        return f"MAD-{self.buoy_counter:02d}"

    def _generate_timestamp(self):
        """Generiert einen aktuellen Zeitstempel im ISO-8601-Format (UTC)."""
        return datetime.now(timezone.utc).isoformat(timespec='milliseconds')[:-6] + 'Z'

    def _generate_position_near_nato_harbor(self):
        """
        Wählt einen zufälligen NATO-Hafen aus der HARBORS-Liste und generiert
        eine Position in dessen geografischer Nähe.
        """
        harbor = random.choice(HARBORS)
        base_lat = harbor["Latitude"]
        base_lon = harbor["Longitude"]

        offset_lat = random.uniform(-0.1, 0.1)
        offset_lon = random.uniform(-0.1, 0.1)

        latitude = round(base_lat + offset_lat, 4)
        longitude = round(base_lon + offset_lon, 4)

        return {"lat": latitude, "lon": longitude}

    def _generate_altitude(self):
        """Generiert eine zufällige Flughöhe der Boje in Metern."""
        return random.randint(50, 200)

    def _generate_magnetic_field_data(self, obj_example):
        """
        Generiert magnetische Felddaten basierend auf dem gewählten Objekttyp-Beispiel.
        """
        total_field = round(random.uniform(49900.0, 50300.0), 1)
        anomaly = round(random.uniform(-300.0, 300.0), 1)

        gradient_options = ["VERY_STEEP", "MODERATE", "SHALLOW", "STEADY", "FLAT", "BROAD", "LINEAR", "ERRATIC"]
        gradient = obj_example.get("gradient", random.choice(gradient_options))

        if obj_example["type"] == "SUBMARINE":
            anomaly = round(random.uniform(-200.0, -10.0), 1)
        elif obj_example["type"] == "GEOLOGICAL":
            anomaly = round(random.uniform(100.0, 300.0), 1)
        elif obj_example["type"] == "NATURAL_PHENOMENON":
            anomaly = round(random.uniform(-5.0, 5.0), 1)

        return {
            "totalField": total_field,
            "anomaly": anomaly,
            "gradient": gradient
        }

    def _generate_detection_confidence(self, obj_example):
        """
        Generiert die Detektions-Konfidenz, passend zum Objekttyp-Beispiel.
        """
        confidence_options = ["VERY_HIGH", "HIGH", "MEDIUM", "LOW"]
        return obj_example.get("detectionConfidence", random.choice(confidence_options))

    def _generate_object_payload(self):
        """
        Generiert das 'object'-Feld innerhalb des 'payload'.
        """
        obj_example = random.choice(OBJECT_TYPES_EXAMPLES)
        obj_data = {}

        # Füllt obj_data mit Werten aus dem ausgewählten Beispiel
        for key, value in obj_example.items():
            if callable(value):
                obj_data[key] = value()
            else:
                obj_data[key] = value

        return obj_data, obj_example

    def transform(self, context, flowFile):
        """
        Die Hauptmethode des NiFi-Prozessors. Sie ignoriert das eingehende FlowFile
        und generiert eine konfigurierbare Anzahl von neuen MAD-Bojen-Datensätzen.
        """
        try:
            output_records = []

            num_records_str = context.getProperty(self.NUM_RECORDS.name).getValue()
            num_records = int(num_records_str) if num_records_str and num_records_str.isdigit() else 1

            for _ in range(num_records):
                buoy_id = self._generate_mad_buoy_id()
                timestamp = self._generate_timestamp()
                position = self._generate_position_near_nato_harbor()
                altitude = self._generate_altitude()

                object_data, obj_example_ref = self._generate_object_payload()
                magnetic_field = self._generate_magnetic_field_data(obj_example_ref)
                detection_confidence = self._generate_detection_confidence(obj_example_ref)

                # Flaches JSON-Objekt
                mad_data_record = {
                    "buoyid": buoy_id,
                    "ts": timestamp,
                    "geo_position_lat": position['lat'],
                    "geo_position_lon": position['lon'],
                    "altitude": altitude,
                    "payload_magneticField_totalField": magnetic_field['totalField'],
                    "payload_magneticField_anomaly": magnetic_field['anomaly'],
                    "payload_magneticField_gradient": magnetic_field['gradient'],
                    "payload_detectionConfidence": detection_confidence,
                    "payload_object_type": object_data.get('type'),
                    "payload_object_classification": object_data.get('classification'),
                    "payload_object_confidence": object_data.get('confidence'),
                    "payload_object_estimatedDepth": object_data.get('estimatedDepth'),
                    "payload_object_motion": object_data.get('motion'),
                    "payload_object_extent": object_data.get('extent'),
                    "payload_object_notes": object_data.get('notes'),
                    "payload_object_orientation": object_data.get('orientation'),
                    "payload_object_correlationId": object_data.get('correlationId')
                }
                output_records.append(mad_data_record)

            output_content_string = "\n".join([json.dumps(record) for record in output_records])

            return FlowFileTransformResult(
                contents=output_content_string.encode("utf-8"),
                attributes={"format": "json", "mad.simulation": "true", "geo.near.nato.harbor": "true"},
                relationship="success"
            )

        except Exception as e:
            self.logger.error(f"Fehler beim Generieren der MAD-Bojen-Daten: {e}")
            return FlowFileTransformResult(
                contents=f"Fehler: {e}".encode("utf-8"),
                attributes={"error": str(e), "mad.generation.error": "true"},
                relationship="failure"
            )

