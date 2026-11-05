import json
import math
import random
from datetime import datetime, timezone

from nifiapi.flowfiletransform import FlowFileTransform, FlowFileTransformResult
from nifiapi.properties import PropertyDescriptor, StandardValidators

class GPSJammerSimulator(FlowFileTransform):
    class Java:
        implements = ['org.apache.nifi.python.processor.FlowFileTransform']

    class ProcessorDetails:
        version = '2.2.0'
        description = 'Generiert simulierte GPS-Jamming-Daten (ADS-B NIC) für die Ostsee unter Verwendung von Geohashes.'
        dependencies = [] # Keine externen Bibliotheken wie Faker für dieses Modul nötig

    # Definition der Properties
    GEOHASH_PRECISION = PropertyDescriptor(
        name="Geohash Precision",
        description="Länge des Geohash-Strings (6 = ~1.2km, 7 = ~150m).",
        validators=[StandardValidators.INTEGER_VALIDATOR],
        allowable_values=["6", "7"],
        default_value="6",
        required=True
    )

    NUM_RECORDS = PropertyDescriptor(
        name="Number of Records per FlowFile",
        description="Anzahl der zu generierenden GPS-Jamming Stichproben.",
        validators=[StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR],
        default_value="25",
        required=False
    )

    def __init__(self, **kwargs):
        # WICHTIG: jvm-Argument abfangen, um den TypeError in NiFi zu vermeiden
        kwargs.pop("jvm", None)
        super().__init__(**kwargs)
        self.descriptors = [self.GEOHASH_PRECISION, self.NUM_RECORDS]
        self.LAT_RANGE = (53.0, 61.0)
        self.LON_RANGE = (10.0, 30.0)
        # Taktische Standorte der Jammer
        self.JAMMERS = [
            {"name": "Kaliningrad_Fixed", "pos": (54.7, 20.5), "active": True, "radius": 2.5},
            {"name": "St_Petersburg_Unit", "pos": (59.9, 30.3), "active": True, "radius": 3.0},
            {"name": "Gotland_Mobile", "pos": (57.2, 18.5), "active": True, "radius": 1.5}
        ]

    def getPropertyDescriptors(self):
        return self.descriptors

    def _encode_geohash(self, lat, lon, precision):
        """Generiert einen Geohash-String."""
        base32 = "0123456789bcdefghjkmnpqrstuvwxyz"
        lat_interval, lon_interval = (-90.0, 90.0), (-180.0, 180.0)
        geohash = ""
        bits = [16, 8, 4, 2, 1]
        bit, ch, even = 0, 0, True
        while len(geohash) < precision:
            if even:
                mid = (lon_interval[0] + lon_interval[1]) / 2
                if lon > mid:
                    ch |= bits[bit]; lon_interval = (mid, lon_interval[1])
                else: lon_interval = (lon_interval[0], mid)
            else:
                mid = (lat_interval[0] + lat_interval[1]) / 2
                if lat > mid:
                    ch |= bits[bit]; lat_interval = (mid, lat_interval[1])
                else: lat_interval = (lat_interval[0], mid)
            even = not even
            if bit < 4: bit += 1
            else:
                geohash += base32[ch]; bit = 0; ch = 0
        return geohash

    def transform(self, context, flowFile):
        try:
            output_records = []
            
            # Properties sicher abrufen
            precision_str = context.getProperty(self.GEOHASH_PRECISION.name).getValue()
            precision = int(precision_str) if precision_str else 6
            
            num_records_str = context.getProperty(self.NUM_RECORDS.name).getValue()
            num_records = int(num_records_str) if num_records_str and num_records_str.isdigit() else 25

            timestamp = datetime.now(timezone.utc).isoformat(timespec='milliseconds')[:-6] + 'Z'

            for _ in range(num_records):
                lat = random.uniform(self.LAT_RANGE[0], self.LAT_RANGE[1])
                lon = random.uniform(self.LON_RANGE[0], self.LON_RANGE[1])
                
                # Jamming-Einfluss (ADS-B NIC Simulation)
                max_inf = 0.0
                for j in self.JAMMERS:
                    dist = math.sqrt((lat - j["pos"][0])**2 + (lon - j["pos"][1])**2)
                    if dist < j["radius"]:
                        inf = (j["radius"] - dist) / j["radius"]
                        max_inf = max(max_inf, inf)
                
                nic = int(8 * (1 - max_inf))
                integrity = round(0.90 * (1 - max_inf), 2)
                
                record = {
                    "geohash": self._encode_geohash(lat, lon, precision),
                    "ts": timestamp,
                    "latitude": round(lat, 5),
                    "longitude": round(lon, 5),
                    "adsb_nic": nic,
                    "signal_integrity": integrity,
                    "jamming_indicator": nic < 5,
                    "event_type": "gps_jammer_event"
                }
                output_records.append(record)

            # NDJSON Format (eine Zeile pro Record)
            output_content = "\n".join([json.dumps(r) for r in output_records])

            return FlowFileTransformResult(
                relationship="success",
                contents=output_content.encode('utf-8'),
                attributes={
                    "mime.type": "application/x-ndjson",
                    "schema.name": "gps_jammer_event",
                    "simulation.type": "gps_jamming"
                }
            )

        except Exception as e:
            self.logger.error(f"Fehler im GPSJammerSimulator: {e}")
            return FlowFileTransformResult(relationship="failure")
