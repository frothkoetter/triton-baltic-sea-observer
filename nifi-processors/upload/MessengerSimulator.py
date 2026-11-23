import sys
from pathlib import Path

_EXTENSIONS_DIR = Path(__file__).resolve().parent
if _EXTENSIONS_DIR.as_posix() not in sys.path:
    sys.path.insert(0, _EXTENSIONS_DIR.as_posix())

from nifiapi.flowfiletransform import FlowFileTransform, FlowFileTransformResult
from nifiapi.properties import PropertyDescriptor, StandardValidators
import json
import random
import datetime
from datetime import timezone

from socmint_catalog import pick_scenario, scenario_to_record

try:
    from socmint_catalog import TRUST_DISINFO, TRUST_CORROBORATED
except ImportError:
    TRUST_DISINFO = "DISINFORMATION_CONFIRMED"
    TRUST_CORROBORATED = "SENSOR_CORROBORATED"


class TwitterMessageGenerator(FlowFileTransform):
    """
    NiFi-Python-Prozessor: generiert SOCMINT-nahe JSON-Nachrichten (X/Telegram) für die Ostsee.

    Enthält simulierte Verifikation (Trust-Tags, Desinformation, Sensor-Korrelation) für
  ``defense.social_media_messages``, Lagebild und NBA.
    """

    class Java:
        implements = ['org.apache.nifi.python.processor.FlowFileTransform']

    class ProcessorDetails:
        version = '3.0.0'
        description = (
            'Generiert realistische Ostsee-SOCMINT-NDJSON (Baltiysk, Eagle S/EstLink, '
            'GPS-Jamming, PL-Wind-Desinfo) inkl. Trust-Tags und Verifikationsnotizen.'
        )
        dependencies = []

    MAX_OUTPUT_MESSAGES = PropertyDescriptor(
        name="Max Output Messages",
        description=(
            "Max. Nachrichten pro FlowFile. Leer/0 = unbegrenzt (intern Default 10). "
            "Nutzt gewichtete Szenario-Bibliothek statt Zufallswörter."
        ),
        validators=[StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR],
        default_value="10",
        required=False,
    )

    SOCMINT_SEED = PropertyDescriptor(
        name="SOCMINT Random Seed",
        description="Optionaler Seed für reproduzierbare Demos (leer = nondeterministisch).",
        validators=[StandardValidators.INTEGER_VALIDATOR],
        required=False,
    )

    def __init__(self, **kwargs):
        kwargs.pop("jvm", None)
        super().__init__(**kwargs)
        self.descriptors = [self.MAX_OUTPUT_MESSAGES, self.SOCMINT_SEED]

    def getPropertyDescriptors(self):
        return self.descriptors

    def transform(self, context, flowFile):
        try:
            max_messages_str = context.getProperty(self.MAX_OUTPUT_MESSAGES.name).getValue()
            max_messages = int(max_messages_str) if max_messages_str and max_messages_str.isdigit() else 10
            if max_messages <= 0:
                max_messages = 10

            seed_prop = context.getProperty(self.SOCMINT_SEED.name).getValue()
            seed = int(seed_prop) if seed_prop and str(seed_prop).strip().lstrip("-").isdigit() else None
            rng = random.Random(seed)
            ts = datetime.datetime.now(timezone.utc).isoformat()

            output_records = []
            for _ in range(max_messages):
                scenario = pick_scenario(rng)
                output_records.append(scenario_to_record(scenario, rng, ts))

            output_content_string = "\n".join(json.dumps(record) for record in output_records)

            disinfo_count = sum(1 for r in output_records if r.get("disinformation_flag"))
            corroborated = sum(
                1 for r in output_records if r.get("trust_tag") == TRUST_CORROBORATED
            )

            return FlowFileTransformResult(
                contents=output_content_string.encode("utf-8"),
                attributes={
                    "format": "json",
                    "tweet.generated": "true",
                    "simulated.twitter.fields": "true",
                    "socmint.mode": "realistic",
                    "socmint.count": str(len(output_records)),
                    "socmint.disinformation": str(disinfo_count),
                    "socmint.sensor_corroborated": str(corroborated),
                },
                relationship="success",
            )

        except Exception as e:
            self.logger.error(f"Fehler beim Generieren der SOCMINT-Nachrichten: {e}")
            return FlowFileTransformResult(
                contents=f"Fehler: {e}".encode("utf-8"),
                attributes={"error": str(e), "tweet.generation.error": "true"},
                relationship="failure",
            )