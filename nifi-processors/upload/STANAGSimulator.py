import os
import sys
from pathlib import Path

_EXTENSIONS_DIR = Path(__file__).resolve().parent
if _EXTENSIONS_DIR.as_posix() not in sys.path:
    sys.path.insert(0, _EXTENSIONS_DIR.as_posix())

from nifiapi.flowfiletransform import FlowFileTransform, FlowFileTransformResult
from nifiapi.properties import PropertyDescriptor, StandardValidators

from marine_fleet_registry import (
    GERMAN_NAVAL_FLEET,
    MARINESHIP_STATE_FILE,
    load_simulated_ship_positions,
    parse_marine_simulator_ndjson,
)
from stanag_message_engine import (
    DEFAULT_EXERCISE_NAMES,
    TRAFFIC_PROFILES,
    StanagMessage,
    batch_summary_attributes,
    generate_stanag_batch,
    render_ndjson,
    render_text_bundle,
    resolve_message_count,
)
from mission_nifi_flowfile import flowfile_attr_map
from mission_target_track import tag_stanag_message_body
from dataclasses import replace


class StanagMessageGenerator(FlowFileTransform):
    """
    NiFi Python-Prozessor: STANAG / ADatP-3-ähnlicher Nachrichtenverkehr
  (Einheit ↔ HQ, HQ → Einheit, Einheit ↔ Einheit) für Übungslagen.
    """

    class Java:
        implements = ['org.apache.nifi.python.processor.FlowFileTransform']

    class ProcessorDetails:
        version = '2.1.0'
        description = (
            'NATO-style operational messaging (SITREP, POSREP, FRAGO, WMSG, SPOT, '
            'SOCINT/INTREP, LOGREQ, MEDREP, inter-unit PIM). SOCINT lines reuse '
            'Ostsee SOCMINT scenarios (social_media_messages). POSITION matches MarineShipSimulator. '
            'Traffic profiles simulate realistic counts per exercise phase.'
        )
        dependencies = []

    NUM_MESSAGES = PropertyDescriptor(
        name="Number of Messages",
        description="Used when Traffic Profile is custom (1–80).",
        validators=[StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR],
        default_value="12",
        required=True,
    )

    TRAFFIC_PROFILE = PropertyDescriptor(
        name="Traffic Profile",
        description="Preset message batch size and mix (overrides Number of Messages unless custom).",
        allowable_values=list(TRAFFIC_PROFILES.keys()),
        default_value="watch_section",
        required=True,
    )

    EXERCISE_DESIGNATION = PropertyDescriptor(
        name="Exercise Designation",
        description="Exercise name in SUBJ/TXT (e.g. BALTOPS). Empty = random NATO-style codename.",
        default_value="",
        required=False,
    )

    OUTPUT_FORMAT = PropertyDescriptor(
        name="Output Format",
        description="text_bundle: ACP-style blocks; ndjson: one JSON object per line for SplitRecord.",
        allowable_values=["text_bundle", "ndjson"],
        default_value="text_bundle",
        required=True,
    )

    INCLUDE_INTER_UNIT = PropertyDescriptor(
        name="Include Inter-Unit Traffic",
        description="Ship-to-ship PIM, rendezvous, fuel coordination.",
        allowable_values=["true", "false"],
        default_value="true",
        required=True,
    )

    INCLUDE_HQ_TRAFFIC = PropertyDescriptor(
        name="Include HQ Traffic",
        description="Orders and warnings from Maritime Operations Centre / TF HQ.",
        allowable_values=["true", "false"],
        default_value="true",
        required=True,
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

    LEGACY_SIMPLE_MODE = PropertyDescriptor(
        name="Legacy Simple Mode",
        description="When true and ≤3 messages, emit pre-2.0 short SITREP/SICK style only.",
        allowable_values=["true", "false"],
        default_value="false",
        required=True,
    )

    def __init__(self, **kwargs):
        kwargs.pop("jvm", None)
        super().__init__(**kwargs)
        self.descriptors = [
            self.NUM_MESSAGES,
            self.TRAFFIC_PROFILE,
            self.EXERCISE_DESIGNATION,
            self.OUTPUT_FORMAT,
            self.INCLUDE_INTER_UNIT,
            self.INCLUDE_HQ_TRAFFIC,
            self.REQUIRE_MARINE_POSITIONS,
            self.PREFER_INCOMING_NDJSON,
            self.LEGACY_SIMPLE_MODE,
        ]

    def getPropertyDescriptors(self):
        return self.descriptors

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

    def transform(self, context, flowFile):
        try:
            import random

            num_messages = int(context.getProperty(self.NUM_MESSAGES.name).getValue() or "12")
            profile = context.getProperty(self.TRAFFIC_PROFILE.name).getValue() or "watch_section"
            exercise = (context.getProperty(self.EXERCISE_DESIGNATION.name).getValue() or "").strip()
            if not exercise:
                exercise = random.choice(DEFAULT_EXERCISE_NAMES)
            output_format = context.getProperty(self.OUTPUT_FORMAT.name).getValue() or "text_bundle"
            include_inter = context.getProperty(self.INCLUDE_INTER_UNIT.name).getValue() == "true"
            include_hq = context.getProperty(self.INCLUDE_HQ_TRAFFIC.name).getValue() == "true"
            require_marine = (
                context.getProperty(self.REQUIRE_MARINE_POSITIONS.name).getValue() == "true"
            )
            prefer_ndjson = (
                context.getProperty(self.PREFER_INCOMING_NDJSON.name).getValue() == "true"
            )
            legacy_simple = context.getProperty(self.LEGACY_SIMPLE_MODE.name).getValue() == "true"

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
                        "lat": 54.3233 + (i * 0.05),
                        "lon": 10.1228 + (i * 0.08),
                    }
                    for i, u in enumerate(GERMAN_NAVAL_FLEET[:12])
                ]

            count = resolve_message_count(
                profile,
                num_messages,
                include_inter_unit=include_inter,
                include_hq=include_hq,
            )

            messages = generate_stanag_batch(
                ships,
                count=count,
                exercise=exercise,
                include_inter_unit=include_inter,
                include_hq_traffic=include_hq,
                legacy_simple=legacy_simple,
            )

            ff_attrs = flowfile_attr_map(flowFile)
            mission_id = ff_attrs.get("mission.id", "")
            buoy_id = (ff_attrs.get("mission.buoy.ids") or "").split(",")[0].strip()
            if mission_id:
                tagged: list[StanagMessage] = []
                for m in messages:
                    new_text = tag_stanag_message_body(
                        m.message_text,
                        mission_id=mission_id,
                        buoy_id=buoy_id or None,
                    )
                    new_subject = m.message_subject
                    if not new_subject.startswith("NBA-MSN"):
                        new_subject = f"NBA-MSN/{mission_id[:8]} {new_subject}"
                    tagged.append(
                        replace(m, message_text=new_text, message_subject=new_subject, body=m.body)
                    )
                messages = tagged

            position_source = "registry_fallback"
            if prefer_ndjson and content:
                text = content.decode("utf-8", errors="replace")
                if parse_marine_simulator_ndjson(text):
                    position_source = "ndjson"
                elif os.path.exists(MARINESHIP_STATE_FILE):
                    position_source = "state_file"
            elif os.path.exists(MARINESHIP_STATE_FILE):
                position_source = "state_file"

            if output_format == "ndjson":
                output_content = render_ndjson(messages)
                fmt = "application/x-ndjson"
            else:
                output_content = render_text_bundle(messages)
                fmt = "text/plain"

            mmsi_list = list(dict.fromkeys(m.mmsi for m in messages))
            attrs = {
                "format": fmt,
                "message.type": "stanag_report",
                "stanag.version": "2.1.0",
                "stanag.output.format": output_format,
                "stanag.traffic.profile": profile,
                "marine.position.source": position_source,
                "marine.mmsi.list": ",".join(mmsi_list),
            }
            if mission_id:
                attrs["mission.id"] = mission_id
                if buoy_id:
                    attrs["mission.buoy.id"] = buoy_id
            attrs.update(batch_summary_attributes(messages))

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