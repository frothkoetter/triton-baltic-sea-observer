"""
STANAG / ADatP-3 inspired naval message generation for Baltic exercises.

Generates realistic message mixes: unit → HQ, HQ → unit, and unit ↔ unit,
with NATO-style DTG, MSGID, and sectioned plain-text bodies (ACP-127 flavour).
"""

from __future__ import annotations

import datetime
import json
import random
import re
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple

from marine_fleet_registry import GERMAN_NAVAL_FLEET, MARINE_POSITION_DECIMALS

# --- Traffic volume (messages per processor invocation) ----------------------------

TRAFFIC_PROFILES: Dict[str, Tuple[int, int]] = {
    "custom": (0, 0),  # use explicit Number of Messages
    "routine_patrol": (6, 12),
    "watch_section": (14, 22),
    "snap_exercise": (24, 36),
    "major_exercise": (42, 58),
    "hq_pulse": (10, 18),  # mostly HQ → unit (orders / warnings)
}

DEFAULT_EXERCISE_NAMES = (
    "BALTOPS",
    "OPEN GUARDIAN",
    "BALTIC SHIELD",
    "SNMG-1 STEX",
    "SAFE PASSAGE",
    "IRON WOLF",
)

HEADQUARTERS: List[Dict[str, str]] = [
    {"id": "MOC", "name": "MARITIME OPERATIONS CENTRE", "short": "MOC"},
    {"id": "TFHQ", "name": "TASK FORCE HQ BALTIC", "short": "TFHQ-BAL"},
    {"id": "SFHQ", "name": "STRIKING FORCE HQ SNMG1", "short": "SNMG1-HQ"},
    {"id": "LOGCELL", "name": "LOGISTICS COORDINATION CELL", "short": "LOGCELL-BAL"},
    {"id": "MEDCO", "name": "MEDICAL COORDINATION CELL", "short": "MEDCO-NRF"},
    {"id": "NOC", "name": "NETWORK OPERATIONS CENTRE", "short": "NOC-MAR"},
]

BALTIC_AOI = (
    "FEHMARN BELT",
    "GOTLAND SURROUNDINGS",
    "GDAŃSK BIGHT",
    "GULF OF FINLAND APPROACHES",
    "BORNholm BANK",
    "ARKONA BASIN",
    "KIEL CANAL APPROACH",
)

CONTACT_TYPES = (
    "unidentified surface contact",
    "fishing cluster",
    "merchant traffic",
    "submarine contact (sonar)",
    "fast small craft",
    "low slow flyer (possible UAV)",
    "mine-like object (MILCO)",
)

# Weighted mix approximating NATO SNF reporting during a 60–90 min window
DIRECTION_WEIGHTS = {
    "unit_to_hq": 0.38,
    "hq_to_unit": 0.28,
    "unit_to_unit": 0.34,
}


@dataclass(frozen=True)
class StanagMessage:
    message_id: str
    message_dtg: str
    message_from: str
    message_to: str
    message_subject: str
    message_text: str
    message_position: str
    traffic_direction: str
    message_category: str
    exercise: str
    mmsi: str
    body: str

    def to_dict(self) -> Dict[str, Any]:
        return {
            "message_id": self.message_id,
            "message_dtg": self.message_dtg,
            "message_from": self.message_from,
            "message_to": self.message_to,
            "message_subject": self.message_subject,
            "message_text": self.message_text,
            "message_position": self.message_position,
            "traffic_direction": self.traffic_direction,
            "message_category": self.message_category,
            "exercise": self.exercise,
            "mmsi": self.mmsi,
            "body_stanag": self.body,
        }


def format_nato_dtg(when: Optional[datetime.datetime] = None, offset_minutes: int = 0) -> str:
    """DDhhmmZMMMYY (e.g. 151230ZNOV25) — parseable by defense.marine_messages view."""
    when = when or datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)
    if offset_minutes:
        when = when - datetime.timedelta(minutes=offset_minutes)
    return f"{when.strftime('%d%H%M')}Z{when.strftime('%b%y').upper()}"


def format_position(lat: float, lon: float) -> str:
    lat_r = round(lat, MARINE_POSITION_DECIMALS)
    lon_r = round(lon, MARINE_POSITION_DECIMALS)
    return f"LAT {lat_r}°N, LON {lon_r}°E"


def resolve_message_count(
    profile: str,
    explicit: int,
    *,
    include_inter_unit: bool,
    include_hq: bool,
) -> int:
    if profile != "custom" and profile in TRAFFIC_PROFILES:
        lo, hi = TRAFFIC_PROFILES[profile]
        base = random.randint(lo, hi)
    else:
        base = max(1, explicit)

    if not include_inter_unit:
        base = max(1, int(base * 0.72))
    if not include_hq:
        base = max(1, int(base * 0.65))
    return min(80, max(1, base))


def _unit_key(name: str) -> str:
    return re.sub(r"[^A-Za-z0-9]", "", name)[:12].upper()


def _pick_direction(rng: random.Random, include_hq: bool, include_inter: bool) -> str:
    weights = dict(DIRECTION_WEIGHTS)
    if not include_hq:
        weights["hq_to_unit"] = 0.0
        weights["unit_to_hq"] *= 0.85
    if not include_inter:
        weights["unit_to_unit"] = 0.0
    total = sum(weights.values()) or 1.0
    roll = rng.random() * total
    acc = 0.0
    for key, w in weights.items():
        acc += w
        if roll <= acc:
            return key
    return "unit_to_hq"


def _ship_meta(ship: Dict[str, Any]) -> Dict[str, str]:
    mmsi = str(ship.get("mmsi", ""))
    name = str(ship.get("name", mmsi))
    fleet = next((u for u in GERMAN_NAVAL_FLEET if u["mmsi"] == mmsi), None)
    unit_class = fleet["class"] if fleet else "Surface Unit"
    return {"name": name, "mmsi": mmsi, "class": unit_class}


# --- Message builders -----------------------------------------------------------


def _body_wrapper(sections: List[str]) -> str:
    return "\n\n".join(s for s in sections if s).strip()


def _compile_message(
    *,
    msgid: str,
    dtg: str,
    msg_from: str,
    msg_to: str,
    subject: str,
    position: str,
    txt: str,
    ampns: Optional[List[str]] = None,
    refs: Optional[str] = None,
) -> str:
    head = [
        "ZCZC",
        f"MSGID/{msgid}",
        f"DTG/{dtg}",
        f"FROM/{msg_from}",
        f"TO/{msg_to}",
    ]
    if refs:
        head.append(f"REF/{refs}")
    head.extend([f"SUBJ/{subject}", "", f"POSITION/{position}"])
    if ampns:
        for i, line in enumerate(ampns, start=1):
            head.append(f"AMPN/{i}/ {line}")
    head.extend(["", f"TXT/{txt}", "", "NNNN"])
    return "\n".join(head)


def _sitrep_unit_to_hq(
    ship: Dict[str, Any],
    hq: Dict[str, str],
    exercise: str,
    aoi: str,
    dtg: str,
    seq: int,
    rng: random.Random,
) -> StanagMessage:
    meta = _ship_meta(ship)
    lat, lon = float(ship["lat"]), float(ship["lon"])
    pos = format_position(lat, lon)
    activity = rng.choice(
        ["PATROLLING", "TRANSITING", "STATIONING", "CONDUCTING MCM OPS", "ASW SCREENING"]
    )
    contact = rng.choice(CONTACT_TYPES)
    outcome = rng.choice(
        [
            "no hostile intent assessed",
            "contact tracked; picture forwarded to TF",
            "contact cleared after visual ID",
            "passing close abeam — CPA acceptable",
            "EW environment quiet",
        ]
    )
    subject = "SITUATION REPORT (SITREP)"
    txt = (
        f"EX {exercise}: {meta['name']} ({meta['class']}) {activity} in {aoi}. "
        f"Observed {contact}. Assessment: {outcome}. Next POSREP per OPTASK schedule."
    )
    msgid = f"SITREP/{_unit_key(meta['name'])}/{seq:04d}"
    body = _compile_message(
        msgid=msgid,
        dtg=dtg,
        msg_from=meta["name"],
        msg_to=hq["name"],
        subject=subject,
        position=pos,
        txt=txt,
        ampns=[
            f"FUEL STATE {rng.randint(45, 92)} PCT",
            f"WEAPONS/ SENSORS GREEN",
            f"REMARK/ SSE OF {rng.randint(8, 18)} KTS",
        ],
    )
    return StanagMessage(
        message_id=msgid,
        message_dtg=dtg,
        message_from=meta["name"],
        message_to=hq["name"],
        message_subject=subject,
        message_text=txt,
        message_position=pos,
        traffic_direction="unit_to_hq",
        message_category="SITREP",
        exercise=exercise,
        mmsi=meta["mmsi"],
        body=body,
    )


def _posrep_unit_to_hq(
    ship: Dict[str, Any],
    hq: Dict[str, str],
    exercise: str,
    _aoi: str,
    dtg: str,
    seq: int,
    rng: random.Random,
) -> StanagMessage:
    meta = _ship_meta(ship)
    pos = format_position(float(ship["lat"]), float(ship["lon"]))
    course = rng.randint(0, 359)
    speed = rng.randint(6, 22)
    subject = "POSITION REPORT (POSREP)"
    txt = (
        f"POSREP EX {exercise}: {meta['name']} at reported position. "
        f"SOG {speed} KT, COG {course:03d} DEG. Intent: remain in assigned patrol box."
    )
    msgid = f"POSREP/{_unit_key(meta['name'])}/{seq:04d}"
    body = _compile_message(
        msgid=msgid, dtg=dtg, msg_from=meta["name"], msg_to=hq["name"],
        subject=subject, position=pos, txt=txt,
    )
    return StanagMessage(
        message_id=msgid,
        message_dtg=dtg,
        message_from=meta["name"],
        message_to=hq["name"],
        message_subject=subject,
        message_text=txt,
        message_position=pos,
        traffic_direction="unit_to_hq",
        message_category="POSREP",
        exercise=exercise,
        mmsi=meta["mmsi"],
        body=body,
    )


def _contact_report(
    ship: Dict[str, Any],
    hq: Dict[str, str],
    exercise: str,
    aoi: str,
    dtg: str,
    seq: int,
    rng: random.Random,
) -> StanagMessage:
    meta = _ship_meta(ship)
    pos = format_position(float(ship["lat"]), float(ship["lon"]))
    contact = rng.choice(CONTACT_TYPES)
    subject = "CONTACT REPORT (SPOT)"
    bearing = rng.randint(10, 350)
    range_nm = rng.randint(4, 28)
    txt = (
        f"SPOT REPORT: {contact} bearing {bearing:03d} deg, range {range_nm} NM "
        f"from {meta['name']} in {aoi}. Contact course/speed unknown. TF notified."
    )
    msgid = f"SPOT/{_unit_key(meta['name'])}/{seq:04d}"
    body = _compile_message(
        msgid=msgid, dtg=dtg, msg_from=meta["name"], msg_to=hq["name"],
        subject=subject, position=pos, txt=txt,
    )
    return StanagMessage(
        message_id=msgid,
        message_dtg=dtg,
        message_from=meta["name"],
        message_to=hq["name"],
        message_subject=subject,
        message_text=txt,
        message_position=pos,
        traffic_direction="unit_to_hq",
        message_category="SPOT",
        exercise=exercise,
        mmsi=meta["mmsi"],
        body=body,
    )


def _hq_frago_to_unit(
    ship: Dict[str, Any],
    hq: Dict[str, str],
    exercise: str,
    aoi: str,
    dtg: str,
    seq: int,
    rng: random.Random,
) -> StanagMessage:
    meta = _ship_meta(ship)
    pos = format_position(float(ship["lat"]), float(ship["lon"]))
    subject = "FRAGMENTARY ORDER (FRAGO)"
    task = rng.choice(
        [
            f"Shift patrol box east of {aoi} at time DTG+90 min",
            "Assume plane guard station for helo ops vic 55N 17E",
            "Investigate merchant traffic with sanctions flag — VID only",
            "Close to deny transit line between two AIS clusters",
            "Prepare for VERTREP from A1411 Berlin at scheduled RV point",
        ]
    )
    txt = (
        f"FRAGO EX {exercise} / TF DIR: {meta['name']} {task}. "
        "Acknowledge by separate MSG. ROE unchanged."
    )
    msgid = f"FRAGO/{hq['short']}/{seq:04d}"
    body = _compile_message(
        msgid=msgid,
        dtg=dtg,
        msg_from=hq["name"],
        msg_to=meta["name"],
        subject=subject,
        position=pos,
        txt=txt,
        refs=f"EX {exercise} OPORD DTG 020800Z",
    )
    return StanagMessage(
        message_id=msgid,
        message_dtg=dtg,
        message_from=hq["name"],
        message_to=meta["name"],
        message_subject=subject,
        message_text=txt,
        message_position=pos,
        traffic_direction="hq_to_unit",
        message_category="FRAGO",
        exercise=exercise,
        mmsi=meta["mmsi"],
        body=body,
    )


def _hq_warning_to_unit(
    ship: Dict[str, Any],
    hq: Dict[str, str],
    exercise: str,
    _aoi: str,
    dtg: str,
    seq: int,
    rng: random.Random,
) -> StanagMessage:
    meta = _ship_meta(ship)
    pos = format_position(float(ship["lat"]), float(ship["lon"]))
    subject = "WARNING ORDER (WMSG)"
    threat = rng.choice(
        [
            "possible GPS jamming activity",
            "restricted area INFORCE vic Bornholm",
            "UAV activity reported",
            "increased AIS spoofing",
            "weather deteriorating to BF 7",
        ]
    )
    txt = f"WMSG: All TF units — {threat}. {meta['name']} acknowledge compliance."
    msgid = f"WMSG/{hq['short']}/{seq:04d}"
    body = _compile_message(
        msgid=msgid, dtg=dtg, msg_from=hq["name"], msg_to=meta["name"],
        subject=subject, position=pos, txt=txt,
    )
    return StanagMessage(
        message_id=msgid,
        message_dtg=dtg,
        message_from=hq["name"],
        message_to=meta["name"],
        message_subject=subject,
        message_text=txt,
        message_position=pos,
        traffic_direction="hq_to_unit",
        message_category="WMSG",
        exercise=exercise,
        mmsi=meta["mmsi"],
        body=body,
    )


def _inter_unit_pim(
    ship_a: Dict[str, Any],
    ship_b: Dict[str, Any],
    exercise: str,
    _aoi: str,
    dtg: str,
    seq: int,
    rng: random.Random,
) -> StanagMessage:
    ma, mb = _ship_meta(ship_a), _ship_meta(ship_b)
    pos = format_position(float(ship_a["lat"]), float(ship_a["lon"]))
    subject = "PICTURE EXCHANGE (INTSNO)"
    txt = (
        f"INTSNO EX {exercise}: {ma['name']} reports course {rng.randint(80, 250):03d}, "
        f"speed {rng.randint(10, 18)} KT. Request {mb['name']} maintain 12 NM separation "
        "on starboard quarter for escort handover at next waypoint."
    )
    msgid = f"INTSNO/{_unit_key(ma['name'])}/{seq:04d}"
    body = _compile_message(
        msgid=msgid, dtg=dtg, msg_from=ma["name"], msg_to=mb["name"],
        subject=subject, position=pos, txt=txt,
    )
    return StanagMessage(
        message_id=msgid,
        message_dtg=dtg,
        message_from=ma["name"],
        message_to=mb["name"],
        message_subject=subject,
        message_text=txt,
        message_position=pos,
        traffic_direction="unit_to_unit",
        message_category="INTSNO",
        exercise=exercise,
        mmsi=ma["mmsi"],
        body=body,
    )


def _inter_unit_rendezvous(
    ship_a: Dict[str, Any],
    ship_b: Dict[str, Any],
    exercise: str,
    _aoi: str,
    dtg: str,
    seq: int,
    rng: random.Random,
) -> StanagMessage:
    ma, mb = _ship_meta(ship_a), _ship_meta(ship_b)
    pos = format_position(float(ship_a["lat"]), float(ship_a["lon"]))
    subject = "RENDEZVOUS (LOGREQ RV)"
    offset = rng.randint(30, 180)
    rv_dtg = format_nato_dtg(offset_minutes=-offset)
    txt = (
        f"RV: {ma['name']} proposes UNREP / GENPASS with {mb['name']} at DTG {rv_dtg}, "
        f"position {pos}. Fuel required {rng.randint(15, 45)} MT F76."
    )
    msgid = f"LOGREQ/{_unit_key(ma['name'])}/{seq:04d}"
    body = _compile_message(
        msgid=msgid, dtg=dtg, msg_from=ma["name"], msg_to=mb["name"],
        subject=subject, position=pos, txt=txt,
    )
    return StanagMessage(
        message_id=msgid,
        message_dtg=dtg,
        message_from=ma["name"],
        message_to=mb["name"],
        message_subject=subject,
        message_text=txt,
        message_position=pos,
        traffic_direction="unit_to_unit",
        message_category="LOGREQ",
        exercise=exercise,
        mmsi=ma["mmsi"],
        body=body,
    )


def _sick_report(
    ship: Dict[str, Any],
    hq: Dict[str, str],
    exercise: str,
    _aoi: str,
    dtg: str,
    seq: int,
    rng: random.Random,
) -> StanagMessage:
    meta = _ship_meta(ship)
    pos = format_position(float(ship["lat"]), float(ship["lon"]))
    illness = rng.choice(
        ["respiratory infection", "minor orthopedic injury", "sea sickness", "gastroenteritis"]
    )
    subject = "MEDICAL REPORT (SICK REPORT)"
    txt = (
        f"SICK REPORT: one crewmember aboard {meta['name']} diagnosed with {illness}. "
        "Condition stable. Request telemedicine advice / possible MEDEVAC window."
    )
    msgid = f"MEDREP/{_unit_key(meta['name'])}/{seq:04d}"
    body = _compile_message(
        msgid=msgid, dtg=dtg, msg_from=meta["name"], msg_to=hq["name"],
        subject=subject, position=pos, txt=txt,
    )
    return StanagMessage(
        message_id=msgid,
        message_dtg=dtg,
        message_from=meta["name"],
        message_to=hq["name"],
        message_subject=subject,
        message_text=txt,
        message_position=pos,
        traffic_direction="unit_to_hq",
        message_category="MEDREP",
        exercise=exercise,
        mmsi=meta["mmsi"],
        body=body,
    )


def _legacy_sick_simple(unit_name: str, lat: float, lon: float, mmsi: str, rng: random.Random) -> StanagMessage:
    """Backward-compatible short sick report (pre-2.0 style)."""
    pos = format_position(lat, lon)
    dtg = format_nato_dtg()
    illness = rng.choice(["fever", "stomach flu", "respiratory infection", "sea sickness"])
    subject = "SICK REPORT"
    txt = (
        f"SICK REPORT: ONE CREWMEMBER ONBOARD {unit_name} HAS BEEN DIAGNOSED WITH "
        f"{illness}. CONDITION IS STABLE. REQUESTING MEDICAL ADVICE."
    )
    msgid = f"SITREP/{_unit_key(unit_name)}/0001"
    body = _compile_message(
        msgid=msgid, dtg=dtg, msg_from=unit_name, msg_to="MARITIME OPERATIONS CENTRE",
        subject=subject, position=pos, txt=txt,
    )
    return StanagMessage(
        message_id=msgid,
        message_dtg=dtg,
        message_from=unit_name,
        message_to="MARITIME OPERATIONS CENTRE",
        message_subject=subject,
        message_text=txt,
        message_position=pos,
        traffic_direction="unit_to_hq",
        message_category="SICK",
        exercise="",
        mmsi=mmsi,
        body=body,
    )


Builder = Callable[..., StanagMessage]

UNIT_TO_HQ_BUILDERS: List[Builder] = [_sitrep_unit_to_hq, _posrep_unit_to_hq, _contact_report, _sick_report]
HQ_TO_UNIT_BUILDERS: List[Builder] = [_hq_frago_to_unit, _hq_warning_to_unit]
UNIT_TO_UNIT_BUILDERS: List[Builder] = [_inter_unit_pim, _inter_unit_rendezvous]


def generate_stanag_batch(
    ships: Sequence[Dict[str, Any]],
    *,
    count: int,
    exercise: str,
    include_inter_unit: bool = True,
    include_hq_traffic: bool = True,
    legacy_simple: bool = False,
    seed: Optional[int] = None,
) -> List[StanagMessage]:
    """Build *count* messages using *ships* positions (MarineShipSimulator aligned)."""
    if not ships:
        return []

    rng = random.Random(seed)
    pool = list(ships)
    rng.shuffle(pool)
    messages: List[StanagMessage] = []
    seq_by_prefix: Dict[str, int] = {}

    def next_seq(prefix: str) -> int:
        seq_by_prefix[prefix] = seq_by_prefix.get(prefix, 0) + 1
        return seq_by_prefix[prefix]

    for i in range(count):
        ship = pool[i % len(pool)]
        offset_min = i * rng.randint(2, 9) + rng.randint(0, 4)
        dtg = format_nato_dtg(offset_minutes=offset_min)
        hq = rng.choice(HEADQUARTERS)
        aoi = rng.choice(BALTIC_AOI)

        if legacy_simple and count <= 3:
            messages.append(
                _legacy_sick_simple(
                    str(ship["name"]), float(ship["lat"]), float(ship["lon"]), str(ship["mmsi"]), rng
                )
            )
            continue

        direction = _pick_direction(rng, include_hq_traffic, include_inter_unit)
        seq = next_seq(direction)

        if direction == "unit_to_hq":
            builder = rng.choice(UNIT_TO_HQ_BUILDERS)
            messages.append(builder(ship, hq, exercise, aoi, dtg, seq, rng))
        elif direction == "hq_to_unit":
            builder = rng.choice(HQ_TO_UNIT_BUILDERS)
            messages.append(builder(ship, hq, exercise, aoi, dtg, seq, rng))
        else:
            if len(pool) < 2:
                builder = rng.choice(UNIT_TO_HQ_BUILDERS)
                messages.append(builder(ship, hq, exercise, aoi, dtg, seq, rng))
            else:
                other = rng.choice([s for s in pool if s is not ship])
                builder = rng.choice(UNIT_TO_UNIT_BUILDERS)
                messages.append(builder(ship, other, exercise, aoi, dtg, seq, rng))

    return messages


def batch_summary_attributes(messages: Sequence[StanagMessage]) -> Dict[str, str]:
    """FlowFile attributes for NiFi routing / AttributesToJSON (first message + counts)."""
    if not messages:
        return {}
    first = messages[0]
    by_dir: Dict[str, int] = {}
    by_cat: Dict[str, int] = {}
    for m in messages:
        by_dir[m.traffic_direction] = by_dir.get(m.traffic_direction, 0) + 1
        by_cat[m.message_category] = by_cat.get(m.message_category, 0) + 1
    return {
        "message_id": first.message_id,
        "message_dtg": first.message_dtg,
        "message_from": first.message_from,
        "message_to": first.message_to,
        "message_subject": first.message_subject,
        "message_text": first.message_text[:500],
        "message_position": first.message_position,
        "stanag.message.count": str(len(messages)),
        "stanag.traffic.unit_to_hq": str(by_dir.get("unit_to_hq", 0)),
        "stanag.traffic.hq_to_unit": str(by_dir.get("hq_to_unit", 0)),
        "stanag.traffic.unit_to_unit": str(by_dir.get("unit_to_unit", 0)),
        "stanag.categories": ",".join(f"{k}:{v}" for k, v in sorted(by_cat.items())),
        "stanag.exercise": first.exercise,
    }


def render_text_bundle(messages: Sequence[StanagMessage]) -> str:
    sep = "\n\n" + ("=" * 60) + "\n\n"
    return sep.join(m.body for m in messages)


def render_ndjson(messages: Sequence[StanagMessage]) -> str:
    return "\n".join(json.dumps(m.to_dict(), ensure_ascii=False) for m in messages) + "\n"