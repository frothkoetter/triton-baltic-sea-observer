"""
Ostsee-SOCMINT-Szenarien und simulierte Verifikation (Ukraine-Krieg / Baltic-Sentry-Lektionen).

Reine stdlib — wird von ``MessengerSimulator`` (TwitterMessageGenerator) und Unit-Tests genutzt.
"""

from __future__ import annotations

import random
from dataclasses import dataclass
from typing import Any


TRUST_UNVERIFIED = "UNVERIFIED_SOCMINT"
TRUST_CORROBORATED = "SENSOR_CORROBORATED"
TRUST_DISINFO = "DISINFORMATION_CONFIRMED"
TRUST_LOW = "LOW_CONFIDENCE"


@dataclass(frozen=True)
class SocmintScenario:
    scenario_id: str
    weight: int
    platform: str
    user_name: str
    user_username: str
    tweet: str
    latitude: float
    longitude: float
    intel_category: str
    base_priority: str
    disinformation_flag: bool
    trust_tag: str
    verification_notes: str
    image_recycle_id: str | None = None
    correlation_hint: str | None = None


def _scenarios() -> list[SocmintScenario]:
    """Statische, trainingsnahe Meldungen (Baltiysk, Eagle S, GPS-Jamming, PL-Wind-IW)."""
    return [
      SocmintScenario(
          scenario_id="baltiysk_ad_geoloc",
          weight=14,
          platform="telegram",
          user_name="Dnipro OSINT",
          user_username="dnipro_osint",
          tweet=(
              "🔴 Baltiysk / Kaliningrad: Mehrere Detonationen nahe Ostseeflotte-Stützpunkt. "
              "Video zeigt Flakfeuer; Geolok ~54.6501°N 19.9053°E (Hafen-Luftabwehr). "
              "#Baltiysk #Kaliningrad #OSINT"
          ),
          latitude=54.6501,
          longitude=19.9053,
          intel_category="kinetic",
          base_priority="mittel",
          disinformation_flag=False,
          trust_tag=TRUST_UNVERIFIED,
          verification_notes=(
              "Pending cross-check: SAR pass + coastal radar contact within 15 min window."
          ),
          correlation_hint="await_sar_baltiysk",
      ),
      SocmintScenario(
          scenario_id="baltiysk_fake_iran_strike_photo",
          weight=10,
          platform="x",
          user_name="Exilenova+",
          user_username="exilenova_plus",
          tweet=(
              "BREAKING: Erster Drohnentreffer auf Russlands Ostseeflotte in Baltiysk — Beweisfoto! "
              "Litauen muss zur Verantwortung gezogen werden. 🇱🇹❌"
          ),
          latitude=54.6412,
          longitude=19.9088,
          intel_category="disinformation",
          base_priority="hoch",
          disinformation_flag=True,
          trust_tag=TRUST_DISINFO,
          verification_notes=(
              "Reverse-image match: recycled US strike imagery, Qeshm Island (Iran) — not Baltiysk. "
              "Coordinated blame narrative vs Lithuania."
          ),
          image_recycle_id="qeshm_island_us_strike_archive",
      ),
      SocmintScenario(
          scenario_id="sparta_ii_gulf_finland",
          weight=8,
          platform="telegram",
          user_name="Baltic Fleet Watch",
          user_username="baltfleetwatch",
          tweet=(
              "Finnischer Meerbusen: Bericht über Drohnenangriff auf Logistikschip Sparta II "
              "(IMO-Rumour). Augenzeugenclip zeigt Rauchplume NW von Kotlin. #SpartaII #GulfOfFinland"
          ),
          latitude=59.85,
          longitude=25.42,
          intel_category="kinetic",
          base_priority="mittel",
          disinformation_flag=False,
          trust_tag=TRUST_UNVERIFIED,
          verification_notes="No AIS gap yet; awaiting NATO shipping lane MDA feed.",
          correlation_hint="ais_sparta_ii",
      ),
      SocmintScenario(
          scenario_id="eagle_s_estlink_ais",
          weight=12,
          platform="x",
          user_name="Jonas Auonsson",
          user_username="Auonsson",
          tweet=(
              "Shadow-fleet tanker Eagle S: AIS shows ~25% speed drop 40 min before EstLink-2 corridor "
              "crossing (24 Dec pattern). Track replay + port registry flags match IStories profile. "
              "#EstLink2 #ShadowFleet #BalticSea"
          ),
          latitude=60.12,
          longitude=22.98,
          intel_category="shadow_fleet",
          base_priority="hoch",
          disinformation_flag=False,
          trust_tag=TRUST_CORROBORATED,
          verification_notes=(
              "AIS open data + cable operator timing align; tagged SENSOR_CORROBORATED for demo fusion."
          ),
          correlation_hint="ais_eagle_s_estlink",
      ),
      SocmintScenario(
          scenario_id="yi_peng_balticconnector",
          weight=6,
          platform="x",
          user_name="Cable Sentinel",
          user_username="cable_sentinel",
          tweet=(
              "Yi Peng 3 anchor-drag pattern (~330 km) vs Balticconnector 2023 baseline — "
              "same MMSI family now lurking Skagerrak approach. #Balticconnector #CriticalInfra"
          ),
          latitude=56.45,
          longitude=11.12,
          intel_category="shadow_fleet",
          base_priority="mittel",
          disinformation_flag=False,
          trust_tag=TRUST_UNVERIFIED,
          verification_notes="Historical pattern match only; no live DAS spike in sim.",
          correlation_hint="das_balticconnector",
      ),
      SocmintScenario(
          scenario_id="gps_jamming_tallinn_spoof",
          weight=10,
          platform="x",
          user_name="GPSJam EU",
          user_username="gpsjam_eu",
          tweet=(
              "Ostsee EW: AIS spoofing cluster — multiple shadow-fleet contacts 'parked' at "
              "Tallinn airport (TLL) while true tracks remain offshore. Jamming polygon updated. "
              "#GPSJamming #BalticSea"
          ),
          latitude=59.413,
          longitude=24.798,
          intel_category="electronic_warfare",
          base_priority="hoch",
          disinformation_flag=False,
          trust_tag=TRUST_CORROBORATED,
          verification_notes="Correlates with defense.gps_jammer sim + AIS outlier CTE.",
          correlation_hint="gps_jammer_tallinn",
      ),
      SocmintScenario(
          scenario_id="poland_offshore_wind_iw",
          weight=11,
          platform="x",
          user_name="Energia Bałtyk",
          user_username="bot_wind_4821",
          tweet=(
              "Offshore-Wind in Polen = NATO-Trojaner. Stoppt Bałtyk 1 bevor die Küste brennt. "
              "Teilen! #wiatr #dezinformacja"
          ),
          latitude=54.85,
          longitude=16.2,
          intel_category="disinformation",
          base_priority="mittel",
          disinformation_flag=True,
          trust_tag=TRUST_DISINFO,
          verification_notes=(
              "Bot-network fingerprint (burst cadence + duplicated phrasing). "
              "Maps to PL ~70k energy-disinfo campaign archetype."
          ),
          image_recycle_id=None,
      ),
      SocmintScenario(
          scenario_id="ukraine_style_prebunk_buildup",
          weight=5,
          platform="x",
          user_name="Baltic OSINT Desk",
          user_username="baltic_osint_desk",
          tweet=(
              "Kaliningrad-Bahn: Open-source convoy clips + railcar counts spike (prebunk pattern). "
              "Not kinetic yet — monitor Baltiysk loaders via sat pass list. #SOCMINT #BalticSentry"
          ),
          latitude=54.7101,
          longitude=20.5108,
          intel_category="indicators_warning",
          base_priority="niedrig",
          disinformation_flag=False,
          trust_tag=TRUST_LOW,
          verification_notes="Strategic indicator only; no sensor corroboration required for tagging.",
      ),
      SocmintScenario(
          scenario_id="e_vorog_style_coast_report",
          weight=7,
          platform="telegram",
          user_name="CoastWatch EE",
          user_username="coastwatch_ee",
          tweet=(
              "eMeldung (Demo): Drohne Richtung Nord, Höhe ~150m, nahe Muuga. "
              "Bitte keine eigenen Truppenbewegungen posten (OPSEC). #eVorog #Baltic"
          ),
          latitude=59.45,
          longitude=24.97,
          intel_category="crowdsourced",
          base_priority="mittel",
          disinformation_flag=False,
          trust_tag=TRUST_UNVERIFIED,
          verification_notes="Crowdsourcing lane — fuse like Ukraine eVorog → Delta before C2 display.",
          correlation_hint="uav_muuga",
      ),
      SocmintScenario(
          scenario_id="routine_ferry_traffic",
          weight=17,
          platform="x",
          user_name="Kiel Ferry Cam",
          user_username="kiel_ferry_cam",
          tweet=(
              "Routine: Scandlines Abfahrt Kiel on time, kein Security-Event. #Ferry #Ostsee"
          ),
          latitude=54.32,
          longitude=10.145,
          intel_category="routine",
          base_priority="niedrig",
          disinformation_flag=False,
          trust_tag=TRUST_LOW,
          verification_notes="Nominal traffic; suppress in NBA unless correlated incident.",
      ),
  ]


_SCENARIOS: list[SocmintScenario] | None = None
_WEIGHTS: list[int] | None = None


def scenario_pool() -> list[SocmintScenario]:
    global _SCENARIOS
    if _SCENARIOS is None:
        _SCENARIOS = _scenarios()
    return _SCENARIOS


def pick_scenario(rng: random.Random) -> SocmintScenario:
    global _WEIGHTS
    pool = scenario_pool()
    if _WEIGHTS is None:
        _WEIGHTS = [s.weight for s in pool]
    return rng.choices(pool, weights=_WEIGHTS, k=1)[0]


def apply_simulated_verification(
    scenario: SocmintScenario, rng: random.Random
) -> dict[str, Any]:
    """
    Simuliert NiFi/Cloudera-Vorfilterung: Bild-Reuse, Sensor-Korrelation, Bot-Kampagne.

    Kinetische UNVERIFIED-Meldungen können per Zufall (Demo) auf SENSOR_CORROBORATED springen,
    wenn ``correlation_hint`` gesetzt ist (~45 %).
    """
    trust_tag = scenario.trust_tag
    disinfo = scenario.disinformation_flag
    notes = scenario.verification_notes
    priority = scenario.base_priority

    if (
        scenario.trust_tag == TRUST_UNVERIFIED
        and scenario.correlation_hint
        and rng.random() < 0.45
    ):
        trust_tag = TRUST_CORROBORATED
        notes = (
            notes + " | Demo fusion: mock SAR/AIS/DAS corroboration within 12 min."
        ).strip()
        if priority == "mittel":
            priority = "hoch"

    if scenario.intel_category == "disinformation" and disinfo:
        priority = "hoch"

    metrics_retweets = rng.randint(5, 800)
    metrics_likes = rng.randint(20, 4000)
    metrics_replies = rng.randint(0, 120)
    if disinfo:
        metrics_retweets = rng.randint(500, 12000)
        metrics_likes = rng.randint(800, 25000)
    elif trust_tag == TRUST_CORROBORATED:
        metrics_retweets = rng.randint(50, 2500)

    return {
        "trust_tag": trust_tag,
        "disinformation_flag": disinfo,
        "verification_notes": notes,
        "priority": priority,
        "metrics_retweets": metrics_retweets,
        "metrics_likes": metrics_likes,
        "metrics_replies": metrics_replies,
    }


def scenario_to_record(scenario: SocmintScenario, rng: random.Random, ts: str) -> dict[str, Any]:
    v = apply_simulated_verification(scenario, rng)
    return {
        "user_name": scenario.user_name,
        "user_username": scenario.user_username,
        "tweet": scenario.tweet,
        "ts": ts,
        "priority": v["priority"],
        "latitude": scenario.latitude,
        "longitude": scenario.longitude,
        "metrics_retweets": v["metrics_retweets"],
        "metrics_likes": v["metrics_likes"],
        "metrics_replies": v["metrics_replies"],
        "platform": scenario.platform,
        "intel_category": scenario.intel_category,
        "trust_tag": v["trust_tag"],
        "disinformation_flag": v["disinformation_flag"],
        "verification_notes": v["verification_notes"],
        "scenario_id": scenario.scenario_id,
        "image_recycle_id": scenario.image_recycle_id,
        "correlation_hint": scenario.correlation_hint,
    }


def generate_socmint_batch(count: int, *, seed: int | None = None) -> list[dict[str, Any]]:
    """NDJSON-freundliche Liste für Tests."""
    from datetime import datetime, timezone

    rng = random.Random(seed)
    ts = datetime.now(timezone.utc).isoformat()
    return [scenario_to_record(pick_scenario(rng), rng, ts) for _ in range(count)]