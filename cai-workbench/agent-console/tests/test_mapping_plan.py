from src.mapping_plan import build_mapping_plan, resolve_golden_example


def test_resolve_sanctioned_ais():
    r = resolve_golden_example(
        "Which SanctionedMaritimePlatform records have a latest AISTrackReport position?"
    )
    assert r["ok"] is True
    assert r["example"]["id"] == "gq_sanctioned_ais"


def test_resolve_gps_jamming_de():
    r = resolve_golden_example("ProbableGPSJamming GPSElectronicWarfareIndicator adsb_nic")
    assert r["ok"] is True
    assert r["example"]["id"] == "gq_gps_jamming"


def test_resolve_fishing_kiel():
    r = resolve_golden_example("fishing vessels vicinity of kiel port")
    assert r["ok"] is True
    assert r["example"]["id"] == "gq_fishing_kiel"


def test_mapping_plan_tables():
    ex = resolve_golden_example("sanctioned latest ais")["example"]
    plan = build_mapping_plan(ex)
    assert "sanctioned_vessels" in plan["tables"]
    assert "ais_events_ice" in plan["tables"]
    assert plan["warehouse"]["schema"] == "defense"