"""
Regression pins for ``is_baltic_water`` — inland/coastal cities (land) and offshore samples (sea).

Used by ``test_baltic_water_placement.py``. Extend when a map review finds land leakage.
"""

from __future__ import annotations

from typing import List, Tuple

Pin = Tuple[str, float, float]  # name, lon, lat

# Must NOT classify as Baltic sea (no CHANNEL fairway at pin).
LAND_PINS: List[Pin] = [
    # Germany — coast & islands
    ("Kiel inland", 10.12, 54.32),
    ("Lübeck", 10.69, 53.87),
    ("Schwerin", 11.41, 53.63),
    ("Rostock city", 12.10, 54.09),
    ("Greifswald", 13.39, 54.10),
    ("Stralsund town", 13.08, 54.31),
    ("Rügen interior", 13.22, 54.52),
    ("Neubrandenburg", 13.26, 53.56),
    ("Wismar hinterland", 11.45, 53.90),
    ("Plön lakes", 10.42, 54.15),
    ("Oldenburg Holstein", 10.90, 54.31),
    # Denmark (Baltic drainage / straits region)
    ("Copenhagen area", 12.57, 55.68),
    ("Odense", 10.39, 55.40),
    ("Aarhus", 10.20, 56.15),
    ("Bornholm interior", 14.92, 55.13),
    # Poland
    ("Szczecin", 14.55, 53.43),
    ("Gdańsk city", 18.65, 54.35),
    ("Słupsk", 17.03, 54.46),
    # Sweden (Baltic coast hinterland)
    ("Malmö", 13.00, 55.61),
    ("Kalmar", 16.36, 56.66),
    ("Stockholm city", 18.07, 59.33),
    ("Uppsala", 17.64, 59.86),
    ("Visby town", 18.30, 57.64),
    ("Gotland interior", 18.50, 57.50),
    # Finland
    ("Helsinki city", 24.94, 60.17),
    ("Espoo inland", 24.65, 60.22),
    ("Turku city", 22.27, 60.45),
    ("Tampere", 23.76, 61.50),
    # Estonia
    ("Tallinn old town", 24.75, 59.44),
    ("Keila", 24.42, 59.40),
    ("Rapla", 24.80, 58.99),
    ("Tartu", 26.73, 58.38),
    ("Rakke", 26.25, 58.97),
    ("Haapsalu town", 23.54, 58.94),
    # Latvia
    ("Riga center", 24.11, 56.95),
    ("Valmiera", 25.43, 57.54),
    ("Liepāja town", 21.01, 56.51),
    # Lithuania
    ("Klaipėda town", 21.13, 55.71),
    ("Telšiai", 22.25, 55.98),
    ("Vilnius", 25.28, 54.69),
    # Russia (Baltic littoral inland)
    ("Kaliningrad city", 20.51, 54.71),
    ("Pskov", 28.33, 57.81),
]

# Must classify as Baltic sea (approaches / fairways).
WATER_PINS: List[Pin] = [
    ("Kiel roadstead", 10.35, 54.42),
    ("Rostock offshore", 12.05, 54.25),
    ("Strelasund mid", 13.05, 54.52),
    ("Arkona offshore", 15.65, 55.15),
    ("Gdańsk Bay", 19.05, 54.62),
    ("Bornholm passage", 15.60, 55.25),
    ("Gotland E lane", 19.50, 57.05),
    ("Irbe Strait", 21.50, 57.45),
    ("Gulf of Riga mid", 23.25, 57.40),
    ("Moonsund", 22.20, 58.40),
    ("Tallinn road", 24.70, 59.55),
    ("GoF mid", 25.80, 59.70),
    ("Ventspils roads", 21.00, 57.55),
    ("Öresund approach", 12.85, 56.25),
    ("Sassnitz offshore", 13.55, 54.48),
]
