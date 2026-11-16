-- One-time patch: North Sea trade + naval ports missing from older baltic_sea_harbours seed.
-- Safe to re-run after DELETE (Trino / iceberg.defense).

USE iceberg.defense;

DELETE FROM baltic_sea_harbours
WHERE name IN (
  'Hamburg', 'Bremerhaven', 'Wilhelmshaven', 'Emden', 'Cuxhaven',
  'Rotterdam', 'Antwerp', 'IJmuiden',
  'Eckernförde', 'Warnemünde', 'Baltiysk', 'Kronstadt'
);

INSERT INTO baltic_sea_harbours VALUES
('Hamburg', 'Germany', 53.551, 9.993),
('Bremerhaven', 'Germany', 53.539, 8.580),
('Wilhelmshaven', 'Germany', 53.513, 8.145),
('Emden', 'Germany', 53.367, 7.206),
('Cuxhaven', 'Germany', 53.867, 8.697),
('Rotterdam', 'Netherlands', 51.949, 4.145),
('Antwerp', 'Belgium', 51.263, 4.397),
('IJmuiden', 'Netherlands', 52.463, 4.556),
('Eckernförde', 'Germany', 54.473, 9.838),
('Warnemünde', 'Germany', 54.176, 12.089),
('Baltiysk', 'Russia', 54.64, 19.82),
('Kronstadt', 'Russia', 59.99, 29.77);