-- Seed Data: 5 Platforms, ~50 Equipment, ~250 Sensors, Thresholds

-- ===================== PLATFORMS =====================
INSERT INTO platforms (platform_id, platform_name, platform_type, latitude, longitude, region, timezone, status, commissioned_at)
VALUES
    ('ALPHA',   'Alpha Station',    'Deep-water Drilling',      28.5,   -88.5,  'Gulf of Mexico',   'America/Chicago',      'ACTIVE', '2018-03-15'),
    ('BRAVO',   'Bravo Platform',   'Production & Separation',  57.5,   1.5,    'North Sea',        'Europe/London',        'ACTIVE', '2015-07-22'),
    ('CHARLIE', 'Charlie FPSO',     'Floating Production',      -25.0,  -43.0,  'Santos Basin',     'America/Sao_Paulo',    'ACTIVE', '2020-11-10'),
    ('DELTA',   'Delta Jack-up',    'Shallow Water',            26.5,   51.5,   'Persian Gulf',     'Asia/Dubai',           'ACTIVE', '2019-06-01'),
    ('ECHO',    'Echo Semi-sub',    'Semi-submersible',         -22.5,  -40.0,  'Campos Basin',     'America/Sao_Paulo',    'ACTIVE', '2021-02-28')
ON CONFLICT (platform_id) DO NOTHING;

-- ===================== EQUIPMENT =====================
-- Alpha Station (11 equipment)
INSERT INTO equipment (equipment_id, platform_id, equipment_name, equipment_type, manufacturer, model, install_date, status)
VALUES
    ('ALPHA-PUMP-01',   'ALPHA', 'Main Mud Pump A',        'pump',         'National Oilwell',  'NOV-14P220', '2018-03-15', 'OPERATIONAL'),
    ('ALPHA-PUMP-02',   'ALPHA', 'Main Mud Pump B',        'pump',         'National Oilwell',  'NOV-14P220', '2018-03-15', 'OPERATIONAL'),
    ('ALPHA-COMP-01',   'ALPHA', 'Gas Compressor A',       'compressor',   'Atlas Copco',       'ZR-900',     '2018-03-15', 'OPERATIONAL'),
    ('ALPHA-COMP-02',   'ALPHA', 'Gas Compressor B',       'compressor',   'Atlas Copco',       'ZR-900',     '2018-03-15', 'OPERATIONAL'),
    ('ALPHA-TURB-01',   'ALPHA', 'Power Turbine A',        'turbine',      'GE Aviation',       'LM2500',     '2018-03-15', 'OPERATIONAL'),
    ('ALPHA-TURB-02',   'ALPHA', 'Power Turbine B',        'turbine',      'GE Aviation',       'LM2500',     '2018-03-15', 'OPERATIONAL'),
    ('ALPHA-GEN-01',    'ALPHA', 'Diesel Generator',       'generator',    'Caterpillar',       'CAT-3516',   '2018-03-15', 'OPERATIONAL'),
    ('ALPHA-SEP-01',    'ALPHA', 'Oil/Gas Separator',      'separator',    'Schlumberger',      'SLB-SEP-V2', '2018-03-15', 'OPERATIONAL'),
    ('ALPHA-HEAT-01',   'ALPHA', 'Process Heater',         'heater',       'Waukesha',          'WK-APG36',   '2018-03-15', 'OPERATIONAL'),
    ('ALPHA-VALVE-01',  'ALPHA', 'BOP Control System',     'valve',        'Cameron',           'CAM-BOP-18', '2018-03-15', 'OPERATIONAL'),
    ('ALPHA-CRANE-01',  'ALPHA', 'Platform Crane',         'crane',        'Liebherr',          'LHM-550',    '2018-03-15', 'OPERATIONAL')
ON CONFLICT (equipment_id) DO NOTHING;

-- Bravo Platform (10 equipment)
INSERT INTO equipment (equipment_id, platform_id, equipment_name, equipment_type, manufacturer, model, install_date, status)
VALUES
    ('BRAVO-PUMP-01',   'BRAVO', 'Export Pump A',          'pump',         'Sulzer',            'MSD-RO',     '2015-07-22', 'OPERATIONAL'),
    ('BRAVO-PUMP-02',   'BRAVO', 'Export Pump B',          'pump',         'Sulzer',            'MSD-RO',     '2015-07-22', 'OPERATIONAL'),
    ('BRAVO-COMP-01',   'BRAVO', 'Gas Lift Compressor',    'compressor',   'Siemens',           'STC-SV',     '2015-07-22', 'OPERATIONAL'),
    ('BRAVO-TURB-01',   'BRAVO', 'Main Power Turbine',     'turbine',      'Rolls-Royce',       'RB211',      '2015-07-22', 'OPERATIONAL'),
    ('BRAVO-GEN-01',    'BRAVO', 'Emergency Generator',    'generator',    'Caterpillar',       'CAT-3512',   '2015-07-22', 'OPERATIONAL'),
    ('BRAVO-SEP-01',    'BRAVO', 'Three-Phase Separator',  'separator',    'FMC Technologies', 'FMC-3PS',    '2015-07-22', 'OPERATIONAL'),
    ('BRAVO-SEP-02',    'BRAVO', 'Water Treatment Unit',   'separator',    'Veolia',            'VEO-WTU',    '2015-07-22', 'OPERATIONAL'),
    ('BRAVO-HEAT-01',   'BRAVO', 'Crude Oil Heater',       'heater',       'Waukesha',          'WK-APG36',   '2015-07-22', 'OPERATIONAL'),
    ('BRAVO-VALVE-01',  'BRAVO', 'Subsea Choke Valve',     'valve',        'Cameron',           'CAM-SC-12',  '2015-07-22', 'OPERATIONAL'),
    ('BRAVO-METER-01',  'BRAVO', 'Fiscal Flow Meter',      'meter',        'Emerson',           'CMF-400',    '2015-07-22', 'OPERATIONAL')
ON CONFLICT (equipment_id) DO NOTHING;

-- Charlie FPSO (10 equipment)
INSERT INTO equipment (equipment_id, platform_id, equipment_name, equipment_type, manufacturer, model, install_date, status)
VALUES
    ('CHARLIE-PUMP-01', 'CHARLIE', 'Topside Pump A',       'pump',         'Flowserve',         'FS-HPX',     '2020-11-10', 'OPERATIONAL'),
    ('CHARLIE-PUMP-02', 'CHARLIE', 'Topside Pump B',       'pump',         'Flowserve',         'FS-HPX',     '2020-11-10', 'OPERATIONAL'),
    ('CHARLIE-COMP-01', 'CHARLIE', 'VRU Compressor',       'compressor',   'MAN Energy',        'MAN-VRU',    '2020-11-10', 'OPERATIONAL'),
    ('CHARLIE-COMP-02', 'CHARLIE', 'Gas Injection Comp',   'compressor',   'MAN Energy',        'MAN-GIC',    '2020-11-10', 'OPERATIONAL'),
    ('CHARLIE-TURB-01', 'CHARLIE', 'FPSO Turbine A',       'turbine',      'GE Aviation',       'LM6000',     '2020-11-10', 'OPERATIONAL'),
    ('CHARLIE-TURB-02', 'CHARLIE', 'FPSO Turbine B',       'turbine',      'GE Aviation',       'LM6000',     '2020-11-10', 'OPERATIONAL'),
    ('CHARLIE-SEP-01',  'CHARLIE', 'Main Separator',       'separator',    'Aker Solutions',    'AK-SEP',     '2020-11-10', 'OPERATIONAL'),
    ('CHARLIE-HEAT-01', 'CHARLIE', 'Gas Dryer',            'heater',       'Prosernat',         'PRO-GD',     '2020-11-10', 'OPERATIONAL'),
    ('CHARLIE-SWIV-01', 'CHARLIE', 'Turret Swivel',        'swivel',       'SBM Offshore',      'SBM-TS',     '2020-11-10', 'OPERATIONAL'),
    ('CHARLIE-OFFLD-01','CHARLIE', 'Offloading System',    'offloading',   'SBM Offshore',      'SBM-OL',     '2020-11-10', 'OPERATIONAL')
ON CONFLICT (equipment_id) DO NOTHING;

-- Delta Jack-up (9 equipment)
INSERT INTO equipment (equipment_id, platform_id, equipment_name, equipment_type, manufacturer, model, install_date, status)
VALUES
    ('DELTA-PUMP-01',   'DELTA', 'Drilling Mud Pump',      'pump',         'National Oilwell',  'NOV-12P160', '2019-06-01', 'OPERATIONAL'),
    ('DELTA-PUMP-02',   'DELTA', 'Cement Pump',            'pump',         'Halliburton',       'HAL-HT400',  '2019-06-01', 'OPERATIONAL'),
    ('DELTA-COMP-01',   'DELTA', 'Air Compressor',         'compressor',   'Ingersoll Rand',    'IR-SSR-UP6', '2019-06-01', 'OPERATIONAL'),
    ('DELTA-TURB-01',   'DELTA', 'Power Turbine',          'turbine',      'Solar Turbines',    'SOL-T60',    '2019-06-01', 'OPERATIONAL'),
    ('DELTA-GEN-01',    'DELTA', 'Main Generator',         'generator',    'Caterpillar',       'CAT-3516',   '2019-06-01', 'OPERATIONAL'),
    ('DELTA-GEN-02',    'DELTA', 'Backup Generator',       'generator',    'Caterpillar',       'CAT-3512',   '2019-06-01', 'OPERATIONAL'),
    ('DELTA-DRAW-01',   'DELTA', 'Drawworks',              'drawworks',    'National Oilwell',  'NOV-ADS10',  '2019-06-01', 'OPERATIONAL'),
    ('DELTA-JACK-01',   'DELTA', 'Jack-up Leg System A',   'jack_system',  'GustoMSC',          'CJ46',       '2019-06-01', 'OPERATIONAL'),
    ('DELTA-CRANE-01',  'DELTA', 'Pedestal Crane',         'crane',        'NOV',               'NOV-PC',     '2019-06-01', 'OPERATIONAL')
ON CONFLICT (equipment_id) DO NOTHING;

-- Echo Semi-sub (10 equipment)
INSERT INTO equipment (equipment_id, platform_id, equipment_name, equipment_type, manufacturer, model, install_date, status)
VALUES
    ('ECHO-PUMP-01',    'ECHO', 'Mud Pump A',              'pump',         'National Oilwell',  'NOV-14P220', '2021-02-28', 'OPERATIONAL'),
    ('ECHO-PUMP-02',    'ECHO', 'Mud Pump B',              'pump',         'National Oilwell',  'NOV-14P220', '2021-02-28', 'OPERATIONAL'),
    ('ECHO-COMP-01',    'ECHO', 'HP Compressor',           'compressor',   'Atlas Copco',       'ZR-1100',    '2021-02-28', 'OPERATIONAL'),
    ('ECHO-TURB-01',    'ECHO', 'Dual-fuel Turbine A',     'turbine',      'GE Aviation',       'LM2500+G4',  '2021-02-28', 'OPERATIONAL'),
    ('ECHO-TURB-02',    'ECHO', 'Dual-fuel Turbine B',     'turbine',      'GE Aviation',       'LM2500+G4',  '2021-02-28', 'OPERATIONAL'),
    ('ECHO-GEN-01',     'ECHO', 'Emergency Gen',           'generator',    'Caterpillar',       'CAT-3516',   '2021-02-28', 'OPERATIONAL'),
    ('ECHO-SEP-01',     'ECHO', 'HP Separator',            'separator',    'TechnipFMC',        'TFMC-HPS',   '2021-02-28', 'OPERATIONAL'),
    ('ECHO-BOP-01',     'ECHO', 'BOP Stack',               'bop',          'Cameron',           'CAM-18-15K', '2021-02-28', 'OPERATIONAL'),
    ('ECHO-RISER-01',   'ECHO', 'Riser Tensioner',         'riser',        'Aker Solutions',    'AK-RT',      '2021-02-28', 'OPERATIONAL'),
    ('ECHO-DP-01',      'ECHO', 'DP Thruster System',      'thruster',     'Rolls-Royce',       'RR-UUC',     '2021-02-28', 'OPERATIONAL')
ON CONFLICT (equipment_id) DO NOTHING;

-- ===================== SENSOR THRESHOLDS =====================
INSERT INTO sensor_thresholds (sensor_type, subtype, min_normal, max_normal, warning_low, warning_high, critical_low, critical_high, unit)
VALUES
    ('temperature', 'bearing_temp',      30.0,  85.0,   25.0,  100.0,  15.0,  120.0, 'celsius'),
    ('temperature', 'exhaust_temp',     200.0, 450.0,  180.0,  500.0, 150.0,  600.0, 'celsius'),
    ('temperature', 'ambient_temp',     -10.0,  45.0,  -15.0,   50.0, -25.0,   60.0, 'celsius'),
    ('pressure',    'inlet_pressure',    50.0, 200.0,   40.0,  250.0,  20.0,  350.0, 'bar'),
    ('pressure',    'outlet_pressure',   20.0, 150.0,   15.0,  180.0,   5.0,  250.0, 'bar'),
    ('pressure',    'differential',       5.0,  50.0,    3.0,   60.0,   1.0,   80.0, 'bar'),
    ('vibration',   'axial_vibration',    0.0,  10.0,    0.0,   15.0,   0.0,   25.0, 'mm_s'),
    ('vibration',   'radial_vibration',   0.0,  12.0,    0.0,   18.0,   0.0,   30.0, 'mm_s'),
    ('vibration',   'velocity_vibration', 0.0,   8.0,    0.0,   12.0,   0.0,   20.0, 'mm_s'),
    ('flow_rate',   'flow_rate',        500.0, 8000.0, 200.0, 9000.0, 100.0, 10000.0,'m3_h'),
    ('flow_rate',   'flow_velocity',      1.0,  15.0,    0.5,   18.0,   0.2,   25.0, 'm_s'),
    ('flow_rate',   'mass_flow',        100.0, 5000.0,  50.0, 6000.0,  20.0,  8000.0,'kg_s')
ON CONFLICT (sensor_type, subtype) DO NOTHING;

-- ===================== SENSORS (partial - key sensors per equipment) =====================
-- We generate ~5 sensors per equipment = ~250 total

-- Helper: generate sensors for a pump
INSERT INTO sensors (sensor_id, equipment_id, platform_id, sensor_type, unit, min_range, max_range)
SELECT
    eq.equipment_id || '-' || s.suffix,
    eq.equipment_id,
    eq.platform_id,
    s.sensor_type,
    s.unit,
    s.min_range,
    s.max_range
FROM equipment eq
CROSS JOIN (VALUES
    ('TEMP-01', 'temperature', 'celsius',  0.0,   300.0),
    ('PRES-01', 'pressure',    'bar',      0.0,   500.0),
    ('VIBR-01', 'vibration',   'mm_s',     0.0,    50.0),
    ('FLOW-01', 'flow_rate',   'm3_h',     0.0, 10000.0),
    ('TEMP-02', 'temperature', 'celsius',  0.0,   300.0)
) AS s(suffix, sensor_type, unit, min_range, max_range)
WHERE eq.equipment_type IN ('pump', 'compressor', 'turbine', 'generator', 'separator', 'heater')
ON CONFLICT (sensor_id) DO NOTHING;

-- ===================== ALERT ESCALATION RULES =====================
INSERT INTO alert_escalation_rules (severity, action_type, target_endpoint, delay_seconds, max_retries, enabled)
VALUES
    ('INFO',     'log',     NULL,                           0, 1, true),
    ('WARNING',  'webhook', '/api/v1/alerts/warning',      0, 3, true),
    ('WARNING',  'email',   'ops-team@platform.internal',  60, 2, true),
    ('CRITICAL', 'webhook', '/api/v1/alerts/critical',     0, 3, true),
    ('CRITICAL', 'pagerduty','/api/v1/alerts/pagerduty',   0, 5, true),
    ('CRITICAL', 'sms',     '+1-555-OPS-TEAM',            30, 3, true)
ON CONFLICT DO NOTHING;
