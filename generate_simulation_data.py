#!/usr/bin/env python3
"""
generate_simulation_data.py

Creates 100 realistic semiconductor fab log files in simulation_data/
for use with simulate_stream.py to stress-test the LogPipe pipeline.

Distribution:
  JSON (40): 10 P0 incident, 15 P1 error, 12 P2 routine, 3 deadletter
  LOG  (25):  5 P0 incident,  8 P1 error, 10 P2 routine, 2 deadletter
  CSV  (20):  4 P0 incident,  6 P1 error,  8 P2 routine, 2 deadletter
  XML  (15):  3 P0 incident,  5 P1 error,  6 P2 routine, 1 deadletter

Usage: python generate_simulation_data.py
"""

import json
from pathlib import Path
from datetime import datetime, timedelta

OUTPUT_DIR = Path("simulation_data")
OUTPUT_DIR.mkdir(exist_ok=True)

_clock = [datetime(2026, 4, 21, 0, 0, 30)]

def tick(minutes: float) -> str:
    _clock[0] += timedelta(minutes=minutes)
    return _clock[0].strftime("%Y-%m-%dT%H:%M:%SZ")

def at(h, m, s=0) -> str:
    return f"2026-04-21T{h:02d}:{m:02d}:{s:02d}Z"

created = []

def wj(name, events):
    (OUTPUT_DIR / name).write_text(json.dumps(events, indent=2))
    created.append(name)

def wl(name, lines):
    (OUTPUT_DIR / name).write_text("\n".join(lines) + "\n")
    created.append(name)

def wc(name, rows):
    (OUTPUT_DIR / name).write_text("\n".join(rows) + "\n")
    created.append(name)

def wx(name, body, exported):
    content = (
        '<?xml version="1.0" encoding="UTF-8"?>\n'
        f'<EventLog facility="semiconductor-fab" site="FAB-12" exported="{exported}">\n'
        f'{body}\n'
        '</EventLog>'
    )
    (OUTPUT_DIR / name).write_text(content)
    created.append(name)


# ════════════════════════════════════════════════════════════════════
#  JSON FILES  (40 total)
# ════════════════════════════════════════════════════════════════════

# ── P0 Incident files (10) ──────────────────────────────────────────

wj("json_P0_INCIDENT_PECVD01_20260421_020833_001.json", [
    {"timestamp": at(2, 8, 33), "source": "tool_PECVD-01", "event_type": "fire_alarm",
     "severity": "CRITICAL", "message": "Smoke detected in chamber exhaust duct — auto-shutoff triggered",
     "zone": "Fab Bay 3", "wafer_lot": "LOT-88521"},
    {"timestamp": at(2, 8, 47), "source": "tool_PECVD-01", "event_type": "gas_leak",
     "severity": "CRITICAL", "message": "Silane (SiH4) concentration above LEL threshold (11%). Emergency purge initiated.",
     "zone": "Fab Bay 3", "wafer_lot": "LOT-88521", "sensor_ppm": 3850},
    {"timestamp": at(2, 9, 2), "source": "machine_GAS-PANEL-01", "event_type": "isolation_valve_closed",
     "severity": "CRITICAL", "message": "SiH4 isolation valve closed by interlock. Gas supply halted to Fab Bay 3.",
     "zone": "Gas Cabinet A"},
    {"timestamp": at(2, 9, 10), "source": "machine_MES-SERVER-01", "event_type": "production_hold",
     "severity": "CRITICAL", "message": "PRODUCTION HOLD: Fab Bay 3 evacuated. Tools: PECVD-01, PECVD-02, PECVD-03.",
     "zone": "Fab Bay 3"},
])

wj("json_P0_INCIDENT_ETCH05_20260421_041102_002.json", [
    {"timestamp": at(4, 11, 2), "source": "tool_ETCH-05", "event_type": "gas_leak",
     "severity": "CRITICAL", "message": "HBr gas line pressure drop detected — possible line rupture upstream of MFC. Emergency purge.",
     "zone": "Etch Bay 1", "wafer_lot": "LOT-88534"},
    {"timestamp": at(4, 11, 6), "source": "machine_GAS-PANEL-02", "event_type": "isolation_valve_closed",
     "severity": "CRITICAL", "message": "HBr line isolation valve closed. Gas flow to Etch Bay 1 halted.",
     "zone": "Gas Cabinet B"},
    {"timestamp": at(4, 11, 20), "source": "machine_MES-SERVER-01", "event_type": "production_hold",
     "severity": "CRITICAL", "message": "PRODUCTION HOLD: Etch Bay 1. Tools ETCH-03 ETCH-05 offline. Personnel evacuated.",
     "zone": "Etch Bay 1"},
])

wj("json_P0_INCIDENT_ROBOT02_20260421_061505_003.json", [
    {"timestamp": at(6, 15, 5), "source": "machine_ROBOT-ARM-02", "event_type": "collision",
     "severity": "CRITICAL", "message": "Wafer handling arm collision at end-effector — E-stop engaged. Wafer breakage confirmed.",
     "zone": "Transfer Chamber 2", "wafer_lot": "LOT-88547", "arm_position": "slot_09"},
    {"timestamp": at(6, 15, 9), "source": "machine_MES-SERVER-01", "event_type": "equipment_alarm",
     "severity": "CRITICAL", "message": "EQUIPMENT ALARM: ROBOT-ARM-02 E-stop. Transfer chamber vented for recovery. Engineering paged.",
     "zone": "Transfer Chamber 2"},
    {"timestamp": at(6, 42, 0), "source": "machine_ROBOT-ARM-02", "event_type": "recovery_complete",
     "severity": "INFO", "message": "Broken wafer removed. Transfer chamber re-pumped. Robot arm re-qualified and returned to service.",
     "zone": "Transfer Chamber 2"},
])

wj("json_P0_INCIDENT_PUMP_CMP03_20260421_083041_004.json", [
    {"timestamp": at(8, 30, 41), "source": "device_PUMP-CMP-03", "event_type": "overheat",
     "severity": "CRITICAL", "message": "Slurry pump motor temperature exceeded 140°C. Cooling system failure. CMP tool halted.",
     "zone": "CMP Bay 2", "motor_temp_c": 144, "coolant_flow_lpm": 0.0},
    {"timestamp": at(8, 31, 0), "source": "tool_CMP-03", "event_type": "emergency_stop",
     "severity": "CRITICAL", "message": "CMP-03 emergency stop triggered by pump overheat interlock. Wafer carrier lifted.",
     "zone": "CMP Bay 2", "wafer_lot": "LOT-88558"},
])

wj("json_P0_INCIDENT_ETCH03_20260421_034719_005.json", [
    {"timestamp": at(3, 47, 19), "source": "tool_ETCH-03", "event_type": "arc_fault",
     "severity": "CRITICAL", "message": "RF arc detected in process chamber — plasma discharge abnormal. Recipe aborted.",
     "zone": "Etch Bay 1", "wafer_lot": "LOT-88528", "rf_power_w": 1420, "dc_bias_v": -310},
    {"timestamp": at(3, 47, 45), "source": "tool_ETCH-03", "event_type": "chamber_vent",
     "severity": "CRITICAL", "message": "ETCH-03 chamber vented after arc fault. RF generator powered down for inspection.",
     "zone": "Etch Bay 1"},
])

wj("json_P0_INCIDENT_IMPLANT01_20260421_054017_006.json", [
    {"timestamp": at(5, 40, 17), "source": "tool_IMPLANT-01", "event_type": "beam_current_fault",
     "severity": "CRITICAL", "message": "Ion beam current dropped to zero during boron implant. Source filament failure. Lot quarantined.",
     "zone": "Implant Bay", "wafer_lot": "LOT-88541", "beam_current_ma": 0.0, "expected_ma": 14.5},
    {"timestamp": at(5, 40, 30), "source": "machine_MES-SERVER-01", "event_type": "lot_quarantine",
     "severity": "CRITICAL", "message": "LOT-88541 quarantined pending implant dose verification. 25 wafers at risk.",
     "zone": "Implant Bay"},
])

wj("json_P0_INCIDENT_ROBOT01_20260421_141514_007.json", [
    {"timestamp": at(14, 15, 14), "source": "machine_ROBOT-ARM-01", "event_type": "wafer_drop",
     "severity": "CRITICAL", "message": "Wafer drop event at loadlock transfer. Wafer detected on floor sensor. E-stop engaged.",
     "zone": "Transfer Chamber 1", "wafer_lot": "LOT-88591"},
    {"timestamp": at(14, 15, 19), "source": "machine_MES-SERVER-01", "event_type": "equipment_alarm",
     "severity": "CRITICAL", "message": "ROBOT-ARM-01 E-stop. Chamber vented for recovery. Engineering team paged.",
     "zone": "Transfer Chamber 1"},
    {"timestamp": at(15, 2, 0), "source": "machine_ROBOT-ARM-01", "event_type": "recovery_complete",
     "severity": "INFO", "message": "Broken wafer removed. Chamber cleaned and re-pumped. Robot re-qualified. LOT-88591 wafer count adjusted (1 scrapped).",
     "zone": "Transfer Chamber 1"},
])

wj("json_P0_INCIDENT_PECVD03_20260421_161233_008.json", [
    {"timestamp": at(16, 12, 33), "source": "tool_PECVD-03", "event_type": "fire_alarm",
     "severity": "CRITICAL", "message": "Smoke detected in PECVD-03 exhaust manifold. Halon system pre-armed. Auto-shutoff engaged.",
     "zone": "Fab Bay 3", "wafer_lot": "LOT-88603"},
    {"timestamp": at(16, 12, 41), "source": "tool_PECVD-03", "event_type": "gas_leak",
     "severity": "CRITICAL", "message": "N2O concentration rising in exhaust duct (800 ppm). Exceeded 500 ppm alarm threshold.",
     "zone": "Fab Bay 3", "sensor_ppm": 820},
    {"timestamp": at(16, 13, 5), "source": "machine_GAS-PANEL-03", "event_type": "isolation_valve_closed",
     "severity": "CRITICAL", "message": "N2O and SiH4 isolation valves closed. Gas supply isolated to PECVD-03.",
     "zone": "Gas Cabinet C"},
])

wj("json_P0_INCIDENT_CHILLER_CMP01_20260421_041802_009.json", [
    {"timestamp": at(4, 18, 2), "source": "device_CHILLER-CMP-01", "event_type": "coolant_leak",
     "severity": "CRITICAL", "message": "Coolant leak at chiller outlet manifold. Flow dropped to 0.2 L/min (normal: 4.5 L/min). CMP-01 emergency stop.",
     "zone": "CMP Bay 1", "flow_lpm_normal": 4.5, "flow_lpm_actual": 0.2},
    {"timestamp": at(4, 18, 15), "source": "tool_CMP-01", "event_type": "emergency_stop",
     "severity": "CRITICAL", "message": "CMP-01 halted due to coolant loss. Wafer carrier retracted. Slurry drain initiated.",
     "zone": "CMP Bay 1"},
])

wj("json_P0_INCIDENT_GAS_PANEL02_20260421_225511_010.json", [
    {"timestamp": at(22, 55, 11), "source": "machine_GAS-PANEL-02", "event_type": "gas_leak",
     "severity": "CRITICAL", "message": "NF3 gas detector alarm in Gas Cabinet B — concentration 120 ppm (alarm: 10 ppm). Facility EHS notified.",
     "zone": "Gas Cabinet B", "sensor_ppm": 120},
    {"timestamp": at(22, 55, 18), "source": "machine_GAS-PANEL-02", "event_type": "isolation_valve_closed",
     "severity": "CRITICAL", "message": "NF3 supply isolation valve closed. EHS team dispatched to Gas Cabinet B.",
     "zone": "Gas Cabinet B"},
    {"timestamp": at(22, 55, 30), "source": "machine_MES-SERVER-01", "event_type": "production_hold",
     "severity": "CRITICAL", "message": "PRODUCTION HOLD: CVD area tools halted pending Gas Cabinet B clearance.",
     "zone": "Fab Bay 2"},
])

# ── P1 Error files (15) ─────────────────────────────────────────────

wj("json_P1_ETCH03_ESC_20260421_003005_011.json", [
    {"timestamp": at(0, 30, 5), "source": "tool_ETCH-03", "event_type": "temperature_reading",
     "severity": "WARNING", "message": "Electrostatic chuck temperature elevated: 62.3°C (setpoint: 55°C, limit: 60°C).",
     "zone": "Etch Bay 1", "wafer_lot": "LOT-88503", "actual_c": 62.3, "setpoint_c": 55.0},
    {"timestamp": at(0, 35, 11), "source": "tool_ETCH-03", "event_type": "temperature_reading",
     "severity": "ERROR", "message": "ESC temperature critically high 71.8°C — chuck failure risk. Recipe paused.",
     "zone": "Etch Bay 1", "wafer_lot": "LOT-88503", "actual_c": 71.8, "setpoint_c": 55.0},
])

wj("json_P1_PECVD02_pressure_20260421_015444_012.json", [
    {"timestamp": at(1, 54, 44), "source": "tool_PECVD-02", "event_type": "pressure_fault",
     "severity": "WARNING", "message": "Process pressure drifting above spec: 8.1 mTorr vs 7.0 mTorr target during oxide deposition.",
     "zone": "Fab Bay 3", "wafer_lot": "LOT-88510", "target_mtorr": 7.0, "actual_mtorr": 8.1},
    {"timestamp": at(2, 0, 5), "source": "tool_PECVD-02", "event_type": "pressure_fault",
     "severity": "ERROR", "message": "Pressure deviation persisted > 5 min. Deposition step aborted. Lot flagged for review.",
     "zone": "Fab Bay 3", "wafer_lot": "LOT-88510", "target_mtorr": 7.0, "actual_mtorr": 8.4},
])

wj("json_P1_CMP01_slurry_20260421_030515_013.json", [
    {"timestamp": at(3, 5, 15), "source": "tool_CMP-01", "event_type": "removal_rate_reading",
     "severity": "WARNING", "message": "Cu CMP removal rate below spec: 437 Å/min (target 500, lower limit 450).",
     "zone": "CMP Bay 1", "wafer_lot": "LOT-88520", "value": 437, "unit": "A/min", "setpoint": 500},
    {"timestamp": at(3, 10, 22), "source": "tool_CMP-01", "event_type": "removal_rate_reading",
     "severity": "ERROR", "message": "Cu removal rate critically low 388 Å/min — lot at risk of non-uniform planarization. Slurry replaced.",
     "zone": "CMP Bay 1", "wafer_lot": "LOT-88520", "value": 388, "unit": "A/min", "setpoint": 500},
])

wj("json_P1_LITHO07_overlay_20260421_041005_014.json", [
    {"timestamp": at(4, 10, 5), "source": "tool_LITHO-SCANNER-07", "event_type": "overlay_reading",
     "severity": "WARNING", "message": "Overlay approaching limit on M2 metal layer: 4.3 nm (spec ≤ 5.0 nm). Monitoring closely.",
     "zone": "Lithography Bay", "wafer_lot": "LOT-88530", "value": 4.3, "unit": "nm"},
    {"timestamp": at(4, 15, 0), "source": "tool_LITHO-SCANNER-07", "event_type": "overlay_reading",
     "severity": "ERROR", "message": "Overlay out of spec on M2 layer: 6.1 nm. Lot requires rework. Scanner lens re-qualified.",
     "zone": "Lithography Bay", "wafer_lot": "LOT-88530", "value": 6.1, "unit": "nm"},
])

wj("json_P1_DIFFUSION02_temp_20260421_130522_015.json", [
    {"timestamp": at(13, 5, 22), "source": "tool_DIFFUSION-02", "event_type": "temperature_deviation",
     "severity": "ERROR", "message": "Furnace zone 3 temperature deviated +18°C from setpoint for >120s. Wafer lot quarantined.",
     "zone": "Diffusion Bay", "wafer_lot": "LOT-88582", "setpoint_c": 950, "actual_c": 968, "duration_s": 127},
])

wj("json_P1_ANNEAL02_lamp_20260421_082233_016.json", [
    {"timestamp": at(8, 22, 33), "source": "tool_ANNEAL-02", "event_type": "lamp_failure",
     "severity": "ERROR", "message": "RTP lamp bank C output 18% below target during ramp. Possible lamp failure in positions C3, C4. Recipe paused.",
     "zone": "Anneal Bay", "wafer_lot": "LOT-88553"},
    {"timestamp": at(8, 23, 0), "source": "tool_ANNEAL-02", "event_type": "temperature_uniformity",
     "severity": "ERROR", "message": "Temperature uniformity sigma: 4.8°C (spec: ≤2.0°C). LOT-88553 quarantined pending lamp replacement.",
     "zone": "Anneal Bay", "wafer_lot": "LOT-88553"},
])

wj("json_P1_IMPLANT01_neutralizer_20260421_113000_017.json", [
    {"timestamp": at(11, 15, 44), "source": "tool_IMPLANT-01", "event_type": "neutralizer_fault",
     "severity": "WARNING", "message": "Beam neutraliser current low: 0.3 mA vs 0.8 mA target. Possible Xe feed issue. Monitoring.",
     "zone": "Implant Bay", "wafer_lot": "LOT-88571"},
    {"timestamp": at(11, 30, 0), "source": "tool_IMPLANT-01", "event_type": "neutralizer_fault",
     "severity": "ERROR", "message": "Neutraliser current dropped to zero. Wafer charge buildup risk. LOT-88571 processing paused.",
     "zone": "Implant Bay", "wafer_lot": "LOT-88571"},
])

wj("json_P1_WETBENCH02_chemical_20260421_071255_018.json", [
    {"timestamp": at(7, 12, 55), "source": "machine_WET-BENCH-02", "event_type": "chemical_concentration_oos",
     "severity": "WARNING", "message": "HF bath concentration: 0.38% (spec: 0.50% ±0.05%). Refreshing bath before next lot.",
     "zone": "Wet Clean Bay", "spec_pct": 0.50, "measured_pct": 0.38, "bath_id": "HF-BATH-02"},
])

wj("json_P1_CVD_film_thickness_20260421_010311_019.json", [
    {"timestamp": at(1, 3, 11), "source": "tool_PECVD-04", "event_type": "film_thickness_oos",
     "severity": "ERROR", "message": "TEOS oxide deposition thickness 312 nm vs 400 nm target. Run aborted after step 2.",
     "zone": "Fab Bay 3", "wafer_lot": "LOT-88505", "target_nm": 400, "measured_nm": 312, "recipe": "TEOS-STD-400NM-v3"},
])

wj("json_P1_ETCH04_endpoint_miss_20260421_025544_020.json", [
    {"timestamp": at(2, 55, 44), "source": "tool_ETCH-04", "event_type": "endpoint_miss",
     "severity": "ERROR", "message": "Etch endpoint not detected within 180s timeout. Wafer may be over-etched. Flagged for metrology.",
     "zone": "Etch Bay 1", "wafer_lot": "LOT-88514", "timeout_s": 180, "recipe": "POLY-GATE-ETCH-v7"},
])

wj("json_P1_LITHO_contamination_20260421_111809_021.json", [
    {"timestamp": at(11, 18, 9), "source": "tool_LITHO-SCANNER-07", "event_type": "contamination_alert",
     "severity": "ERROR", "message": "Reticle contamination: 47 particles detected (limit 5). Scanner locked pending cleaning.",
     "zone": "Lithography Bay", "wafer_lot": "LOT-88570", "particle_count": 47, "particle_size_nm": 120},
])

wj("json_P1_GAS_MFC_flow_20260421_172205_022.json", [
    {"timestamp": at(17, 22, 5), "source": "machine_GAS-PANEL-01", "event_type": "mass_flow_warning",
     "severity": "WARNING", "message": "N2O MFC flow deviation: commanded 500 sccm, measured 471 sccm. MFC recalibration required.",
     "zone": "Gas Cabinet A", "commanded_sccm": 500, "measured_sccm": 471},
])

wj("json_P1_PUMP_ETCH04_pressure_20260421_013502_023.json", [
    {"timestamp": at(1, 5, 33), "source": "device_PUMP-ETCH-04", "event_type": "pressure_rising",
     "severity": "WARNING", "message": "Exhaust pump inlet pressure rising: 2.1 Torr (baseline: 0.8 Torr). Possible partial blockage.",
     "zone": "Etch Bay 1"},
    {"timestamp": at(1, 18, 44), "source": "device_PUMP-ETCH-04", "event_type": "pressure_rising",
     "severity": "WARNING", "message": "Inlet pressure still elevated: 2.4 Torr. Possible partial blockage in foreline. Tool standby.",
     "zone": "Etch Bay 1"},
    {"timestamp": at(1, 35, 2), "source": "device_PUMP-ETCH-04", "event_type": "interlock_trip",
     "severity": "ERROR", "message": "Inlet pressure reached 3.8 Torr — interlock tripped. ETCH-04 taken offline. Maintenance notified.",
     "zone": "Etch Bay 1"},
])

wj("json_P1_VACUUM_GAUGE_drift_20260421_103000_024.json", [
    {"timestamp": at(10, 30, 0), "source": "device_VACUUM-PUMP-ETCH-01", "event_type": "sensor_drift",
     "severity": "WARNING", "message": "Capacitance manometer zero offset exceeded 0.5 mTorr (actual: 0.52 mTorr). Recalibration scheduled.",
     "zone": "Etch Bay 1", "zero_offset_mtorr": 0.52},
])

wj("json_P1_ANNEAL_ramp_fault_20260421_144839_025.json", [
    {"timestamp": at(14, 48, 39), "source": "tool_ANNEAL-02", "event_type": "ramp_rate_fault",
     "severity": "ERROR", "message": "Temperature ramp rate exceeded spec during RTP anneal: 85°C/s vs max 75°C/s. Thermal stress risk. Lot held.",
     "zone": "Anneal Bay", "wafer_lot": "LOT-88592", "max_ramp_cs": 75, "actual_ramp_cs": 85, "recipe": "RTP-NiSi-ANNEAL-v4"},
])

# ── P2 Routine files (12) ────────────────────────────────────────────

wj("json_P2_MES_shift_handover_day_20260421_080000_026.json", [
    {"timestamp": at(8, 0, 0), "source": "machine_MES-SERVER-01", "event_type": "shift_handover",
     "severity": "INFO", "message": "Shift handover complete. Day shift started. 28 active lots in fab.",
     "zone": "MES"},
    {"timestamp": at(8, 1, 15), "source": "machine_MES-SERVER-01", "event_type": "wip_snapshot",
     "severity": "INFO", "message": "WIP snapshot: PECVD (3 lots), ETCH (5 lots), LITHO (4 lots), CMP (2 lots), IMPL (2 lots), WET (6 lots), DIFF (3 lots), METROLOGY (3 lots).",
     "zone": "MES"},
])

wj("json_P2_PECVD01_routine_deposition_20260421_000203_027.json", [
    {"timestamp": at(0, 2, 3), "source": "tool_PECVD-01", "event_type": "recipe_loaded",
     "severity": "INFO", "message": "Recipe OXIDE-STD-500NM-v5 loaded for LOT-88500.",
     "zone": "Fab Bay 3", "wafer_lot": "LOT-88500"},
    {"timestamp": at(0, 4, 2), "source": "tool_PECVD-01", "event_type": "preconditioning_started",
     "severity": "INFO", "message": "Chamber pre-conditioning started. Estimated duration: 15 min.",
     "zone": "Fab Bay 3"},
    {"timestamp": at(0, 19, 30), "source": "tool_PECVD-01", "event_type": "preconditioning_complete",
     "severity": "INFO", "message": "Pre-conditioning complete. Process pressure stabilised at 7.01 mTorr.",
     "zone": "Fab Bay 3"},
    {"timestamp": at(0, 20, 0), "source": "tool_PECVD-01", "event_type": "deposition_started",
     "severity": "INFO", "message": "Deposition started for LOT-88500 (25 wafers, target 500 nm TEOS oxide).",
     "zone": "Fab Bay 3", "wafer_lot": "LOT-88500"},
    {"timestamp": at(0, 52, 18), "source": "tool_PECVD-01", "event_type": "deposition_complete",
     "severity": "INFO", "message": "Deposition complete. Measured thickness 498 nm (spec: 500±10 nm). PASS.",
     "zone": "Fab Bay 3", "wafer_lot": "LOT-88500"},
])

wj("json_P2_LITHO03_routine_exposure_20260421_020145_028.json", [
    {"timestamp": at(2, 1, 45), "source": "tool_LITHO-SCANNER-03", "event_type": "reticle_qualified",
     "severity": "INFO", "message": "Reticle WL-POLY-GATE-R11 loaded and qualified. Alignment marks verified OK.",
     "zone": "Lithography Bay"},
    {"timestamp": at(2, 3, 0), "source": "tool_LITHO-SCANNER-03", "event_type": "exposure_started",
     "severity": "INFO", "message": "Exposure sequence started for LOT-88513 (25 wafers, M1 poly gate layer).",
     "zone": "Lithography Bay", "wafer_lot": "LOT-88513"},
    {"timestamp": at(2, 44, 10), "source": "tool_LITHO-SCANNER-03", "event_type": "exposure_complete",
     "severity": "INFO", "message": "Exposure complete. 25 wafers passed inline CD check (target 45 nm ±3 nm, mean 44.8 nm). PASS.",
     "zone": "Lithography Bay", "wafer_lot": "LOT-88513"},
])

wj("json_P2_PECVD02_PM_window_20260421_090000_029.json", [
    {"timestamp": at(9, 0, 0), "source": "machine_MES-SERVER-01", "event_type": "pm_started",
     "severity": "INFO", "message": "Preventive maintenance window started for tool_PECVD-02. Estimated: 4 hours. 3 lots re-routed.",
     "zone": "MES"},
    {"timestamp": at(9, 5, 12), "source": "tool_PECVD-02", "event_type": "pump_down_complete",
     "severity": "INFO", "message": "Pump-down sequence complete post-PM. Base pressure 5.2e-7 Torr. Proceeding to chamber seasoning.",
     "zone": "Fab Bay 3"},
    {"timestamp": at(16, 30, 0), "source": "tool_PECVD-02", "event_type": "pm_complete",
     "severity": "INFO", "message": "PM complete. Chamber seasoning finished (50 dummy wafer cycles). Tool qualified and returned to production.",
     "zone": "Fab Bay 3"},
])

wj("json_P2_MES_wip_yield_report_20260421_180000_030.json", [
    {"timestamp": at(17, 0, 0), "source": "machine_MES-SERVER-01", "event_type": "wip_snapshot",
     "severity": "INFO", "message": "End-of-day WIP: 33 active lots, 2 on hold, 0 tools in PM, 0 offline."},
    {"timestamp": at(18, 0, 0), "source": "machine_MES-SERVER-01", "event_type": "yield_report",
     "severity": "INFO", "message": "Yield report: D0 (killer defect density) 0.08/cm². Target ≤0.10/cm². ON TARGET."},
])

wj("json_P2_CMP01_routine_polish_20260421_053110_031.json", [
    {"timestamp": at(5, 31, 10), "source": "tool_CMP-01", "event_type": "polishing_started",
     "severity": "INFO", "message": "Cu CMP polishing started for LOT-88540. Carrier 45 rpm, platen 42 rpm, downforce 3.2 psi.",
     "zone": "CMP Bay 1", "wafer_lot": "LOT-88540"},
    {"timestamp": at(6, 1, 50), "source": "tool_CMP-01", "event_type": "endpoint_detected",
     "severity": "INFO", "message": "Endpoint detected at 62s (target 60s ±10s). Post-CMP thickness 215 nm. PASS.",
     "zone": "CMP Bay 1", "wafer_lot": "LOT-88540"},
])

wj("json_P2_WETBENCH03_SC1_routine_20260421_031022_032.json", [
    {"timestamp": at(3, 10, 22), "source": "machine_WET-BENCH-03", "event_type": "bath_temp_warning",
     "severity": "WARNING", "message": "SC1 bath temperature 78.2°C — 3°C above setpoint. Heater controller PID tuning may be required.",
     "zone": "Wet Clean Bay"},
    {"timestamp": at(3, 11, 0), "source": "machine_WET-BENCH-03", "event_type": "lot_hold",
     "severity": "INFO", "message": "LOT-88519 load held pending SC1 temperature stabilisation.",
     "zone": "Wet Clean Bay", "wafer_lot": "LOT-88519"},
    {"timestamp": at(3, 15, 55), "source": "machine_WET-BENCH-03", "event_type": "bath_temp_ok",
     "severity": "INFO", "message": "SC1 temperature back within spec (75.1°C). LOT-88519 load resumed.",
     "zone": "Wet Clean Bay", "wafer_lot": "LOT-88519"},
])

wj("json_P2_ETCH02_routine_etch_20260421_100530_033.json", [
    {"timestamp": at(10, 5, 30), "source": "tool_ETCH-02", "event_type": "recipe_started",
     "severity": "INFO", "message": "Poly gate etch started for LOT-88563. Recipe POLY-GATE-v8, 25 wafers.",
     "zone": "Etch Bay 1", "wafer_lot": "LOT-88563"},
    {"timestamp": at(10, 22, 0), "source": "tool_ETCH-02", "event_type": "etch_complete",
     "severity": "INFO", "message": "Etch complete. CD mean 44.1 nm (spec 45±5 nm). PASS. Lot dispatched to metrology.",
     "zone": "Etch Bay 1", "wafer_lot": "LOT-88563"},
])

wj("json_P2_MES_shift_handover_swing_20260421_130000_034.json", [
    {"timestamp": at(13, 0, 0), "source": "machine_MES-SERVER-01", "event_type": "shift_handover",
     "severity": "INFO", "message": "Shift handover. Swing shift taking over. 31 active lots in fab.",
     "zone": "MES"},
    {"timestamp": at(13, 1, 0), "source": "machine_MES-SERVER-01", "event_type": "wip_snapshot",
     "severity": "INFO", "message": "Active lots by area: ETCH (6), PECVD (4), LITHO (5), CMP (3), IMPL (3), DIFF (4), WET (6).",
     "zone": "MES"},
])

wj("json_P2_METROLOGY_CDSEM_20260421_061500_035.json", [
    {"timestamp": at(6, 15, 0), "source": "machine_METROLOGY-CD-SEM-01", "event_type": "measurement_complete",
     "severity": "INFO", "message": "CD-SEM measurement complete for LOT-88546 post-etch. Mean gate CD: 43.9 nm. Cpk: 1.52. PASS.",
     "zone": "Metrology Bay", "wafer_lot": "LOT-88546"},
])

wj("json_P2_PUMP_vibration_normal_20260421_010000_036.json", [
    {"timestamp": at(1, 0, 0), "source": "device_PUMP-CVD-02", "event_type": "vibration_reading",
     "severity": "INFO", "message": "Dry pump vibration within normal range: 1.2 mm/s (limit 3.5 mm/s).",
     "zone": "Fab Bay 2", "value": 1.2, "unit": "mm/s", "limit": 3.5},
])

wj("json_P2_MES_shift_handover_night_20260421_000114_037.json", [
    {"timestamp": at(0, 1, 14), "source": "machine_MES-SERVER-01", "event_type": "shift_handover",
     "severity": "INFO", "message": "Shift handover complete. Night shift started. 25 active lots in fab.",
     "zone": "MES"},
    {"timestamp": at(0, 3, 45), "source": "machine_MES-SERVER-01", "event_type": "lot_dispatch",
     "severity": "INFO", "message": "LOT-88501 dispatched to WET-BENCH-01 for post-dep clean.",
     "zone": "MES"},
])

# ── Deadletter files (3) ─────────────────────────────────────────────

wj("json_DL_UNKNOWN_SRC_20260421_073311_038.json", [
    {"timestamp": at(7, 33, 11), "source": "LEGACY_CTRL_SYS_7B", "event_type": "UNKNOWN_FAULT_TYPE_0x4F",
     "severity": "error", "message": "%%FLT:0x4F ERR:timeout@REG[0x3A2] — ctx=fab_bay_3 unit=pcvd_chamber_x",
     "raw_code": "0x4F", "register": "0x3A2", "unit": "pcvd_chamber_x"},
    {"timestamp": at(7, 33, 45), "source": "LEGACY_CTRL_SYS_7B", "event_type": "WATCHDOG_TIMEOUT",
     "severity": "error", "message": "Watchdog timeout on process thread #7. No heartbeat for 45s.",
     "thread_id": 7, "timeout_s": 45},
])

wj("json_DL_GARBLED_20260421_152244_039.json", [
    {"timestamp": at(15, 22, 44), "source": "EQUIP_UNKNOWN_09", "event_type": "UNDEFINED_EVENT",
     "severity": "error", "message": "?? unrecognized alarm class 'TypeX' -- check vendor manual §12.4"},
    {"timestamp": at(15, 23, 0), "source": "EQUIP_UNKNOWN_09", "event_type": "UNDEFINED_EVENT",
     "severity": "error", "message": "Repeated: alarm class 'TypeX' persists. No handler registered."},
])

wj("json_DL_LOW_CONFIDENCE_20260421_210055_040.json", [
    {"timestamp": at(21, 0, 55), "source": "sensor_node_unknown_class_B44", "event_type": "telemetry_anomaly",
     "severity": "error", "message": "Unstructured telemetry burst received. Format unknown. Vendor: unregistered.",
     "vendor_id": "UNREGISTERED", "payload_hex": "4F 3A 00 FF 12 AB"},
])


# ════════════════════════════════════════════════════════════════════
#  LOG FILES  (25 total)
# ════════════════════════════════════════════════════════════════════

# ── P0 Incident log files (5) ───────────────────────────────────────

wl("log_P0_INCIDENT_ETCH05_HBr_20260421_040211_001.log", [
    "2026-04-21T04:02:11Z CRITICAL tool_ETCH-05 SAFETY INTERLOCK: HBr gas line pressure drop. Possible line rupture upstream of MFC. Emergency purge. Evacuate Etch Bay 1.",
    "2026-04-21T04:02:14Z CRITICAL machine_GAS-PANEL-01 HBr line isolation valve closed by interlock. Gas flow halted.",
    "2026-04-21T04:03:30Z CRITICAL machine_MES-SERVER-01 PRODUCTION HOLD: Etch Bay 1 (tools ETCH-03, ETCH-05). Awaiting safety clearance.",
    "2026-04-21T04:45:00Z INFO machine_MES-SERVER-01 Etch Bay 1 safety inspection complete. False alarm — pressure transducer fault, no leak. Bay cleared.",
    "2026-04-21T04:46:20Z INFO machine_GAS-PANEL-01 HBr isolation valve re-opened. Normal gas supply restored.",
    "2026-04-21T04:48:00Z INFO machine_MES-SERVER-01 PRODUCTION HOLD lifted for Etch Bay 1. Tools returning to ready state.",
])

wl("log_P0_INCIDENT_PECVD01_fire_20260421_021433_002.log", [
    "2026-04-21T02:14:33Z CRITICAL tool_PECVD-01 Smoke detected in chamber exhaust duct — auto-shutoff triggered.",
    "2026-04-21T02:14:51Z CRITICAL tool_PECVD-01 SiH4 concentration above LEL threshold (12%). Emergency purge initiated. sensor_ppm=4200",
    "2026-04-21T02:15:01Z CRITICAL machine_GAS-PANEL-01 SiH4 isolation valve closed. All PECVD gas flows halted.",
    "2026-04-21T02:15:15Z CRITICAL machine_MES-SERVER-01 PRODUCTION HOLD: Fab Bay 3 evacuated. All PECVD tools offline.",
    "2026-04-21T03:10:00Z INFO machine_MES-SERVER-01 Fire safety inspection complete. Exhaust duct cleaned. Halon system re-armed.",
    "2026-04-21T03:15:00Z INFO machine_GAS-PANEL-01 SiH4 supply restored after safety inspection. Flow verified at 200 sccm.",
    "2026-04-21T03:18:00Z INFO machine_MES-SERVER-01 PRODUCTION HOLD lifted Fab Bay 3. PECVD tools available for re-qualification.",
])

wl("log_P0_INCIDENT_ROBOT01_wafer_drop_20260421_141514_003.log", [
    "2026-04-21T14:15:14Z CRITICAL machine_ROBOT-ARM-01 Wafer drop event at loadlock transfer. Floor sensor triggered. E-stop engaged. lot=LOT-88591",
    "2026-04-21T14:15:19Z CRITICAL machine_MES-SERVER-01 EQUIPMENT ALARM: ROBOT-ARM-01 E-stop. Transfer chamber vented for recovery. Engineering paged.",
    "2026-04-21T14:16:00Z CRITICAL machine_MES-SERVER-01 Transfer chamber offline. 3 lots re-routed to alternate cluster tool.",
    "2026-04-21T15:00:00Z INFO machine_ROBOT-ARM-01 Broken wafer removed. Transfer chamber cleaned and re-pumped to 1.2e-7 Torr.",
    "2026-04-21T15:10:00Z INFO machine_ROBOT-ARM-01 Robot arm re-qualified. Wafer placement accuracy verified ±0.2 mm. PASS.",
    "2026-04-21T15:12:00Z INFO machine_MES-SERVER-01 ROBOT-ARM-01 returned to production. LOT-88591 wafer count adjusted (1 scrapped).",
])

wl("log_P0_INCIDENT_CHILLER_CMP01_coolant_20260421_041802_004.log", [
    "2026-04-21T04:18:02Z CRITICAL device_CHILLER-CMP-01 Coolant leak at outlet manifold. Flow 0.2 L/min (normal 4.5 L/min). CMP-01 emergency stop.",
    "2026-04-21T04:18:10Z CRITICAL tool_CMP-01 Emergency stop triggered by coolant loss interlock. Wafer carrier retracted. Slurry drain initiated.",
    "2026-04-21T04:19:00Z CRITICAL machine_MES-SERVER-01 CMP Bay 1 offline. LOT-88532 affected. Transferred to CMP-02.",
    "2026-04-21T06:30:00Z INFO device_CHILLER-CMP-01 Coolant leak repaired. Manifold fitting replaced. System leak-tested. PASS.",
    "2026-04-21T06:45:00Z INFO tool_CMP-01 CMP-01 re-qualified. Slurry flow verified 350 mL/min. Coolant flow 4.6 L/min. Ready.",
])

wl("log_P0_INCIDENT_IMPLANT01_beam_20260421_054017_005.log", [
    "2026-04-21T05:40:17Z CRITICAL tool_IMPLANT-01 Ion beam current dropped to zero during boron implant. Source filament failure. lot=LOT-88541",
    "2026-04-21T05:40:30Z CRITICAL machine_MES-SERVER-01 LOT-88541 quarantined. 25 wafers at dose risk. Implant log saved for analysis.",
    "2026-04-21T05:41:00Z CRITICAL tool_IMPLANT-01 IMPLANT-01 offline. Source chamber vented for filament replacement.",
    "2026-04-21T09:15:00Z INFO tool_IMPLANT-01 Filament replaced. Source re-conditioned. Beam current 14.3 mA (target 14.5 mA). PASS.",
    "2026-04-21T09:20:00Z INFO machine_MES-SERVER-01 IMPLANT-01 returned to production. LOT-88541 disposition: partial re-implant on alternate tool.",
])

# ── P1 Error log files (8) ──────────────────────────────────────────

wl("log_P1_PUMP_ETCH04_pressure_20260421_010533_006.log", [
    "2026-04-21T01:05:33Z WARNING device_PUMP-ETCH-04 Exhaust pump inlet pressure rising. Current 2.1 Torr (baseline 0.8 Torr). Monitoring.",
    "2026-04-21T01:18:44Z WARNING device_PUMP-ETCH-04 Inlet pressure still elevated 2.4 Torr. Possible partial blockage in foreline. Tool standby.",
    "2026-04-21T01:35:02Z ERROR device_PUMP-ETCH-04 Inlet pressure 3.8 Torr — interlock tripped. ETCH-04 offline. Maintenance notified.",
    "2026-04-21T01:36:00Z INFO machine_MES-SERVER-01 LOT-88507 re-routed to ETCH-05 due to ETCH-04 offline.",
])

wl("log_P1_DIFFUSION02_thermocouple_20260421_070000_007.log", [
    "2026-04-21T07:00:00Z WARNING tool_DIFFUSION-02 Zone 2 TC reading intermittent. Suspected thermocouple wire degradation. Maintenance ticket opened.",
    "2026-04-21T07:00:45Z WARNING tool_DIFFUSION-02 Switching to redundant TC for Zone 2. Monitoring temperature stability.",
    "2026-04-21T07:10:00Z WARNING tool_DIFFUSION-02 Redundant TC temperature lag 2.8°C vs primary. Furnace in supervised mode.",
    "2026-04-21T08:30:00Z INFO tool_DIFFUSION-02 Thermocouple replaced during brief PM. Zone 2 temperature verified stable. Tool resuming production.",
])

wl("log_P1_ANNEAL02_lamp_failure_20260421_082233_008.log", [
    "2026-04-21T08:22:33Z ERROR tool_ANNEAL-02 RTP lamp bank C output 18% below target during ramp. Lamp failure C3 C4 suspected. Recipe paused.",
    "2026-04-21T08:23:00Z ERROR tool_ANNEAL-02 Temperature uniformity sigma 4.8°C (spec ≤2.0°C). LOT-88553 quarantined pending lamp replacement.",
    "2026-04-21T08:25:00Z INFO machine_MES-SERVER-01 LOT-88553 placed on quality hold. Engineering notified for lamp swap.",
    "2026-04-21T11:00:00Z INFO tool_ANNEAL-02 Lamp bank C replaced. RTP qualified with dummy wafers. Uniformity sigma 1.4°C. PASS.",
])

wl("log_P1_LITHO07_overlay_20260421_040518_009.log", [
    "2026-04-21T04:05:18Z WARNING tool_LITHO-SCANNER-07 Overlay approaching limit M2 layer: 4.3 nm (spec ≤5.0 nm). lot=LOT-88530",
    "2026-04-21T04:15:00Z ERROR tool_LITHO-SCANNER-07 Overlay out of spec: 6.1 nm on M2 layer. LOT-88530 rework required. Scanner locked.",
    "2026-04-21T04:16:00Z INFO tool_LITHO-SCANNER-07 Lens heating compensation enabled. Wafer stage re-levelled. Qualifying with reference wafer.",
    "2026-04-21T04:45:00Z INFO tool_LITHO-SCANNER-07 Scanner re-qualified. Reference wafer overlay 1.9 nm. Production resumed.",
])

wl("log_P1_IMPLANT01_Xe_feed_20260421_113000_010.log", [
    "2026-04-21T11:15:44Z WARNING tool_IMPLANT-01 Beam neutraliser current low: 0.3 mA (target 0.8 mA). Possible Xe feed issue. lot=LOT-88571",
    "2026-04-21T11:30:00Z ERROR tool_IMPLANT-01 Neutraliser current dropped to zero. Wafer charge buildup risk. LOT-88571 processing paused.",
    "2026-04-21T11:35:00Z INFO tool_IMPLANT-01 Xe supply valve inspected. Minor restriction cleared. Flow restored.",
    "2026-04-21T11:55:00Z INFO tool_IMPLANT-01 Xe supply restored. Neutraliser current 0.79 mA. LOT-88571 resumed.",
])

wl("log_P1_WETBENCH03_SC1_temp_20260421_031022_011.log", [
    "2026-04-21T03:10:22Z WARNING machine_WET-BENCH-03 SC1 bath temperature 78.2°C — 3°C above setpoint. PID tuning may be required.",
    "2026-04-21T03:11:00Z INFO machine_WET-BENCH-03 LOT-88519 load held pending temperature stabilisation.",
    "2026-04-21T03:15:55Z INFO machine_WET-BENCH-03 SC1 temperature within spec (75.1°C). LOT-88519 load resumed.",
])

wl("log_P1_ETCH03_endpoint_miss_20260421_025544_012.log", [
    "2026-04-21T02:55:44Z ERROR tool_ETCH-03 Etch endpoint not detected within 180s timeout. Wafer may be over-etched. lot=LOT-88514",
    "2026-04-21T02:56:00Z ERROR tool_ETCH-03 Recipe aborted. Wafer lot flagged for metrology review. Tool entering idle state.",
    "2026-04-21T02:57:00Z INFO machine_MES-SERVER-01 LOT-88514 routed to CD-SEM for post-etch inspection. Hold status: PENDING_REVIEW.",
])

wl("log_P1_PECVD04_film_thickness_20260421_010311_013.log", [
    "2026-04-21T01:03:11Z ERROR tool_PECVD-04 TEOS oxide deposition thickness 312 nm vs 400 nm target. Run aborted after step 2. lot=LOT-88505",
    "2026-04-21T01:04:00Z ERROR tool_PECVD-04 Deposition rate calculated at 4.2 nm/min vs target 6.4 nm/min. Precursor flow anomaly suspected.",
    "2026-04-21T01:05:00Z INFO machine_MES-SERVER-01 LOT-88505 quarantined. Metrology required before rework decision.",
    "2026-04-21T01:06:00Z INFO tool_PECVD-04 TEOS source bubbler temperature verified: 38.2°C (target 40°C). Bubbler heater replaced.",
])

# ── P2 Routine log files (10) ───────────────────────────────────────

wl("log_P2_MES_night_shift_start_20260421_000114_014.log", [
    "2026-04-21T00:01:14Z INFO machine_MES-SERVER-01 Shift handover complete. Night shift started. 25 active lots in fab.",
    "2026-04-21T00:03:45Z INFO tool_PECVD-01 Recipe OXIDE-STD-500NM-v5 loaded for LOT-88500.",
    "2026-04-21T00:04:02Z INFO tool_PECVD-01 Chamber pre-conditioning started. Estimated 15 min.",
    "2026-04-21T00:07:00Z INFO machine_MES-SERVER-01 Night shift WIP: 25 lots active, 3 tools in idle, 2 tools in PM.",
    "2026-04-21T00:19:30Z INFO tool_PECVD-01 Pre-conditioning complete. Process pressure stabilised at 7.01 mTorr.",
    "2026-04-21T00:20:00Z INFO tool_PECVD-01 Deposition started for LOT-88500 (25 wafers, target 500 nm TEOS oxide).",
])

wl("log_P2_PECVD01_full_run_20260421_001103_015.log", [
    "2026-04-21T00:11:03Z INFO tool_PECVD-01 Recipe NITRIDE-SIN-200NM-v2 loaded for LOT-88502.",
    "2026-04-21T00:12:00Z INFO tool_PECVD-01 SiH4 and NH3 flows verified: SiH4 200 sccm, NH3 400 sccm. Ratios nominal.",
    "2026-04-21T00:13:15Z INFO tool_PECVD-01 Chamber conditioning complete. RF power stable at 300W.",
    "2026-04-21T00:14:00Z INFO tool_PECVD-01 SiN deposition started for LOT-88502.",
    "2026-04-21T00:41:20Z INFO tool_PECVD-01 Deposition complete. Thickness measured 201 nm (spec 200±5 nm). PASS.",
    "2026-04-21T00:42:30Z INFO machine_MES-SERVER-01 LOT-88502 dispatched to METROLOGY-ELLIPS for film characterisation.",
])

wl("log_P2_LITHO03_exposure_routine_20260421_020145_016.log", [
    "2026-04-21T02:00:00Z INFO tool_LITHO-SCANNER-03 Reticle WL-POLY-GATE-R11 loaded. Alignment marks verified.",
    "2026-04-21T02:01:45Z INFO tool_LITHO-SCANNER-03 Exposure started for LOT-88513 (25 wafers, M1 poly gate).",
    "2026-04-21T02:44:10Z INFO tool_LITHO-SCANNER-03 Exposure complete. CD mean 44.8 nm (target 45 nm). PASS.",
    "2026-04-21T02:45:00Z INFO machine_MES-SERVER-01 LOT-88513 dispatched to ETCH for poly gate etch.",
])

wl("log_P2_CMP01_routine_polish_20260421_053110_017.log", [
    "2026-04-21T05:30:00Z INFO tool_CMP-01 Slurry supply primed. Cu CMP recipe CU-POLISH-STD-v9 queued for LOT-88540.",
    "2026-04-21T05:31:10Z INFO tool_CMP-01 Polishing started. Carrier 45 rpm, platen 42 rpm, downforce 3.2 psi.",
    "2026-04-21T06:01:50Z INFO tool_CMP-01 Endpoint detected at 62s (target 60s ±10s). Post-CMP thickness 215 nm. PASS.",
    "2026-04-21T06:05:00Z INFO machine_MES-SERVER-01 LOT-88540 dispatched to WET-BENCH for post-CMP clean.",
])

wl("log_P2_METROLOGY_CDSEM_20260421_061500_018.log", [
    "2026-04-21T06:15:00Z INFO machine_METROLOGY-CD-SEM-01 CD-SEM measurement for LOT-88546 post-etch. Mean gate CD 43.9 nm. Cpk 1.52. PASS.",
    "2026-04-21T06:16:30Z INFO machine_MES-SERVER-01 LOT-88546 released from metrology hold. Dispatched to next process step.",
    "2026-04-21T06:20:00Z INFO machine_METROLOGY-CD-SEM-01 Next measurement queued: LOT-88547 incoming from LITHO.",
])

wl("log_P2_MES_day_shift_start_20260421_080000_019.log", [
    "2026-04-21T08:00:00Z INFO machine_MES-SERVER-01 Day shift handover complete. 28 active lots in fab.",
    "2026-04-21T08:01:15Z INFO machine_MES-SERVER-01 Lots by area: PECVD 3, ETCH 5, LITHO 4, CMP 2, IMPL 2, WET 6, DIFF 3, METROLOGY 3.",
    "2026-04-21T08:02:00Z INFO machine_MES-SERVER-01 Tool status: 0 offline, 1 in PM (PECVD-02), 22 active, 4 idle.",
    "2026-04-21T08:05:00Z INFO tool_ETCH-06 Recipe SHALLOW-TRENCH-ETCH-v4 loaded for LOT-88555.",
    "2026-04-21T08:06:00Z INFO tool_ETCH-06 Plasma conditioning started. CF4/O2 flow established. Pressure 5.5 mTorr.",
])

wl("log_P2_PUMP_CVD02_vibration_20260421_010000_020.log", [
    "2026-04-21T01:00:00Z INFO device_PUMP-CVD-02 Dry pump vibration within normal range: 1.2 mm/s (limit 3.5 mm/s).",
    "2026-04-21T01:05:00Z WARNING device_PUMP-CVD-02 Vibration increasing: 2.9 mm/s. Possible bearing wear. Monitoring.",
    "2026-04-21T01:10:00Z INFO device_PUMP-CVD-02 Vibration stabilised at 2.7 mm/s. Maintenance inspection scheduled next PM.",
])

wl("log_P2_DIFFUSION02_routine_anneal_20260421_060000_021.log", [
    "2026-04-21T06:00:00Z INFO tool_DIFFUSION-02 Inert anneal started for LOT-88545. N2 ambient, temp setpoint 900°C.",
    "2026-04-21T06:02:00Z INFO tool_DIFFUSION-02 All 5 furnace zones within ±1°C of setpoint. Anneal in progress.",
    "2026-04-21T06:05:00Z INFO tool_DIFFUSION-02 Ambient O2 within spec during inert anneal: 0.8 ppm (limit 2.0 ppm).",
    "2026-04-21T07:30:00Z INFO tool_DIFFUSION-02 Anneal complete. Cool-down started. LOT-88545 to be unloaded at 300°C.",
    "2026-04-21T08:15:00Z INFO machine_MES-SERVER-01 LOT-88545 unloaded from DIFFUSION-02. Dispatched to implant for source/drain.",
])

wl("log_P2_PECVD02_PM_window_20260421_090000_022.log", [
    "2026-04-21T09:00:00Z INFO machine_MES-SERVER-01 PECVD-02 entering 4-hour PM window. 3 lots re-routed to PECVD-01 PECVD-03.",
    "2026-04-21T09:05:12Z INFO device_VACUUM-PUMP-PECVD-02 Pump down complete post-PM. Base pressure 5.2e-7 Torr.",
    "2026-04-21T09:30:00Z INFO tool_PECVD-02 Chamber wet clean complete. Particle baseline measurement: 2 particles >0.1 µm. PASS.",
    "2026-04-21T12:00:00Z INFO tool_PECVD-02 Seasoning started: 50 dummy wafer cycles at production recipe conditions.",
    "2026-04-21T16:30:00Z INFO tool_PECVD-02 PM complete. Tool re-qualified. Returned to production schedule.",
])

wl("log_P2_MES_eod_yield_20260421_180000_023.log", [
    "2026-04-21T17:00:00Z INFO machine_MES-SERVER-01 End-of-day WIP: 33 active lots, 2 on hold, 0 tools in PM, 0 offline.",
    "2026-04-21T18:00:00Z INFO machine_MES-SERVER-01 Yield report: D0=0.08/cm² (target ≤0.10/cm²). ON TARGET.",
    "2026-04-21T18:01:00Z INFO machine_MES-SERVER-01 Cycle time report: Mean lot cycle 8.4 days. Target 8.0 days. SLIGHTLY ABOVE TARGET.",
    "2026-04-21T18:02:00Z INFO machine_MES-SERVER-01 Equipment utilisation: PECVD 91.2%, ETCH 88.7%, LITHO 95.1%, CMP 79.4%.",
])

# ── Deadletter log files (2) ─────────────────────────────────────────

wl("log_DL_GARBLED_LEGACY_20260421_073311_024.log", [
    "2026-04-21T07:33:11Z ??? LEGACY_CTRL_SYS_7B %%FLT:0x4F ERR:timeout@REG[0x3A2] ctx=fab_bay_3",
    "2026-04-21T07:33:45Z ??? LEGACY_CTRL_SYS_7B WATCHDOG_TIMEOUT thread=7 no_heartbeat_s=45",
    "2026-04-21T07:34:02Z ??? LEGACY_CTRL_SYS_7B SYS_RESET initiated. bootloader v0.2.1 OK",
    "<<MALFORMED_LINE_BINARY_0x00 0xFF 0x3C 0x00>>",
    "2026-04-21T07:35:00Z ??? LEGACY_CTRL_SYS_7B Post-reset self-check: FAIL code=0xAB",
])

wl("log_DL_UNKNOWN_VENDOR_20260421_152244_025.log", [
    "2026-04-21T15:22:44Z error EQUIP_UNKNOWN_09 alarm_class=TypeX desc='??'",
    "2026-04-21T15:23:00Z error EQUIP_UNKNOWN_09 alarm_class=TypeX persists no_handler_registered",
    "2026-04-21T15:23:30Z error EQUIP_UNKNOWN_09 vendor=UNREGISTERED protocol=UNKNOWN payload_hex=4F3A00FF12AB",
])


# ════════════════════════════════════════════════════════════════════
#  CSV FILES  (20 total)
# ════════════════════════════════════════════════════════════════════

CSV_HDR = "timestamp,source,event_type,severity,message,zone,wafer_lot,value,unit,setpoint,limit_low,limit_high"

# ── P0 sensor limit breach (4) ──────────────────────────────────────

wc("csv_P0_PUMP_CVD02_vibration_20260421_010000_001.csv", [
    CSV_HDR,
    "2026-04-21T01:00:00Z,device_PUMP-CVD-02,vibration_reading,INFO,Dry pump vibration within normal range,Fab Bay 2,,1.2,mm/s,,,3.5",
    "2026-04-21T01:05:00Z,device_PUMP-CVD-02,vibration_reading,WARNING,Dry pump vibration increasing — bearing wear possible,Fab Bay 2,,2.9,mm/s,,,3.5",
    "2026-04-21T01:10:00Z,device_PUMP-CVD-02,vibration_reading,CRITICAL,Dry pump vibration exceeded limit — emergency shutoff,Fab Bay 2,,4.1,mm/s,,,3.5",
])

wc("csv_P0_ETCH03_ESC_temp_20260421_003005_002.csv", [
    CSV_HDR,
    "2026-04-21T00:30:05Z,tool_ETCH-03,temperature_reading,INFO,ESC temperature nominal at process start,Etch Bay 1,LOT-88503,50.1,degC,55.0,40.0,60.0",
    "2026-04-21T00:35:00Z,tool_ETCH-03,temperature_reading,WARNING,ESC temperature elevated,Etch Bay 1,LOT-88503,62.3,degC,55.0,40.0,60.0",
    "2026-04-21T00:40:00Z,tool_ETCH-03,temperature_reading,CRITICAL,ESC temperature critically high — chuck failure risk,Etch Bay 1,LOT-88503,74.1,degC,55.0,40.0,60.0",
])

wc("csv_P0_CHILLER_ETCH01_coolant_temp_20260421_050000_003.csv", [
    CSV_HDR,
    "2026-04-21T05:00:00Z,device_CHILLER-ETCH-01,coolant_temp_reading,INFO,Chiller outlet temperature nominal,Etch Bay 2,,18.1,degC,18.0,17.0,19.0",
    "2026-04-21T05:05:00Z,device_CHILLER-ETCH-01,coolant_temp_reading,WARNING,Chiller outlet temp above upper limit,Etch Bay 2,,19.8,degC,18.0,17.0,19.0",
    "2026-04-21T05:10:00Z,device_CHILLER-ETCH-01,coolant_temp_reading,CRITICAL,Chiller outlet temp critically high — coolant loss suspected,Etch Bay 2,,22.5,degC,18.0,17.0,19.0",
])

wc("csv_P0_GAS_PANEL02_SiH4_flow_20260421_020000_004.csv", [
    CSV_HDR,
    "2026-04-21T02:00:00Z,machine_GAS-PANEL-02,flow_reading,INFO,SiH4 MFC flow on target,Gas Cabinet B,LOT-88512,200.1,sccm,200.0,190.0,210.0",
    "2026-04-21T02:05:00Z,machine_GAS-PANEL-02,flow_reading,WARNING,SiH4 MFC flow below lower limit,Gas Cabinet B,LOT-88512,185.3,sccm,200.0,190.0,210.0",
    "2026-04-21T02:10:00Z,machine_GAS-PANEL-02,flow_reading,ERROR,SiH4 MFC flow critically low — deposition quality impacted,Gas Cabinet B,LOT-88512,162.7,sccm,200.0,190.0,210.0",
    "2026-04-21T02:15:00Z,machine_GAS-PANEL-02,flow_reading,CRITICAL,SiH4 MFC zero reading — possible MFC failure. Recipe aborted.,Gas Cabinet B,LOT-88512,0.0,sccm,200.0,190.0,210.0",
])

# ── P1 out-of-spec sensor readings (6) ──────────────────────────────

wc("csv_P1_CMP01_removal_rate_20260421_030000_005.csv", [
    CSV_HDR,
    "2026-04-21T03:00:00Z,tool_CMP-01,removal_rate_reading,INFO,Cu CMP removal rate on target,CMP Bay 1,LOT-88520,520.0,A/min,500.0,450.0,550.0",
    "2026-04-21T03:05:00Z,tool_CMP-01,removal_rate_reading,WARNING,Cu removal rate below spec — slurry may be degraded,CMP Bay 1,LOT-88520,437.0,A/min,500.0,450.0,550.0",
    "2026-04-21T03:10:00Z,tool_CMP-01,removal_rate_reading,ERROR,Cu removal rate critically low — non-uniform planarization risk,CMP Bay 1,LOT-88520,388.0,A/min,500.0,450.0,550.0",
])

wc("csv_P1_LITHO07_overlay_20260421_040000_006.csv", [
    CSV_HDR,
    "2026-04-21T04:00:00Z,tool_LITHO-SCANNER-07,overlay_reading,INFO,Overlay within spec on M2 layer,Lithography Bay,LOT-88530,1.8,nm,,,-5.0",
    "2026-04-21T04:05:00Z,tool_LITHO-SCANNER-07,overlay_reading,WARNING,Overlay approaching limit on M2 layer,Lithography Bay,LOT-88530,4.3,nm,,,-5.0",
    "2026-04-21T04:10:00Z,tool_LITHO-SCANNER-07,overlay_reading,ERROR,Overlay out of spec on M2 layer — lot requires rework,Lithography Bay,LOT-88530,6.1,nm,,,-5.0",
])

wc("csv_P1_IMPLANT01_dose_20260421_070000_007.csv", [
    CSV_HDR,
    "2026-04-21T07:00:00Z,tool_IMPLANT-01,dose_reading,INFO,As implant dose on target,Implant Bay,LOT-88551,1.01e14,ions/cm2,1.0e14,9.5e13,1.05e14",
    "2026-04-21T07:05:00Z,tool_IMPLANT-01,dose_reading,WARNING,As implant dose slightly above upper spec,Implant Bay,LOT-88551,1.07e14,ions/cm2,1.0e14,9.5e13,1.05e14",
    "2026-04-21T07:10:00Z,tool_IMPLANT-01,dose_reading,ERROR,As implant dose out of spec — lot quarantined,Implant Bay,LOT-88551,1.15e14,ions/cm2,1.0e14,9.5e13,1.05e14",
])

wc("csv_P1_DIFFUSION02_oxygen_20260421_060000_008.csv", [
    CSV_HDR,
    "2026-04-21T06:00:00Z,tool_DIFFUSION-02,oxygen_concentration,INFO,Ambient O2 within spec during inert anneal,Diffusion Bay,LOT-88545,0.8,ppm,,0.0,2.0",
    "2026-04-21T06:05:00Z,tool_DIFFUSION-02,oxygen_concentration,ERROR,O2 spike detected during anneal — wafer oxidation risk,Diffusion Bay,LOT-88545,8.4,ppm,,0.0,2.0",
])

wc("csv_P1_PECVD01_rf_power_20260421_002000_009.csv", [
    CSV_HDR,
    "2026-04-21T00:15:00Z,tool_PECVD-01,rf_power_reading,INFO,RF power delivery nominal,Fab Bay 3,LOT-88500,299.8,W,300.0,290.0,310.0",
    "2026-04-21T00:25:00Z,tool_PECVD-01,rf_power_reading,WARNING,RF power slightly below lower limit,Fab Bay 3,LOT-88500,285.1,W,300.0,290.0,310.0",
    "2026-04-21T00:30:00Z,tool_PECVD-01,rf_power_reading,ERROR,RF power critically low — plasma may not be sustained,Fab Bay 3,LOT-88500,261.4,W,300.0,290.0,310.0",
])

wc("csv_P1_ETCH03_dc_bias_20260421_003000_010.csv", [
    CSV_HDR,
    "2026-04-21T00:30:00Z,tool_ETCH-03,dc_bias_reading,INFO,DC bias stable during main etch,Etch Bay 1,LOT-88505,-285.0,V,-280.0,-300.0,-260.0",
    "2026-04-21T00:40:00Z,tool_ETCH-03,dc_bias_reading,WARNING,DC bias drifting toward lower limit,Etch Bay 1,LOT-88505,-304.0,V,-280.0,-300.0,-260.0",
    "2026-04-21T00:45:00Z,tool_ETCH-03,dc_bias_reading,ERROR,DC bias out of spec — plasma etch uniformity at risk,Etch Bay 1,LOT-88505,-315.0,V,-280.0,-300.0,-260.0",
])

# ── P2 nominal sensor readings (8) ──────────────────────────────────

wc("csv_P2_PECVD01_temp_pressure_20260421_000500_011.csv", [
    CSV_HDR,
    "2026-04-21T00:05:00Z,tool_PECVD-01,temperature_reading,INFO,Chamber wall temperature within spec,Fab Bay 3,LOT-88500,385.2,degC,385.0,375.0,395.0",
    "2026-04-21T00:10:00Z,tool_PECVD-01,pressure_reading,INFO,Process pressure stable during deposition,Fab Bay 3,LOT-88500,7.01,mTorr,7.0,6.5,7.5",
    "2026-04-21T00:15:00Z,tool_PECVD-01,rf_power_reading,INFO,RF power delivery nominal,Fab Bay 3,LOT-88500,299.8,W,300.0,290.0,310.0",
    "2026-04-21T00:20:00Z,tool_PECVD-01,temperature_reading,INFO,Chamber wall temperature stable mid-run,Fab Bay 3,LOT-88500,385.8,degC,385.0,375.0,395.0",
    "2026-04-21T00:30:00Z,tool_PECVD-01,pressure_reading,INFO,Process pressure stable mid-run,Fab Bay 3,LOT-88500,7.02,mTorr,7.0,6.5,7.5",
])

wc("csv_P2_ETCH04_nominal_20260421_083000_012.csv", [
    CSV_HDR,
    "2026-04-21T08:30:00Z,tool_ETCH-04,dc_bias_reading,INFO,DC bias stable at start of etch,Etch Bay 1,LOT-88555,-279.5,V,-280.0,-300.0,-260.0",
    "2026-04-21T08:35:00Z,tool_ETCH-04,temperature_reading,INFO,ESC temperature nominal,Etch Bay 1,LOT-88555,54.8,degC,55.0,40.0,60.0",
    "2026-04-21T08:40:00Z,tool_ETCH-04,rf_power_reading,INFO,RF power delivery stable,Etch Bay 1,LOT-88555,401.2,W,400.0,380.0,420.0",
    "2026-04-21T08:45:00Z,tool_ETCH-04,pressure_reading,INFO,Chamber pressure stable during main etch,Etch Bay 1,LOT-88555,5.52,mTorr,5.5,5.0,6.0",
])

wc("csv_P2_CMP02_nominal_polish_20260421_110000_013.csv", [
    CSV_HDR,
    "2026-04-21T11:00:00Z,tool_CMP-02,removal_rate_reading,INFO,Cu CMP removal rate on target,CMP Bay 2,LOT-88568,502.0,A/min,500.0,450.0,550.0",
    "2026-04-21T11:10:00Z,tool_CMP-02,removal_rate_reading,INFO,Cu CMP removal rate stable,CMP Bay 2,LOT-88568,498.5,A/min,500.0,450.0,550.0",
    "2026-04-21T11:20:00Z,tool_CMP-02,removal_rate_reading,INFO,Cu CMP removal rate on target — approaching endpoint,CMP Bay 2,LOT-88568,495.1,A/min,500.0,450.0,550.0",
])

wc("csv_P2_LITHO03_cd_readings_20260421_020200_014.csv", [
    CSV_HDR,
    "2026-04-21T02:02:00Z,tool_LITHO-SCANNER-03,cd_reading,INFO,Pre-exposure reference CD within spec,Lithography Bay,LOT-88513,45.0,nm,45.0,42.0,48.0",
    "2026-04-21T02:44:00Z,tool_LITHO-SCANNER-03,cd_reading,INFO,Post-exposure inline CD check passed,Lithography Bay,LOT-88513,44.8,nm,45.0,42.0,48.0",
])

wc("csv_P2_DIFFUSION02_temp_zones_20260421_060200_015.csv", [
    CSV_HDR,
    "2026-04-21T06:02:00Z,tool_DIFFUSION-02,temperature_reading,INFO,Furnace zone 1 within spec,Diffusion Bay,LOT-88545,900.2,degC,900.0,895.0,905.0",
    "2026-04-21T06:02:10Z,tool_DIFFUSION-02,temperature_reading,INFO,Furnace zone 2 within spec,Diffusion Bay,LOT-88545,899.8,degC,900.0,895.0,905.0",
    "2026-04-21T06:02:20Z,tool_DIFFUSION-02,temperature_reading,INFO,Furnace zone 3 within spec,Diffusion Bay,LOT-88545,900.5,degC,900.0,895.0,905.0",
    "2026-04-21T06:02:30Z,tool_DIFFUSION-02,temperature_reading,INFO,Furnace zone 4 within spec,Diffusion Bay,LOT-88545,900.1,degC,900.0,895.0,905.0",
    "2026-04-21T06:02:40Z,tool_DIFFUSION-02,temperature_reading,INFO,Furnace zone 5 within spec,Diffusion Bay,LOT-88545,899.9,degC,900.0,895.0,905.0",
])

wc("csv_P2_IMPLANT01_dose_nominal_20260421_120000_016.csv", [
    CSV_HDR,
    "2026-04-21T12:00:00Z,tool_IMPLANT-01,dose_reading,INFO,B implant dose on target,Implant Bay,LOT-88575,5.02e13,ions/cm2,5.0e13,4.75e13,5.25e13",
    "2026-04-21T12:30:00Z,tool_IMPLANT-01,dose_reading,INFO,B implant dose on target mid-run,Implant Bay,LOT-88575,5.01e13,ions/cm2,5.0e13,4.75e13,5.25e13",
])

wc("csv_P2_WETBENCH01_chemical_nominal_20260421_070000_017.csv", [
    CSV_HDR,
    "2026-04-21T07:00:00Z,machine_WET-BENCH-01,chemical_concentration,INFO,SC1 bath H2O2 concentration within spec,Wet Clean Bay,,0.25,pct,0.25,0.20,0.30",
    "2026-04-21T07:05:00Z,machine_WET-BENCH-01,chemical_concentration,INFO,SC1 bath NH4OH concentration within spec,Wet Clean Bay,,0.25,pct,0.25,0.20,0.30",
    "2026-04-21T07:10:00Z,machine_WET-BENCH-01,temperature_reading,INFO,SC1 bath temperature nominal,Wet Clean Bay,,75.2,degC,75.0,73.0,77.0",
])

wc("csv_P2_CHILLER_CVD01_nominal_20260421_090000_018.csv", [
    CSV_HDR,
    "2026-04-21T09:00:00Z,device_CHILLER-CVD-01,coolant_temp_reading,INFO,CVD chiller outlet temperature nominal,Fab Bay 2,,20.1,degC,20.0,19.0,21.0",
    "2026-04-21T09:30:00Z,device_CHILLER-CVD-01,coolant_temp_reading,INFO,CVD chiller outlet temperature stable,Fab Bay 2,,20.0,degC,20.0,19.0,21.0",
    "2026-04-21T10:00:00Z,device_CHILLER-CVD-01,coolant_flow_reading,INFO,Coolant flow nominal,Fab Bay 2,,4.4,L/min,4.5,4.0,5.0",
])

# ── Deadletter CSV (2) ───────────────────────────────────────────────

wc("csv_DL_CORRUPT_SENSOR_20260421_073311_019.csv", [
    "timestamp,source,event_type,severity,message,zone,wafer_lot,value,unit",
    "2026-04-21T07:33:11Z,SENSOR_NODE_UNKNOWN,UNDEFINED,???,,,,NaN,unknown",
    "CORRUPT_LINE|||||0xFF|0x00|overflow",
    "2026-04-21T07:34:00Z,SENSOR_NODE_UNKNOWN,UNDEFINED,error,unstructured telemetry burst,,,-999.0,unknown",
])

wc("csv_DL_UNKNOWN_FORMAT_20260421_152244_020.csv", [
    "ts,src,type,lvl,msg",
    "20260421T152244,EQUIP_UNK_09,TypeX,err,??",
    "20260421T152300,EQUIP_UNK_09,TypeX,err,alarm_class=TypeX no_handler",
    "not_a_timestamp,EQUIP_UNK_09,UNDEFINED,UNKNOWN,payload=4F3A00FF12AB",
])


# ════════════════════════════════════════════════════════════════════
#  XML FILES  (15 total)
# ════════════════════════════════════════════════════════════════════

# ── P0 XML incident files (3) ───────────────────────────────────────

wx("xml_P0_INCIDENT_CMP01_coolant_leak_20260421_041802_001.xml", """
  <Event>
    <timestamp>2026-04-21T04:18:02Z</timestamp>
    <source>device_CHILLER-CMP-01</source>
    <event_type>coolant_leak</event_type>
    <severity>CRITICAL</severity>
    <message>Coolant leak at chiller outlet manifold. Flow dropped to 0.2 L/min (normal 4.5 L/min). CMP tool emergency stop.</message>
    <zone>CMP Bay 1</zone>
    <flow_lpm_normal>4.5</flow_lpm_normal>
    <flow_lpm_actual>0.2</flow_lpm_actual>
  </Event>
  <Event>
    <timestamp>2026-04-21T04:18:15Z</timestamp>
    <source>tool_CMP-01</source>
    <event_type>emergency_stop</event_type>
    <severity>CRITICAL</severity>
    <message>CMP-01 emergency stop. Coolant loss interlock triggered. Wafer carrier retracted. Slurry drain initiated.</message>
    <zone>CMP Bay 1</zone>
    <wafer_lot>LOT-88532</wafer_lot>
  </Event>""", "2026-04-21T05:00:00Z")

wx("xml_P0_INCIDENT_IMPLANT01_beam_fault_20260421_054017_002.xml", """
  <Event>
    <timestamp>2026-04-21T05:40:17Z</timestamp>
    <source>tool_IMPLANT-01</source>
    <event_type>beam_current_fault</event_type>
    <severity>CRITICAL</severity>
    <message>Ion beam current dropped to zero during boron implant. Source filament failure. Lot quarantined.</message>
    <wafer_lot>LOT-88541</wafer_lot>
    <recipe>BORON-BODY-IMPLANT-v2</recipe>
    <beam_current_ma>0.0</beam_current_ma>
    <expected_ma>14.5</expected_ma>
  </Event>
  <Event>
    <timestamp>2026-04-21T05:40:30Z</timestamp>
    <source>machine_MES-SERVER-01</source>
    <event_type>lot_quarantine</event_type>
    <severity>CRITICAL</severity>
    <message>LOT-88541 quarantined. 25 wafers at implant dose risk. Engineering team paged.</message>
    <wafer_lot>LOT-88541</wafer_lot>
  </Event>""", "2026-04-21T06:00:00Z")

wx("xml_P0_INCIDENT_ETCH05_arc_fault_20260421_034719_003.xml", """
  <Event>
    <timestamp>2026-04-21T03:47:19Z</timestamp>
    <source>tool_ETCH-05</source>
    <event_type>arc_fault</event_type>
    <severity>CRITICAL</severity>
    <message>RF arc detected in process chamber — plasma discharge abnormal. Recipe aborted immediately.</message>
    <zone>Etch Bay 1</zone>
    <wafer_lot>LOT-88528</wafer_lot>
    <rf_power_w>1450</rf_power_w>
    <dc_bias_v>-320</dc_bias_v>
  </Event>
  <Event>
    <timestamp>2026-04-21T03:47:50Z</timestamp>
    <source>tool_ETCH-05</source>
    <event_type>chamber_vent</event_type>
    <severity>CRITICAL</severity>
    <message>ETCH-05 chamber vented after arc fault. RF generator powered down for inspection.</message>
    <zone>Etch Bay 1</zone>
  </Event>""", "2026-04-21T04:00:00Z")

# ── P1 XML error files (5) ──────────────────────────────────────────

wx("xml_P1_CVD_film_thickness_oos_20260421_010311_004.xml", """
  <Event>
    <timestamp>2026-04-21T01:03:11Z</timestamp>
    <source>tool_PECVD-04</source>
    <event_type>film_thickness_oos</event_type>
    <severity>ERROR</severity>
    <message>TEOS oxide deposition thickness 312 nm vs 400 nm target. Run aborted after step 2.</message>
    <wafer_lot>LOT-88505</wafer_lot>
    <target_nm>400</target_nm>
    <measured_nm>312</measured_nm>
    <recipe>TEOS-STD-400NM-v3</recipe>
  </Event>""", "2026-04-21T02:00:00Z")

wx("xml_P1_ETCH03_endpoint_miss_20260421_025544_005.xml", """
  <Event>
    <timestamp>2026-04-21T02:55:44Z</timestamp>
    <source>tool_ETCH-03</source>
    <event_type>endpoint_miss</event_type>
    <severity>ERROR</severity>
    <message>Etch endpoint not detected within 180s timeout. Wafer may be over-etched. Flagged for metrology.</message>
    <wafer_lot>LOT-88514</wafer_lot>
    <timeout_s>180</timeout_s>
    <recipe>POLY-GATE-ETCH-v7</recipe>
  </Event>""", "2026-04-21T03:00:00Z")

wx("xml_P1_WETBENCH02_chemical_oos_20260421_071255_006.xml", """
  <Event>
    <timestamp>2026-04-21T07:12:55Z</timestamp>
    <source>machine_WET-BENCH-02</source>
    <event_type>chemical_concentration_oos</event_type>
    <severity>WARNING</severity>
    <message>HF bath concentration 0.38% (spec 0.50% ±0.05%). Refreshing bath before next lot.</message>
    <zone>Wet Clean Bay</zone>
    <spec_pct>0.50</spec_pct>
    <measured_pct>0.38</measured_pct>
    <bath_id>HF-BATH-02</bath_id>
  </Event>""", "2026-04-21T08:00:00Z")

wx("xml_P1_LITHO03_focus_error_20260421_090530_007.xml", """
  <Event>
    <timestamp>2026-04-21T09:05:30Z</timestamp>
    <source>tool_LITHO-SCANNER-03</source>
    <event_type>focus_error</event_type>
    <severity>WARNING</severity>
    <message>Autofocus failed on wafer slot 7 — surface height out of range. Wafer excluded from exposure.</message>
    <wafer_lot>LOT-88561</wafer_lot>
    <failed_slot>7</failed_slot>
    <recipe>M1-METAL-LAYER-v12</recipe>
  </Event>""", "2026-04-21T10:00:00Z")

wx("xml_P1_ANNEAL02_ramp_fault_20260421_144839_008.xml", """
  <Event>
    <timestamp>2026-04-21T14:48:39Z</timestamp>
    <source>tool_ANNEAL-02</source>
    <event_type>ramp_rate_fault</event_type>
    <severity>ERROR</severity>
    <message>Temperature ramp rate exceeded spec during RTP anneal: 85°C/s vs max 75°C/s. Lot held for inspection.</message>
    <wafer_lot>LOT-88592</wafer_lot>
    <max_ramp_cs>75</max_ramp_cs>
    <actual_ramp_cs>85</actual_ramp_cs>
    <recipe>RTP-NiSi-ANNEAL-v4</recipe>
  </Event>""", "2026-04-21T15:00:00Z")

# ── P2 XML routine files (6) ─────────────────────────────────────────

wx("xml_P2_VACUUM_GAUGE_sensor_drift_20260421_103000_009.xml", """
  <Event>
    <timestamp>2026-04-21T10:30:00Z</timestamp>
    <source>device_VACUUM-PUMP-ETCH-01</source>
    <event_type>sensor_drift</event_type>
    <severity>INFO</severity>
    <message>Capacitance manometer zero offset exceeded 0.5 mTorr (actual 0.52 mTorr). Recalibration scheduled next PM.</message>
    <zone>Etch Bay 1</zone>
    <zero_offset_mtorr>0.52</zero_offset_mtorr>
  </Event>""", "2026-04-21T11:00:00Z")

wx("xml_P2_PECVD03_pressure_warning_20260421_154431_010.xml", """
  <Event>
    <timestamp>2026-04-21T15:44:31Z</timestamp>
    <source>tool_PECVD-03</source>
    <event_type>pressure_fault</event_type>
    <severity>WARNING</severity>
    <message>Process chamber pressure drifted above spec during deposition: 8.2 mTorr vs 7.0 mTorr target.</message>
    <zone>Fab Bay 3</zone>
    <wafer_lot>LOT-88597</wafer_lot>
    <target_mtorr>7.0</target_mtorr>
    <actual_mtorr>8.2</actual_mtorr>
  </Event>""", "2026-04-21T16:00:00Z")

wx("xml_P2_GAS_MFC_deviation_20260421_172205_011.xml", """
  <Event>
    <timestamp>2026-04-21T17:22:05Z</timestamp>
    <source>machine_GAS-PANEL-01</source>
    <event_type>mass_flow_warning</event_type>
    <severity>WARNING</severity>
    <message>N2O MFC flow deviation: commanded 500 sccm, measured 471 sccm. MFC recalibration required.</message>
    <zone>Gas Cabinet A</zone>
    <commanded_sccm>500</commanded_sccm>
    <measured_sccm>471</measured_sccm>
  </Event>""", "2026-04-21T18:00:00Z")

wx("xml_P2_VACUUM_PUMP_maintenance_20260421_190000_012.xml", """
  <Event>
    <timestamp>2026-04-21T19:00:00Z</timestamp>
    <source>device_VACUUM-PUMP-ETCH-02</source>
    <event_type>maintenance_due</event_type>
    <severity>INFO</severity>
    <message>Dry pump scheduled maintenance interval reached (2000 hrs). Schedule downtime for service.</message>
    <zone>Etch Bay 1</zone>
    <runtime_hours>2001</runtime_hours>
  </Event>""", "2026-04-21T19:30:00Z")

wx("xml_P2_LITHO_scanner_calibration_20260421_210000_013.xml", """
  <Event>
    <timestamp>2026-04-21T21:00:00Z</timestamp>
    <source>tool_LITHO-SCANNER-05</source>
    <event_type>calibration_reminder</event_type>
    <severity>INFO</severity>
    <message>Weekly lens aberration calibration due. Schedule 2-hour slot during next low-priority window.</message>
    <zone>Lithography Bay</zone>
    <last_calibration_date>2026-04-14</last_calibration_date>
  </Event>""", "2026-04-21T21:30:00Z")

wx("xml_P2_MES_end_of_day_20260421_230000_014.xml", """
  <Event>
    <timestamp>2026-04-21T23:00:00Z</timestamp>
    <source>machine_MES-SERVER-01</source>
    <event_type>end_of_day_report</event_type>
    <severity>INFO</severity>
    <message>End-of-day: 33 active lots, D0=0.08/cm² (target ≤0.10). All safety systems nominal.</message>
  </Event>
  <Event>
    <timestamp>2026-04-21T23:01:00Z</timestamp>
    <source>machine_MES-SERVER-01</source>
    <event_type>shift_handover</event_type>
    <severity>INFO</severity>
    <message>Swing to night shift handover. Outgoing crew: 31 lots processed. No outstanding safety holds.</message>
  </Event>""", "2026-04-21T23:30:00Z")

# ── Deadletter XML (1) ───────────────────────────────────────────────

wx("xml_DL_MALFORMED_VENDOR_20260421_073311_015.xml", """
  <Event>
    <timestamp>2026-04-21T07:33:11Z</timestamp>
    <source>LEGACY_CTRL_SYS_7B</source>
    <event_type>UNKNOWN_FAULT_TYPE_0x4F</event_type>
    <severity>error</severity>
    <message>%%FLT:0x4F ERR:timeout@REG[0x3A2] — ctx=fab_bay_3 unit=pcvd_chamber_x</message>
    <raw_code>0x4F</raw_code>
    <register>0x3A2</register>
  </Event>
  <CORRUPT_ELEMENT vendor="UNREGISTERED" payload="0x00 0xFF 0x3C"/>""", "2026-04-21T08:00:00Z")


# ════════════════════════════════════════════════════════════════════
#  FEW-SHOT RAG + TEMPORAL ANOMALY DEMO — CENTURA-12B
# ════════════════════════════════════════════════════════════════════

# ── Phase 1 (DL JSON files) ──────────────────────────────────────────────────
# Machine: tool_CENTURA-12B (new, no DynamoDB rule yet)
# Format: proprietary DCBX alarm codes — AI cannot confidently categorize
# Expected: category=unknown, confidence≈0.25 → dead letter / review queue
# Sorted alphabetically (json_DL_*) → land in Phase 1 regardless of batch size

wj("json_DL_CENTURA12B_20260421_091422_101.json", [
    {"timestamp": at(9, 14, 22), "source": "tool_CENTURA-12B",
     "event_type": "ALMID=0x4F2A",
     "severity": "WARNING",
     "message": "ALMID=0x4F2A GRP=DCBX MOD=PM3 CHNL=03 VAL=+237 THR=100 UNIT=mV SEQ=449812 RCP=ETCH_HBR_V4 LOT=M25B-019 WFR=W07"},
])

wj("json_DL_CENTURA12B_20260421_091705_102.json", [
    {"timestamp": at(9, 17, 5), "source": "tool_CENTURA-12B",
     "event_type": "ALMID=0x4F2A",
     "severity": "WARNING",
     "message": "ALMID=0x4F2A GRP=DCBX MOD=PM3 CHNL=03 VAL=+291 THR=100 UNIT=mV SEQ=449813 RCP=ETCH_HBR_V4 LOT=M25B-019 WFR=W08"},
])

wj("json_DL_CENTURA12B_20260421_091948_103.json", [
    {"timestamp": at(9, 19, 48), "source": "tool_CENTURA-12B",
     "event_type": "ALMID=0x4F2A",
     "severity": "WARNING",
     "message": "ALMID=0x4F2A GRP=DCBX MOD=PM3 CHNL=03 VAL=+318 THR=100 UNIT=mV SEQ=449814 RCP=ETCH_HBR_V4 LOT=M25B-019 WFR=W09"},
])

wj("json_DL_CENTURA12B_20260421_092214_104.json", [
    {"timestamp": at(9, 22, 14), "source": "tool_CENTURA-12B",
     "event_type": "ALMID=0x4F2B",
     "severity": "ERROR",
     "message": "ALMID=0x4F2B GRP=DCBX MOD=PM3 CHNL=03 VAL=+489 THR=100 UNIT=mV SEQ=449815 RCP=ETCH_HBR_V4 LOT=M25B-019 WFR=W10"},
])

# ── Phase 2 (LOG files, after human approval) ─────────────────────────────────
# Same machine: tool_CENTURA-12B — now has DynamoDB rule + approved pool examples
# Expected: few-shot RAG → category=electrical, confidence≈0.90+ → P1
# log_P1_* sorts after all JSONs, landing in Phase 2

wl("log_P1_CENTURA12B_20260421_140755_105.log", [
    "2026-04-21T14:07:55Z [WARN] tool_CENTURA-12B ALMID=0x4F2A GRP=DCBX MOD=PM3 CHNL=01 VAL=+441 THR=100 UNIT=mV SEQ=449913 RCP=ETCH_HBR_V4 LOT=M25B-021 WFR=W03",
])

wl("log_P1_CENTURA12B_20260421_141122_106.log", [
    "2026-04-21T14:11:22Z [WARN] tool_CENTURA-12B ALMID=0x4F2A GRP=DCBX MOD=PM3 CHNL=01 VAL=+503 THR=100 UNIT=mV SEQ=449914 RCP=ETCH_HBR_V4 LOT=M25B-021 WFR=W04",
])

wl("log_P1_CENTURA12B_20260421_141509_107.log", [
    "2026-04-21T14:15:09Z [ERROR] tool_CENTURA-12B ALMID=0x4F2B GRP=DCBX MOD=PM3 CHNL=01 VAL=+672 THR=100 UNIT=mV SEQ=449915 RCP=ETCH_HBR_V4 LOT=M25B-021 WFR=W05",
])


# ════════════════════════════════════════════════════════════════════
#  SUMMARY
# ════════════════════════════════════════════════════════════════════

total = len(created)
by_ext = {}
for f in created:
    ext = f.split(".")[1] if "." in f else "?"
    by_ext[ext] = by_ext.get(ext, 0) + 1

print(f"\n{'='*55}")
print(f"  simulation_data/ — {total} files created")
print(f"{'='*55}")
for ext, count in sorted(by_ext.items()):
    print(f"  .{ext:<5} {count:>3} files")
print(f"{'─'*55}")
print(f"  total  {total:>3} files")
print(f"{'='*55}")
print("\nNow run:  python simulate_stream.py --mode demo")
