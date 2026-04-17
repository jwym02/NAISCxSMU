#!/usr/bin/env python3
"""
Bulk load normalization rules into DynamoDB local.
Run: python scripts/load_dynamodb_rules.py
"""

import json
import boto3
import sys

# DynamoDB local configuration
dynamodb = boto3.resource(
    "dynamodb",
    endpoint_url="http://localhost:8000",
    region_name="ap-southeast-1",
    aws_access_key_id="test",
    aws_secret_access_key="test",
)

TABLE_NAME = "normalization-rules"

# Your normalization rules JSON
RULES = [
  {
    "vendorId": "Lam_Research_Etch",
    "fieldName": "error_code",
    "fieldValue": "VACUUM_FAULT",
    "category": "mechanical_event",
    "priority": "P0",
    "notes": "Chamber vacuum lost; auto abort"
  },
  {
    "vendorId": "Lam_Research_Etch",
    "fieldName": "error_code",
    "fieldValue": "COOLANT_TEMP_HIGH",
    "category": "thermal_event",
    "priority": "P1",
    "notes": "Coolant >25°C above setpoint"
  },
  {
    "vendorId": "Lam_Research_Etch",
    "fieldName": "error_code",
    "fieldValue": "RF_POWER_MISMATCH",
    "category": "electrical_event",
    "priority": "P1",
    "notes": "Forward/reflected power imbalance"
  },
  {
    "vendorId": "Lam_Research_Etch",
    "fieldName": "error_code",
    "fieldValue": "CHAMBER_PRESSURE_CRITICAL",
    "category": "mechanical_event",
    "priority": "P0",
    "notes": "Pressure outside safe band"
  },
  {
    "vendorId": "Lam_Research_Etch",
    "fieldName": "error_code",
    "fieldValue": "GAS_FLOW_FAULT",
    "category": "safety_event",
    "priority": "P1",
    "notes": "MFC deviation >5% from setpoint"
  },
  {
    "vendorId": "Lam_Research_Etch",
    "fieldName": "error_code",
    "fieldValue": "ESC_FAULT",
    "category": "mechanical_event",
    "priority": "P1",
    "notes": "Electrostatic chuck clamping failure"
  },
  {
    "vendorId": "Lam_Research_Etch",
    "fieldName": "error_code",
    "fieldValue": "PUMP_FAIL",
    "category": "mechanical_event",
    "priority": "P0",
    "notes": "Turbo or roughing pump failure"
  },
  {
    "vendorId": "Lam_Research_Etch",
    "fieldName": "error_code",
    "fieldValue": "WAFER_DROPPED",
    "category": "safety_event",
    "priority": "P0",
    "notes": "Substrate handling error"
  },
  {
    "vendorId": "Lam_Research_Etch",
    "fieldName": "pneumatic_psi",
    "fieldValue": "<83 PSI triggers alarm",
    "category": "mechanical_event",
    "priority": "P1",
    "notes": "AutoEtch air pressure interlock"
  },
  {
    "vendorId": "Lam_Research_Etch",
    "fieldName": "alarm_text_keyword",
    "fieldValue": "fire detected",
    "category": "safety_event",
    "priority": "P0",
    "notes": "Immediate EMO trigger"
  },
  {
    "vendorId": "Lam_Research_Etch",
    "fieldName": "alarm_text_keyword",
    "fieldValue": "emo activated",
    "category": "safety_event",
    "priority": "P0",
    "notes": "Emergency machine off"
  },
  {
    "vendorId": "Applied_Materials_CVD",
    "fieldName": "error_code",
    "fieldValue": "HEATER_TEMP_OVER_LIMIT",
    "category": "thermal_event",
    "priority": "P0",
    "notes": "PECVD >500°C; SACVD/HDP >900°C"
  },
  {
    "vendorId": "Applied_Materials_CVD",
    "fieldName": "error_code",
    "fieldValue": "CHAMBER_LEAK",
    "category": "safety_event",
    "priority": "P0",
    "notes": "Process gas leak to atmosphere"
  },
  {
    "vendorId": "Applied_Materials_CVD",
    "fieldName": "error_code",
    "fieldValue": "RF_MATCH_FAULT",
    "category": "electrical_event",
    "priority": "P1",
    "notes": "Impedance match network failure"
  },
  {
    "vendorId": "Applied_Materials_CVD",
    "fieldName": "error_code",
    "fieldValue": "COOLANT_FLOW_LOW",
    "category": "thermal_event",
    "priority": "P1",
    "notes": "Cooling water flow below threshold"
  },
  {
    "vendorId": "Applied_Materials_CVD",
    "fieldName": "error_code",
    "fieldValue": "GAS_VALVE_FAIL",
    "category": "safety_event",
    "priority": "P0",
    "notes": "Process gas valve stuck/failed"
  },
  {
    "vendorId": "Applied_Materials_CVD",
    "fieldName": "error_code",
    "fieldValue": "VACUUM_INTERLOCK",
    "category": "mechanical_event",
    "priority": "P0",
    "notes": "Load lock or process vacuum fault"
  },
  {
    "vendorId": "Applied_Materials_CVD",
    "fieldName": "error_code",
    "fieldValue": "MFC_OUT_OF_RANGE",
    "category": "process_event",
    "priority": "P1",
    "notes": "Mass flow controller deviation"
  },
  {
    "vendorId": "Applied_Materials_CVD",
    "fieldName": "process_temp_c",
    "fieldValue": "PECVD: 350-400°C nominal; >450°C critical",
    "category": "thermal_event",
    "priority": "P1",
    "notes": "Ceramic heater zone alarm"
  },
  {
    "vendorId": "Applied_Materials_CVD",
    "fieldName": "process_temp_c",
    "fieldValue": "HDP-CVD: 400-800°C nominal; >900°C critical",
    "category": "thermal_event",
    "priority": "P0",
    "notes": "Causes chamber damage"
  },
  {
    "vendorId": "Applied_Materials_CVD",
    "fieldName": "alarm_text_keyword",
    "fieldValue": "arc detected",
    "category": "electrical_event",
    "priority": "P0",
    "notes": "Plasma arcing in chamber"
  },
  {
    "vendorId": "Applied_Materials_CVD",
    "fieldName": "alarm_text_keyword",
    "fieldValue": "exhaust fail",
    "category": "safety_event",
    "priority": "P0",
    "notes": "Abatement or exhaust blockage"
  },
  {
    "vendorId": "ASML_DUV_EUV",
    "fieldName": "error_code",
    "fieldValue": "RETICLE_STAGE_FAULT",
    "category": "mechanical_event",
    "priority": "P0",
    "notes": "Stage position or motion error"
  },
  {
    "vendorId": "ASML_DUV_EUV",
    "fieldName": "error_code",
    "fieldValue": "WAFER_STAGE_FAULT",
    "category": "mechanical_event",
    "priority": "P0",
    "notes": "TWINSCAN stage malfunction"
  },
  {
    "vendorId": "ASML_DUV_EUV",
    "fieldName": "error_code",
    "fieldValue": "ILLUMINATION_FAULT",
    "category": "electrical_event",
    "priority": "P0",
    "notes": "ArF/KrF/EUV source power loss"
  },
  {
    "vendorId": "ASML_DUV_EUV",
    "fieldName": "error_code",
    "fieldValue": "VACUUM_LOSS",
    "category": "mechanical_event",
    "priority": "P0",
    "notes": "EUV vessel or stage vacuum failure"
  },
  {
    "vendorId": "ASML_DUV_EUV",
    "fieldName": "error_code",
    "fieldValue": "RETICLE_HANDLER_ERROR",
    "category": "mechanical_event",
    "priority": "P1",
    "notes": "Reticle drop or misalignment"
  },
  {
    "vendorId": "ASML_DUV_EUV",
    "fieldName": "overlay_nm",
    "fieldValue": "<3 nm normal; >5 nm critical",
    "category": "quality_event",
    "priority": "P1",
    "notes": "Layer-to-layer alignment metric"
  },
  {
    "vendorId": "ASML_DUV_EUV",
    "fieldName": "lens_temp_deviation_c",
    "fieldValue": "<0.05°C normal; >0.1°C alarm",
    "category": "thermal_event",
    "priority": "P1",
    "notes": "Projection optics thermal drift"
  },
  {
    "vendorId": "ASML_DUV_EUV",
    "fieldName": "dose_mj_cm2",
    "fieldValue": "Deviation >2% from recipe triggers P1",
    "category": "quality_event",
    "priority": "P1",
    "notes": "Exposure energy control"
  },
  {
    "vendorId": "ASML_DUV_EUV",
    "fieldName": "alarm_text_keyword",
    "fieldValue": "wafer dropped",
    "category": "safety_event",
    "priority": "P0",
    "notes": "Wafer handler drop event"
  },
  {
    "vendorId": "Axcelis_Ion_Implanter",
    "fieldName": "error_code",
    "fieldValue": "ION_BEAM_ABORT",
    "category": "process_event",
    "priority": "P0",
    "notes": "Beam loss or interlock abort"
  },
  {
    "vendorId": "Axcelis_Ion_Implanter",
    "fieldName": "error_code",
    "fieldValue": "HIGH_VOLTAGE_FAULT",
    "category": "electrical_event",
    "priority": "P0",
    "notes": "Accelerator HV arc or loss"
  },
  {
    "vendorId": "Axcelis_Ion_Implanter",
    "fieldName": "error_code",
    "fieldValue": "VACUUM_INTERLOCK",
    "category": "mechanical_event",
    "priority": "P0",
    "notes": "Beam line or end station vacuum"
  },
  {
    "vendorId": "Axcelis_Ion_Implanter",
    "fieldName": "error_code",
    "fieldValue": "SOURCE_GAS_LEAK",
    "category": "safety_event",
    "priority": "P0",
    "notes": "Arsine/phosphine toxic gas alarm"
  },
  {
    "vendorId": "Axcelis_Ion_Implanter",
    "fieldName": "error_code",
    "fieldValue": "DOSE_UNIFORMITY_FAIL",
    "category": "quality_event",
    "priority": "P1",
    "notes": "Uniformity >1% 1-sigma"
  },
  {
    "vendorId": "Axcelis_Ion_Implanter",
    "fieldName": "error_code",
    "fieldValue": "SCAN_FAULT",
    "category": "mechanical_event",
    "priority": "P1",
    "notes": "Wafer scan or tilt mechanism error"
  },
  {
    "vendorId": "Axcelis_Ion_Implanter",
    "fieldName": "error_code",
    "fieldValue": "FARADAY_CUP_FAULT",
    "category": "electrical_event",
    "priority": "P1",
    "notes": "Beam current measurement fail"
  },
  {
    "vendorId": "Axcelis_Ion_Implanter",
    "fieldName": "target_temp_c",
    "fieldValue": ">200°C triggers P1; >300°C triggers P0",
    "category": "thermal_event",
    "priority": "P1",
    "notes": "Wafer heating during high-dose"
  },
  {
    "vendorId": "Axcelis_Ion_Implanter",
    "fieldName": "alarm_text_keyword",
    "fieldValue": "gas leak detected",
    "category": "safety_event",
    "priority": "P0",
    "notes": "Toxic dopant gas interlock"
  },
  {
    "vendorId": "TEL_Coater_Developer",
    "fieldName": "error_code",
    "fieldValue": "SPIN_SPEED_FAULT",
    "category": "mechanical_event",
    "priority": "P1",
    "notes": "Chuck RPM deviation >5%"
  },
  {
    "vendorId": "TEL_Coater_Developer",
    "fieldName": "error_code",
    "fieldValue": "HOT_PLATE_TEMP_OVER",
    "category": "thermal_event",
    "priority": "P1",
    "notes": "Bake plate >±5°C from setpoint"
  },
  {
    "vendorId": "TEL_Coater_Developer",
    "fieldName": "error_code",
    "fieldValue": "RESIST_DISPENSE_FAIL",
    "category": "process_event",
    "priority": "P1",
    "notes": "Nozzle or pump dispense fault"
  },
  {
    "vendorId": "TEL_Coater_Developer",
    "fieldName": "error_code",
    "fieldValue": "N2_PRESSURE_LOW",
    "category": "safety_event",
    "priority": "P1",
    "notes": "Nitrogen purge pressure drop"
  },
  {
    "vendorId": "TEL_Coater_Developer",
    "fieldName": "error_code",
    "fieldValue": "ROBOT_FAULT",
    "category": "mechanical_event",
    "priority": "P1",
    "notes": "Cluster tool wafer transfer arm"
  },
  {
    "vendorId": "TEL_Coater_Developer",
    "fieldName": "hot_plate_temp_c",
    "fieldValue": "PEB: 90-130°C nominal; >145°C critical",
    "category": "thermal_event",
    "priority": "P1",
    "notes": "Post-exposure bake control"
  },
  {
    "vendorId": "TEL_Coater_Developer",
    "fieldName": "hot_plate_temp_c",
    "fieldValue": "Hard bake: 120-180°C nominal; >200°C alarm",
    "category": "thermal_event",
    "priority": "P1",
    "notes": "Final resist cure step"
  },
  {
    "vendorId": "Kokusai_TEL_Furnace",
    "fieldName": "error_code",
    "fieldValue": "TUBE_TEMP_OVER_LIMIT",
    "category": "thermal_event",
    "priority": "P0",
    "notes": "Process tube >1100°C"
  },
  {
    "vendorId": "Kokusai_TEL_Furnace",
    "fieldName": "error_code",
    "fieldValue": "TUBE_LEAK",
    "category": "safety_event",
    "priority": "P0",
    "notes": "Process gas leak from quartz tube"
  },
  {
    "vendorId": "Kokusai_TEL_Furnace",
    "fieldName": "error_code",
    "fieldValue": "BOAT_FAULT",
    "category": "mechanical_event",
    "priority": "P1",
    "notes": "Wafer boat load/unload error"
  },
  {
    "vendorId": "Kokusai_TEL_Furnace",
    "fieldName": "error_code",
    "fieldValue": "GAS_PURGE_FAIL",
    "category": "safety_event",
    "priority": "P1",
    "notes": "N2 purge or vent failure"
  },
  {
    "vendorId": "Kokusai_TEL_Furnace",
    "fieldName": "error_code",
    "fieldValue": "RAMP_RATE_FAULT",
    "category": "thermal_event",
    "priority": "P1",
    "notes": "Uncontrolled ramp >150°C/min"
  },
  {
    "vendorId": "Kokusai_TEL_Furnace",
    "fieldName": "process_temp_c",
    "fieldValue": "Oxidation: 800-1100°C; >1150°C critical",
    "category": "thermal_event",
    "priority": "P0",
    "notes": "Wafer damage threshold"
  },
  {
    "vendorId": "Kokusai_TEL_Furnace",
    "fieldName": "process_temp_c",
    "fieldValue": "LPCVD: 600-800°C; >900°C alarm",
    "category": "thermal_event",
    "priority": "P1",
    "notes": "Low-pressure CVD tube temp"
  },
  {
    "vendorId": "Kokusai_TEL_Furnace",
    "fieldName": "process_temp_c",
    "fieldValue": "Anneal: 700-1050°C; >1100°C alarm",
    "category": "thermal_event",
    "priority": "P1",
    "notes": "Dopant activation anneal"
  },
  {
    "vendorId": "Applied_Materials_CMP",
    "fieldName": "error_code",
    "fieldValue": "WAFER_ESCAPE",
    "category": "safety_event",
    "priority": "P0",
    "notes": "Substrate ejected from carrier"
  },
  {
    "vendorId": "Applied_Materials_CMP",
    "fieldName": "error_code",
    "fieldValue": "SLURRY_FLOW_LOW",
    "category": "process_event",
    "priority": "P1",
    "notes": "Chemical slurry delivery fault"
  },
  {
    "vendorId": "Applied_Materials_CMP",
    "fieldName": "error_code",
    "fieldValue": "HEAD_PRESSURE_FAULT",
    "category": "mechanical_event",
    "priority": "P1",
    "notes": "Polish head downforce deviation"
  },
  {
    "vendorId": "Applied_Materials_CMP",
    "fieldName": "error_code",
    "fieldValue": "ENDPOINT_DETECT_FAIL",
    "category": "quality_event",
    "priority": "P1",
    "notes": "OES or motor current endpoint miss"
  },
  {
    "vendorId": "Applied_Materials_CMP",
    "fieldName": "pad_temp_c",
    "fieldValue": "<55°C normal; >65°C alarm; >75°C P0",
    "category": "thermal_event",
    "priority": "P1",
    "notes": "Polishing pad thermal runaway"
  },
  {
    "vendorId": "KLA_Inspection",
    "fieldName": "defect_density_per_wafer",
    "fieldValue": "<500 normal; >2000 P2; >5000 P1 critical",
    "category": "quality_event",
    "priority": "P1",
    "notes": "Post-etch or post-CMP inspection"
  },
  {
    "vendorId": "KLA_Inspection",
    "fieldName": "overlay_nm",
    "fieldValue": "<2nm normal; >4nm P2; >8nm P1",
    "category": "quality_event",
    "priority": "P1",
    "notes": "YieldStar / optical metrology"
  },
  {
    "vendorId": "KLA_Inspection",
    "fieldName": "error_code",
    "fieldValue": "STAGE_POSITION_ERROR",
    "category": "mechanical_event",
    "priority": "P1",
    "notes": "Wafer stage motion fault"
  },
  {
    "vendorId": "KLA_Inspection",
    "fieldName": "error_code",
    "fieldValue": "EBEAM_COLUMN_FAULT",
    "category": "electrical_event",
    "priority": "P1",
    "notes": "e-beam inspection column failure"
  },
  {
    "vendorId": "KLA_Inspection",
    "fieldName": "error_code",
    "fieldValue": "TOOL_VIBRATION_HIGH",
    "category": "mechanical_event",
    "priority": "P1",
    "notes": "Floor vibration exceeds threshold"
  },
  {
    "vendorId": "SEMI_GEM_Generic",
    "fieldName": "alarm_severity",
    "fieldValue": "personal_safety → P0",
    "category": "safety_event",
    "priority": "P0",
    "notes": "SEMI E30 alarm class 1"
  },
  {
    "vendorId": "SEMI_GEM_Generic",
    "fieldName": "alarm_severity",
    "fieldValue": "equipment_safety → P0/P1",
    "category": "safety_event",
    "priority": "P0",
    "notes": "SEMI E30 alarm class 2"
  },
  {
    "vendorId": "SEMI_GEM_Generic",
    "fieldName": "alarm_severity",
    "fieldValue": "parameter_control → P1/P2",
    "category": "process_event",
    "priority": "P1",
    "notes": "SEMI E30 alarm class 3"
  },
  {
    "vendorId": "SEMI_GEM_Generic",
    "fieldName": "gem_stream",
    "fieldValue": "S5F1 → alarm_notification",
    "category": "process_event",
    "priority": "P1",
    "notes": "GEM alarm message stream"
  },
  {
    "vendorId": "SEMI_GEM_Generic",
    "fieldName": "gem_stream",
    "fieldValue": "S6F11 → collection_event",
    "category": "process_event",
    "priority": "P2",
    "notes": "GEM event data report"
  },
  {
    "vendorId": "SEMI_GEM_Generic",
    "fieldName": "temp_c",
    "fieldValue": "Normalized field: process_temperature_celsius",
    "category": "thermal_event",
    "priority": "P2",
    "notes": "Vendor field mapping"
  },
  {
    "vendorId": "SEMI_GEM_Generic",
    "fieldName": "chamber_pressure_mtorr",
    "fieldValue": "Normalized field: chamber_pressure_mtorr",
    "category": "mechanical_event",
    "priority": "P2",
    "notes": "Vendor field mapping"
  },
  {
    "vendorId": "SEMI_GEM_Generic",
    "fieldName": "rf_power_w",
    "fieldValue": "Normalized field: rf_forward_power_watts",
    "category": "electrical_event",
    "priority": "P2",
    "notes": "Vendor field mapping"
  },
  {
    "vendorId": "SEMI_GEM_Generic",
    "fieldName": "coolant_flow_lpm",
    "fieldValue": "Normalized field: coolant_flow_liters_per_min",
    "category": "thermal_event",
    "priority": "P2",
    "notes": "Vendor field mapping"
  },
  {
    "vendorId": "SEMI_GEM_Generic",
    "fieldName": "gas_flow_sccm",
    "fieldValue": "Normalized field: gas_flow_standard_cc_per_min",
    "category": "process_event",
    "priority": "P2",
    "notes": "Vendor field mapping"
  },
  {
    "vendorId": "SEMI_GEM_Generic",
    "fieldName": "alarm_text_keyword",
    "fieldValue": "flood detected",
    "category": "safety_event",
    "priority": "P0",
    "notes": "Coolant or DI water leak"
  },
  {
    "vendorId": "SEMI_GEM_Generic",
    "fieldName": "alarm_text_keyword",
    "fieldValue": "power failure",
    "category": "electrical_event",
    "priority": "P0",
    "notes": "Facility or UPS power loss"
  },
  {
    "vendorId": "SEMI_GEM_Generic",
    "fieldName": "alarm_text_keyword",
    "fieldValue": "vacuum lost",
    "category": "mechanical_event",
    "priority": "P0",
    "notes": "Cross-vendor vacuum interlock"
  },
  {
    "vendorId": "SEMI_GEM_Generic",
    "fieldName": "alarm_text_keyword",
    "fieldValue": "e-stop activated",
    "category": "safety_event",
    "priority": "P0",
    "notes": "Emergency stop button pressed"
  },
  {
    "vendorId": "SEMI_GEM_Generic",
    "fieldName": "alarm_text_keyword",
    "fieldValue": "wafer broken",
    "category": "safety_event",
    "priority": "P0",
    "notes": "Particle contamination risk"
  },
  {
    "vendorId": "Novellus_Lam_CVD",
    "fieldName": "error_code",
    "fieldValue": "HEATER_OVERTEMP",
    "category": "thermal_event",
    "priority": "P0",
    "notes": "PECVD/HDP heater runaway"
  },
  {
    "vendorId": "Novellus_Lam_CVD",
    "fieldName": "error_code",
    "fieldValue": "PLASMA_LOSS",
    "category": "process_event",
    "priority": "P1",
    "notes": "Unintended plasma extinguish"
  },
  {
    "vendorId": "Novellus_Lam_CVD",
    "fieldName": "error_code",
    "fieldValue": "WET_DETECT",
    "category": "safety_event",
    "priority": "P0",
    "notes": "Coolant leak in system"
  },
  {
    "vendorId": "Novellus_Lam_CVD",
    "fieldName": "error_code",
    "fieldValue": "PRECURSOR_FLOW_FAULT",
    "category": "process_event",
    "priority": "P1",
    "notes": "CVD precursor delivery fault"
  },
  {
    "vendorId": "Novellus_Lam_CVD",
    "fieldName": "process_temp_c",
    "fieldValue": "PECVD: 200-400°C; >500°C P0",
    "category": "thermal_event",
    "priority": "P0",
    "notes": "ALTUS/VECTOR CVD temp range"
  }
]


def load_rules():
    """Load all normalization rules into DynamoDB.
    
    Groups rules by vendorId and stores them as a list attribute.
    This avoids duplicate partition key issues.
    """
    table = dynamodb.Table(TABLE_NAME)
    
    print(f"Loading {len(RULES)} rules into {TABLE_NAME}...")
    
    # Group rules by vendorId
    rules_by_vendor = {}
    for rule in RULES:
        vendor = rule['vendorId']
        if vendor not in rules_by_vendor:
            rules_by_vendor[vendor] = []
        rules_by_vendor[vendor].append(rule)
    
    print(f"Grouped into {len(rules_by_vendor)} vendor entries")
    
    with table.batch_writer() as batch:
        for i, (vendor_id, vendor_rules) in enumerate(rules_by_vendor.items(), 1):
            try:
                batch.put_item(Item={
                    'vendorId': vendor_id,
                    'rules': vendor_rules,
                    'ruleCount': len(vendor_rules)
                })
                print(f"  [{i}/{len(rules_by_vendor)}] {vendor_id}: {len(vendor_rules)} rules")
            except Exception as e:
                print(f"  ❌ Error writing vendor {vendor_id}: {e}")
                return False
    
    print(f"\n✅ Successfully loaded {len(RULES)} rules across {len(rules_by_vendor)} vendors!")
    return True


if __name__ == "__main__":
    try:
        success = load_rules()
        sys.exit(0 if success else 1)
    except Exception as e:
        print(f"❌ Failed to load rules: {e}")
        sys.exit(1)
