# hv-grid-monitor
Async Python monitor for high-voltage substations (kV and Hz) using CSV streams or replay, hysteresis state machines, transient filtering, baseline tracking, severity escalation, and CSV event/telemetry logging.
Features - 
Independent Channel Monitoring: Tracks voltage (`VOLTAGE`) and grid frequency (`FREQUENCY`) as separate `MetricChannelMonitor` state machines. 
Hysteresis & Persistence: Uses separate entry/exit thresholds and `min_duration_sec` to reduce alert chatter and separate short transients (`TRANSIENT` / `TRANSIENT_END`) from confirmed events (`ALERT_START`, `ALERT_ONGOING`, `ALERT_ESCALATED`, `ALERT_END`).  
Severity Escalation: Maps deviation scores to `INFO`, `WARNING`, and `CRITICAL`, escalating active non-critical alerts once when thresholds are crossed. 
Rolling Baseline & CSV Replay:Builds a rolling baseline from clean voltage samples outside active alerts and supports paced or instant CSV replay (`replay_speed`, `loop_replay`).
Buffered Telemetry & Event Logs: Writes buffered telemetry rows and event transitions (including `FORCE_END` on shutdown) to CSV files. ## Input CSV Format The CSV reader accepts common column aliases (case-insensitive) and decimal commas or dots: 
Timestamp: `timestamp`, `time`, `ts`, `datetime` 
Voltage (kV): `kv`, `voltage_kv`, `u_kv`, `voltage`, `v_kv`
Frequency (Hz): `freq_hz`, `freq`, `frequency_hz`, `frequency`, `f_hz` Example `measurements_220kv.csv`: ```csv timestamp,kv,freq_hz 2026-11-19 10:00:00.000,221.4,50.01 2026-11-19 10:00:00.200,220.8,50.00 2026-11-19 10:00:00.400,206.5,49.98 Quick Start Save your script (for example as monitor.py) alongside your input CSV file (default: measurements_220kv.csv). Run with Python 3.10+ (uses only the Python standard library): python monitor.py Output Files 

By default, the monitor creates two CSV files named after substation_id (for example RET_HV_220KV_01):

hv_telemetry_<substation_id>.csv — timestamped voltage, baseline, frequency, normalized scores, and channel states. hv_events_<substation_id>.csv — alert transitions, event types (HV_UNDER_VOLTAGE, HV_OVER_VOLTAGE, HV_VOLTAGE_BASELINE_SHIFT, HV_VOLTAGE_NOMINAL_DRIFT, HV_FREQ_LOW, HV_FREQ_HIGH), severity, duration, and operational hints. 
