import asyncio
import csv
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Callable, Iterator, Optional, Tuple


@dataclass
class AlertRuleConfig:
    warning_score: float = 0.35
    critical_score: float = 0.75
    exit_score: float = 0.22
    min_duration_sec: float = 1.0


@dataclass
class MetricChannelMonitor:
    channel_name: str
    entry_threshold: float
    exit_threshold: float
    warning_threshold: float
    critical_threshold: float
    min_duration_sec: float
    candidate_start: Optional[datetime] = None
    active: bool = False
    event_type: Optional[str] = None
    severity: str = "NOMINAL"
    event_start: Optional[datetime] = None
    transient_sec: float = 0.0
    score_samples: list[float] = field(default_factory=list)
    total_events: int = 0
    total_transients: int = 0
    last_state: str = "NOMINAL"
    last_score: float = 0.0

    def classify_severity(self, score: float) -> str:
        # Map numeric deviation score to alert severity levels
        if score >= self.critical_threshold:
            return "CRITICAL"
        if score >= self.warning_threshold:
            return "WARNING"
        return "INFO"

    def reset(self) -> None:
        # Clear candidate and active alert tracking state
        self.candidate_start = None
        self.active = False
        self.event_type = None
        self.severity = "NOMINAL"
        self.event_start = None
        self.transient_sec = 0.0
        self.score_samples.clear()

    def stats(self, fallback_score: float) -> tuple[float, float]:
        # Compute average and peak score over the current alert window
        if not self.score_samples:
            return fallback_score, fallback_score
        return sum(self.score_samples) / len(self.score_samples), max(self.score_samples)

    def step(
        self,
        dt: datetime,
        score: float,
        event_label_fn: Callable[[], str],
        action_hint_fn: Callable[[str, str], str],
    ) -> tuple[str, Optional[dict]]:
        # Advance channel state machine with hysteresis, persistence, and severity escalation
        self.last_score = score
        condition_met = (
            score >= self.exit_threshold if self.active else score >= self.entry_threshold
        )
        alert_payload: Optional[dict] = None
        state = "NOMINAL"

        if condition_met:
            if self.candidate_start is None:
                self.candidate_start = dt

            elapsed = max(0.0, (dt - self.candidate_start).total_seconds())
            if elapsed >= self.min_duration_sec:
                current_sev = self.classify_severity(score)
                if not self.active:
                    self.active = True
                    self.event_start = self.candidate_start
                    self.transient_sec = elapsed
                    self.score_samples = [score]
                    self.total_events += 1
                    self.event_type = event_label_fn()
                    self.severity = current_sev
                    state = "ALERT_START"
                    alert_payload = {
                        "transition": "START",
                        "channel": self.channel_name,
                        "event_type": self.event_type,
                        "severity": self.severity,
                        "score": score,
                        "avg_score": score,
                        "peak_score": score,
                        "transient_sec": self.transient_sec,
                        "duration_sec": elapsed,
                        "action_hint": action_hint_fn(self.event_type, self.severity),
                    }
                else:
                    state = "ALERT_ONGOING"
                    self.score_samples.append(score)
                    # Escalate once from non-critical to CRITICAL when score crosses threshold
                    if self.severity != "CRITICAL" and current_sev == "CRITICAL":
                        self.severity = "CRITICAL"
                        state = "ALERT_ESCALATED"
                        avg_s, peak_s = self.stats(score)
                        duration_sec = (
                            max(0.0, (dt - self.event_start).total_seconds())
                            if self.event_start
                            else 0.0
                        )
                        alert_payload = {
                            "transition": "ESCALATED",
                            "channel": self.channel_name,
                            "event_type": self.event_type,
                            "severity": self.severity,
                            "score": score,
                            "avg_score": avg_s,
                            "peak_score": peak_s,
                            "transient_sec": self.transient_sec,
                            "duration_sec": duration_sec,
                            "action_hint": action_hint_fn(self.event_type, self.severity),
                        }
            else:
                state = "TRANSIENT"
        else:
            if self.active:
                total_dur = (
                    max(0.0, (dt - self.event_start).total_seconds())
                    if self.event_start
                    else 0.0
                )
                avg_s, peak_s = self.stats(score)
                state = "ALERT_END"
                alert_payload = {
                    "transition": "END",
                    "channel": self.channel_name,
                    "event_type": f"{self.event_type}_END",
                    "severity": self.severity,
                    "score": score,
                    "avg_score": avg_s,
                    "peak_score": peak_s,
                    "transient_sec": self.transient_sec,
                    "duration_sec": total_dur,
                    "action_hint": "Returned below exit threshold; verify system stability.",
                }
                self.reset()
            elif self.candidate_start is not None:
                # Count short excursions below min_duration_sec as filtered transients
                state = "TRANSIENT_END"
                self.total_transients += 1
                self.reset()

        self.last_state = state
        return state, alert_payload


class HighVoltageRegionalMonitor:
    def __init__(
        self,
        substation_id: str = "RET_HV_220KV_01",
        input_csv_path: Optional[str] = None,
        replay_speed: float = 0.0,
        max_sleep_sec: float = 2.0,
        loop_replay: bool = False,
        nominal_kv: float = 220.0,
        nominal_freq_hz: float = 50.0,
        min_normal_kv: Optional[float] = 210.0,
        max_normal_kv: Optional[float] = 235.0,
        voltage_deadband_pct: float = 0.035,
        max_voltage_dev_pct: float = 0.10,
        max_freq_dev_hz: float = 0.20,
        voltage_rules: Optional[AlertRuleConfig] = None,
        freq_rules: Optional[AlertRuleConfig] = None,
        window_size: int = 600,
        warmup_samples: int = 50,
        buffer_limit: int = 50,
    ):
        # Substation identity and CSV replay settings
        self.substation_id = substation_id
        self.input_csv_path = input_csv_path
        self.replay_speed = replay_speed
        self.max_sleep_sec = max_sleep_sec
        self.loop_replay = loop_replay

        # Nominal ratings and normal operating voltage bounds
        self.nominal_kv = nominal_kv
        self.nominal_freq_hz = nominal_freq_hz
        self.voltage_deadband_pct = voltage_deadband_pct
        self.min_normal_kv = (
            min_normal_kv
            if min_normal_kv is not None
            else nominal_kv * (1.0 - voltage_deadband_pct)
        )
        self.max_normal_kv = (
            max_normal_kv
            if max_normal_kv is not None
            else nominal_kv * (1.0 + voltage_deadband_pct)
        )

        # Scaling factors for normalizing deviations into [0.0, 1.0]
        self.max_voltage_dev_pct = max_voltage_dev_pct
        self.max_freq_dev_hz = max_freq_dev_hz

        # Instantiate fresh rule configs per monitor instance to avoid shared mutable defaults
        resolved_voltage_rules = (
            voltage_rules
            if voltage_rules is not None
            else AlertRuleConfig(
                warning_score=0.35,
                critical_score=0.75,
                exit_score=0.22,
                min_duration_sec=1.0,
            )
        )
        resolved_freq_rules = (
            freq_rules
            if freq_rules is not None
            else AlertRuleConfig(
                warning_score=0.30,
                critical_score=0.70,
                exit_score=0.18,
                min_duration_sec=0.5,
            )
        )

        # Rolling baseline window for clean nominal voltage samples
        self.baseline_window: deque[float] = deque(maxlen=window_size)
        self.warmup_samples = warmup_samples

        # Independent channel monitors for voltage and frequency
        self.v_monitor = MetricChannelMonitor(
            channel_name="VOLTAGE",
            entry_threshold=resolved_voltage_rules.warning_score,
            exit_threshold=resolved_voltage_rules.exit_score,
            warning_threshold=resolved_voltage_rules.warning_score,
            critical_threshold=resolved_voltage_rules.critical_score,
            min_duration_sec=resolved_voltage_rules.min_duration_sec,
        )
        self.f_monitor = MetricChannelMonitor(
            channel_name="FREQUENCY",
            entry_threshold=resolved_freq_rules.warning_score,
            exit_threshold=resolved_freq_rules.exit_score,
            warning_threshold=resolved_freq_rules.warning_score,
            critical_threshold=resolved_freq_rules.critical_score,
            min_duration_sec=resolved_freq_rules.min_duration_sec,
        )

        self._last_metrics: Optional[dict] = None

        # Telemetry and event CSV log paths plus buffered writer state
        self.telemetry_log = f"hv_telemetry_{self.substation_id}.csv"
        self.events_log = f"hv_events_{self.substation_id}.csv"
        self.csv_buffer: list[list[str]] = []
        self.buffer_limit = buffer_limit
        self._init_logs()

    def _init_logs(self) -> None:
        # Initialize telemetry CSV with column headers
        with open(self.telemetry_log, mode="w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow([
                "Timestamp",
                "Substation",
                "Voltage_Kv",
                "Baseline_Kv",
                "Freq_Hz",
                "VoltageScore",
                "FreqScore",
                "VoltageState",
                "FreqState",
            ])

        # Initialize events CSV with column headers
        with open(self.events_log, mode="w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow([
                "Timestamp",
                "Substation",
                "Channel",
                "Transition",
                "EventType",
                "Severity",
                "MetricValue",
                "ReferenceValue",
                "Score",
                "AvgScore",
                "PeakScore",
                "Transient_Sec",
                "Duration_Sec",
                "ActionHint",
            ])

    def flush_csv_buffer(self) -> None:
        # Flush accumulated telemetry rows to disk in a single batch
        if not self.csv_buffer:
            return
        with open(self.telemetry_log, mode="a", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerows(self.csv_buffer)
        self.csv_buffer.clear()

    def _log_alert_event(
        self,
        ts_str: str,
        metric_val: float,
        ref_val: float,
        alert: dict,
    ) -> None:
        # Append a confirmed alert transition record to the events CSV
        with open(self.events_log, mode="a", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow([
                ts_str,
                self.substation_id,
                alert["channel"],
                alert["transition"],
                alert["event_type"],
                alert["severity"],
                f"{metric_val:.4f}",
                f"{ref_val:.4f}",
                f"{alert['score']:.4f}",
                f"{alert['avg_score']:.4f}",
                f"{alert['peak_score']:.4f}",
                f"{alert['transient_sec']:.2f}",
                f"{alert['duration_sec']:.2f}",
                alert["action_hint"],
            ])

    def _voltage_event_type(self, kv: float, baseline_kv: float) -> str:
        # Classify voltage deviation relative to normal band or baseline/nominal references
        if kv < self.min_normal_kv:
            return "HV_UNDER_VOLTAGE"
        if kv > self.max_normal_kv:
            return "HV_OVER_VOLTAGE"
        if abs(kv - baseline_kv) >= abs(kv - self.nominal_kv):
            return "HV_VOLTAGE_BASELINE_SHIFT"
        return "HV_VOLTAGE_NOMINAL_DRIFT"

    def _freq_event_type(self, freq_hz: float) -> str:
        # Classify frequency deviation relative to nominal grid frequency
        return "HV_FREQ_HIGH" if freq_hz > self.nominal_freq_hz else "HV_FREQ_LOW"

    @staticmethod
    def _action_hint(event_type: str, severity: str) -> str:
        # Provide an operational check hint based on event category
        if event_type == "HV_UNDER_VOLTAGE":
            return "Check reactive power injection / transformer tap changers / local bus configuration."
        if event_type == "HV_OVER_VOLTAGE":
            return "Check reactive power absorption / shunt reactors / tap changers / overvoltage risk."
        if event_type.startswith("HV_VOLTAGE"):
            return "Check deviation from baseline/nominal voltage and local telemetry sensor quality."
        if event_type == "HV_FREQ_LOW":
            return "System-wide indicator (50 Hz): check active power deficit / primary frequency reserves."
        if event_type == "HV_FREQ_HIGH":
            return "System-wide indicator (50 Hz): check active power surplus / generation curtailment."
        return f"Severity {severity}: inspect signal integrity."

    @staticmethod
    def _parse_timestamp(raw_ts: str) -> Optional[datetime]:
        # Parse ISO-8601 or common timestamp strings into a naive datetime
        cleaned = raw_ts.strip()
        if not cleaned:
            return None
        if cleaned.endswith("Z"):
            cleaned = cleaned[:-1] + "+00:00"
        try:
            dt = datetime.fromisoformat(cleaned)
            return dt.replace(tzinfo=None) if dt.tzinfo is not None else dt
        except ValueError:
            pass

        formats = (
            "%Y-%m-%d %H:%M:%S.%f",
            "%Y-%m-%d %H:%M:%S",
            "%d.%m.%Y %H:%M:%S.%f",
            "%d.%m.%Y %H:%M:%S",
            "%Y/%m/%d %H:%M:%S",
        )
        for fmt in formats:
            try:
                return datetime.strptime(cleaned, fmt)
            except ValueError:
                continue
        return None

    @staticmethod
    def _pick_field(row: dict[str, str], candidates: Tuple[str, ...]) -> Optional[str]:
        # Return the first non-empty value matching any candidate column alias
        normalized = {
            k.strip().lower(): (v.strip() if v is not None else "")
            for k, v in row.items()
            if k
        }
        for key in candidates:
            if key in normalized and normalized[key] != "":
                return normalized[key]
        return None

    def _iter_csv_rows(self, csv_path: Path) -> Iterator[Tuple[datetime, float, float]]:
        # Stream valid (timestamp, kv, freq_hz) tuples from a CSV file
        with open(csv_path, mode="r", newline="", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            for line_num, row in enumerate(reader, start=2):
                raw_ts = self._pick_field(row, ("timestamp", "time", "ts", "datetime"))
                raw_kv = self._pick_field(
                    row, ("kv", "voltage_kv", "u_kv", "voltage", "v_kv")
                )
                raw_freq = self._pick_field(
                    row, ("freq_hz", "freq", "frequency_hz", "frequency", "f_hz")
                )
                if raw_ts is None or raw_kv is None or raw_freq is None:
                    continue

                ts_dt = self._parse_timestamp(raw_ts)
                if ts_dt is None:
                    print(f"[CSV Skip] Invalid timestamp at line {line_num}: {raw_ts!r}")
                    continue

                try:
                    kv = float(raw_kv.replace(",", "."))
                    freq_hz = float(raw_freq.replace(",", "."))
                except ValueError:
                    print(
                        f"[CSV Skip] Non-numeric values at line {line_num}: "
                        f"kv={raw_kv!r}, freq={raw_freq!r}"
                    )
                    continue

                yield ts_dt, kv, freq_hz

    async def stream_regional_measurements(self, interval_sec: float = 0.1):
        # Async generator reading voltage and frequency rows from input_csv_path
        if not self.input_csv_path:
            raise ValueError(
                "input_csv_path must be provided to read measurements from a CSV file."
            )

        csv_path = Path(self.input_csv_path)
        if not csv_path.is_file():
            raise FileNotFoundError(f"Input CSV file not found: {csv_path}")

        while True:
            prev_ts: Optional[datetime] = None
            emitted_any = False

            for ts_dt, kv, freq_hz in self._iter_csv_rows(csv_path):
                emitted_any = True

                # Pace playback according to timestamp deltas when replay_speed > 0
                if prev_ts is not None:
                    delta_sec = (ts_dt - prev_ts).total_seconds()
                    if delta_sec > 0 and self.replay_speed > 0:
                        sleep_sec = min(delta_sec / self.replay_speed, self.max_sleep_sec)
                        if sleep_sec > 0:
                            await asyncio.sleep(sleep_sec)
                elif interval_sec > 0 and self.replay_speed > 0:
                    await asyncio.sleep(min(interval_sec, self.max_sleep_sec))

                prev_ts = ts_dt
                yield {"timestamp": ts_dt, "kv": kv, "freq_hz": freq_hz}

            if not emitted_any or not self.loop_replay:
                break

    async def process_sample(self, sample: dict, now_dt: Optional[datetime] = None) -> None:
        # Process one measurement sample across voltage and frequency channels
        sample_dt = sample.get("timestamp")
        event_dt = sample_dt if isinstance(sample_dt, datetime) else (now_dt or datetime.now())
        ts_str = event_dt.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]

        kv = float(sample["kv"])
        freq_hz = float(sample.get("freq_hz", self.nominal_freq_hz))
        if kv <= 0:
            return

        # Fill initial baseline window before evaluating deviations
        if len(self.baseline_window) < self.warmup_samples:
            self.baseline_window.append(kv)
            return

        baseline_kv = sum(self.baseline_window) / len(self.baseline_window)
        safe_base = max(baseline_kv, 1e-6)
        safe_nom = max(self.nominal_kv, 1e-6)

        # Compute normalized voltage deviation score
        rel_kv_dev = abs(kv - baseline_kv) / safe_base
        if kv < self.min_normal_kv:
            excess_band_kv = self.min_normal_kv - kv
        elif kv > self.max_normal_kv:
            excess_band_kv = kv - self.max_normal_kv
        else:
            excess_band_kv = 0.0

        excess_band_pct = excess_band_kv / safe_nom
        raw_nom_kv_dev = abs(kv - self.nominal_kv) / safe_nom

        v_rel_factor = min(1.0, rel_kv_dev / max(self.max_voltage_dev_pct, 1e-6))
        v_band_factor = min(1.0, excess_band_pct / max(self.max_voltage_dev_pct, 1e-6))
        v_nom_factor = min(1.0, raw_nom_kv_dev / max(self.max_voltage_dev_pct, 1e-6))
        voltage_score = max(v_band_factor, 0.65 * v_rel_factor + 0.35 * v_nom_factor)

        # Compute normalized frequency deviation score
        freq_dev = abs(freq_hz - self.nominal_freq_hz)
        freq_score = min(1.0, freq_dev / max(self.max_freq_dev_hz, 1e-6))

        self._last_metrics = {
            "timestamp": event_dt,
            "kv": kv,
            "baseline_kv": baseline_kv,
            "freq_hz": freq_hz,
            "v_score": voltage_score,
            "f_score": freq_score,
        }

        v_state, v_alert = self.v_monitor.step(
            dt=event_dt,
            score=voltage_score,
            event_label_fn=lambda: self._voltage_event_type(kv, baseline_kv),
            action_hint_fn=self._action_hint,
        )
        f_state, f_alert = self.f_monitor.step(
            dt=event_dt,
            score=freq_score,
            event_label_fn=lambda: self._freq_event_type(freq_hz),
            action_hint_fn=self._action_hint,
        )

        if v_alert is not None:
            self._log_alert_event(ts_str, kv, baseline_kv, v_alert)
            print(
                f"[{v_alert['severity']} | V-{v_alert['transition']}] {ts_str} | "
                f"{v_alert['event_type']} | U={kv:.2f} kV (Base={baseline_kv:.2f} kV) | "
                f"Score={v_alert['score']:.4f} | Dur={v_alert['duration_sec']:.2f}s"
            )

        if f_alert is not None:
            self._log_alert_event(ts_str, freq_hz, self.nominal_freq_hz, f_alert)
            print(
                f"[{f_alert['severity']} | F-{f_alert['transition']}] {ts_str} | "
                f"{f_alert['event_type']} | f={freq_hz:.3f} Hz (Nom={self.nominal_freq_hz:.2f} Hz) | "
                f"Score={f_alert['score']:.4f} | Dur={f_alert['duration_sec']:.2f}s"
            )

        # Update rolling baseline only when voltage is clean and outside any active alert
        inside_normal_band = self.min_normal_kv <= kv <= self.max_normal_kv
        is_clean_voltage_sample = (
            not self.v_monitor.active
            and v_state == "NOMINAL"
            and inside_normal_band
            and voltage_score < self.v_monitor.exit_threshold
            and rel_kv_dev < 0.02
            and raw_nom_kv_dev < 0.08
        )
        if is_clean_voltage_sample:
            self.baseline_window.append(kv)

        self.csv_buffer.append([
            ts_str,
            self.substation_id,
            f"{kv:.3f}",
            f"{baseline_kv:.3f}",
            f"{freq_hz:.4f}",
            f"{voltage_score:.4f}",
            f"{freq_score:.4f}",
            v_state,
            f_state,
        ])
        if len(self.csv_buffer) >= self.buffer_limit:
            self.flush_csv_buffer()

    def _force_close_channel(
        self,
        channel_mon: MetricChannelMonitor,
        ts_str: str,
        now_dt: datetime,
        metric_val: float,
        ref_val: float,
        reason: str,
    ) -> None:
        # Force-close a single open channel alert and log a FORCE_END record
        if not channel_mon.active or channel_mon.event_start is None:
            return
        avg_s, peak_s = channel_mon.stats(channel_mon.last_score)
        dur = max(0.0, (now_dt - channel_mon.event_start).total_seconds())
        self._log_alert_event(
            ts_str,
            metric_val,
            ref_val,
            {
                "channel": channel_mon.channel_name,
                "transition": "FORCE_END",
                "event_type": f"{channel_mon.event_type}_END",
                "severity": channel_mon.severity,
                "score": channel_mon.last_score,
                "avg_score": avg_s,
                "peak_score": peak_s,
                "transient_sec": channel_mon.transient_sec,
                "duration_sec": dur,
                "action_hint": f"Forced closure ({reason})",
            },
        )
        channel_mon.reset()

    def close(self, reason: str = "Monitor stopped") -> None:
        # Force-close active channel alerts and flush buffered telemetry
        now_dt = (
            self._last_metrics["timestamp"]
            if self._last_metrics and "timestamp" in self._last_metrics
            else datetime.now()
        )
        ts_str = now_dt.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        kv = self._last_metrics["kv"] if self._last_metrics else 0.0
        base_kv = self._last_metrics["baseline_kv"] if self._last_metrics else self.nominal_kv
        freq_hz = self._last_metrics["freq_hz"] if self._last_metrics else self.nominal_freq_hz

        self._force_close_channel(
            self.v_monitor, ts_str, now_dt, kv, base_kv, reason
        )
        self._force_close_channel(
            self.f_monitor, ts_str, now_dt, freq_hz, self.nominal_freq_hz, reason
        )
        self.flush_csv_buffer()

    def print_summary(self) -> None:
        # Print summary counters and log file paths
        baseline_samples = len(self.baseline_window)
        last_base = (
            sum(self.baseline_window) / baseline_samples
            if baseline_samples > 0
            else self.nominal_kv
        )
        print("\n--- Regional HV Monitor Summary (Separated Channels) ---")
        print(
            f"Substation       : {self.substation_id} "
            f"({self.nominal_kv} kV / {self.nominal_freq_hz} Hz)"
        )
        print(f"Source CSV       : {self.input_csv_path}")
        print(f"Normal Voltage   : {self.min_normal_kv:.1f} - {self.max_normal_kv:.1f} kV")
        print(
            f"Voltage Alerts   : {self.v_monitor.total_events} "
            f"(Transients: {self.v_monitor.total_transients})"
        )
        print(
            f"Frequency Alerts : {self.f_monitor.total_events} "
            f"(Transients: {self.f_monitor.total_transients})"
        )
        print(
            f"Baseline Samples : {baseline_samples} "
            f"(Last Baseline: {last_base:.3f} kV)"
        )
        print(f"Telemetry Log    : {self.telemetry_log}")
        print(f"Events Log       : {self.events_log}")

    async def run(self, interval_sec: float = 0.1) -> None:
        # Run the async stream loop and guarantee cleanup on exit
        print(
            f"Starting HighVoltageRegionalMonitor [{self.substation_id}] "
            f"| Source: {self.input_csv_path} "
            f"| Nominal: {self.nominal_kv} kV ({self.min_normal_kv:.1f}-{self.max_normal_kv:.1f} kV) "
            f"/ {self.nominal_freq_hz} Hz\n"
        )
        try:
            async for sample in self.stream_regional_measurements(interval_sec=interval_sec):
                await self.process_sample(sample, sample.get("timestamp"))
        except Exception as e:
            print(f"\n[Monitor Error]: {e}")
            raise
        finally:
            self.close("Stream finished or interrupted")


if __name__ == "__main__":
    monitor = HighVoltageRegionalMonitor(
        substation_id="RET_HV_220KV_01",
        input_csv_path="measurements_220kv.csv",
        replay_speed=0.0,
        nominal_kv=220.0,
        nominal_freq_hz=50.0,
        min_normal_kv=210.0,
        max_normal_kv=235.0,
        max_voltage_dev_pct=0.10,
        max_freq_dev_hz=0.20,
    )
    try:
        asyncio.run(monitor.run(interval_sec=0.1))
    except KeyboardInterrupt:
        monitor.print_summary()
    else:
        monitor.print_summary()
