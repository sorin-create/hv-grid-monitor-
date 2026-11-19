import asyncio
import csv
import tempfile
from datetime import datetime, timedelta
from pathlib import Path

from hv_grid_monitor import AlertRuleConfig, HighVoltageRegionalMonitor


def generate_test_csv(csv_path: Path) -> None:
    # Generate a synthetic CSV covering baseline warmup, a transient, a voltage alert, and a frequency alert
    start = datetime(2026, 11, 19, 10, 0, 0)
    rows: list[tuple[str, float, float]] = []

    def add_sequence(count: int, step_sec: float, kv_fn, freq_fn) -> None:
        nonlocal start
        for i in range(count):
            ts = start.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
            rows.append((ts, kv_fn(i), freq_fn(i)))
            start += timedelta(seconds=step_sec)

    # 1. Warmup (10 samples at 220 kV and 50.0 Hz)
    add_sequence(10, 0.5, lambda _: 220.0, lambda _: 50.0)

    # 2. Short voltage transient (less than 1.0s) to verify filtering
    add_sequence(1, 0.5, lambda _: 198.0, lambda _: 50.0)

    # 3. Brief return to nominal values
    add_sequence(4, 0.5, lambda _: 220.0, lambda _: 50.0)

    # 4. Voltage alert: START (WARNING) -> ESCALATED (CRITICAL) -> END
    # With max_voltage_dev_pct=0.10 (22 kV):
    # - 208 kV gives a 12 kV deviation (score ~0.545, between 0.35 and 0.75) -> WARNING
    # - 196 kV gives a 24 kV deviation (score 1.00, above 0.75) -> CRITICAL
    add_sequence(4, 0.5, lambda _: 208.0, lambda _: 50.0)
    add_sequence(4, 0.5, lambda _: 196.0, lambda _: 50.0)
    add_sequence(4, 0.5, lambda _: 220.0, lambda _: 50.0)

    # 5. Frequency alert: START -> END (with max_freq_dev_hz=0.20, 49.84 Hz gives 0.16 Hz deviation -> score 0.80)
    add_sequence(4, 0.5, lambda _: 220.0, lambda _: 49.84)
    add_sequence(4, 0.5, lambda _: 220.0, lambda _: 50.0)

    with open(csv_path, mode="w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["Timestamp", "Voltage_Kv", "Freq_Hz"])
        writer.writerows(rows)


async def run_simulation_test() -> None:
    # Run the monitor against a temporary CSV file so test logs stay isolated
    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp_path = Path(tmp_dir)
        csv_file = tmp_path / "sim_220kv.csv"
        generate_test_csv(csv_file)

        monitor = HighVoltageRegionalMonitor(
            substation_id="TEST_220KV_01",
            input_csv_path=str(csv_file),
            replay_speed=0.0,
            nominal_kv=220.0,
            nominal_freq_hz=50.0,
            min_normal_kv=210.0,
            max_normal_kv=235.0,
            max_voltage_dev_pct=0.10,
            max_freq_dev_hz=0.20,
            voltage_rules=AlertRuleConfig(
                warning_score=0.35,
                critical_score=0.75,
                exit_score=0.22,
                min_duration_sec=1.0,
            ),
            freq_rules=AlertRuleConfig(
                warning_score=0.30,
                critical_score=0.70,
                exit_score=0.18,
                min_duration_sec=0.5,
            ),
            warmup_samples=10,
            buffer_limit=20,
        )

        # Point log outputs to the temporary directory and reinitialize headers
        monitor.telemetry_log = str(tmp_path / "telemetry.csv")
        monitor.events_log = str(tmp_path / "events.csv")
        monitor._init_logs()

        await monitor.run()
        monitor.print_summary()

        print("\n--- events.csv contents ---")
        with open(monitor.events_log, mode="r", encoding="utf-8") as f:
            print(f.read())


if __name__ == "__main__":
    asyncio.run(run_simulation_test())
