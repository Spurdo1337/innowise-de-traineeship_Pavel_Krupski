import json
from decimal import Decimal
from pathlib import Path

from exporters.base_exporter import Exporter


def _json_default(value):
    if isinstance(value, Decimal):
        return float(value)
    raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")


class JSONExporter(Exporter):
    def export(self, rows: list[dict], output_path: Path) -> None:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with output_path.open("w", encoding="utf-8") as file:
            json.dump(rows, file, indent=2, ensure_ascii=False, default=_json_default)
