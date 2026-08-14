import json
from pathlib import Path

from exporters.base_exporter import Exporter


class JSONExporter(Exporter):
    def export(self, rows: list[dict], output_path: Path) -> None:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with output_path.open("w", encoding="utf-8") as file:
            json.dump(rows, file, indent=2, ensure_ascii=False, default=str)
