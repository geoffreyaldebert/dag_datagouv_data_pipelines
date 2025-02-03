from typing import Optional
from pathlib import Path
import yaml


class DataGouvLog:
    def __init__(
        self,
        type: str,
        catalog_resource_id: str,
        catalog_columns: dict[str, str],
        segments: list[str],
        global_pattern: str,
        static_segments: list[str] = [],
        additional_patterns: Optional[dict[str, str]] = None,
    ) -> None:
        self.type = type
        self.catalog_columns = catalog_columns
        self.catalog_destination_name = f"catalog_{self.type}.csv"
        self.catalog_download_url = (
            f"https://www.data.gouv.fr/fr/datasets/r/{catalog_resource_id}"
        )
        self.static_segments = static_segments
        self.output_columns = (
            ["date_metric"]
            + [
                column
                for column in self.catalog_columns.values()
                if column not in ["url", "slug"]
            ]
            + ["nb_visit", "nb_visit_apis", "nb_visit_total"]
            + [f"nb_visit_{segment.replace('/', '')}" for segment in segments]
            + ["nb_visit_static"]
        )
        self.log_patterns = {
            segment.replace("/", ""):
                rf"{global_pattern}/{segment}/{self.type}/([^/?\s]*)"
            for segment in segments
        }
        if additional_patterns:
            additional_patterns = {
                segment: global_pattern + pattern
                for segment, pattern in additional_patterns.items()
            }
            self.log_patterns.update(additional_patterns)


class MetricsConfig:
    code_folder_full_path = Path(__file__).parent.absolute()
    tmp_folder = "metrics/"

    def __init__(
        self,
    ) -> None:
        with open(f"{self.code_folder_full_path}/config.yaml", "r") as config_file:
            config_data = yaml.safe_load(config_file)
            self.database_schema = config_data["database_schema"]
            self.api_segments = config_data["api_segments"]
            self.web_segments = config_data["web_segments"]
            self.all_segments = self.api_segments + self.web_segments
            self.logs_config = [
                DataGouvLog(
                    segments=self.all_segments,
                    global_pattern=config_data["global_pattern"],
                    **obj,
                )
                for obj in config_data["datagouv_logs"]
            ]
        self.all_static_segments = [
            segment for config in self.logs_config for segment in config.static_segments
        ]
