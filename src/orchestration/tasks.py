"""ASE-ETL Luigi orchestration tasks."""

import json
import hashlib
import re
import logging
import os
import subprocess
import sys
from datetime import datetime, timezone

import luigi

from src.orchestration.config import (
    PROJECT_ROOT,
    LOG_ROOT,
    STATE_ROOT,
)

from pathlib import Path
from sqlalchemy import text

logger = logging.getLogger(__name__)


class BronzeIngestion(luigi.Task):
    """Run the existing incremental Bronze ingestion pipeline."""

    run_id = luigi.Parameter()
    source = luigi.ChoiceParameter(
        choices=["local", "minio"],
        default="local",
    )

    def output(self):
        return luigi.LocalTarget(
            str(
                STATE_ROOT
                / f"bronze_{self.run_id}_{self.source}.json"
            )
        )

    def run(self):
        logger.info(
            "Starting Bronze ingestion: run_id=%s, source=%s",
            self.run_id,
            self.source,
        )

        command = [
            sys.executable,
            "-m",
            "src.ingestion.bronze_ingestion",
            "--source",
            self.source,
        ]

        log_path = (
            LOG_ROOT / f"bronze_{self.run_id}.log"
        )

        with log_path.open(
            "w",
            encoding="utf-8",
        ) as log_file:
            result = subprocess.run(
                command,
                cwd=PROJECT_ROOT,
                stdout=log_file,
                stderr=subprocess.STDOUT,
                check=False,
            )

        if result.returncode != 0:
            raise RuntimeError(
                "Bronze ingestion failed. "
                f"See log: {log_path}"
            )

        # Verify Bronze publication before marking
        # the Luigi task as completed.

        verification_command = [
            sys.executable,
            str(
                PROJECT_ROOT
                / "tests"
                / "verify_bronze_publication.py"
            ),
        ]

        with log_path.open("a", encoding="utf-8") as log_file:
            verification = subprocess.run(
                verification_command,
                cwd=PROJECT_ROOT,
                stdout=log_file,
                stderr=subprocess.STDOUT,
                check=False,
            )

        if verification.returncode != 0:
            raise RuntimeError(
                "Bronze publication verification failed. "
                f"See log: {log_path}"
            )

        completed_at = datetime.now(
            timezone.utc
        ).isoformat()

        metadata = {
            "run_id": self.run_id,
            "task": "BronzeIngestion",
            "source": self.source,
            "status": "SUCCESS",
            "completed_at_utc": completed_at,
            "log_path": str(log_path),
        }

        with self.output().open("w") as file:
            json.dump(metadata, file, indent=2)

        logger.info(
            "Bronze ingestion finished: %s",
            self.run_id,
        )

class SilverTransformation(luigi.Task):
    """Transform verified Bronze data into local Silver datasets."""

    run_id = luigi.Parameter()
    source = luigi.ChoiceParameter(
        choices=["local", "minio"],
        default="local",
    )

    def requires(self):
        return BronzeIngestion(
            run_id=self.run_id,
            source=self.source,
        )

    def output(self):
        return luigi.LocalTarget(
            str(
                STATE_ROOT
                / f"silver_transform_{self.run_id}_{self.source}.json"
            )
        )

    def run(self):
        logger.info(
            "Starting Silver transformation: %s",
            self.run_id,
        )

        command = [
            sys.executable,
            "-m",
            "src.silver.pipeline",
        ]

        log_path = (
            LOG_ROOT
            / f"silver_transform_{self.run_id}.log"
        )

        with log_path.open("w", encoding="utf-8") as log_file:
            result = subprocess.run(
                command,
                cwd=PROJECT_ROOT,
                stdout=log_file,
                stderr=subprocess.STDOUT,
                check=False,
            )

        if result.returncode != 0:
            raise RuntimeError(
                "Silver transformation failed. "
                f"See log: {log_path}"
            )

        metadata = {
            "run_id": self.run_id,
            "task": "SilverTransformation",
            "status": "SUCCESS",
            "completed_at_utc": datetime.now(
                timezone.utc
            ).isoformat(),
            "log_path": str(log_path),
        }

        with self.output().open("w") as file:
            json.dump(metadata, file, indent=2)

        logger.info(
            "Silver transformation completed: %s",
            self.run_id,
        )

class SilverPublication(luigi.Task):
    """Publish validated Silver datasets to PostgreSQL and MinIO."""

    run_id = luigi.Parameter()
    source = luigi.ChoiceParameter(
        choices=["local", "minio"],
        default="local",
    )

    def requires(self):
        return SilverTransformation(
            run_id=self.run_id,
            source=self.source,
        )

    def output(self):
        return luigi.LocalTarget(
            str(
                STATE_ROOT
                / f"silver_publish_{self.run_id}_{self.source}.json"
            )
        )

    def run(self):
        logger.info(
            "Starting Silver publication: %s",
            self.run_id,
        )

        command = [
            sys.executable,
            "-m",
            "src.silver.publish_idempotent",
        ]

        log_path = (
            LOG_ROOT
            / f"silver_publish_{self.run_id}.log"
        )

        with log_path.open("w", encoding="utf-8") as log_file:
            result = subprocess.run(
                command,
                cwd=PROJECT_ROOT,
                stdout=log_file,
                stderr=subprocess.STDOUT,
                check=False,
            )

        if result.returncode != 0:
            raise RuntimeError(
                "Silver publication failed. "
                f"See log: {log_path}"
            )

        metadata = {
            "run_id": self.run_id,
            "task": "SilverPublication",
            "status": "SUCCESS",
            "completed_at_utc": datetime.now(
                timezone.utc
            ).isoformat(),
            "log_path": str(log_path),
        }

        with self.output().open("w") as file:
            json.dump(metadata, file, indent=2)

        logger.info(
            "Silver publication completed: %s",
            self.run_id,
        )

class GoldModeling(luigi.Task):
    """Build and validate Gold dimensional tables."""

    run_id = luigi.Parameter()
    source = luigi.ChoiceParameter(
        choices=["local", "minio"],
        default="local",
    )

    def requires(self):
        return SilverPublication(
            run_id=self.run_id,
            source=self.source,
        )

    def output(self):
        return luigi.LocalTarget(
            str(
                STATE_ROOT
                / f"gold_model_{self.run_id}_{self.source}.json"
            )
        )

    def run(self):
        command = [
            sys.executable,
            "-m",
            "src.gold.pipeline",
        ]

        log_path = LOG_ROOT / f"gold_model_{self.run_id}.log"

        with log_path.open("w", encoding="utf-8") as log_file:
            result = subprocess.run(
                command,
                cwd=PROJECT_ROOT,
                stdout=log_file,
                stderr=subprocess.STDOUT,
                check=False,
            )

        if result.returncode != 0:
            raise RuntimeError(
                f"Gold modeling failed. See: {log_path}"
            )

        metadata = {
            "run_id": self.run_id,
            "task": "GoldModeling",
            "status": "SUCCESS",
            "completed_at_utc": datetime.now(
                timezone.utc
            ).isoformat(),
            "log_path": str(log_path),
        }

        with self.output().open("w") as file:
            json.dump(metadata, file, indent=2)

class GoldPublication(luigi.Task):
    """Publish validated Gold warehouse data to MinIO."""

    run_id = luigi.Parameter()
    source = luigi.ChoiceParameter(
        choices=["local", "minio"],
        default="local",
    )

    def requires(self):
        return GoldModeling(
            run_id=self.run_id,
            source=self.source,
        )

    def output(self):
        return luigi.LocalTarget(
            str(
                STATE_ROOT
                / f"gold_publish_{self.run_id}_{self.source}.json"
            )
        )

    def run(self):
        command = [
            sys.executable,
            "-m",
            "src.gold.release",
        ]

        log_path = LOG_ROOT / f"gold_publish_{self.run_id}.log"

        with log_path.open("w", encoding="utf-8") as log_file:
            result = subprocess.run(
                command,
                cwd=PROJECT_ROOT,
                stdout=log_file,
                stderr=subprocess.STDOUT,
                check=False,
            )

        if result.returncode != 0:
            raise RuntimeError(
                f"Gold publication failed. See: {log_path}"
            )

        metadata = {
            "run_id": self.run_id,
            "task": "GoldPublication",
            "status": "SUCCESS",
            "completed_at_utc": datetime.now(
                timezone.utc
            ).isoformat(),
            "log_path": str(log_path),
        }

        with self.output().open("w") as file:
            json.dump(metadata, file, indent=2)

class EmployeeSyncAcknowledgement(luigi.Task):
    """Acknowledge API employee changes after Gold publication."""

    run_id = luigi.Parameter()

    source = luigi.ChoiceParameter(
        choices=["local", "minio"],
        default="local",
    )

    def requires(self):
        return GoldPublication(
            run_id=self.run_id,
            source=self.source,
        )

    def output(self):
        return luigi.LocalTarget(
            str(
                STATE_ROOT
                / f"employee_sync_{self.run_id}_{self.source}.json"
            )
        )

    def run(self):
        command = [
            sys.executable,
            "-m",
            "src.orchestration.acknowledge",
        ]

        log_path = LOG_ROOT / f"employee_sync_{self.run_id}.log"

        with log_path.open("w", encoding="utf-8") as log_file:
            result = subprocess.run(
                command,
                cwd=PROJECT_ROOT,
                stdout=log_file,
                stderr=subprocess.STDOUT,
                check=False,
            )

        if result.returncode != 0:
            raise RuntimeError(
                f"API synchronization failed. See: {log_path}"
            )

        metadata = {
            "run_id": self.run_id,
            "task": "EmployeeSyncAcknowledgement",
            "status": "SUCCESS",
            "completed_at_utc": datetime.now(
                timezone.utc
            ).isoformat(),
            "log_path": str(log_path),
        }

        with self.output().open("w") as file:
            json.dump(metadata, file, indent=2)

class SQLAnalytics(luigi.Task):
    """Build and validate SQL analytics and data marts."""

    run_id = luigi.Parameter()

    source = luigi.ChoiceParameter(
        choices=["local", "minio"],
        default="local",
    )

    def requires(self):
        return EmployeeSyncAcknowledgement(
            run_id=self.run_id,
            source=self.source,
        )

    def output(self):
        return luigi.LocalTarget(
            str(
                STATE_ROOT
                / f"analytics_{self.run_id}_{self.source}.json"
            )
        )

    def run(self):
        logger.info(
            "Starting SQL analytics: %s",
            self.run_id,
        )

        command = [
            sys.executable,
            "-m",
            "src.analytics.pipeline",
        ]

        log_path = (
            LOG_ROOT
            / f"analytics_{self.run_id}.log"
        )

        with log_path.open(
            "w",
            encoding="utf-8",
        ) as log_file:
            result = subprocess.run(
                command,
                cwd=PROJECT_ROOT,
                stdout=log_file,
                stderr=subprocess.STDOUT,
                check=False,
            )

        if result.returncode != 0:
            raise RuntimeError(
                "SQL analytics pipeline failed. "
                f"See log: {log_path}"
            )

        metadata = {
            "run_id": self.run_id,
            "task": "SQLAnalytics",
            "source": self.source,
            "status": "SUCCESS",
            "completed_at_utc": datetime.now(
                timezone.utc
            ).isoformat(),
            "log_path": str(log_path),
        }

        with self.output().open("w") as file:
            json.dump(metadata, file, indent=2)

        logger.info(
            "SQL analytics completed: %s",
            self.run_id,
        )

class ReportGeneration(luigi.Task):
    """Generate the final analytics CSV reports."""

    run_id = luigi.Parameter()

    source = luigi.ChoiceParameter(
        choices=["local", "minio"],
        default="local",
    )

    def requires(self):
        return SQLAnalytics(
            run_id=self.run_id,
            source=self.source,
        )

    def output(self):
        return luigi.LocalTarget(
            str(
                STATE_ROOT
                / f"reports_{self.run_id}_{self.source}.json"
            )
        )

    def complete(self):
        """
        Return True only when the report output exists
        and matches the active Gold publication.
        """

        if not self.output().exists():
            return False

        try:
            # Read this Luigi run's completion metadata.
            with self.output().open("r") as file:
                state = json.load(file)

            if (
                state.get("run_id") != self.run_id
                or state.get("source") != self.source
                or state.get("status") != "SUCCESS"
            ):
                return False

            report_dir = Path(
                state["report_directory"]
            )

            manifest_path = report_dir / "manifest.json"

            if not manifest_path.is_file():
                return False

            manifest = json.loads(
                manifest_path.read_text(
                    encoding="utf-8"
                )
            )

            from src.analytics.reporting import REPORTS
            from src.silver.publish import ENGINE

            # Confirm expected report inventory.
            if set(manifest.get("reports", {})) != set(REPORTS):
                return False

            for report_name, view_name in REPORTS.items():
                metadata = manifest["reports"][report_name]

                if metadata.get("source_view") != view_name:
                    return False

                filename = metadata.get("file")

                if filename != f"{report_name}.csv":
                    return False

                report_path = report_dir / filename
                if not report_path.is_file():
                    return False
                expected_hash = metadata.get("sha256")
                if not isinstance(expected_hash, str) or not re.fullmatch(r"[0-9a-f]{64}", expected_hash):
                    return False
                digest = hashlib.sha256()
                with report_path.open("rb") as stream:
                    for block in iter(lambda: stream.read(1024 * 1024), b""):
                        digest.update(block)
                if digest.hexdigest() != expected_hash:
                    return False

            # Retrieve the currently published Gold snapshot.
            with ENGINE.connect() as connection:
                publication = connection.execute(
                    text("""
                        SELECT
                            gold_snapshot_fingerprint,
                            publication_status
                        FROM gold._publication_registry
                        WHERE publication_id = 1
                    """)
                ).mappings().one_or_none()

            if publication is None:
                return False

            if publication["publication_status"] != "PUBLISHED":
                return False

            current_fingerprint = publication[
                "gold_snapshot_fingerprint"
            ]

            reported_fingerprint = manifest.get(
                "publication", {}
            ).get("gold_snapshot_fingerprint")

            if not current_fingerprint:
                return False

            return (
                reported_fingerprint == current_fingerprint
            )

        except (
            OSError,
            ValueError,
            KeyError,
            TypeError,
            json.JSONDecodeError,
        ):
            return False

    def run(self):
        import os

        logger.info(
            "Starting report generation: %s",
            self.run_id,
        )

        command = [
            sys.executable,
            "-m",
            "src.analytics.reporting",
        ]

        log_path = LOG_ROOT / f"reports_{self.run_id}.log"

        with log_path.open(
            "w",
            encoding="utf-8",
        ) as log_file:
            result = subprocess.run(
                command,
                cwd=PROJECT_ROOT,
                stdout=log_file,
                stderr=subprocess.STDOUT,
                check=False,
            )

        if result.returncode != 0:
            raise RuntimeError(
                "Report generation failed. "
                f"See log: {log_path}"
            )

        # Select the report batch emitted by THIS exporter invocation,
        # never an unrelated latest directory.
        output_text = log_path.read_text(
            encoding="utf-8",
            errors="replace",
        )
        matched = re.findall(r"^Output:\s*(.+?)\s*$", output_text, re.MULTILINE)
        if len(matched) != 1:
            raise RuntimeError(f"Expected one report output path in {log_path}; found {len(matched)}")
        report_dir = Path(matched[0].strip())
        if not report_dir.is_absolute():
            report_dir = PROJECT_ROOT / report_dir
        report_dir = report_dir.resolve()
        expected_root = (PROJECT_ROOT / "reports" / "analytics").resolve()
        if report_dir.parent != expected_root or not (report_dir / "manifest.json").is_file():
            raise RuntimeError("Exporter reported an invalid report batch location")

        # Verify exactly this exported batch, including checksums and rows.
        verification_command = [
            sys.executable,
            "-m",
            "tests.verify_analytics_reports",
            "--directory",
            str(report_dir),
        ]

        with log_path.open("a", encoding="utf-8") as log_file:
            verification = subprocess.run(
                verification_command,
                cwd=PROJECT_ROOT,
                stdout=log_file,
                stderr=subprocess.STDOUT,
                check=False,
            )

        if verification.returncode != 0:
            raise RuntimeError(
                "Analytics report verification failed. "
                f"See log: {log_path}"
            )

        manifest_path = report_dir / "manifest.json"

        manifest = json.loads(
            manifest_path.read_text(encoding="utf-8")
        )

        if len(manifest.get("reports", {})) != 13:
            raise ValueError(
                "Expected 13 analytics reports."
            )

        metadata = {
            "run_id": self.run_id,
            "task": "ReportGeneration",
            "source": self.source,
            "status": "SUCCESS",
            "completed_at_utc": datetime.now(
                timezone.utc
            ).isoformat(),
            "report_directory": str(report_dir),
            "report_count": len(manifest["reports"]),
            "log_path": str(log_path),
        }

        with self.output().open("w") as file:
            json.dump(metadata, file, indent=2)

        logger.info(
            "Report generation completed: %s",
            self.run_id,
        )

class RunETLPipeline(luigi.WrapperTask):
    """Entry point for the complete ASE-ETL workflow."""

    run_id = luigi.Parameter()

    source = luigi.ChoiceParameter(
        choices=["local", "minio"],
        default="local",
    )

    def requires(self):
        return ReportGeneration(
            run_id=self.run_id,
            source=self.source,
        )