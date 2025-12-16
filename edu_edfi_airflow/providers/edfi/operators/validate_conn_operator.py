# providers/edfi/operators/validate_edfi_connections.py

import json
import logging
from typing import Any, Dict, List

from airflow.models import BaseOperator
from airflow.utils.context import Context

from edu_edfi_airflow.scripts.validate_edfi_connections import validate_edfi_connections

class ValidateEdFiConnectionsOperator(BaseOperator):
    """
    Airflow operator to validate all EdFi connections whose conn_id matches `edfi_{tenant}_{year}`.

    tenant_lea_mapping_json : str
        JSON string mapping tenant codes to LEA IDs. This is templated, so you
        can pull from Airflow Variables (e.g. {{ var.value.edfi_tenant_lea_mapping }}).
    quiet : bool
        If True, suppress individual connection result output
    fail_on_any_issue : bool
        If True, task will fail if any connection is MISMATCH, NO_MAPPING, NO_ORG_ID, or ERROR.
    """

    template_fields = ("tenant_lea_mapping_json",)

    def __init__(
        self,
        *,
        tenant_lea_mapping_json: str,
        quiet: bool = False,
        fail_on_any_issue: bool = True,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)

        self.tenant_lea_mapping_json = tenant_lea_mapping_json
        self.quiet = quiet
        self.fail_on_any_issue = fail_on_any_issue

    def execute(self, context: Context) -> List[Dict[str, Any]]:
        """
        Run EdFi LEA validation and optionally fail the task based on results.
        """

        # Parse tenant mapping
        tenant_lea_mapping: Dict[str, str] = json.loads(self.tenant_lea_mapping_json)

        if not self.quiet:
            self.log.info(
                "Starting EdFi connection validation for %d tenants",
                len(tenant_lea_mapping),
            )

        # run validate
        results = validate_edfi_connections(tenant_lea_mapping, quiet=self.quiet)

        # Detailed summary by type
        total = len(results)
        matches = [r for r in results if r["status"] == "MATCH"]
        mismatches = [r for r in results if r["status"] == "MISMATCH"]
        no_mapping = [r for r in results if r["status"] == "NO_MAPPING"]
        no_org_id = [r for r in results if r["status"] == "NO_ORG_ID"]
        errors = [r for r in results if r["status"] == "ERROR"]

        logging.info(
            f"\nSummary: {len(matches)}/{total} matches, {len(mismatches)} mismatches, "
            f"{len(no_mapping)} no mapping, {len(no_org_id)} onnections with no org ID, {len(errors)} errors"
        )
        
        # Detailed breakdown (always show in quiet mode, or if there are issues)
        if not self.quiet or (mismatches or no_mapping or no_org_id or errors):
            if mismatches:
                self.log.error("Mismatches (%d):", len(mismatches))
                for r in mismatches:
                    self.log.error(
                        "  - %s: expected %s, got %s",
                        r["connection_id"], r.get("expected_lea_id"), r.get("actual_org_id")
                    )

            if no_mapping:
                self.log.error("No Mapping Found (%d):", len(no_mapping))
                for r in no_mapping:
                    self.log.error(
                        "  - %s: tenant '%s' not in mapping",
                        r["connection_id"], r.get("tenant")
                    )

            if no_org_id:
                self.log.error("No LEA ID Found (%d):", len(no_org_id))
                for r in no_org_id:
                    self.log.error(
                        "  - %s: %s",
                        r["connection_id"], r.get("message")
                    )

            if errors:
                self.log.error("Errors (%d):", len(errors))
                for r in errors:
                    self.log.error(
                        "  - %s: %s",
                        r["connection_id"], r.get("message")
                    )

        # Fail run if any of this status are returned
        if self.fail_on_any_issue and (mismatches or no_mapping or no_org_id or errors):
            raise RuntimeError(
                f"EdFi validation failed: {len(mismatches)} mismatches, "
                f"{len(no_mapping)} no mapping, "
                f"{len(no_org_id)} connections with no org ID, "
                f"{len(errors)} errors."
            )

        return results