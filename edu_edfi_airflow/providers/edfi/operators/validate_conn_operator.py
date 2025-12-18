import json
import logging
from typing import Any, Dict, List, Optional

from airflow.models import BaseOperator
from airflow.utils.context import Context

from edu_edfi_airflow.scripts.validate_edfi_connections import validate_edfi_connections

class ValidateEdFiConnectionsOperator(BaseOperator):
    """
    Airflow operator to validate all EdFi connections whose conn_id matches `{conn_prefix}_{tenant}_{year}`.

    tenant_lea_mapping_json : str
        JSON string mapping tenant codes to LEA IDs. This is templated, so you
        can pull from Airflow Variables (e.g. {{ var.value.edfi_tenant_lea_mapping }}).
    conn_prefix : str
        Connection ID prefix (default: "edfi"). Connections must match pattern: {conn_prefix}_{tenant}_{year}
    exclude_conns : list[str], optional
        List of connection IDs to skip during validation.
    fail_on : list[str], optional
        List of statuses that should cause task failure. Valid values: MISMATCH, NO_MAPPING, NO_ORG_ID, ERROR.
        Defaults to all four if not specified. Pass empty list to never fail.
    """

    template_fields = ("tenant_lea_mapping_json",)

    def __init__(
        self,
        *,
        tenant_lea_mapping_json: str,
        conn_prefix: str = "edfi",
        exclude_conns: Optional[List[str]] = None,
        fail_on: Optional[List[str]] = ["MISMATCH", "NO_MAPPING", "NO_ORG_ID", "ERROR"],
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)

        self.tenant_lea_mapping_json = tenant_lea_mapping_json
        self.conn_prefix = conn_prefix
        self.exclude_conns = exclude_conns or []
        self.fail_on = fail_on

    def execute(self, context: Context) -> List[Dict[str, Any]]:
        """
        Run EdFi LEA validation and optionally fail the task based on results.
        """

        # Parse tenant mapping
        tenant_lea_mapping: Dict[str, str] = json.loads(self.tenant_lea_mapping_json)

        self.log.info("Starting EdFi connection validation (This task will fail on: %s)", ", ".join(self.fail_on) if self.fail_on else "nothing")

        # run validate
        results = validate_edfi_connections(tenant_lea_mapping, conn_prefix=self.conn_prefix, exclude_conns=self.exclude_conns)

        # Detailed summary by type
        total = len(results)
        matches = [r for r in results if r["status"] == "MATCH"]
        mismatches = [r for r in results if r["status"] == "MISMATCH"]
        no_mapping = [r for r in results if r["status"] == "NO_MAPPING"]
        no_org_id = [r for r in results if r["status"] == "NO_ORG_ID"]
        errors = [r for r in results if r["status"] == "ERROR"]

        logging.info(
            f"\nSummary: {len(matches)}/{total} matches, {len(mismatches)} mismatches, "
            f"{len(no_mapping)} no mapping, {len(no_org_id)} connections with no org ID, {len(errors)} errors"
        )
        
        # Log issue details
        if mismatches:
            self.log.error("Mismatches (%d):", len(mismatches))
            for r in mismatches:
                self.log.error("  - %s: expected %s, got %s", r['connection_id'], r.get('expected_lea_id'), r.get('actual_org_id'))

        if no_mapping:
            self.log.error("No Mapping Found (%d):", len(no_mapping))
            for r in no_mapping:
                self.log.error("  - %s: tenant '%s' not in mapping", r['connection_id'], r.get('tenant'))

        if no_org_id:
            self.log.error("No LEA ID Found (%d):", len(no_org_id))
            for r in no_org_id:
                self.log.error("  - %s: %s", r['connection_id'], r.get('message'))

        if errors:
            self.log.error("Errors (%d):", len(errors))
            for r in errors:
                self.log.error("  - %s: %s", r['connection_id'], r.get('message'))


        # Fail if any results match the fail_on list
        failures = [r for r in results if r["status"] in self.fail_on]
        if failures:
            raise RuntimeError(
                f"EdFi validation failed: {len(mismatches)} mismatches, "
                f"{len(no_mapping)} no mapping, "
                f"{len(no_org_id)} connections with no org ID, "
                f"{len(errors)} errors."
            )

        self.log.info("Validation complete. No issues in fail_on list (%s)", ", ".join(self.fail_on) if self.fail_on else "nothing")
        return results