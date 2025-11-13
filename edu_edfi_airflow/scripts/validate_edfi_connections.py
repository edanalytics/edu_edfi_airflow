import re
import json
import yaml
import argparse
from typing import Dict, List, Tuple, Optional
from airflow.models import Connection
from edfi_api_client import EdFiClient
from ea_airflow_util.callables.airflow_connection import list_conn


def get_lea_id_with_fallback(client: EdFiClient, expected_lea_id: str) -> Tuple[Optional[str], str]:
    """
    Get LEA ID using fallback strategy: token_info -> LEAs endpoint -> calendars/schools.
    
    Args:
        client: EdFiClient instance
        expected_lea_id: Expected LEA ID for validation
        
    Returns:
        Tuple of (lea_id, method_used)
    """
    # Step 1: Try token_info
    lea_ids, method = try_token_info(client)
    if len(lea_ids) == 1:
        # Only one LEA returned, check if it matches expected
        if str(expected_lea_id) == lea_ids[0]:
            return lea_ids[0], f"token_info ({method})"
        else:
            return None, f"token_info_mismatch (returned: {lea_ids}, expected: {expected_lea_id})"
    elif len(lea_ids) > 1:
        # Multiple LEAs returned, flag as red flag and do not continue
        return None, f"token_info_multiple_leas (returned: {lea_ids})"
    
    # Step 2: Try LEAs endpoint
    lea_ids, method = try_leas_endpoint(client, expected_lea_id)
    if len(lea_ids) == 1:
        return lea_ids[0], f"leas_endpoint ({method})"
    # Step 3: Try calendars -> schools
    lea_ids, method = try_calendars_schools(client, expected_lea_id)
    if len(lea_ids) == 1:
        return lea_ids[0], f"calendars_schools ({method})"
    return None, "all_methods_failed"


def try_token_info(client: EdFiClient) -> Tuple[List[str], str]:
    """Try to get LEA IDs from token_info endpoint."""
    try:
        token_info = client.get_token_info()
        education_orgs = token_info.get('education_organizations', [])
        
        lea_ids = [str(org['local_education_agency_id']) for org in education_orgs
                  if org.get('type') == 'edfi.LocalEducationAgency' and 'local_education_agency_id' in org]
        
        return lea_ids, f"found_{len(lea_ids)}_leas" if lea_ids else "no_leas_found"
    except Exception as e:
        return [], f"error: {str(e)}"


def try_leas_endpoint(client: EdFiClient, expected_lea_id: str) -> Tuple[List[str], str]:
    """Try to get LEA IDs from LEAs endpoint."""
    try:
        leas_resource = client.resource('leas')
        
        # Get all LEAs
        leas = leas_resource.get()
        lea_ids = [str(lea['localEducationAgencyId']) for lea in leas if 'localEducationAgencyId' in lea]
        
        return lea_ids, f"found_{len(lea_ids)}_leas"
    except Exception as e:
        return [], f"error: {str(e)}"


def try_calendars_schools(client: EdFiClient, expected_lea_id: str) -> Tuple[List[str], str]:
    """Try to get LEA IDs through calendars -> schools relationship."""
    try:
        # Get school IDs from calendars
        calendars = client.resource('calendars').get()
        school_ids = {cal['schoolReference']['schoolId'] for cal in calendars 
                     if 'schoolReference' in cal and 'schoolId' in cal['schoolReference']}
        
        if not school_ids:
            return [], "no_schools_in_calendars"
        
        # Get LEA IDs from schools
        schools_resource = client.resource('schools')
        lea_ids = set()
        
        # Get all schools and filter by the school IDs we found
        schools = schools_resource.get()
        for school in schools:
            if 'schoolId' in school and school['schoolId'] in school_ids:
                if 'localEducationAgencyReference' in school:
                    lea_ref = school['localEducationAgencyReference']
                    if 'localEducationAgencyId' in lea_ref:
                        lea_ids.add(str(lea_ref['localEducationAgencyId']))
        
        return list(lea_ids), f"found_{len(lea_ids)}_leas_via_schools"
    except Exception as e:
        return [], f"error: {str(e)}"


def validate_edfi_connections(tenant_lea_mapping: Dict[str, str], quiet: bool = False) -> List[Dict]:
    """
    Validate EdFi connections against tenant LEA ID mappings.
    
    Args:
        tenant_lea_mapping: Dict mapping tenant codes to LEA IDs
        quiet: If True, suppress individual connection result output
        
    Returns:
        List of validation results
    """
    pattern = re.compile(r'edfi_(\w+)_(\d{4})')
    results = []
    
    # Get all connections matching the edfi pattern using ea_airflow_util
    all_edfi_conn_ids = list_conn('edfi_')
    
    # Filter to only connections that match the full pattern
    matching_conn_ids = [conn_id for conn_id in all_edfi_conn_ids if pattern.match(conn_id)]
    
    if not quiet:
        print(f"Found {len(matching_conn_ids)} matching connections")
    
    for conn_id in matching_conn_ids:
        match = pattern.match(conn_id)
        tenant, year = match.groups()
        expected_lea_id = tenant_lea_mapping.get(tenant)
        
        try:
            # Get connection and create EdFi client
            conn = Connection.get_connection_from_secrets(conn_id)
            client = EdFiClient(
                base_url=conn.host,
                client_key=conn.login,
                client_secret=conn.password,
                **conn.extra_dejson
            )
            
            # Use fallback strategy to get LEA ID
            actual_org_id, method_used = get_lea_id_with_fallback(client, str(expected_lea_id) if expected_lea_id else "")
            
            # Compare and record result (ensure both are strings for comparison)
            expected_lea_id_str = str(expected_lea_id) if expected_lea_id is not None else None
            
            if expected_lea_id is None:
                status = "NO_MAPPING"
                message = f"No LEA ID mapping for tenant: {tenant}"
            elif actual_org_id is None:
                status = "NO_ORG_ID"
                message = f"No LEA ID found using any method ({method_used})"
            elif expected_lea_id_str == actual_org_id:
                status = "MATCH"
                message = f"LEA ID matches: {actual_org_id} (via {method_used})"
            else:
                status = "MISMATCH"
                message = f"Expected {expected_lea_id_str}, got {actual_org_id} (via {method_used})"
            
            result = {
                'connection_id': conn_id,
                'tenant': tenant,
                'year': year,
                'expected_lea_id': expected_lea_id,
                'actual_org_id': actual_org_id,
                'method_used': method_used,
                'status': status,
                'message': message
            }
            
        except Exception as e:
            result = {
                'connection_id': conn_id,
                'tenant': tenant,
                'year': year,
                'expected_lea_id': expected_lea_id,
                'actual_org_id': None,
                'method_used': 'error',
                'status': 'ERROR',
                'message': str(e)
            }
        
        results.append(result)
        if not quiet:
            print(f"{result['status']}: {conn_id} - {result['message']}")
    
    return results


def main(tenant_lea_mapping: Dict[str, str], quiet: bool = False):
    """
    Main function that validates EdFi connections.
    
    Args:
        tenant_lea_mapping: Dict mapping tenant codes to LEA IDs (required)
        quiet: If True, suppress individual connection result output
    """
    if not quiet:
        print("EdFi Connection Validation")
        print("=" * 30)
    
    results = validate_edfi_connections(tenant_lea_mapping, quiet)
    
    # Check for multiple LEA IDs per tenant
    tenant_orgs = {}
    for result in results:
        if result['actual_org_id'] and result['status'] in ['MATCH', 'MISMATCH']:
            tenant = result['tenant']
            if tenant not in tenant_orgs:
                tenant_orgs[tenant] = set()
            tenant_orgs[tenant].add(result['actual_org_id'])
    
    multiple_orgs = {tenant: list(orgs) for tenant, orgs in tenant_orgs.items() if len(orgs) > 1}
    if multiple_orgs and not quiet:
        print("\nTenants with multiple LEA IDs:")
        for tenant, lea_ids in multiple_orgs.items():
            print(f"  {tenant}: {', '.join(lea_ids)}")
    
    # Detailed summary by type
    total = len(results)
    matches = [r for r in results if r['status'] == 'MATCH']
    mismatches = [r for r in results if r['status'] == 'MISMATCH']
    no_mapping = [r for r in results if r['status'] == 'NO_MAPPING']
    no_org_id = [r for r in results if r['status'] == 'NO_ORG_ID']
    errors = [r for r in results if r['status'] == 'ERROR']
    
    print(f"\nSummary: {len(matches)}/{total} matches, {len(mismatches)} mismatches, {len(no_mapping)} no mapping, {len(no_org_id)} no org ID, {len(errors)} errors")
    
    # Detailed breakdown (always show in quiet mode, or if there are issues)
    if not quiet or (mismatches or no_mapping or no_org_id or errors):
        if mismatches:
            print(f"\nMismatches ({len(mismatches)}):")
            for r in mismatches:
                print(f"  - {r['connection_id']}: expected {r['expected_lea_id']}, got {r['actual_org_id']}")
        
        if no_mapping:
            print(f"\nNo Mapping Found ({len(no_mapping)}):")
            for r in no_mapping:
                print(f"  - {r['connection_id']}: tenant '{r['tenant']}' not in mapping")
        
        if no_org_id:
            print(f"\nNo LEA ID Found ({len(no_org_id)}):")
            for r in no_org_id:
                print(f"  - {r['connection_id']}: no Local Education Agency found in token_info")
        
        if errors:
            print(f"\nErrors ({len(errors)}):")
            for r in errors:
                print(f"  - {r['connection_id']}: {r['message']}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Validate EdFi connections against tenant LEA ID mappings')
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('--mapping-file', '-f', help='JSON or YAML file containing tenant to LEA ID mapping')
    group.add_argument('--mapping', '-m', help='JSON string containing tenant to LEA ID mapping')
    parser.add_argument('--quiet', '-q', action='store_true', help='Suppress individual connection output, show only summary')
    
    args = parser.parse_args()
    
    if args.mapping_file:
        with open(args.mapping_file, 'r') as f:
            # Try to detect file format by extension
            if args.mapping_file.lower().endswith(('.yml', '.yaml')):
                tenant_lea_mapping = yaml.safe_load(f)
            else:
                # Default to JSON
                tenant_lea_mapping = json.load(f)
    else:  # args.mapping
        tenant_lea_mapping = json.loads(args.mapping)
    
    main(tenant_lea_mapping, quiet=args.quiet)
