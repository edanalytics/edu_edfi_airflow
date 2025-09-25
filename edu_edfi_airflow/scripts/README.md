# EdFi Connection Validation Script

Simple script to validate EdFi connections against tenant LEA ID mappings.

## Quick Start

```bash
# Create a mapping file (YAML format)
cat > tenant_mapping.yaml << EOF
aiken: 201
allendale: 301
anderson1: 401
EOF

# Run validation
python validate_edfi_connections.py --mapping-file tenant_mapping.yaml
```

## Usage

### Basic validation
```bash
python validate_edfi_connections.py --mapping-file tenant_mapping.yaml
```

### Quiet mode (summary only)
```bash
python validate_edfi_connections.py --mapping-file tenant_mapping.yaml --quiet
```

### Inline mapping
```bash
python validate_edfi_connections.py --mapping '{"aiken": "201", "allendale": "301"}'
```

## Mapping File Format

YAML format (recommended):
```yaml
aiken: 201
allendale: 301
anderson1: 401
```

JSON format (also supported):
```json
{
  "aiken": "201",
  "allendale": "301", 
  "anderson1": "401"
}
```

## What It Does

1. Finds all Airflow connections matching `edfi_{tenant}_{year}` pattern
2. Gets the LEA ID from each connection using EdFi API
3. Compares against your mapping
4. Reports matches, mismatches, and errors

## Output

```
Found 3 matching connections
MATCH: edfi_aiken_2024 - LEA ID matches: 201
MISMATCH: edfi_allendale_2024 - Expected 301, got 999
ERROR: edfi_unknown_2024 - No LEA ID mapping for tenant: unknown

Summary: 1/3 matches, 1 mismatches, 1 no mapping, 0 no org ID, 1 errors
```

## Requirements

- Airflow environment with EdFi connections
- `edfi_api_client` package installed
- Network access to EdFi ODS instances
