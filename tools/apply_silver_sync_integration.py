"""Carefully patch existing Phase 4 Silver integration points.

Run from D:\\ASE-ETL after copying the files from the ZIP.
The script refuses unknown source layouts and saves .pre_7_7_2 backups.
No database connection or publication is performed.
"""

from pathlib import Path


def replace_once(source: str, before: str, after: str, description: str) -> str:
    found = source.count(before)
    if found != 1:
        raise RuntimeError(f'Cannot apply {description}: expected 1 anchor, found {found}. No files changed.')
    return source.replace(before, after, 1)


def plan_changes() -> dict[Path, str]:
    pipeline_file = Path('src/silver/pipeline.py')
    publish_file = Path('src/silver/publish.py')
    stage_module = Path('src/api/employee_sync_stage.py')

    if not stage_module.is_file():
        raise RuntimeError(f'Missing {stage_module}; extract ZIP into your project first.')
    for file in (pipeline_file, publish_file):
        if not file.is_file():
            raise RuntimeError(f'Missing {file}. Run from the ASE-ETL project root.')

    pipeline = pipeline_file.read_text(encoding='utf-8')
    publisher = publish_file.read_text(encoding='utf-8')
    if 'stage_employee_overlay' in pipeline or 'api_employee_overrides' in publisher:
        raise RuntimeError('Integration markers already exist. Review files; not reapplying.')

    pipeline = replace_once(
        pipeline,
        'LINEAGE_KEY = [',
        'from src.api.employee_sync_stage import stage_employee_overlay\n\n\nLINEAGE_KEY = [',
        'Silver overlay import',
    )

    pipeline = replace_once(
        pipeline,
        '    # ----------------------------------------------\n    # 8. Write staged outputs',
        '''    # ----------------------------------------------
    # 7B. Apply a frozen API change-ledger snapshot.
    # Bronze/source reconciliation above stays unchanged.
    # ----------------------------------------------
    employees_clean, employee_overrides_audit = stage_employee_overlay(
        employees_clean,
        expected_source_count=50,
    )
    validate_primary_key(
        employees_clean,
        ["client_employee_id"],
        "employees_with_api_overrides",
    )

    # ----------------------------------------------
    # 8. Write staged outputs''',
        'Silver stage insertion',
    )
    pipeline = replace_once(
        pipeline,
        '            "employees": employees_clean,',
        '            "employees": employees_clean,\n            "audit/api_employee_overrides": employee_overrides_audit,',
        'Silver audit stage output',
    )
    publish_mark = '    "audit/schema_evolution",\n]'
    publisher = replace_once(
        publisher,
        publish_mark,
        '    "audit/schema_evolution",\n    "audit/api_employee_overrides",\n]',
        'Silver publication dataset registration',
    )

    start = publisher.find('def verify_local():\n')
    end = publisher.find('\ndef upload_and_verify(', start)
    if start == -1 or end == -1 or end <= start:
        raise RuntimeError('Cannot find the expected verify_local function. No files changed.')
    previous_verify = publisher[start:end]
    required_fragments = (
        'for name in DATASETS:',
        'counts[name] = sum(',
        'expected = EXPECTED_COUNTS[name]',
        'return counts',
    )
    if not all(fragment in previous_verify for fragment in required_fragments):
        raise RuntimeError('Unexpected verify_local implementation. No files changed.')

    new_verify = '''def verify_local():
    """Verify the original Silver contracts plus a pinned API profile delta."""
    from src.api.employee_sync_stage import (
        AUDIT_DATASET,
        validate_employee_override_snapshot,
    )

    counts = {}
    for name in DATASETS:
        files = dataset_files(name)
        counts[name] = sum(pq.read_metadata(file).num_rows for file in files)
        expected = EXPECTED_COUNTS.get(name)
        # API-modified employee count and its audit count are dynamic.
        # All original Bronze-derived datasets keep their exact checks.
        if name not in ('employees', AUDIT_DATASET):
            if expected is None or counts[name] != expected:
                raise ValueError(
                    f'{name}: expected {expected}, found {counts[name]}'
                )

    employees = pd.concat(
        [pd.read_parquet(file) for file in dataset_files('employees')],
        ignore_index=True,
    )
    override_audit = pd.concat(
        [pd.read_parquet(file) for file in dataset_files(AUDIT_DATASET)],
        ignore_index=True,
    )
    expected_employees = validate_employee_override_snapshot(
        employees,
        override_audit,
        source_employee_count=EXPECTED_COUNTS['employees'],
    )
    if counts['employees'] != expected_employees:
        raise ValueError('Parquet employee count differs from API overlay reconciliation')
    if counts[AUDIT_DATASET] != len(override_audit):
        raise ValueError('Parquet API audit count mismatch')

    return counts

'''
    publisher = publisher[:start] + new_verify + publisher[end:]
    return {pipeline_file: pipeline, publish_file: publisher}


def main():
    changes = plan_changes()  # Validate both files before touching either.
    for file, new_text in changes.items():
        backup = file.with_name(file.name + '.pre_7_7_2')
        if backup.exists():
            raise RuntimeError(f'Backup already exists: {backup}. No changes made.')
    for file, new_text in changes.items():
        backup = file.with_name(file.name + '.pre_7_7_2')
        backup.write_bytes(file.read_bytes())
        file.write_text(new_text, encoding='utf-8')
        print(f'PATCHED: {file} (backup: {backup})')
    print('PASS: Silver stage and publisher integration code patched.')
    print('NO DATABASE/MINIO PUBLICATION HAS RUN.')


if __name__ == '__main__':
    main()
