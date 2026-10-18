from pathlib import Path
import shutil

root = Path.cwd()

def change(path, old, new):
    target = root / path
    if not target.exists():
        raise RuntimeError(f'Missing: {target}')
    src = target.read_text(encoding='utf-8')
    if new in src:
        print('Already patched:', path)
        return
    if src.count(old) != 1:
        raise RuntimeError(f'Expected exactly one match in {path}: {old[:70]!r}')
    backup = target.with_suffix(target.suffix + '.phase8backup')
    if not backup.exists():
        shutil.copy2(target, backup)
    target.write_text(src.replace(old, new, 1), encoding='utf-8')
    print('Patched:', path)

change('src/silver/pipeline.py', 'expected_source_count=50,', 'expected_source_count=len(employees_clean),')

change('src/silver/publish.py', '''        expected = EXPECTED_COUNTS.get(name)
        # API-modified employee count and its audit count are dynamic.
        # All original Bronze-derived datasets keep their exact checks.
        if name not in ('employees', AUDIT_DATASET):
            if expected is None or counts[name] != expected:
                raise ValueError(
                    f'{name}: expected {expected}, found {counts[name]}'
                )
''', '''        # Actual Bronze Parquet and audit counts are reconciled below.
''')

change('src/silver/publish.py', "source_employee_count=EXPECTED_COUNTS['employees'],", 'source_employee_count=source_employees,')

change('src/silver/publish.py', '    expected_employees = validate_employee_override_snapshot(', '''    # Reconcile exact Bronze Parquet parts to their schema-audit records.
    from src.silver.config import BRONZE_ROOT
    schema_audit = pd.concat(
        [pd.read_parquet(p) for p in dataset_files('audit/schema_evolution')],
        ignore_index=True,
    )
    seen = set()
    source_employees = 0
    source_timesheets = 0
    parts = sorted(BRONZE_ROOT.glob('*/*.parquet'))
    if not parts:
        raise ValueError('No Bronze Parquet inputs')
    for part in parts:
        name = part.relative_to(BRONZE_ROOT).as_posix()
        matches = schema_audit.loc[schema_audit['source_name'] == name]
        if len(matches) != 1:
            raise ValueError(f'Missing or duplicate schema audit for {name}')
        rows = pq.read_metadata(part).num_rows
        if int(matches.iloc[0]['source_rows']) != rows:
            raise ValueError(f'Schema audit row mismatch for {name}')
        seen.add(name)
        if part.parent.name.startswith('employee_'):
            source_employees += rows
        elif part.parent.name.startswith('timesheet_'):
            source_timesheets += rows
        else:
            raise ValueError(f'Unknown Bronze source type: {name}')
    if len(seen) != len(schema_audit):
        raise ValueError('Schema audit contains orphan records')
    if counts['audit/schema_evolution'] != len(schema_audit):
        raise ValueError('Schema audit Parquet count mismatch')
    if (counts['timesheets'] + counts['audit/rejected_timesheets']
            + counts['audit/duplicate_records'] != source_timesheets):
        raise ValueError('Silver timesheet reconciliation failed')
    source_employees -= counts['audit/rejected_employees']
    if source_employees < 0:
        raise ValueError('Employee rejects exceed Bronze source records')

    expected_employees = validate_employee_override_snapshot(''')

change('src/silver/publish_idempotent.py', '''    if int(schema_audit["source_rows"].sum()) != (
        50 + 412571
    ):
        raise ValueError(
            "Schema audit does not reconcile with "
            "the current Bronze snapshot"
        )
''', '''    # verify_local() already checks all schema audit rows against
    # the corresponding Bronze Parquet parts dynamically.
    if (schema_audit["source_rows"] < 0).any():
        raise ValueError("Negative source rows in schema audit")
''')

change('src/orchestration/tasks.py', 'output_text = log_path.read_text(encoding="utf-8")', 'output_text = log_path.read_text(encoding="utf-8", errors="replace")') if 'output_text = log_path.read_text(encoding="utf-8")' in (root/'src/orchestration/tasks.py').read_text(encoding='utf-8') else print('Log decoding fix already present')

change('src/orchestration/pipeline.py', '''    result = subprocess.run(
        command,
        cwd=PROJECT_ROOT,
        check=False,
    )''', '''    # Coordinate runs launched through this entry point. PostgreSQL
    # session advisory lock is held for the entire Luigi subprocess.
    from sqlalchemy import text
    from src.silver.publish import ENGINE

    with ENGINE.connect() as lock_conn:
        locked = lock_conn.execute(
            text("SELECT pg_try_advisory_lock(7182499)")
        ).scalar_one()
        lock_conn.commit()
        if not locked:
            raise RuntimeError("An ASE-ETL workflow is already running")
        try:
            result = subprocess.run(
                command,
                cwd=PROJECT_ROOT,
                check=False,
            )
        finally:
            lock_conn.execute(
                text("SELECT pg_advisory_unlock(7182499)")
            )
            lock_conn.commit()''')

print('Completed guarded phase 8 patch.')
