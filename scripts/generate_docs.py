"""
Generate markdown documentation from dbt manifest.json and catalog.json.

Output structure mirrors the dbt model path, e.g.:
  models/marts/finance/fct_active_contract.sql
  -> docs/confluence/catalog/marts/finance/fct_active_contract.md

Usage:
  python scripts/generate_docs.py                        # runs dbt docs generate --target prod, then writes md files
  python scripts/generate_docs.py --dbt-target local     # use local target instead
  python scripts/generate_docs.py --skip-generate        # skip dbt docs generate, use existing target/ files
"""

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).parent.parent


def load_env(env_path: Path):
    if not env_path.exists():
        return
    for line in env_path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith('#') or '=' not in line:
            continue
        key, _, value = line.partition('=')
        os.environ.setdefault(key.strip(), value.strip())


def run_dbt_docs_generate(dbt_target: str):
    load_env(REPO_ROOT / '.env')
    dbt_project_dir = REPO_ROOT / 'dbt_analytics'
    print(f'Running dbt docs generate --target {dbt_target}...')
    result = subprocess.run(
        [str(REPO_ROOT / '.venv' / 'bin' / 'dbt'), 'docs', 'generate', '--target', dbt_target],
        cwd=dbt_project_dir,
    )
    if result.returncode != 0:
        print('dbt docs generate failed. Aborting.')
        sys.exit(result.returncode)


def load_json(path: Path) -> dict:
    with open(path) as f:
        return json.load(f)


def clean_type(pg_type: str) -> str:
    """Shorten verbose Postgres type names."""
    mapping = {
        'character varying': 'varchar',
        'timestamp without time zone': 'timestamp',
        'timestamp with time zone': 'timestamptz',
        'double precision': 'float',
    }
    return mapping.get(pg_type, pg_type)


def model_path_to_output(model_path: str) -> Path:
    """
    Convert a manifest model path to the output .md path.
    Strips the leading 'models/' prefix if present, then replaces .sql with .md.
    e.g. 'models/marts/finance/fct_active_contract.sql'
      -> 'marts/finance/fct_active_contract.md'
    """
    p = Path(model_path)
    parts = p.parts
    # Strip leading 'models' directory if present
    if parts[0] == 'models':
        parts = parts[1:]
    return Path(*parts).with_suffix('.md')


def render_model(node: dict, catalog_node: dict | None) -> str:
    name = node['name']
    description = node.get('description', '').strip()
    config = node.get('config', {})
    materialized = config.get('materialized', 'unknown')
    schema = node.get('schema', '')
    tags = node.get('tags', [])
    is_snapshot = node.get('resource_type') == 'snapshot'

    # Upstream refs
    refs = [r['name'] for r in node.get('refs', [])]

    # Columns: merge manifest descriptions with catalog types
    manifest_cols = node.get('columns', {})
    catalog_cols = catalog_node['columns'] if catalog_node else {}

    # Build unified column list ordered by catalog index where available
    all_col_names = list(catalog_cols.keys()) or list(manifest_cols.keys())
    columns = []
    for col_name in all_col_names:
        cat = catalog_cols.get(col_name, {})
        man = manifest_cols.get(col_name, manifest_cols.get(col_name.lower(), {}))
        columns.append({
            'name': col_name,
            'type': clean_type(cat.get('type', '')),
            'description': man.get('description', '').strip(),
        })

    lines = []

    # Title
    lines.append(f'# {name}')
    lines.append('')

    if description:
        lines.append(description)
        lines.append('')

    # Metadata
    lines.append('## Details')
    lines.append('')
    lines.append('| | |')
    lines.append('|---|---|')
    lines.append(f'| **Schema** | `{schema}` |')
    lines.append(f'| **Materialization** | {materialized} |')
    if is_snapshot:
        lines.append(f'| **Strategy** | {config.get("strategy", "")} |')
        lines.append(f'| **Unique Key** | `{config.get("unique_key", "")}` |')
        if config.get('updated_at'):
            lines.append(f'| **Updated At** | `{config.get("updated_at")}` |')
    if tags:
        lines.append(f'| **Tags** | {", ".join(tags)} |')
    lines.append('')

    # Upstream dependencies
    if refs:
        lines.append('## Depends On')
        lines.append('')
        for ref in sorted(refs):
            lines.append(f'- `{ref}`')
        lines.append('')

    # Columns
    if columns:
        lines.append('## Columns')
        lines.append('')
        lines.append('| Column | Type | Description |')
        lines.append('|---|---|---|')
        for col in columns:
            desc = col['description'] or ''
            lines.append(f'| `{col["name"]}` | {col["type"]} | {desc} |')
        lines.append('')

    return '\n'.join(lines)


def main():
    parser = argparse.ArgumentParser(description='Generate dbt model docs as markdown.')
    parser.add_argument(
        '--target-dir',
        default='dbt_analytics/target',
        help='Path to dbt target/ directory containing manifest.json and catalog.json',
    )
    parser.add_argument(
        '--output-dir',
        default='docs/confluence/catalog',
        help='Root output directory for markdown files',
    )
    parser.add_argument(
        '--dbt-target',
        default='prod',
        help='dbt target to pass to dbt docs generate (default: prod)',
    )
    parser.add_argument(
        '--skip-generate',
        action='store_true',
        help='Skip running dbt docs generate and use existing target/ files',
    )
    args = parser.parse_args()

    if not args.skip_generate:
        run_dbt_docs_generate(args.dbt_target)

    target_dir = REPO_ROOT / args.target_dir
    output_dir = REPO_ROOT / args.output_dir

    manifest = load_json(target_dir / 'manifest.json')
    catalog = load_json(target_dir / 'catalog.json')

    catalog_nodes = catalog.get('nodes', {})
    manifest_nodes = manifest.get('nodes', {})

    generated = 0
    skipped = 0

    # Track per-directory entries for index generation: dir -> list of row dicts
    dir_entries: dict[Path, list[dict]] = {}

    for unique_id, node in manifest_nodes.items():
        # Only process models from this project, skip dbt_utils etc.
        if node.get('resource_type') not in ('model', 'snapshot'):
            continue
        if node.get('package_name') != manifest['metadata']['project_name']:
            continue

        model_path = node.get('original_file_path', '')
        if not model_path:
            skipped += 1
            continue

        if '_deprecated' in Path(model_path).parts:
            skipped += 1
            continue

        catalog_node = catalog_nodes.get(unique_id)

        md_content = render_model(node, catalog_node)

        relative_path = model_path_to_output(model_path)
        output_path = output_dir / relative_path
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(md_content)

        print(f'  wrote {output_path}')
        generated += 1

        # Collect entry for the directory index
        entry = {
            'name': node['name'],
            'file': output_path.name,
            'materialized': node.get('config', {}).get('materialized', ''),
            'description': node.get('description', '').strip(),
        }
        dir_entries.setdefault(output_path.parent, []).append(entry)

    # Write index.md for each directory
    indexes = 0
    for directory, entries in dir_entries.items():
        entries.sort(key=lambda e: e['name'])
        folder_name = directory.name
        lines = [
            f'# {folder_name}',
            '',
            f'| Model | Type | Description |',
            '|---|---|---|',
        ]
        for entry in entries:
            link = f'[{entry["name"]}]({entry["file"]})'
            desc = entry['description'] or ''
            lines.append(f'| {link} | {entry["materialized"]} | {desc} |')
        lines.append('')
        index_path = directory / 'index.md'
        index_path.write_text('\n'.join(lines))
        print(f'  wrote {index_path}')
        indexes += 1

    print(f'\nDone: {generated} models written, {indexes} indexes written, {skipped} skipped.')


if __name__ == '__main__':
    main()