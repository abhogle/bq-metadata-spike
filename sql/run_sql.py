#!/usr/bin/env python3
"""
SQL Runner for BigQuery
Executes SQL files with multiple statements against BigQuery.

Usage: python sql/run_sql.py sql/02-raw-layer.sql
"""

import os
import sys
import re
from google.cloud import bigquery


def split_statements(sql_content: str) -> list[str]:
    """
    Split SQL content into individual statements.
    Handles semicolons inside string literals.
    """
    statements = []
    current = []
    in_single_quote = False
    in_double_quote = False
    i = 0

    while i < len(sql_content):
        char = sql_content[i]

        # Handle escape sequences
        if char == '\\' and i + 1 < len(sql_content):
            current.append(char)
            current.append(sql_content[i + 1])
            i += 2
            continue

        # Track string literals
        if char == "'" and not in_double_quote:
            in_single_quote = not in_single_quote
        elif char == '"' and not in_single_quote:
            in_double_quote = not in_double_quote

        # Split on semicolon only when not in a string
        if char == ';' and not in_single_quote and not in_double_quote:
            stmt = ''.join(current).strip()
            if stmt:
                statements.append(stmt)
            current = []
        else:
            current.append(char)

        i += 1

    # Don't forget the last statement (may not end with semicolon)
    stmt = ''.join(current).strip()
    if stmt:
        statements.append(stmt)

    return statements


def clean_statement(stmt: str) -> str:
    """Remove comment-only lines and clean up whitespace."""
    lines = []
    for line in stmt.split('\n'):
        stripped = line.strip()
        # Skip empty lines and comment-only lines
        if stripped and not stripped.startswith('--'):
            lines.append(line)
    return '\n'.join(lines).strip()


def run_sql_file(filepath: str, project_id: str):
    """Run all statements in a SQL file."""
    print(f"\n{'='*60}")
    print(f"Running: {filepath}")
    print(f"Project: {project_id}")
    print('='*60 + '\n')

    # Read the SQL file
    with open(filepath, 'r') as f:
        sql_content = f.read()

    # Split into statements
    raw_statements = split_statements(sql_content)

    # Clean statements (remove comment-only ones)
    statements = []
    for stmt in raw_statements:
        cleaned = clean_statement(stmt)
        if cleaned:
            statements.append(cleaned)

    print(f"Found {len(statements)} statement(s) to execute\n")

    # Initialize BigQuery client
    client = bigquery.Client(project=project_id)

    # Execute each statement
    for i, stmt in enumerate(statements, 1):
        # Get first 80 chars for display (single line)
        preview = ' '.join(stmt.split())[:80]
        if len(stmt) > 80:
            preview += '...'

        print(f"[{i}/{len(statements)}] {preview}")

        try:
            query_job = client.query(stmt)
            query_job.result()  # Wait for completion
            print(f"         ✅ Success\n")
        except Exception as e:
            print(f"         ❌ FAILED\n")
            print(f"Error: {e}\n")
            print("Full statement that failed:")
            print("-" * 40)
            print(stmt)
            print("-" * 40)
            sys.exit(1)

    print(f"\n✅ All {len(statements)} statements completed successfully!")


def main():
    if len(sys.argv) != 2:
        print("Usage: python sql/run_sql.py <sql_file>")
        print("Example: python sql/run_sql.py sql/02-raw-layer.sql")
        sys.exit(1)

    filepath = sys.argv[1]

    if not os.path.exists(filepath):
        print(f"Error: File not found: {filepath}")
        sys.exit(1)

    project_id = os.environ.get('GCP_PROJECT_ID', 'bq-metadata-spike')

    if not os.environ.get('GOOGLE_APPLICATION_CREDENTIALS'):
        print("Warning: GOOGLE_APPLICATION_CREDENTIALS not set")
        print("Attempting to use default credentials...")

    run_sql_file(filepath, project_id)


if __name__ == '__main__':
    main()
