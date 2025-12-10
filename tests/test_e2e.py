"""End-to-end tests against real BigQuery instance."""

import time
from datetime import date, datetime

import pytest

from pydantic_bq.settings import settings

# Skip all tests in this module if no credentials are configured
pytestmark = [
    pytest.mark.e2e,
    pytest.mark.skipif(not settings.has_credentials, reason='BigQuery credentials not configured'),
]


class TestFullTableLifecycle:
    """Test complete table lifecycle: create -> insert -> query -> delete."""

    def test_create_insert_query_delete(self, e2e_dataset, e2e_model):
        """Full CRUD lifecycle against real BigQuery."""
        # Create table
        table = e2e_dataset.table(e2e_model)
        table.create()

        try:
            # Insert a row
            instance = e2e_model(
                code='E2E001',
                name='E2E Test Item',
                count=42,
                price=123.45,
                is_active=True,
                created_at=datetime(2024, 6, 15, 10, 30, 0),
                birth_date=date(1990, 1, 15),
                description='End-to-end test',
                tags=['e2e', 'test'],
            )
            table.add_rows(instance)

            # Wait for data to be available (BigQuery streaming buffer)
            time.sleep(2)

            # Query rows
            rows = table.get_rows()
            assert len(rows) == 1
            assert rows[0].code == 'E2E001'
            assert rows[0].name == 'E2E Test Item'
            assert rows[0].count == 42
            assert rows[0].is_active is True

            # Count rows
            count = table.count_rows()
            assert count == 1

            # Query with filter
            filtered = table.get_rows(where="code = 'E2E001'")
            assert len(filtered) == 1

            # Delete rows
            table.delete_rows("code = 'E2E001'")

            # Wait for deletion to propagate
            time.sleep(2)

            # Verify deletion
            count_after = table.count_rows()
            assert count_after == 0

        finally:
            # Clean up: delete the table
            table.delete()


class TestSchemaTypesRoundtrip:
    """Test that all field types survive the BigQuery roundtrip."""

    def test_all_types_roundtrip(self, e2e_dataset, unique_table_suffix):
        """Insert model with all field types, verify they come back correctly."""
        from tests.conftest import create_e2e_model

        Model = create_e2e_model(unique_table_suffix + '_types')
        table = e2e_dataset.table(Model)
        table.create()

        try:
            test_datetime = datetime(2024, 3, 15, 14, 30, 45)
            test_date = date(1985, 7, 20)

            instance = Model(
                code='TYPES001',
                name='Type Test',
                count=999,
                price=1234.56,
                is_active=False,
                created_at=test_datetime,
                birth_date=test_date,
                description='Testing all types',
                tags=['alpha', 'beta', 'gamma'],
            )
            table.add_rows(instance)

            time.sleep(2)

            rows = table.get_rows()
            assert len(rows) == 1

            row = rows[0]
            assert row.code == 'TYPES001'
            assert row.name == 'Type Test'
            assert row.count == 999
            assert abs(row.price - 1234.56) < 0.01  # Float comparison
            assert row.is_active is False
            assert row.description == 'Testing all types'
            assert row.tags == ['alpha', 'beta', 'gamma']

            # Date/datetime may come back as different types from BQ
            # Just verify the values are correct
            assert row.birth_date.year == 1985
            assert row.birth_date.month == 7
            assert row.birth_date.day == 20

        finally:
            table.delete()


class TestBatchInsert:
    """Test inserting multiple rows."""

    def test_batch_insert(self, e2e_dataset, unique_table_suffix):
        """Insert multiple rows and verify count."""
        from tests.conftest import create_e2e_model

        Model = create_e2e_model(unique_table_suffix + '_batch')
        table = e2e_dataset.table(Model)
        table.create()

        try:
            # Create 50 test instances
            instances = [
                Model(
                    code=f'BATCH{i:03d}',
                    name=f'Batch Item {i}',
                    count=i,
                    price=float(i) * 1.5,
                    is_active=i % 2 == 0,
                    created_at=datetime.now(),
                    birth_date=date.today(),
                )
                for i in range(50)
            ]

            table.add_rows(*instances)

            time.sleep(3)

            count = table.count_rows()
            assert count == 50

            # Query with limit
            limited = table.get_rows(limit=10)
            assert len(limited) == 10

        finally:
            table.delete()


class TestQueryWithFilters:
    """Test querying with WHERE and LIMIT clauses."""

    def test_query_filters(self, e2e_dataset, unique_table_suffix):
        """Test filtering and limiting query results."""
        from tests.conftest import create_e2e_model

        Model = create_e2e_model(unique_table_suffix + '_filter')
        table = e2e_dataset.table(Model)
        table.create()

        try:
            # Insert test data
            instances = [
                Model(
                    code='ACTIVE1',
                    name='Active One',
                    count=10,
                    price=100.0,
                    is_active=True,
                    created_at=datetime.now(),
                    birth_date=date.today(),
                ),
                Model(
                    code='ACTIVE2',
                    name='Active Two',
                    count=20,
                    price=200.0,
                    is_active=True,
                    created_at=datetime.now(),
                    birth_date=date.today(),
                ),
                Model(
                    code='INACTIVE1',
                    name='Inactive One',
                    count=30,
                    price=300.0,
                    is_active=False,
                    created_at=datetime.now(),
                    birth_date=date.today(),
                ),
            ]
            table.add_rows(*instances)

            time.sleep(2)

            # Filter by is_active
            active_rows = table.get_rows(where='is_active = true')
            assert len(active_rows) == 2

            # Count with filter
            active_count = table.count_rows(where='is_active = true')
            assert active_count == 2

            inactive_count = table.count_rows(where='is_active = false')
            assert inactive_count == 1

            # Query specific fields
            fields_only = table.get_rows(fields=['code', 'name'])
            assert len(fields_only) == 3
            assert 'code' in fields_only[0]
            assert 'name' in fields_only[0]

        finally:
            table.delete()


class TestTableRecreate:
    """Test table recreation."""

    def test_recreate_table(self, e2e_dataset, unique_table_suffix):
        """Test deleting and recreating a table."""
        from tests.conftest import create_e2e_model

        Model = create_e2e_model(unique_table_suffix + '_recreate')
        table = e2e_dataset.table(Model)

        # Create and add data
        table.create()
        instance = Model(
            code='OLD001',
            name='Old Data',
            count=1,
            price=1.0,
            is_active=True,
            created_at=datetime.now(),
            birth_date=date.today(),
        )
        table.add_rows(instance)

        time.sleep(2)
        assert table.count_rows() == 1

        # Recreate the table
        table.recreate()

        time.sleep(1)

        # Table should be empty after recreation
        count = table.count_rows()
        assert count == 0

        # Clean up
        table.delete()


class TestRawQuery:
    """Test raw SQL query execution."""

    def test_raw_query(self, e2e_dataset, unique_table_suffix):
        """Test executing raw SQL queries."""
        from tests.conftest import create_e2e_model

        Model = create_e2e_model(unique_table_suffix + '_rawq')
        table = e2e_dataset.table(Model)
        table.create()

        try:
            instance = Model(
                code='RAW001',
                name='Raw Query Test',
                count=100,
                price=50.0,
                is_active=True,
                created_at=datetime.now(),
                birth_date=date.today(),
            )
            table.add_rows(instance)

            time.sleep(2)

            # Execute raw query
            table_full_name = f'{e2e_dataset.dataset_name}.{Model.Meta.table_id}'
            results = e2e_dataset.query(f'SELECT code, count FROM {table_full_name}')

            assert len(results) == 1
            assert results[0]['code'] == 'RAW001'
            assert results[0]['count'] == 100

        finally:
            table.delete()


class TestViewOperations:
    """Test view wrapper operations."""

    def test_view_get_rows(self, e2e_dataset, unique_table_suffix):
        """Test querying through view wrapper."""
        from tests.conftest import create_e2e_model

        Model = create_e2e_model(unique_table_suffix + '_view')
        table = e2e_dataset.table(Model)
        table.create()

        try:
            instance = Model(
                code='VIEW001',
                name='View Test',
                count=77,
                price=77.77,
                is_active=True,
                created_at=datetime.now(),
                birth_date=date.today(),
            )
            table.add_rows(instance)

            time.sleep(2)

            # Use view wrapper to query the same table
            view = e2e_dataset.view(Model)
            rows = view.get_rows()

            assert len(rows) == 1
            assert rows[0].code == 'VIEW001'

            count = view.count_rows()
            assert count == 1

        finally:
            table.delete()
