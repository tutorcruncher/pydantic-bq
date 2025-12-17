"""End-to-end tests for BigQuery client operations."""

from unittest.mock import MagicMock, patch

import pytest
from google.api_core.exceptions import Forbidden, NotFound

from pydantic_bq.client import BQTable, BQView, DatasetClient, create_client, job_result


class TestCreateClient:
    """Tests for create_client function."""

    def test_create_client_success(self, mock_bq_client, mock_settings_with_creds):
        """Successfully creates a BigQuery client with valid credentials."""
        client = create_client()

        assert client is not None
        assert client.project == 'test-project'

    def test_create_client_no_credentials(self, mock_settings_no_creds):
        """Raises RuntimeError when no credentials are configured."""
        with pytest.raises(RuntimeError, match='No BigQuery credentials found'):
            create_client()


class TestJobResult:
    """Tests for job_result retry logic."""

    def test_job_result_success(self):
        """Returns result on successful job completion."""
        mock_job = MagicMock()
        mock_job.result.return_value = [{'id': 1}]

        result = job_result(mock_job)

        assert result == [{'id': 1}]
        mock_job.result.assert_called_once()

    def test_job_result_rate_limit_retry(self):
        """Retries on rate limit exceeded, then succeeds."""
        mock_job = MagicMock()
        mock_job.result.side_effect = [
            Forbidden('rateLimitExceeded'),
            [{'id': 1}],
        ]

        with patch('pydantic_bq.client.time.sleep'):
            job_result(mock_job)

        # First call fails, recursion succeeds
        assert mock_job.result.call_count == 2

    def test_job_result_rate_limit_exhausted(self):
        """Raises after max retries on persistent rate limit."""
        mock_job = MagicMock()
        mock_job.result.side_effect = Forbidden('rateLimitExceeded')

        with patch('pydantic_bq.client.time.sleep'):
            with pytest.raises(Forbidden):
                job_result(mock_job, retry=6)

    def test_job_result_other_exception(self):
        """Raises and logs on non-rate-limit exceptions."""
        mock_job = MagicMock()
        mock_job.result.side_effect = ValueError('Some error')
        mock_job.errors = [{'message': 'error1'}]

        with pytest.raises(ValueError):
            job_result(mock_job)

    def test_job_result_forbidden_non_rate_limit(self):
        """Non-rate-limit Forbidden errors are silently ignored (returns None)."""
        mock_job = MagicMock()
        mock_job.result.side_effect = Forbidden('quotaExceeded')

        result = job_result(mock_job)

        # Current behavior: non-rate-limit Forbidden returns None
        assert result is None


class TestDatasetClient:
    """Tests for DatasetClient initialization and methods."""

    def test_init_with_dataset_name(self, mock_bq_client, mock_settings_with_creds):
        """Initializes with explicit dataset name."""
        client = DatasetClient('my_dataset')

        assert client.dataset_name == 'my_dataset'
        assert client.dataset_ref.dataset_id == 'my_dataset'

    def test_query(self, dataset_client):
        """Executes raw SQL query and returns results."""
        mock_result = [MagicMock(), MagicMock()]
        mock_result[0].__iter__ = lambda self: iter([('id', 1), ('name', 'test')])
        mock_result[1].__iter__ = lambda self: iter([('id', 2), ('name', 'test2')])

        dataset_client._client.query.return_value.result.return_value = mock_result

        results = dataset_client.query('SELECT * FROM table')

        dataset_client._client.query.assert_called_with('SELECT * FROM table')
        assert len(results) == 2

    def test_table_returns_bq_table(self, dataset_client, sample_model):
        """table() returns a BQTable instance."""
        table = dataset_client.table(sample_model)

        assert isinstance(table, BQTable)
        assert table.model is sample_model

    def test_view_returns_bq_view(self, dataset_client, sample_model):
        """view() returns a BQView instance."""
        view = dataset_client.view(sample_model)

        assert isinstance(view, BQView)
        assert view.model is sample_model

    def test_create_table(self, dataset_client, sample_model):
        """create_table creates table and returns BQTable."""
        table = dataset_client.create_table(sample_model)

        assert isinstance(table, BQTable)
        dataset_client._client.create_table.assert_called_once()

    def test_delete_table(self, dataset_client, sample_model):
        """delete_table deletes the table."""
        dataset_client.delete_table(sample_model)

        dataset_client._client.delete_table.assert_called_once()

    def test_recreate_table(self, dataset_client, sample_model):
        """recreate_table deletes and recreates table."""
        table = dataset_client.recreate_table(sample_model)

        assert isinstance(table, BQTable)
        dataset_client._client.delete_table.assert_called_once()
        dataset_client._client.create_table.assert_called_once()

    def test_add_rows(self, dataset_client, sample_instance):
        """add_rows delegates to BQTable.add_rows."""
        mock_job = MagicMock()
        mock_job.result.return_value = None
        dataset_client._client.load_table_from_file.return_value = mock_job

        result = dataset_client.add_rows(sample_instance)

        assert result == []
        dataset_client._client.load_table_from_file.assert_called_once()


class TestBQTableCRUDLifecycle:
    """End-to-end test of full table CRUD lifecycle."""

    def test_full_crud_lifecycle(self, dataset_client, sample_model, sample_instance):
        """
        Complete lifecycle: create -> add_rows -> get_rows -> count -> delete_rows -> delete
        """
        table = dataset_client.table(sample_model)

        # Create table
        table.create()
        dataset_client._client.create_table.assert_called_once()
        created_table = dataset_client._client.create_table.call_args[0][0]
        assert created_table.description == 'A sample table for testing'

        # Add rows (file upload path)
        mock_job = MagicMock()
        mock_job.result.return_value = None
        dataset_client._client.load_table_from_file.return_value = mock_job

        table.add_rows(sample_instance)
        dataset_client._client.load_table_from_file.assert_called_once()

        # Get rows - mock query result using a dict-like object
        row_data = {
            'code': 'TEST001',
            'name': 'Test Item',
            'count': 10,
            'price': 99.99,
            'is_active': True,
            'created_at': sample_instance.created_at,
            'birth_date': sample_instance.birth_date,
            'description': 'A test item',
            'tags': ['tag1', 'tag2'],
        }
        dataset_client._client.query.return_value.result.return_value = [row_data]

        rows = table.get_rows()
        assert len(rows) == 1
        assert rows[0].code == 'TEST001'

        # Count rows
        dataset_client._client.query.return_value.result.return_value = iter([{'f0_': 1}])

        count = table.count_rows()
        assert count == 1

        # Delete specific rows
        table.delete_rows("code = 'TEST001'")
        # Verify DELETE query was executed
        calls = dataset_client._client.query.call_args_list
        delete_call = [c for c in calls if 'DELETE' in str(c)]
        assert len(delete_call) > 0

        # Delete table
        table.delete()
        dataset_client._client.delete_table.assert_called()


class TestBQTableCreate:
    """Tests for BQTable.create method."""

    def test_create_with_schema(self, dataset_client, sample_model):
        """Creates table with correct schema from model."""
        table = dataset_client.table(sample_model)
        table.create()

        call_args = dataset_client._client.create_table.call_args[0][0]
        schema_fields = {f.name for f in call_args.schema}

        assert 'code' in schema_fields
        assert 'name' in schema_fields
        assert 'count' in schema_fields
        assert 'created_at' in schema_fields

    def test_table_id_property(self, dataset_client, sample_model):
        """_table_id returns model's Meta.table_id."""
        table = dataset_client.table(sample_model)
        assert table._table_id == 'sample_table'

    def test_table_description_property(self, dataset_client, sample_model, minimal_model):
        """_table_description returns model's Meta.table_description."""
        table = dataset_client.table(sample_model)
        assert table._table_description == 'A sample table for testing'

        minimal_table = dataset_client.table(minimal_model)
        assert minimal_table._table_description == ''


class TestBQTableRecreate:
    """Tests for BQTable.recreate method."""

    def test_recreate_when_exists(self, dataset_client, sample_model):
        """Deletes existing table then creates new one."""
        table = dataset_client.table(sample_model)
        table.recreate()

        dataset_client._client.delete_table.assert_called_once()
        dataset_client._client.create_table.assert_called_once()

    def test_recreate_when_not_found(self, dataset_client, sample_model):
        """Creates table when it doesn't exist (NotFound on delete)."""
        dataset_client._client.delete_table.side_effect = NotFound('Table not found')

        table = dataset_client.table(sample_model)
        table.recreate()

        dataset_client._client.delete_table.assert_called_once()
        dataset_client._client.create_table.assert_called_once()


class TestBQTableAddRows:
    """Tests for BQTable.add_rows method."""

    def test_add_rows_empty(self, dataset_client, sample_model):
        """Returns empty list when no objects provided."""
        table = dataset_client.table(sample_model)
        result = table.add_rows()

        assert result == []
        dataset_client._client.load_table_from_file.assert_not_called()

    def test_add_rows_as_file(self, dataset_client, sample_model, sample_instance):
        """Uploads rows as NDJSON file (default behavior)."""
        mock_job = MagicMock()
        mock_job.result.return_value = None
        dataset_client._client.load_table_from_file.return_value = mock_job

        table = dataset_client.table(sample_model)
        result = table.add_rows(sample_instance)

        assert result == []
        dataset_client._client.load_table_from_file.assert_called_once()

        # Verify job config was passed
        call_args = dataset_client._client.load_table_from_file.call_args
        assert call_args is not None

    def test_add_rows_streaming(self, dataset_client, sample_instance):
        """Uses streaming insert when send_as_file=False."""
        from tests.conftest import MinimalModel

        instance = MinimalModel(code='A', value=1)
        table = dataset_client.table(MinimalModel)
        table.add_rows(instance, send_as_file=False)

        dataset_client._client.insert_rows_json.assert_called()

    def test_add_rows_streaming_not_found_retry(self, dataset_client):
        """Retries with smaller batches on NotFound."""
        from tests.conftest import MinimalModel

        # First call raises NotFound, subsequent calls succeed
        dataset_client._client.insert_rows_json.side_effect = [
            NotFound('Table not found'),
            None,
        ]

        instance = MinimalModel(code='A', value=1)
        table = dataset_client.table(MinimalModel)
        table.add_rows(instance, send_as_file=False)

        # Should have retried with smaller batch
        assert dataset_client._client.insert_rows_json.call_count == 2

    def test_add_rows_streaming_error(self, dataset_client):
        """Raises RuntimeError on persistent streaming errors."""
        from tests.conftest import MinimalModel

        # First NotFound triggers retry, then another error
        dataset_client._client.insert_rows_json.side_effect = [
            NotFound('Table not found'),
            ValueError('Persistent error'),
        ]

        instance = MinimalModel(code='A', value=1)
        table = dataset_client.table(MinimalModel)

        with pytest.raises(RuntimeError, match='Problem with batch'):
            table.add_rows(instance, send_as_file=False)


class TestBQTableGetRows:
    """Tests for BQTable.get_rows method."""

    def test_get_rows_as_objects(self, dataset_client, sample_model):
        """Returns Pydantic model instances by default."""
        from datetime import date, datetime

        row_data = {
            'code': 'TEST001',
            'name': 'Test',
            'count': 5,
            'price': 10.0,
            'is_active': True,
            'created_at': datetime(2024, 1, 1),
            'birth_date': date(1990, 1, 1),
            'description': None,
            'tags': [],
        }
        dataset_client._client.query.return_value.result.return_value = [row_data]

        table = dataset_client.table(sample_model)
        rows = table.get_rows()

        assert len(rows) == 1
        assert rows[0].code == 'TEST001'
        assert isinstance(rows[0], sample_model)

    def test_get_rows_as_dicts(self, dataset_client, sample_model):
        """Returns dicts when as_objects=False."""
        row_data = {'code': 'TEST001'}
        dataset_client._client.query.return_value.result.return_value = [row_data]

        table = dataset_client.table(sample_model)
        rows = table.get_rows(as_objects=False)

        assert len(rows) == 1
        assert isinstance(rows[0], dict)
        assert rows[0]['code'] == 'TEST001'

    def test_get_rows_with_fields(self, dataset_client, sample_model):
        """Returns dicts when specific fields requested."""
        row_data = {'code': 'TEST001', 'name': 'Test'}
        dataset_client._client.query.return_value.result.return_value = [row_data]

        table = dataset_client.table(sample_model)
        rows = table.get_rows(fields=['code', 'name'])

        # With fields specified, returns dicts even if as_objects=True
        assert isinstance(rows[0], dict)

        # Verify query contains only requested fields
        query_call = dataset_client._client.query.call_args[0][0]
        assert 'code,name' in query_call

    def test_get_rows_with_where(self, dataset_client, sample_model):
        """Includes WHERE clause in query."""
        dataset_client._client.query.return_value.result.return_value = []

        table = dataset_client.table(sample_model)
        table.get_rows(where="code = 'TEST001'")

        query_call = dataset_client._client.query.call_args[0][0]
        assert "WHERE code = 'TEST001'" in query_call

    def test_get_rows_with_limit(self, dataset_client, sample_model):
        """Includes LIMIT clause in query."""
        dataset_client._client.query.return_value.result.return_value = []

        table = dataset_client.table(sample_model)
        table.get_rows(limit=10)

        query_call = dataset_client._client.query.call_args[0][0]
        assert 'LIMIT 10' in query_call

    def test_get_rows_with_order_by(self, dataset_client, sample_model):
        """Includes ORDER BY clause in query."""
        dataset_client._client.query.return_value.result.return_value = []

        table = dataset_client.table(sample_model)
        table.get_rows(order_by='created_at DESC')

        query_call = dataset_client._client.query.call_args[0][0]
        assert 'ORDER BY created_at DESC' in query_call

    def test_get_rows_with_order_by_multiple_columns(self, dataset_client, sample_model):
        """Includes ORDER BY clause with multiple columns."""
        dataset_client._client.query.return_value.result.return_value = []

        table = dataset_client.table(sample_model)
        table.get_rows(order_by='is_active DESC, created_at ASC')

        query_call = dataset_client._client.query.call_args[0][0]
        assert 'ORDER BY is_active DESC, created_at ASC' in query_call


class TestBQTableCountRows:
    """Tests for BQTable.count_rows method."""

    def test_count_rows(self, dataset_client, sample_model):
        """Returns row count."""
        dataset_client._client.query.return_value.result.return_value = iter([{'f0_': 42}])

        table = dataset_client.table(sample_model)
        count = table.count_rows()

        assert count == 42

    def test_count_rows_with_where(self, dataset_client, sample_model):
        """Includes WHERE clause in count query."""
        dataset_client._client.query.return_value.result.return_value = iter([{'f0_': 5}])

        table = dataset_client.table(sample_model)
        table.count_rows(where='is_active = true')

        query_call = dataset_client._client.query.call_args[0][0]
        assert 'WHERE is_active = true' in query_call


class TestBQTableDeleteRows:
    """Tests for BQTable.delete_rows method."""

    def test_delete_rows(self, dataset_client, sample_model):
        """Executes DELETE query with WHERE clause."""
        table = dataset_client.table(sample_model)
        table.delete_rows("code = 'TEST001'")

        query_call = dataset_client._client.query.call_args[0][0]
        assert 'DELETE FROM' in query_call
        assert "WHERE code = 'TEST001'" in query_call


class TestBQTableGetSchema:
    """Tests for BQTable.get_schema method."""

    def test_get_schema(self, dataset_client, sample_model):
        """get_schema returns the table schema."""
        from google.cloud.bigquery import SchemaField

        mock_schema = [SchemaField('code', 'STRING'), SchemaField('name', 'STRING')]
        dataset_client._client.get_table.return_value.schema = mock_schema

        table = dataset_client.table(sample_model)
        schema = table.get_schema()

        assert len(schema) == 2
        assert schema[0].name == 'code'


class TestBQView:
    """Tests for BQView operations."""

    def test_view_get_rows(self, dataset_client, sample_model):
        """View can fetch rows."""
        row_data = {'code': 'V001'}
        dataset_client._client.query.return_value.result.return_value = [row_data]

        view = dataset_client.view(sample_model)
        rows = view.get_rows(as_objects=False)

        assert len(rows) == 1
        assert rows[0]['code'] == 'V001'

    def test_view_count_rows(self, dataset_client, sample_model):
        """View can count rows."""
        dataset_client._client.query.return_value.result.return_value = iter([{'f0_': 100}])

        view = dataset_client.view(sample_model)
        count = view.count_rows()

        assert count == 100

    def test_view_create(self, dataset_client, sample_model):
        """View can be created with SQL."""
        view = dataset_client.view(sample_model)
        view.create('SELECT * FROM other_table')

        dataset_client._client.create_table.assert_called_once()
        created_view = dataset_client._client.create_table.call_args[0][0]
        assert created_view.view_query == 'SELECT * FROM other_table'
        assert created_view.description == 'A sample table for testing'

    def test_view_query_property(self, dataset_client, sample_model):
        """view_query returns the SQL of the view."""
        dataset_client._client.get_table.return_value.view_query = 'SELECT * FROM source'

        view = dataset_client.view(sample_model)
        assert view.view_query == 'SELECT * FROM source'


class TestQueryBuilders:
    """Tests for query building methods."""

    def test_select_query_all_fields(self, dataset_client, sample_model):
        """Builds SELECT * query."""
        table = dataset_client.table(sample_model)
        query = table._select_query()

        assert query == 'SELECT * FROM test_dataset.sample_table'

    def test_select_query_with_fields(self, dataset_client, sample_model):
        """Builds SELECT with specific fields."""
        table = dataset_client.table(sample_model)
        query = table._select_query(fields=['code', 'name'])

        assert query == 'SELECT code,name FROM test_dataset.sample_table'

    def test_select_query_with_where(self, dataset_client, sample_model):
        """Builds SELECT with WHERE clause."""
        table = dataset_client.table(sample_model)
        query = table._select_query(where="code = 'X'")

        assert "WHERE code = 'X'" in query

    def test_select_query_with_limit(self, dataset_client, sample_model):
        """Builds SELECT with LIMIT clause."""
        table = dataset_client.table(sample_model)
        query = table._select_query(limit=100)

        assert 'LIMIT 100' in query

    def test_select_query_with_order_by(self, dataset_client, sample_model):
        """Builds SELECT with ORDER BY clause."""
        table = dataset_client.table(sample_model)
        query = table._select_query(order_by='created_at DESC')

        assert query == 'SELECT * FROM test_dataset.sample_table ORDER BY created_at DESC'

    def test_select_query_with_all_clauses(self, dataset_client, sample_model):
        """Builds SELECT with WHERE, ORDER BY, and LIMIT clauses in correct order."""
        table = dataset_client.table(sample_model)
        query = table._select_query(
            fields=['code', 'name'],
            where='is_active = true',
            order_by='created_at DESC',
            limit=50,
        )

        expected = (
            'SELECT code,name FROM test_dataset.sample_table WHERE is_active = true ORDER BY created_at DESC LIMIT 50'
        )
        assert query == expected

    def test_count_query(self, dataset_client, sample_model):
        """Builds COUNT query."""
        table = dataset_client.table(sample_model)
        query = table._count_query()

        assert query == 'SELECT COUNT(*) FROM test_dataset.sample_table'

    def test_count_query_with_where(self, dataset_client, sample_model):
        """Builds COUNT query with WHERE."""
        table = dataset_client.table(sample_model)
        query = table._count_query(where='active = true')

        assert 'WHERE active = true' in query

    def test_delete_query(self, dataset_client, sample_model):
        """Builds DELETE query."""
        table = dataset_client.table(sample_model)
        query = table._delete_query(where='id = 1')

        assert 'DELETE FROM test_dataset.sample_table' in query
        assert 'WHERE id = 1' in query

    def test_delete_query_no_where(self, dataset_client, sample_model):
        """Builds DELETE query without WHERE clause."""
        table = dataset_client.table(sample_model)
        query = table._delete_query()

        assert query == 'DELETE FROM test_dataset.sample_table'
