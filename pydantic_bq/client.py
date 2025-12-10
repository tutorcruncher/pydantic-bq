"""BigQuery client with Pydantic model support."""

import base64
import json
import os
import time
from dataclasses import dataclass
from io import BytesIO
from operator import itemgetter
from typing import Any, Type

from google.api_core.exceptions import Forbidden, NotFound
from google.cloud import bigquery
from google.cloud.bigquery import DatasetReference, LoadJob, TableReference
from google.oauth2 import service_account

from .schema import BQBaseModel
from .settings import Settings, settings as default_settings
from .types import logger


def job_result(job, retry=1):
    """Wait for BigQuery job result with retry on rate limit."""
    try:
        result = job.result()
        return result
    except Forbidden as e:
        if 'rateLimitExceeded' in str(e):
            if retry > 5:
                logger.error('Rate limit exceeded, tried 5 times, giving up')
                raise
            else:
                logger.info('Rate limit exceeded, retrying job after %d seconds...', retry * 2)
                time.sleep(retry * 2)
                retry += 1
                job_result(job, retry=retry)
    except Exception as exc:
        logger.error('Error details: %s:%s', exc, '\n'.join(str(e) for e in (job.errors or [])))
        raise


def create_client(
    credentials_base64: str = None,
    credentials_json: str = None,
    credentials_file: str = None,
    credentials_dict: dict = None,
    settings: Settings = None,
) -> bigquery.Client:
    """
    Create a BigQuery client using credentials from various sources.
    
    Priority order:
    1. credentials_base64 - Base64 encoded service account JSON
    2. credentials_json - Raw service account JSON string
    3. credentials_file - Path to service account JSON file
    4. credentials_dict - Dict with service account credentials
    5. settings - Settings object with Google credentials
    6. Default settings from environment/.env file
    
    Returns:
        bigquery.Client: Authenticated BigQuery client
    
    Raises:
        RuntimeError: If no valid credentials are found
    """
    raw_creds = None
    
    # Option 1: Base64 encoded credentials passed directly
    if credentials_base64:
        raw_creds = json.loads(base64.urlsafe_b64decode(credentials_base64.encode()).decode())
    
    # Option 2: Raw JSON string passed directly
    elif credentials_json:
        raw_creds = json.loads(credentials_json)
    
    # Option 3: File path passed directly
    elif credentials_file:
        with open(credentials_file) as f:
            raw_creds = json.load(f)
    
    # Option 4: Dict passed directly
    elif credentials_dict:
        raw_creds = credentials_dict
    
    # Option 5: Settings object passed
    elif settings and settings.has_credentials:
        if settings.google_application_credentials:
            with open(settings.google_application_credentials) as f:
                raw_creds = json.load(f)
        else:
            raw_creds = settings.google_credentials
    
    # Option 6: Default settings from env/.env
    elif default_settings.has_credentials:
        if default_settings.google_application_credentials:
            with open(default_settings.google_application_credentials) as f:
                raw_creds = json.load(f)
        else:
            raw_creds = default_settings.google_credentials
    
    if raw_creds:
        credentials = service_account.Credentials.from_service_account_info(raw_creds)
        return bigquery.Client(project=raw_creds['project_id'], credentials=credentials)
    else:
        raise RuntimeError(
            'No BigQuery credentials found. Provide credentials via:\n'
            '  - credentials_base64, credentials_json, credentials_file, or credentials_dict parameter\n'
            '  - Settings object with g_project_id, g_private_key, g_client_email\n'
            '  - .env file with G_PROJECT_ID, G_PRIVATE_KEY, G_CLIENT_EMAIL'
        )


@dataclass
class _BQTableViewBase:
    """Base class for BigQuery table and view operations."""
    
    dataset_client: 'DatasetClient'
    model: Type[BQBaseModel]

    @property
    def _bq_client(self) -> bigquery.Client:
        return self.dataset_client._client

    @property
    def _bq_table_ref(self) -> TableReference:
        return self.dataset_client.dataset_ref.table(self._table_id)

    @classmethod
    def _gen_table_id(cls, table_id):
        # Used to mock tests
        return table_id

    @property
    def _table_id(self) -> str:
        return self._gen_table_id(self.model.Meta.table_id)

    @property
    def _table_description(self) -> str:
        return self.model.Meta.table_description or ''

    def _count_query(self, where: str = None) -> str:
        q = f'SELECT COUNT(*) FROM {self.dataset_client.dataset_name}.{self._table_id}'
        if where:
            q += f' WHERE {where}'
        return q

    def _select_query(self, fields: list[str] = None, where: str = None, limit: int = None) -> str:
        fields = ','.join(fields) if fields else '*'
        q = f'SELECT {fields} FROM {self.dataset_client.dataset_name}.{self._table_id}'
        if where:
            q += f' WHERE {where}'
        if limit:
            q += f' LIMIT {limit}'
        return q

    def _delete_query(self, where: str = None):
        q = f'DELETE FROM {self.dataset_client.dataset_name}.{self._table_id}'
        if where:
            q += f' WHERE {where}'
        return q

    def get_rows(
        self, fields: list[str] = None, where: str = None, limit: int = None, as_objects: bool = True
    ) -> list[Any | dict]:
        """
        Fetch rows from the table/view.
        
        Args:
            fields: List of field names to select (None for all)
            where: WHERE clause condition
            limit: Maximum number of rows to return
            as_objects: If True, return as Pydantic model instances; else as dicts
            
        Returns:
            List of model instances or dicts
        """
        q = self._select_query(fields=fields, where=where, limit=limit)
        _rows = self._bq_client.query(q).result()
        if as_objects and not fields:
            return [self.model(**dict(r)) for r in _rows]
        else:
            return [dict(r) for r in _rows]

    def count_rows(self, where: str = None) -> int:
        """Count rows in the table/view."""
        q = self._count_query(where=where)
        _rows = self._bq_client.query(q).result()
        return list(dict(next(_rows)).values())[0]


@dataclass
class BQView(_BQTableViewBase):
    """BigQuery view wrapper with query support."""
    pass


@dataclass
class BQTable(_BQTableViewBase):
    """BigQuery table wrapper with full CRUD support."""
    
    def create(self):
        """Create the table in BigQuery."""
        t = bigquery.Table(self._bq_table_ref, schema=self.model.bq_schema())
        t.description = self._table_description
        self._bq_client.create_table(t)
        logger.info('table "%s" created', self._table_id)

    def delete(self):
        """Delete the table from BigQuery."""
        self._bq_client.delete_table(self._bq_table_ref)

    def recreate(self):
        """Delete and recreate the table."""
        try:
            self.delete()
        except NotFound:
            pass
        else:
            logger.info('table "%s" deleted', self._table_id)
        self.create()

    def add_rows(self, *objs: BQBaseModel, send_as_file: bool = True) -> list[dict]:
        """
        Add rows to the table.
        
        Args:
            objs: Pydantic model instances to insert
            send_as_file: If True, use load job (better for large data); else use streaming insert
            
        Returns:
            List of errors (empty if successful)
        """
        logger.info('loading %d rows to %s', len(objs), self._table_id)
        if not objs:
            return []

        if send_as_file:
            # Convert list of JSON strings to newline-delimited bytes
            objs_json = [obj.model_dump_json() for obj in objs]
            ndjson_data = '\n'.join(objs_json).encode('utf-8')
            file = BytesIO(ndjson_data)
            file.seek(0)

            job_config = bigquery.LoadJobConfig()
            job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON

            job = self._bq_client.load_table_from_file(file, self._bq_table_ref, job_config=job_config)
            job_result(job)
            return []
        else:
            objs_dumped = sorted([obj.model_dump() for obj in objs], key=itemgetter('code'))
            obj_batches = [objs_dumped[i : i + 500] for i in range(0, len(objs_dumped), 500)]
            for i, batch in enumerate(obj_batches):
                try:
                    self._bq_client.insert_rows_json(self._bq_table_ref, batch)
                except NotFound:
                    # Now breaking that batch in batches of 50
                    batch_batches = [batch[i : i + 50] for i in range(0, len(batch), 50)]
                    for j, batch_batch in enumerate(batch_batches):
                        try:
                            self._bq_client.insert_rows_json(self._bq_table_ref, batch_batch)
                        except Exception:
                            raise RuntimeError(f'Problem with batch {j} of {i} in {self._table_id}')

    def delete_rows(self, where: str):
        """Delete rows matching the WHERE condition."""
        q = self._delete_query(where=where)
        return self._bq_client.query(q).result()


class DatasetClient:
    """
    Client for interacting with a BigQuery dataset.
    
    Example:
        >>> client = DatasetClient('my_dataset')
        >>> rows = client.table(MyModel).get_rows(limit=10)
    """
    
    def __init__(
        self,
        dataset_name: str = None,
        credentials_base64: str = None,
        credentials_json: str = None,
        credentials_file: str = None,
        credentials_dict: dict = None,
        settings: Settings = None,
    ):
        """
        Initialize dataset client.
        
        Args:
            dataset_name: Name of the BigQuery dataset (or use BQ_DATASET env var)
            credentials_base64: Base64 encoded service account JSON
            credentials_json: Raw service account JSON string  
            credentials_file: Path to service account JSON file
            credentials_dict: Dict with service account credentials
            settings: Settings object with Google credentials
        """
        self._client: bigquery.Client = create_client(
            credentials_base64=credentials_base64,
            credentials_json=credentials_json,
            credentials_file=credentials_file,
            credentials_dict=credentials_dict,
            settings=settings,
        )
        # Use provided dataset_name, or fall back to settings
        if dataset_name:
            self.dataset_name = dataset_name
        elif settings and settings.bq_dataset:
            self.dataset_name = settings.bq_dataset
        elif default_settings.bq_dataset:
            self.dataset_name = default_settings.bq_dataset
        else:
            raise ValueError('dataset_name is required (or set BQ_DATASET env var)')
        
        self.dataset_ref = DatasetReference(self._client.project, self.dataset_name)

    def query(self, sql: str) -> list[dict]:
        """
        Execute a raw SQL query.
        
        Args:
            sql: SQL query string
            
        Returns:
            List of result rows as dicts
        """
        result = self._client.query(sql).result()
        return [dict(row) for row in result]

    def add_rows(self, *objs: BQBaseModel, defer: bool = False, job_id: str = None) -> LoadJob:
        """Add rows to the appropriate table based on model type."""
        table = BQTable(self, type(objs[0]))
        return table.add_rows(*objs)

    def view(self, model: Type[BQBaseModel]) -> BQView:
        """Get a view wrapper for the given model."""
        return BQView(self, model)

    def table(self, model: Type[BQBaseModel]) -> BQTable:
        """Get a table wrapper for the given model."""
        return BQTable(self, model)

    def create_table(self, model: Type[BQBaseModel]) -> BQTable:
        """Create a new table for the given model."""
        table = self.table(model)
        table.create()
        return table

    def delete_table(self, model: Type[BQBaseModel]) -> None:
        """Delete the table for the given model."""
        table = self.table(model)
        table.delete()

    def recreate_table(self, model: Type[BQBaseModel]) -> BQTable:
        """Delete and recreate the table for the given model."""
        table = self.table(model)
        table.recreate()
        return table

