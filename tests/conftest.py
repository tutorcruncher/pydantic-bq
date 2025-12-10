"""Shared fixtures for pydantic-bq tests."""

import base64
import json
from datetime import date, datetime
from typing import Optional
from unittest.mock import MagicMock, patch

import pytest

from pydantic_bq.schema import BQBaseModel


@pytest.fixture
def sample_credentials():
    """Sample Google service account credentials for testing."""
    return {
        'type': 'service_account',
        'project_id': 'test-project',
        'private_key_id': 'key123',
        'private_key': '-----BEGIN RSA PRIVATE KEY-----\ntest\n-----END RSA PRIVATE KEY-----\n',
        'client_email': 'test@test-project.iam.gserviceaccount.com',
        'client_id': '123456789',
        'auth_uri': 'https://accounts.google.com/o/oauth2/auth',
        'token_uri': 'https://oauth2.googleapis.com/token',
        'auth_provider_x509_cert_url': 'https://www.googleapis.com/oauth2/v1/certs',
        'client_x509_cert_url': 'https://www.googleapis.com/robot/v1/metadata/x509/test',
    }


@pytest.fixture
def sample_credentials_base64(sample_credentials):
    """Base64 encoded credentials."""
    return base64.urlsafe_b64encode(json.dumps(sample_credentials).encode()).decode()


@pytest.fixture
def mock_settings_with_creds(sample_credentials):
    """Patch settings with test credentials using individual fields."""
    with patch('pydantic_bq.client.settings') as mock_settings:
        mock_settings.has_credentials = True
        mock_settings.bq_dataset = 'test_dataset'
        mock_settings.google_credentials = sample_credentials
        yield mock_settings


@pytest.fixture
def mock_settings_no_creds():
    """Patch settings with no credentials."""
    with patch('pydantic_bq.client.settings') as mock_settings:
        mock_settings.has_credentials = False
        mock_settings.bq_dataset = ''
        yield mock_settings


@pytest.fixture
def mock_bq_client():
    """Mock BigQuery client and credentials."""
    with (
        patch('pydantic_bq.client.service_account.Credentials') as mock_creds,
        patch('pydantic_bq.client.bigquery.Client') as mock_client_class,
    ):
        mock_creds.from_service_account_info.return_value = MagicMock()
        mock_client = MagicMock()
        mock_client.project = 'test-project'
        mock_client_class.return_value = mock_client
        yield mock_client


@pytest.fixture
def dataset_client(mock_bq_client, mock_settings_with_creds):
    """Ready-to-use DatasetClient with all mocks in place."""
    from pydantic_bq.client import DatasetClient

    return DatasetClient('test_dataset')


class SampleModel(BQBaseModel):
    """Test model with various field types for comprehensive testing."""

    code: str
    name: str
    count: int
    price: float
    is_active: bool
    created_at: datetime
    birth_date: date
    description: Optional[str] = None
    tags: list[str] = []

    class Meta:
        table_id = 'sample_table'
        table_description = 'A sample table for testing'


class MinimalModel(BQBaseModel):
    """Minimal model with just required fields."""

    code: str
    value: int

    class Meta:
        table_id = 'minimal_table'
        table_description = ''


@pytest.fixture
def sample_model():
    """Return the SampleModel class."""
    return SampleModel


@pytest.fixture
def minimal_model():
    """Return the MinimalModel class."""
    return MinimalModel


@pytest.fixture
def sample_instance():
    """A sample model instance for testing."""
    return SampleModel(
        code='TEST001',
        name='Test Item',
        count=10,
        price=99.99,
        is_active=True,
        created_at=datetime(2024, 1, 15, 10, 30, 0),
        birth_date=date(1990, 5, 20),
        description='A test item',
        tags=['tag1', 'tag2'],
    )


@pytest.fixture
def minimal_instance():
    """A minimal model instance for testing."""
    return MinimalModel(code='MIN001', value=42)


# =============================================================================
# E2E Test Fixtures (Real BigQuery)
# =============================================================================

E2E_DATASET_NAME = 'pydantic_bq_test'


@pytest.fixture(scope='session')
def e2e_dataset():
    """
    Create test dataset once per session, yield the DatasetClient.

    The dataset is created if it doesn't exist and is reused across tests.
    Tables are cleaned up by individual tests.
    """
    from pydantic_bq.settings import settings

    if not settings.has_credentials:
        pytest.skip('BigQuery credentials not configured')

    from google.cloud import bigquery

    from pydantic_bq.client import create_client

    # Create the raw BQ client
    client = create_client()

    # Create dataset if it doesn't exist
    dataset_ref = bigquery.Dataset(f'{client.project}.{E2E_DATASET_NAME}')
    dataset_ref.location = 'US'

    try:
        client.get_dataset(E2E_DATASET_NAME)
    except Exception:
        client.create_dataset(dataset_ref, exists_ok=True)

    # Now create a DatasetClient for tests to use
    from pydantic_bq.client import DatasetClient

    yield DatasetClient(E2E_DATASET_NAME)


@pytest.fixture
def unique_table_suffix():
    """Generate a unique suffix for table names to avoid conflicts."""
    import time

    return f'{int(time.time() * 1000)}'


def create_e2e_model(table_suffix: str):
    """Factory to create a model class with a unique table name."""

    class E2ETestModel(BQBaseModel):
        """Model for E2E testing with unique table name."""

        code: str
        name: str
        count: int
        price: float
        is_active: bool
        created_at: datetime
        birth_date: date
        description: Optional[str] = None
        tags: list[str] = []

        class Meta:
            table_id = f'e2e_test_{table_suffix}'
            table_description = 'E2E test table'

    return E2ETestModel


@pytest.fixture
def e2e_model(unique_table_suffix):
    """Create a model class with a unique table name for E2E tests."""
    return create_e2e_model(unique_table_suffix)
