"""Tests for settings and credentials handling."""

import base64
import json
import os
from unittest.mock import patch

import pytest

from pydantic_bq.settings import Settings


class TestGoogleCredentials:
    """Tests for google_credentials property."""

    def test_credentials_from_base64(self, sample_credentials):
        """Test decoding credentials from base64."""
        encoded = base64.urlsafe_b64encode(json.dumps(sample_credentials).encode()).decode()

        # Use _env_file=None to prevent loading from .env
        settings = Settings(bigquery_credentials=encoded, _env_file=None)
        result = settings.google_credentials

        assert result == sample_credentials
        assert result['project_id'] == 'test-project'

    def test_credentials_from_individual_fields(self):
        """Test building credentials from individual fields."""
        settings = Settings(
            g_project_id='my-project',
            g_client_email='test@example.com',
            g_private_key_id='key123',
            g_private_key='-----BEGIN PRIVATE KEY-----\\ntest\\n-----END PRIVATE KEY-----',
            g_client_id='12345',
            _env_file=None,
        )
        result = settings.google_credentials

        assert result['type'] == 'service_account'
        assert result['project_id'] == 'my-project'
        assert result['client_email'] == 'test@example.com'
        assert result['private_key_id'] == 'key123'
        # Check newline replacement
        assert '\\n' not in result['private_key']
        assert '\n' in result['private_key']

    def test_base64_takes_precedence_over_fields(self, sample_credentials):
        """Base64 credentials should be used if both are provided."""
        encoded = base64.urlsafe_b64encode(json.dumps(sample_credentials).encode()).decode()

        settings = Settings(
            bigquery_credentials=encoded,
            g_project_id='other-project',
            g_client_email='other@example.com',
            g_private_key='other-key',
            _env_file=None,
        )
        result = settings.google_credentials

        # Should use base64, not individual fields
        assert result['project_id'] == 'test-project'


class TestHasCredentials:
    """Tests for has_credentials property."""

    def test_has_credentials_with_base64(self, sample_credentials_base64):
        """Returns True when base64 credentials are set."""
        settings = Settings(bigquery_credentials=sample_credentials_base64, _env_file=None)
        assert settings.has_credentials is True

    def test_has_credentials_with_individual_fields(self):
        """Returns True when required individual fields are set."""
        settings = Settings(
            g_project_id='my-project',
            g_private_key='my-key',
            g_client_email='test@example.com',
            _env_file=None,
        )
        assert settings.has_credentials is True

    def test_has_credentials_partial_fields(self):
        """Returns False when only some individual fields are set."""
        # Clear any env vars that might interfere
        with patch.dict(os.environ, {}, clear=True):
            settings = Settings(
                g_project_id='my-project',
                g_private_key='my-key',
                g_client_email='',  # Explicitly empty
                bigquery_credentials='',
                _env_file=None,
            )
            assert settings.has_credentials is False

    def test_has_credentials_none(self):
        """Returns False when no credentials are configured."""
        with patch.dict(os.environ, {}, clear=True):
            settings = Settings(
                bigquery_credentials='',
                g_project_id='',
                g_private_key='',
                g_client_email='',
                _env_file=None,
            )
            assert settings.has_credentials is False


class TestCreateClientNoCredentials:
    """Test create_client error handling."""

    def test_create_client_raises_without_credentials(self):
        """create_client should raise RuntimeError when no credentials."""
        from pydantic_bq.client import create_client

        with patch('pydantic_bq.client.settings') as mock_settings:
            mock_settings.has_credentials = False

            with pytest.raises(RuntimeError, match='No BigQuery credentials found'):
                create_client()
