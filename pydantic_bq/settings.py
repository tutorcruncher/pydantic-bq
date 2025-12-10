"""Settings for BigQuery configuration using Pydantic."""

import base64
import json

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """BigQuery application settings."""

    model_config = SettingsConfigDict(env_file='.env', extra='ignore', case_sensitive=False)

    # Option 1: Base64 encoded credentials (simplest)
    bigquery_credentials: str = ''

    # Option 2: Google service account credentials (broken down for env vars)
    g_project_id: str = ''
    g_client_email: str = ''
    g_private_key_id: str = ''
    g_private_key: str = ''
    g_client_id: str = ''
    g_auth_uri: str = 'https://accounts.google.com/o/oauth2/auth'
    g_token_uri: str = 'https://oauth2.googleapis.com/token'
    g_auth_provider_x509_cert_url: str = 'https://www.googleapis.com/oauth2/v1/certs'
    g_client_x509_cert_url: str = ''

    @property
    def google_credentials(self) -> dict:
        """Build Google service account credentials dict from base64 or individual fields."""
        if self.bigquery_credentials:
            return json.loads(base64.urlsafe_b64decode(self.bigquery_credentials.encode()).decode())

        return {
            'type': 'service_account',
            'project_id': self.g_project_id,
            'private_key_id': self.g_private_key_id,
            'private_key': self.g_private_key.replace('\\n', '\n'),
            'client_email': self.g_client_email,
            'client_id': self.g_client_id,
            'auth_uri': self.g_auth_uri,
            'token_uri': self.g_token_uri,
            'auth_provider_x509_cert_url': self.g_auth_provider_x509_cert_url,
            'client_x509_cert_url': self.g_client_x509_cert_url,
        }

    @property
    def has_credentials(self) -> bool:
        """Check if credentials are configured."""
        return bool(self.bigquery_credentials or (self.g_project_id and self.g_private_key and self.g_client_email))


settings = Settings()
