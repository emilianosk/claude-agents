from functools import lru_cache
from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file='.env', env_file_encoding='utf-8', extra='ignore')

    app_env: str = Field(default='dev', alias='APP_ENV')
    app_timezone: str = Field(default='Australia/Brisbane', alias='APP_TIMEZONE')
    app_name: str = Field(default='claude-agents', alias='APP_NAME')
    app_host: str = Field(default='0.0.0.0', alias='APP_HOST')
    app_port: int = Field(default=8080, alias='APP_PORT')

    storage_root: str = Field(default='app/storage', alias='STORAGE_ROOT')
    uploads_dir: str = Field(default='uploads', alias='UPLOADS_DIR')
    results_dir: str = Field(default='results', alias='RESULTS_DIR')

    databricks_host: str = Field(default='', alias='DATABRICKS_HOST')
    databricks_token: str = Field(default='', alias='DATABRICKS_TOKEN')
    databricks_http_path: str = Field(default='', alias='DATABRICKS_HTTP_PATH')
    databricks_sql_warehouse_id: str = Field(default='', alias='DATABRICKS_SQL_WAREHOUSE_ID')
    databricks_catalog: str = Field(default='', alias='DATABRICKS_CATALOG')
    databricks_schema: str = Field(default='', alias='DATABRICKS_SCHEMA')
    databricks_wait_timeout: str = Field(default='30s', alias='DATABRICKS_WAIT_TIMEOUT')
    databricks_ssl_verify: bool = Field(default=True, alias='DATABRICKS_SSL_VERIFY')

    databricks_oauth_tenant_id: str = Field(default='', alias='DATABRICKS_OAUTH_TENANT_ID')
    databricks_oauth_client_id: str = Field(default='', alias='DATABRICKS_OAUTH_CLIENT_ID')
    databricks_oauth_client_secret: str = Field(default='', alias='DATABRICKS_OAUTH_CLIENT_SECRET')
    databricks_oauth_token_url: str = Field(default='', alias='DATABRICKS_OAUTH_TOKEN_URL')

    deputy_base: str = Field(default='', alias='DEPUTY_BASE')
    deputy_access_token: str = Field(default='', alias='DEPUTY_ACCESS_TOKEN')
    deputy_client_id: str = Field(default='', alias='DEPUTY_CLIENT_ID')
    deputy_client_secret: str = Field(default='', alias='DEPUTY_CLIENT_SECRET')
    deputy_redirect_uri: str = Field(default='', alias='DEPUTY_REDIRECT_URI')
    deputy_ssl_verify: bool = Field(default=True, alias='DEPUTY_SSL_VERIFY')
    deputy_timeout_seconds: int = Field(default=30, alias='DEPUTY_TIMEOUT_SECONDS')

    salesforce_org_id: str = Field(default='', alias='SALESFORCE_ORG_ID')
    salesforce_record_type_id: str = Field(default='', alias='SALESFORCE_RECORD_TYPE_ID')
    salesforce_base_url: str = Field(default='', alias='SALESFORCE_BASE_URL')
    salesforce_base_login_url: str = Field(default='', alias='SALESFORCE_BASE_LOGIN_URL')
    salesforce_client_id: str = Field(default='', alias='SALESFORCE_CLIENT_ID')
    salesforce_client_secret: str = Field(default='', alias='SALESFORCE_CLIENT_SECRET')
    salesforce_refresh_token: str = Field(default='', alias='SALESFORCE_REFRESH_TOKEN')
    salesforce_api_version: str = Field(default='v59.0', alias='SALESFORCE_API_VERSION')
    salesforce_timeout_seconds: int = Field(default=30, alias='SALESFORCE_TIMEOUT_SECONDS')
    salesforce_ssl_verify: bool = Field(default=True, alias='SALESFORCE_SSL_VERIFY')

    ls_api_version: str = Field(default='2026-04', alias='LS_API_VERSION')
    ls_region_default: str = Field(default='AU', alias='LS_REGION_DEFAULT')
    ls_domain_au: str = Field(default='', alias='LS_DOMAIN_AU')
    ls_token_au: str = Field(default='', alias='LS_TOKEN_AU')
    ls_domain_nz: str = Field(default='', alias='LS_DOMAIN_NZ')
    ls_token_nz: str = Field(default='', alias='LS_TOKEN_NZ')
    ls_ssl_verify: bool = Field(default=True, alias='LS_SSL_VERIFY')
    ls_timeout_seconds: int = Field(default=30, alias='LS_TIMEOUT_SECONDS')

    anthropic_api_key: str = Field(default='', alias='ANTHROPIC_API_KEY')
    anthropic_model: str = Field(default='claude-sonnet-4-5', alias='ANTHROPIC_MODEL')
    anthropic_max_tokens: int = Field(default=2500, alias='ANTHROPIC_MAX_TOKENS')

    query_map_file: str = Field(default='config/query-map.json', alias='QUERY_MAP_FILE')
    datasets_config_file: str = Field(default='config/datasets.yaml', alias='DATASETS_CONFIG_FILE')
    agents_config_file: str = Field(default='config/agents.yaml', alias='AGENTS_CONFIG_FILE')
    agent_prompts_dir: str = Field(default='config/prompts', alias='AGENT_PROMPTS_DIR')

    @property
    def uploads_path(self) -> Path:
        return Path(self.storage_root) / self.uploads_dir

    @property
    def results_path(self) -> Path:
        return Path(self.storage_root) / self.results_dir


@lru_cache
def get_settings() -> Settings:
    return Settings()
