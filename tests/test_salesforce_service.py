from app.services.salesforce_service import SalesforceConfig, SalesforceService


def test_salesforce_validate_config_missing_values() -> None:
    service = SalesforceService(SalesforceConfig())
    result = service.validate_config()
    assert result['valid'] is False
    assert len(result['errors']) > 0


def test_salesforce_validate_config_ok() -> None:
    service = SalesforceService(
        SalesforceConfig(
            base_login_url='https://test.salesforce.com',
            client_id='abc',
            client_secret='def',
            refresh_token='ghi',
        )
    )
    result = service.validate_config()
    assert result['valid'] is True
    assert result['errors'] == []
