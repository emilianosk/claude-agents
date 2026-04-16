from app.services.deputy_service import DeputyConfig, DeputyService


def test_deputy_is_configured() -> None:
    service = DeputyService(DeputyConfig(base_url='https://example.deputy.com', access_token='abc123'))
    assert service.is_configured() is True


def test_deputy_is_not_configured_without_token() -> None:
    service = DeputyService(DeputyConfig(base_url='https://example.deputy.com', access_token=''))
    assert service.is_configured() is False
