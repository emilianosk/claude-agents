from app.services.lightspeed_service import LightspeedConfig, LightspeedRegionConfig, LightspeedService


def test_lightspeed_is_configured_for_region() -> None:
    cfg = LightspeedConfig(
        regions={
            'AU': LightspeedRegionConfig(domain='skinkandyaus', token='token-au'),
            'NZ': LightspeedRegionConfig(domain='skinkandynz', token='token-nz'),
        }
    )
    service = LightspeedService(cfg)
    assert service.is_configured('AU') is True
    assert service.is_configured('NZ') is True


def test_lightspeed_builds_base_url() -> None:
    service = LightspeedService(LightspeedConfig())
    assert service._build_base_url('skinkandyaus').startswith('https://skinkandyaus.retail.lightspeed.app/api/')
