from shotgun_api3 import _errors


def test_hierarchy() -> None:
    assert issubclass(_errors.Fault, _errors.ShotgunError)
    assert issubclass(_errors.AuthenticationFault, _errors.Fault)
    assert issubclass(_errors.MissingTwoFactorAuthenticationFault, _errors.Fault)
    assert issubclass(_errors.UserCredentialsNotAllowedForSSOAuthenticationFault, _errors.Fault)
    assert issubclass(_errors.UserCredentialsNotAllowedForOxygenAuthenticationFault, _errors.Fault)
    assert issubclass(_errors.ShotgunFileDownloadError, _errors.ShotgunError)
    assert issubclass(_errors.ShotgunThumbnailNotReady, _errors.ShotgunError)
