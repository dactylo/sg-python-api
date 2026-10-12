import xmlrpc.client

from shotgun_api3 import _errors


def test_hierarchy() -> None:
    assert issubclass(_errors.Fault, _errors.ShotgunError)
    assert issubclass(_errors.AuthenticationFault, _errors.Fault)
    assert issubclass(_errors.MissingTwoFactorAuthenticationFault, _errors.Fault)
    assert issubclass(_errors.UserCredentialsNotAllowedForSSOAuthenticationFault, _errors.Fault)
    assert issubclass(_errors.UserCredentialsNotAllowedForOxygenAuthenticationFault, _errors.Fault)
    assert issubclass(_errors.ShotgunFileDownloadError, _errors.ShotgunError)
    assert issubclass(_errors.ShotgunThumbnailNotReady, _errors.ShotgunError)


def test_xmlrpc_aliases_are_the_stdlib_classes() -> None:
    assert _errors.ProtocolError is xmlrpc.client.ProtocolError
    assert _errors.ResponseError is xmlrpc.client.ResponseError
    assert _errors.Error is xmlrpc.client.Error
