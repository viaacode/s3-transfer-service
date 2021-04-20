import paramiko
import pytest
import requests


@pytest.fixture(autouse=True)
def disable_network_calls(monkeypatch):
    def stunted_head():
        raise RuntimeError("Network access not allowed during testing!")

    def stunted_ssh_connect():
        raise RuntimeError("Network access not allowed during testing!")

    monkeypatch.setattr(requests, "head", lambda *args, **kwargs: stunted_head())
    monkeypatch.setattr(
        paramiko.SSHClient, "connect", lambda *args, **kwargs: stunted_ssh_connect()
    )
