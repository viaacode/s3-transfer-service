#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from unittest.mock import patch

import pytest

from app.helpers.message_parser import InvalidMessageException
from app.app import EventListener


@pytest.fixture
@patch("app.app.RabbitClient")
def event_listener(rabbit_client_mock):
    return EventListener()


@patch("app.app.parse_validate_json", side_effect=InvalidMessageException("invalid"))
@patch("app.app.Transfer")
def test_do_work_invalid_message(transfer_mock, parse_mock, event_listener, caplog):
    event_listener.do_work(None, None, None)
    assert "invalid" in caplog.messages
    rabbit_client_mock = event_listener.rabbit_client
    assert rabbit_client_mock.connection.add_callback_threadsafe.call_count == 1
    assert not transfer_mock.call_count
