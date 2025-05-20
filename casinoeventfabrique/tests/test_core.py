"""Tests for the core module."""

import pytest
from casinoeventfabrique.core import Event


def test_event_creation():
    """Test basic event creation."""
    event = Event(name="test_event")
    assert event.name == "test_event"
    assert event.data == {}
    
    
def test_event_with_data():
    """Test event creation with data."""
    data = {"key": "value", "count": 42}
    event = Event(name="test_event", data=data)
    assert event.name == "test_event"
    assert event.data == data
    assert event.data["key"] == "value"
    assert event.data["count"] == 42