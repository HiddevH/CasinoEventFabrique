"""Core functionality for CasinoEventFabrique."""

from typing import Dict, Any, Optional


class Event:
    """Base event class for CasinoEventFabrique."""

    def __init__(self, name: str, data: Optional[Dict[str, Any]] = None):
        """Initialize an event.
        
        Args:
            name: The name of the event
            data: Optional data payload for the event
        """
        self.name = name
        self.data = data or {}
        
    def __repr__(self) -> str:
        """Return string representation of the event."""
        return f"Event(name={self.name}, data={self.data})"