"""Replay functionality for CasinoEventFabrique."""

import csv
import json
import logging
import os
import random
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, List, Optional, Iterator, Tuple

try:
    from azure.eventhub import EventHubProducerClient, EventData
    from azure.eventhub.exceptions import EventHubError
    HAS_EVENTHUB = True
except ImportError:
    HAS_EVENTHUB = False

from .core import Event


class CSVEventReader:
    """Reader for CSV event data."""

    def __init__(
        self,
        file_path: str,
        timestamp_field: str = "timestamp",
        delimiter: str = ",",
        encoding: str = "utf-8",
    ):
        """Initialize CSV event reader.
        
        Args:
            file_path: Path to the CSV file
            timestamp_field: Name of the timestamp field in the CSV
            delimiter: CSV delimiter character
            encoding: File encoding
        """
        self.file_path = file_path
        self.timestamp_field = timestamp_field
        self.delimiter = delimiter
        self.encoding = encoding
        self.logger = logging.getLogger(__name__)

    def read_events(
        self, last_timestamp: Optional[str] = None
    ) -> Iterator[Dict[str, Any]]:
        """Read events from CSV file.
        
        Args:
            last_timestamp: If provided, skip events with timestamp <= last_timestamp
            
        Yields:
            Dict containing event data
        """
        self.logger.info(f"Reading events from {self.file_path}")
        
        try:
            with open(self.file_path, mode="r", encoding=self.encoding) as csvfile:
                reader = csv.DictReader(csvfile, delimiter=self.delimiter)
                
                for row in reader:
                    # Skip rows with timestamp <= last_timestamp
                    if last_timestamp and row.get(self.timestamp_field) and row[self.timestamp_field] <= last_timestamp:
                        continue
                    
                    yield row
                    
        except Exception as e:
            self.logger.error(f"Error reading CSV file: {e}")
            raise


class EventHubPublisher:
    """Publisher for Azure Event Hub."""
    
    def __init__(
        self,
        connection_string: str,
        eventhub_name: str,
    ):
        """Initialize Event Hub publisher.
        
        Args:
            connection_string: Azure Event Hub connection string
            eventhub_name: Name of the Event Hub
        """
        if not HAS_EVENTHUB:
            raise ImportError(
                "Azure Event Hub package not installed. "
                "Install with: pip install azure-eventhub"
            )
            
        self.connection_string = connection_string
        self.eventhub_name = eventhub_name
        self.producer = None
        self.logger = logging.getLogger(__name__)
        
    def __enter__(self):
        """Context manager entry."""
        self.producer = EventHubProducerClient.from_connection_string(
            conn_str=self.connection_string,
            eventhub_name=self.eventhub_name
        )
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        if self.producer:
            self.producer.close()
            
    def send_event(self, event_data: Dict[str, Any], partition_key: Optional[str] = None) -> bool:
        """Send a single event to Event Hub.
        
        Args:
            event_data: Event data to send
            partition_key: Optional partition key for routing
            
        Returns:
            True if sent successfully, False otherwise
        """
        if not self.producer:
            raise RuntimeError("Publisher not initialized. Use with context manager.")
            
        try:
            event_batch = self.producer.create_batch()
            event_batch.add(EventData(body=json.dumps(event_data).encode('utf-8')))
            
            self.producer.send_batch(event_batch)
            return True
            
        except EventHubError as e:
            self.logger.error(f"Error sending event to Event Hub: {e}")
            return False
            
    def send_events(self, events: List[Dict[str, Any]], partition_key: Optional[str] = None) -> int:
        """Send multiple events to Event Hub.
        
        Args:
            events: List of event data to send
            partition_key: Optional partition key for routing
            
        Returns:
            Number of events sent successfully
        """
        if not self.producer:
            raise RuntimeError("Publisher not initialized. Use with context manager.")
            
        try:
            event_batch = self.producer.create_batch()
            sent_count = 0
            
            for event_data in events:
                event_data_encoded = EventData(body=json.dumps(event_data).encode('utf-8'))
                
                # If we can't add the event to the current batch, send the batch and create a new one
                if not event_batch.add(event_data_encoded):
                    self.producer.send_batch(event_batch)
                    sent_count += len(event_batch)
                    
                    # Create a new batch
                    event_batch = self.producer.create_batch()
                    
                    # Try adding the event to the new batch
                    if not event_batch.add(event_data_encoded):
                        # If it still doesn't fit, this is an unusually large event
                        self.logger.warning(f"Event too large to fit in batch, skipping: {event_data}")
                        continue
            
            # Send the final batch if it contains events
            if len(event_batch) > 0:
                self.producer.send_batch(event_batch)
                sent_count += len(event_batch)
                
            return sent_count
            
        except EventHubError as e:
            self.logger.error(f"Error sending events to Event Hub: {e}")
            return 0


class EventReplay:
    """Replays events from a source to a destination."""
    
    def __init__(
        self,
        event_reader: CSVEventReader,
        event_publisher: EventHubPublisher,
        state_file: str = "state.json",
        replay_delay: float = 0.1,  # Default delay between events (seconds)
        apply_jitter: bool = False,  # Whether to apply jitter to amounts
        jitter_fields: Optional[List[str]] = None,  # Fields to apply jitter to
        jitter_percentage: float = 0.05,  # ±5% jitter by default
        partition_key_field: Optional[str] = "pseudo_player_id",  # Field to use as partition key
    ):
        """Initialize event replay.
        
        Args:
            event_reader: Reader for event data
            event_publisher: Publisher for events
            state_file: Path to state file
            replay_delay: Delay between events in seconds
            apply_jitter: Whether to apply jitter to numerical fields
            jitter_fields: Fields to apply jitter to (if None, applies to all number fields)
            jitter_percentage: Percentage of jitter to apply (0.05 = ±5%)
            partition_key_field: Field to use as partition key
        """
        self.event_reader = event_reader
        self.event_publisher = event_publisher
        self.state_file = state_file
        self.replay_delay = replay_delay
        self.apply_jitter = apply_jitter
        self.jitter_fields = jitter_fields or []
        self.jitter_percentage = jitter_percentage
        self.partition_key_field = partition_key_field
        self.logger = logging.getLogger(__name__)
        
    def load_state(self) -> Dict[str, Any]:
        """Load replay state from file.
        
        Returns:
            State dictionary
        """
        if os.path.exists(self.state_file):
            try:
                with open(self.state_file, mode="r") as f:
                    return json.load(f)
            except Exception as e:
                self.logger.error(f"Error loading state file: {e}")
        
        return {"last_timestamp": None, "events_sent": 0}
    
    def save_state(self, state: Dict[str, Any]) -> None:
        """Save replay state to file.
        
        Args:
            state: State dictionary
        """
        try:
            with open(self.state_file, mode="w") as f:
                json.dump(state, f, indent=2)
        except Exception as e:
            self.logger.error(f"Error saving state file: {e}")
    
    def apply_event_jitter(self, event: Dict[str, Any]) -> Dict[str, Any]:
        """Apply jitter to numerical fields in the event.
        
        Args:
            event: Event data
            
        Returns:
            Event data with jitter applied
        """
        if not self.apply_jitter:
            return event
            
        result = event.copy()
        fields_to_jitter = self.jitter_fields if self.jitter_fields else []
        
        # If no specific fields provided, try to detect numerical fields
        if not fields_to_jitter:
            for key, value in event.items():
                if isinstance(value, (int, float)) or (isinstance(value, str) and value.replace(".", "", 1).isdigit()):
                    fields_to_jitter.append(key)
        
        for field in fields_to_jitter:
            if field in result:
                try:
                    # Convert to float if it's a string representing a number
                    if isinstance(result[field], str):
                        value = float(result[field])
                    else:
                        value = result[field]
                        
                    # Apply jitter
                    jitter = 1.0 + (random.random() * 2 - 1) * self.jitter_percentage
                    result[field] = value * jitter
                    
                except (ValueError, TypeError):
                    # Skip if not a numerical field
                    pass
                    
        return result
    
    def replay(
        self,
        limit: Optional[int] = None,
        reset_state: bool = False,
    ) -> Tuple[int, str]:
        """Replay events to the destination.
        
        Args:
            limit: Maximum number of events to replay
            reset_state: Whether to reset the replay state
            
        Returns:
            Tuple of (number of events sent, last timestamp)
        """
        state = {"last_timestamp": None, "events_sent": 0} if reset_state else self.load_state()
        last_timestamp = state.get("last_timestamp")
        events_sent = state.get("events_sent", 0)
        
        self.logger.info(f"Starting replay from timestamp: {last_timestamp}")
        
        try:
            count = 0
            with self.event_publisher:
                for event in self.event_reader.read_events(last_timestamp):
                    # Apply jitter to event data if needed
                    if self.apply_jitter:
                        event = self.apply_event_jitter(event)
                    
                    # Extract partition key if available
                    partition_key = event.get(self.partition_key_field) if self.partition_key_field else None
                    
                    # Send the event
                    success = self.event_publisher.send_event(event, partition_key)
                    
                    if success:
                        count += 1
                        events_sent += 1
                        last_timestamp = event.get("timestamp")
                        
                        # Update state after each event
                        state["last_timestamp"] = last_timestamp
                        state["events_sent"] = events_sent
                        self.save_state(state)
                        
                        # Apply replay delay
                        if self.replay_delay > 0:
                            time.sleep(self.replay_delay)
                    
                    # Check if we've reached the limit
                    if limit is not None and count >= limit:
                        self.logger.info(f"Reached event limit: {limit}")
                        break
                        
            self.logger.info(f"Replay complete. Sent {count} events. Total sent: {events_sent}")
            return count, last_timestamp
            
        except Exception as e:
            self.logger.error(f"Error during replay: {e}")
            raise