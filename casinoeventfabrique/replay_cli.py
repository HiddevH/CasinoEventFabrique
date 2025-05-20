"""Command-line interface for CasinoEventFabrique replay functionality."""

import argparse
import logging
import os
import sys
from typing import Dict, Any, List, Optional

from .replay import CSVEventReader, EventHubPublisher, EventReplay


def setup_logging(verbose: bool = False) -> None:
    """Set up logging configuration.
    
    Args:
        verbose: Whether to use verbose logging
    """
    log_level = logging.DEBUG if verbose else logging.INFO
    log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(level=log_level, format=log_format)


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments.
    
    Returns:
        Parsed arguments
    """
    parser = argparse.ArgumentParser(
        description="Replay iGaming events from CSV to Azure Event Hub"
    )
    
    parser.add_argument(
        "--csv-file",
        "-f",
        required=True,
        help="Path to the CSV file containing event data"
    )
    
    parser.add_argument(
        "--connection-string",
        "-c",
        required=True,
        help="Azure Event Hub connection string"
    )
    
    parser.add_argument(
        "--eventhub-name",
        "-n",
        required=True,
        help="Name of the Azure Event Hub"
    )
    
    parser.add_argument(
        "--timestamp-field",
        "-t",
        default="timestamp",
        help="Name of the timestamp field in the CSV (default: timestamp)"
    )
    
    parser.add_argument(
        "--state-file",
        "-s",
        default="state.json",
        help="Path to the state file (default: state.json)"
    )
    
    parser.add_argument(
        "--reset-state",
        action="store_true",
        help="Reset replay state (replay from beginning)"
    )
    
    parser.add_argument(
        "--replay-delay",
        "-d",
        type=float,
        default=0.1,
        help="Delay between events in seconds (default: 0.1)"
    )
    
    parser.add_argument(
        "--limit",
        "-l",
        type=int,
        help="Maximum number of events to replay"
    )
    
    parser.add_argument(
        "--jitter",
        "-j",
        action="store_true",
        help="Apply jitter to amount fields for more realistic simulation"
    )
    
    parser.add_argument(
        "--jitter-fields",
        nargs="+",
        help="Fields to apply jitter to (if not specified, applied to all numeric fields)"
    )
    
    parser.add_argument(
        "--jitter-percentage",
        type=float,
        default=0.05,
        help="Percentage of jitter to apply (default: 0.05 = ±5%%)"
    )
    
    parser.add_argument(
        "--partition-key-field",
        default="pseudo_player_id",
        help="Field to use as partition key (default: pseudo_player_id)"
    )
    
    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Enable verbose logging"
    )
    
    return parser.parse_args()


def main() -> int:
    """Main entry point for the CLI.
    
    Returns:
        Exit code
    """
    args = parse_args()
    setup_logging(args.verbose)
    logger = logging.getLogger(__name__)
    
    try:
        # Validate CSV file exists
        if not os.path.isfile(args.csv_file):
            logger.error(f"CSV file not found: {args.csv_file}")
            return 1
        
        # Create event reader
        event_reader = CSVEventReader(
            file_path=args.csv_file,
            timestamp_field=args.timestamp_field
        )
        
        # Create event publisher
        event_publisher = EventHubPublisher(
            connection_string=args.connection_string,
            eventhub_name=args.eventhub_name
        )
        
        # Create and run event replay
        replay = EventReplay(
            event_reader=event_reader,
            event_publisher=event_publisher,
            state_file=args.state_file,
            replay_delay=args.replay_delay,
            apply_jitter=args.jitter,
            jitter_fields=args.jitter_fields,
            jitter_percentage=args.jitter_percentage,
            partition_key_field=args.partition_key_field
        )
        
        events_sent, last_timestamp = replay.replay(
            limit=args.limit,
            reset_state=args.reset_state
        )
        
        logger.info(f"Replay finished. Sent {events_sent} events. Last timestamp: {last_timestamp}")
        return 0
        
    except KeyboardInterrupt:
        logger.info("Replay interrupted by user")
        return 0
        
    except Exception as e:
        logger.exception(f"Error during replay: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())