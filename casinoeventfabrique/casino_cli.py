"""Command-line interface for CasinoEventFabrique Casino Simulation."""

import argparse
import logging
import sys
import time
from typing import Dict, Any, Optional

from .casino_simulation import (
    CasinoSimulation, 
    EventHubPublisher, 
    FileEventStore,
    PlayerProfileType
)


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
        description="Simulate casino players and generate events"
    )
    
    # Output options
    output_group = parser.add_mutually_exclusive_group(required=True)
    output_group.add_argument(
        "--output-file",
        "-o",
        help="Path to the output file for events (JSON format)"
    )
    output_group.add_argument(
        "--connection-string",
        "-c",
        help="Azure Event Hub connection string"
    )
    
    # Azure Event Hub options
    parser.add_argument(
        "--eventhub-name",
        "-n",
        help="Name of the Azure Event Hub (required with --connection-string)"
    )
    
    # Player distribution options
    parser.add_argument(
        "--normal-players",
        type=int,
        default=40,
        help="Number of normal players (default: 40)"
    )
    parser.add_argument(
        "--high-roller-players",
        type=int,
        default=3,
        help="Number of high-roller players (default: 3)"
    )
    parser.add_argument(
        "--occasional-players",
        type=int,
        default=4,
        help="Number of occasional players (default: 4)"
    )
    parser.add_argument(
        "--addict-players",
        type=int,
        default=1,
        help="Number of players with addictive behavior (default: 1)"
    )
    parser.add_argument(
        "--bonus-hunter-players",
        type=int,
        default=1,
        help="Number of bonus hunter players (default: 1)"
    )
    parser.add_argument(
        "--fraudster-players",
        type=int,
        default=1,
        help="Number of fraudulent players (default: 1)"
    )
    
    # Simulation options
    parser.add_argument(
        "--duration",
        "-d",
        type=int,
        default=3600,
        help="Duration of simulation in seconds (default: 3600 = 1 hour)"
    )
    parser.add_argument(
        "--threads",
        "-t",
        type=int,
        default=50,
        help="Number of threads for parallel simulation (default: 50)"
    )
    parser.add_argument(
        "--event-delay",
        type=float,
        default=0.05,
        help="Delay between event batches in seconds (default: 0.05)"
    )
    
    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Enable verbose logging"
    )
    
    # Parse arguments
    args = parser.parse_args()
    
    # Validate Event Hub arguments
    if args.connection_string and not args.eventhub_name:
        parser.error("--eventhub-name is required when using --connection-string")
    
    return args


def main() -> int:
    """Main entry point for the CLI.
    
    Returns:
        Exit code
    """
    args = parse_args()
    setup_logging(args.verbose)
    logger = logging.getLogger(__name__)
    
    try:
        # Create publisher
        publisher = None
        if args.connection_string:
            logger.info(f"Using Azure Event Hub: {args.eventhub_name}")
            try:
                publisher = EventHubPublisher(
                    connection_string=args.connection_string,
                    eventhub_name=args.eventhub_name
                )
            except ImportError as e:
                logger.error(f"Failed to initialize Event Hub publisher: {e}")
                return 1
        else:
            logger.info(f"Using file output: {args.output_file}")
            publisher = FileEventStore(args.output_file)
        
        # Start time measurement
        start_time = time.time()
        
        # Create and run simulation
        simulation = CasinoSimulation(
            publisher=publisher,
            num_normal_players=args.normal_players,
            num_high_roller_players=args.high_roller_players,
            num_occasional_players=args.occasional_players,
            num_addict_players=args.addict_players,
            num_bonus_hunter_players=args.bonus_hunter_players,
            num_fraudster_players=args.fraudster_players,
            simulation_duration=args.duration,
            thread_count=args.threads,
            event_delay=args.event_delay,
            output_file=args.output_file if not args.connection_string else None
        )
        
        # Start the simulation
        simulation.start()
        
        # Report final stats
        elapsed_time = time.time() - start_time
        logger.info(f"Simulation completed in {elapsed_time:.2f} seconds")
        logger.info(f"Total events generated: {simulation.total_events_generated}")
        logger.info(f"Total events sent/stored: {simulation.total_events_sent}")
        
        return 0
        
    except KeyboardInterrupt:
        logger.info("Simulation interrupted by user")
        return 0
        
    except Exception as e:
        logger.exception(f"Error during simulation: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
