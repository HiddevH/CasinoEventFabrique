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

    # Set Azure SDK logger levels to WARNING to suppress INFO logs
    logging.getLogger("azure.eventhub").setLevel(logging.WARNING)
    logging.getLogger("azure.identity").setLevel(logging.WARNING)
    logging.getLogger("azure.core.pipeline.policies.http_logging_policy").setLevel(logging.WARNING)


def parse_args():
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Run casino player simulation"
    )
    
    # Output options
    parser.add_argument(
        "--output-file", "-o",
        type=str,
        help="Path to output file for events (JSON format)"
    )
    
    # Event Hub connection options
    parser.add_argument(
        "--connection-string", "-c",
        type=str,
        help="Azure Event Hub connection string"
    )
    
    parser.add_argument(
        "--eventhub-name", "-n",
        type=str,
        help="Name of the Azure Event Hub"
    )
    
    # Identity-based authentication options
    parser.add_argument(
        "--eventhub-namespace",
        type=str,
        help="Event Hub fully qualified namespace (e.g., 'mynamespace.servicebus.windows.net')"
    )
    
    parser.add_argument(
        "--use-managed-identity",
        action="store_true",
        help="Use Azure Managed Identity for authentication"
    )
    
    parser.add_argument(
        "--use-default-credential",
        action="store_true",
        help="Use Azure DefaultAzureCredential for authentication"
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
    
    return args


def main():
    """Main entry point for the casino CLI."""
    args = parse_args()
    
    # Set up logging
    setup_logging(args.verbose) # Use the setup_logging function
    logger = logging.getLogger("casino_cli")
    
    # Determine the publisher based on arguments
    publisher = None
    
    if args.connection_string:
        # Connection string authentication
        if not args.eventhub_name:
            logger.error("--eventhub-name is required when using --connection-string")
            return 1
            
        publisher = EventHubPublisher(
            connection_string=args.connection_string,
            eventhub_name=args.eventhub_name
        )
        logger.info("Using Event Hub with connection string authentication")
        
    elif args.eventhub_namespace and args.eventhub_name:
        # Identity-based authentication
        credential = None
        
        if args.use_managed_identity:
            try:
                from azure.identity import ManagedIdentityCredential
                credential = ManagedIdentityCredential()
                logger.info("Using Managed Identity credential")
            except ImportError:
                logger.error("Azure Identity package not installed. Install with: pip install azure-identity")
                return 1
                
        elif args.use_default_credential:
            try:
                from azure.identity import DefaultAzureCredential
                credential = DefaultAzureCredential()
                logger.info("Using Default Azure credential")
            except ImportError:
                logger.error("Azure Identity package not installed. Install with: pip install azure-identity")
                return 1
        else:
            # Default to DefaultAzureCredential if no specific credential is specified
            try:
                from azure.identity import DefaultAzureCredential
                credential = DefaultAzureCredential()
                logger.info("Using Default Azure credential (default)")
            except ImportError:
                logger.error("Azure Identity package not installed. Install with: pip install azure-identity")
                return 1
        
        publisher = EventHubPublisher(
            eventhub_name=args.eventhub_name,
            fully_qualified_namespace=args.eventhub_namespace,
            credential=credential
        )
        logger.info("Using Event Hub with identity-based authentication")
        
    elif args.output_file:
        # File output
        publisher = FileEventStore(args.output_file)
        logger.info(f"Using file output: {args.output_file}")
        
    else:
        # Default to file output
        default_file = "casino_events.json"
        publisher = FileEventStore(default_file)
        logger.info(f"No publisher specified, using default file output: {default_file}")
    
    try:
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
