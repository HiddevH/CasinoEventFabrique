#!/usr/bin/env python
"""Example of using the casino simulation with identity-based authentication."""

import os
import logging
from azure.identity import DefaultAzureCredential, ManagedIdentityCredential

from casinoeventfabrique import (
    CasinoSimulation,
    EventHubPublisher,
    PlayerProfileType,
)

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("casino_identity_example")

def main():
    """Run casino simulation with identity-based authentication."""
    
    # Configuration - replace with your values
    eventhub_namespace = ""
    eventhub_name = ""
    
    # Choose your authentication method
    
    # Option 1: Use DefaultAzureCredential (recommended)
    credential = DefaultAzureCredential()
    
    # Option 2: Use ManagedIdentityCredential (when running on Azure)
    # credential = ManagedIdentityCredential()
    
    # Option 3: Use specific credential types
    # from azure.identity import EnvironmentCredential, AzureCliCredential
    # credential = EnvironmentCredential()  # Uses environment variables
    # credential = AzureCliCredential()     # Uses Azure CLI login
    
    # Create Event Hub publisher with identity-based authentication
    with EventHubPublisher(
        eventhub_name=eventhub_name,
        fully_qualified_namespace=eventhub_namespace,
        credential=credential
    ) as publisher:
        
        # Create the simulation
        simulation = CasinoSimulation(
            publisher=publisher,
            num_normal_players=10,
            num_high_roller_players=2,
            num_occasional_players=3,
            num_addict_players=1,
            num_bonus_hunter_players=1,
            num_fraudster_players=1,
            simulation_duration=60,  # 1 minute for example
            thread_count=10
        )
        
        logger.info("Starting casino simulation with identity-based authentication...")
        simulation.start()
        logger.info("Casino simulation completed.")


if __name__ == "__main__":
    main()