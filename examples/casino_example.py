#!/usr/bin/env python
"""Example of using the casino simulation programmatically."""

import os
import logging

from casinoeventfabrique import (
    CasinoSimulation,
    FileEventStore,
    PlayerProfileType,
)

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("casino_example")

# Create output directory if it doesn't exist
os.makedirs("output", exist_ok=True)

# Create a file store for the events
event_store = FileEventStore("output/casino_simulation_output.json")

# Create the simulation with a mix of player types
simulation = CasinoSimulation(
    publisher=event_store,
    num_normal_players=20,         # Regular players
    num_high_roller_players=3,     # Big spenders
    num_occasional_players=5,      # Casual players
    num_addict_players=2,          # Players showing addictive patterns
    num_bonus_hunter_players=1,    # Players hunting for bonuses
    num_fraudster_players=2,       # Players showing fraudulent behavior
    simulation_duration=300,       # Run for 5 minutes
    thread_count=30,               # Use 30 threads
    event_delay=0.05               # Small delay between events
)

logger.info("Starting casino simulation...")

# Start the simulation
simulation.start()

# Report results
logger.info(f"Simulation complete!")
logger.info(f"Generated {simulation.total_events_generated} events")
logger.info(f"Stored {simulation.total_events_sent} events")
logger.info(f"Output file: {os.path.abspath('output/casino_simulation_output.json')}")
