# CasinoEventFabrique

A Python project using the UV package manager.

## Setup

This project uses [UV](https://github.com/astral-sh/uv) for dependency management.

### Create a virtual environment

```bash
uv venv
```

### Activate the virtual environment

```bash
source .venv/bin/activate  # On Unix/macOS
```

### Install dependencies

```bash
uv pip install -e .  # Install the package in development mode
uv pip install -e ".[dev]"  # Install development dependencies
```

## Development

- Run tests: `pytest`
- Format code: `black .`
- Sort imports: `isort .`
- Lint code: `ruff .`
- Type checking: `mypy .`

## Casino Simulation Functionality

CasinoEventFabrique includes a casino player simulation system that can generate realistic casino player events, including both normal player behavior and various types of abnormal behaviors like fraudulent or addictive patterns.

### Installation

To use the casino simulation, install the package with the required dependencies:

```bash
uv pip install -e .
```

### Command-line Usage

The package provides a command-line tool for casino simulation:

```bash
# Output to a JSON file
casinoeventfabrique-casino --output-file events/casino_events.json --duration 300

# Output to Azure Event Hub
casinoeventfabrique-casino --connection-string "YOUR_EVENT_HUB_CONNECTION_STRING" \
                      --eventhub-name "YOUR_EVENT_HUB_NAME" \
                      --addict-players 2 \
                      --fraudster-players 3 \
                      --duration 3600
```

### Options

- `--output-file`, `-o`: Path to the output file for events (JSON format)
- `--connection-string`, `-c`: Azure Event Hub connection string
- `--eventhub-name`, `-n`: Name of the Azure Event Hub (required with --connection-string)
- `--normal-players`: Number of normal players (default: 40)
- `--high-roller-players`: Number of high-roller players (default: 3)
- `--occasional-players`: Number of occasional players (default: 4)
- `--addict-players`: Number of players with addictive behavior (default: 1)
- `--bonus-hunter-players`: Number of bonus hunter players (default: 1)
- `--fraudster-players`: Number of fraudulent players (default: 1)
- `--duration`, `-d`: Duration of simulation in seconds (default: 3600 = 1 hour)
- `--threads`, `-t`: Number of threads for parallel simulation (default: 50)
- `--event-delay`: Delay between event batches in seconds (default: 0.05)
- `--verbose`, `-v`: Enable verbose logging

### Player Profiles

The simulator includes several player profile types:

1. **Normal**: Standard players with balanced deposits and withdrawals
2. **High Roller**: Players who bet large amounts and play premium games
3. **Occasional**: Casual players who play infrequently with small bets
4. **Addictive**: Players showing signs of addiction (high frequency, long sessions, rare withdrawals)
5. **Bonus Hunter**: Players who primarily play during bonus periods
6. **Fraudster**: Players with suspicious deposit/withdrawal patterns

### Using the Library in Custom Scripts

You can also use the casino simulation programmatically in your custom Python scripts:

```python
from casinoeventfabrique import CasinoSimulation, FileEventStore

# Create a file store for the events
event_store = FileEventStore("events/casino_output.json")

# Create the simulation
simulation = CasinoSimulation(
    publisher=event_store,
    num_normal_players=20,
    num_high_roller_players=3,
    num_occasional_players=5,
    num_addict_players=2,
    num_bonus_hunter_players=1, 
    num_fraudster_players=2,
    simulation_duration=1800,  # 30 minutes
    thread_count=50
)

# Start the simulation
simulation.start()

print(f"Generated {simulation.total_events_generated} events")
print(f"Stored {simulation.total_events_sent} events")
```

## Event Replay Functionality

CasinoEventFabrique includes a powerful event replay system that can stream events from a CSV file to an Azure Event Hub, simulating a real-time data stream.

### Installation

To use the replay functionality, install the package with the required dependencies:

```bash
uv pip install -e .
```

For ADX (Azure Data Explorer) support, install the optional dependencies:

```bash
uv pip install -e ".[adx]"
```

### Command-line Usage

The package provides a command-line tool for replaying events:

```bash
casinoeventfabrique-replay --csv-file data/pseudo_export.csv \
                    --connection-string "YOUR_EVENT_HUB_CONNECTION_STRING" \
                    --eventhub-name "YOUR_EVENT_HUB_NAME" \
                    --replay-delay 0.1
```

### Options

- `--csv-file`, `-f`: Path to the CSV file containing event data (required)
- `--connection-string`, `-c`: Azure Event Hub connection string (required)
- `--eventhub-name`, `-n`: Name of the Azure Event Hub (required)
- `--timestamp-field`, `-t`: Name of the timestamp field in the CSV (default: "timestamp")
- `--state-file`, `-s`: Path to the state file (default: "state.json")
- `--reset-state`: Reset replay state (replay from beginning)
- `--replay-delay`, `-d`: Delay between events in seconds (default: 0.1)
- `--limit`, `-l`: Maximum number of events to replay
- `--jitter`, `-j`: Apply jitter to amount fields for more realistic simulation
- `--jitter-fields`: Fields to apply jitter to (if not specified, applied to all numeric fields)
- `--jitter-percentage`: Percentage of jitter to apply (default: 0.05 = ±5%)
- `--partition-key-field`: Field to use as partition key (default: "pseudo_player_id")
- `--verbose`, `-v`: Enable verbose logging

### Using the Library in Custom Scripts

You can also use the replay functionality programmatically in your custom Python scripts:

```python
from casinoeventfabrique import CSVEventReader, EventHubPublisher, EventReplay

# Create event reader
reader = CSVEventReader(
    file_path="data/pseudo_export.csv",
    timestamp_field="timestamp"
)

# Create event publisher
publisher = EventHubPublisher(
    connection_string="YOUR_EVENT_HUB_CONNECTION_STRING",
    eventhub_name="YOUR_EVENT_HUB_NAME"
)

# Create replay configuration
replay = EventReplay(
    event_reader=reader,
    event_publisher=publisher,
    replay_delay=0.1,
    apply_jitter=True
)

# Start the replay
events_sent, last_timestamp = replay.replay(limit=1000)
print(f"Replayed {events_sent} events. Last timestamp: {last_timestamp}")
```

### State Management

The replay system maintains a state file (default: `state.json`) that tracks the last timestamp processed. When restarting a replay, events with timestamps less than or equal to the last processed timestamp will be skipped. This prevents duplicate events from being sent.

To force a replay from the beginning, use the `--reset-state` flag or set `reset_state=True` in the `replay()` method.
