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

The package provides a command-line tool for casino simulation with multiple authentication options:

#### Using Connection String (traditional method)

```bash
casinoeventfabrique-casino --connection-string "YOUR_EVENT_HUB_CONNECTION_STRING" \
                          --eventhub-name "YOUR_EVENT_HUB_NAME" \
                          --duration 300
```

#### Using Identity-based Authentication (recommended)

```bash
# Using Managed Identity (when running on Azure resources)
casinoeventfabrique-casino --eventhub-namespace "mynamespace.servicebus.windows.net" \
                          --eventhub-name "YOUR_EVENT_HUB_NAME" \
                          --use-managed-identity \
                          --duration 300

# Using Default Azure Credential (tries multiple authentication methods)
casinoeventfabrique-casino --eventhub-namespace "mynamespace.servicebus.windows.net" \
                          --eventhub-name "YOUR_EVENT_HUB_NAME" \
                          --use-default-credential \
                          --duration 300

# Default behavior (uses DefaultAzureCredential automatically)
casinoeventfabrique-casino --eventhub-namespace "mynamespace.servicebus.windows.net" \
                          --eventhub-name "YOUR_EVENT_HUB_NAME" \
                          --duration 300
```

#### Output to File

```bash
casinoeventfabrique-casino --output-file events/casino_events.json --duration 300
```

### Authentication Options

- **Connection String**: Traditional method using a connection string with embedded credentials
- **Managed Identity**: Uses Azure Managed Identity when running on Azure resources (VMs, App Service, etc.)
- **Default Azure Credential**: Tries multiple authentication methods in order:
  1. Environment variables
  2. Managed Identity
  3. Visual Studio Code
  4. Azure CLI
  5. Azure PowerShell
  6. Interactive browser

### Options

- `--output-file`, `-o`: Path to the output file for events (JSON format)
- `--connection-string`, `-c`: Azure Event Hub connection string
- `--eventhub-name`, `-n`: Name of the Azure Event Hub
- `--eventhub-namespace`: Event Hub fully qualified namespace (e.g., 'mynamespace.servicebus.windows.net')
- `--use-managed-identity`: Use Azure Managed Identity for authentication
- `--use-default-credential`: Use Azure DefaultAzureCredential for authentication

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
