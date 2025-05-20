"""Casino player simulation for CasinoEventFabrique."""

import json
import logging
import random
import threading
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Dict, Any, List, Optional, Set, Tuple, Union

try:
    from azure.eventhub import EventHubProducerClient, EventData
    from azure.eventhub.exceptions import EventHubError
    HAS_EVENTHUB = True
except ImportError:
    HAS_EVENTHUB = False

from .core import Event


# Define casino game types
class GameType(Enum):
    """Types of casino games."""
    SLOTS = "slots"
    BLACKJACK = "blackjack"
    ROULETTE = "roulette"
    POKER = "poker"
    BACCARAT = "baccarat"


class PlayerProfileType(Enum):
    """Types of player profiles."""
    NORMAL = "normal"
    HIGH_ROLLER = "high_roller"
    OCCASIONAL = "occasional"
    ADDICT = "addictive"  # Shows signs of addiction, high frequency at odd hours
    BONUS_HUNTER = "bonus_hunter"  # Only plays during bonus periods
    FRAUDSTER = "fraudster"  # Shows suspicious deposit/withdrawal patterns


@dataclass
class PlayerProfile:
    """Profile defining player behavior."""
    profile_type: PlayerProfileType
    
    # Deposit behavior
    min_deposit: float = 10.0
    max_deposit: float = 100.0
    deposit_frequency: float = 0.1  # probability of making a deposit in a given session
    
    # Withdrawal behavior
    min_withdrawal: float = 20.0
    max_withdrawal: float = 200.0
    withdrawal_frequency: float = 0.05  # probability of making a withdrawal in a given session
    
    # Playing behavior
    games: List[GameType] = field(default_factory=list)
    min_bet: float = 1.0
    max_bet: float = 10.0
    play_frequency: float = 0.7  # probability of playing in a given cycle
    
    # Session behavior
    avg_session_length: int = 20  # average number of betting actions per session
    session_length_variance: int = 10  # variance in session length


# Define default player profiles
PLAYER_PROFILES = {
    PlayerProfileType.NORMAL: PlayerProfile(
        profile_type=PlayerProfileType.NORMAL,
        min_deposit=50.0,
        max_deposit=200.0,
        deposit_frequency=0.1,
        min_withdrawal=50.0,
        max_withdrawal=300.0,
        withdrawal_frequency=0.05,
        games=[GameType.SLOTS, GameType.BLACKJACK, GameType.ROULETTE],
        min_bet=1.0,
        max_bet=10.0,
        play_frequency=0.7,
        avg_session_length=30,
        session_length_variance=10
    ),
    
    PlayerProfileType.HIGH_ROLLER: PlayerProfile(
        profile_type=PlayerProfileType.HIGH_ROLLER,
        min_deposit=500.0,
        max_deposit=10000.0,
        deposit_frequency=0.15,
        min_withdrawal=1000.0,
        max_withdrawal=15000.0,
        withdrawal_frequency=0.1,
        games=[GameType.BLACKJACK, GameType.BACCARAT, GameType.POKER],
        min_bet=100.0,
        max_bet=1000.0,
        play_frequency=0.6,
        avg_session_length=50,
        session_length_variance=20
    ),
    
    PlayerProfileType.OCCASIONAL: PlayerProfile(
        profile_type=PlayerProfileType.OCCASIONAL,
        min_deposit=20.0,
        max_deposit=100.0,
        deposit_frequency=0.05,
        min_withdrawal=30.0,
        max_withdrawal=150.0,
        withdrawal_frequency=0.1,
        games=[GameType.SLOTS, GameType.ROULETTE],
        min_bet=0.5,
        max_bet=5.0,
        play_frequency=0.3,
        avg_session_length=15,
        session_length_variance=5
    ),
    
    PlayerProfileType.ADDICT: PlayerProfile(
        profile_type=PlayerProfileType.ADDICT,
        min_deposit=100.0,
        max_deposit=500.0,
        deposit_frequency=0.3,  # Much higher deposit frequency
        min_withdrawal=50.0,
        max_withdrawal=200.0,
        withdrawal_frequency=0.02,  # Much lower withdrawal frequency
        games=[GameType.SLOTS],  # Often fixates on a single game type
        min_bet=5.0,
        max_bet=50.0,
        play_frequency=0.9,  # Very high play frequency
        avg_session_length=100,  # Very long sessions
        session_length_variance=30
    ),
    
    PlayerProfileType.BONUS_HUNTER: PlayerProfile(
        profile_type=PlayerProfileType.BONUS_HUNTER,
        min_deposit=50.0,
        max_deposit=200.0,
        deposit_frequency=0.2,
        min_withdrawal=100.0,
        max_withdrawal=500.0,
        withdrawal_frequency=0.2,  # High withdrawal frequency after bonus clearing
        games=[GameType.SLOTS, GameType.BLACKJACK],
        min_bet=1.0,
        max_bet=20.0,
        play_frequency=0.8,
        avg_session_length=40,
        session_length_variance=10
    ),
    
    PlayerProfileType.FRAUDSTER: PlayerProfile(
        profile_type=PlayerProfileType.FRAUDSTER,
        min_deposit=200.0,
        max_deposit=1000.0,
        deposit_frequency=0.4,
        min_withdrawal=190.0,  # Withdraws almost exactly what was deposited
        max_withdrawal=950.0,
        withdrawal_frequency=0.35,  # Very high withdrawal frequency
        games=[GameType.BLACKJACK, GameType.ROULETTE],
        min_bet=5.0,
        max_bet=10.0,
        play_frequency=0.4,  # Low play frequency
        avg_session_length=10,  # Short sessions
        session_length_variance=5
    )
}


class CasinoPlayer:
    """Simulated casino player."""
    
    def __init__(
        self,
        player_id: str,
        profile: PlayerProfile,
        initial_balance: float = 0.0
    ):
        """Initialize a casino player.
        
        Args:
            player_id: Unique identifier for the player
            profile: Player behavior profile
            initial_balance: Starting balance for the player
        """
        self.player_id = player_id
        self.profile = profile
        self.balance = initial_balance
        self.session_count = 0
        self.total_deposits = 0.0
        self.total_withdrawals = 0.0
        self.total_bets = 0.0
        self.total_wins = 0.0
        self.total_losses = 0.0
        self.last_action_timestamp = None
        self.logger = logging.getLogger(f"{__name__}.player.{player_id}")
        
    def deposit(self) -> Dict[str, Any]:
        """Simulate a deposit.
        
        Returns:
            Deposit event data
        """
        amount = random.uniform(self.profile.min_deposit, self.profile.max_deposit)
        amount = round(amount, 2)
        
        self.balance += amount
        self.total_deposits += amount
        self.last_action_timestamp = datetime.now()
        
        self.logger.debug(f"Player {self.player_id} deposited ${amount:.2f}")
        
        return {
            "event_type": "deposit",
            "timestamp": self.last_action_timestamp.isoformat(),
            "player_id": self.player_id,
            "amount": amount,
            "balance": self.balance,
            "profile_type": self.profile.profile_type.value
        }
    
    def withdraw(self) -> Optional[Dict[str, Any]]:
        """Simulate a withdrawal.
        
        Returns:
            Withdrawal event data if successful, None otherwise
        """
        # First check if balance meets minimum withdrawal requirement
        if self.balance < self.profile.min_withdrawal:
            self.logger.debug(f"Player {self.player_id} attempted withdrawal but balance too low")
            return None
            
        # Calculate withdrawal amount between min and max limits, but not exceeding balance
        amount = min(random.uniform(self.profile.min_withdrawal, self.profile.max_withdrawal), self.balance)
        amount = round(amount, 2)
        
        self.balance -= amount
        self.total_withdrawals += amount
        self.last_action_timestamp = datetime.now()
        
        self.logger.debug(f"Player {self.player_id} withdrew ${amount:.2f}")
        
        return {
            "event_type": "withdrawal",
            "timestamp": self.last_action_timestamp.isoformat(),
            "player_id": self.player_id,
            "amount": amount,
            "balance": self.balance,
            "profile_type": self.profile.profile_type.value
        }
    
    def play_game(self) -> Dict[str, Any]:
        """Simulate playing a game.
        
        Returns:
            Game play event data
        """
        # Randomly select a game from the player's preferred games
        game = random.choice(self.profile.games)
        
        # Determine bet amount
        bet_amount = random.uniform(self.profile.min_bet, min(self.profile.max_bet, self.balance))
        bet_amount = round(bet_amount, 2)
        
        # Adjust if the balance is too low
        if bet_amount > self.balance:
            bet_amount = self.balance
        
        if bet_amount <= 0:
            self.logger.debug(f"Player {self.player_id} attempted to play but has no funds")
            return {
                "event_type": "game_attempt",
                "timestamp": datetime.now().isoformat(),
                "player_id": self.player_id,
                "game": game.value,
                "status": "failed",
                "reason": "insufficient_funds",
                "balance": self.balance,
                "profile_type": self.profile.profile_type.value
            }
        
        # Determine outcome based on the game type
        win_probability = self._get_win_probability(game)
        outcome = random.random() < win_probability
        
        # Calculate win/loss amount
        if outcome:
            win_amount = bet_amount * self._get_win_multiplier(game)
            win_amount = round(win_amount, 2)
            self.balance += win_amount - bet_amount
            self.total_wins += win_amount
            result = "win"
        else:
            self.balance -= bet_amount
            self.total_losses += bet_amount
            win_amount = 0
            result = "loss"
        
        self.total_bets += bet_amount
        self.last_action_timestamp = datetime.now()
        
        self.logger.debug(
            f"Player {self.player_id} played {game.value} and "
            f"{result} (bet: ${bet_amount:.2f}, win: ${win_amount:.2f})"
        )
        
        return {
            "event_type": "game_play",
            "timestamp": self.last_action_timestamp.isoformat(),
            "player_id": self.player_id,
            "game": game.value,
            "bet_amount": bet_amount,
            "result": result,
            "win_amount": win_amount,
            "net_change": win_amount - bet_amount if outcome else -bet_amount,
            "balance": self.balance,
            "profile_type": self.profile.profile_type.value
        }
    
    def simulate_session(self) -> List[Dict[str, Any]]:
        """Simulate a player session.
        
        Returns:
            List of event data from the session
        """
        self.session_count += 1
        events = []
        
        self.logger.debug(f"Player {self.player_id} starting session {self.session_count}")
        
        # Possibly deposit money at the start of a session
        if random.random() < self.profile.deposit_frequency:
            events.append(self.deposit())
            
        # Determine session length with some randomness
        session_length = max(1, int(random.gauss(
            self.profile.avg_session_length, 
            self.profile.session_length_variance
        )))
        
        # Simulate games in the session
        for _ in range(session_length):
            if self.balance <= 0:
                # Player has no money, might deposit more
                if random.random() < self.profile.deposit_frequency * 2:  # Higher chance when broke
                    events.append(self.deposit())
                else:
                    break  # End session if no money and no deposit
            
            # Play a game with some probability
            if random.random() < self.profile.play_frequency:
                events.append(self.play_game())
            
            # Small pause between actions
            time.sleep(random.uniform(0.01, 0.1))
        
        # Possibly withdraw money at the end of a session
        if self.balance > self.profile.min_withdrawal and random.random() < self.profile.withdrawal_frequency:
            withdrawal_event = self.withdraw()
            if withdrawal_event:
                events.append(withdrawal_event)
        
        self.logger.debug(f"Player {self.player_id} finished session with balance ${self.balance:.2f}")
        
        return events
    
    def _get_win_probability(self, game: GameType) -> float:
        """Get the win probability based on the game type.
        
        Args:
            game: Type of game
            
        Returns:
            Win probability (0.0 to 1.0)
        """
        # Different games have different probabilities
        if game == GameType.SLOTS:
            return 0.35
        elif game == GameType.BLACKJACK:
            return 0.48
        elif game == GameType.ROULETTE:
            return 0.47
        elif game == GameType.POKER:
            return 0.40
        elif game == GameType.BACCARAT:
            return 0.45
        else:
            return 0.40
    
    def _get_win_multiplier(self, game: GameType) -> float:
        """Get the win multiplier based on the game type.
        
        Args:
            game: Type of game
            
        Returns:
            Win multiplier
        """
        # Different games have different payouts
        if game == GameType.SLOTS:
            # Higher variance for slots with potential big wins
            return random.choice([1.5, 1.8, 2.0, 2.5, 3.0, 5.0, 10.0, 100.0])
        elif game == GameType.BLACKJACK:
            return 2.0
        elif game == GameType.ROULETTE:
            # Different bet types
            return random.choice([1.5, 2.0, 3.0, 36.0])
        elif game == GameType.POKER:
            return random.uniform(1.5, 5.0)
        elif game == GameType.BACCARAT:
            return 1.95
        else:
            return 2.0


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
            
    def send_events(self, events: List[Dict[str, Any]], partition_key_field: str = "player_id") -> int:
        """Send multiple events to Event Hub.
        
        Args:
            events: List of event data to send
            partition_key_field: Field to use as partition key
            
        Returns:
            Number of events sent successfully
        """
        if not self.producer:
            raise RuntimeError("Publisher not initialized. Use with context manager.")
            
        try:
            event_batches = {}  # Batches by partition key
            sent_count = 0
            
            for event_data in events:
                partition_key = str(event_data.get(partition_key_field, ""))
                
                if partition_key not in event_batches:
                    event_batches[partition_key] = self.producer.create_batch(partition_key=partition_key)
                    
                event_data_encoded = EventData(body=json.dumps(event_data).encode('utf-8'))
                
                # If we can't add the event to the current batch, send it and create a new one
                if not event_batches[partition_key].add(event_data_encoded):
                    self.producer.send_batch(event_batches[partition_key])
                    sent_count += len(event_batches[partition_key])
                    
                    # Create a new batch for this partition key
                    event_batches[partition_key] = self.producer.create_batch(partition_key=partition_key)
                    
                    # Try adding the event to the new batch
                    if not event_batches[partition_key].add(event_data_encoded):
                        self.logger.warning(f"Event too large to fit in batch, skipping: {event_data}")
                        continue
            
            # Send any remaining batches
            for partition_key, batch in event_batches.items():
                if len(batch) > 0:
                    self.producer.send_batch(batch)
                    sent_count += len(batch)
                    
            return sent_count
            
        except EventHubError as e:
            self.logger.error(f"Error sending events to Event Hub: {e}")
            return 0


class FileEventStore:
    """Store events in a local file."""
    
    def __init__(self, file_path: str):
        """Initialize file event store.
        
        Args:
            file_path: Path to the event storage file
        """
        self.file_path = file_path
        self.logger = logging.getLogger(__name__)
        
        # Create directory if it doesn't exist
        Path(file_path).parent.mkdir(exist_ok=True, parents=True)
        
        # Create file if it doesn't exist
        if not Path(file_path).exists():
            with open(self.file_path, mode="w", encoding="utf-8") as f:
                f.write("[]")
    
    def store_events(self, events: List[Dict[str, Any]]) -> int:
        """Store events to file.
        
        Args:
            events: List of event data to store
            
        Returns:
            Number of events stored
        """
        try:
            # Read existing events
            with open(self.file_path, mode="r", encoding="utf-8") as f:
                try:
                    existing_events = json.load(f)
                except json.JSONDecodeError:
                    existing_events = []
            
            # Append new events
            existing_events.extend(events)
            
            # Write back to file
            with open(self.file_path, mode="w", encoding="utf-8") as f:
                json.dump(existing_events, f, indent=2)
                
            return len(events)
            
        except Exception as e:
            self.logger.error(f"Error storing events to file: {e}")
            return 0


class CasinoSimulation:
    """Simulate casino players and generate events."""
    
    def __init__(
        self,
        publisher: Optional[Union[EventHubPublisher, FileEventStore]] = None,
        num_normal_players: int = 40,
        num_high_roller_players: int = 3,
        num_occasional_players: int = 4,
        num_addict_players: int = 1,
        num_bonus_hunter_players: int = 1,
        num_fraudster_players: int = 1,
        simulation_duration: int = 3600,  # default 1 hour in seconds
        thread_count: int = 50,
        event_delay: float = 0.05,  # delay between event batches in seconds
        output_file: Optional[str] = "casino_events.json",  # File output if no publisher provided
    ):
        """Initialize casino simulation.
        
        Args:
            publisher: Event publisher (EventHub or File)
            num_normal_players: Number of normal players to simulate
            num_high_roller_players: Number of high roller players to simulate
            num_occasional_players: Number of occasional players to simulate
            num_addict_players: Number of addictive behavior players to simulate
            num_bonus_hunter_players: Number of bonus hunter players to simulate
            num_fraudster_players: Number of fraudulent players to simulate
            simulation_duration: Duration of simulation in seconds
            thread_count: Number of threads for parallel simulation
            event_delay: Delay between event batches in seconds
            output_file: Path to save events if no publisher provided
        """
        self.publisher = publisher
        self.simulation_duration = simulation_duration
        self.thread_count = min(thread_count, 100)  # Maximum 100 threads
        self.event_delay = event_delay
        self.output_file = output_file
        self.stop_event = threading.Event()
        self.players: List[CasinoPlayer] = []
        self.active_threads: List[threading.Thread] = []
        self.events_lock = threading.Lock()
        self.total_events_generated = 0
        self.total_events_sent = 0
        self.logger = logging.getLogger(__name__)
        
        # Initialize player distribution
        self._create_players(
            num_normal_players,
            num_high_roller_players,
            num_occasional_players,
            num_addict_players,
            num_bonus_hunter_players,
            num_fraudster_players
        )
        
        # Create file store if no publisher provided
        if self.publisher is None and output_file:
            self.publisher = FileEventStore(output_file)
    
    def _create_players(
        self,
        num_normal: int,
        num_high_roller: int,
        num_occasional: int,
        num_addict: int,
        num_bonus_hunter: int,
        num_fraudster: int
    ) -> None:
        """Create player instances based on the specified distribution.
        
        Args:
            num_normal: Number of normal players
            num_high_roller: Number of high roller players
            num_occasional: Number of occasional players
            num_addict: Number of addictive behavior players
            num_bonus_hunter: Number of bonus hunter players
            num_fraudster: Number of fraudulent players
        """
        player_counts = {
            PlayerProfileType.NORMAL: num_normal,
            PlayerProfileType.HIGH_ROLLER: num_high_roller,
            PlayerProfileType.OCCASIONAL: num_occasional,
            PlayerProfileType.ADDICT: num_addict,
            PlayerProfileType.BONUS_HUNTER: num_bonus_hunter,
            PlayerProfileType.FRAUDSTER: num_fraudster
        }
        
        # Create players of each type
        for profile_type, count in player_counts.items():
            for i in range(count):
                player_id = f"{profile_type.value}_{i}_{uuid.uuid4().hex[:8]}"
                profile = PLAYER_PROFILES[profile_type]
                
                # Give players an initial balance based on their profile
                initial_balance = random.uniform(
                    profile.min_deposit * 0.5, 
                    profile.max_deposit * 2
                )
                
                player = CasinoPlayer(player_id, profile, initial_balance)
                self.players.append(player)
        
        self.logger.info(f"Created {len(self.players)} players")
        
        # Log breakdown of player types
        for profile_type, count in player_counts.items():
            if count > 0:
                self.logger.info(f"- {count} {profile_type.value} players")
    
    def _player_thread(self, player_group: List[CasinoPlayer]) -> None:
        """Thread function for simulating a group of players.
        
        Args:
            player_group: List of players to simulate in this thread
        """
        events_buffer = []
        
        while not self.stop_event.is_set():
            # Randomly select a player from the group
            player = random.choice(player_group)
            
            # Simulate a session and collect events
            events = player.simulate_session()
            if events:
                events_buffer.extend(events)
            
            # If we have enough events, publish them
            if len(events_buffer) >= 10:  # Batch size
                self._publish_events(events_buffer)
                events_buffer = []
            
            # Random pause between player sessions
            time.sleep(random.uniform(0.1, 1.0))
        
        # Publish any remaining events
        if events_buffer:
            self._publish_events(events_buffer)
    
    def _publish_events(self, events: List[Dict[str, Any]]) -> None:
        """Publish events using the configured publisher.
        
        Args:
            events: List of events to publish
        """
        if not events:
            return
            
        with self.events_lock:
            self.total_events_generated += len(events)
            
            if self.publisher:
                if isinstance(self.publisher, EventHubPublisher):
                    sent_count = self.publisher.send_events(events)
                elif isinstance(self.publisher, FileEventStore):
                    sent_count = self.publisher.store_events(events)
                else:
                    self.logger.warning(f"Unknown publisher type: {type(self.publisher)}")
                    sent_count = 0
                    
                self.total_events_sent += sent_count
                
                if sent_count != len(events):
                    self.logger.warning(
                        f"Not all events were sent: {sent_count}/{len(events)}"
                    )
            else:
                # Just log if no publisher
                self.logger.info(f"Generated {len(events)} events (no publisher)")
    
    def start(self) -> None:
        """Start the casino simulation."""
        if not self.players:
            self.logger.error("No players defined for the simulation")
            return
        
        self.logger.info(f"Starting casino simulation with {len(self.players)} players on {self.thread_count} threads")
        
        # Distribute players across threads
        players_per_thread = max(1, len(self.players) // self.thread_count)
        player_groups = [
            self.players[i:i+players_per_thread]
            for i in range(0, len(self.players), players_per_thread)
        ]
        
        # If we have more threads than groups, adjust
        while len(player_groups) < self.thread_count and len(player_groups) > 0:
            # Split the largest group
            largest_idx = max(range(len(player_groups)), key=lambda i: len(player_groups[i]))
            group = player_groups[largest_idx]
            if len(group) <= 1:
                break  # Can't split further
                
            split_idx = len(group) // 2
            player_groups.append(group[split_idx:])
            player_groups[largest_idx] = group[:split_idx]
        
        # Create and start threads
        for i, player_group in enumerate(player_groups):
            thread = threading.Thread(
                target=self._player_thread,
                args=(player_group,),
                name=f"player-thread-{i}"
            )
            thread.daemon = True  # Make thread a daemon so it exits when the main thread exits
            thread.start()
            self.active_threads.append(thread)
            
        self.logger.info(f"Started {len(self.active_threads)} simulation threads")
        
        try:
            # Run for the specified duration
            start_time = time.time()
            while time.time() - start_time < self.simulation_duration:
                # Periodically log progress
                if int(time.time() - start_time) % 10 == 0:  # Every 10 seconds
                    with self.events_lock:
                        self.logger.info(
                            f"Simulation progress: {int(time.time() - start_time)}/{self.simulation_duration}s, "
                            f"Generated: {self.total_events_generated} events, "
                            f"Sent: {self.total_events_sent} events"
                        )
                        
                time.sleep(1)
                
        except KeyboardInterrupt:
            self.logger.info("Simulation interrupted")
            
        finally:
            # Signal threads to stop
            self.stop_event.set()
            
            # Wait for threads to finish (with timeout)
            for thread in self.active_threads:
                thread.join(timeout=1.0)
                
            with self.events_lock:
                self.logger.info(
                    f"Simulation complete. "
                    f"Generated {self.total_events_generated} events, "
                    f"Sent {self.total_events_sent} events"
                )
