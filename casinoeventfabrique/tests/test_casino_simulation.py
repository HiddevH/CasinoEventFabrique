"""Tests for casino simulation."""

import json
import os
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from casinoeventfabrique.casino_simulation import (
    CasinoPlayer,
    CasinoSimulation,
    FileEventStore,
    GameType,
    PlayerProfile,
    PlayerProfileType,
    PLAYER_PROFILES
)


class TestPlayerProfile:
    """Test the PlayerProfile class."""
    
    def test_player_profile_creation(self):
        """Test creating a player profile."""
        profile = PlayerProfile(
            profile_type=PlayerProfileType.NORMAL,
            min_deposit=50.0,
            max_deposit=200.0,
            deposit_frequency=0.1
        )
        
        assert profile.profile_type == PlayerProfileType.NORMAL
        assert profile.min_deposit == 50.0
        assert profile.max_deposit == 200.0
        assert profile.deposit_frequency == 0.1
        
    def test_default_profiles_exist(self):
        """Test that default profiles exist."""
        for profile_type in PlayerProfileType:
            assert profile_type in PLAYER_PROFILES
            assert isinstance(PLAYER_PROFILES[profile_type], PlayerProfile)


class TestCasinoPlayer:
    """Test the CasinoPlayer class."""
    
    def test_player_creation(self):
        """Test creating a player."""
        profile = PLAYER_PROFILES[PlayerProfileType.NORMAL]
        player = CasinoPlayer("test_player_1", profile, 100.0)
        
        assert player.player_id == "test_player_1"
        assert player.profile == profile
        assert player.balance == 100.0
        
    def test_deposit(self):
        """Test player deposit."""
        profile = PlayerProfile(
            profile_type=PlayerProfileType.NORMAL,
            min_deposit=50.0,
            max_deposit=100.0
        )
        player = CasinoPlayer("test_player_2", profile, 0.0)
        
        event = player.deposit()
        
        assert event["event_type"] == "deposit"
        assert event["player_id"] == "test_player_2"
        assert 50.0 <= event["amount"] <= 100.0
        assert event["balance"] == player.balance
        assert player.balance > 0.0
        
    def test_withdraw_success(self):
        """Test successful withdrawal."""
        profile = PlayerProfile(
            profile_type=PlayerProfileType.NORMAL,
            min_withdrawal=20.0,
            max_withdrawal=50.0
        )
        player = CasinoPlayer("test_player_3", profile, 100.0)
        initial_balance = player.balance
        
        event = player.withdraw()
        
        assert event is not None
        assert event["event_type"] == "withdrawal"
        assert event["player_id"] == "test_player_3"
        assert 20.0 <= event["amount"] <= 50.0
        assert event["balance"] == player.balance
        assert player.balance < initial_balance
        
    def test_withdraw_insufficient_funds(self):
        """Test withdrawal with insufficient funds."""
        profile = PlayerProfile(
            profile_type=PlayerProfileType.NORMAL,
            min_withdrawal=50.0,
            max_withdrawal=100.0
        )
        player = CasinoPlayer("test_player_4", profile, 20.0)
        
        event = player.withdraw()
        
        assert event is None
        assert player.balance == 20.0
        
    def test_play_game(self):
        """Test playing a game."""
        profile = PlayerProfile(
            profile_type=PlayerProfileType.NORMAL,
            min_bet=10.0,
            max_bet=20.0,
            games=[GameType.SLOTS]
        )
        player = CasinoPlayer("test_player_5", profile, 100.0)
        initial_balance = player.balance
        
        event = player.play_game()
        
        assert event["event_type"] == "game_play"
        assert event["player_id"] == "test_player_5"
        assert event["game"] == GameType.SLOTS.value
        assert 10.0 <= event["bet_amount"] <= 20.0
        assert event["result"] in ["win", "loss"]
        assert event["balance"] == player.balance
        
        # Check that the balance changed
        if event["result"] == "win":
            assert event["win_amount"] > 0
        else:
            assert event["win_amount"] == 0
            
    def test_play_game_no_funds(self):
        """Test playing a game with no funds."""
        profile = PlayerProfile(
            profile_type=PlayerProfileType.NORMAL,
            min_bet=10.0,
            max_bet=20.0,
            games=[GameType.SLOTS]
        )
        player = CasinoPlayer("test_player_6", profile, 0.0)
        
        event = player.play_game()
        
        assert event["event_type"] == "game_attempt"
        assert event["status"] == "failed"
        assert event["reason"] == "insufficient_funds"
        
    def test_simulate_session(self):
        """Test simulating a player session."""
        profile = PlayerProfile(
            profile_type=PlayerProfileType.NORMAL,
            min_deposit=50.0,
            max_deposit=100.0,
            deposit_frequency=1.0,  # Always deposit
            min_withdrawal=20.0,
            max_withdrawal=50.0,
            withdrawal_frequency=0.0,  # Never withdraw
            games=[GameType.SLOTS],
            min_bet=10.0,
            max_bet=20.0,
            play_frequency=1.0,  # Always play
            avg_session_length=5,
            session_length_variance=0  # Fixed length
        )
        player = CasinoPlayer("test_player_7", profile, 0.0)
        
        events = player.simulate_session()
        
        # Should have at least a deposit and some game plays
        assert len(events) >= 2
        assert events[0]["event_type"] == "deposit"
        
        # Check that all events have the correct player_id
        for event in events:
            assert event["player_id"] == "test_player_7"


class TestFileEventStore:
    """Test the FileEventStore class."""
    
    def test_store_events(self):
        """Test storing events in a file."""
        with tempfile.TemporaryDirectory() as tmpdirname:
            file_path = os.path.join(tmpdirname, "test_events.json")
            store = FileEventStore(file_path)
            
            events = [
                {"id": 1, "name": "test1"},
                {"id": 2, "name": "test2"}
            ]
            
            count = store.store_events(events)
            
            assert count == 2
            assert os.path.exists(file_path)
            
            with open(file_path, "r") as f:
                stored_events = json.load(f)
                assert len(stored_events) == 2
                assert stored_events[0]["id"] == 1
                assert stored_events[1]["id"] == 2


class TestCasinoSimulation:
    """Test the CasinoSimulation class."""
    
    def test_simulation_creation(self):
        """Test creating a simulation."""
        with tempfile.TemporaryDirectory() as tmpdirname:
            file_path = os.path.join(tmpdirname, "test_events.json")
            store = FileEventStore(file_path)
            
            sim = CasinoSimulation(
                publisher=store,
                num_normal_players=2,
                num_high_roller_players=1,
                num_occasional_players=1,
                num_addict_players=0,
                num_bonus_hunter_players=0,
                num_fraudster_players=0,
                simulation_duration=1,
                thread_count=2
            )
            
            assert len(sim.players) == 4
            
    @patch('eventfabricue.casino_simulation.threading.Thread')
    def test_simulation_start(self, mock_thread):
        """Test starting a simulation."""
        with tempfile.TemporaryDirectory() as tmpdirname:
            file_path = os.path.join(tmpdirname, "test_events.json")
            store = FileEventStore(file_path)
            
            # Create a mock thread that sets the simulation duration to 0.1 seconds
            mock_thread_instance = MagicMock()
            mock_thread.return_value = mock_thread_instance
            
            sim = CasinoSimulation(
                publisher=store,
                num_normal_players=2,
                num_high_roller_players=0,
                simulation_duration=0.1,  # Very short duration
                thread_count=1
            )
            
            sim.start()
            
            # Verify thread was started
            assert mock_thread.called
            assert mock_thread_instance.start.called
