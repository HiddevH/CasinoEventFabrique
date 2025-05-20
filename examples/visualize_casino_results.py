#!/usr/bin/env python
"""Visualize casino simulation results for CasinoEventFabrique."""

import json
import argparse
import os
from collections import Counter, defaultdict
from datetime import datetime
import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns


def parse_args():
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Visualize casino simulation results"
    )
    parser.add_argument(
        "--input-file",
        "-i",
        required=True,
        help="Path to the JSON file containing casino simulation events"
    )
    parser.add_argument(
        "--output-dir",
        "-o",
        default="visualization_output",
        help="Directory to save visualization files"
    )
    
    return parser.parse_args()


def load_events(file_path):
    """Load events from JSON file."""
    with open(file_path, "r") as f:
        return json.load(f)


def convert_to_dataframe(events):
    """Convert events to pandas DataFrame."""
    # Convert events to a DataFrame
    df = pd.DataFrame(events)
    
    # Convert timestamp strings to datetime
    if "timestamp" in df.columns:
        df["timestamp"] = pd.to_datetime(df["timestamp"])
        
    return df


def plot_event_types(df, output_dir):
    """Plot distribution of event types."""
    plt.figure(figsize=(10, 6))
    ax = sns.countplot(y=df["event_type"])
    ax.set_title("Distribution of Event Types")
    ax.set_xlabel("Count")
    ax.set_ylabel("Event Type")
    
    # Add count labels to bars
    for container in ax.containers:
        ax.bar_label(container)
        
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, "event_types.png"))
    plt.close()


def plot_profile_distribution(df, output_dir):
    """Plot distribution of player profiles."""
    plt.figure(figsize=(10, 6))
    ax = sns.countplot(y=df["profile_type"])
    ax.set_title("Distribution of Player Profiles")
    ax.set_xlabel("Count")
    ax.set_ylabel("Profile Type")
    
    # Add count labels to bars
    for container in ax.containers:
        ax.bar_label(container)
        
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, "profile_distribution.png"))
    plt.close()


def plot_game_distribution(df, output_dir):
    """Plot distribution of game types."""
    # Filter only game play events
    game_df = df[df["event_type"] == "game_play"]
    
    if len(game_df) == 0:
        print("No game play events found.")
        return
    
    plt.figure(figsize=(10, 6))
    ax = sns.countplot(y=game_df["game"])
    ax.set_title("Distribution of Game Types")
    ax.set_xlabel("Count")
    ax.set_ylabel("Game Type")
    
    # Add count labels to bars
    for container in ax.containers:
        ax.bar_label(container)
        
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, "game_distribution.png"))
    plt.close()


def plot_player_balances(df, output_dir):
    """Plot player balances over time."""
    # Get the final balance for each player
    player_balances = {}
    
    for _, row in df.iterrows():
        if "player_id" in row and "balance" in row:
            player_balances[row["player_id"]] = row["balance"]
    
    # Create a DataFrame for plotting
    balance_df = pd.DataFrame({
        "player_id": list(player_balances.keys()),
        "balance": list(player_balances.values())
    })
    
    if "profile_type" in df.columns:
        # Merge with profile information
        player_profiles = df.drop_duplicates(subset=["player_id"])[["player_id", "profile_type"]]
        balance_df = pd.merge(balance_df, player_profiles, on="player_id")
    
    plt.figure(figsize=(12, 8))
    
    if "profile_type" in balance_df.columns:
        ax = sns.boxplot(x="profile_type", y="balance", data=balance_df)
        ax.set_title("Player Balances by Profile Type")
        ax.set_xlabel("Profile Type")
    else:
        balance_df = balance_df.sort_values(by="balance", ascending=False)
        ax = sns.barplot(x="player_id", y="balance", data=balance_df.head(20))
        ax.set_title("Top 20 Player Balances")
        ax.set_xlabel("Player ID")
        plt.xticks(rotation=90)
        
    ax.set_ylabel("Balance")
        
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, "player_balances.png"))
    plt.close()


def plot_bet_amounts_by_profile(df, output_dir):
    """Plot bet amounts by profile type."""
    # Filter only game play events
    game_df = df[df["event_type"] == "game_play"]
    
    if len(game_df) == 0 or "bet_amount" not in game_df.columns:
        print("No bet amount data found.")
        return
    
    plt.figure(figsize=(10, 6))
    ax = sns.boxplot(x="profile_type", y="bet_amount", data=game_df)
    ax.set_title("Bet Amounts by Profile Type")
    ax.set_xlabel("Profile Type")
    ax.set_ylabel("Bet Amount")
    
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, "bet_amounts_by_profile.png"))
    plt.close()


def plot_deposit_withdrawal_ratio(df, output_dir):
    """Plot deposit/withdrawal ratio by profile type."""
    # Group by player_id and profile_type
    deposits = defaultdict(float)
    withdrawals = defaultdict(float)
    
    for _, row in df.iterrows():
        if "player_id" in row and "profile_type" in row and "amount" in row:
            if row["event_type"] == "deposit":
                deposits[(row["player_id"], row["profile_type"])] += row["amount"]
            elif row["event_type"] == "withdrawal":
                withdrawals[(row["player_id"], row["profile_type"])] += row["amount"]
    
    # Create data for plotting
    plot_data = []
    for (player_id, profile_type), deposit_amount in deposits.items():
        withdrawal_amount = withdrawals.get((player_id, profile_type), 0)
        ratio = 0 if withdrawal_amount == 0 else deposit_amount / withdrawal_amount
        
        plot_data.append({
            "player_id": player_id,
            "profile_type": profile_type,
            "deposit": deposit_amount,
            "withdrawal": withdrawal_amount,
            "ratio": ratio
        })
    
    ratio_df = pd.DataFrame(plot_data)
    
    if len(ratio_df) == 0:
        print("No deposit/withdrawal data found.")
        return
    
    # Filter out infinite ratios
    ratio_df = ratio_df[ratio_df["ratio"] < 100]
    
    plt.figure(figsize=(12, 8))
    
    # Plot deposit/withdrawal by profile type
    ax = plt.subplot(1, 2, 1)
    data = ratio_df.groupby("profile_type").agg({
        "deposit": "sum",
        "withdrawal": "sum"
    }).reset_index()
    data_melted = pd.melt(
        data,
        id_vars=["profile_type"],
        value_vars=["deposit", "withdrawal"],
        var_name="transaction_type",
        value_name="amount"
    )
    sns.barplot(x="profile_type", y="amount", hue="transaction_type", data=data_melted, ax=ax)
    ax.set_title("Deposit vs Withdrawal by Profile Type")
    ax.set_xlabel("Profile Type")
    ax.set_ylabel("Total Amount")
    plt.xticks(rotation=45)
    
    # Plot ratio by profile type
    ax = plt.subplot(1, 2, 2)
    sns.boxplot(x="profile_type", y="ratio", data=ratio_df, ax=ax)
    ax.set_title("Deposit/Withdrawal Ratio by Profile Type")
    ax.set_xlabel("Profile Type")
    ax.set_ylabel("Deposit/Withdrawal Ratio")
    plt.xticks(rotation=45)
    
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, "deposit_withdrawal_ratio.png"))
    plt.close()


def generate_summary_stats(df, output_dir):
    """Generate summary statistics."""
    # Count events by type
    event_counts = Counter(df["event_type"])
    
    # Count players by profile
    if "profile_type" in df.columns:
        profile_counts = Counter(df["profile_type"].unique())
        
    # Count games by type
    if "game" in df.columns:
        game_counts = Counter(df[df["event_type"] == "game_play"]["game"])
        
    # Calculate total deposits, withdrawals, bets
    total_deposits = df[df["event_type"] == "deposit"]["amount"].sum()
    total_withdrawals = df[df["event_type"] == "withdrawal"]["amount"].sum()
    
    if "bet_amount" in df.columns:
        total_bets = df[df["event_type"] == "game_play"]["bet_amount"].sum()
    else:
        total_bets = 0
        
    if "win_amount" in df.columns:
        total_wins = df[df["event_type"] == "game_play"]["win_amount"].sum()
    else:
        total_wins = 0
        
    # Write summary to file
    with open(os.path.join(output_dir, "summary.txt"), "w") as f:
        f.write("Casino Simulation Summary\n")
        f.write("=======================\n\n")
        
        f.write("Event Counts:\n")
        for event_type, count in event_counts.items():
            f.write(f"  {event_type}: {count}\n")
        f.write("\n")
        
        if "profile_type" in df.columns:
            f.write("Player Profiles:\n")
            for profile_type, count in profile_counts.items():
                f.write(f"  {profile_type}: {count}\n")
            f.write("\n")
            
        if "game" in df.columns:
            f.write("Game Types:\n")
            for game_type, count in game_counts.items():
                f.write(f"  {game_type}: {count}\n")
            f.write("\n")
            
        f.write("Financial Summary:\n")
        f.write(f"  Total Deposits: ${total_deposits:.2f}\n")
        f.write(f"  Total Withdrawals: ${total_withdrawals:.2f}\n")
        f.write(f"  Total Bets: ${total_bets:.2f}\n")
        f.write(f"  Total Wins: ${total_wins:.2f}\n")
        f.write(f"  House Edge: ${total_bets - total_wins:.2f}\n")
        f.write(f"  House Edge %: {100 * (total_bets - total_wins) / total_bets if total_bets > 0 else 0:.2f}%\n")


def main():
    """Main entry point."""
    args = parse_args()
    
    # Create output directory
    os.makedirs(args.output_dir, exist_ok=True)
    
    print(f"Loading events from {args.input_file}...")
    events = load_events(args.input_file)
    print(f"Loaded {len(events)} events.")
    
    print("Converting events to DataFrame...")
    df = convert_to_dataframe(events)
    
    print("Generating visualizations...")
    plot_event_types(df, args.output_dir)
    
    if "profile_type" in df.columns:
        plot_profile_distribution(df, args.output_dir)
        
    if "game" in df.columns:
        plot_game_distribution(df, args.output_dir)
        
    if "balance" in df.columns:
        plot_player_balances(df, args.output_dir)
        
    if "profile_type" in df.columns and "bet_amount" in df.columns:
        plot_bet_amounts_by_profile(df, args.output_dir)
        
    if "profile_type" in df.columns:
        plot_deposit_withdrawal_ratio(df, args.output_dir)
        
    generate_summary_stats(df, args.output_dir)
    
    print(f"Visualizations and summary saved to {args.output_dir}")


if __name__ == "__main__":
    main()
