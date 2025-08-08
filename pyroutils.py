"""
Pyrogram utilities for handling peer IDs and chat/channel identification.
"""
from pyrogram import utils as pyroutils
# Peer ID constants for proper chat/channel identification
MIN_CHAT_ID = -999999999999
MIN_CHANNEL_ID = -100999999999999

def is_channel_id(peer_id: int) -> bool:
    """Check if peer ID is a channel."""
    return peer_id <= MIN_CHANNEL_ID

def is_chat_id(peer_id: int) -> bool:
    """Check if peer ID is a chat (group)."""
    return MIN_CHANNEL_ID < peer_id <= MIN_CHAT_ID

def is_user_id(peer_id: int) -> bool:
    """Check if peer ID is a user."""
    return peer_id > MIN_CHAT_ID

def format_peer_id(peer_id: int) -> str:
    """Format peer ID for display."""
    if is_channel_id(peer_id):
        return f"Channel: {peer_id}"
    elif is_chat_id(peer_id):
        return f"Group: {peer_id}"
    else:
        return f"User: {peer_id}"