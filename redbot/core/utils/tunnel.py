import asyncio
import discord
from datetime import datetime
from redbot.core.utils.chat_formatting import pagify
import io
import weakref
from typing import List, Optional, Union
from .common_filters import filter_mass_mentions

__all__ = ("Tunnel",)

_instances = weakref.WeakValueDictionary()


class TunnelMeta(type):
    """Prevent multiple tunnels with the same origin and recipient."""

    def __call__(cls, *args, **kwargs):
        lockout_tuple = ((kwargs.get("sender"), kwargs.get("origin")), kwargs.get("recipient"))

        # Check if this tunnel already exists
        if lockout_tuple in _instances:
            return _instances[lockout_tuple]

        if not (any(lockout_tuple[0] == x[0] for x in _instances.keys()) or 
                any(lockout_tuple[1] == x[1] for x in _instances.keys())):
            # Create a new tunnel if unique
            tunnel_instance = super().__call__(*args, **kwargs)
            _instances[lockout_tuple] = tunnel_instance
            return tunnel_instance
        return None


class Tunnel(metaclass=TunnelMeta):
    """
    A tunnel interface for messages.

    Attributes
    ----------
    sender: discord.Member
        The person who opened the tunnel.
    origin: Union[discord.TextChannel, discord.VoiceChannel, discord.StageChannel, discord.Thread]
        The channel in which it was opened.
    recipient: discord.User
        The user on the other end of the tunnel.
    """

    def __init__(self, *, sender: discord.Member, origin: Union[discord.TextChannel, discord.VoiceChannel, discord.StageChannel, discord.Thread], recipient: discord.User):
        self.sender = sender
        self.origin = origin
        self.recipient = recipient
        self.last_interaction = datetime.utcnow()

    async def react_close(self, *, uid: int, message: str = ""):
        send_to = self.recipient if uid == self.sender.id else self.origin
        closer = self.sender if uid == self.sender.id else self.recipient
        await send_to.send(filter_mass_mentions(message.format(closer=closer)))

    @property
    def members(self):
        return self.sender, self.recipient

    @property
    def minutes_since(self):
        return int((datetime.utcnow() - self.last_interaction).seconds / 60)

    @staticmethod
    async def message_forwarder(*, destination: discord.abc.Messageable, content: str = None, embed=None, files: Optional[List[discord.File]] = None) -> List[discord.Message]:
        """
        Forwards a message to the specified destination.

        Returns List[discord.Message]: Messages sent as a result.
        """
        messages = []
        if content:
            for page in pagify(content):
                messages.append(await destination.send(page, files=files, embed=embed))
                files = embed = None
        elif embed or files:
            messages.append(await destination.send(files=files, embed=embed))
        return messages

    @staticmethod
    async def files_from_attach(m: discord.Message, *, use_cached: bool = False, images_only: bool = False) -> List[discord.File]:
        """
        Creates a list of file objects from a message.

        Returns List[discord.File]: A list of file objects, or an empty list.
        """
        files = []
        max_size = 26214400  # 25MB
        if m.attachments and sum(a.size for a in m.attachments) <= max_size:
            for a in m.attachments:
                if images_only and a.height is None:
                    continue
                try:
                    file = await a.to_file()
                except discord.HTTPException as e:
                    if not (e.status == 415 and images_only and use_cached):
                        raise
                else:
                    files.append(file)
        return files

    async def close_because_disabled(self, close_message: str):
        """Informs both ends of the tunnel that it is now closed."""
        tasks = [destination.send(close_message) for destination in (self.recipient, self.origin)]
        await asyncio.gather(*tasks, return_exceptions=True)

    async def communicate(self, *, message: discord.Message, topic: str = None, skip_message_content: bool = False):
        """
        Forwards a message based on the defined rules.

        Returns Tuple[int, int]: Message IDs of the forwarded message and the bot's last message.
        """
        if message.channel.id == self.origin.id and message.author == self.sender:
            send_to = self.recipient
        elif message.author == self.recipient and message.guild is None:
            send_to = self.origin
        else:
            return None

        content = f"{topic}\n{message.content}" if topic and not skip_message_content else topic

        attach = await self.files_from_attach(message) if message.attachments else []

        if not attach and message.attachments:
            await message.channel.send("Could not forward attachments. Total size of attachments must be less than 25MB.")

        rets = await self.message_forwarder(destination=send_to, content=content, files=attach)

        await message.add_reaction("\N{WHITE HEAVY CHECK MARK}")
        await message.add_reaction("\N{NEGATIVE SQUARED CROSS MARK}")
        self.last_interaction = datetime.utcnow()
        await rets[-1].add_reaction("\N{NEGATIVE SQUARED CROSS MARK}")
        return [rets[-1].id, message.id]
