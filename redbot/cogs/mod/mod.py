import asyncio
import logging
import re
from abc import ABC
from collections import defaultdict
from typing import Literal

from redbot.core import Config, commands
from redbot.core.bot import Red
from redbot.core.i18n import Translator, cog_i18n
from redbot.core.utils import AsyncIter
from redbot.core.utils._internal_utils import send_to_owners_with_prefix_replaced
from redbot.core.utils.chat_formatting import inline
from .events import Events
from .kickban import KickBanMixin
from .names import ModInfo
from .slowmode import Slowmode
from .settings import ModSettings

_ = T_ = Translator("Mod", __file__)

__version__ = "1.2.0"

class CompositeMetaClass(type(commands.Cog), type(ABC)):
    pass

@cog_i18n(_)
class Mod(
    ModSettings,
    Events,
    KickBanMixin,
    ModInfo,
    Slowmode,
    commands.Cog,
    metaclass=CompositeMetaClass,
):
    """Moderation tools."""

    default_global_settings = {
        "version": "",
        "track_all_names": True,
    }

    default_guild_settings = {
        "mention_spam": {"ban": None, "kick": None, "warn": None, "strict": False},
        "delete_repeats": -1,
        "ignored": False,
        "respect_hierarchy": True,
        "delete_delay": -1,
        "reinvite_on_unban": False,
        "current_tempbans": [],
        "dm_on_kickban": False,
        "require_reason": False,
        "default_days": 0,
        "default_tempban_duration": 60 * 60 * 24,
        "track_nicknames": True,
    }

    default_channel_settings = {"ignored": False}
    default_member_settings = {"past_nicks": [], "perms_cache": {}, "banned_until": False}
    default_user_settings = {"past_names": [], "past_display_names": []}

    def __init__(self, bot: Red):
        super().__init__()
        self.bot = bot

        self.config = Config.get_conf(self, 4961522000, force_registration=True)
        self.config.register_global(**self.default_global_settings)
        self.config.register_guild(**self.default_guild_settings)
        self.config.register_channel(**self.default_channel_settings)
        self.config.register_member(**self.default_member_settings)
        self.config.register_user(**self.default_user_settings)
        self.cache: dict = {}
        self.last_case: dict = defaultdict(dict)

        self.tban_expiry_task = None
        self.start_tempban_task()

    def start_tempban_task(self):
        if self.tban_expiry_task is None or self.tban_expiry_task.done():
            self.tban_expiry_task = asyncio.create_task(self.tempban_expirations_task())

    def cog_unload(self):
        if self.tban_expiry_task:
            self.tban_expiry_task.cancel()
            self.tban_expiry_task = None

    async def red_delete_data_for_user(
        self,
        *,
        requester: Literal["discord_deleted_user", "owner", "user", "user_strict"],
        user_id: int,
    ):
        if requester != "discord_deleted_user":
            return

        all_members = await self.config.all_members()

        async for guild_id, guild_data in AsyncIter(all_members.items(), steps=100):
            if user_id in guild_data:
                await self.config.member_from_ids(guild_id, user_id).clear()

        await self.config.user_from_id(user_id).clear()

        guild_data = await self.config.all_guilds()

        async for guild_id, guild_data in AsyncIter(guild_data.items(), steps=100):
            if user_id in guild_data["current_tempbans"]:
                async with self.config.guild_from_id(guild_id).current_tempbans() as tbs:
                    try:
                        tbs.remove(user_id)
                    except ValueError:
                        pass

    async def cog_load(self) -> None:
        await self._maybe_update_config()
        self.start_tempban_task()
