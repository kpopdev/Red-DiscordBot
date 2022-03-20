"""The checks in this module run on every command."""
import logging

from . import commands

log = logging.getLogger("red")


def init_global_checks(bot):
    @bot.check_once
    async def check_message_is_eligible_as_command(ctx: commands.Context) -> bool:
        try:
            return await ctx.bot.message_eligible_as_command(ctx.message)
        except TypeError:
            log.warning(
                "Discarded a command message (ID: %s) with PartialMessageable channel: %r",
                ctx.message.id,
                ctx.channel,
            )
            return False
