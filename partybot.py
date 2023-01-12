import discord
import dataclasses
import sqlite3
import asyncio
import contextlib
import inspect
import json
import random
import re
import os

from typing import *

intents = discord.Intents().all()
client: discord.Client = discord.Client(intents=intents)

USER_ID_PATTERN = re.compile(r'<?@?!?(\d+)>?')
CHANNEL_LIMIT = 50
REQUIRED_PERMISSIONS = discord.Permissions(16778256)
TOKEN = os.getenv('PARTYBOT_TOKEN')
print(f"TOKEN: {TOKEN}")


@dataclasses.dataclass
class PartyBotOwnerChannel:
    channel_id: int
    user_id: int

    def __repr__(self):
        return str(vars(self))


@dataclasses.dataclass
class PartyBotGuildSettings:
    guild_id: int
    join_channel_id: int
    command_channel_id: int
    main_category_id: int
    moderator_role_id: int
    manage_role_id: int
    dynamic_category_creation: bool
    max_categories: int

    @staticmethod
    def is_valid_channel_id(guild, channel_id):
        return guild.get_channel(channel_id)

    @staticmethod
    def is_valid_join_channel_id(guild, channel_id):
        return PartyBotGuildSettings.is_valid_channel_id(guild, channel_id)

    @staticmethod
    def is_valid_command_channel_id(guild, channel_id):
        return PartyBotGuildSettings.is_valid_channel_id(guild, channel_id)

    @staticmethod
    def is_valid_main_category_id(guild, channel_id):
        return PartyBotGuildSettings.is_valid_channel_id(guild, channel_id)

    @staticmethod
    def is_valid_manage_role_id(guild, role_id):
        return guild.get_role(role_id)

    @staticmethod
    def is_valid_moderator_role_id(guild, role_id):
        return guild.get_role(role_id)

    def __repr__(self):
        return str(vars(self))


class Storage:
    PARTYBOT_SELECT_QUERY = "SELECT * FROM partybot WHERE guild_id=?"
    PARTYBOT_SELECT_OWNER_CHANNEL_QUERY = "SELECT * FROM partybot_owners WHERE user_id=?"

    PARTYBOT_INSERT_QUERY = """
INSERT INTO partybot VALUES (
?, ?, ?, ?, ?, ?, ?, ?
) ON CONFLICT(guild_id)
DO UPDATE SET
join_here_channel_id=excluded.join_here_channel_id,
command_channel_id=excluded.command_channel_id,
main_category_id=excluded.main_category_id,
moderator_role_id=excluded.moderator_role_id,
manage_role_id=excluded.manage_role_id,
dynamic_category_creation=excluded.dynamic_category_creation,
max_categories=excluded.max_categories;
"""

    CREATE_PARTYBOT_TABLE_QUERY = """
CREATE TABLE IF NOT EXISTS partybot(
    guild_id INTEGER PRIMARY KEY,
    join_here_channel_id INTEGER,
    command_channel_id INTEGER,
    main_category_id INTEGER,
    moderator_role_id INTEGER,
    manage_role_id INTEGER,
    dynamic_category_creation BOOL,
    max_categories INTEGER
)
"""

    CREATE_CATEGORIES_TABLE_QUERY = """
CREATE TABLE IF NOT EXISTS partybot_categories(
    category_id INTEGER PRIMARY KEY,
    guild_id INTEGER
)
"""

    CREATE_OWNERS_TABLE_QUERY = """
CREATE TABLE IF NOT EXISTS partybot_owners(
    channel_id INTEGER PRIMARY KEY,
    user_id INTEGER
)
"""

    PARTYBOT_INSERT_OWNER_QUERY = """
INSERT INTO partybot_owners VALUES (
?, ?
) ON CONFLICT(channel_id)
DO UPDATE SET
user_id=excluded.user_id;
"""

    PARTYBOT_DELETE_OWNER_CHANNEL_QUERY = """
DELETE FROM partybot_owners WHERE channel_id = ?;
"""

    PARTYBOT_SELECT_CHANNEL_OWNER_QUERY = """
SELECT user_id FROM partybot_owners WHERE channel_id=?;
"""
    PARTYBOT_SELECT_OWNER_CHANNEL_QUERY = """
SELECT channel_id FROM partybot_owners WHERE user_id=?;
"""

    PARTYBOT_DELETE_OWNERS_QUERY = """
DELETE FROM partybot_owners;
"""

    PARTYBOT_SELECT_CATEGORIES_QUERY = """
SELECT category_id FROM partybot_categories WHERE guild_id = ?;
"""

    PARTYBOT_INSERT_CATEGORIES_QUERY = """
INSERT INTO partybot_categories VALUES (?, ?);
"""

    PARTYBOT_DELETE_CATEGORIES_QUERY = """
DELETE FROM partybot_categories WHERE category_id=?;
"""

    PARTYBOT_DELETE_ALL_CATEGORIES_QUERY = """
DELETE FROM partybot_categories WHERE guild_id=?;
"""

    def __init__(self, filename):
        self.connection = sqlite3.connect(filename, check_same_thread=False)
        self.party_bot_guild_settings: Dict[int, PartyBotGuildSettings] = {}
        self.party_bot_channels: Dict[int, int] = {}
        self.party_bot_owners: Dict[int, int] = {}
        self.categories = {}

        with contextlib.closing(self.connection.cursor()) as cursor:
            cursor.execute(Storage.CREATE_PARTYBOT_TABLE_QUERY)
            cursor.execute(Storage.CREATE_CATEGORIES_TABLE_QUERY)
            cursor.execute(Storage.CREATE_OWNERS_TABLE_QUERY)
            self.connection.commit()

    async def _fetchone_async(self, query, params):
        def _f():
            with contextlib.closing(self.connection.cursor()) as cursor:
                return cursor.execute(query, params).fetchone()

        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            None, _f)

    async def _fetchall_async(self, query, params):
        def _f():
            with contextlib.closing(self.connection.cursor()) as cursor:
                return cursor.execute(query, params).fetchall()

        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            None, _f)

    async def _execute_and_commit_async(self, query, params):
        def _f():
            with contextlib.closing(self.connection.cursor()) as cursor:
                cursor.execute(query, params)
                self.connection.commit()

        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            None, _f)

    async def get_party_bot_guild_settings(self, guild_id: int) -> Optional[PartyBotGuildSettings]:
        guild_settings = self.party_bot_guild_settings.get(guild_id, -1)
        if guild_settings == -1:
            result = await self._fetchone_async(Storage.PARTYBOT_SELECT_QUERY, (guild_id,))
            if result:
                guild_settings = PartyBotGuildSettings(
                    result[0], result[1], result[2], result[3], result[4], result[5], result[6], result[7]
                )
                self.party_bot_guild_settings[guild_id] = guild_settings
            else:
                self.party_bot_guild_settings[guild_id] = None
        return guild_settings if guild_settings != -1 else None

    async def set_party_bot_guild_settings(self, guild_settings: PartyBotGuildSettings):
        self.party_bot_guild_settings[guild_settings.guild_id] = guild_settings
        await self._execute_and_commit_async(
            Storage.PARTYBOT_INSERT_QUERY, (
                guild_settings.guild_id,
                guild_settings.join_channel_id,
                guild_settings.command_channel_id,
                guild_settings.main_category_id,
                guild_settings.moderator_role_id,
                guild_settings.manage_role_id,
                guild_settings.dynamic_category_creation,
                guild_settings.max_categories
            )
        )

    async def get_categories(self, guild_id):
        categories = self.categories.get(guild_id)
        if not categories:
            result = await self._fetchall_async(
                Storage.PARTYBOT_SELECT_CATEGORIES_QUERY,
                (guild_id,)
            )

            result = [a[0] for a in result]
            result.append(
                self.party_bot_guild_settings[guild_id].main_category_id)
            self.categories[guild_id] = result
        return self.categories[guild_id]

    async def add_category(self, guild_id, category_id):
        if not self.categories.get(guild_id):
            await self.get_categories(guild_id)

        await self._execute_and_commit_async(
            Storage.PARTYBOT_INSERT_CATEGORIES_QUERY,
            (category_id, guild_id)
        )

        self.categories[guild_id].append(category_id)

    async def remove_category(self, guild_id, category_id):
        if not self.categories.get(guild_id):
            await self.get_categories(guild_id)

        await self._execute_and_commit_async(
            Storage.PARTYBOT_DELETE_CATEGORIES_QUERY,
            (category_id,)
        )

        self.categories[guild_id].remove(category_id)

    async def delete_all_additional_categories(self, guild_id):
        await self._execute_and_commit_async(
            Storage.PARTYBOT_DELETE_ALL_CATEGORIES_QUERY,
            (guild_id,)
        )

        try:
            del self.categories[guild_id]
        except:
            pass

    async def delete_all_owners(self):
        await self._execute_and_commit_async(
            Storage.PARTYBOT_DELETE_OWNERS_QUERY, ()
        )

        self.party_bot_channels = {}
        self.party_bot_owners = {}

    async def get_channel_owner(self, channel_id):
        owner_id = self.party_bot_channels.get(channel_id, -1)
        if owner_id == -1:
            result = await self._fetchone_async(
                Storage.PARTYBOT_SELECT_CHANNEL_OWNER_QUERY,
                (channel_id,)
            )
            if result:
                owner_id = result[0]
                self.party_bot_channels[channel_id] = owner_id
                self.party_bot_owners[owner_id] = channel_id
        return owner_id if owner_id != -1 else None

    async def get_owner_channel(self, user_id):
        channel_id = self.party_bot_owners.get(user_id, -1)
        if channel_id == -1:
            result = await self._fetchone_async(
                Storage.PARTYBOT_SELECT_OWNER_CHANNEL_QUERY,
                (user_id,)
            )
            if result:
                channel_id = result[0]
                self.party_bot_channels[channel_id] = user_id
                self.party_bot_owners[user_id] = channel_id
        return channel_id if channel_id != -1 else None

    async def set_channel_owner(self, channel_id, owner_id):
        await self._execute_and_commit_async(
            Storage.PARTYBOT_INSERT_OWNER_QUERY,
            (channel_id, owner_id)
        )

        try:
            previous_owner = self.party_bot_channels[channel_id]
            del self.party_bot_owners[previous_owner]
        except:
            pass

        self.party_bot_channels[channel_id] = owner_id
        self.party_bot_owners[owner_id] = channel_id

    async def delete_channel(self, channel_id):
        await self._execute_and_commit_async(
            Storage.PARTYBOT_DELETE_OWNER_CHANNEL_QUERY,
            (channel_id,)
        )

        try:
            owner_id = self.party_bot_channels[channel_id]
            del self.party_bot_owners[owner_id]
            del self.party_bot_channels[channel_id]
        except:
            pass


@client.event
async def on_message(message: discord.Message):
    if message.author == client.user:
        return

    guild_settings = await storage.get_party_bot_guild_settings(message.author.guild.id)
    if not guild_settings:
        guild_settings = PartyBotGuildSettings(
            message.author.guild.id,
            None,
            None,
            None,
            None,
            None,
            False,
            1
        )

    if message.channel.id == guild_settings.command_channel_id:
        async with contextlib.AsyncExitStack() as stack:
            if message.author.guild.get_role(guild_settings.manage_role_id) not in message.author.roles and \
                    message.author.guild.get_role(guild_settings.moderator_role_id) not in message.author.roles and \
                    not message.author.permissions_in(message.channel).administrator:
                stack.push_async_callback(message.delete)

            channel_id = await storage.get_owner_channel(message.author.id)
            voice_channel = message.channel.guild.get_channel(channel_id)
            if not voice_channel:
                if channel_id:
                    await storage.delete_channel(channel_id)
            else:
                if message.content.startswith("!pb"):
                    argument_list = message.content.split()
                    if len(argument_list) < 2:
                        return

                    command = argument_list[1]

                    if command == "lock":
                        member_length = max(1, len(voice_channel.members))
                        await voice_channel.edit(user_limit=member_length)
                    elif command == "unlock":
                        join_voice_channel = message.channel.guild.get_channel(
                            guild_settings.join_channel_id)
                        await voice_channel.edit(user_limit=join_voice_channel.user_limit)

                    if len(argument_list) < 3:
                        return

                    user_id = argument_list[2]

                    match = USER_ID_PATTERN.match(user_id)
                    if not match:
                        return

                    user_id = int(match.group(1))
                    user = message.channel.guild.get_member(user_id)

                    if not user:
                        return

                    if user not in voice_channel.members:
                        return

                    if command == "kick":
                        if message.author.guild.get_role(guild_settings.manage_role_id) not in user.roles and \
                                message.author.guild.get_role(guild_settings.moderator_role_id) not in user.roles and \
                                not user.permissions_in(message.channel).administrator:
                            await user.move_to(None, reason=f"PartyBot {message.author.id} disconnected user.")
                    elif command == "owner":
                        await storage.set_channel_owner(voice_channel.id, user.id)
                        print(storage.party_bot_owners)
                        print(storage.party_bot_channels)

    if not message.author.permissions_in(message.channel).administrator and \
            (message.author.guild.get_role(guild_settings.manage_role_id) not in message.author.roles):
        return

    if message.channel.permissions_for(message.channel.guild.get_member(client.user.id)) < REQUIRED_PERMISSIONS:
        print(discord.user.permissions_in(message.channel))
        return

    if message.content.startswith("!pb"):
        argument_list = message.content.split()

        if len(argument_list) < 2:
            return

        command = argument_list[1]

        if command == "delete_all_addition_categories":
            await storage.delete_all_additional_categories(message.channel.guild.id)
            await message.channel.send(f"Deleteing additional categories.")

        if command == "delete_all_owners":
            await storage.delete_all_owners()
            await message.channel.send(f"Deleteing all channel owners.")

        if command == "fill_category":
            if len(argument_list) < 3:
                return

            category_id = int(argument_list[2])
            category = message.channel.guild.get_channel(category_id)
            if category:
                for i in range(0, 50 - len(category.channels)):
                    await category.create_voice_channel(
                        f"PartyBot Room",
                        bitrate=32000,
                        user_limit=4,
                        reason="PartyBot create member channel."
                    )

        if command == "settings":
            await message.channel.send(f"```{json.dumps(vars(guild_settings), indent=4, sort_keys=True)}```")

        elif command == "set":
            if len(argument_list) < 4:
                return

            sub_command = argument_list[2]
            parameter = argument_list[3]

            signature = inspect.signature(PartyBotGuildSettings)
            if sub_command in signature.parameters and sub_command != 'guild_id':
                type_ = signature.parameters[sub_command].annotation
                try:
                    if type_ == bool:
                        property_ = parameter.lower() == 'true'
                    else:
                        property_ = type_(parameter)
                except ValueError:
                    await message.channel.send(f"{parameter} is not a valid type for {sub_command}, needs to be type {type_}.")
                    return

                if "id" in sub_command:
                    valid_property = getattr(
                        guild_settings, "is_valid_" + sub_command)(message.channel.guild, property_)
                    if valid_property:
                        await message.channel.send(f"Set {sub_command} set to {valid_property.name}.")
                    else:
                        await message.channel.send(f"{property_} is not a valid ID.")
                        return
                else:
                    await message.channel.send(f"Set {sub_command} set to {property_}.")

                setattr(guild_settings, sub_command, property_)

            guild_settings = PartyBotGuildSettings(
                guild_settings.guild_id,
                guild_settings.join_channel_id,
                guild_settings.command_channel_id,
                guild_settings.main_category_id,
                guild_settings.moderator_role_id,
                guild_settings.manage_role_id,
                guild_settings.dynamic_category_creation,
                guild_settings.max_categories
            )

            await storage.set_party_bot_guild_settings(guild_settings)


@client.event
async def on_ready():
    print("We have logged in as {0.user}".format(client))


async def get_unfilled_category(guild, categories):
    for category_id in categories:
        try:
            category = guild.get_channel(category_id)
            if len(category.channels) < CHANNEL_LIMIT - 1:
                return category
        except AttributeError:
            await storage.remove_category(guild.id, category_id)
    return None

@client.event
async def on_guild_channel_delete(channel):
    categories = await storage.get_categories(channel.guild.id)
    guild_settings = await storage.get_party_bot_guild_settings(channel.guild.id)
    
    if isinstance(channel, discord.VoiceChannel) and channel.category in categories:
        if storage.partybot_channels.get(channel.id):
            print("removing owner channel")
            await storage.delete_channel(channel.id)
    elif isinstance(channel, discord.CategoryChannel) and channel in categories:
        print("removing category")
        await storage.remove_category(channel.guild.id, channel.id)
    elif channel.id == guild_settings.join_channel_id:
        guild_settings.join_channel_id = None
        await storage.set_party_bot_guild_settings(guild_settings)

@client.event
async def on_voice_state_update(
        member: discord.Member,
        before: discord.VoiceState,
        after: discord.VoiceState):

    guild_settings = await storage.get_party_bot_guild_settings(member.guild.id)

    if not guild_settings.join_channel_id or not guild_settings.main_category_id:
        return

    main_category = member.guild.get_channel(guild_settings.main_category_id)
    if not main_category:
        guild_settings.main_category_id = None
        await storage.set_party_bot_guild_settings(guild_settings)
        return

    if client.get_channel(guild_settings.join_channel_id) \
             .permissions_for(member.guild.get_member(client.user.id)) < REQUIRED_PERMISSIONS:
        return

    categories = await storage.get_categories(member.guild.id)

    if before.channel and before.channel.id != guild_settings.join_channel_id and \
       before.channel.category_id in categories:
        if len(before.channel.members) == 0:
            if await storage.get_owner_channel(member.id) == before.channel.id:
                await storage.delete_channel(before.channel.id)
            await before.channel.delete(reason="PartyBot no more members in channel.")
            if before.channel.category_id != guild_settings.main_category_id:
                if len(before.channel.category.channels) == 0:
                    category_id = before.channel.category.id
                    await before.channel.category.delete(reason="PartyBot no more channels in category.")
                    await storage.remove_category(member.guild.id, category_id)
        else:
            if await storage.get_owner_channel(member.id) == before.channel.id and \
               await storage.get_channel_owner(before.channel.id) == member.id:
                new_owner = random.choice(before.channel.members)
                await storage.set_channel_owner(before.channel.id, new_owner.id)
                #await new_owner.send(f"The previous owner left, you're the captain now of {before.channel.name}.")
                #Apparently DMing the user here is considered spam, so TODO: find a better way to do this, maybe a channel?

    if after.channel and after.channel.id == guild_settings.join_channel_id:
        category = await get_unfilled_category(member.guild, categories)

        if not category:
            if guild_settings.dynamic_category_creation and len(categories) < guild_settings.max_categories:
                main_cateogry = member.guild.get_channel(
                    guild_settings.main_category_id)

                category = await member.guild.create_category(
                    f"{main_cateogry.name} {len(categories) + 1}",
                    position=member.guild.get_channel(
                        categories[-1]).position + 1,
                    reason="PartyBot dynamic category creation."
                )

                await storage.add_category(member.guild.id, category.id)
            else:
                await member.move_to(None, reason="PartyBot no more open categories.")
                #await member.send("There are no more open categories, contact an administrator or moderator for more details.")
                return

        if category != None:
            channel: discord.VoiceChannel = await member.guild.create_voice_channel(
                f"PartyBot Room",
                category=category,
                bitrate=after.channel.bitrate,
                user_limit=after.channel.user_limit,
                reason="PartyBot create member channel."
            )

            if channel.category == None:
                channel.delete("PartyBot invalid category.")
                return

            await storage.set_channel_owner(channel.id, member.id)
            await member.move_to(channel, reason="PartyBot move user.")

if __name__ == "__main__":
    storage = Storage("partybot.db")
    client.run(TOKEN)
