import discord
import sqlite3
import asyncio
import contextlib
import os

from typing import *

client: discord.Client = discord.Client()

REQUIRED_PERMISSIONS = discord.Permissions(16778256)
TOKEN = os.getenv('PARTYBOT_TOKEN')
print(f"TOKEN: {TOKEN}")

class PartyBotOwnerChannel:
    def __init__(self, channel_id: int, user_id: int):
        self.channel_id = channel_id
        self.user_id = user_id

    def __repr__(self):
        return str(vars(self))
    
class PartyBotGuildSettings:
    def __init__(self,
            guild_id: int,
            join_channel_id: Optional[int],
            command_channel_id: Optional[int],
            main_cateogry_id: Optional[int],
            role_id: Optional[int],
            dynamic_category_creation: bool,
            max_categories: int):

        self.guild_id = guild_id
        self.join_channel_id = join_channel_id
        self.main_cateogry_id = main_cateogry_id
        self.role_id = role_id
        self.command_channel_id = command_channel_id
        self.dynamic_category_creation = dynamic_category_creation
        self.max_categories = max_categories

    def __repr__(self):
        return str(vars(self))

class Storage:
    PARTYBOT_SELECT_QUERY = "SELECT * FROM partybot WHERE guild_id=?"
    PARTYBOT_SELECT_OWNER_CHANNEL_QUERY = "SELECT * FROM partybot_owners WHERE user_id=?"

    PARTYBOT_INSERT_QUERY = """
INSERT INTO partybot VALUES (
?, ?, ?, ?, ?, ?, ?
) ON CONFLICT(guild_id)
DO UPDATE SET
join_here_channel_id=excluded.join_here_channel_id,
command_channel_id=excluded.command_channel_id,
main_category_id=excluded.main_category_id,
role_id=excluded.role_id,
dynamic_cateogry_creation=excluded.dynamic_cateogry_creation,
max_categories=excluded.max_categories;
"""

    CREATE_PARTYBOT_TABLE_QUERY = """
CREATE TABLE IF NOT EXISTS partybot(
    guild_id INTEGER PRIMARY KEY,
    join_here_channel_id INTEGER,
    command_channel_id INTEGER,
    main_category_id INTEGER,
    role_id INTEGER,
    dynamic_cateogry_creation BOOL,
    max_categories INTEGER
)
"""

    CREATE_CATEGORIES_TABLE_QUERY = """
CREATE TABLE IF NOT EXISTS partybot_categories(
    category_id INTEGER PRIMARY KEY,
    guild_id INTEGER,
)
"""

    CREATE_OWNERS_TABLE_QUERY = """
CREATE TABLE IF NOT EXISTS partybot_owners(
    channel_id INTEGER PRIMARY KEY,
    user_id INTEGER,
)
"""

    PARTYBOT_INSERT_OWNER_QUERY = """
INSERT INTO partybot_owners VALUES (
?, ?,
) ON CONFLICT(channel_id)
DO UPDATE SET
user_id=excluded.user_id;
"""

    PARTYBOT_DELETE_OWNER_QUERY = """
DELETE FROM partybot_owners WHERE channel_id = ?;
"""
    
    def __init__(self, filename):
        self.connection = sqlite3.connect(filename, check_same_thread=False)
        self.party_bot_guild_settings: Dict[int, PartyBotGuildSettings] = {}
        self.party_bot_owner_channels: Dict[int, PartyBotOwnerChannel] = {}
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
                    result[0], result[1], result[2], result[3], result[4], result[5], result[6]
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
                guild_settings.main_cateogry_id,
                guild_settings.role_id,
                guild_settings.dynamic_category_creation,
                guild_settings.max_categories
             )
        )

    async def set_channel_owner(self, owner_channel: PartyBotOwnerChannel):
        self.party_bot_owner_channels[owner_channel.user_id] = owner_channel
        await self._fetchone_async(
            Storage.PARTYBOT_INSERT_OWNER_QUERY,
            (owner_channel.channel_id, owner_channel.user_id)
        )

    async def get_channel_owner(self, channel_id: int):
        await self._execute_and_commit_async(
            Storage.PARTYBOT_INSERT_OWNER_QUERY,
            (owner_channel.channel_id, owner_channel.user_id)
        )

    async def get_owner_channel(self, user_id: int):
        owner_channel = self.party_bot_owner_channels.get(user_id, -1)
        if owner_channel == -1
            await self._execute_and_commit_async(
                Storage.PARTYBOT_INSERT_OWNER_QUERY,
                (owner_channel.channel_id, owner_channel.user_id)
            )

    async def delete_owner_channel(self, channel_id):
        await self._execute_and_commit_async(
            Storage.PARTYBOT_DELETE_OWNER_QUERY,
            (channel_id,)
        )

    async def delete_owner_channel(self, user_id):
        await self._execute_and_commit_async(
            Storage.PARTYBOT_DELETE_OWNER_QUERY,
            (channel_id,)
        )
        


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
            False,
            1
        )
    else:
        print(guild_settings)

    if not message.author.permissions_in(message.channel).administrator and \
            (guild_settings and message.author.guild.get_role(guild_settings.role_id) not in message.author.roles):
        print("Not administrator.")
        return

    if message.channel.permissions_for(message.channel.guild.get_member(client.user.id)) < REQUIRED_PERMISSIONS:
        print(discord.user.permissions_in(message.channel))
        return

    if message.content.startswith("!partybot"):
        argument_list = message.content.split()

        if len(argument_list) < 2:
            return

        command = argument_list[1]
        if command == "set":
            if len(argument_list) < 4:
                return

            sub_command = argument_list[2]
            parameter = argument_list[3]

            if sub_command == "manage_role_id":
                manage_role_id = int(parameter)
                manage_role = message.channel.guild.get_role(manage_role_id)
                if manage_role:
                    await message.channel.send(f"Set managing role to: {manage_role.name}.")
                else:
                    await message.channel.send(f"Role does not exist: {manage_role_id}.")
                    return
                guild_settings.role_id = manage_role_id
            elif sub_command == "join_channel_id":
                try:
                    join_channel_id = int(parameter)
                except ValueError:
                    await message.channel.send(f"{join_channel_id} is not a valid integer.")
                    return
                
                join_channel = message.channel.guild.get_channel(join_channel_id)
                if join_channel:
                    await message.channel.send(f"Set join channel to: {join_channel.name}.")
                else:
                    await message.channel.send(f"Channel does not exist: {join_channel_id}.")
                    return
                guild_settings.join_channel_id = join_channel_id
            elif sub_command == "main_cateogry_id":
                try:
                    main_cateogry_id = int(parameter)
                except ValueError:
                    await message.channel.send(f"{main_cateogry_id} is not a valid integer.")
                    return
                
                main_cateogry = message.channel.guild.get_channel(main_cateogry_id)
                if main_cateogry:
                    await message.channel.send(f"Set main category to: {main_cateogry.name}.")
                else:
                    await message.channel.send(f"Category does not exist: {main_cateogry_id}.")
                    return
                guild_settings.main_cateogry_id = main_cateogry_id
            elif sub_command == "command_channel_id":
                try:
                    command_channel_id = int(parameter)
                except ValueError:
                    await message.channel.send(f"{command_channel_id} is not a valid integer.")
                    return
                
                command_channel = message.channel.guild.get_channel(command_channel_id)
                if command_channel:
                    await message.channel.send(f"Set command channel to: {command_channel.name}.")
                else:
                    await message.channel.send(f"Channel does not exist: {command_channel_id}.")
                    return
                guild_settings.command_channel_id = command_channel_id
    
            guild_settings = PartyBotGuildSettings(
                guild_settings.guild_id,
                guild_settings.join_channel_id,
                guild_settings.command_channel_id,
                guild_settings.main_cateogry_id,
                guild_settings.role_id,
                guild_settings.dynamic_category_creation,
                guild_settings.max_categories
            )
    
        await storage.set_party_bot_guild_settings(guild_settings)

@client.event
async def on_ready():
    print("We have logged in as {0.user}".format(client))

@client.event
async def on_voice_state_update(
        member: discord.Member,
        before: discord.VoiceState,
        after: discord.VoiceState):
    return

    if not join_here_channel_id or not manage_category_id:
        if not await try_sync_guild_settings(member.guild.id):
            return

    if client.get_channel(join_here_channel_id) \
             .permissions_for(member.guild.get_member(client.user.id)) < REQUIRED_PERMISSIONS:
        return

    if before.channel and before.channel.id != join_here_channel_id and \
       before.channel.category_id == manage_category_id:
        if len(before.channel.members) == 0:
            await before.channel.delete(reason="PartyBot no more members in channel.")

    if after.channel and after.channel.id == join_here_channel_id:
        channel: discord.VoiceChannel = await member.guild.create_voice_channel(
            f"{member.nick if member.nick else member.name}'s Room",
            category=client.get_channel(manage_category_id),
            bitrate=after.channel.bitrate,
            user_limit=after.channel.user_limit,
            reason="PartyBot create member channel."
        )

        await member.move_to(channel, reason="PartyBot move user.")
        voice_channel_owners[member.id] = channel.id

if __name__ == "__main__":
    storage = Storage("partybot.db")
    client.run(TOKEN)
