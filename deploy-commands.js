require('dotenv').config();
const { ChannelType, PermissionFlagsBits, REST, Routes, SlashCommandBuilder } = require('discord.js');

const commands = [
  new SlashCommandBuilder()
    .setName('ping')
    .setDescription('Test if the bot is alive')
    .toJSON(),

  new SlashCommandBuilder()
    .setName('promote')
    .setDescription('Promote a member, update roles, and post a promotion announcement')
    .setDMPermission(false)
    .setDefaultMemberPermissions(PermissionFlagsBits.ManageRoles)
    .addUserOption(option =>
      option
        .setName('user')
        .setDescription('Member to promote')
        .setRequired(true)
    )
    .addRoleOption(option =>
      option
        .setName('rank_role')
        .setDescription('Main rank role to assign')
        .setRequired(true)
    )
    .addStringOption(option =>
      option
        .setName('extra_roles')
        .setDescription('Optional extra roles to add, separated by commas or role mentions')
        .setRequired(false)
        .setMaxLength(500)
    )
    .addStringOption(option =>
      option
        .setName('remove_roles')
        .setDescription('Optional roles to remove, separated by commas or role mentions')
        .setRequired(false)
        .setMaxLength(500)
    )
    .addStringOption(option =>
      option
        .setName('custom_message')
        .setDescription('Optional text to add to the announcement')
        .setRequired(false)
        .setMaxLength(1200)
    )
    .addChannelOption(option =>
      option
        .setName('channel')
        .setDescription('Announcement channel, defaults to the configured promotions channel')
        .setRequired(false)
        .addChannelTypes(ChannelType.GuildText, ChannelType.GuildAnnouncement)
    )
    .toJSON(),

  new SlashCommandBuilder()
    .setName('citizen')
    .setDescription('Look up a public Star Citizen citizen dossier')
    .addStringOption(option =>
      option
        .setName('username')
        .setDescription('Star Citizen handle')
        .setRequired(false)
        .setMaxLength(80)
    )
    .addUserOption(option =>
      option
        .setName('user')
        .setDescription('Discord user with a linked RSI handle')
        .setRequired(false)
    )
    .toJSON(),

  new SlashCommandBuilder()
    .setName('me')
    .setDescription('Show your internal organisation profile')
    .toJSON(),

  new SlashCommandBuilder()
    .setName('route')
    .setDescription('Find the best hauling route')
    .addStringOption(option =>
      option
        .setName('ship')
        .setDescription('Ship name')
        .setRequired(true)
        .setAutocomplete(true)
    )
    .addIntegerOption(option =>
      option
        .setName('cargo')
        .setDescription('Optional cargo amount in SCU, defaults to full ship capacity')
        .setRequired(false)
        .setMinValue(1)
    )
    .addIntegerOption(option =>
      option
        .setName('budget')
        .setDescription('Optional aUEC budget for buying cargo')
        .setRequired(false)
        .setMinValue(1)
    )
    .addStringOption(option =>
      option
        .setName('location')
        .setDescription('Optional starting location')
        .setRequired(false)
        .setAutocomplete(true)
    )
    .addStringOption(option =>
      option
        .setName('finish')
        .setDescription('Optional finishing location')
        .setRequired(false)
        .setAutocomplete(true)
    )
    .toJSON(),

  new SlashCommandBuilder()
    .setName('best-routes')
    .setDescription('Show the best route for each cargo bracket from a starting location')
    .addStringOption(option =>
      option
        .setName('location')
        .setDescription('Required starting location')
        .setRequired(true)
        .setAutocomplete(true)
    )
    .addStringOption(option =>
      option
        .setName('finish')
        .setDescription('Optional finishing location')
        .setRequired(false)
        .setAutocomplete(true)
    )
    .toJSON(),

  new SlashCommandBuilder()
    .setName('location')
    .setDescription('Show grouped commodity shops under a location')
    .addStringOption(option =>
      option
        .setName('location')
        .setDescription('Main location name')
        .setRequired(true)
        .setAutocomplete(true)
    )
    .toJSON(),

  new SlashCommandBuilder()
    .setName('buyers')
    .setDescription('Show the best 5 buyers for a commodity')
    .addStringOption(option =>
      option
        .setName('commodity')
        .setDescription('Commodity name')
        .setRequired(true)
        .setAutocomplete(true)
    )
    .addIntegerOption(option =>
      option
        .setName('amount')
        .setDescription('Optional amount in SCU')
        .setRequired(false)
        .setMinValue(1)
    )
    .addStringOption(option =>
      option
        .setName('location')
        .setDescription('Optional buyer location filter')
        .setRequired(false)
        .setAutocomplete(true)
    )
    .toJSON(),

  new SlashCommandBuilder()
    .setName('players')
    .setDescription('Show who is currently detected playing Star Citizen and recent peaks')
    .toJSON(),

  new SlashCommandBuilder()
    .setName('top')
    .setDescription('Show leaderboards for voice activity, messages, and SC activity')
    .toJSON(),

  new SlashCommandBuilder()
    .setName('stats')
    .setDescription('Tracked stats commands')
    .addSubcommand(subcommand =>
      subcommand
        .setName('user')
        .setDescription('Show tracked stats for one server member')
        .addUserOption(option =>
          option
            .setName('user')
            .setDescription('Member to inspect, for example @gerald')
            .setRequired(true)
        )
    )
    .toJSON(),

  new SlashCommandBuilder()
    .setName('server')
    .setDescription('Show total tracked server activity stats')
    .toJSON(),

  new SlashCommandBuilder()
    .setName('ship')
    .setDescription('Show cargo information for a ship')
    .addStringOption(option =>
      option
        .setName('ship')
        .setDescription('Ship name')
        .setRequired(true)
        .setAutocomplete(true)
    )
    .toJSON(),
];

const rest = new REST({ version: '10' }).setToken(process.env.DISCORD_TOKEN);

(async () => {
  try {
    console.log('Registering slash commands...');
    await rest.put(
      Routes.applicationGuildCommands(process.env.CLIENT_ID, process.env.GUILD_ID),
      { body: commands }
    );
    console.log('Slash commands registered successfully.');
  } catch (error) {
    console.error('Failed to register commands:', error);
  }
})();
