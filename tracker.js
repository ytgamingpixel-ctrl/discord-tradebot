require('dotenv').config();
const fs = require('node:fs');
const path = require('node:path');
const {
  ActionRowBuilder,
  ButtonBuilder,
  ButtonStyle,
  EmbedBuilder,
  StringSelectMenuBuilder,
} = require('discord.js');

const STATE_FILE = path.join(__dirname, 'stats-state.json');
const MAX_DAYS = 35;
const ONE_DAY_MS = 24 * 60 * 60 * 1000;
const STAR_CITIZEN_MATCH = /star\s*citizen/i;
const THUMBNAIL_URL = 'https://robertsspaceindustries.com/media/zlgck6fw560rdr/logo/SPACEWHLE-Logo.png';
const TRACKED_ROLE_NAME = (process.env.TRACKED_ROLE_NAME || 'SPACEWHLE').replace(/^@/, '').trim();
const TRACKED_ROLE_ID = process.env.TRACKED_ROLE_ID || null;

function safeReadJson(filePath, fallback) {
  try {
    if (!fs.existsSync(filePath)) return fallback;
    return JSON.parse(fs.readFileSync(filePath, 'utf8'));
  } catch {
    return fallback;
  }
}

function safeWriteJson(filePath, value) {
  try {
    fs.writeFileSync(filePath, JSON.stringify(value, null, 2), 'utf8');
  } catch (error) {
    console.error('Failed to write tracker state:', error);
  }
}

function now() {
  return Date.now();
}

function getDayKey(timestamp = now()) {
  return new Date(timestamp).toISOString().slice(0, 10);
}

function formatDisplayDate(dayKey) {
  const date = new Date(`${dayKey}T00:00:00.000Z`);
  return date.toLocaleDateString('en-GB', { day: 'numeric', month: 'short' });
}

function formatHours(seconds) {
  return `${(Number(seconds || 0) / 3600).toFixed(1)}h`;
}

function formatNumber(value) {
  return Number(value || 0).toLocaleString('en-GB');
}

function formatSessionLength(startedAt) {
  if (!startedAt) return '—';
  return formatHours(Math.max(0, Math.floor((now() - startedAt) / 1000)));
}

function clampFieldText(text, fallback = 'No data yet.') {
  const value = String(text || '').trim();
  if (!value) return fallback;
  return value.length > 1024 ? `${value.slice(0, 1021)}…` : value;
}

function createDailyStatMap() {
  return {};
}

function cleanupDailyMap(map) {
  if (!map || typeof map !== 'object') return;
  const cutoff = new Date(now() - MAX_DAYS * ONE_DAY_MS).toISOString().slice(0, 10);

  for (const key of Object.keys(map)) {
    if (key < cutoff) delete map[key];
  }
}

function createEmptyUser(userId, username = 'Unknown User') {
  return {
    userId,
    username,
    messages: createDailyStatMap(),
    voiceSeconds: createDailyStatMap(),
    starCitizenSeconds: createDailyStatMap(),
    totals: {
      messages: 0,
      voiceSeconds: 0,
      starCitizenSeconds: 0,
    },
    current: {
      voiceStartedAt: null,
      voiceChannelId: null,
      starCitizenStartedAt: null,
      tracked: false,
    },
  };
}

function createEmptyTextChannel(channelId, name = 'Unknown Channel') {
  return {
    channelId,
    name,
    messages: createDailyStatMap(),
    totals: { messages: 0 },
  };
}

function createEmptyVoiceChannel(channelId, name = 'Unknown Voice') {
  return {
    channelId,
    name,
    voiceSeconds: createDailyStatMap(),
    totals: { voiceSeconds: 0 },
  };
}

function parseLegacyDays(customId, fallback = 7) {
  const parsed = Number(String(customId || '').split(':').pop());
  return [1, 7, 14, 30].includes(parsed) ? parsed : fallback;
}

function encodeStatsButton(action, panel, targetId, days, category = 'overview', showTime = false, graphMenuEnabled = false) {
  return `stats:${action}:${panel}:${targetId || 'global'}:${days}:${category}:${showTime ? 1 : 0}:${graphMenuEnabled ? 1 : 0}`;
}

function decodeStatsButton(customId) {
  const parts = String(customId || '').split(':');

  if (parts.length >= 8 && parts[0] === 'stats') {
    return {
      mode: 'modern',
      action: parts[1],
      panel: parts[2],
      targetId: parts[3],
      days: [1, 7, 14, 30].includes(Number(parts[4])) ? Number(parts[4]) : 7,
      category: parts[5] || 'overview',
      showTime: parts[6] === '1',
      graphMenuEnabled: parts[7] === '1',
    };
  }

  if (parts.length >= 7 && parts[0] === 'stats') {
    return {
      mode: 'modern',
      action: parts[1],
      panel: parts[2],
      targetId: parts[3],
      days: [1, 7, 14, 30].includes(Number(parts[4])) ? Number(parts[4]) : 7,
      category: parts[5] || 'overview',
      showTime: parts[6] === '1',
      graphMenuEnabled: false,
    };
  }

  if (parts.length >= 4 && parts[0] === 'stats') {
    return {
      mode: 'legacy',
      action: 'range',
      panel: parts[1],
      targetId: parts[2],
      days: parseLegacyDays(customId, 7),
      category: 'overview',
      showTime: false,
      graphMenuEnabled: false,
    };
  }

  return null;
}

function encodeStatsSelectMenu(panel, targetId, days, category = 'overview', showTime = false, graphMenuEnabled = false) {
  return `statsmenu:${panel}:${targetId || 'global'}:${days}:${category}:${showTime ? 1 : 0}:${graphMenuEnabled ? 1 : 0}`;
}

function decodeStatsSelectMenu(customId) {
  const parts = String(customId || '').split(':');

  if (parts.length >= 7 && parts[0] === 'statsmenu') {
    return {
      panel: parts[1],
      targetId: parts[2],
      days: [1, 7, 14, 30].includes(Number(parts[3])) ? Number(parts[3]) : 7,
      category: parts[4] || 'overview',
      showTime: parts[5] === '1',
      graphMenuEnabled: parts[6] === '1',
    };
  }

  return null;
}

class StatsTracker {
  constructor(client) {
    this.client = client;
    this.state = safeReadJson(STATE_FILE, {
      users: {},
      textChannels: {},
      voiceChannels: {},
      concurrency: {},
      peaks: {},
      meta: { createdAt: now(), lastSavedAt: null },
    });

    this.state.users ??= {};
    this.state.textChannels ??= {};
    this.state.voiceChannels ??= {};
    this.state.concurrency ??= {};
    this.state.peaks ??= {};
    this.state.meta ??= { createdAt: now(), lastSavedAt: null };

    this.saveTimer = null;
  }

  init() {
    this.cleanupState();
    this.attachEvents();
    setInterval(() => this.flushOpenSessions(), 60 * 1000).unref();
    setInterval(() => this.save(), 2 * 60 * 1000).unref();
  }

  attachEvents() {
    this.client.on('messageCreate', message => {
      try {
        if (!message.guild || message.author?.bot) return;
        if (!this.memberHasTrackedRole(message.member)) return;

        const username = message.member?.displayName || message.author?.username || 'Unknown User';
        const channelName = message.channel?.name || message.channel?.id || 'unknown-channel';
        this.incrementMessage(message.author.id, username, message.channel.id, channelName);
      } catch (error) {
        console.error('messageCreate tracker error:', error);
      }
    });

    this.client.on('voiceStateUpdate', (oldState, newState) => {
      try {
        const member = newState.member || oldState.member;
        if (!member || member.user?.bot) return;

        const userId = member.id;
        const username = member.displayName || member.user.username || 'Unknown User';
        const oldChannelId = oldState.channelId || null;
        const newChannelId = newState.channelId || null;
        const newChannelName = newState.channel?.name || newChannelId || 'Unknown Voice';
        const tracked = this.memberHasTrackedRole(member);

        if (!tracked) {
          this.stopVoice(userId, username);
          this.stopStarCitizen(userId, username);
          const user = this.getUser(userId, username);
          user.current.tracked = false;
          return;
        }

        const user = this.getUser(userId, username);
        user.current.tracked = true;

        if (!oldChannelId && newChannelId) {
          this.startVoice(userId, username, newChannelId, newChannelName);
        } else if (oldChannelId && !newChannelId) {
          this.stopVoice(userId, username);
        } else if (oldChannelId && newChannelId && oldChannelId !== newChannelId) {
          this.stopVoice(userId, username);
          this.startVoice(userId, username, newChannelId, newChannelName);
        }
      } catch (error) {
        console.error('voiceStateUpdate tracker error:', error);
      }
    });

    this.client.on('presenceUpdate', (oldPresence, newPresence) => {
      try {
        const presence = newPresence || oldPresence;
        const member = presence?.member;
        if (!member || member.user?.bot) return;

        const userId = member.id;
        const username = member.displayName || member.user.username || 'Unknown User';
        const tracked = this.memberHasTrackedRole(member);

        if (!tracked) {
          this.stopStarCitizen(userId, username);
          this.stopVoice(userId, username);
          const user = this.getUser(userId, username);
          user.current.tracked = false;
          this.captureConcurrencySnapshot(member.guild);
          return;
        }

        const user = this.getUser(userId, username);
        user.current.tracked = true;

        const oldPlaying = this.isPlayingStarCitizen(oldPresence);
        const newPlaying = this.isPlayingStarCitizen(newPresence);

        if (!oldPlaying && newPlaying) {
          this.startStarCitizen(userId, username);
        } else if (oldPlaying && !newPlaying) {
          this.stopStarCitizen(userId, username);
        } else {
          this.touchUser(userId, username);
        }

        this.captureConcurrencySnapshot(member.guild);
      } catch (error) {
        console.error('presenceUpdate tracker error:', error);
      }
    });

    this.client.on('guildMemberRemove', member => {
      try {
        if (member.user?.bot) return;
        const username = member.displayName || member.user.username || 'Unknown User';
        this.stopVoice(member.id, username);
        this.stopStarCitizen(member.id, username);
      } catch (error) {
        console.error('guildMemberRemove tracker error:', error);
      }
    });

    this.client.on('guildMemberUpdate', (oldMember, newMember) => {
      try {
        if (newMember.user?.bot) return;

        const hadRole = this.memberHasTrackedRole(oldMember);
        const hasRole = this.memberHasTrackedRole(newMember);
        const username = newMember.displayName || newMember.user.username || 'Unknown User';
        const user = this.getUser(newMember.id, username);

        if (!hadRole && hasRole) {
          user.current.tracked = true;

          if (newMember.voice?.channelId) {
            this.startVoice(
              newMember.id,
              username,
              newMember.voice.channelId,
              newMember.voice.channel?.name || newMember.voice.channelId,
            );
          }

          if (this.isPlayingStarCitizen(newMember.presence)) {
            this.startStarCitizen(newMember.id, username);
          }
        }

        if (hadRole && !hasRole) {
          user.current.tracked = false;
          this.stopVoice(newMember.id, username);
          this.stopStarCitizen(newMember.id, username);
        }

        this.captureConcurrencySnapshot(newMember.guild);
      } catch (error) {
        console.error('guildMemberUpdate tracker error:', error);
      }
    });
  }

  memberHasTrackedRole(member) {
    if (!member || !member.roles?.cache) return false;
    if (TRACKED_ROLE_ID) return member.roles.cache.has(TRACKED_ROLE_ID);

    const target = TRACKED_ROLE_NAME.toLowerCase();
    return member.roles.cache.some(role => String(role.name || '').trim().toLowerCase() === target);
  }

  async hydrateGuild(guild) {
    if (!guild) return;

    try {
      await guild.members.fetch();
    } catch (error) {
      console.error(`Failed to fetch members for guild ${guild.id}:`, error.message);
    }

    for (const member of guild.members.cache.values()) {
      if (member.user?.bot) continue;
      if (!this.memberHasTrackedRole(member)) continue;

      const username = member.displayName || member.user.username || 'Unknown User';
      const user = this.touchUser(member.id, username);
      user.current.tracked = true;

      if (member.voice?.channelId) {
        this.startVoice(
          member.id,
          username,
          member.voice.channelId,
          member.voice.channel?.name || member.voice.channelId,
        );
      }

      if (this.isPlayingStarCitizen(member.presence)) {
        this.startStarCitizen(member.id, username);
      }
    }

    this.captureConcurrencySnapshot(guild);
    this.save();
  }

  isPlayingStarCitizen(presence) {
    const activities = presence?.activities || [];
    return activities.some(activity =>
      STAR_CITIZEN_MATCH.test(activity?.name || '') ||
      STAR_CITIZEN_MATCH.test(activity?.details || ''),
    );
  }

  getUser(userId, username = 'Unknown User') {
    if (!this.state.users[userId]) {
      this.state.users[userId] = createEmptyUser(userId, username);
    }

    this.touchUser(userId, username);
    return this.state.users[userId];
  }

  touchUser(userId, username) {
    const user = this.state.users[userId] || createEmptyUser(userId, username);
    if (username) user.username = username;
    this.state.users[userId] = user;
    return user;
  }

  getTextChannel(channelId, channelName = 'Unknown Channel') {
    if (!channelId) return null;
    if (!this.state.textChannels) this.state.textChannels = {};

    if (!this.state.textChannels[channelId]) {
      this.state.textChannels[channelId] = createEmptyTextChannel(channelId, channelName);
    }

    const channel = this.state.textChannels[channelId];
    if (channelName) channel.name = channelName;
    return channel;
  }

  getVoiceChannel(channelId, channelName = 'Unknown Voice') {
    if (!channelId) return null;
    if (!this.state.voiceChannels) this.state.voiceChannels = {};

    if (!this.state.voiceChannels[channelId]) {
      this.state.voiceChannels[channelId] = createEmptyVoiceChannel(channelId, channelName);
    }

    const channel = this.state.voiceChannels[channelId];
    if (channelName) channel.name = channelName;
    return channel;
  }

  incrementMessage(userId, username, channelId = null, channelName = null) {
    const user = this.getUser(userId, username);
    const dayKey = getDayKey();

    user.messages[dayKey] = (user.messages[dayKey] || 0) + 1;
    user.totals.messages += 1;

    const textChannel = this.getTextChannel(channelId, channelName);
    if (textChannel) {
      textChannel.messages[dayKey] = (textChannel.messages[dayKey] || 0) + 1;
      textChannel.totals.messages += 1;
    }

    this.scheduleSave();
  }

  addSeconds(mapKey, totalsKey, userId, username, seconds, dayKey = getDayKey()) {
    if (seconds <= 0) return;

    const user = this.getUser(userId, username);
    user[mapKey][dayKey] = (user[mapKey][dayKey] || 0) + seconds;
    user.totals[totalsKey] += seconds;
    this.scheduleSave();
  }

  addVoiceChannelSeconds(channelId, channelName, seconds, dayKey = getDayKey()) {
    if (!channelId || seconds <= 0) return;

    const voiceChannel = this.getVoiceChannel(channelId, channelName);
    if (!voiceChannel) return;

    voiceChannel.voiceSeconds[dayKey] = (voiceChannel.voiceSeconds[dayKey] || 0) + seconds;
    voiceChannel.totals.voiceSeconds += seconds;
    this.scheduleSave();
  }

  startVoice(userId, username, channelId, channelName) {
    const user = this.getUser(userId, username);

    if (!user.current.voiceStartedAt) {
      user.current.voiceStartedAt = now();
      user.current.voiceChannelId = channelId || null;
      if (channelId) this.getVoiceChannel(channelId, channelName);
      return;
    }

    user.current.voiceChannelId = channelId || user.current.voiceChannelId;
    if (channelId) this.getVoiceChannel(channelId, channelName);
  }

  stopVoice(userId, username) {
    const user = this.getUser(userId, username);
    if (!user.current.voiceStartedAt) return;

    const elapsed = Math.max(0, Math.floor((now() - user.current.voiceStartedAt) / 1000));
    const channelId = user.current.voiceChannelId || null;
    const channelName = channelId ? (this.state.voiceChannels[channelId]?.name || channelId) : 'Unknown Voice';

    user.current.voiceStartedAt = null;
    user.current.voiceChannelId = null;

    this.addSeconds('voiceSeconds', 'voiceSeconds', userId, username, elapsed);
    this.addVoiceChannelSeconds(channelId, channelName, elapsed);
  }

  startStarCitizen(userId, username) {
    const user = this.getUser(userId, username);
    if (!user.current.starCitizenStartedAt) {
      user.current.starCitizenStartedAt = now();
    }
  }

  stopStarCitizen(userId, username) {
    const user = this.getUser(userId, username);
    if (!user.current.starCitizenStartedAt) return;

    const elapsed = Math.max(0, Math.floor((now() - user.current.starCitizenStartedAt) / 1000));
    user.current.starCitizenStartedAt = null;
    this.addSeconds('starCitizenSeconds', 'starCitizenSeconds', userId, username, elapsed);
  }

  flushOpenSessions() {
    const stamp = now();
    const dayKey = getDayKey(stamp);

    for (const user of Object.values(this.state.users)) {
      if (!user.current?.tracked) continue;

      if (user.current.voiceStartedAt) {
        const elapsed = Math.max(0, Math.floor((stamp - user.current.voiceStartedAt) / 1000));
        user.current.voiceStartedAt = stamp;

        user.voiceSeconds[dayKey] = (user.voiceSeconds[dayKey] || 0) + elapsed;
        user.totals.voiceSeconds += elapsed;

        if (user.current.voiceChannelId) {
          const channelId = user.current.voiceChannelId;
          const channelName = this.state.voiceChannels[channelId]?.name || channelId;
          this.addVoiceChannelSeconds(channelId, channelName, elapsed, dayKey);
        }
      }

      if (user.current.starCitizenStartedAt) {
        const elapsed = Math.max(0, Math.floor((stamp - user.current.starCitizenStartedAt) / 1000));
        user.current.starCitizenStartedAt = stamp;
        user.starCitizenSeconds[dayKey] = (user.starCitizenSeconds[dayKey] || 0) + elapsed;
        user.totals.starCitizenSeconds += elapsed;
      }
    }

    this.captureAllConcurrencySnapshots();
    this.scheduleSave();
  }

  captureAllConcurrencySnapshots() {
    for (const guild of this.client.guilds.cache.values()) {
      this.captureConcurrencySnapshot(guild);
    }
  }

  captureConcurrencySnapshot(guild) {
    if (!guild) return;

    let count = 0;
    const names = [];

    for (const member of guild.members.cache.values()) {
      if (member.user?.bot) continue;
      if (!this.memberHasTrackedRole(member)) continue;

      if (this.isPlayingStarCitizen(member.presence)) {
        count += 1;
        names.push(member.displayName || member.user.username || 'Unknown User');
      }
    }

    const dayKey = getDayKey();
    if (!this.state.concurrency[dayKey]) this.state.concurrency[dayKey] = [];
    this.state.concurrency[dayKey].push({ ts: now(), count });
    this.state.concurrency[dayKey] = this.state.concurrency[dayKey].slice(-1500);

    if (!this.state.peaks[dayKey] || count >= this.state.peaks[dayKey].count) {
      this.state.peaks[dayKey] = { ts: now(), count, names: names.slice(0, 25) };
    }

    this.scheduleSave();
  }

  cleanupState() {
    for (const user of Object.values(this.state.users || {})) {
      cleanupDailyMap(user.messages || {});
      cleanupDailyMap(user.voiceSeconds || {});
      cleanupDailyMap(user.starCitizenSeconds || {});
    }

    for (const channel of Object.values(this.state.textChannels || {})) {
      cleanupDailyMap(channel.messages || {});
    }

    for (const channel of Object.values(this.state.voiceChannels || {})) {
      cleanupDailyMap(channel.voiceSeconds || {});
    }

    const cutoff = new Date(now() - MAX_DAYS * ONE_DAY_MS).toISOString().slice(0, 10);

    for (const key of Object.keys(this.state.concurrency || {})) {
      if (key < cutoff) delete this.state.concurrency[key];
    }

    for (const key of Object.keys(this.state.peaks || {})) {
      if (key < cutoff) delete this.state.peaks[key];
    }
  }

  scheduleSave() {
    if (this.saveTimer) return;

    this.saveTimer = setTimeout(() => {
      this.saveTimer = null;
      this.save();
    }, 5000).unref();
  }

  save() {
    this.cleanupState();
    this.state.meta.lastSavedAt = now();
    safeWriteJson(STATE_FILE, this.state);
  }

  getRangeDayKeys(days = 7) {
    const keys = [];
    for (let i = days - 1; i >= 0; i -= 1) {
      keys.push(getDayKey(now() - i * ONE_DAY_MS));
    }
    return keys;
  }

  sumMap(map, dayKeys) {
    return dayKeys.reduce((sum, dayKey) => sum + Number(map?.[dayKey] || 0), 0);
  }

  getTrackedMemberIds() {
    const ids = new Set();

    for (const guild of this.client.guilds.cache.values()) {
      for (const member of guild.members.cache.values()) {
        if (member.user?.bot) continue;
        if (this.memberHasTrackedRole(member)) ids.add(member.id);
      }
    }

    return ids;
  }

  getTrackedUsers() {
    const trackedIds = this.getTrackedMemberIds();
    return Array.from(trackedIds)
      .map(userId => this.state.users[userId])
      .filter(Boolean);
  }

  getLeaderboard(days = 7) {
    const dayKeys = this.getRangeDayKeys(days);
    const trackedUsers = this.getTrackedUsers();

    const rows = trackedUsers
      .map(user => ({
        userId: user.userId,
        username: user.username,
        messages: this.sumMap(user.messages, dayKeys),
        voiceSeconds: this.sumMap(user.voiceSeconds, dayKeys),
        starCitizenSeconds: this.sumMap(user.starCitizenSeconds, dayKeys),
      }))
      .filter(row => row.messages > 0 || row.voiceSeconds > 0 || row.starCitizenSeconds > 0);

    return {
      all: rows,
      messages: [...rows].sort((a, b) => b.messages - a.messages),
      voice: [...rows].sort((a, b) => b.voiceSeconds - a.voiceSeconds),
      starCitizen: [...rows].sort((a, b) => b.starCitizenSeconds - a.starCitizenSeconds),
      dayKeys,
      trackedUsers,
    };
  }

  getUserStats(userId, days = 7) {
    const user = this.state.users[userId];
    if (!user) return null;

    const dayKeys = this.getRangeDayKeys(days);

    return {
      userId,
      username: user.username,
      totals: {
        messages: this.sumMap(user.messages, dayKeys),
        voiceSeconds: this.sumMap(user.voiceSeconds, dayKeys),
        starCitizenSeconds: this.sumMap(user.starCitizenSeconds, dayKeys),
      },
      current: {
        voiceStartedAt: user.current?.voiceStartedAt || null,
        starCitizenStartedAt: user.current?.starCitizenStartedAt || null,
      },
      daily: dayKeys.map(dayKey => ({
        dayKey,
        label: formatDisplayDate(dayKey),
        messages: Number(user.messages?.[dayKey] || 0),
        voiceHours: Number((Number(user.voiceSeconds?.[dayKey] || 0) / 3600).toFixed(2)),
        starCitizenHours: Number((Number(user.starCitizenSeconds?.[dayKey] || 0) / 3600).toFixed(2)),
      })),
    };
  }

  getServerStats(days = 7) {
    const board = this.getLeaderboard(days);

    const daily = board.dayKeys.map(dayKey => ({
      dayKey,
      label: formatDisplayDate(dayKey),
      messages: board.trackedUsers.reduce((sum, user) => sum + Number(user.messages?.[dayKey] || 0), 0),
      voiceHours: Number(
        (
          board.trackedUsers.reduce((sum, user) => sum + Number(user.voiceSeconds?.[dayKey] || 0), 0) / 3600
        ).toFixed(2),
      ),
      starCitizenHours: Number(
        (
          board.trackedUsers.reduce((sum, user) => sum + Number(user.starCitizenSeconds?.[dayKey] || 0), 0) / 3600
        ).toFixed(2),
      ),
    }));

    return {
      totals: {
        messages: daily.reduce((sum, day) => sum + Number(day.messages || 0), 0),
        voiceSeconds: board.all.reduce((sum, row) => sum + Number(row.voiceSeconds || 0), 0),
        starCitizenSeconds: board.all.reduce((sum, row) => sum + Number(row.starCitizenSeconds || 0), 0),
      },
      daily,
    };
  }

  getUserRankings(userId, days) {
    const board = this.getLeaderboard(days);
    return {
      messages: board.messages.findIndex(row => row.userId === userId) + 1,
      voice: board.voice.findIndex(row => row.userId === userId) + 1,
      starCitizen: board.starCitizen.findIndex(row => row.userId === userId) + 1,
      totalTracked: board.all.length,
      board,
    };
  }

  getCurrentPlayers(guild) {
    if (!guild) return { count: 0, players: [] };

    const players = [];
    for (const member of guild.members.cache.values()) {
      if (member.user?.bot) continue;
      if (!this.memberHasTrackedRole(member)) continue;

      if (this.isPlayingStarCitizen(member.presence)) {
        players.push({
          id: member.id,
          name: member.displayName || member.user.username || 'Unknown User',
          startedAt: this.state.users[member.id]?.current?.starCitizenStartedAt || null,
        });
      }
    }

    players.sort((a, b) => a.name.localeCompare(b.name));
    return { count: players.length, players };
  }

  getPeakForRange(days = 7) {
    const dayKeys = this.getRangeDayKeys(days);
    let best = { count: 0, ts: null, names: [] };

    for (const dayKey of dayKeys) {
      const peak = this.state.peaks?.[dayKey];
      if (peak && peak.count >= best.count) best = peak;
    }

    return best;
  }

  buildStatsControlRow(panel, targetId, days, category = 'overview', showTime = false) {
    return new ActionRowBuilder().addComponents(
      new ButtonBuilder()
        .setCustomId(encodeStatsButton('refresh', panel, targetId, days, category, showTime))
        .setLabel('Refresh')
        .setStyle(ButtonStyle.Primary),
      new ButtonBuilder()
        .setCustomId(encodeStatsButton('time', panel, targetId, days, category, showTime))
        .setEmoji('🗓️')
        .setStyle(ButtonStyle.Secondary),
    );
  }

  buildRangeButtons(panel, targetId, days, category = 'overview', showTime = true) {
    const ranges = [1, 7, 14, 30];

    return new ActionRowBuilder().addComponents(
      ...ranges.map(range =>
        new ButtonBuilder()
          .setCustomId(encodeStatsButton('range', panel, targetId, range, category, showTime))
          .setLabel(`${range}d`)
          .setStyle(range === days ? ButtonStyle.Primary : ButtonStyle.Secondary),
      ),
    );
  }

  buildDualAxisChartUrl({ labels, messages, voiceHours, playtimeHours }) {
    const config = {
      type: 'line',
      data: {
        labels,
        datasets: [
          { label: 'Messages', data: messages, borderColor: '#60a5fa', yAxisID: 'y1', fill: false, tension: 0.3 },
          { label: 'VC Hours', data: voiceHours, borderColor: '#34d399', yAxisID: 'y', fill: false, tension: 0.3 },
          { label: 'SC Hours', data: playtimeHours, borderColor: '#f59e0b', yAxisID: 'y', fill: false, tension: 0.3 },
        ],
      },
      options: {
        plugins: { legend: { position: 'top' } },
        scales: {
          x: { grid: { display: false } },
          y: { beginAtZero: true, position: 'left', title: { display: true, text: 'Hours' } },
          y1: { beginAtZero: true, position: 'right', grid: { drawOnChartArea: false }, title: { display: true, text: 'Messages' } },
        },
      },
    };

    return `https://quickchart.io/chart?width=1000&height=400&devicePixelRatio=2&version=4&c=${encodeURIComponent(JSON.stringify(config))}`;
  }

  buildPlayersChartUrl({ labels, players }) {
    const config = {
      type: 'line',
      data: {
        labels,
        datasets: [
          { label: 'Peak Players', data: players, borderColor: '#a78bfa', fill: false, tension: 0.3 },
        ],
      },
      options: {
        plugins: { legend: { display: true } },
        scales: {
          x: { grid: { display: false } },
          y: { beginAtZero: true, title: { display: true, text: 'Players' } },
        },
      },
    };

    return `https://quickchart.io/chart?width=1000&height=400&devicePixelRatio=2&version=4&c=${encodeURIComponent(JSON.stringify(config))}`;
  }

  formatTopThree(rows, type) {
    const top = rows.slice(0, 3);
    if (!top.length) return '```\nNo data yet.\n```';

    const lines = top.map((row, index) => {
      let value = '0';
      if (type === 'messages') value = formatNumber(row.messages);
      if (type === 'voice') value = formatHours(row.voiceSeconds);
      if (type === 'starCitizen') value = formatHours(row.starCitizenSeconds);
      return `${index + 1}. ${row.username} | ${value}`;
    });

    return `\`\`\`\n${clampFieldText(lines.join('\n'))}\n\`\`\``;
  }

  buildBaseEmbed(title) {
    return new EmbedBuilder().setColor(0x5865f2).setTitle(title).setThumbnail(THUMBNAIL_URL);
  }

  buildTopEmbed(days = 7, category = 'overview', showTime = false) {
    const board = this.getLeaderboard(days);
    const labels = board.dayKeys.map(formatDisplayDate);

    const messagesSeries = board.dayKeys.map(dayKey =>
      board.trackedUsers.reduce((sum, user) => sum + Number(user.messages?.[dayKey] || 0), 0),
    );
    const voiceSeries = board.dayKeys.map(dayKey =>
      Number((board.trackedUsers.reduce((sum, user) => sum + Number(user.voiceSeconds?.[dayKey] || 0), 0) / 3600).toFixed(2)),
    );
    const playtimeSeries = board.dayKeys.map(dayKey =>
      Number((board.trackedUsers.reduce((sum, user) => sum + Number(user.starCitizenSeconds?.[dayKey] || 0), 0) / 3600).toFixed(2)),
    );

    const totalMessages = messagesSeries.reduce((sum, value) => sum + value, 0);
    const totalVoiceHours = voiceSeries.reduce((sum, value) => sum + value, 0);
    const totalPlaytimeHours = playtimeSeries.reduce((sum, value) => sum + value, 0);

    const embed = this.buildBaseEmbed(`Server Stats · Last ${days} Day${days === 1 ? '' : 's'}`)
      .setImage(this.buildDualAxisChartUrl({
        labels,
        messages: messagesSeries,
        voiceHours: voiceSeries,
        playtimeHours: playtimeSeries,
      }))
      .addFields(
        {
          name: 'Overview',
          value: `\`\`\`\nMembers   | ${formatNumber(board.trackedUsers.length)}\nMessages  | ${formatNumber(totalMessages)}\nVC Hours  | ${totalVoiceHours.toFixed(1)}h\nSC Hours  | ${totalPlaytimeHours.toFixed(1)}h\n\`\`\``,
          inline: false,
        },
        { name: 'Top Messages', value: this.formatTopThree(board.messages, 'messages'), inline: true },
        { name: 'Top VC Hours', value: this.formatTopThree(board.voice, 'voice'), inline: true },
        { name: 'Top SC Hours', value: this.formatTopThree(board.starCitizen, 'starCitizen'), inline: true },
      );

    const components = [this.buildStatsControlRow('top', 'global', days, category, showTime)];
    if (showTime) components.push(this.buildRangeButtons('top', 'global', days, category, true));

    return { embeds: [embed], components };
  }

  buildUserStatsEmbed(userId, days = 7, showTime = false) {
    const memberStillTracked = this.getTrackedMemberIds().has(userId);
    const stats = this.getUserStats(userId, days);

    if (!stats || !memberStillTracked) {
      const components = [this.buildStatsControlRow('user', userId, days, 'overview', showTime)];
      if (showTime) components.push(this.buildRangeButtons('user', userId, days, 'overview', true));
      return {
        content: 'No tracked data for that user yet.',
        embeds: [],
        components,
      };
    }

    const rankings = this.getUserRankings(userId, days);
    const labels = stats.daily.map(day => day.label);
    const messageValues = stats.daily.map(day => day.messages);
    const voiceValues = stats.daily.map(day => day.voiceHours);
    const playtimeValues = stats.daily.map(day => day.starCitizenHours);

    const embed = this.buildBaseEmbed(`${stats.username} · Last ${days} Day${days === 1 ? '' : 's'}`)
      .setImage(this.buildDualAxisChartUrl({
        labels,
        messages: messageValues,
        voiceHours: voiceValues,
        playtimeHours: playtimeValues,
      }))
      .addFields(
        {
          name: 'Summary',
          value: `\`\`\`\nMessages  | ${formatNumber(stats.totals.messages)}\nVC Hours  | ${(stats.totals.voiceSeconds / 3600).toFixed(1)}h\nSC Hours  | ${(stats.totals.starCitizenSeconds / 3600).toFixed(1)}h\nVC Live   | ${formatSessionLength(stats.current.voiceStartedAt)}\nSC Live   | ${formatSessionLength(stats.current.starCitizenStartedAt)}\n\`\`\``,
          inline: false,
        },
        {
          name: 'Rank',
          value: `\`\`\`\nMessages  | #${rankings.messages || '-'}\nVC Hours  | #${rankings.voice || '-'}\nSC Hours  | #${rankings.starCitizen || '-'}\nTracked   | ${formatNumber(rankings.totalTracked)}\n\`\`\``,
          inline: true,
        },
        { name: 'Top Messages', value: this.formatTopThree(rankings.board.messages, 'messages'), inline: true },
        { name: 'Top VC Hours', value: this.formatTopThree(rankings.board.voice, 'voice'), inline: true },
        { name: 'Top SC Hours', value: this.formatTopThree(rankings.board.starCitizen, 'starCitizen'), inline: true },
      );

    const components = [this.buildStatsControlRow('user', userId, days, 'overview', showTime)];
    if (showTime) components.push(this.buildRangeButtons('user', userId, days, 'overview', true));

    return { embeds: [embed], components };
  }

  buildPlayersEmbed(guild, days = 7, showTime = false) {
    const current = this.getCurrentPlayers(guild);
    const peak = this.getPeakForRange(days);
    const dayKeys = this.getRangeDayKeys(days);
    const labels = dayKeys.map(formatDisplayDate);
    const playerSeries = dayKeys.map(dayKey => Number(this.state.peaks?.[dayKey]?.count || 0));

    const currentTable = current.players.length
      ? `\`\`\`\n${current.players.slice(0, 12).map(player => `${player.name} | ${formatSessionLength(player.startedAt)}`).join('\n')}\n\`\`\``
      : '```\nNo one currently detected in Star Citizen.\n```';

    const embed = this.buildBaseEmbed(`Players · Last ${days} Day${days === 1 ? '' : 's'}`)
      .setImage(this.buildPlayersChartUrl({ labels, players: playerSeries }))
      .addFields(
        {
          name: 'Overview',
          value: `\`\`\`\nLive Now   | ${current.count}\nPeak       | ${peak.count || 0}\nPeak Time  | ${peak.ts ? new Date(peak.ts).toLocaleString('en-GB') : 'No data'}\n\`\`\``,
          inline: false,
        },
        {
          name: 'Current Players',
          value: clampFieldText(currentTable),
          inline: false,
        },
      );

    const components = [this.buildStatsControlRow('players', guild?.id || 'global', days, 'overview', showTime)];
    if (showTime) components.push(this.buildRangeButtons('players', guild?.id || 'global', days, 'overview', true));

    return { embeds: [embed], components };
  }

  async handleSelectMenu() {
    return null;
  }

  async handleButton(interaction) {
    const decoded = decodeStatsButton(interaction.customId);
    if (!decoded) return null;

    if (decoded.mode === 'legacy') {
      if (decoded.panel === 'top') return interaction.editReply(this.buildTopEmbed(decoded.days, 'overview', false));
      if (decoded.panel === 'user') return interaction.editReply(this.buildUserStatsEmbed(decoded.targetId, decoded.days, false));
      if (decoded.panel === 'players') return interaction.editReply(this.buildPlayersEmbed(interaction.guild, decoded.days, false));
      return null;
    }

    const { action, panel, targetId, days, category, showTime } = decoded;

    if (action === 'refresh') {
      if (panel === 'top') return interaction.editReply(this.buildTopEmbed(days, category, showTime));
      if (panel === 'user') return interaction.editReply(this.buildUserStatsEmbed(targetId, days, showTime));
      if (panel === 'players') return interaction.editReply(this.buildPlayersEmbed(interaction.guild, days, showTime));
      return null;
    }

    if (action === 'time') {
      const nextShowTime = !showTime;
      if (panel === 'top') return interaction.editReply(this.buildTopEmbed(days, category, nextShowTime));
      if (panel === 'user') return interaction.editReply(this.buildUserStatsEmbed(targetId, days, nextShowTime));
      if (panel === 'players') return interaction.editReply(this.buildPlayersEmbed(interaction.guild, days, nextShowTime));
      return null;
    }

    if (action === 'range') {
      if (panel === 'top') return interaction.editReply(this.buildTopEmbed(days, category, true));
      if (panel === 'user') return interaction.editReply(this.buildUserStatsEmbed(targetId, days, true));
      if (panel === 'players') return interaction.editReply(this.buildPlayersEmbed(interaction.guild, days, true));
      return null;
    }

    return null;
  }

  getAllowedCategories(panel) {
    if (panel === 'players') return ['overview', 'players'];
    return ['overview', 'messages', 'voice', 'starCitizen'];
  }

  normalizeCategory(panel, category, graphMenuEnabled = false) {
    if (!graphMenuEnabled) return 'overview';
    return this.getAllowedCategories(panel).includes(category) ? category : 'overview';
  }

  getMetricConfig(category) {
    const configs = {
      messages: {
        label: 'Messages',
        axisTitle: 'Messages',
        color: '#60a5fa',
        fillColor: 'rgba(96, 165, 250, 0.18)',
        getLeaderboardValue: row => Number(row.messages || 0),
        getDailyValue: day => Number(day.messages || 0),
        formatValue: value => `${formatNumber(value)} message${Number(value) === 1 ? '' : 's'}`,
      },
      voice: {
        label: 'Voice Activity',
        axisTitle: 'Hours',
        color: '#34d399',
        fillColor: 'rgba(52, 211, 153, 0.18)',
        getLeaderboardValue: row => Number((Number(row.voiceSeconds || 0) / 3600).toFixed(2)),
        getDailyValue: day => Number(day.voiceHours || 0),
        formatValue: value => `${Number(value || 0).toFixed(1)} hours`,
      },
      starCitizen: {
        label: 'Star Citizen Activity',
        axisTitle: 'Hours',
        color: '#f59e0b',
        fillColor: 'rgba(245, 158, 11, 0.18)',
        getLeaderboardValue: row => Number((Number(row.starCitizenSeconds || 0) / 3600).toFixed(2)),
        getDailyValue: day => Number(day.starCitizenHours || 0),
        formatValue: value => `${Number(value || 0).toFixed(1)} hours`,
      },
      players: {
        label: 'Player Peaks',
        axisTitle: 'Players',
        color: '#a78bfa',
        fillColor: 'rgba(167, 139, 250, 0.18)',
        getLeaderboardValue: row => Number(row.players || 0),
        getDailyValue: day => Number(day.players || 0),
        formatValue: value => `${formatNumber(value)} player${Number(value) === 1 ? '' : 's'}`,
      },
    };

    return configs[category] || null;
  }

  getBoardRows(board, category) {
    if (category === 'messages') return board.messages;
    if (category === 'voice') return board.voice;
    if (category === 'starCitizen') return board.starCitizen;
    return [];
  }

  formatBubbleSummary(items) {
    return clampFieldText(
      items
        .filter(item => item && item.label)
        .map(item => `\`${item.label}\` **${item.value}**`)
        .join('\n'),
    );
  }

  formatLeaderboardBubble(rows, category, limit = 5) {
    const metric = this.getMetricConfig(category);
    const top = rows.slice(0, limit);
    if (!metric || !top.length) return '`No data yet.`';

    const valueHeader = category === 'messages' ? 'Messages' : category === 'players' ? 'Players' : 'Hours';
    const rowsForDisplay = top.map((row, index) => {
      const rawName = `${index + 1}. ${String(row.username || 'Unknown User').trim()}`;
      const value = category === 'messages'
        ? formatNumber(row.messages)
        : category === 'players'
          ? formatNumber(row.players || 0)
          : Number(metric.getLeaderboardValue(row)).toFixed(1);

      return { rawName, value };
    });

    const nameWidth = Math.min(
      20,
      Math.max(
        'User'.length,
        ...rowsForDisplay.map(item => Math.min(item.rawName.length, 20)),
      ),
    );
    const valueWidth = Math.max(valueHeader.length, ...rowsForDisplay.map(item => item.value.length));
    const truncate = value => (value.length > nameWidth ? `${value.slice(0, Math.max(1, nameWidth - 3))}...` : value);

    const lines = [
      `\`${'User'.padEnd(nameWidth)} ${valueHeader.padStart(valueWidth)}\``,
      ...rowsForDisplay.map(item => `\`${truncate(item.rawName).padEnd(nameWidth)} ${item.value.padStart(valueWidth)}\``),
    ];

    return clampFieldText(lines.join('\n'));
  }

  buildStatsControlRow(panel, targetId, days, category = 'overview', showTime = false, graphMenuEnabled = false) {
    const activeCategory = this.normalizeCategory(panel, category, graphMenuEnabled);

    return new ActionRowBuilder().addComponents(
      new ButtonBuilder()
        .setCustomId(encodeStatsButton('refresh', panel, targetId, days, activeCategory, showTime, graphMenuEnabled))
        .setEmoji('\u{1F504}')
        .setStyle(ButtonStyle.Primary),
      new ButtonBuilder()
        .setCustomId(encodeStatsButton('time', panel, targetId, days, activeCategory, showTime, graphMenuEnabled))
        .setEmoji('\u{1F552}')
        .setStyle(showTime ? ButtonStyle.Primary : ButtonStyle.Secondary),
      new ButtonBuilder()
        .setCustomId(encodeStatsButton('graph', panel, targetId, days, activeCategory, showTime, graphMenuEnabled))
        .setEmoji('\u{1F4CA}')
        .setStyle(graphMenuEnabled ? ButtonStyle.Primary : ButtonStyle.Secondary),
    );
  }

  buildRangeButtons(panel, targetId, days, category = 'overview', showTime = true, graphMenuEnabled = false) {
    const activeCategory = this.normalizeCategory(panel, category, graphMenuEnabled);
    const ranges = [1, 7, 14, 30];

    return new ActionRowBuilder().addComponents(
      ...ranges.map(range =>
        new ButtonBuilder()
          .setCustomId(encodeStatsButton('range', panel, targetId, range, activeCategory, showTime, graphMenuEnabled))
          .setLabel(`${range}d`)
          .setStyle(range === days ? ButtonStyle.Primary : ButtonStyle.Secondary),
      ),
    );
  }

  buildCategorySelectRow(panel, targetId, days, category = 'overview', showTime = false, graphMenuEnabled = false) {
    const activeCategory = this.normalizeCategory(panel, category, graphMenuEnabled);
    const options = panel === 'players'
      ? [
          {
            label: 'Players Graph',
            value: 'players',
            description: 'Show the peak player chart for the range.',
            default: activeCategory === 'players',
          },
        ]
      : [
          ...(panel === 'top'
            ? [{
                label: 'Overview',
                value: 'overview',
                description: 'Show the leaderboard overview cards.',
                default: activeCategory === 'overview',
              }]
            : []),
          {
            label: 'Messages Graph',
            value: 'messages',
            description: panel === 'top' ? 'Compare top users by messages.' : 'Show this member daily messages.',
            default: activeCategory === 'messages',
          },
          {
            label: 'Voice Activity Graph',
            value: 'voice',
            description: panel === 'top' ? 'Compare top users by voice activity.' : 'Show this member daily voice activity.',
            default: activeCategory === 'voice',
          },
          {
            label: 'Star Citizen Activity Graph',
            value: 'starCitizen',
            description: panel === 'top' ? 'Compare top users by Star Citizen activity.' : 'Show this member daily Star Citizen activity.',
            default: activeCategory === 'starCitizen',
          },
        ];

    return new ActionRowBuilder().addComponents(
      new StringSelectMenuBuilder()
        .setCustomId(encodeStatsSelectMenu(panel, targetId, days, activeCategory, showTime, graphMenuEnabled))
        .setPlaceholder(
          panel === 'top' ? 'Choose view' : 'Choose graph',
        )
        .setMinValues(1)
        .setMaxValues(1)
        .addOptions(options),
    );
  }

  buildLeaderboardChartUrl({ labels, values, label, color, axisTitle }) {
    const config = {
      type: 'bar',
      data: {
        labels,
        datasets: [
          {
            label,
            data: values,
            backgroundColor: color,
            borderColor: color,
            borderWidth: 1,
            borderRadius: 14,
            borderSkipped: false,
            maxBarThickness: 30,
          },
        ],
      },
      options: {
        indexAxis: 'y',
        layout: { padding: 22 },
        plugins: {
          legend: { display: false },
          title: {
            display: true,
            text: label,
            color: '#111827',
            font: { size: 22, weight: 'bold' },
            padding: { bottom: 18 },
          },
        },
        scales: {
          x: {
            beginAtZero: true,
            grid: { color: '#d1d5db' },
            ticks: {
              color: '#111827',
              font: { size: 16, weight: 'bold' },
            },
            title: {
              display: true,
              text: axisTitle,
              color: '#111827',
              font: { size: 18, weight: 'bold' },
            },
          },
          y: {
            grid: { display: false },
            ticks: {
              color: '#111827',
              font: { size: 17, weight: 'bold' },
            },
          },
        },
      },
    };

    return `https://quickchart.io/chart?width=1200&height=560&devicePixelRatio=2&backgroundColor=%23f3f4f6&version=4&c=${encodeURIComponent(JSON.stringify(config))}`;
  }

  buildTrendChartUrl({ labels, values, label, color, fillColor, axisTitle }) {
    const config = {
      type: 'line',
      data: {
        labels,
        datasets: [
          {
            label,
            data: values,
            borderColor: color,
            backgroundColor: fillColor,
            fill: true,
            pointBackgroundColor: color,
            pointBorderColor: color,
            pointRadius: 5,
            pointHoverRadius: 6,
            borderWidth: 4,
            tension: 0.3,
          },
        ],
      },
      options: {
        layout: { padding: 22 },
        plugins: {
          legend: { display: false },
          title: {
            display: true,
            text: label,
            color: '#111827',
            font: { size: 22, weight: 'bold' },
            padding: { bottom: 18 },
          },
        },
        scales: {
          x: {
            grid: { color: '#d1d5db' },
            ticks: {
              color: '#111827',
              font: { size: 16, weight: 'bold' },
              maxRotation: 0,
            },
          },
          y: {
            beginAtZero: true,
            grid: { color: '#d1d5db' },
            ticks: {
              color: '#111827',
              font: { size: 16, weight: 'bold' },
            },
            title: {
              display: true,
              text: axisTitle,
              color: '#111827',
              font: { size: 18, weight: 'bold' },
            },
          },
        },
      },
    };

    return `https://quickchart.io/chart?width=1200&height=560&devicePixelRatio=2&backgroundColor=%23f3f4f6&version=4&c=${encodeURIComponent(JSON.stringify(config))}`;
  }

  buildBaseEmbed(title) {
    return new EmbedBuilder().setColor(0x5865f2).setTitle(title).setThumbnail(THUMBNAIL_URL);
  }

  getTopSectionName(category) {
    if (category === 'messages') return '# Messages';
    if (category === 'voice') return '\u{1F50A} Voice Activity';
    if (category === 'starCitizen') return '\u{1F680} Star Citizen Activity';
    return 'Activity';
  }

  buildTopEmbed(days = 7, category = 'overview', showTime = false, graphMenuEnabled = false) {
    const activeCategory = this.normalizeCategory('top', category, graphMenuEnabled);
    const board = this.getLeaderboard(days);
    const embed = new EmbedBuilder()
      .setColor(0x3b3f45)
      .setTitle(`Top Activity - Last ${days} Day${days === 1 ? '' : 's'}`)
      .setFooter({
        text: `Server Lookback: Last ${days} Day${days === 1 ? '' : 's'} - Timezone: UTC`,
      });

    if (activeCategory === 'overview') {
      embed.addFields(
        { name: this.getTopSectionName('messages'), value: this.formatLeaderboardBubble(board.messages, 'messages', 3), inline: false },
        { name: this.getTopSectionName('voice'), value: this.formatLeaderboardBubble(board.voice, 'voice', 3), inline: false },
        { name: this.getTopSectionName('starCitizen'), value: this.formatLeaderboardBubble(board.starCitizen, 'starCitizen', 3), inline: false },
      );
    } else {
      const metric = this.getMetricConfig(activeCategory);
      const chartRows = this.getBoardRows(board, activeCategory).slice(0, 8);

      if (chartRows.length) {
        embed.setImage(this.buildLeaderboardChartUrl({
          labels: chartRows.map(row => row.username),
          values: chartRows.map(row => metric.getLeaderboardValue(row)),
          label: metric.label,
          color: metric.color,
          axisTitle: metric.axisTitle,
        }));
      }

      embed.addFields({
        name: this.getTopSectionName(activeCategory),
        value: this.formatLeaderboardBubble(chartRows, activeCategory, 8),
        inline: false,
      });
    }

    const components = [this.buildStatsControlRow('top', 'global', days, activeCategory, showTime, graphMenuEnabled)];
    if (showTime) components.push(this.buildRangeButtons('top', 'global', days, activeCategory, true, graphMenuEnabled));
    components.push(this.buildCategorySelectRow('top', 'global', days, activeCategory, showTime, graphMenuEnabled));

    return { embeds: [embed], components };
  }

  buildUserStatsEmbed(userId, days = 7, category = 'overview', showTime = false, graphMenuEnabled = false) {
    const activeCategory = this.normalizeCategory('user', category, graphMenuEnabled);
    const memberStillTracked = this.getTrackedMemberIds().has(userId);
    const stats = this.getUserStats(userId, days);

    const components = [this.buildStatsControlRow('user', userId, days, activeCategory, showTime, graphMenuEnabled)];
    if (showTime) components.push(this.buildRangeButtons('user', userId, days, activeCategory, true, graphMenuEnabled));
    components.push(this.buildCategorySelectRow('user', userId, days, activeCategory, showTime, graphMenuEnabled));

    if (!stats || !memberStillTracked) {
      return {
        content: 'No tracked data for that user yet.',
        embeds: [],
        components,
      };
    }

    const rankings = this.getUserRankings(userId, days);
    const embed = this.buildBaseEmbed(`@${stats.username} - Last ${days} Day${days === 1 ? '' : 's'}`).addFields(
        {
          name: 'Summary',
          value: this.formatBubbleSummary([
            { label: 'Messages', value: formatNumber(stats.totals.messages) },
            { label: 'Voice Activity', value: `${(stats.totals.voiceSeconds / 3600).toFixed(1)}h` },
            { label: 'Star Citizen Activity', value: `${(stats.totals.starCitizenSeconds / 3600).toFixed(1)}h` },
          ]),
          inline: true,
        },
        {
          name: 'Rank',
          value: this.formatBubbleSummary([
            { label: 'Messages', value: `#${rankings.messages || '-'}` },
            { label: 'Voice Activity', value: `#${rankings.voice || '-'}` },
            { label: 'Star Citizen Activity', value: `#${rankings.starCitizen || '-'}` },
          ]),
          inline: true,
        },
      );

    if (activeCategory !== 'overview') {
      const metric = this.getMetricConfig(activeCategory);
      const labels = stats.daily.map(day => day.label);
      const values = stats.daily.map(day => metric.getDailyValue(day));

      if (labels.length) {
        embed.setImage(this.buildTrendChartUrl({
          labels,
          values,
          label: metric.label,
          color: metric.color,
          fillColor: metric.fillColor,
          axisTitle: metric.axisTitle,
        }));
      }
    }

    return {
      content: `<@${userId}>`,
      embeds: [embed],
      components,
    };
  }

  buildServerStatsEmbed(days = 7, category = 'overview', showTime = false, graphMenuEnabled = false) {
    const activeCategory = this.normalizeCategory('server', category, graphMenuEnabled);
    const stats = this.getServerStats(days);

    const components = [this.buildStatsControlRow('server', 'global', days, activeCategory, showTime, graphMenuEnabled)];
    if (showTime) components.push(this.buildRangeButtons('server', 'global', days, activeCategory, true, graphMenuEnabled));
    components.push(this.buildCategorySelectRow('server', 'global', days, activeCategory, showTime, graphMenuEnabled));

    const embed = this.buildBaseEmbed(`Server - Last ${days} Day${days === 1 ? '' : 's'}`).addFields({
      name: 'Summary',
      value: this.formatBubbleSummary([
        { label: 'Messages', value: formatNumber(stats.totals.messages) },
        { label: 'Voice Activity', value: `${(stats.totals.voiceSeconds / 3600).toFixed(1)}h` },
        { label: 'Star Citizen Activity', value: `${(stats.totals.starCitizenSeconds / 3600).toFixed(1)}h` },
      ]),
      inline: false,
    });

    if (activeCategory !== 'overview') {
      const metric = this.getMetricConfig(activeCategory);
      const labels = stats.daily.map(day => day.label);
      const values = stats.daily.map(day => metric.getDailyValue(day));

      if (labels.length) {
        embed.setImage(this.buildTrendChartUrl({
          labels,
          values,
          label: metric.label,
          color: metric.color,
          fillColor: metric.fillColor,
          axisTitle: metric.axisTitle,
        }));
      }
    }

    return { embeds: [embed], components };
  }

  buildPlayersEmbed(guild, days = 1, category = 'overview', showTime = false, graphMenuEnabled = false) {
    const activeCategory = this.normalizeCategory('players', category, graphMenuEnabled);
    const current = this.getCurrentPlayers(guild);
    const peak = this.getPeakForRange(days);
    const dayKeys = this.getRangeDayKeys(days);
    const labels = dayKeys.map(formatDisplayDate);
    const playerSeries = dayKeys.map(dayKey => Number(this.state.peaks?.[dayKey]?.count || 0));

    const currentPlayers = current.players.length
      ? current.players.slice(0, 12).map((player, index) => `\`${index + 1}. ${player.name}\``).join('\n')
      : 'No one currently detected in Star Citizen.';

    const embed = this.buildBaseEmbed(`Players - Last ${days} Day${days === 1 ? '' : 's'}`).addFields(
        {
          name: 'Overview',
          value: this.formatBubbleSummary([
            { label: 'Live Now', value: formatNumber(current.count) },
            { label: 'Peak', value: formatNumber(peak.count || 0) },
            { label: 'Peak Time', value: peak.ts ? new Date(peak.ts).toLocaleString('en-GB') : 'No data' },
          ]),
          inline: false,
        },
        {
          name: 'Online Now',
          value: currentPlayers,
          inline: false,
        },
      );

    if (activeCategory === 'players' && labels.length) {
      const metric = this.getMetricConfig('players');
      embed.setImage(this.buildTrendChartUrl({
        labels,
        values: playerSeries,
        label: metric.label,
        color: metric.color,
        fillColor: metric.fillColor,
        axisTitle: metric.axisTitle,
      }));
    }

    const components = [this.buildStatsControlRow('players', guild?.id || 'global', days, activeCategory, showTime, graphMenuEnabled)];
    if (showTime) components.push(this.buildRangeButtons('players', guild?.id || 'global', days, activeCategory, true, graphMenuEnabled));
    components.push(this.buildCategorySelectRow('players', guild?.id || 'global', days, activeCategory, showTime, graphMenuEnabled));

    return { embeds: [embed], components };
  }

  buildPanel(panel, targetId, days, category = 'overview', showTime = false, graphMenuEnabled = false, guild = null) {
    if (panel === 'top') return this.buildTopEmbed(days, category, showTime, graphMenuEnabled);
    if (panel === 'user') return this.buildUserStatsEmbed(targetId, days, category, showTime, graphMenuEnabled);
    if (panel === 'server') return this.buildServerStatsEmbed(days, category, showTime, graphMenuEnabled);
    if (panel === 'players') return this.buildPlayersEmbed(guild, days, category, showTime, graphMenuEnabled);
    return null;
  }

  async handleSelectMenu(interaction) {
    const decoded = decodeStatsSelectMenu(interaction.customId);
    if (!decoded) return null;

    const selectedCategory = this.getAllowedCategories(decoded.panel).includes(interaction.values?.[0])
      ? interaction.values[0]
      : 'overview';

    const nextGraphMenuEnabled = decoded.graphMenuEnabled || selectedCategory !== 'overview';

    return interaction.editReply(
      this.buildPanel(
        decoded.panel,
        decoded.targetId,
        decoded.days,
        selectedCategory,
        decoded.showTime,
        nextGraphMenuEnabled,
        interaction.guild,
      ),
    );
  }

  async handleButton(interaction) {
    const decoded = decodeStatsButton(interaction.customId);
    if (!decoded) return null;

    if (decoded.mode === 'legacy') {
      return interaction.editReply(
        this.buildPanel(decoded.panel, decoded.targetId, decoded.days, 'overview', false, false, interaction.guild),
      );
    }

    const {
      action,
      panel,
      targetId,
      days,
      category,
      showTime,
      graphMenuEnabled,
    } = decoded;

    const activeCategory = this.normalizeCategory(panel, category, graphMenuEnabled);

    if (action === 'refresh') {
      return interaction.editReply(
        this.buildPanel(panel, targetId, days, activeCategory, showTime, graphMenuEnabled, interaction.guild),
      );
    }

    if (action === 'time') {
      return interaction.editReply(
        this.buildPanel(panel, targetId, days, activeCategory, !showTime, graphMenuEnabled, interaction.guild),
      );
    }

    if (action === 'graph') {
      const nextGraphMenuEnabled = !graphMenuEnabled;
      const nextCategory = nextGraphMenuEnabled ? activeCategory : 'overview';

      return interaction.editReply(
        this.buildPanel(panel, targetId, days, nextCategory, showTime, nextGraphMenuEnabled, interaction.guild),
      );
    }

    if (action === 'range') {
      return interaction.editReply(
        this.buildPanel(panel, targetId, days, activeCategory, true, graphMenuEnabled, interaction.guild),
      );
    }

    return null;
  }
}

module.exports = {
  StatsTracker,
};
