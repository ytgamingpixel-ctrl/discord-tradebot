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

const COLORS = {
  brand: 0x5865f2,
  messages: 0x60a5fa,
  voice: 0x34d399,
  playtime: 0xf59e0b,
  panel: 0x0f172a,
};

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

function formatNumber(value) {
  return Number(value || 0).toLocaleString('en-GB');
}

function formatHours(seconds) {
  return `${(Number(seconds || 0) / 3600).toFixed(1)}h`;
}

function clampFieldText(text, fallback = 'No data yet.') {
  const value = String(text || '').trim();
  if (!value) return fallback;
  return value.length > 1024 ? `${value.slice(0, 1021)}…` : value;
}

function parseLegacyDays(customId, fallback = 7) {
  const parsed = Number(String(customId || '').split(':').pop());
  return [1, 7, 14, 30].includes(parsed) ? parsed : fallback;
}

function encodeStatsButton(action, panel, targetId, days, category = 'overview', showTime = false) {
  return `stats:${action}:${panel}:${targetId || 'global'}:${days}:${category}:${showTime ? 1 : 0}`;
}

function decodeStatsButton(customId) {
  const parts = String(customId || '').split(':');

  if (parts.length >= 7 && parts[0] === 'stats') {
    return {
      mode: 'modern',
      action: parts[1],
      panel: parts[2],
      targetId: parts[3],
      days: [1, 7, 14, 30].includes(Number(parts[4])) ? Number(parts[4]) : 7,
      category: parts[5] || 'overview',
      showTime: parts[6] === '1',
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
    };
  }

  return null;
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
    totals: {
      messages: 0,
    },
  };
}

function createEmptyVoiceChannel(channelId, name = 'Unknown Voice') {
  return {
    channelId,
    name,
    voiceSeconds: createDailyStatMap(),
    totals: {
      voiceSeconds: 0,
    },
  };
}

function makeBars(values, {
  width = 12,
  empty = '▱',
  full = '▰',
  formatter = value => String(value),
} = {}) {
  const clean = values.map(v => Number(v || 0));
  const max = Math.max(...clean, 0);

  if (max <= 0) {
    return clean.map(value => `${empty.repeat(width)} ${formatter(value)}`);
  }

  return clean.map(value => {
    const filled = Math.max(0, Math.round((value / max) * width));
    return `${full.repeat(filled)}${empty.repeat(Math.max(0, width - filled))} ${formatter(value)}`;
  });
}

function formatSessionLength(startedAt) {
  if (!startedAt) return '—';
  return formatHours(Math.max(0, Math.floor((now() - startedAt) / 1000)));
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
              newMember.voice.channel?.name || newMember.voice.channelId
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

    if (TRACKED_ROLE_ID) {
      return member.roles.cache.has(TRACKED_ROLE_ID);
    }

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
          member.voice.channel?.name || member.voice.channelId
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
      STAR_CITIZEN_MATCH.test(activity?.details || '')
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
      messages: [...rows].sort((a, b) => b.messages - a.messages).slice(0, 10),
      voice: [...rows].sort((a, b) => b.voiceSeconds - a.voiceSeconds).slice(0, 10),
      starCitizen: [...rows].sort((a, b) => b.starCitizenSeconds - a.starCitizenSeconds).slice(0, 10),
      dayKeys,
      trackedUsers,
    };
  }

  getTextChannelLeaderboard(days = 7) {
    const dayKeys = this.getRangeDayKeys(days);

    return Object.values(this.state.textChannels || {})
      .map(channel => ({
        channelId: channel.channelId,
        name: channel.name,
        messages: this.sumMap(channel.messages, dayKeys),
      }))
      .filter(row => row.messages > 0)
      .sort((a, b) => b.messages - a.messages)
      .slice(0, 10);
  }

  getVoiceChannelLeaderboard(days = 7) {
    const dayKeys = this.getRangeDayKeys(days);

    return Object.values(this.state.voiceChannels || {})
      .map(channel => ({
        channelId: channel.channelId,
        name: channel.name,
        voiceSeconds: this.sumMap(channel.voiceSeconds, dayKeys),
      }))
      .filter(row => row.voiceSeconds > 0)
      .sort((a, b) => b.voiceSeconds - a.voiceSeconds)
      .slice(0, 10);
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

  formatLeaderboardRows(rows, type) {
    if (!rows.length) return 'No data yet.';

    return rows.map((row, index) => {
      let value = '0';
      if (type === 'messages') value = `${formatNumber(row.messages)} msgs`;
      if (type === 'voice') value = formatHours(row.voiceSeconds);
      if (type === 'starCitizen') value = formatHours(row.starCitizenSeconds);
      return `**${index + 1}.** ${row.username} — ${value}`;
    }).join('\n');
  }

  formatTextChannelRows(rows) {
    if (!rows.length) return 'No data yet.';

    return rows.map((row, index) => {
      const mention = row.channelId ? `<#${row.channelId}>` : row.name;
      return `**${index + 1}.** ${mention} — ${formatNumber(row.messages)} msgs`;
    }).join('\n');
  }

  formatVoiceChannelRows(rows) {
    if (!rows.length) return 'No data yet.';

    return rows.map((row, index) => {
      const mention = row.channelId ? `<#${row.channelId}>` : row.name;
      return `**${index + 1}.** ${mention} — ${formatHours(row.voiceSeconds)}`;
    }).join('\n');
  }

  buildMiniChartEmbed(title, color, labels, values, formatter, description) {
    const bars = makeBars(values, { formatter });
    const lines = labels.map((label, index) => `\`${label.padEnd(6, ' ')}\` ${bars[index]}`);
    const chunk = clampFieldText(lines.join('\n'), 'No data yet.');

    return new EmbedBuilder()
      .setColor(color)
      .setTitle(title)
      .setDescription(description)
      .setThumbnail(THUMBNAIL_URL)
      .addFields({
        name: 'Trend',
        value: `\`\`\`\n${chunk}\n\`\`\``,
        inline: false,
      })
      .setFooter({ text: 'SPACEWHLE • smooth stat panel' });
  }

  buildTopSelectRow(days = 7, category = 'overview') {
    return new ActionRowBuilder().addComponents(
      new StringSelectMenuBuilder()
        .setCustomId(`stats:view:top:global:${days}`)
        .setPlaceholder('Switch leaderboard view')
        .addOptions([
          {
            label: 'Overview',
            value: 'overview',
            description: 'Full statboard summary',
            default: category === 'overview',
          },
          {
            label: 'Messages',
            value: 'messages',
            description: 'Message leaderboard focus',
            default: category === 'messages',
          },
          {
            label: 'Voice',
            value: 'voice',
            description: 'Voice leaderboard focus',
            default: category === 'voice',
          },
          {
            label: 'Playtime',
            value: 'starCitizen',
            description: 'Star Citizen playtime focus',
            default: category === 'starCitizen',
          },
          {
            label: 'Channels',
            value: 'channels',
            description: 'Top text and voice channels',
            default: category === 'channels',
          },
        ])
    );
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
          .setStyle(range === days ? ButtonStyle.Primary : ButtonStyle.Secondary)
      )
    );
  }

  buildTopEmbed(days = 7, category = 'overview', showTime = false) {
    const board = this.getLeaderboard(days);
    const textChannels = this.getTextChannelLeaderboard(days);
    const voiceChannels = this.getVoiceChannelLeaderboard(days);
    const labels = board.dayKeys.map(formatDisplayDate);

    const messagesSeries = board.dayKeys.map(dayKey =>
      board.trackedUsers.reduce((sum, user) => sum + Number(user.messages?.[dayKey] || 0), 0)
    );

    const voiceSeries = board.dayKeys.map(dayKey =>
      Number((board.trackedUsers.reduce((sum, user) => sum + Number(user.voiceSeconds?.[dayKey] || 0), 0) / 3600).toFixed(2))
    );

    const playtimeSeries = board.dayKeys.map(dayKey =>
      Number((board.trackedUsers.reduce((sum, user) => sum + Number(user.starCitizenSeconds?.[dayKey] || 0), 0) / 3600).toFixed(2))
    );

    const summaryEmbed = new EmbedBuilder()
      .setColor(COLORS.brand)
      .setTitle(`SPACEWHLE Statboard · Last ${days} Day${days === 1 ? '' : 's'}`)
      .setDescription(`Tracking **@${TRACKED_ROLE_NAME}** members only.`)
      .setThumbnail(THUMBNAIL_URL)
      .addFields(
        { name: 'Tracked members', value: formatNumber(board.trackedUsers.length), inline: true },
        { name: 'Window', value: `${days} day${days === 1 ? '' : 's'}`, inline: true },
        { name: 'View', value: category === 'starCitizen' ? 'Playtime' : `${category.charAt(0).toUpperCase()}${category.slice(1)}`, inline: true },
      )
      .setFooter({ text: 'SPACEWHLE • premium statboard' });

    if (category === 'messages') {
      summaryEmbed.addFields({
        name: 'Top message senders',
        value: clampFieldText(this.formatLeaderboardRows(board.messages, 'messages')),
        inline: false,
      });
    } else if (category === 'voice') {
      summaryEmbed.addFields({
        name: 'Top voice members',
        value: clampFieldText(this.formatLeaderboardRows(board.voice, 'voice')),
        inline: false,
      });
    } else if (category === 'starCitizen') {
      summaryEmbed.addFields({
        name: 'Top playtime members',
        value: clampFieldText(this.formatLeaderboardRows(board.starCitizen, 'starCitizen')),
        inline: false,
      });
    } else if (category === 'channels') {
      summaryEmbed.addFields(
        {
          name: 'Top text channels',
          value: clampFieldText(this.formatTextChannelRows(textChannels)),
          inline: true,
        },
        {
          name: 'Top voice channels',
          value: clampFieldText(this.formatVoiceChannelRows(voiceChannels)),
          inline: true,
        }
      );
    } else {
      summaryEmbed.addFields(
        {
          name: 'Top messages',
          value: clampFieldText(this.formatLeaderboardRows(board.messages, 'messages')),
          inline: true,
        },
        {
          name: 'Top voice',
          value: clampFieldText(this.formatLeaderboardRows(board.voice, 'voice')),
          inline: true,
        },
        {
          name: 'Top playtime',
          value: clampFieldText(this.formatLeaderboardRows(board.starCitizen, 'starCitizen')),
          inline: true,
        }
      );
    }

    const embeds = [
      summaryEmbed,
      this.buildMiniChartEmbed(
        'Messages Trend',
        COLORS.messages,
        labels,
        messagesSeries,
        value => `${Math.round(value)}`,
        'Daily message activity'
      ),
      this.buildMiniChartEmbed(
        'Voice Hours Trend',
        COLORS.voice,
        labels,
        voiceSeries,
        value => `${Number(value).toFixed(1)}h`,
        'Daily voice activity'
      ),
      this.buildMiniChartEmbed(
        'Playtime Hours Trend',
        COLORS.playtime,
        labels,
        playtimeSeries,
        value => `${Number(value).toFixed(1)}h`,
        'Daily Star Citizen activity'
      ),
    ];

    const components = [
      this.buildTopSelectRow(days, category),
      this.buildStatsControlRow('top', 'global', days, category, showTime),
    ];

    if (showTime) {
      components.push(this.buildRangeButtons('top', 'global', days, category, true));
    }

    return { embeds, components };
  }

  buildUserStatsEmbed(userId, days = 7, showTime = false) {
    const memberStillTracked = this.getTrackedMemberIds().has(userId);
    const stats = this.getUserStats(userId, days);

    if (!stats || !memberStillTracked) {
      const components = [this.buildStatsControlRow('user', userId, days, 'overview', showTime)];
      if (showTime) {
        components.push(this.buildRangeButtons('user', userId, days, 'overview', true));
      }

      return {
        content: 'No tracked SPACEWHLE data for that user yet.',
        embeds: [],
        components,
      };
    }

    const labels = stats.daily.map(day => day.label);
    const messageValues = stats.daily.map(day => day.messages);
    const voiceValues = stats.daily.map(day => day.voiceHours);
    const playtimeValues = stats.daily.map(day => day.starCitizenHours);

    const summaryEmbed = new EmbedBuilder()
      .setColor(COLORS.brand)
      .setTitle(`${stats.username} · Stat Profile`)
      .setDescription(`Tracking **@${TRACKED_ROLE_NAME}** activity only.`)
      .setThumbnail(THUMBNAIL_URL)
      .addFields(
        { name: 'Messages', value: formatNumber(stats.totals.messages), inline: true },
        { name: 'Voice hours', value: formatHours(stats.totals.voiceSeconds), inline: true },
        { name: 'Playtime hours', value: formatHours(stats.totals.starCitizenSeconds), inline: true },
        { name: 'Current voice session', value: formatSessionLength(stats.current.voiceStartedAt), inline: true },
        { name: 'Current SC session', value: formatSessionLength(stats.current.starCitizenStartedAt), inline: true },
        { name: 'Window', value: `${days} day${days === 1 ? '' : 's'}`, inline: true },
      )
      .setFooter({ text: 'SPACEWHLE • live profile tracking' });

    const embeds = [
      summaryEmbed,
      this.buildMiniChartEmbed(
        `${stats.username} · Messages`,
        COLORS.messages,
        labels,
        messageValues,
        value => `${Math.round(value)}`,
        'Daily messages'
      ),
      this.buildMiniChartEmbed(
        `${stats.username} · Voice Hours`,
        COLORS.voice,
        labels,
        voiceValues,
        value => `${Number(value).toFixed(1)}h`,
        'Daily voice time'
      ),
      this.buildMiniChartEmbed(
        `${stats.username} · Playtime Hours`,
        COLORS.playtime,
        labels,
        playtimeValues,
        value => `${Number(value).toFixed(1)}h`,
        'Daily Star Citizen time'
      ),
    ];

    const components = [
      this.buildStatsControlRow('user', userId, days, 'overview', showTime),
    ];

    if (showTime) {
      components.push(this.buildRangeButtons('user', userId, days, 'overview', true));
    }

    return { embeds, components };
  }

  buildPlayersEmbed(guild, days = 7, showTime = false) {
    const current = this.getCurrentPlayers(guild);
    const peak = this.getPeakForRange(days);
    const dayKeys = this.getRangeDayKeys(days);

    const peakSeries = dayKeys.map(dayKey => Number(this.state.peaks?.[dayKey]?.count || 0));
    const labels = dayKeys.map(formatDisplayDate);

    const currentLines = current.players.length
      ? current.players.slice(0, 15).map(player => {
          const extra = player.startedAt ? ` · ${formatSessionLength(player.startedAt)} session` : '';
          return `• ${player.name}${extra}`;
        }).join('\n')
      : 'Nobody currently detected in Star Citizen.';

    const summaryEmbed = new EmbedBuilder()
      .setColor(COLORS.brand)
      .setTitle('Star Citizen Player Tracker')
      .setDescription(`Live player panel for **@${TRACKED_ROLE_NAME}**.`)
      .setThumbnail(THUMBNAIL_URL)
      .addFields(
        { name: 'Playing right now', value: String(current.count), inline: true },
        { name: `Peak in last ${days}d`, value: String(peak.count || 0), inline: true },
        { name: 'Peak time', value: peak.ts ? `<t:${Math.floor(peak.ts / 1000)}:f>` : 'No data yet', inline: true },
        { name: 'Current players', value: clampFieldText(currentLines, 'Nobody currently detected in Star Citizen.'), inline: false },
      )
      .setFooter({ text: 'SPACEWHLE • presence-based SC tracking' });

    const trendEmbed = this.buildMiniChartEmbed(
      'Peak Player Trend',
      COLORS.playtime,
      labels,
      peakSeries,
      value => `${Math.round(value)}`,
      'Daily peak Star Citizen presence'
    );

    const components = [
      this.buildStatsControlRow('players', guild?.id || 'global', days, 'overview', showTime),
    ];

    if (showTime) {
      components.push(this.buildRangeButtons('players', guild?.id || 'global', days, 'overview', true));
    }

    return {
      embeds: [summaryEmbed, trendEmbed],
      components,
    };
  }

  async handleSelectMenu(interaction) {
    const parts = String(interaction.customId || '').split(':');

    if (parts[0] !== 'stats' || parts[1] !== 'view') return null;

    const panel = parts[2] || 'top';
    const days = [1, 7, 14, 30].includes(Number(parts[4])) ? Number(parts[4]) : 7;
    const selected = interaction.values?.[0] || 'overview';

    if (panel === 'top') {
      return interaction.editReply(this.buildTopEmbed(days, selected, false));
    }

    return null;
  }

  async handleButton(interaction) {
    const decoded = decodeStatsButton(interaction.customId);
    if (!decoded) return null;

    if (decoded.mode === 'legacy') {
      if (decoded.panel === 'top') {
        return interaction.editReply(this.buildTopEmbed(decoded.days, 'overview', false));
      }

      if (decoded.panel === 'user') {
        return interaction.editReply(this.buildUserStatsEmbed(decoded.targetId, decoded.days, false));
      }

      if (decoded.panel === 'players') {
        return interaction.editReply(this.buildPlayersEmbed(interaction.guild, decoded.days, false));
      }

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
}

module.exports = {
  StatsTracker,
};