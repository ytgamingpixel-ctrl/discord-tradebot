require('dotenv').config();
const fs = require('node:fs');
const path = require('node:path');
const {
  ActionRowBuilder,
  ButtonBuilder,
  ButtonStyle,
  EmbedBuilder,
} = require('discord.js');

const STATE_FILE = path.join(__dirname, 'stats-state.json');
const MAX_DAYS = 35;
const ONE_DAY_MS = 24 * 60 * 60 * 1000;
const STAR_CITIZEN_MATCH = /star\s*citizen/i;
const THUMBNAIL_URL = 'https://robertsspaceindustries.com/media/zlgck6fw560rdr/logo/SPACEWHLE-Logo.png';

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

function rangeDaysFromCustomId(customId, fallback = 7) {
  const parsed = Number(String(customId || '').split(':').pop());
  return [1, 7, 14, 30].includes(parsed) ? parsed : fallback;
}

function createEmptyUser(userId, username = 'Unknown User') {
  return {
    userId,
    username,
    messages: {},
    voiceSeconds: {},
    starCitizenSeconds: {},
    totals: {
      messages: 0,
      voiceSeconds: 0,
      starCitizenSeconds: 0,
    },
    current: {
      voiceStartedAt: null,
      starCitizenStartedAt: null,
      lastKnownStarCitizen: false,
    },
  };
}

function cleanupDailyMap(map) {
  const cutoff = new Date(now() - MAX_DAYS * ONE_DAY_MS).toISOString().slice(0, 10);
  for (const key of Object.keys(map || {})) {
    if (key < cutoff) delete map[key];
  }
}

class StatsTracker {
  constructor(client) {
    this.client = client;
    this.state = safeReadJson(STATE_FILE, {
      users: {},
      concurrency: {},
      peaks: {},
      meta: { createdAt: now(), lastSavedAt: null },
    });
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
        this.incrementMessage(message.author.id, message.author.username || message.member?.displayName || 'Unknown User');
      } catch (error) {
        console.error('messageCreate tracker error:', error);
      }
    });

    this.client.on('voiceStateUpdate', (oldState, newState) => {
      try {
        const member = newState.member || oldState.member;
        if (!member || member.user?.bot) return;
        const userId = member.id;
        const username = member.user.username || member.displayName || 'Unknown User';
        const wasInVoice = Boolean(oldState.channelId);
        const isInVoice = Boolean(newState.channelId);

        if (!wasInVoice && isInVoice) {
          this.startVoice(userId, username);
        } else if (wasInVoice && !isInVoice) {
          this.stopVoice(userId, username);
        }
      } catch (error) {
        console.error('voiceStateUpdate tracker error:', error);
      }
    });

    this.client.on('presenceUpdate', async (oldPresence, newPresence) => {
      try {
        const presence = newPresence || oldPresence;
        const member = presence?.member;
        if (!member || member.user?.bot) return;
        const userId = member.id;
        const username = member.user.username || member.displayName || 'Unknown User';
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
        this.stopVoice(member.id, member.user.username || member.displayName || 'Unknown User');
        this.stopStarCitizen(member.id, member.user.username || member.displayName || 'Unknown User');
      } catch (error) {
        console.error('guildMemberRemove tracker error:', error);
      }
    });
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
      const username = member.user.username || member.displayName || 'Unknown User';
      this.touchUser(member.id, username);
      if (member.voice?.channelId) this.startVoice(member.id, username);
      if (this.isPlayingStarCitizen(member.presence)) this.startStarCitizen(member.id, username);
    }

    this.captureConcurrencySnapshot(guild);
    this.save();
  }

  isPlayingStarCitizen(presence) {
    const activities = presence?.activities || [];
    return activities.some(activity => STAR_CITIZEN_MATCH.test(activity?.name || '') || STAR_CITIZEN_MATCH.test(activity?.details || ''));
  }

  getUser(userId, username = 'Unknown User') {
    if (!this.state.users[userId]) this.state.users[userId] = createEmptyUser(userId, username);
    this.touchUser(userId, username);
    return this.state.users[userId];
  }

  touchUser(userId, username) {
    const user = this.state.users[userId] || createEmptyUser(userId, username);
    if (username) user.username = username;
    this.state.users[userId] = user;
    return user;
  }

  incrementMessage(userId, username) {
    const user = this.getUser(userId, username);
    const dayKey = getDayKey();
    user.messages[dayKey] = (user.messages[dayKey] || 0) + 1;
    user.totals.messages += 1;
    this.scheduleSave();
  }

  addSeconds(map, totalsKey, userId, username, seconds, dayKey = getDayKey()) {
    if (seconds <= 0) return;
    const user = this.getUser(userId, username);
    map = user[map];
    map[dayKey] = (map[dayKey] || 0) + seconds;
    user.totals[totalsKey] += seconds;
    this.scheduleSave();
  }

  startVoice(userId, username) {
    const user = this.getUser(userId, username);
    if (!user.current.voiceStartedAt) user.current.voiceStartedAt = now();
  }

  stopVoice(userId, username) {
    const user = this.getUser(userId, username);
    if (!user.current.voiceStartedAt) return;
    const elapsed = Math.max(0, Math.floor((now() - user.current.voiceStartedAt) / 1000));
    user.current.voiceStartedAt = null;
    this.addSeconds('voiceSeconds', 'voiceSeconds', userId, username, elapsed);
  }

  startStarCitizen(userId, username) {
    const user = this.getUser(userId, username);
    user.current.lastKnownStarCitizen = true;
    if (!user.current.starCitizenStartedAt) user.current.starCitizenStartedAt = now();
  }

  stopStarCitizen(userId, username) {
    const user = this.getUser(userId, username);
    user.current.lastKnownStarCitizen = false;
    if (!user.current.starCitizenStartedAt) return;
    const elapsed = Math.max(0, Math.floor((now() - user.current.starCitizenStartedAt) / 1000));
    user.current.starCitizenStartedAt = null;
    this.addSeconds('starCitizenSeconds', 'starCitizenSeconds', userId, username, elapsed);
  }

  flushOpenSessions() {
    const stamp = now();
    const dayKey = getDayKey(stamp);

    for (const user of Object.values(this.state.users)) {
      if (user.current.voiceStartedAt) {
        const elapsed = Math.max(0, Math.floor((stamp - user.current.voiceStartedAt) / 1000));
        user.current.voiceStartedAt = stamp;
        user.voiceSeconds[dayKey] = (user.voiceSeconds[dayKey] || 0) + elapsed;
        user.totals.voiceSeconds += elapsed;
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
    for (const user of Object.values(this.state.users)) {
      cleanupDailyMap(user.messages || {});
      cleanupDailyMap(user.voiceSeconds || {});
      cleanupDailyMap(user.starCitizenSeconds || {});
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

  getLeaderboard(days = 7) {
    const dayKeys = this.getRangeDayKeys(days);
    const rows = Object.values(this.state.users)
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
      daily: dayKeys.map(dayKey => ({
        dayKey,
        label: formatDisplayDate(dayKey),
        messages: Number(user.messages?.[dayKey] || 0),
        voiceHours: Number(user.voiceSeconds?.[dayKey] || 0) / 3600,
        starCitizenHours: Number(user.starCitizenSeconds?.[dayKey] || 0) / 3600,
      })),
    };
  }

  getCurrentPlayers(guild) {
    if (!guild) return { count: 0, players: [] };
    const players = [];
    for (const member of guild.members.cache.values()) {
      if (member.user?.bot) continue;
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

  buildQuickChartUrl(title, labels, datasets) {
    const config = {
      type: 'line',
      data: {
        labels,
        datasets,
      },
      options: {
        plugins: {
          title: { display: true, text: title },
          legend: { display: true },
        },
        scales: {
          y: { beginAtZero: true },
        },
      },
    };

    return `https://quickchart.io/chart?c=${encodeURIComponent(JSON.stringify(config))}`;
  }

  formatHours(seconds) {
    return `${(seconds / 3600).toFixed(1)}h`;
  }

  formatLeaderboardRows(rows, type) {
    if (!rows.length) return 'No data yet.';
    return rows.map((row, index) => {
      let value = '0';
      if (type === 'messages') value = row.messages.toLocaleString();
      if (type === 'voice') value = this.formatHours(row.voiceSeconds);
      if (type === 'starCitizen') value = this.formatHours(row.starCitizenSeconds);
      return `**${index + 1}.** ${row.username} — ${value}`;
    }).join('\n');
  }

  buildRangeButtons(view, targetId, days) {
    const ranges = [1, 7, 14, 30];
    return new ActionRowBuilder().addComponents(
      ...ranges.map(range => new ButtonBuilder()
        .setCustomId(`stats:${view}:${targetId || 'global'}:${range}`)
        .setLabel(`${range}d`)
        .setStyle(range === days ? ButtonStyle.Primary : ButtonStyle.Secondary))
    );
  }

  buildTopEmbed(days = 7) {
    const board = this.getLeaderboard(days);
    const labels = board.dayKeys.map(formatDisplayDate);
    const chartUrl = this.buildQuickChartUrl(
      `Server activity over ${days} day${days === 1 ? '' : 's'}`,
      labels,
      [
        {
          label: 'Messages',
          data: board.dayKeys.map(dayKey => Object.values(this.state.users).reduce((sum, user) => sum + Number(user.messages?.[dayKey] || 0), 0)),
          borderColor: '#60a5fa',
          fill: false,
        },
        {
          label: 'Voice hours',
          data: board.dayKeys.map(dayKey => Object.values(this.state.users).reduce((sum, user) => sum + Number(user.voiceSeconds?.[dayKey] || 0), 0) / 3600),
          borderColor: '#34d399',
          fill: false,
        },
        {
          label: 'Star Citizen hours',
          data: board.dayKeys.map(dayKey => Object.values(this.state.users).reduce((sum, user) => sum + Number(user.starCitizenSeconds?.[dayKey] || 0), 0) / 3600),
          borderColor: '#f59e0b',
          fill: false,
        },
      ]
    );

    const embed = new EmbedBuilder()
      .setColor(0x22d3ee)
      .setTitle(`Server leaderboards · last ${days} day${days === 1 ? '' : 's'}`)
      .setThumbnail(THUMBNAIL_URL)
      .setImage(chartUrl)
      .addFields(
        { name: 'Top voice hours', value: this.formatLeaderboardRows(board.voice, 'voice'), inline: true },
        { name: 'Top messages', value: this.formatLeaderboardRows(board.messages, 'messages'), inline: true },
        { name: 'Top Star Citizen hours', value: this.formatLeaderboardRows(board.starCitizen, 'starCitizen'), inline: true },
      )
      .setFooter({ text: 'Buttons change the tracked timescale.' });

    return { embeds: [embed], components: [this.buildRangeButtons('top', 'global', days)] };
  }

  buildUserStatsEmbed(userId, days = 7) {
    const stats = this.getUserStats(userId, days);
    if (!stats) {
      return {
        content: 'No tracked data for that user yet.',
        embeds: [],
        components: [this.buildRangeButtons('user', userId, days)],
      };
    }

    const chartUrl = this.buildQuickChartUrl(
      `${stats.username} · last ${days} day${days === 1 ? '' : 's'}`,
      stats.daily.map(day => day.label),
      [
        { label: 'Messages', data: stats.daily.map(day => day.messages), borderColor: '#60a5fa', fill: false },
        { label: 'Voice hours', data: stats.daily.map(day => day.voiceHours.toFixed(2)), borderColor: '#34d399', fill: false },
        { label: 'Star Citizen hours', data: stats.daily.map(day => day.starCitizenHours.toFixed(2)), borderColor: '#f59e0b', fill: false },
      ],
    );

    const embed = new EmbedBuilder()
      .setColor(0x38bdf8)
      .setTitle(`${stats.username} · activity stats`)
      .setThumbnail(THUMBNAIL_URL)
      .setImage(chartUrl)
      .addFields(
        { name: 'Messages', value: stats.totals.messages.toLocaleString(), inline: true },
        { name: 'Voice hours', value: `${(stats.totals.voiceSeconds / 3600).toFixed(1)}h`, inline: true },
        { name: 'Star Citizen hours', value: `${(stats.totals.starCitizenSeconds / 3600).toFixed(1)}h`, inline: true },
      )
      .setFooter({ text: 'Tracked from presence, voice state, and message events.' });

    return { embeds: [embed], components: [this.buildRangeButtons('user', userId, days)] };
  }

  buildPlayersEmbed(guild, days = 7) {
    const current = this.getCurrentPlayers(guild);
    const peak = this.getPeakForRange(days);
    const dayKeys = this.getRangeDayKeys(days);
    const chartUrl = this.buildQuickChartUrl(
      `Star Citizen players online · last ${days} day${days === 1 ? '' : 's'}`,
      dayKeys.map(formatDisplayDate),
      [{
        label: 'Peak concurrent players',
        data: dayKeys.map(dayKey => Number(this.state.peaks?.[dayKey]?.count || 0)),
        borderColor: '#a78bfa',
        fill: false,
      }],
    );

    const currentLines = current.players.length
      ? current.players.slice(0, 15).map(player => {
          const extra = player.startedAt ? ` · ${(Math.max(0, now() - player.startedAt) / 3600000).toFixed(1)}h this session` : '';
          return `• ${player.name}${extra}`;
        }).join('\n')
      : 'Nobody currently detected in Star Citizen.';

    const embed = new EmbedBuilder()
      .setColor(0xa78bfa)
      .setTitle('Star Citizen player tracker')
      .setThumbnail(THUMBNAIL_URL)
      .setImage(chartUrl)
      .addFields(
        { name: 'Playing right now', value: String(current.count), inline: true },
        { name: `Peak in last ${days}d`, value: String(peak.count || 0), inline: true },
        { name: 'Peak time', value: peak.ts ? `<t:${Math.floor(peak.ts / 1000)}:f>` : 'No data yet', inline: true },
        { name: 'Current players', value: currentLines.slice(0, 1024), inline: false },
      )
      .setFooter({ text: 'Presence must show Star Citizen to be tracked.' });

    return { embeds: [embed], components: [this.buildRangeButtons('players', guild?.id || 'global', days)] };
  }

  async handleButton(interaction) {
    const [, view, targetId] = String(interaction.customId || '').split(':');
    const days = rangeDaysFromCustomId(interaction.customId);

    if (view === 'top') {
      return interaction.editReply(this.buildTopEmbed(days));
    }

    if (view === 'user') {
      return interaction.editReply(this.buildUserStatsEmbed(targetId, days));
    }

    if (view === 'players') {
      return interaction.editReply(this.buildPlayersEmbed(interaction.guild, days));
    }

    return null;
  }
}

module.exports = {
  StatsTracker,
};