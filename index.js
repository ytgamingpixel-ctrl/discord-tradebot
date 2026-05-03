require('dotenv').config();
const fs = require('node:fs');
const path = require('node:path');
const crypto = require('node:crypto');
const sqlite3 = require('sqlite3').verbose();
const express = require('express');
const voiceSessions = new Map();
const app = express(); 
const PORT = process.env.PORT || 8080;
app.use(express.json());
app.use(express.urlencoded({ extended: true }));

const {
  Client,
  GatewayIntentBits,
  Events,
  EmbedBuilder,
  ButtonBuilder,
  ButtonStyle,
  ActionRowBuilder,
  ChannelType,
  PermissionFlagsBits,
} = require('discord.js');

// ... (Keep all your existing StatsTracker, ship-data imports, and client setup right below this!) ...
const { StatsTracker, assertStatsImageRenderer } = require('./tracker');
const {
  ensureShipData,
  getShipChoices,
  getShipProfile,
  getShipSourceLabel,
} = require('./ship-data');

const client = new Client({
  intents: [
    GatewayIntentBits.Guilds,
    GatewayIntentBits.GuildMembers,
    GatewayIntentBits.GuildPresences,
    GatewayIntentBits.GuildMessages,
    GatewayIntentBits.GuildVoiceStates,
    GatewayIntentBits.MessageContent,
  ],
});

const tracker = new StatsTracker(client);
tracker.init();
const statsRendererBytes = assertStatsImageRenderer();
console.log(`Stats image renderer ready (${statsRendererBytes} byte self-test PNG).`);

const CACHE_TTL_MS = 15 * 60 * 1000;
const STATE_TTL_MS = 7 * 24 * 60 * 60 * 1000;
const ROUTE_HINT_TTL_MS = 30 * 60 * 1000;
const PRICE_HISTORY_TTL_MS = 12 * 60 * 60 * 1000;
const CITIZEN_CACHE_MINUTES = Number(process.env.CITIZEN_CACHE_MINUTES || 20);
const CITIZEN_CACHE_TTL_MS = (Number.isFinite(CITIZEN_CACHE_MINUTES) ? Math.max(5, CITIZEN_CACHE_MINUTES) : 20) * 60 * 1000;
const API_BASE_URL = String(process.env.API_BASE_URL || '').trim().replace(/\/+$/, '');
const API_AUTH_TOKEN = String(process.env.API_AUTH_TOKEN || '').trim();
const API_AUTH_HEADER = String(process.env.API_AUTH_HEADER || 'Authorization').trim();
const API_AUTH_SCHEME = String(process.env.API_AUTH_SCHEME || 'Bearer').trim();
const API_MEMBER_PROFILE_PATH = process.env.API_MEMBER_PROFILE_PATH || '/api/discord/{discordId}/profile';
const API_RSI_LINK_PATH = process.env.API_RSI_LINK_PATH || '/api/discord/{discordId}/rsi';
const PROMOTIONS_CHANNEL_ID = process.env.PROMOTIONS_CHANNEL_ID || null;
const PROMOTE_ALLOWED_ROLE_IDS = String(process.env.PROMOTE_ALLOWED_ROLE_IDS || '')
  .split(',')
  .map(value => value.trim())
  .filter(Boolean);

const EMBED_THUMBNAIL_URL =
  'https://robertsspaceindustries.com/media/zlgck6fw560rdr/logo/SPACEWHLE-Logo.png';
const EMBED_BANNER_URL =
  'https://s1.cdn.autoevolution.com/images/news/star-citizen-unveils-a-massive-space-hauler-crowdfunding-passes-400-million-175169_1.jpg';

const STATE_FILE = path.join(__dirname, 'route-state.json');

const cache = {
  lastUpdated: 0,
  groups: [],
  shortGroupNames: [],
  routes: [],
  commodityNames: [],
  terminalsById: new Map(),
  commodityRankings: new Map(),
  commodityRankingsByName: new Map(),
  routeHintsByCommodity: new Map(),
  priceHistory: new Map(),
};

const routeStates = new Map();
const citizenProfileCache = new Map();
const activeMessageLocks = new Set();
let marketDataWarmPromise = null;

const MONITOR_FILE = path.join(__dirname, 'monitor-state.json');
const ALERT_GUILD_ID = process.env.ALERT_GUILD_ID || null;
const ALERT_USER_ID = process.env.ALERT_USER_ID || null;
let heartbeatTimer = null;
let shuttingDown = false;

function normalizeText(value) {
  return String(value || '').trim().toLowerCase();
}

function isUnknownInteractionError(error) {
  return Number(error?.code) === 10062 || /Unknown interaction/i.test(String(error?.message || ''));
}

const PUBLIC_CHAT_COMMANDS = new Set([
  'citizen',
  'me',
  'route',
  'best-routes',
  'location',
  'buyers',
  'players',
  'top',
  'stats',
  'server',
  'ship',
]);

async function deferChatInputCommand(interaction) {
  if (interaction.deferred || interaction.replied) return;
  await interaction.deferReply({
    ephemeral: interaction.commandName === 'promote' || !PUBLIC_CHAT_COMMANDS.has(interaction.commandName),
  });
}

function clamp(value, min, max) {
  return Math.max(min, Math.min(max, value));
}

function toNumber(value, fallback = 0) {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : fallback;
}

function firstPositiveNumber(...values) {
  for (const value of values) {
    const parsed = Number(value);
    if (Number.isFinite(parsed) && parsed > 0) return parsed;
  }
  return 0;
}

function unixSecondsToMs(value) {
  const parsed = Number(value || 0);
  if (!Number.isFinite(parsed) || parsed <= 0) return 0;
  return parsed * 1000;
}

function formatAuec(value) {
  return `${Math.round(Number(value || 0)).toLocaleString('en-GB')} aUEC`;
}

function formatPercent(value, decimals = 1) {
  return `${Number(value || 0).toFixed(decimals)}%`;
}

function formatConfidence(value) {
  return `${Math.round(clamp(Number(value || 0), 0, 1) * 100)}%`;
}

function formatFreshnessAge(timestampMs) {
  if (!timestampMs) return 'Unknown';
  const hours = Math.max(0, (Date.now() - timestampMs) / (60 * 60 * 1000));
  if (hours < 1) return '<1h old';
  if (hours < 24) return `${hours.toFixed(hours >= 10 ? 0 : 1)}h old`;
  return `${(hours / 24).toFixed(1)}d old`;
}

function getArrayPayload(payload) {
  if (Array.isArray(payload)) return payload;
  if (Array.isArray(payload?.data)) return payload.data;
  if (Array.isArray(payload?.result)) return payload.result;
  if (Array.isArray(payload?.items)) return payload.items;
  return [];
}

function safeReadJson(filePath, fallback) {
  try {
    if (!fs.existsSync(filePath)) return fallback;
    const raw = fs.readFileSync(filePath, 'utf8');
    return JSON.parse(raw);
  } catch {
    return fallback;
  }
}

function safeWriteJson(filePath, value) {
  try {
    fs.writeFileSync(filePath, JSON.stringify(value, null, 2), 'utf8');
  } catch (error) {
    console.error('Failed writing state file:', error);
  }
}

function getAlertGuild(clientInstance = client) {
  if (!clientInstance) return null;
  if (ALERT_GUILD_ID && clientInstance.guilds.cache.has(ALERT_GUILD_ID)) {
    return clientInstance.guilds.cache.get(ALERT_GUILD_ID);
  }
  return clientInstance.guilds.cache.first() || null;
}

async function getAlertUser(clientInstance = client) {
  if (!clientInstance?.isReady?.()) return null;

  if (ALERT_USER_ID) {
    try {
      return await clientInstance.users.fetch(ALERT_USER_ID);
    } catch (error) {
      console.error('Failed to fetch ALERT_USER_ID:', error);
    }
  }

  const guild = getAlertGuild(clientInstance);
  if (!guild) return null;

  try {
    const owner = await guild.fetchOwner();
    return owner?.user || null;
  } catch (error) {
    console.error('Failed to fetch alert guild owner:', error);
    return null;
  }
}

function readMonitorState() {
  return safeReadJson(MONITOR_FILE, {
    running: false,
    cleanShutdown: true,
    lastHeartbeat: null,
    lastBootAt: null,
    lastShutdownAt: null,
    lastShutdownReason: null,
    lastCrashCode: null,
    lastCrashSource: null,
    guildId: null,
  });
}

function writeMonitorState(patch = {}) {
  const next = {
    ...readMonitorState(),
    ...patch,
  };
  safeWriteJson(MONITOR_FILE, next);
  return next;
}

function touchHeartbeat() {
  writeMonitorState({
    running: true,
    cleanShutdown: false,
    lastHeartbeat: Date.now(),
  });
}

function startHeartbeat() {
  if (heartbeatTimer) clearInterval(heartbeatTimer);
  touchHeartbeat();
  heartbeatTimer = setInterval(() => {
    touchHeartbeat();
  }, 60 * 1000);
  heartbeatTimer.unref();
}

function stopHeartbeat() {
  if (heartbeatTimer) {
    clearInterval(heartbeatTimer);
    heartbeatTimer = null;
  }
}

function formatErrorStack(error) {
  const stack = String(error?.stack || error || 'No stack available');
  return stack.split('\n').map(line => line.trim()).filter(Boolean);
}

function extractSourceHint(error) {
  const stackLines = formatErrorStack(error);
  const match = stackLines.find(line =>
    line.includes('/discord-tradebot/') || line.includes('index.js') || line.includes('tracker.js')
  );

  if (!match) {
    return {
      sourceText: 'Unknown source',
      sourceLink: 'Not available',
    };
  }

  const fileMatch = match.match(/(\/[^:\s)]+\.(?:js|mjs|cjs)):(\d+):(\d+)/);
  if (!fileMatch) {
    return {
      sourceText: match,
      sourceLink: 'Not available',
    };
  }

  const [, filePath, line, column] = fileMatch;
  return {
    sourceText: `${filePath}:${line}:${column}`,
    sourceLink: `file://${filePath}#L${line}`,
  };
}

async function sendAlert({ code, summary, details, error = null, source = null }) {
  try {
    const user = await getAlertUser(client);
    if (!user) return;

    const sourceHint = source || extractSourceHint(error);
    const stackLines = formatErrorStack(error).slice(0, 5);

    const embed = new EmbedBuilder()
      .setColor(0xef4444)
      .setTitle(`Bot Alert · ${code}`)
      .addFields(
        { name: 'Summary', value: summary || 'No summary provided.', inline: false },
        { name: 'Explanation', value: details || 'No extra details provided.', inline: false },
        { name: 'Source', value: sourceHint.sourceText || 'Unknown source', inline: false },
        { name: 'Origin Link', value: sourceHint.sourceLink || 'Not available', inline: false },
      )
      .setTimestamp();

    if (stackLines.length) {
      embed.addFields({
        name: 'Stack',
        value: `\`\`\`\n${stackLines.join('\n').slice(0, 980)}\n\`\`\``,
        inline: false,
      });
    }

    await user.send({ embeds: [embed] });
  } catch (alertError) {
    console.error('Failed to send alert DM:', alertError);
  }
}

async function handleRecoveredOfflineState() {
  const previous = readMonitorState();
  if (!previous.lastBootAt) return;
  if (previous.cleanShutdown) return;

  const downtimeText = previous.lastHeartbeat
    ? `<t:${Math.floor(previous.lastHeartbeat / 1000)}:R>`
    : 'unknown time';

  await sendAlert({
    code: 'BOT_RECOVERED',
    summary: 'The bot restarted after an unclean shutdown.',
    details: `The previous run appears to have gone offline unexpectedly. Last heartbeat was ${downtimeText}.`,
    source: {
      sourceText: previous.lastCrashSource || 'Previous runtime state',
      sourceLink: previous.lastCrashSource ? `file://${String(previous.lastCrashSource).split(':')[0]}` : 'Not available',
    },
  });
}

async function markStartupState() {
  await handleRecoveredOfflineState();
  const guild = getAlertGuild(client);
  writeMonitorState({
    running: true,
    cleanShutdown: false,
    lastBootAt: Date.now(),
    lastHeartbeat: Date.now(),
    lastShutdownAt: null,
    lastShutdownReason: null,
    guildId: guild?.id || null,
    lastCrashCode: null,
    lastCrashSource: null,
  });
  startHeartbeat();
}

async function gracefulShutdown(reason = 'SIGINT') {
  if (shuttingDown) return;
  shuttingDown = true;
  stopHeartbeat();
  writeMonitorState({
    running: false,
    cleanShutdown: true,
    lastShutdownAt: Date.now(),
    lastShutdownReason: reason,
  });
  process.exit(0);
}

process.on('SIGINT', () => {
  void gracefulShutdown('SIGINT');
});

process.on('SIGTERM', () => {
  void gracefulShutdown('SIGTERM');
});

process.on('unhandledRejection', reason => {
  const error = reason instanceof Error ? reason : new Error(String(reason));
  console.error('Unhandled promise rejection:', error);
  const sourceHint = extractSourceHint(error);
  writeMonitorState({
    running: true,
    cleanShutdown: false,
    lastCrashCode: 'UNHANDLED_REJECTION',
    lastCrashSource: sourceHint.sourceText,
    lastHeartbeat: Date.now(),
  });
  void sendAlert({
    code: 'UNHANDLED_REJECTION',
    summary: 'A promise rejection was not handled by the bot.',
    details: error.message,
    error,
    source: sourceHint,
  });
});

process.on('uncaughtException', error => {
  console.error('Uncaught exception:', error);
  const sourceHint = extractSourceHint(error);
  writeMonitorState({
    running: false,
    cleanShutdown: false,
    lastCrashCode: 'UNCAUGHT_EXCEPTION',
    lastCrashSource: sourceHint.sourceText,
    lastHeartbeat: Date.now(),
  });
  void sendAlert({
    code: 'UNCAUGHT_EXCEPTION',
    summary: 'The bot hit an uncaught exception and is shutting down.',
    details: error.message,
    error,
    source: sourceHint,
  }).finally(() => {
    setTimeout(() => process.exit(1), 750).unref();
  });
});

function loadStateStoreFromDisk() {
  const data = safeReadJson(STATE_FILE, {});
  const now = Date.now();

  for (const [key, value] of Object.entries(data)) {
    if (!value || typeof value !== 'object') continue;
    if (!value.createdAt || now - value.createdAt > STATE_TTL_MS) continue;
    routeStates.set(key, value);
  }

  persistStateStore();
}

function persistStateStore() {
  const obj = {};
  const now = Date.now();

  for (const [key, value] of routeStates.entries()) {
    if (!value || !value.createdAt) continue;
    if (now - value.createdAt > STATE_TTL_MS) continue;
    obj[key] = value;
  }

  safeWriteJson(STATE_FILE, obj);
}

function cleanupRouteStates() {
  const now = Date.now();
  let changed = false;

  for (const [key, value] of routeStates.entries()) {
    if (now - value.createdAt > STATE_TTL_MS) {
      routeStates.delete(key);
      changed = true;
    }
  }

  if (changed) persistStateStore();
}

function saveRouteState(state, existingId = null) {
  cleanupRouteStates();

  const id = existingId || crypto.randomBytes(8).toString('hex');
  routeStates.set(id, {
    ...state,
    createdAt: Date.now(),
  });
  persistStateStore();
  return id;
}

function getRouteState(stateId) {
  cleanupRouteStates();
  return routeStates.get(stateId) || null;
}

async function fetchJson(url) {
  return fetchJsonWithHeaders(url, {
    headers: {
      'User-Agent': 'SPACEWHLE Trade Command Bot',
      Accept: 'application/json',
    },
  });
}

function createHttpError(response, url) {
  const error = new Error(`HTTP ${response.status} for ${url}`);
  error.status = response.status;
  error.url = url;
  return error;
}

async function fetchWithTimeout(url, options = {}, timeoutMs = 20000) {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), timeoutMs);

  try {
    return await fetch(url, {
      ...options,
      signal: controller.signal,
    });
  } finally {
    clearTimeout(timeout);
  }
}

async function fetchJsonWithHeaders(url, options = {}) {
  const response = await fetchWithTimeout(
    url,
    {
      method: options.method || 'GET',
      headers: options.headers || {},
    },
    options.timeoutMs || 20000,
  );

  if (!response.ok) throw createHttpError(response, url);
  return await response.json();
}

async function fetchTextWithHeaders(url, options = {}) {
  const response = await fetchWithTimeout(
    url,
    {
      method: options.method || 'GET',
      headers: options.headers || {},
    },
    options.timeoutMs || 20000,
  );

  if (!response.ok) throw createHttpError(response, url);
  return await response.text();
}

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

function renderPathTemplate(template, params = {}) {
  return String(template || '').replace(/\{([a-zA-Z0-9_]+)\}/g, (_, key) => {
    const value = params[key] ?? '';
    return encodeURIComponent(String(value));
  });
}

function buildApiUrl(pathTemplate, params = {}) {
  const rendered = renderPathTemplate(pathTemplate, params);
  if (/^https?:\/\//i.test(rendered)) return rendered;

  if (!API_BASE_URL) {
    const error = new Error('Website API is not configured.');
    error.code = 'API_NOT_CONFIGURED';
    throw error;
  }

  const separator = rendered.startsWith('/') ? '' : '/';
  return `${API_BASE_URL}${separator}${rendered}`;
}

function buildWebsiteApiHeaders() {
  const headers = {
    'User-Agent': 'SPACEWHLE Trade Command Bot',
    Accept: 'application/json',
  };

  if (API_AUTH_TOKEN && API_AUTH_HEADER) {
    headers[API_AUTH_HEADER] = API_AUTH_SCHEME.toLowerCase() === 'none'
      ? API_AUTH_TOKEN
      : `${API_AUTH_SCHEME} ${API_AUTH_TOKEN}`.trim();
  }

  return headers;
}

async function fetchWebsiteJson(pathTemplate, params = {}) {
  const url = buildApiUrl(pathTemplate, params);
  return fetchJsonWithHeaders(url, {
    headers: buildWebsiteApiHeaders(),
    timeoutMs: 12000,
  });
}

function unwrapObjectPayload(payload) {
  let current = payload;

  for (let i = 0; i < 5; i += 1) {
    if (!current || typeof current !== 'object' || Array.isArray(current)) return current;

    const nextKey = ['data', 'result', 'member', 'profile'].find(key =>
      current[key] && typeof current[key] === 'object' && !Array.isArray(current[key])
    );

    if (!nextKey) return current;
    current = current[nextKey];
  }

  return current;
}

function getPathValue(obj, pathName) {
  if (!obj || typeof obj !== 'object') return undefined;
  return String(pathName)
    .split('.')
    .reduce((current, key) => (current == null ? undefined : current[key]), obj);
}

function firstDefinedPath(obj, paths) {
  for (const pathName of paths) {
    const value = getPathValue(obj, pathName);
    if (value !== undefined && value !== null && value !== '') return value;
  }

  return null;
}

function stringifyProfileValue(value) {
  if (value === null || value === undefined) return null;
  if (typeof value === 'string') return value.trim() || null;
  if (typeof value === 'number' || typeof value === 'bigint') return String(value);
  if (typeof value === 'boolean') return value ? 'Yes' : 'No';

  if (Array.isArray(value)) {
    const values = value.map(item => stringifyProfileValue(item)).filter(Boolean);
    return values.length ? values.join(', ') : null;
  }

  if (typeof value === 'object') {
    for (const key of ['name', 'title', 'label', 'displayName', 'value', 'rank', 'position']) {
      const text = stringifyProfileValue(value[key]);
      if (text) return text;
    }
  }

  return null;
}

function pickStringPath(obj, paths) {
  return stringifyProfileValue(firstDefinedPath(obj, paths));
}

function formatProfileMetric(value, fallback = 'Unknown') {
  const text = stringifyProfileValue(value);
  return text || fallback;
}

function parseDateValue(value) {
  if (!value) return null;
  if (value instanceof Date && !Number.isNaN(value.valueOf())) return value;

  if (typeof value === 'number') {
    const date = new Date(value > 100000000000 ? value : value * 1000);
    return Number.isNaN(date.valueOf()) ? null : date;
  }

  const text = stringifyProfileValue(value);
  if (!text) return null;

  if (/^\d+$/.test(text)) {
    const numeric = Number(text);
    if (Number.isFinite(numeric)) return parseDateValue(numeric);
  }

  const parsed = new Date(text);
  return Number.isNaN(parsed.valueOf()) ? null : parsed;
}

function formatDateValue(value, fallback = 'Unknown') {
  const date = parseDateValue(value);
  if (!date) return stringifyProfileValue(value) || fallback;
  return `<t:${Math.floor(date.getTime() / 1000)}:D>`;
}

function formatDurationSince(value, fallback = 'Unknown') {
  const date = parseDateValue(value);
  if (!date) return fallback;

  const days = Math.max(0, Math.floor((Date.now() - date.getTime()) / (24 * 60 * 60 * 1000)));
  if (days <= 0) return 'Today';

  const years = Math.floor(days / 365.25);
  const months = Math.floor((days - years * 365.25) / 30.44);
  if (years > 0 && months > 0) return `${years}y ${months}m`;
  if (years > 0) return `${years}y`;
  if (months > 0) return `${months}m`;
  return `${days}d`;
}

function formatLeaderboardPosition(value) {
  const text = stringifyProfileValue(value);
  if (!text || text === '0') return 'Unranked';

  const numeric = Number(String(text).replace(/[^\d.-]/g, ''));
  if (Number.isFinite(numeric) && numeric > 0 && /^#?\d+/.test(String(text).trim())) {
    return `#${Math.round(numeric).toLocaleString('en-GB')}`;
  }

  return text.startsWith('#') ? text : text;
}

function formatRequirementsRemaining(value) {
  if (value === null || value === undefined || value === '') return null;

  if (Array.isArray(value)) {
    const rows = value.map(item => stringifyProfileValue(item)).filter(Boolean);
    return rows.length ? rows.slice(0, 8).join('\n').slice(0, 1024) : null;
  }

  if (typeof value === 'object') {
    const rows = Object.entries(value)
      .map(([key, item]) => {
        const text = stringifyProfileValue(item);
        return text ? `${key}: ${text}` : null;
      })
      .filter(Boolean);

    return rows.length ? rows.slice(0, 8).join('\n').slice(0, 1024) : null;
  }

  return stringifyProfileValue(value);
}

function normalizeMemberProfile(payload, discordUser) {
  const profile = unwrapObjectPayload(payload);
  if (!profile || typeof profile !== 'object' || Array.isArray(profile)) return null;

  const leaderboardActivity = firstDefinedPath(profile, [
    'leaderboard.activity',
    'leaderboard.activity.rank',
    'leaderboard.activity.position',
    'leaderboards.activity',
    'leaderboards.activity.rank',
    'leaderboards.activity.position',
    'leaderboardPositions.activity',
    'activityLeaderboardPosition',
    'activity_position',
    'activityRank',
  ]);
  const leaderboardAttendance = firstDefinedPath(profile, [
    'leaderboard.attendance',
    'leaderboard.attendance.rank',
    'leaderboard.attendance.position',
    'leaderboards.attendance',
    'leaderboards.attendance.rank',
    'leaderboards.attendance.position',
    'leaderboardPositions.attendance',
    'attendanceLeaderboardPosition',
    'attendance_position',
    'attendanceRank',
  ]);
  const leaderboardParticipation = firstDefinedPath(profile, [
    'leaderboard.participation',
    'leaderboard.participation.rank',
    'leaderboard.participation.position',
    'leaderboards.participation',
    'leaderboards.participation.rank',
    'leaderboards.participation.position',
    'leaderboardPositions.participation',
    'participationLeaderboardPosition',
    'participation_position',
    'participationRank',
  ]);
  const nextRank = pickStringPath(profile, [
    'promotionProgress.nextRank',
    'promotion_progress.next_rank',
    'progress.nextRank',
    'nextRank',
    'next_rank',
  ]);
  const requirementsRemaining = firstDefinedPath(profile, [
    'promotionProgress.requirementsRemaining',
    'promotion_progress.requirements_remaining',
    'promotionProgress.remaining',
    'progress.requirementsRemaining',
    'requirementsRemaining',
    'requirements_remaining',
  ]);

  return {
    username: pickStringPath(profile, ['username', 'displayName', 'display_name', 'name', 'handle'])
      || discordUser.globalName
      || discordUser.username
      || 'Member',
    rank: pickStringPath(profile, ['rank', 'rank.name', 'currentRank', 'current_rank', 'orgRank', 'org_rank']),
    joinDate: firstDefinedPath(profile, ['joinDate', 'join_date', 'joinedAt', 'joined_at', 'organisationJoinDate', 'organizationJoinDate']),
    eventsAttended: firstDefinedPath(profile, ['eventsAttended', 'events_attended', 'attendance.events', 'stats.eventsAttended']),
    activityScore: firstDefinedPath(profile, ['activityScore', 'activity_score', 'score.activity', 'stats.activityScore']),
    attendanceStreak: firstDefinedPath(profile, ['attendanceStreak', 'attendance_streak', 'streak.attendance', 'stats.attendanceStreak']),
    leaderboard: {
      activity: leaderboardActivity,
      attendance: leaderboardAttendance,
      participation: leaderboardParticipation,
    },
    promotion: {
      nextRank,
      requirementsRemaining,
    },
  };
}

function buildMemberProfileEmbed(profile, discordUser) {
  const embed = new EmbedBuilder()
    .setColor(0x22d3ee)
    .setTitle(`${profile.username}'s Profile`)
    .setThumbnail(discordUser.displayAvatarURL({ size: 128 }))
    .addFields(
      { name: 'Rank', value: formatProfileMetric(profile.rank), inline: true },
      { name: 'Time in Organisation', value: formatDurationSince(profile.joinDate), inline: true },
      { name: 'Events Attended', value: formatProfileMetric(profile.eventsAttended), inline: true },
      { name: 'Activity Score', value: formatProfileMetric(profile.activityScore), inline: true },
      { name: 'Attendance Streak', value: formatProfileMetric(profile.attendanceStreak), inline: true },
      {
        name: 'Leaderboard',
        value: [
          `Activity: ${formatLeaderboardPosition(profile.leaderboard.activity)}`,
          `Attendance: ${formatLeaderboardPosition(profile.leaderboard.attendance)}`,
          `Participation: ${formatLeaderboardPosition(profile.leaderboard.participation)}`,
        ].join('\n'),
        inline: false,
      },
    );

  const progressLines = [];
  if (profile.promotion.nextRank) progressLines.push(`Next Rank: ${profile.promotion.nextRank}`);

  const requirementsRemaining = formatRequirementsRemaining(profile.promotion.requirementsRemaining);
  if (requirementsRemaining) progressLines.push(`Requirements Remaining: ${requirementsRemaining}`);

  if (progressLines.length) {
    embed.addFields({
      name: 'Progress',
      value: progressLines.join('\n').slice(0, 1024),
      inline: false,
    });
  }

  return embed;
}

async function handleMeCommand(interaction) {
  await deferChatInputCommand(interaction);

  try {
    const payload = await fetchWebsiteJson(API_MEMBER_PROFILE_PATH, {
      discordId: interaction.user.id,
      userId: interaction.user.id,
    });
    const profile = normalizeMemberProfile(payload, interaction.user);

    if (!profile) {
      await interaction.editReply('I found a profile response, but it did not include usable member data yet.');
      return;
    }

    await interaction.editReply({ embeds: [buildMemberProfileEmbed(profile, interaction.user)] });
  } catch (error) {
    if (error.code === 'API_NOT_CONFIGURED') {
      await interaction.editReply('The member profile API is not configured yet. Set `API_BASE_URL` and, if needed, `API_MEMBER_PROFILE_PATH`.');
      return;
    }

    if (Number(error.status) === 404) {
      await interaction.editReply('I could not find a linked organisation profile for you yet. Link your account on the website, then try again.');
      return;
    }

    if (Number(error.status) === 401 || Number(error.status) === 403) {
      await interaction.editReply('The profile API rejected the bot request. Check the API auth environment settings.');
      return;
    }

    console.error('Member profile lookup failed:', error);
    await interaction.editReply('I could not load your profile right now. Try again in a minute.');
  }
}

function getHtmlAttr(tag, attrName) {
  const pattern = new RegExp(`${attrName}\\s*=\\s*("([^"]*)"|'([^']*)'|([^\\s>]+))`, 'i');
  const match = String(tag || '').match(pattern);
  return match ? (match[2] || match[3] || match[4] || '').trim() : null;
}

function extractMetaContent(html, names) {
  const wanted = new Set(names.map(name => name.toLowerCase()));
  const tags = String(html || '').match(/<meta\b[^>]*>/gi) || [];

  for (const tag of tags) {
    const name = (getHtmlAttr(tag, 'property') || getHtmlAttr(tag, 'name') || '').toLowerCase();
    if (!wanted.has(name)) continue;

    const content = getHtmlAttr(tag, 'content');
    if (content) return content;
  }

  return null;
}

function decodeHtmlEntities(value) {
  const entities = {
    amp: '&',
    lt: '<',
    gt: '>',
    quot: '"',
    apos: "'",
    nbsp: ' ',
  };

  return String(value || '').replace(/&(#x[0-9a-f]+|#\d+|[a-z]+);/gi, (match, entity) => {
    const key = entity.toLowerCase();
    if (key.startsWith('#x')) return String.fromCodePoint(parseInt(key.slice(2), 16));
    if (key.startsWith('#')) return String.fromCodePoint(parseInt(key.slice(1), 10));
    return entities[key] || match;
  });
}

function htmlToTextLines(html) {
  const text = String(html || '')
    .replace(/<script\b[\s\S]*?<\/script>/gi, ' ')
    .replace(/<style\b[\s\S]*?<\/style>/gi, ' ')
    .replace(/<(br|p|div|li|section|article|header|footer|h[1-6]|dt|dd)\b[^>]*>/gi, '\n')
    .replace(/<\/(p|div|li|section|article|header|footer|h[1-6]|dt|dd)>/gi, '\n')
    .replace(/<[^>]+>/g, ' ');

  return decodeHtmlEntities(text)
    .split(/\n+/)
    .map(line => line.replace(/\s+/g, ' ').trim())
    .filter(Boolean);
}

function isUsefulCitizenLine(line) {
  return !/^(image|overview|organizations?|profile|citizen dossier)$/i.test(String(line || '').trim());
}

function extractLineValue(lines, label) {
  const labelText = String(label || '').toLowerCase();

  for (let index = 0; index < lines.length; index += 1) {
    const line = lines[index];
    const lower = line.toLowerCase();

    if (lower === labelText) {
      for (let nextIndex = index + 1; nextIndex < lines.length; nextIndex += 1) {
        if (isUsefulCitizenLine(lines[nextIndex])) return lines[nextIndex];
      }
    }

    if (lower.startsWith(`${labelText} `) || lower.startsWith(`${labelText}:`)) {
      const value = line.slice(label.length).replace(/^[:\s]+/, '').trim();
      if (value) return value;
    }
  }

  return null;
}

function absoluteRsiUrl(value) {
  const url = String(value || '').trim();
  if (!url) return null;
  if (/^https?:\/\//i.test(url)) return url;
  if (url.startsWith('//')) return `https:${url}`;
  if (url.startsWith('/')) return `https://robertsspaceindustries.com${url}`;
  return `https://robertsspaceindustries.com/${url}`;
}

function extractCitizenImageUrl(html) {
  const metaImage = extractMetaContent(html, ['og:image', 'twitter:image']);
  if (metaImage) return absoluteRsiUrl(metaImage);

  const imgTags = String(html || '').match(/<img\b[^>]*>/gi) || [];
  for (const tag of imgTags) {
    const src = getHtmlAttr(tag, 'src');
    if (!src || /logo|icon|badge/i.test(src)) continue;
    if (/\/media\//i.test(src) || /profile/i.test(tag)) return absoluteRsiUrl(src);
  }

  return null;
}

function parseCitizenProfileHtml(html, username, profileUrl) {
  const lines = htmlToTextLines(html);
  const metaTitle = extractMetaContent(html, ['og:title', 'twitter:title']);
  const displayFromTitle = metaTitle
    ? decodeHtmlEntities(metaTitle).split('|')[0].trim()
    : null;
  const displayName = extractLineValue(lines, 'Handle name') || displayFromTitle || username;
  const enlisted = extractLineValue(lines, 'Enlisted');
  const organisation = extractLineValue(lines, 'Main organization') || extractLineValue(lines, 'Main organisation');
  const organisationRank = extractLineValue(lines, 'Organization rank') || extractLineValue(lines, 'Organisation rank');
  const location = extractLineValue(lines, 'Location');
  const bio = extractLineValue(lines, 'Bio') || extractLineValue(lines, 'Biography');

  return {
    username,
    profileUrl,
    displayName,
    enlisted,
    organisation,
    organisationRank,
    location,
    bio,
    imageUrl: extractCitizenImageUrl(html),
  };
}

async function fetchCitizenProfile(username) {
  const normalized = normalizeText(username);
  const cached = citizenProfileCache.get(normalized);
  if (cached && cached.expiresAt > Date.now()) return cached.profile;

  const profileUrl = `https://robertsspaceindustries.com/citizens/${encodeURIComponent(username)}`;
  let lastError = null;

  for (let attempt = 0; attempt < 2; attempt += 1) {
    try {
      const html = await fetchTextWithHeaders(profileUrl, {
        headers: {
          'User-Agent': 'SPACEWHLE Trade Command Bot',
          Accept: 'text/html',
        },
        timeoutMs: 12000,
      });
      const profile = parseCitizenProfileHtml(html, username, profileUrl);
      citizenProfileCache.set(normalized, {
        expiresAt: Date.now() + CITIZEN_CACHE_TTL_MS,
        profile,
      });
      return profile;
    } catch (error) {
      if (Number(error.status) === 404) throw error;
      lastError = error;
      if (attempt === 0) await sleep(500);
    }
  }

  throw lastError;
}

function extractLinkedRsiHandle(payload) {
  const data = unwrapObjectPayload(payload);
  return pickStringPath(data, [
    'rsiHandle',
    'rsi_handle',
    'rsiUsername',
    'rsi_username',
    'citizenHandle',
    'citizen_handle',
    'handle',
    'username',
    'rsi.handle',
    'rsi.username',
    'starCitizen.handle',
    'starCitizen.username',
    'star_citizen.handle',
    'star_citizen.username',
  ]);
}

async function fetchLinkedRsiHandle(discordUserId) {
  const payload = await fetchWebsiteJson(API_RSI_LINK_PATH, {
    discordId: discordUserId,
    userId: discordUserId,
  });

  return extractLinkedRsiHandle(payload);
}

function buildCitizenEmbed(profile) {
  const embed = new EmbedBuilder()
    .setColor(0x38bdf8)
    .setTitle(profile.displayName || profile.username)
    .setURL(profile.profileUrl)
    .addFields(
      { name: 'Enlisted', value: formatDateValue(profile.enlisted), inline: true },
      { name: 'Organisation', value: profile.organisation || 'None listed', inline: true },
      { name: 'Rank', value: profile.organisationRank || 'Unknown', inline: true },
      { name: 'Account Age', value: formatDurationSince(profile.enlisted), inline: true },
    )
    .setFooter({ text: 'Data from public RSI profile' });

  if (profile.location) {
    embed.addFields({ name: 'Location', value: profile.location, inline: true });
  }

  if (profile.bio) {
    embed.setDescription(profile.bio.slice(0, 500));
  }

  if (profile.imageUrl) {
    embed.setThumbnail(profile.imageUrl);
  }

  return embed;
}

async function handleCitizenCommand(interaction) {
  await deferChatInputCommand(interaction);

  const usernameInput = interaction.options.getString('username', false)?.trim();
  const linkedUser = interaction.options.getUser('user', false);
  let username = usernameInput;

  if (!username && linkedUser) {
    try {
      username = await fetchLinkedRsiHandle(linkedUser.id);
    } catch (error) {
      if (error.code === 'API_NOT_CONFIGURED' || Number(error.status) === 404) {
        await interaction.editReply('That Discord user does not have a linked RSI handle yet. Ask them to link their account on the website, or provide `username` directly.');
        return;
      }

      console.error('Linked RSI lookup failed:', error);
      await interaction.editReply('I could not resolve that user\'s linked RSI handle right now.');
      return;
    }
  }

  if (!username) {
    await interaction.editReply('Provide a Star Citizen handle with `username`, or choose a Discord `user` who has linked their RSI handle.');
    return;
  }

  try {
    const profile = await fetchCitizenProfile(username);
    await interaction.editReply({ embeds: [buildCitizenEmbed(profile)] });
  } catch (error) {
    if (Number(error.status) === 404) {
      await interaction.editReply(`I could not find a public RSI profile for \`${username}\`.`);
      return;
    }

    console.error('Citizen profile lookup failed:', error);
    await interaction.editReply('I could not fetch that public RSI profile right now. Try again in a minute.');
  }
}

function escapeDiscordMarkdown(value) {
  return String(value || '').replace(/([\\`*_~|>])/g, '\\$1');
}

function resolveRoleToken(guild, token) {
  const raw = String(token || '').trim();
  if (!raw) return null;

  const id = raw.match(/^<@&(\d{17,20})>$/)?.[1] || raw.match(/^\d{17,20}$/)?.[0];
  if (id) return guild.roles.cache.get(id) || null;

  const normalized = normalizeText(raw);
  return guild.roles.cache.find(role => normalizeText(role.name) === normalized) || null;
}

function resolveRoleListInput(guild, input) {
  const roles = [];
  const unresolved = [];
  const text = String(input || '').trim();
  if (!text) return { roles, unresolved };

  const idPattern = /<@&(\d{17,20})>|\b(\d{17,20})\b/g;
  const matchedRanges = [];

  for (const match of text.matchAll(idPattern)) {
    const roleId = match[1] || match[2];
    const role = guild.roles.cache.get(roleId);
    if (role) roles.push(role);
    else unresolved.push(match[0]);
    matchedRanges.push([match.index, match.index + match[0].length]);
  }

  let remaining = '';
  for (let index = 0; index < text.length; index += 1) {
    if (matchedRanges.some(([start, end]) => index >= start && index < end)) continue;
    remaining += text[index];
  }

  const nameTokens = remaining
    .split(',')
    .map(value => value.trim())
    .filter(Boolean);

  for (const token of nameTokens) {
    const role = resolveRoleToken(guild, token);
    if (role) roles.push(role);
    else unresolved.push(token);
  }

  return {
    roles: Array.from(new Map(roles.map(role => [role.id, role])).values()),
    unresolved,
  };
}

function roleHierarchyError(role, managerMember, actorMember, guild) {
  if (!role || role.id === guild.id) return 'The @everyone role cannot be managed.';
  if (role.managed) return `I cannot manage the managed role "${role.name}".`;

  if (role.position >= managerMember.roles.highest.position) {
    return `My highest role must be above "${role.name}" before I can manage it.`;
  }

  if (actorMember.id !== guild.ownerId && role.position >= actorMember.roles.highest.position) {
    return `Your highest role must be above "${role.name}" before you can assign or remove it.`;
  }

  return null;
}

function canRunPromotionCommand(member) {
  if (!member) return false;
  if (member.permissions?.has(PermissionFlagsBits.Administrator)) return true;
  if (member.permissions?.has(PermissionFlagsBits.ManageRoles)) return true;
  return PROMOTE_ALLOWED_ROLE_IDS.some(roleId => member.roles.cache.has(roleId));
}

async function resolvePromotionChannel(interaction, managerMember) {
  const requestedChannel = interaction.options.getChannel('channel', false);
  const channel = requestedChannel || (PROMOTIONS_CHANNEL_ID
    ? await interaction.guild.channels.fetch(PROMOTIONS_CHANNEL_ID).catch(() => null)
    : null);

  if (!channel) {
    return {
      error: requestedChannel
        ? 'I could not use that announcement channel.'
        : 'No promotions channel is configured. Set `PROMOTIONS_CHANNEL_ID` or provide `channel`.',
    };
  }

  if (![ChannelType.GuildText, ChannelType.GuildAnnouncement].includes(channel.type) || !channel.isTextBased()) {
    return { error: 'Promotion announcements must be sent to a text or announcement channel.' };
  }

  const permissions = channel.permissionsFor(managerMember);
  if (!permissions?.has(PermissionFlagsBits.ViewChannel) || !permissions?.has(PermissionFlagsBits.SendMessages)) {
    return { error: `I cannot send promotion announcements in ${channel}.` };
  }

  return { channel };
}

function buildPromotionAnnouncement(targetMember, rankRole, addedRoles, customMessage) {
  const rolesList = addedRoles
    .map(role => `- ${escapeDiscordMarkdown(role.name)}`)
    .join('\n');
  const parts = [
    '\u{1F389} **Promotion Announcement**',
    `Congratulations to ${targetMember} on being promoted to **${escapeDiscordMarkdown(rankRole.name)}**.`,
    `**Roles Received:**\n${rolesList}`,
  ];

  if (customMessage) parts.push(customMessage);
  return parts.join('\n\n');
}

async function handlePromoteCommand(interaction) {
  await deferChatInputCommand(interaction);

  if (!interaction.guild) {
    await interaction.editReply('Promotions can only be run inside a server.');
    return;
  }

  const actorMember = await interaction.guild.members.fetch(interaction.user.id);
  if (!canRunPromotionCommand(actorMember)) {
    await interaction.editReply('You need a staff role, Administrator, or Manage Roles permission to use `/promote`.');
    return;
  }

  const managerMember = interaction.guild.members.me || await interaction.guild.members.fetchMe();
  if (!managerMember.permissions.has(PermissionFlagsBits.ManageRoles)) {
    await interaction.editReply('I need the Manage Roles permission before I can promote members.');
    return;
  }

  const targetUser = interaction.options.getUser('user', true);
  const targetMember = await interaction.guild.members.fetch(targetUser.id).catch(() => null);
  if (!targetMember) {
    await interaction.editReply('I could not find that member in this server.');
    return;
  }

  if (targetMember.id === interaction.guild.ownerId) {
    await interaction.editReply('I cannot change roles for the server owner.');
    return;
  }

  if (targetMember.roles.highest.position >= managerMember.roles.highest.position && targetMember.id !== managerMember.id) {
    await interaction.editReply('My highest role must be above the member I am promoting.');
    return;
  }

  const rankRoleOption = interaction.options.getRole('rank_role', true);
  const rankRole = interaction.guild.roles.cache.get(rankRoleOption.id);
  if (!rankRole) {
    await interaction.editReply('I could not resolve that rank role.');
    return;
  }

  const extraRolesInput = interaction.options.getString('extra_roles', false);
  const removeRolesInput = interaction.options.getString('remove_roles', false);
  const customMessage = interaction.options.getString('custom_message', false)?.trim();
  const extraResult = resolveRoleListInput(interaction.guild, extraRolesInput);
  const removeResult = resolveRoleListInput(interaction.guild, removeRolesInput);

  if (extraResult.unresolved.length || removeResult.unresolved.length) {
    await interaction.editReply(`I could not resolve these roles: ${[...extraResult.unresolved, ...removeResult.unresolved].map(value => `\`${value}\``).join(', ')}.`);
    return;
  }

  const rolesToAdd = Array.from(new Map([rankRole, ...extraResult.roles].map(role => [role.id, role])).values());
  const rolesToRemove = Array.from(new Map(removeResult.roles.map(role => [role.id, role])).values());
  const overlap = rolesToRemove.find(role => rolesToAdd.some(addedRole => addedRole.id === role.id));
  if (overlap) {
    await interaction.editReply(`"${overlap.name}" cannot be both added and removed in the same promotion.`);
    return;
  }

  for (const role of [...rolesToAdd, ...rolesToRemove]) {
    const error = roleHierarchyError(role, managerMember, actorMember, interaction.guild);
    if (error) {
      await interaction.editReply(error);
      return;
    }
  }

  const channelResult = await resolvePromotionChannel(interaction, managerMember);
  if (channelResult.error) {
    await interaction.editReply(channelResult.error);
    return;
  }

  try {
    if (rolesToAdd.length) {
      await targetMember.roles.add(rolesToAdd, `Promotion by ${interaction.user.tag}`);
    }

    if (rolesToRemove.length) {
      await targetMember.roles.remove(rolesToRemove, `Promotion by ${interaction.user.tag}`);
    }
  } catch (error) {
    console.error('Promotion role update failed:', error);
    await interaction.editReply('I could not update those roles. Check my permissions and role hierarchy, then try again.');
    return;
  }

  try {
    const announcement = buildPromotionAnnouncement(targetMember, rankRole, rolesToAdd, customMessage);
    await channelResult.channel.send({
      content: announcement,
      allowedMentions: {
        users: [targetMember.id],
        roles: [],
        repliedUser: false,
      },
    });
  } catch (error) {
    console.error('Promotion announcement failed:', error);
    await interaction.editReply('The roles were updated, but I could not send the promotion announcement.');
    return;
  }

  const removedText = rolesToRemove.length
    ? ` Removed: ${rolesToRemove.map(role => role.name).join(', ')}.`
    : '';
  await interaction.editReply(`Promoted ${targetMember} to ${rankRole.name} and announced it in ${channelResult.channel}.${removedText}`);
}

function getRiskLabel(score) {
  if (score <= 20) return 'Low';
  if (score <= 40) return 'Moderate';
  if (score <= 60) return 'Elevated';
  if (score <= 80) return 'High';
  return 'Severe';
}

function locationDisplayName(terminal) {
  return terminal.fullname || terminal.displayname || terminal.terminal_name || terminal.name || 'Unknown Terminal';
}

function pickMainLocationName(terminal) {
  return (
    terminal.space_station_name ||
    terminal.city_name ||
    terminal.outpost_name ||
    terminal.poi_name ||
    terminal.moon_name ||
    terminal.planet_name ||
    terminal.star_system_name ||
    locationDisplayName(terminal)
  );
}

function pickSystemName(terminal) {
  return terminal.star_system_name || 'Unknown System';
}

function isAtmosphericTerminal(terminal) {
  return Boolean(
    terminal.id_city ||
    terminal.id_outpost ||
    (terminal.id_planet && !terminal.id_space_station)
  );
}

function getTerminalTypeLabel(terminal) {
  if (terminal.space_station_name) return 'Station';
  if (terminal.city_name) return 'City';
  if (terminal.outpost_name) return 'Outpost';
  if (terminal.poi_name) return 'POI';
  if (terminal.moon_name) return 'Moon';
  if (terminal.planet_name) return 'Planet';
  return 'Location';
}

function getTerminalFeatures(terminal) {
  return {
    hasDockingPort: Boolean(terminal.has_docking_port),
    hasFreightElevator: Boolean(terminal.has_freight_elevator),
    hasLoadingDock: Boolean(terminal.has_loading_dock),
    isCargoCenter: Boolean(terminal.is_cargo_center),
    isAutoLoad: Boolean(terminal.is_auto_load),
    isRefuel: Boolean(terminal.is_refuel),
    isPlayerOwned: Boolean(terminal.is_player_owned),
    isAvailableLive: Boolean(terminal.is_available_live ?? terminal.is_available),
    maxContainerSize: toNumber(terminal.max_container_size),
    terminalCode: terminal.code || '',
    terminalFullName: locationDisplayName(terminal),
  };
}

function buildGroupedLocationIndex(terminals, prices) {
  const terminalMap = new Map(terminals.map(terminal => [Number(terminal.id), terminal]));
  const groupsMap = new Map();
  const commodityNames = new Set();

  for (const row of prices) {
    const terminal = terminalMap.get(Number(row.id_terminal));
    if (!terminal) continue;

    const commodityName = row.commodity_name || row.name || 'Unknown Commodity';
    commodityNames.add(commodityName);

    const mainLocation = pickMainLocationName(terminal);
    const system = pickSystemName(terminal);
    const key = `${normalizeText(mainLocation)}|${normalizeText(system)}`;

    if (!groupsMap.has(key)) {
      groupsMap.set(key, {
        key,
        name: `${mainLocation} — ${system}`,
        shortName: mainLocation,
        system,
        locationType: getTerminalTypeLabel(terminal),
        atmospheric: false,
        terminals: new Map(),
      });
    }

    const group = groupsMap.get(key);
    group.atmospheric ||= isAtmosphericTerminal(terminal);

    const terminalId = Number(row.id_terminal);
    if (!group.terminals.has(terminalId)) {
      group.terminals.set(terminalId, {
        terminalId,
        name: locationDisplayName(terminal),
        sells: [],
        buys: [],
        atmospheric: isAtmosphericTerminal(terminal),
        features: getTerminalFeatures(terminal),
      });
    }

    const terminalEntry = group.terminals.get(terminalId);
    const modifiedAt = unixSecondsToMs(row.date_modified || row.date_added);

    if (Number(row.price_buy) > 0) {
      terminalEntry.sells.push({
        commodityId: toNumber(row.id_commodity),
        commodity: commodityName,
        price: toNumber(row.price_buy),
        priceAverage: firstPositiveNumber(row.price_buy_avg),
        stock: firstPositiveNumber(row.scu_buy, row.scu_buy_avg),
        stockReported: toNumber(row.scu_buy),
        stockAverage: toNumber(row.scu_buy_avg),
        status: toNumber(row.status_buy),
        modifiedAt,
      });
    }

    if (Number(row.price_sell) > 0) {
      terminalEntry.buys.push({
        commodityId: toNumber(row.id_commodity),
        commodity: commodityName,
        price: toNumber(row.price_sell),
        priceAverage: firstPositiveNumber(row.price_sell_avg),
        demand: firstPositiveNumber(row.scu_sell, row.scu_sell_stock, row.scu_sell_avg, row.scu_sell_stock_avg),
        demandReported: firstPositiveNumber(row.scu_sell, row.scu_sell_stock),
        demandAverage: firstPositiveNumber(row.scu_sell_avg, row.scu_sell_stock_avg),
        status: toNumber(row.status_sell),
        modifiedAt,
      });
    }
  }

  const groups = Array.from(groupsMap.values())
    .map(group => {
      group.terminals = Array.from(group.terminals.values()).sort((a, b) => a.name.localeCompare(b.name));

      const sells = new Map();
      const buys = new Map();

      for (const terminal of group.terminals) {
        for (const item of terminal.sells) {
          const existing = sells.get(item.commodity);
          if (!existing || item.price < existing.price) {
            sells.set(item.commodity, {
              commodityId: item.commodityId,
              commodity: item.commodity,
              price: item.price,
              stock: item.stock,
              stockReported: item.stockReported,
              stockAverage: item.stockAverage,
              priceAverage: item.priceAverage,
              status: item.status,
              terminalName: terminal.name,
              atmospheric: terminal.atmospheric,
              terminalId: terminal.terminalId,
              modifiedAt: item.modifiedAt,
              ...terminal.features,
            });
          }
        }

        for (const item of terminal.buys) {
          const existing = buys.get(item.commodity);
          if (!existing || item.price > existing.price) {
            buys.set(item.commodity, {
              commodityId: item.commodityId,
              commodity: item.commodity,
              price: item.price,
              demand: item.demand,
              demandReported: item.demandReported,
              demandAverage: item.demandAverage,
              priceAverage: item.priceAverage,
              status: item.status,
              terminalName: terminal.name,
              atmospheric: terminal.atmospheric,
              terminalId: terminal.terminalId,
              modifiedAt: item.modifiedAt,
              ...terminal.features,
            });
          }
        }
      }

      group.sells = Array.from(sells.values()).sort((a, b) => a.commodity.localeCompare(b.commodity));
      group.buys = Array.from(buys.values()).sort((a, b) => a.commodity.localeCompare(b.commodity));

      return group;
    })
    .filter(group => group.sells.length || group.buys.length)
    .sort((a, b) => a.name.localeCompare(b.name));

  return {
    groups,
    shortGroupNames: groups
      .map(group => group.shortName)
      .filter((value, index, arr) => arr.indexOf(value) === index)
      .sort((a, b) => a.localeCompare(b)),
    commodityNames: Array.from(commodityNames).sort((a, b) => a.localeCompare(b)),
    terminalMap,
  };
}

function buildRouteIndex(groups) {
  const sellersByCommodity = new Map();
  const buyersByCommodity = new Map();

  for (const group of groups) {
    for (const sell of group.sells) {
      const commodityKey = `${sell.commodityId || 0}|${sell.commodity}`;
      if (!sellersByCommodity.has(commodityKey)) sellersByCommodity.set(commodityKey, []);
      sellersByCommodity.get(commodityKey).push({
        commodityId: sell.commodityId,
        commodity: sell.commodity,
        groupName: group.name,
        shortGroupName: group.shortName,
        system: group.system,
        locationType: group.locationType,
        atmospheric: group.atmospheric || sell.atmospheric,
        terminalName: sell.terminalName,
        terminalId: sell.terminalId,
        price: sell.price,
        stock: sell.stock,
        stockReported: sell.stockReported,
        stockAverage: sell.stockAverage,
        priceAverage: sell.priceAverage,
        status: sell.status,
        modifiedAt: sell.modifiedAt,
        hasDockingPort: sell.hasDockingPort,
        hasFreightElevator: sell.hasFreightElevator,
        hasLoadingDock: sell.hasLoadingDock,
        isCargoCenter: sell.isCargoCenter,
        isAutoLoad: sell.isAutoLoad,
        isRefuel: sell.isRefuel,
        isPlayerOwned: sell.isPlayerOwned,
        isAvailableLive: sell.isAvailableLive,
        maxContainerSize: sell.maxContainerSize,
        terminalCode: sell.terminalCode,
        terminalFullName: sell.terminalFullName,
      });
    }

    for (const buy of group.buys) {
      const commodityKey = `${buy.commodityId || 0}|${buy.commodity}`;
      if (!buyersByCommodity.has(commodityKey)) buyersByCommodity.set(commodityKey, []);
      buyersByCommodity.get(commodityKey).push({
        commodityId: buy.commodityId,
        commodity: buy.commodity,
        groupName: group.name,
        shortGroupName: group.shortName,
        system: group.system,
        locationType: group.locationType,
        atmospheric: group.atmospheric || buy.atmospheric,
        terminalName: buy.terminalName,
        terminalId: buy.terminalId,
        price: buy.price,
        demand: buy.demand,
        demandReported: buy.demandReported,
        demandAverage: buy.demandAverage,
        priceAverage: buy.priceAverage,
        status: buy.status,
        modifiedAt: buy.modifiedAt,
        hasDockingPort: buy.hasDockingPort,
        hasFreightElevator: buy.hasFreightElevator,
        hasLoadingDock: buy.hasLoadingDock,
        isCargoCenter: buy.isCargoCenter,
        isAutoLoad: buy.isAutoLoad,
        isRefuel: buy.isRefuel,
        isPlayerOwned: buy.isPlayerOwned,
        isAvailableLive: buy.isAvailableLive,
        maxContainerSize: buy.maxContainerSize,
        terminalCode: buy.terminalCode,
        terminalFullName: buy.terminalFullName,
      });
    }
  }

  const routes = [];

  for (const [commodityKey, sellersRaw] of sellersByCommodity.entries()) {
    const buyersRaw = buyersByCommodity.get(commodityKey);
    if (!buyersRaw?.length) continue;

    const sellers = [...sellersRaw].sort((a, b) => a.price - b.price).slice(0, 20);
    const buyers = [...buyersRaw].sort((a, b) => b.price - a.price).slice(0, 20);

    for (const seller of sellers) {
      for (const buyer of buyers) {
        if (seller.groupName === buyer.groupName) continue;

        const profitPerScu = buyer.price - seller.price;
        if (profitPerScu <= 0) continue;

        routes.push({
          commodityId: seller.commodityId || buyer.commodityId || 0,
          commodity: seller.commodity,
          buyGroup: seller.groupName,
          buyShortGroup: seller.shortGroupName,
          sellGroup: buyer.groupName,
          sellShortGroup: buyer.shortGroupName,
          buyTerminal: seller.terminalName,
          sellTerminal: buyer.terminalName,
          buyTerminalId: seller.terminalId,
          sellTerminalId: buyer.terminalId,
          buyTerminalCode: seller.terminalCode,
          sellTerminalCode: buyer.terminalCode,
          buyTerminalFullName: seller.terminalFullName,
          sellTerminalFullName: buyer.terminalFullName,
          buySystem: seller.system,
          sellSystem: buyer.system,
          buyLocationType: seller.locationType,
          sellLocationType: buyer.locationType,
          buyRequiresAtmosphere: seller.atmospheric,
          sellRequiresAtmosphere: buyer.atmospheric,
          buyPricePerScu: seller.price,
          sellPricePerScu: buyer.price,
          buyStock: seller.stock,
          buyStockReported: seller.stockReported,
          buyStockAverage: seller.stockAverage,
          sellDemand: buyer.demand,
          sellDemandReported: buyer.demandReported,
          sellDemandAverage: buyer.demandAverage,
          buyPriceAverage: seller.priceAverage,
          sellPriceAverage: buyer.priceAverage,
          buyStatus: seller.status,
          sellStatus: buyer.status,
          buyUpdatedAt: seller.modifiedAt,
          sellUpdatedAt: buyer.modifiedAt,
          buyHasDockingPort: seller.hasDockingPort,
          sellHasDockingPort: buyer.hasDockingPort,
          buyHasFreightElevator: seller.hasFreightElevator,
          sellHasFreightElevator: buyer.hasFreightElevator,
          buyHasLoadingDock: seller.hasLoadingDock,
          sellHasLoadingDock: buyer.hasLoadingDock,
          buyIsCargoCenter: seller.isCargoCenter,
          sellIsCargoCenter: buyer.isCargoCenter,
          buyIsAutoLoad: seller.isAutoLoad,
          sellIsAutoLoad: buyer.isAutoLoad,
          buyIsRefuel: seller.isRefuel,
          sellIsRefuel: buyer.isRefuel,
          buyIsPlayerOwned: seller.isPlayerOwned,
          sellIsPlayerOwned: buyer.isPlayerOwned,
          buyIsAvailableLive: seller.isAvailableLive,
          sellIsAvailableLive: buyer.isAvailableLive,
          buyMaxContainerSize: seller.maxContainerSize,
          sellMaxContainerSize: buyer.maxContainerSize,
          interSystem: seller.system !== buyer.system,
          profitPerScu,
        });
      }
    }
  }

  return routes;
}

async function loadMarketData(force = false) {
  const now = Date.now();
  if (!force && cache.lastUpdated && now - cache.lastUpdated < CACHE_TTL_MS) {
    return cache;
  }

  const [terminalsPayload, pricesPayload, rankingsPayload] = await Promise.all([
    fetchJson('https://api.uexcorp.space/2.0/terminals?type=commodity'),
    fetchJson('https://api.uexcorp.space/2.0/commodities_prices_all'),
    fetchJson('https://api.uexcorp.space/2.0/commodities_ranking'),
  ]);

  const terminals = getArrayPayload(terminalsPayload).filter(
    terminal => String(terminal.type || '').toLowerCase() === 'commodity'
  );
  const prices = getArrayPayload(pricesPayload);
  const rankings = getArrayPayload(rankingsPayload);
  const grouped = buildGroupedLocationIndex(terminals, prices);

  cache.lastUpdated = now;
  cache.groups = grouped.groups;
  cache.shortGroupNames = grouped.shortGroupNames;
  cache.routes = buildRouteIndex(grouped.groups);
  cache.commodityNames = grouped.commodityNames;
  cache.terminalsById = grouped.terminalMap;
  cache.commodityRankings = new Map(rankings.map(row => [Number(row.id), row]));
  cache.commodityRankingsByName = new Map(rankings.map(row => [normalizeText(row.name), row]));

  return cache;
}

function warmMarketData(force = false) {
  const now = Date.now();
  if (!force && cache.lastUpdated && now - cache.lastUpdated < CACHE_TTL_MS) {
    return Promise.resolve(cache);
  }

  if (!force && marketDataWarmPromise) return marketDataWarmPromise;

  const task = loadMarketData(force)
    .catch(error => {
      console.error('Background market data warm failed:', error);
      throw error;
    })
    .finally(() => {
      if (marketDataWarmPromise === task) marketDataWarmPromise = null;
    });

  marketDataWarmPromise = task;
  return task;
}

function getCommodityRanking(route) {
  return (
    cache.commodityRankings.get(Number(route?.commodityId || 0)) ||
    cache.commodityRankingsByName.get(normalizeText(route?.commodity)) ||
    null
  );
}

function buildRouteHintIndex(rows) {
  const index = new Map();
  for (const row of rows) {
    const key = `${Number(row.id_terminal_origin || 0)}|${Number(row.id_terminal_destination || 0)}`;
    index.set(key, row);
  }
  return index;
}

async function getRouteHintIndexForCommodity(commodityId) {
  const id = Number(commodityId || 0);
  if (!id) return new Map();

  const cached = cache.routeHintsByCommodity.get(id);
  if (cached && Date.now() - cached.fetchedAt < ROUTE_HINT_TTL_MS) {
    return cached.index;
  }

  try {
    const payload = await fetchJson(`https://api.uexcorp.space/2.0/commodities_routes?id_commodity=${id}`);
    const rows = getArrayPayload(payload);
    const index = buildRouteHintIndex(rows);
    cache.routeHintsByCommodity.set(id, {
      fetchedAt: Date.now(),
      rows,
      index,
    });
    return index;
  } catch (error) {
    console.error(`Failed loading route hints for commodity ${id}:`, error.message);
    return cached?.index || new Map();
  }
}

function buildPriceHistorySummary(rows, side) {
  const priceKey = side === 'buy' ? 'price_buy' : 'price_sell';
  const statusKey = side === 'buy' ? 'status_buy' : 'status_sell';
  const stockKey = side === 'buy' ? 'scu_buy' : 'scu_sell';
  const sortedRows = [...rows].sort((a, b) => toNumber(a.date_added) - toNumber(b.date_added));
  const values = sortedRows
    .map(row => toNumber(row?.[priceKey]))
    .filter(value => value > 0);

  if (!values.length) return null;

  const latestRow = sortedRows[sortedRows.length - 1];
  const latest = values[values.length - 1];
  const previous = values.length > 1 ? values[values.length - 2] : latest;
  const average = values.reduce((sum, value) => sum + value, 0) / values.length;

  return {
    count: values.length,
    latest,
    previous,
    average,
    minimum: Math.min(...values),
    maximum: Math.max(...values),
    latestStatus: toNumber(latestRow?.[statusKey]),
    latestStock: toNumber(latestRow?.[stockKey]),
    latestAt: unixSecondsToMs(latestRow?.date_added),
    deltaFromAverage: latest - average,
    deltaFromPrevious: latest - previous,
  };
}

async function getPriceHistorySummary(terminalId, commodityId, side) {
  const idTerminal = Number(terminalId || 0);
  const idCommodity = Number(commodityId || 0);
  if (!idTerminal || !idCommodity) return null;

  const key = `${idTerminal}:${idCommodity}:${side}`;
  const cached = cache.priceHistory.get(key);
  if (cached && Date.now() - cached.fetchedAt < PRICE_HISTORY_TTL_MS) {
    return cached.summary;
  }

  try {
    const payload = await fetchJson(`https://api.uexcorp.space/2.0/commodities_prices_history?id_terminal=${idTerminal}&id_commodity=${idCommodity}`);
    const rows = getArrayPayload(payload);
    const summary = buildPriceHistorySummary(rows, side);
    cache.priceHistory.set(key, {
      fetchedAt: Date.now(),
      summary,
    });
    return summary;
  } catch (error) {
    console.error(`Failed loading price history for ${idTerminal}/${idCommodity}/${side}:`, error.message);
    return cached?.summary || null;
  }
}

function getFreshnessInfo(route) {
  const latestRelevant = [route.buyUpdatedAt, route.sellUpdatedAt]
    .map(value => Number(value || 0))
    .filter(Boolean);
  const anchor = latestRelevant.length ? Math.min(...latestRelevant) : 0;
  const ageHours = anchor ? Math.max(0, (Date.now() - anchor) / (60 * 60 * 1000)) : 999;
  let score = 0.25;
  let label = 'Stale';

  if (ageHours <= 1) {
    score = 1;
    label = 'Live';
  } else if (ageHours <= 3) {
    score = 0.93;
    label = 'Fresh';
  } else if (ageHours <= 6) {
    score = 0.85;
    label = 'Recent';
  } else if (ageHours <= 12) {
    score = 0.72;
    label = 'Aging';
  } else if (ageHours <= 24) {
    score = 0.55;
    label = 'Old';
  } else if (ageHours <= 48) {
    score = 0.4;
    label = 'Stale';
  }

  return {
    score,
    ageHours,
    label,
    text: anchor ? `${label} • ${formatFreshnessAge(anchor)}` : 'Freshness unknown',
  };
}

function getNormalizedOriginStatus(status) {
  return clamp(toNumber(status) / 6, 0, 1);
}

function getNormalizedDestinationStatus(status) {
  return clamp(1 - toNumber(status) / 6, 0, 1);
}

function getLiquidityInfo(route, effectiveCargo, hint = null) {
  const stockBasis = firstPositiveNumber(route.buyStock, route.buyStockAverage);
  const demandBasis = firstPositiveNumber(route.sellDemand, route.sellDemandAverage);
  const stockCoverage = stockBasis > 0 && effectiveCargo > 0 ? clamp(stockBasis / effectiveCargo, 0, 1.35) : 0.6;
  const demandCoverage = demandBasis > 0 && effectiveCargo > 0 ? clamp(demandBasis / effectiveCargo, 0, 1.35) : 0.6;
  const originStatus = hint ? getNormalizedOriginStatus(hint.status_origin) : getNormalizedOriginStatus(route.buyStatus);
  const destinationStatus = hint ? getNormalizedDestinationStatus(hint.status_destination) : getNormalizedDestinationStatus(route.sellStatus);
  const score = clamp(
    (Math.min(stockCoverage, 1.2) / 1.2) * 0.34 +
    (Math.min(demandCoverage, 1.2) / 1.2) * 0.34 +
    originStatus * 0.16 +
    destinationStatus * 0.16,
    0,
    1,
  );

  let label = 'Thin';
  if (score >= 0.82) label = 'Deep';
  else if (score >= 0.67) label = 'Healthy';
  else if (score >= 0.5) label = 'Usable';

  return {
    score,
    label,
    stockCoverage,
    demandCoverage,
    text: `${label} • stock ${stockBasis > 0 ? Math.round(stockBasis).toLocaleString('en-GB') : '?'} / demand ${demandBasis > 0 ? Math.round(demandBasis).toLocaleString('en-GB') : '?'}`,
  };
}

function getCommodityMarketInfo(ranking) {
  if (!ranking) {
    return {
      score: 0.48,
      volatilityRatio: 0.75,
      illegal: false,
      text: 'No commodity ranking data',
    };
  }

  const buyAvg = Math.max(1, toNumber(ranking.price_buy_avg, 1));
  const sellAvg = Math.max(1, toNumber(ranking.price_sell_avg, 1));
  const buyVolatility = toNumber(ranking.volatility_price_buy) / buyAvg;
  const sellVolatility = toNumber(ranking.volatility_price_sell) / sellAvg;
  const volatilityRatio = clamp((buyVolatility + sellVolatility) / 2, 0, 3);
  const caxScore = clamp(Math.log10(Math.max(1, toNumber(ranking.cax_score, 1))) / 6, 0, 1);
  const availability = clamp((toNumber(ranking.availability_buy) + toNumber(ranking.availability_sell)) / 40, 0, 1);
  const profitability = clamp(toNumber(ranking.profitability_relative_percentage) / 100, -0.25, 1);
  const score = clamp(
    caxScore * 0.42 +
    availability * 0.23 +
    clamp(1 - (volatilityRatio / 1.25), 0, 1) * 0.2 +
    clamp((profitability + 0.25) / 1.25, 0, 1) * 0.15 -
    (ranking.is_illegal ? 0.15 : 0),
    0,
    1,
  );

  return {
    score,
    volatilityRatio,
    illegal: Boolean(ranking.is_illegal),
    text: `CAX ${Math.round(caxScore * 100)} • avail ${Math.round(availability * 100)} • vol ${(volatilityRatio * 100).toFixed(0)}%`,
  };
}

function getRouteHintQuality(route, hint = null) {
  if (!hint) {
    const access = [
      route.buyHasFreightElevator,
      route.sellHasFreightElevator,
      route.buyHasLoadingDock,
      route.sellHasLoadingDock,
      route.buyHasDockingPort,
      route.sellHasDockingPort,
    ].filter(Boolean).length;

    const score = clamp(0.38 + access * 0.08 - (route.interSystem ? 0.08 : 0), 0, 1);
    return {
      score,
      distance: 0,
      text: 'Heuristic route quality',
      monitoredPair: false,
    };
  }

  const distance = toNumber(hint.distance);
  const scoreValue = clamp(toNumber(hint.score) / 100, 0, 1);
  const userRows = toNumber(hint.price_origin_users_rows) + toNumber(hint.price_destination_users_rows) +
    toNumber(hint.scu_origin_users_rows) + toNumber(hint.scu_destination_users_rows);
  const userData = clamp(userRows / 40, 0, 1);
  const monitored = ((toNumber(hint.is_monitored_origin) ? 1 : 0) + (toNumber(hint.is_monitored_destination) ? 1 : 0)) / 2;
  const accessSignals = [
    hint.has_freight_elevator_origin,
    hint.has_freight_elevator_destination,
    hint.has_loading_dock_origin,
    hint.has_loading_dock_destination,
    hint.has_quantum_marker_origin,
    hint.has_quantum_marker_destination,
    hint.has_refuel_origin,
    hint.has_refuel_destination,
  ].map(value => (toNumber(value) ? 1 : 0));
  const accessScore = accessSignals.reduce((sum, value) => sum + value, 0) / Math.max(1, accessSignals.length);
  const distanceScore = clamp(1 - (distance / 220), 0, 1);
  const score = clamp(
    scoreValue * 0.34 +
    userData * 0.12 +
    monitored * 0.18 +
    accessScore * 0.22 +
    distanceScore * 0.14,
    0,
    1,
  );

  return {
    score,
    distance,
    text: `UEX ${Math.round(scoreValue * 100)} • access ${Math.round(accessScore * 100)} • dist ${distance.toFixed(0)} GM`,
    monitoredPair: monitored === 1,
  };
}

function getFlightTime(route, hint = null) {
  if (!hint) {
    const quantum = route.interSystem ? 18 : 7;
    const endpointOps =
      estimateEndpointTime(route.buyLocationType, route.buyRequiresAtmosphere, route.buyTerminal) +
      estimateEndpointTime(route.sellLocationType, route.sellRequiresAtmosphere, route.sellTerminal);
    const terminalOps = 4;
    const buffer = route.interSystem ? 4 : 2;
    const total = quantum + endpointOps + terminalOps + buffer;

    return { total, quantum, endpointOps, terminalOps, buffer, label: `~${total} min` };
  }

  const distance = toNumber(hint.distance);
  const quantum = route.interSystem
    ? Math.max(16, Math.round(distance * 0.16 + 8))
    : Math.max(4, Math.round(distance * 0.12));
  const originOps =
    3 +
    (toNumber(hint.is_on_ground_origin) ? 4 : 2) +
    (toNumber(hint.has_quantum_marker_origin) ? 0 : 2) +
    (toNumber(hint.has_freight_elevator_origin) || toNumber(hint.has_loading_dock_origin) ? 0 : 2);
  const destinationOps =
    3 +
    (toNumber(hint.is_on_ground_destination) ? 4 : 2) +
    (toNumber(hint.has_quantum_marker_destination) ? 0 : 2) +
    (toNumber(hint.has_freight_elevator_destination) || toNumber(hint.has_loading_dock_destination) ? 0 : 2);
  const terminalOps = 3;
  const buffer = route.interSystem ? 4 : 2;
  const total = quantum + originOps + destinationOps + terminalOps + buffer;

  return {
    total,
    quantum,
    endpointOps: originOps + destinationOps,
    terminalOps,
    buffer,
    label: `~${total} min`,
  };
}

function getLocationRiskModifier(route) {
  let risk = 0;
  const buy = normalizeText(route.buyGroup);
  const sell = normalizeText(route.sellGroup);

  if (buy.includes('ruin station') || sell.includes('ruin station')) risk += 8;
  if (buy.includes('orbituary') || sell.includes('orbituary')) risk += 5;
  if (buy.includes('gateway') || sell.includes('gateway')) risk += 3;

  return risk;
}

function getConfidenceLabel(score) {
  if (score >= 0.85) return 'Exceptional';
  if (score >= 0.72) return 'Strong';
  if (score >= 0.58) return 'Good';
  if (score >= 0.45) return 'Fair';
  return 'Speculative';
}

function getRouteAccessibilityBonus(route, hint = null) {
  let bonus = 0;
  const buyType = normalizeText(route.buyLocationType);
  const sellType = normalizeText(route.sellLocationType);

  if (buyType.includes('station')) bonus += 350;
  if (sellType.includes('station')) bonus += 350;
  if (buyType.includes('city')) bonus += 120;
  if (sellType.includes('city')) bonus += 120;
  if (buyType.includes('outpost')) bonus -= 220;
  if (sellType.includes('outpost')) bonus -= 220;

  if (hint) {
    if (toNumber(hint.has_freight_elevator_origin)) bonus += 180;
    if (toNumber(hint.has_freight_elevator_destination)) bonus += 180;
    if (toNumber(hint.has_loading_dock_origin)) bonus += 150;
    if (toNumber(hint.has_loading_dock_destination)) bonus += 150;
    if (toNumber(hint.has_quantum_marker_origin)) bonus += 120;
    if (toNumber(hint.has_quantum_marker_destination)) bonus += 120;
    if (!toNumber(hint.is_on_ground_origin)) bonus += 60;
    if (!toNumber(hint.is_on_ground_destination)) bonus += 60;
  } else {
    if (route.buyHasFreightElevator) bonus += 150;
    if (route.sellHasFreightElevator) bonus += 150;
    if (route.buyHasLoadingDock) bonus += 120;
    if (route.sellHasLoadingDock) bonus += 120;
  }

  return bonus;
}

function getRiskInfo(route, cargo, cargoValue, ship, hint, freshnessInfo, liquidityInfo, marketInfo) {
  let risk = 0;
  const reasons = [];

  if (route.buySystem === 'Pyro' || route.sellSystem === 'Pyro') risk += 50;
  if (route.buySystem === 'Pyro' || route.sellSystem === 'Pyro') reasons.push('Pyro involvement');
  if (route.buySystem === 'Nyx' || route.sellSystem === 'Nyx') risk += 25;
  if (route.buySystem === 'Nyx' || route.sellSystem === 'Nyx') reasons.push('Nyx involvement');
  if (route.interSystem) risk += 15;
  if (route.interSystem) reasons.push('inter-system hop');
  if (cargo > 750) risk += 10;
  if (cargo > 750) reasons.push('very large cargo load');
  if (cargoValue > 10000000) risk += 15;
  if (cargoValue > 10000000) reasons.push('extremely high cargo value');
  else if (cargoValue > 1000000) risk += 5;
  else if (cargoValue > 1000000) reasons.push('high cargo value');

  risk += getLocationRiskModifier(route);
  risk += ship.shipRiskModifier;

  if (marketInfo.illegal) {
    risk += 30;
    reasons.push('illegal commodity');
  }

  if (marketInfo.volatilityRatio >= 0.8) {
    risk += 12;
    reasons.push('volatile market');
  } else if (marketInfo.volatilityRatio >= 0.55) {
    risk += 6;
    reasons.push('moving prices');
  }

  if (freshnessInfo.score < 0.55) {
    risk += 14;
    reasons.push('stale price data');
  } else if (freshnessInfo.score < 0.72) {
    risk += 6;
    reasons.push('aging reports');
  }

  if (liquidityInfo.score < 0.5) {
    risk += 18;
    reasons.push('thin stock/demand');
  } else if (liquidityInfo.score < 0.67) {
    risk += 8;
    reasons.push('partial fill risk');
  }

  if (hint) {
    if (!toNumber(hint.is_monitored_origin)) {
      risk += 6;
      reasons.push('unmonitored origin');
    }
    if (!toNumber(hint.is_monitored_destination)) {
      risk += 8;
      reasons.push('unmonitored destination');
    }
    if (!toNumber(hint.has_quantum_marker_origin) || !toNumber(hint.has_quantum_marker_destination)) {
      risk += 5;
      reasons.push('weak approach markers');
    }
    if (!toNumber(hint.has_freight_elevator_origin) && !toNumber(hint.has_loading_dock_origin)) {
      risk += 4;
      reasons.push('slow origin loading');
    }
    if (!toNumber(hint.has_freight_elevator_destination) && !toNumber(hint.has_loading_dock_destination)) {
      risk += 4;
      reasons.push('slow destination offload');
    }
  }

  if (ship.military) reasons.push('military hull offsets some route risk');
  else if (ship.cargo <= 32) reasons.push('small cargo footprint');

  const score = Math.round(clamp(risk, 0, 100));
  return {
    score,
    label: getRiskLabel(score),
    reasons: reasons.slice(0, 5),
    text: reasons.slice(0, 5).join(', ') || 'no major risk factors',
  };
}

function estimateEndpointTime(locationType, requiresAtmosphere, terminalName) {
  const type = normalizeText(locationType);
  const terminal = normalizeText(terminalName);
  let time = 2;

  if (type.includes('station')) time += 2;
  if (type.includes('city')) time += 6;
  if (type.includes('outpost')) time += 4;
  if (requiresAtmosphere) time += 4;
  if (terminal.includes('gateway')) time += 3;

  return time;
}

function getFlightTime(route) {
  const quantum = route.interSystem ? 18 : 7;
  const endpointOps =
    estimateEndpointTime(route.buyLocationType, route.buyRequiresAtmosphere, route.buyTerminal) +
    estimateEndpointTime(route.sellLocationType, route.sellRequiresAtmosphere, route.sellTerminal);
  const terminalOps = 4;
  const buffer = route.interSystem ? 4 : 2;
  const total = quantum + endpointOps + terminalOps + buffer;

  return { total, quantum, endpointOps, terminalOps, buffer, label: `~${total} min` };
}

function findMatchingGroup(locationInput) {
  if (!locationInput) return null;
  const needle = normalizeText(locationInput);

  return (
    cache.groups.find(group => normalizeText(group.shortName) === needle || normalizeText(group.name) === needle) ||
    cache.groups.find(group => normalizeText(group.shortName).includes(needle) || normalizeText(group.name).includes(needle)) ||
    null
  );
}

function createSyntheticShipProfile(cargo, label = null) {
  const capacity = Math.max(1, Math.round(Number(cargo || 0)));
  let cargoTier = 'small';
  let shipRiskModifier = 0;

  if (capacity <= 8) {
    cargoTier = 'tiny';
    shipRiskModifier = 8;
  } else if (capacity <= 32) {
    cargoTier = 'small';
    shipRiskModifier = 4;
  } else if (capacity <= 72) {
    cargoTier = 'medium';
    shipRiskModifier = 1;
  } else if (capacity <= 192) {
    cargoTier = 'large';
    shipRiskModifier = -2;
  } else if (capacity <= 696) {
    cargoTier = 'heavy';
    shipRiskModifier = -4;
  } else {
    cargoTier = 'super-heavy';
    shipRiskModifier = -6;
  }

  return {
    name: label || `${capacity} SCU bracket`,
    cargo: capacity,
    military: false,
    cargoTier,
    shipRiskModifier,
  };
}

function scoreRoute(route, desiredCargo, shipName, budget = null, options = {}) {
  const ship = options.shipProfile || getShipProfile(shipName);
  if (!ship) return null;
  const {
    hint = null,
    ranking = getCommodityRanking(route),
  } = options;
  let effectiveCargo = desiredCargo;

  if (budget && route.buyPricePerScu > 0) {
    effectiveCargo = Math.min(effectiveCargo, Math.floor(budget / route.buyPricePerScu));
  }

  effectiveCargo = Math.min(effectiveCargo, ship.cargo);

  if (route.buyStock > 0) effectiveCargo = Math.min(effectiveCargo, route.buyStock);
  if (route.sellDemand > 0) effectiveCargo = Math.min(effectiveCargo, route.sellDemand);
  if (effectiveCargo <= 0) return null;

  const cargoValue = route.buyPricePerScu * effectiveCargo;
  const totalProfit = route.profitPerScu * effectiveCargo;
  const profitPercent = cargoValue > 0 ? (totalProfit / cargoValue) * 100 : 0;
  const fillRatio = desiredCargo > 0 ? effectiveCargo / desiredCargo : 0;
  const freshnessInfo = getFreshnessInfo(route);
  const marketInfo = getCommodityMarketInfo(ranking);
  const routeQuality = getRouteHintQuality(route, hint);
  const liquidityInfo = getLiquidityInfo(route, effectiveCargo, hint);
  const confidenceScore = clamp(
    freshnessInfo.score * 0.24 +
    liquidityInfo.score * 0.33 +
    marketInfo.score * 0.21 +
    routeQuality.score * 0.22,
    0,
    1,
  );
  const risk = getRiskInfo(route, effectiveCargo, cargoValue, ship, hint, freshnessInfo, liquidityInfo, marketInfo);
  const time = getFlightTime(route, hint);
  const expectedProfit = totalProfit * confidenceScore;
  const profitPerMinute = time.total > 0 ? expectedProfit / time.total : expectedProfit;
  const rankingScore =
    (Math.log10(expectedProfit + 1) * 13500) +
    (Math.log10(Math.max(1, profitPerMinute) + 1) * 9500) +
    (Math.min(profitPercent, 150) * 140) +
    (fillRatio * 5200) +
    (confidenceScore * 6500) +
    (routeQuality.score * 2400) +
    getRouteAccessibilityBonus(route, hint) -
    (risk.score * 175) -
    (time.total * 42);

  return {
    ...route,
    effectiveCargo,
    cargoValue,
    totalProfit,
    expectedProfit,
    profitPerMinute,
    profitPercent,
    fillRatio,
    riskScore: risk.score,
    riskLabel: risk.label,
    riskReasons: risk.text,
    riskFactors: risk.reasons,
    time,
    shipProfile: ship,
    hint,
    ranking,
    freshnessInfo,
    marketInfo,
    liquidityInfo,
    confidenceScore,
    confidenceLabel: getConfidenceLabel(confidenceScore),
    routeQuality,
    rankingScore,
  };
}

function getRouteSignature(route) {
  return [
    route.commodity,
    route.buyGroup,
    route.sellGroup,
    route.buyTerminal,
    route.sellTerminal,
    route.effectiveCargo,
  ].join('|');
}

function chooseBestRoute(scoredRoutes, previousSignature = null) {
  if (!scoredRoutes.length) return null;

  if (previousSignature) {
    const different = scoredRoutes.find(route => getRouteSignature(route) !== previousSignature);
    if (different) return different;
  }

  return scoredRoutes[0];
}

async function findBestRoute({ cargo, shipName, shipProfile = null, location, finish, budget, previousSignature = null }) {
  await loadMarketData(false);
  if (!shipProfile) await ensureShipData(false);

  const ship = shipProfile || getShipProfile(shipName);
  if (!ship) throw new Error('Invalid ship.');

  const desiredCargo = cargo || ship.cargo;
  const startGroup = location ? findMatchingGroup(location) : null;
  const finishGroup = finish ? findMatchingGroup(finish) : null;

  const filteredRoutes = cache.routes.filter(route => {
    if (startGroup && route.buyGroup !== startGroup.name) return false;
    if (finishGroup && route.sellGroup !== finishGroup.name) return false;
    return true;
  });

  const preliminaryRoutes = filteredRoutes
    .map(route => scoreRoute(route, desiredCargo, ship.name, budget || null, { shipProfile: ship }))
    .filter(Boolean)
    .sort((a, b) => b.rankingScore - a.rankingScore);

  const topCandidates = preliminaryRoutes.slice(0, 40);
  const commodityIds = Array.from(new Set(
    topCandidates
      .map(route => Number(route.commodityId || 0))
      .filter(Boolean),
  )).slice(0, 10);

  const hintMaps = new Map();
  await Promise.all(commodityIds.map(async commodityId => {
    hintMaps.set(commodityId, await getRouteHintIndexForCommodity(commodityId));
  }));

  const scoredRoutes = topCandidates
    .map(route => {
      const hintKey = `${Number(route.buyTerminalId || 0)}|${Number(route.sellTerminalId || 0)}`;
      const hint = hintMaps.get(Number(route.commodityId || 0))?.get(hintKey) || null;
      return scoreRoute(route, desiredCargo, ship.name, budget || null, {
        shipProfile: ship,
        hint,
        ranking: route.ranking,
      });
    })
    .filter(Boolean)
    .sort((a, b) => b.rankingScore - a.rankingScore);

  return {
    ship,
    desiredCargo,
    route: chooseBestRoute(scoredRoutes, previousSignature),
    alternatives: scoredRoutes.slice(0, 5),
    startGroup,
    finishGroup,
  };
}

function getBracketCaps() {
  return [
    { name: '<25 SCU', cargo: 25 },
    { name: '<100 SCU', cargo: 100 },
    { name: '<250 SCU', cargo: 250 },
    { name: '<500 SCU', cargo: 500 },
    { name: '>500 SCU', cargo: 1000 },
  ];
}

async function getRouteHistoryBundle(route) {
  const [buyHistory, sellHistory] = await Promise.all([
    getPriceHistorySummary(route.buyTerminalId, route.commodityId, 'buy'),
    getPriceHistorySummary(route.sellTerminalId, route.commodityId, 'sell'),
  ]);

  return { buyHistory, sellHistory };
}

function getHistoryNote(history, side) {
  if (!history) return `${side}: no recent history`;
  const delta = history.deltaFromAverage;
  const direction = delta > 0 ? 'above' : delta < 0 ? 'below' : 'at';
  return `${side}: ${Math.round(history.latest).toLocaleString('en-GB')} (${Math.abs(Math.round(delta)).toLocaleString('en-GB')} ${direction} avg)`;
}

function buildRouteRationale(route, historyBundle) {
  const notes = [];

  notes.push(`${route.confidenceLabel} confidence from ${route.freshnessInfo.label.toLowerCase()} reports, ${route.liquidityInfo.label.toLowerCase()} liquidity, and ${route.routeQuality.text.toLowerCase()}.`);

  if (route.marketInfo.illegal) {
    notes.push('This commodity is flagged illegal in the ranking data, so the route is profitable but high-friction.');
  } else if (route.marketInfo.volatilityRatio >= 0.8) {
    notes.push('Commodity pricing is volatile right now, so this route may move faster than the static spread suggests.');
  } else {
    notes.push('Commodity volatility is reasonable, so the current spread is more likely to hold through a single run.');
  }

  notes.push(`${getHistoryNote(historyBundle.buyHistory, 'Buy')} • ${getHistoryNote(historyBundle.sellHistory, 'Sell')}.`);

  return notes.slice(0, 3);
}

function pickAutocompleteChoices(options, current) {
  const needle = normalizeText(current);
  return options
    .filter(option => !needle || normalizeText(option).includes(needle))
    .slice(0, 25)
    .map(option => ({ name: option, value: option }));
}

async function handleAutocomplete(interaction) {
  const focused = interaction.options.getFocused(true);

  if (focused.name === 'ship') {
    void ensureShipData(false).catch(error => {
      console.error('Background ship data warm failed:', error);
    });
    await interaction.respond(pickAutocompleteChoices(getShipChoices(), focused.value));
    return;
  }

  if (focused.name === 'location' || focused.name === 'finish') {
    if (!cache.shortGroupNames.length) {
      void warmMarketData(false).catch(() => {});
      await interaction.respond([]);
      return;
    }

    if (Date.now() - cache.lastUpdated >= CACHE_TTL_MS) {
      void warmMarketData(false).catch(() => {});
    }

    await interaction.respond(pickAutocompleteChoices(cache.shortGroupNames, focused.value));
    return;
  }

  if (focused.name === 'commodity') {
    if (!cache.commodityNames.length) {
      void warmMarketData(false).catch(() => {});
      await interaction.respond([]);
      return;
    }

    if (Date.now() - cache.lastUpdated >= CACHE_TTL_MS) {
      void warmMarketData(false).catch(() => {});
    }

    await interaction.respond(pickAutocompleteChoices(cache.commodityNames, focused.value));
    return;
  }

  await interaction.respond([]);
}

function formatSellListByTerminal(terminals, limitPerTerminal = 8) {
  const lines = [];

  for (const terminal of terminals) {
    if (!terminal.sells.length) continue;

    const items = terminal.sells
      .slice(0, limitPerTerminal)
      .map(item => `${item.commodity} (${item.price.toLocaleString()}, stock ${item.stock || '?'})`)
      .join(', ');

    lines.push(`**${terminal.name}**\n${items}`);
  }

  return lines.length ? lines.slice(0, 10).join('\n\n') : 'Nothing currently listed for sale.';
}

function formatBuyListByTerminal(terminals, limitPerTerminal = 8) {
  const lines = [];

  for (const terminal of terminals) {
    if (!terminal.buys.length) continue;

    const items = terminal.buys
      .slice(0, limitPerTerminal)
      .map(item => `${item.commodity} (${item.price.toLocaleString()}, demand ${item.demand || '?'})`)
      .join(', ');

    lines.push(`**${terminal.name}**\n${items}`);
  }

  return lines.length ? lines.slice(0, 10).join('\n\n') : 'No commodity buy prices currently listed.';
}

function filterBuyersForCommodity(commodity, locationFilter) {
  const commodityNeedle = normalizeText(commodity);
  const locationNeedle = normalizeText(locationFilter);
  const buyers = [];

  for (const group of cache.groups) {
    if (locationNeedle) {
      const match =
        normalizeText(group.shortName).includes(locationNeedle) ||
        normalizeText(group.name).includes(locationNeedle) ||
        normalizeText(group.system).includes(locationNeedle);

      if (!match) continue;
    }

    for (const buy of group.buys) {
      if (normalizeText(buy.commodity) !== commodityNeedle) continue;

      buyers.push({
        groupName: group.name,
        shortGroupName: group.shortName,
        system: group.system,
        terminalName: buy.terminalName,
        price: buy.price,
        demand: buy.demand,
      });
    }
  }

  return buyers.sort((a, b) => b.price - a.price);
}

function buildControlRow(stateId, disabled = false) {
  return new ActionRowBuilder().addComponents(
    new ButtonBuilder()
      .setCustomId(`route:refresh:${stateId}`)
      .setLabel('Refresh')
      .setStyle(ButtonStyle.Primary)
      .setDisabled(disabled),
    new ButtonBuilder()
      .setCustomId(`route:invest_down:${stateId}`)
      .setLabel('Invest -')
      .setStyle(ButtonStyle.Secondary)
      .setDisabled(disabled),
    new ButtonBuilder()
      .setCustomId(`route:invest_up:${stateId}`)
      .setLabel('Invest +')
      .setStyle(ButtonStyle.Secondary)
      .setDisabled(disabled)
  );
}

function getAdjustedBudget(currentBudget, direction) {
  const baseBudget = Math.max(1000, Math.round(Number(currentBudget || 0)));
  const delta = Math.max(1000, Math.round(baseBudget * 0.2));
  const nextBudget = direction === 'up' ? baseBudget + delta : baseBudget - delta;

  return Math.max(1000, Math.round(nextBudget / 1000) * 1000);
}

async function buildRouteResponse(params, existingStateId = null) {
  const result = await findBestRoute(params);

  if (!result.route) {
    return {
      content: 'No matching route found for those filters.',
      embeds: [],
      components: [],
      attachments: [],
      stateId: existingStateId,
    };
  }

  const route = result.route;
  const nextState = {
    shipName: params.shipName,
    cargo: params.cargo ?? null,
    budget: route.cargoValue,
    location: params.location ?? null,
    finish: params.finish ?? null,
    previousSignature: getRouteSignature(route),
  };

  const stateId = saveRouteState(nextState, existingStateId);
  const historyBundle = await getRouteHistoryBundle(route);
  const rationale = buildRouteRationale(route, historyBundle);

  return {
    ...tracker.buildTradeRouteEmbed(route, [buildControlRow(stateId)], historyBundle, rationale),
    stateId,
  };

  const fields = [
    { name: 'Commodity', value: route.commodity, inline: true },
    { name: 'Start', value: route.buyShortGroup, inline: true },
    { name: 'Finish', value: route.sellShortGroup, inline: true },
    { name: 'Cargo Needed', value: `${route.effectiveCargo.toLocaleString()} SCU`, inline: true },
    { name: 'Ship Cargo', value: `${route.shipProfile.cargo.toLocaleString()} SCU`, inline: true },
    { name: 'Investment', value: `${route.cargoValue.toLocaleString()} aUEC`, inline: true },
    { name: 'Profit', value: `${route.totalProfit.toLocaleString()} aUEC`, inline: true },
    { name: 'ROI', value: `${route.profitPercent.toFixed(1)}%`, inline: true },
    { name: 'Risk', value: `${route.riskScore}/100 (${getRiskLabel(route.riskScore)})`, inline: true },
    { name: 'Time', value: route.time.label, inline: true },
    { name: 'Buy Stock', value: route.buyStock ? `${route.buyStock.toLocaleString()} SCU` : 'Unknown', inline: true },
    { name: 'Sell Demand', value: route.sellDemand ? `${route.sellDemand.toLocaleString()} SCU` : 'Unknown', inline: true },
    { name: 'Ship Data Source', value: getShipSourceLabel(), inline: false },
    { name: 'Risk Reasons', value: route.riskReasons, inline: false },
  ];

  const embed = new EmbedBuilder()
    .setColor(0x22d3ee)
    .setTitle(`Best route for ${route.shipProfile.name}`)
    .setDescription(`${route.buyShortGroup} → ${route.sellShortGroup}`)
    .setThumbnail(EMBED_THUMBNAIL_URL)
    .setImage(EMBED_BANNER_URL)
    .addFields(fields)
    .setFooter({
      text: 'SPACEWHLE Trade Command • live UEX grouped location data',
    });

  return {
    content: null,
    embeds: [embed],
    components: [buildControlRow(stateId)],
    stateId,
  };
}

async function handleRouteButton(interaction, action, stateId) {
  const state = getRouteState(stateId);

  if (!state) {
    await interaction.editReply({
      content: 'That button has expired. Run the command again.',
      embeds: [],
      components: [],
    });
    return;
  }

  const lockKey = interaction.message.id;
  if (activeMessageLocks.has(lockKey)) {
    await interaction.followUp({
      content: 'That route is already being updated.',
      ephemeral: true,
    });
    return;
  }

  activeMessageLocks.add(lockKey);

  try {
    await interaction.editReply({
      components: [buildControlRow(stateId, true)],
    });

    const nextState = { ...state };
    const currentBudget = Math.max(1000, Math.round(Number(state.budget || 100000)));

    if (action === 'refresh') {
      nextState.previousSignature = state.previousSignature || null;
    } else if (action === 'invest_up') {
      nextState.budget = getAdjustedBudget(currentBudget, 'up');
      nextState.previousSignature = null;
    } else if (action === 'invest_down') {
      nextState.budget = getAdjustedBudget(currentBudget, 'down');
      nextState.previousSignature = null;
    }

    const response = await buildRouteResponse(nextState, stateId);
    const { stateId: nextStateId, ...payload } = response;
    void nextStateId;
    await interaction.editReply(payload);
  } finally {
    activeMessageLocks.delete(lockKey);
  }
}

client.once(Events.ClientReady, async readyClient => {
  loadStateStoreFromDisk();
  await Promise.allSettled([
    ensureShipData(false),
    warmMarketData(false),
  ]);

  for (const guild of readyClient.guilds.cache.values()) {
    await tracker.hydrateGuild(guild);
  }

  await markStartupState();
  console.log(`Logged in as ${readyClient.user.tag}`);
});

client.on(Events.InteractionCreate, async interaction => {
  if (interaction.isAutocomplete()) {
    try {
      await handleAutocomplete(interaction);
    } catch (error) {
      if (isUnknownInteractionError(error)) {
        console.warn('Autocomplete expired before a response could be sent:', interaction.commandName, interaction.options.getFocused(true)?.name);
        return;
      }

      console.error('Autocomplete error:', error);
      void sendAlert({
        code: 'AUTOCOMPLETE_ERROR',
        summary: 'Autocomplete failed.',
        details: `${interaction.commandName || 'unknown'} autocomplete could not be processed.`,
        error,
      });
      try {
        await interaction.respond([]);
      } catch {}
    }
    return;
  }

  if (interaction.isStringSelectMenu()) {
    try {
      if (typeof tracker.handleSelectMenu === 'function') {
        await interaction.deferUpdate();
        await tracker.handleSelectMenu(interaction);
        return;
      }

      await interaction.reply({
        content: 'That stats menu is not available yet.',
        ephemeral: true,
      });
    } catch (error) {
      console.error('Select menu error:', error);
      void sendAlert({
        code: 'SELECT_MENU_ERROR',
        summary: 'A select menu interaction failed.',
        details: `Custom ID: ${interaction.customId}`,
        error,
      });
      try {
        if (!interaction.replied && !interaction.deferred) {
          await interaction.reply({
            content: 'Could not update that panel.',
            ephemeral: true,
          });
        } else {
          await interaction.followUp({
            content: 'Could not update that panel.',
            ephemeral: true,
          });
        }
      } catch {}
    }
    return;
  }

  if (interaction.isButton()) {
    const parts = interaction.customId.split(':');

    try {
      await interaction.deferUpdate();

      if (parts[0] === 'route') {
        await handleRouteButton(interaction, parts[1], parts[2]);
        return;
      }

      if (parts[0] === 'stats') {
        await tracker.handleButton(interaction);
        return;
      }

      if (parts[0] === 'logistics') {
        if (parts[1] === 'close') {
          const channelId = parts[2];
          const channel = interaction.guild?.channels.cache.get(channelId);
          if (channel) {
            await closeLogisticsTicket(interaction, channel);
          } else {
            await interaction.followUp({
              content: 'Could not find that ticket channel.',
              ephemeral: true,
            });
          }
        }
        return;
      }
    } catch (error) {
      console.error('Button error:', error);
      void sendAlert({
        code: 'BUTTON_ERROR',
        summary: 'A button interaction failed.',
        details: `Custom ID: ${interaction.customId}`,
        error,
      });
      try {
        await interaction.followUp({
          content: 'Could not update that panel.',
          ephemeral: true,
        });
      } catch {}
    }
    return;
  }

  if (!interaction.isChatInputCommand()) return;

  try {
    if (interaction.commandName === 'ping') {
      await interaction.reply('Pong!');
      return;
    }

    await deferChatInputCommand(interaction);

    if (interaction.commandName === 'promote') {
      await handlePromoteCommand(interaction);
      return;
    }

    if (interaction.commandName === 'citizen') {
      await handleCitizenCommand(interaction);
      return;
    }

    if (interaction.commandName === 'me') {
      await handleMeCommand(interaction);
      return;
    }

    if (interaction.commandName === 'route') {
      await deferChatInputCommand(interaction);

      const response = await buildRouteResponse({
        shipName: interaction.options.getString('ship', true),
        cargo: interaction.options.getInteger('cargo'),
        budget: interaction.options.getInteger('budget'),
        location: interaction.options.getString('location'),
        finish: interaction.options.getString('finish'),
      });
      const { stateId, ...payload } = response;
      void stateId;
      await interaction.editReply(payload);
      return;
    }

    if (interaction.commandName === 'best-routes') {
      await deferChatInputCommand(interaction);
      await loadMarketData(false);

      const locationInput = interaction.options.getString('location');
      const finishInput = interaction.options.getString('finish');

      if (!locationInput) {
        await interaction.editReply({ content: 'Choose a starting location for `/best-routes`.' });
        return;
      }

      const startGroup = findMatchingGroup(locationInput);
      const finishGroup = finishInput ? findMatchingGroup(finishInput) : null;

      if (!startGroup) {
        await interaction.editReply({ content: 'I could not find that starting location.' });
        return;
      }

      if (finishInput && !finishGroup) {
        await interaction.editReply({ content: 'I could not find that finishing location.' });
        return;
      }

      const bracketResults = await Promise.all(getBracketCaps().map(async bracket => {
        const bracketShip = createSyntheticShipProfile(bracket.cargo, `${bracket.name} bracket`);
        const result = await findBestRoute({
          cargo: bracket.cargo,
          shipProfile: bracketShip,
          location: startGroup.shortName,
          finish: finishGroup?.shortName || null,
          budget: null,
        });

        return {
          name: bracket.name,
          cargo: bracket.cargo,
          route: result.route || null,
        };
      }));

      await interaction.editReply(tracker.buildBracketRoutesEmbed({
        location: startGroup.shortName,
        finish: finishGroup?.shortName || null,
        brackets: bracketResults,
      }));
      return;

      const lines = [];

      for (const bracket of getBracketCaps()) {
        const result = await findBestRoute({
          shipName,
          cargo: Math.min(bracket.cargo, ship.cargo),
          location: locationInput || null,
          finish: finishInput || null,
          budget: null,
        });

        if (!result.route) {
          lines.push(`**${bracket.name}** — no route found`);
          continue;
        }

        lines.push(
          `**${bracket.name}** — ${result.route.commodity}\n` +
          `${result.route.buyShortGroup} → ${result.route.sellShortGroup}\n` +
          `${result.route.totalProfit.toLocaleString()} aUEC | ROI ${result.route.profitPercent.toFixed(1)}% | ${result.route.time.label}`
        );
      }

      const embed = new EmbedBuilder()
        .setColor(0x38bdf8)
        .setTitle(`Best routes by cargo bracket for ${ship.name}`)
        .setDescription(lines.join('\n\n'))
        .setThumbnail(EMBED_THUMBNAIL_URL)
        .setImage(EMBED_BANNER_URL)
        .addFields(
          { name: 'Start', value: locationInput || 'Any', inline: true },
          { name: 'Finish', value: finishInput || 'Any', inline: true },
          { name: 'Ship Cargo', value: `${ship.cargo.toLocaleString()} SCU`, inline: true },
        )
        .setFooter({
          text: `SPACEWHLE Trade Command • bracket summary • ${getShipSourceLabel()}`,
        });

      await interaction.editReply({ embeds: [embed] });
      return;
    }

    if (interaction.commandName === 'location') {
      await deferChatInputCommand(interaction);
      await loadMarketData(false);

      const locationInput = interaction.options.getString('location', true);
      const group = findMatchingGroup(locationInput);

      if (!group) {
        await interaction.editReply({ content: 'I could not find that location.' });
        return;
      }

      await interaction.editReply(tracker.buildLocationLookupEmbed(group));
      return;

      const embed = new EmbedBuilder()
        .setColor(0x60a5fa)
        .setTitle(group.shortName)
        .setDescription(`Type: **${group.locationType}** | System: **${group.system}**`)
        .setThumbnail(EMBED_THUMBNAIL_URL)
        .setImage(EMBED_BANNER_URL)
        .addFields(
          {
            name: `Commodity Shops (${group.terminals.length})`,
            value: group.terminals.map(t => `• ${t.name}`).join('\n').slice(0, 1024) || 'None',
            inline: false,
          },
          {
            name: 'Sells by Shop',
            value: formatSellListByTerminal(group.terminals).slice(0, 1024),
            inline: false,
          },
          {
            name: 'Buys by Shop',
            value: formatBuyListByTerminal(group.terminals).slice(0, 1024),
            inline: false,
          },
        )
        .setFooter({
          text: 'SPACEWHLE Trade Command • grouped location view',
        });

      await interaction.editReply({ embeds: [embed] });
      return;
    }

    if (interaction.commandName === 'buyers') {
      await deferChatInputCommand(interaction);
      await loadMarketData(false);

      const commodityInput = interaction.options.getString('commodity', true);
      const amountInput = interaction.options.getInteger('amount');
      const locationInput = interaction.options.getString('location');

      const buyers = filterBuyersForCommodity(commodityInput, locationInput)
        .map(buyer => {
          const sellableAmount = amountInput
            ? (buyer.demand > 0 ? Math.min(amountInput, buyer.demand) : amountInput)
            : (buyer.demand > 0 ? buyer.demand : null);

          return {
            ...buyer,
            sellableAmount,
            totalValue: amountInput ? buyer.price * (sellableAmount || amountInput) : null,
          };
        })
        .sort((a, b) => {
          if (amountInput) return (b.totalValue || 0) - (a.totalValue || 0);
          return b.price - a.price;
        })
        .slice(0, 5);

      if (!buyers.length) {
        await interaction.editReply({ content: 'No buyers found for that commodity and location filter.' });
        return;
      }

      await interaction.editReply(tracker.buildBuyersLookupEmbed({
        commodity: commodityInput,
        amount: amountInput || null,
        location: locationInput || null,
        buyers,
      }));
      return;

      const lines = buyers.map((buyer, index) => {
        const extra = amountInput
          ? ` | Sellable: ${buyer.sellableAmount ?? amountInput} SCU | Total: ${(buyer.totalValue || 0).toLocaleString()} aUEC`
          : ` | Demand: ${buyer.demand || 'Unknown'} SCU`;

        return `**${index + 1}. ${buyer.shortGroupName}**\n${buyer.terminalName}\n${buyer.price.toLocaleString()} aUEC / SCU${extra}`;
      }).join('\n\n');

      const embed = new EmbedBuilder()
        .setColor(0xf59e0b)
        .setTitle(`Best buyers for ${commodityInput}`)
        .setDescription(lines)
        .setThumbnail(EMBED_THUMBNAIL_URL)
        .setImage(EMBED_BANNER_URL)
        .addFields(
          { name: 'Commodity', value: commodityInput, inline: true },
          { name: 'Amount', value: amountInput ? `${amountInput.toLocaleString()} SCU` : 'Not set', inline: true },
          { name: 'Location Filter', value: locationInput || 'None', inline: true },
        )
        .setFooter({
          text: 'SPACEWHLE Trade Command • top 5 buyers',
        });

      await interaction.editReply({ embeds: [embed] });
      return;
    }

    if (interaction.commandName === 'players') {
      await deferChatInputCommand(interaction);
      await interaction.editReply(tracker.buildPlayersEmbed(interaction.guild, 7));
      return;
    }

    if (interaction.commandName === 'top') {
      await deferChatInputCommand(interaction);
      await interaction.editReply(await tracker.buildTopEmbed(7));
      return;
    }

    if (interaction.commandName === 'stats') {
      await deferChatInputCommand(interaction);
      const subcommand = interaction.options.getSubcommand(false);
      const user = interaction.options.getUser('user', false);

      if (subcommand === 'user' && user) {
        await interaction.editReply(await tracker.buildUserStatsEmbed(user.id, 7));
        return;
      }

      if (!subcommand && user) {
        await interaction.editReply(await tracker.buildUserStatsEmbed(user.id, 7));
        return;
      }

      await interaction.editReply({
        content: 'Use `/stats user @member` to view a tracked member profile.',
        embeds: [],
        components: [],
        attachments: [],
      });
      return;
    }

    if (interaction.commandName === 'server') {
      await deferChatInputCommand(interaction);
      await interaction.editReply(await tracker.buildServerStatsEmbed(7));
      return;
    }

    if (interaction.commandName === 'ship') {
      await deferChatInputCommand(interaction);
      await ensureShipData(false);
      const shipName = interaction.options.getString('ship', true);
      const ship = getShipProfile(shipName);

      if (!ship) {
        await interaction.editReply({ content: 'I could not find that ship.' });
        return;
      }

      await interaction.editReply(tracker.buildShipLookupEmbed(ship, getShipSourceLabel()));
      return;

      const embed = new EmbedBuilder()
        .setColor(0x8b5cf6)
        .setTitle(ship.name)
        .setThumbnail(EMBED_THUMBNAIL_URL)
        .addFields(
          { name: 'Cargo capacity', value: `${ship.cargo.toLocaleString()} SCU`, inline: true },
          { name: 'Military?', value: ship.military ? 'Yes' : 'No', inline: true },
          { name: 'Cargo tier', value: ship.cargoTier, inline: true },
          { name: 'Ship data source', value: getShipSourceLabel(), inline: false },
        )
        .setFooter({ text: 'Live pull attempted first, then fallback ship data.' });

      await interaction.editReply({ embeds: [embed] });
      return;
    }

    if (interaction.deferred || interaction.replied) {
      await interaction.editReply({
        content: `I do not have a handler for \`/${interaction.commandName}\` in this build.`,
        embeds: [],
        components: [],
      });
    } else {
      await interaction.reply({
        content: `I do not have a handler for \`/${interaction.commandName}\` in this build.`,
        ephemeral: true,
      });
    }
  } catch (error) {
    console.error('Interaction error:', error);
    void sendAlert({
      code: 'COMMAND_ERROR',
      summary: 'A slash command failed.',
      details: `Command: ${interaction.commandName}`,
      error,
    });

    try {
      if (interaction.deferred || interaction.replied) {
        await interaction.editReply({
          content: 'Something went wrong.',
          embeds: [],
          components: [],
        });
      } else {
        await interaction.reply({
          content: 'Something went wrong.',
          ephemeral: true,
        });
      }
    } catch (replyError) {
      console.error('Reply error:', replyError);
    }
  }
 });
// ==============================================================================
// === LOGISTICS TICKET FUNCTIONS ===
// ==============================================================================

const LOGISTICS_CLOSE_ALLOWED_ROLES = String(process.env.LOGISTICS_CLOSE_ALLOWED_ROLES || '')
  .split(',')
  .map(value => value.trim())
  .filter(Boolean);
const LOGISTICS_TRANSCRIPT_CHANNEL_ID = process.env.LOGISTICS_TRANSCRIPT_CHANNEL_ID || null;

function canCloseLogisticsTicket(member) {
  if (!member) return false;
  // Check if they have Administrator permission
  if (member.permissions?.has(PermissionFlagsBits.Administrator)) return true;
  // Check if they have any of the allowed roles
  return LOGISTICS_CLOSE_ALLOWED_ROLES.some(roleId => {
    const resolvedRole = resolveRoleToken(member.guild, roleId);
    return resolvedRole && member.roles.cache.has(resolvedRole.id);
  });
}

async function generateTicketTranscript(channel) {
  try {
    const messages = await channel.messages.fetch({ limit: 100 });
    const sortedMessages = Array.from(messages.values()).reverse();
    
    let transcript = `Logistics Ticket Transcript: ${channel.name}\n`;
    transcript += `Generated: ${new Date().toISOString()}\n`;
    transcript += `Channel: ${channel.name}\n`;
    transcript += `=`.repeat(60) + '\n\n';
    
    for (const msg of sortedMessages) {
      const timestamp = msg.createdAt.toISOString();
      const author = msg.author.username;
      const content = msg.content || '(no text content)';
      transcript += `[${timestamp}] ${author}: ${content}\n`;
      
      if (msg.embeds.length > 0) {
        transcript += `  [Embed: ${msg.embeds[0].title || 'Untitled'}]\n`;
      }
    }
    
    return transcript;
  } catch (error) {
    console.error('Failed to generate transcript:', error);
    return null;
  }
}

async function closeLogisticsTicket(interaction, channel) {
  try {
    const member = await interaction.guild.members.fetch(interaction.user.id);
    
    if (!canCloseLogisticsTicket(member)) {
      await interaction.followUp({
        content: 'You do not have permission to close this logistics ticket.',
        ephemeral: true,
      });
      return;
    }

    // Generate transcript
    const transcript = await generateTicketTranscript(channel);
    
    // Send transcript to transcript channel if configured
    if (LOGISTICS_TRANSCRIPT_CHANNEL_ID && transcript) {
      try {
        const transcriptChannel = await interaction.guild.channels.fetch(LOGISTICS_TRANSCRIPT_CHANNEL_ID);
        if (transcriptChannel && transcriptChannel.isTextBased()) {
          const transcriptEmbed = new EmbedBuilder()
            .setTitle(`Transcript: ${channel.name}`)
            .setColor(0x808080)
            .setDescription(`\`\`\`\n${transcript.slice(0, 4000)}\n\`\`\``)
            .setFooter({ text: `Closed by ${interaction.user.username}` })
            .setTimestamp();
          
          await transcriptChannel.send({ embeds: [transcriptEmbed] });
          console.log(`✅ Transcript saved for ${channel.name}`);
        }
      } catch (transcriptError) {
        console.error('Failed to save transcript:', transcriptError);
      }
    }

    // Delete the channel
    await interaction.followUp({
      content: 'Closing this logistics ticket...',
      ephemeral: true,
    });
    
    await channel.delete(`Closed by ${interaction.user.tag}`);
    console.log(`✅ Closed ticket channel: ${channel.name}`);
    
  } catch (error) {
    console.error('Error closing ticket:', error);
    await interaction.reply({
      content: 'Failed to close the ticket. Check bot permissions.',
      ephemeral: true,
    });
  }
}

// This tells the bot how to respond when the website asks for stats
app.get('/supabase-webhook/:discordId', (req, res) => {
    const discordId = req.params.discordId;
    
    // Path to your database file on this server
    const dbPath = './live_voice_tracker.db'; 
    const db = new sqlite3.Database(dbPath);

    // Query the database for this specific user's stats
    const query = `SELECT messages, voice_seconds, sc_seconds, operations FROM user_stats WHERE user_id = ?`;

    db.get(query, [discordId], (err, row) => {
        db.close();
        if (err) {
            console.error("DB Error:", err.message);
            return res.status(500).json({ error: "Database error" });
        }
        
        if (!row) {
            // If the user isn't in the DB yet, return 0s so the website doesn't crash
            return res.json({ found: false, totals: { operations: 0 } });
        }

        // Send the data back to the website
        res.json({
            found: true,
            totals: {
                messages: row.messages || 0,
                voiceSeconds: row.voice_seconds || 0,
                starCitizenSeconds: row.sc_seconds || 0,
                operations: row.operations || 0 // This number drives your Rank Tracker
            }
        });
    });
});


// ==============================================================================
// === EXPRESS WEB SERVER FOR SUPABASE WEBHOOKS (LOGISTICS TICKET SYSTEM) ===
// ==============================================================================

// Set up your secret key (You should ideally put WEBHOOK_SECRET=YourSecret in your .env file)
const WEBHOOK_SECRET = process.env.WEBHOOK_SECRET || "space_whale_secure_123";

app.post('/supabase-webhook', async (req, res) => {
    console.log("\n-----------------------------------------");
    console.log("📩 NEW WEBHOOK RECEIVED");
    console.log("Headers:", req.headers);
    console.log("Body:", JSON.stringify(req.body, null, 2));

// ADD THIS SAFETY CHECK:
    if (!req.body || !req.body.record) {
        console.warn("⚠️ Ignored invalid webhook payload.");
        return res.status(400).send("Invalid payload");
     }

    // --- SECURITY CHECK ---
    // Make sure the request is actually coming from Supabase
  //  const incomingSecret = req.headers['x-supabase-key'];
    // if (incomingSecret !== WEBHOOK_SECRET) {
      //  console.warn("⚠️ UNAUTHORIZED: Webhook hit without the correct secret key!");
       // return res.status(401).send('Unauthorized');
   // }

    const order = req.body?.record; 

    if (order) {
        const guild = client.guilds.cache.get(process.env.GUILD_ID);
        const categoryId = process.env.LOGISTICS_CATEGORY_ID;
        
        if (guild && categoryId) {
            try {
                // 1. Calculate the next ticket number
                const category = guild.channels.cache.get(categoryId);
                const ticketNumber = category ? (category.children.cache.size + 1) : "x";
                
                // 2. Name the channel sequentially
                const safeChannelName = `logistics-request-${ticketNumber}`;

                // 3. Create the text channel
                const ticketChannel = await guild.channels.create({
                    name: safeChannelName,
                    type: ChannelType.GuildText,
                    parent: categoryId,
                    reason: `Logistics Order #${ticketNumber} created via Website`,
                });

                // 4. Setup Pings
                const requesterPing = `<@${order.user_discord_id}>`;
                const rolePing = process.env.LOGISTICS_ROLE_ID ? `<@&${process.env.LOGISTICS_ROLE_ID}>` : "";

                const embed = new EmbedBuilder()
                    .setTitle(`📦 Logistics Request #${ticketNumber}`)
                    .setColor(0x027320)
                    .setDescription(`New request submitted by ${requesterPing}`)
                    .addFields(
                        { name: "Item Requested", value: order.item || "Unknown", inline: true },
                        { name: "Quantity", value: order.quantity ? order.quantity.toString() : "N/A", inline: true },
                        { name: "Delivery Location", value: order.location || "Not specified", inline: false }
                    )
                    .setFooter({ text: "Use this channel to coordinate. Delete when complete." })
                    .setTimestamp();

                // 5. Create close button
                const closeButton = new ButtonBuilder()
                  .setCustomId(`logistics:close:${ticketChannel.id}`)
                  .setLabel('Close Ticket')
                  .setStyle(ButtonStyle.Danger);
                
                const buttonRow = new ActionRowBuilder().addComponents(closeButton);

                // 6. Send the message with button
                await ticketChannel.send({ 
                    content: `${rolePing} ${requesterPing} - A new logistics ticket has been opened for your request.`, 
                    embeds: [embed],
                    components: [buttonRow],
                });

                console.log(`✅ Successfully created ticket channel: ${safeChannelName}`);
                return res.status(200).send('Ticket Created');

            } catch (error) {
                console.error("❌ Failed to create ticket channel in Discord:", error);
                return res.status(500).send('Discord Error');
            }
        } else {
            console.error("❌ Guild or Category ID not found! Check your .env file.");
            return res.status(500).send('Config Error');
        }
    } else {
        console.warn("⚠️ Webhook received, but no 'record' object was found in the body.");
        return res.status(400).send('No record found');
    }
   });

// ==============================================================================
// === MEMBER STATS API (Combines JSON Tracker + SQLite DB) ===
// ==============================================================================

app.get('/supabase-webhook/:discordId', (req, res) => {
    // Allow the website to read this data
    res.setHeader('Access-Control-Allow-Origin', 'https://join.spacewhle.org');
    res.setHeader('Access-Control-Allow-Methods', 'GET');

    const discordId = req.params.discordId;

    // 1. Get the data from your existing stats-state.json (via tracker.js)
    let jsonStats = { messages: 0, voiceSeconds: 0, starCitizenSeconds: 0 };
    try {
        const stats = tracker.getUserStats(discordId, 30); // Grabs 30 days of data
        if (stats && stats.totals) {
            jsonStats.messages = stats.totals.messages || 0;
            jsonStats.voiceSeconds = stats.totals.voiceSeconds || 0;
            jsonStats.starCitizenSeconds = stats.totals.starCitizenSeconds || 0;
        }
    } catch (error) {
        console.error('Tracker read error:', error);
    }

    // 2. Get the "Operations" from the live_voice_tracker.db
    const dbPath = './live_voice_tracker.db';
    const db = new sqlite3.Database(dbPath);
    
    // IMPORTANT: Make sure 'user_stats' is the correct table name we found earlier
    const query = `SELECT operations FROM user_stats WHERE user_id = ?`;

    db.get(query, [discordId], (err, row) => {
        db.close();
        
        let opsCount = 0;
        if (!err && row) {
            opsCount = row.operations || 0;
        } else if (err) {
            console.error("DB Error:", err.message);
        }

        // 3. Send the COMBINED data back to the website
        return res.json({
            found: true,
            totals: {
                messages: jsonStats.messages,
                voiceSeconds: jsonStats.voiceSeconds,
                starCitizenSeconds: jsonStats.starCitizenSeconds,
                operations: opsCount // Feeds the Rank Tracker
            }
        });
    });
});

// ==============================================================================
// === BOT LOGIN AND SERVER START ===
// ==============================================================================

client.once(Events.ClientReady, c => {
    console.log(`Logged in as ${c.user.tag}`);
});

// Log the bot into Discord
client.login(process.env.DISCORD_TOKEN);

// Start the Express server on Port 8080
app.listen(8080, '0.0.0.0', () => {
    console.log('🎧 Webhook server is listening on port 8080');
});
