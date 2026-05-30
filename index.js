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
app.use((req, res, next) => {
    res.header('Access-Control-Allow-Origin', '*');
    res.header('Access-Control-Allow-Methods', 'GET, POST, PUT, PATCH, DELETE, OPTIONS');
    res.header('Access-Control-Allow-Headers', 'Content-Type, Authorization');
    if (req.method === 'OPTIONS') return res.sendStatus(204);
    next();
});

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
  Partials,
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
    GatewayIntentBits.GuildMessageReactions,
    GatewayIntentBits.GuildVoiceStates,
    GatewayIntentBits.MessageContent,
  ],
  partials: [Partials.Message, Partials.Reaction, Partials.User, Partials.Channel],
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
// Ordered SPACEWHLE rank ladder, low -> high. Mirrors RANK_LADDER_ROLE_IDS used
// by the promotion-announce endpoint. Rank in this org IS a Discord role
// (promotions add/remove exactly these), so /me resolves a member's current
// rank straight from their roles rather than the roster DB.
const RANK_LADDER = [
  { id: '1308509681613934633', name: 'Operator' },
  { id: '1366867228376694824', name: 'Lance Corporal' },
  { id: '1366867077155262524', name: 'Corporal' },
  { id: '1366866972159246346', name: 'Sergeant' },
  { id: '1366866853724553357', name: 'Staff Sergeant' },
  { id: '1388620399398621194', name: 'Master Sergeant' },
  { id: '1386802432134090783', name: 'Sergeant Major' },
  { id: '1429466916157919394', name: 'Officer Cadet' },
  { id: '1366866733205295214', name: 'Second Lieutenant' },
  { id: '1366866565898698915', name: 'Lieutenant' },
  { id: '1388620682145042593', name: 'Wing Commander' },
  { id: '1308509585874747495', name: 'Captain' },
  { id: '1308509266055008378', name: 'Major' },
  { id: '1366876915083776060', name: 'Lieutenant Colonel' },
  { id: '1308509088807780382', name: 'Colonel' },
  { id: '1308508914668535839', name: 'Brigadier' },
  { id: '1354881826178469898', name: 'Lieutenant General' },
  { id: '1308706708683493397', name: 'General' },
  { id: '1308508590809813013', name: 'Field Marshal' },
];
// Hard-default to the SPACEWHLE promotions channel. The env var still
// wins if set, so we can flip targets without redeploying.
const PROMOTIONS_CHANNEL_ID = process.env.PROMOTIONS_CHANNEL_ID || '1308529431907663872';
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

// Highest rank-ladder role the member currently holds (RANK_LADDER is ordered
// low -> high, so the last match wins). Returns null if they hold none.
function resolveMemberRank(member) {
  if (!member || !member.roles || !member.roles.cache) return null;
  let rank = null;
  for (const tier of RANK_LADDER) {
    if (member.roles.cache.has(tier.id)) rank = tier.name;
  }
  return rank;
}

// /me — the calling member's own SPACEWHLE profile, built entirely from data
// the bot already has: rank from their Discord roles, plus all-time activity +
// live leaderboard position from the same scoring the public /leaderboard uses.
async function handleMeCommand(interaction) {
  await deferChatInputCommand(interaction);

  try {
    const userId = interaction.user.id;
    const displayName =
      (interaction.member && interaction.member.displayName) ||
      interaction.user.globalName ||
      interaction.user.username ||
      'Member';

    // Rank straight from Discord roles (source of truth). Refetch the member if
    // the interaction didn't carry a role cache (e.g. stale gateway state).
    let member = interaction.member;
    if ((!member || !member.roles || !member.roles.cache) && interaction.guild) {
      member = await interaction.guild.members.fetch(userId).catch(() => null);
    }
    const rank = resolveMemberRank(member);

    // All-time board (days >= 3650 => all-time in the tracker), scored with the
    // SAME formula the public leaderboard uses. Full board (not sliced) so the
    // caller is always findable even outside the top 50.
    const board = await getScoredLeaderboard(9999);
    const idx = board.findIndex(r => String(r.discord_id) === String(userId));
    const me = idx >= 0 ? board[idx] : null;

    let messages = 0, voiceHours = 0, scHours = 0, eventsAttended = 0, score = 0;
    if (me) {
      messages = me.messages || 0;
      voiceHours = me.voice_hours || 0;
      scHours = me.sc_hours || 0;
      eventsAttended = me.events_attended || 0;
      score = me.score || 0;
    } else {
      // Not on the board yet (no recorded activity) — fall back to raw totals.
      try {
        const t = tracker.getUserTotals(userId, 9999) || {};
        messages = t.messages || 0;
        voiceHours = Math.round((t.voiceSeconds || 0) / 3600 * 10) / 10;
        scHours = Math.round((t.starCitizenSeconds || 0) / 3600 * 10) / 10;
        score = ((t.messages || 0) * 250) + ((t.voiceSeconds || 0) * 8) + ((t.starCitizenSeconds || 0) * 0.3);
      } catch (e) {
        console.error('/me tracker totals error:', e);
      }
    }

    const nf = n => Number(n || 0).toLocaleString('en-GB');
    const position = idx >= 0
      ? `#${nf(idx + 1)} of ${nf(board.length)}`
      : 'Unranked';

    const embed = new EmbedBuilder()
      .setColor(0x22d3ee)
      .setTitle(`${displayName}'s Profile`)
      .setThumbnail(interaction.user.displayAvatarURL({ size: 128 }))
      .addFields(
        { name: 'Rank', value: rank || 'Unranked', inline: true },
        { name: 'Events Attended', value: nf(eventsAttended), inline: true },
        { name: 'Activity Score', value: nf(Math.round(score)), inline: true },
        { name: 'Messages', value: nf(messages), inline: true },
        { name: 'Voice Time', value: `${nf(voiceHours)} h`, inline: true },
        { name: 'Star Citizen Time', value: `${nf(scHours)} h`, inline: true },
        { name: 'Activity Leaderboard', value: position, inline: false },
      )
      .setFooter({ text: 'All-time stats - SPACEWHLE' })
      .setTimestamp();

    await interaction.editReply({ embeds: [embed] });
  } catch (error) {
    console.error('/me command failed:', error);
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
  // The `channel` option was removed — promotions always go to the
  // configured PROMOTIONS_CHANNEL_ID (defaulted to the SPACEWHLE
  // promotions channel at the top of this file).
  const channel = PROMOTIONS_CHANNEL_ID
    ? await interaction.guild.channels.fetch(PROMOTIONS_CHANNEL_ID).catch(() => null)
    : null;

  if (!channel) {
    return {
      error: 'No promotions channel is configured. Set `PROMOTIONS_CHANNEL_ID`.',
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

// SPACEWHLE rank-name → AUEC promotion bonus. Keys are normalised by
// lowercasing and stripping non-alphanumerics so "Wing Commander", "wg cdr"
// and "WgCdr" all match the same entry.
const PROMOTION_AUEC_BONUS = {
    'major': '500k aUEC',                 'maj': '500k aUEC',
    'captain': '400k aUEC',               'capt': '400k aUEC',
    'wingcommander': '350k aUEC',         'wgcdr': '350k aUEC',
    'lieutenant': '300k aUEC',            'lt': '300k aUEC',
    'secondlieutenant': '275k aUEC',      '2lt': '275k aUEC',
    'sergeantmajor': '250k aUEC',         'sm': '250k aUEC',
    'mastersergeant': '200k aUEC',        'msg': '200k aUEC',
    'staffsergeant': '150k aUEC',         'ssgt': '150k aUEC',
    'sergeant': '125k aUEC',              'sgt': '125k aUEC'
};
function lookupPromotionBonus(rankName) {
    if (!rankName) return null;
    const key = String(rankName).toLowerCase().replace(/[^a-z0-9]/g, '');
    return PROMOTION_AUEC_BONUS[key] || null;
}

function buildPromotionAnnouncement(targetMember, rankRole, addedRoles, customMessage) {
    const userMention = targetMember && targetMember.toString ? targetMember.toString() : `<@${targetMember.id || targetMember}>`;
    const rankMention = `<@&${rankRole.id}>`;
    const lines = [
        `Congratulations ${userMention} on their promotion 🎉`,
        '',
        `Promotion: ${rankMention}`
    ];
    const bonus = lookupPromotionBonus(rankRole.name);
    if (bonus) {
        lines.push('');
        lines.push(`AUEC Bonus: ${bonus}`);
    }
    if (customMessage) {
        lines.push('');
        lines.push(customMessage);
    }
    return lines.join('\n');
}

// /promote-spacewhle — light-touch welcome flow:
//   - Silently grants the base SPACEWHLE role (no ping for it).
//   - Removes the three "applicant" / temporary roles if present.
//   - Posts "Welcome to SPACEWHLE!" pinging the orientation role.
// Only the `member` option is exposed; nothing else to configure.
const SPW_BASE_ROLE_ID            = '1308522990119686284';
const SPW_WELCOME_PING_ROLE_ID    = '1383554402182238208';
const SPW_REMOVE_ROLE_IDS         = ['1354890945421774989', '1309576204751339635', '1308509770965323776'];

async function handlePromoteSpacewhleCommand(interaction) {
  // Already deferred by the dispatcher above.
  if (!interaction.guild) {
    await interaction.editReply('This command can only be run inside a server.');
    return;
  }
  const target = interaction.options.getUser('member', true);
  let targetMember;
  try {
    targetMember = await interaction.guild.members.fetch(target.id);
  } catch {
    await interaction.editReply('Could not find that member in this server.');
    return;
  }

  const baseRole = interaction.guild.roles.cache.get(SPW_BASE_ROLE_ID);
  if (!baseRole) {
    await interaction.editReply('The SPACEWHLE base role is missing from this server.');
    return;
  }

  // Add the SPACEWHLE role (silent — no ping for it).
  try {
    if (!targetMember.roles.cache.has(SPW_BASE_ROLE_ID)) {
      await targetMember.roles.add(baseRole, `Welcomed into SPACEWHLE by ${interaction.user.tag}`);
    }
  } catch (e) {
    await interaction.editReply(`Failed to add SPACEWHLE role: ${e.message}`);
    return;
  }

  // Strip the temporary / applicant roles if present.
  const toRemove = SPW_REMOVE_ROLE_IDS.filter(id => targetMember.roles.cache.has(id));
  if (toRemove.length) {
    try {
      await targetMember.roles.remove(toRemove, 'Cleanup on SPACEWHLE welcome');
    } catch (e) {
      console.warn('promote-spacewhle remove-roles failed:', e.message);
    }
  }

  // Post the welcome message into the same channel the command was run in.
  // Format the user explicitly asked for:
  //   Congratulations @user on their promotion 🎉
  //   Promotion: @SPACEWHLE.
  //   Welcome to SPACEWHLE!
  // Ping only the promoted user — the SPACEWHLE role renders as a styled
  // mention but does NOT ping everyone in it (allowedMentions.roles is empty).
  const channel = interaction.channel;
  if (channel && channel.isTextBased()) {
    try {
      const content = [
        `Congratulations <@${target.id}> on their promotion 🎉`,
        '',
        `Promotion: <@&${SPW_BASE_ROLE_ID}>.`,
        '',
        `Welcome to SPACEWHLE!`
      ].join('\n');
      await channel.send({
        content,
        allowedMentions: { users: [target.id], roles: [] }
      });
    } catch (e) {
      console.warn('promote-spacewhle channel send failed:', e.message);
    }
  }

  await interaction.editReply(`✅ Welcomed <@${target.id}> into SPACEWHLE (added role, pinged welcome).`);
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
        roles: [rankRole.id],
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

  // Seed VC-attendance presence from anyone already in the op channels, then
  // kick off the first silent Discord-ID → roster attach scan.
  try { _seedAttendancePresence(); } catch (e) { console.error('attendance seed error:', e); }
  setTimeout(() => {
    attachDiscordIdsToRoster()
      .then(r => console.log('Discord-ID attach:', JSON.stringify(r)))
      .catch(() => {});
  }, 30 * 1000);

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

    if (parts[0] === 'rsvp') {
      await handleRsvpButton(interaction, parts[1], parts[2]);
      return;
    }

    if (parts[0] === 'evnotify') {
      await handleEventNotifyButton(interaction, parts[1]);
      return;
    }

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

    if (interaction.commandName === 'promote-spacewhle') {
      await handlePromoteSpacewhleCommand(interaction);
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

    if (interaction.commandName === 'event') {
      await deferChatInputCommand(interaction);
      const sub = interaction.options.getSubcommand(false);
      if (sub === 'status') {
        await interaction.editReply(buildEventStatusEmbed());
      } else {
        await interaction.editReply({
          content: 'Use `/event status` to see live attendance tracking — who the bot is seeing and whether it is recording.',
          embeds: [],
          components: [],
        });
      }
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
        // Mirror the order into the bot's local store first, so members-area
        // order history works despite Supabase RLS hiding the table from anon
        // reads. Done before ticket creation so it's captured even if Discord
        // channel creation later fails.
        recordLogisticsOrderLocally(order);

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

// This tells the bot how to respond when the website asks for stats
app.get('/member-stats/:discordId', async (req, res) => {
    const discordId = req.params.discordId;
    const discordName = req.query.username || '';

    // 1. Get Voice & Message stats from the JSON Tracker.
    // The rank tracker card on members-area renders these values as the
    // member's all-time stats — use a sentinel "9999 days" window which
    // the tracker treats as effectively unbounded.
    let jsonStats = { messages: 0, voiceSeconds: 0, starCitizenSeconds: 0 };
    try {
        // Totals-only — avoids building the 9999-element daily series this
        // endpoint never uses (was ~1.9s of wasted work; now a few ms).
        const totals = tracker.getUserTotals(discordId, 9999);
        if (totals) jsonStats = totals;
    } catch (error) {
        console.error("Tracker read error:", error);
    }

    // 2. Fetch roster data from the Django API
    let rosterData = null;
    if (discordName) {
        try {
            const rosterRes = await fetch(`https://api.spacewhle.org/api/roster/${encodeURIComponent(discordName)}`);
            if (rosterRes.ok) {
                const rosterJson = await rosterRes.json();
                if (rosterJson.found) {
                    rosterData = rosterJson;
                    delete rosterData.found;
                }
            }
        } catch (error) {
            console.error("Roster fetch error:", error);
        }
    }

    // 3. Send combined data back to the website
    return res.json({
        found: true,
        totals: {
            messages: jsonStats.messages,
            voiceSeconds: jsonStats.voiceSeconds,
            starCitizenSeconds: jsonStats.starCitizenSeconds
        },
        roster: rosterData
    });
});

// ==============================================================================
// === PUBLIC API: DISCORD SCHEDULED EVENTS ===
// ==============================================================================

app.get('/events', async (req, res) => {
    res.header('Access-Control-Allow-Origin', '*');
    try {
        const SPACEWHLE_GUILD_ID = '1308340574457303042';
        const guild = client.guilds.cache.get(SPACEWHLE_GUILD_ID);
        if (!guild) return res.status(503).json({ error: 'Bot not in guild yet' });

        const scheduledEvents = await guild.scheduledEvents.fetch();
        const now = Date.now();

        const events = scheduledEvents
            .filter(e => (e.status === 1 || e.status === 2) &&
                         e.scheduledStartTimestamp >= now)
            .sort((a, b) => a.scheduledStartTimestamp - b.scheduledStartTimestamp)
            .map(e => ({
                id:                   e.id,
                name:                 e.name,
                description:          e.description || '',
                scheduled_start_time: e.scheduledStartAt?.toISOString(),
                entity_type:          e.entityType,
                entity_metadata:      e.entityMetadata,
                status:               e.status,
                user_count:           e.userCount ?? 0
            }));

        res.json(events);
    } catch (err) {
        console.error('GET /events error:', err);
        res.status(500).json({ error: 'Failed to fetch events' });
    }
});

// ==============================================================================
// === PUBLIC API: ACTIVITY LEADERBOARD ===
// ==============================================================================

// Shared activity-leaderboard builder: returns ALL tracked members, enriched
// with roster event counts and scored high -> low (NOT sliced). The scoring here
// intentionally mirrors the GET /leaderboard endpoint below (events x10000,
// messages x250, voice secs x8, SC secs x0.3) so /me reports the exact same
// ranking members see on the site. Keep the two formulas identical if either
// ever changes.
async function getScoredLeaderboard(days) {
    const board = tracker.getLeaderboard(days);
    const allTrackerRows = board.all || [];

    const guild = client.guilds.cache.get(SPACEWHLE_GUILD_ID);
    if (guild && guild.members.cache.size < (guild.memberCount || 0) * 0.5) {
        try { await guild.members.fetch(); } catch (e) { console.warn('LB member fetch failed:', e.message); }
    }

    const identityFor = (userId, fallbackName) => {
        const m = guild && userId ? guild.members.cache.get(userId) : null;
        if (!m) return { displayName: fallbackName, username: (fallbackName || '').toLowerCase() };
        return {
            displayName: m.displayName || m.user?.globalName || m.user?.username || fallbackName,
            username:    (m.user?.username || fallbackName || '').toLowerCase()
        };
    };

    const eventsByHandle = new Map();
    try {
        const rosterRes = await fetch('https://api.spacewhle.org/api/roster-list');
        if (rosterRes.ok) {
            const rosterJson = await rosterRes.json();
            for (const m of (rosterJson.members || [])) {
                const key = String(m.discord_name || '').toLowerCase();
                if (key) eventsByHandle.set(key, (m.events || 0) + (m.soma || 0) + (m.orders || 0));
            }
        }
    } catch (e) { console.warn('LB roster-list fetch failed:', e.message); }

    return allTrackerRows.map(u => {
        const identity = identityFor(u.userId, u.username);
        const eventsAttended =
            eventsByHandle.get(identity.username)
            ?? eventsByHandle.get(String(u.username || '').toLowerCase())
            ?? 0;
        return {
            discord_id:      u.userId,
            display_name:    identity.displayName || u.username,
            messages:        u.messages || 0,
            voice_hours:     Math.round((u.voiceSeconds || 0) / 3600 * 10) / 10,
            sc_hours:        Math.round((u.starCitizenSeconds || 0) / 3600 * 10) / 10,
            events_attended: eventsAttended,
            // Scoring: events x10000, messages x250, voice secs x8, SC secs x0.3
            score: (eventsAttended * 10000)
                 + ((u.messages || 0) * 250)
                 + ((u.voiceSeconds || 0) * 8)
                 + ((u.starCitizenSeconds || 0) * 0.3)
        };
    })
    .sort((a, b) => b.score - a.score);
}

app.get('/leaderboard', async (req, res) => {
    try {
        const days = parseInt(req.query.days) || 30;
        const board = tracker.getLeaderboard(days);
        const allTrackerRows = board.all || [];

        // ── Robust enrichment for EVERY tracked member ──────────────────
        // Two batch lookups instead of per-user API calls so the result is
        // correct regardless of member count, with no premature slicing and
        // no rate-limit risk:
        //   1. Bulk-fetch the whole guild once → resolve userId →
        //      { displayName, username } from cache. Fixes the case where
        //      the tracker's stored display name ("Omoz_2021") doesn't match
        //      the roster handle ("omoz_").
        //   2. Pull the entire roster in one call → map handle → participation
        //      counts (events + soma + orders).
        const guild = client.guilds.cache.get(SPACEWHLE_GUILD_ID);
        // The bot keeps a warm member cache via the GuildMembers intent, so
        // only pay for a full member fetch when the cache looks cold (e.g.
        // right after a restart). A full fetch of ~140 members costs ~1–3s
        // and was running on EVERY leaderboard call — now it's rare.
        if (guild && guild.members.cache.size < (guild.memberCount || 0) * 0.5) {
            try { await guild.members.fetch(); } catch (e) { console.warn('LB member fetch failed:', e.message); }
        }

        function identityFor(userId, fallbackName) {
            const m = guild && userId ? guild.members.cache.get(userId) : null;
            if (!m) return { displayName: fallbackName, username: (fallbackName || '').toLowerCase() };
            return {
                displayName: m.displayName || m.user?.globalName || m.user?.username || fallbackName,
                username:    (m.user?.username || fallbackName || '').toLowerCase()
            };
        }

        // Roster participation, keyed by lowercased discord handle.
        const eventsByHandle = new Map();
        try {
            const rosterRes = await fetch('https://api.spacewhle.org/api/roster-list');
            if (rosterRes.ok) {
                const rosterJson = await rosterRes.json();
                for (const m of (rosterJson.members || [])) {
                    const key = String(m.discord_name || '').toLowerCase();
                    if (key) eventsByHandle.set(key, (m.events || 0) + (m.soma || 0) + (m.orders || 0));
                }
            }
        } catch (e) { console.warn('LB roster-list fetch failed:', e.message); }

        const rows = allTrackerRows.map(u => {
            const identity = identityFor(u.userId, u.username);
            // Look up events by current username first, then the tracker
            // name — both lowercased — covering renames + cache misses.
            const eventsAttended =
                eventsByHandle.get(identity.username)
                ?? eventsByHandle.get(String(u.username || '').toLowerCase())
                ?? 0;
            return {
                discord_id:      u.userId,
                display_name:    identity.displayName || u.username,
                messages:        u.messages || 0,
                voice_hours:     Math.round((u.voiceSeconds || 0) / 3600 * 10) / 10,
                sc_hours:        Math.round((u.starCitizenSeconds || 0) / 3600 * 10) / 10,
                events_attended: eventsAttended,
                // Scoring: events ×10000, messages ×250, voice secs ×8 (heavy), SC secs ×0.3 (low)
                score: (eventsAttended * 10000)
                     + ((u.messages || 0) * 250)
                     + ((u.voiceSeconds || 0) * 8)
                     + ((u.starCitizenSeconds || 0) * 0.3)
            };
        })
        .sort((a, b) => b.score - a.score)
        .slice(0, 50); // frontend renders top 10 + (you ± 1); needs buffer

        res.json(rows);
    } catch (err) {
        console.error('GET /leaderboard error:', err);
        res.status(500).json({ error: 'Failed to fetch leaderboard' });
    }
});


// ==============================================================================
// === PUBLIC API: PAST DISCORD EVENTS (completed operations log) ===
// ==============================================================================

app.get("/events/past", async (req, res) => {
    res.header("Access-Control-Allow-Origin", "*");
    try {
        const SPACEWHLE_GUILD_ID = "1308340574457303042";
        const guild = client.guilds.cache.get(SPACEWHLE_GUILD_ID);
        if (!guild) return res.status(503).json({ error: "Bot not in guild yet" });

        const scheduledEvents = await guild.scheduledEvents.fetch();

        const events = Array.from(scheduledEvents.values())
            .filter(e => e.status === 3 || e.status === 4 ||
                         (e.scheduledStartTimestamp && e.scheduledStartTimestamp < Date.now()))
            .sort((a, b) => b.scheduledStartTimestamp - a.scheduledStartTimestamp)
            .slice(0, 50)
            .map(e => ({
                id:                   e.id,
                name:                 e.name,
                description:          e.description || "",
                scheduled_start_time: e.scheduledStartAt ? e.scheduledStartAt.toISOString() : null,
                entity_type:          e.entityType,
                status:               e.status
            }));

        res.json({ events });
    } catch (err) {
        console.error("GET /events/past error:", err);
        res.status(500).json({ error: "Failed to fetch past events" });
    }
});


// ==============================================================================
// === ADMIN AUTH HELPER (verify Discord OAuth token) ===
// ==============================================================================
const SPACEWHLE_GUILD_ID = '1308340574457303042';
const SUPABASE_URL = 'https://ybclydugcpwwrtfrodet.supabase.co';
const ADMIN_USERNAMES = new Set(['ltz_solar', 'dro.p', 'omoz_', 'aidenatx', 'sukii']);
const ADMIN_ROLE_IDS = new Set([
    '1366876915083776060', // Lt Colonel
    '1308509088807780382', // Colonel
    '1308508914668535839', // Brigadier
    '1354881826178469898', // Lieutenant General
    '1308706708683493397', // General
    '1308508590809813013', // Field Marshal
]);

// ──────────────────────────────────────────────────────────────────────
// Public verification — frontend hits this so it doesn't have to call the
// Discord OAuth API on every page load (provider_token expires within an
// hour, which forces re-login). Bot uses its own bot token to check guild
// membership / roles, so this works as long as the bot is alive.
// ──────────────────────────────────────────────────────────────────────
app.get('/verify/:discordId', async (req, res) => {
    res.header('Access-Control-Allow-Origin', '*');
    try {
        const discordId = String(req.params.discordId || '').trim();
        if (!/^\d{10,25}$/.test(discordId)) {
            return res.status(400).json({ error: 'invalid discordId' });
        }
        const guild = client.guilds.cache.get(SPACEWHLE_GUILD_ID);
        if (!guild) return res.status(503).json({ error: 'bot not in guild yet' });
        // Cache-first: this is on the dashboard-reveal critical path. The
        // member cache is kept warm by the GuildMembers intent, so a cache
        // hit avoids a Discord REST round-trip (~150-300ms saved per load).
        let member = guild.members.cache.get(discordId);
        if (!member) {
            try {
                member = await guild.members.fetch(discordId);
            } catch {
                return res.json({ found: false, roles: [] });
            }
        }
        const roles = [...member.roles.cache.keys()].filter(id => id !== SPACEWHLE_GUILD_ID);
        res.json({
            found: true,
            user_id: member.id,
            username: member.user.username,
            display_name: member.displayName || member.user.globalName || member.user.username,
            roles
        });
    } catch (err) {
        console.error('GET /verify error:', err);
        res.status(500).json({ error: err.message });
    }
});

// Roster vs Discord audit — for each member on the roster, check whether
// they're in the Discord guild AND have the SPACEWHLE base role. Admin
// only.
app.get('/admin/roster-vs-discord', async (req, res) => {
    res.header('Access-Control-Allow-Origin', '*');
    const admin = await verifyAdmin(req);
    if (!admin) return res.status(403).json({ error: 'Unauthorized' });
    try {
        const guild = client.guilds.cache.get(SPACEWHLE_GUILD_ID);
        if (!guild) return res.status(503).json({ error: 'bot not in guild yet' });

        // 1. Pull the public roster
        const rosterRes = await fetch('https://api.spacewhle.org/api/admin/roster', {
            headers: { Authorization: req.headers.authorization || '' }
        });
        if (!rosterRes.ok) {
            return res.status(502).json({ error: 'roster fetch failed', status: rosterRes.status });
        }
        const roster = await rosterRes.json();
        const members = roster.members || roster || [];

        // 2. Fetch full guild member list (cached after first fetch)
        await guild.members.fetch().catch(() => {});

        const SPACEWHLE_ROLE_ID = '1308522990119686284';
        const guildByUsername = new Map();
        for (const m of guild.members.cache.values()) {
            guildByUsername.set(m.user.username.toLowerCase(), m);
        }

        const missing = []; // not in guild at all
        const noRole  = []; // in guild but lacks SPACEWHLE role
        for (const r of members) {
            const handle = String(r.discord || r.discord_name || '').toLowerCase().trim();
            if (!handle) continue;
            const m = guildByUsername.get(handle);
            if (!m) { missing.push({ discord: handle, rsi: r.rsi_handle || '', rank: r.rank || '' }); continue; }
            if (!m.roles.cache.has(SPACEWHLE_ROLE_ID)) {
                noRole.push({ discord: handle, rsi: r.rsi_handle || '', rank: r.rank || '' });
            }
        }
        res.json({
            roster_count: members.length,
            guild_count:  guild.members.cache.size,
            missing,
            no_role: noRole
        });
    } catch (err) {
        console.error('GET /admin/roster-vs-discord error:', err);
        res.status(500).json({ error: err.message });
    }
});

async function verifyAdmin(req) {
    const auth = req.headers.authorization || '';
    if (!auth.startsWith('Bearer ')) return null;
    const token = auth.slice(7);

    // Resolve the caller's Discord identity from the bearer token. TWO token
    // types are accepted so admin web sessions survive long past the ~1h Discord
    // OAuth token lifetime (the cause of the "logged out every hour" problem):
    //   1. Discord OAuth access token (legacy `provider_token`)  -> /users/@me
    //   2. Supabase access token (a JWT, auto-refreshed for days) -> /auth/v1/user,
    //      whose user_metadata carries the Discord id as `provider_id`.
    // A Supabase JWT has exactly two dots; a Discord token has none, so we probe
    // the most likely source first and fall back to the other.
    async function fromDiscord() {
        try {
            const r = await fetch('https://discord.com/api/v10/users/@me', {
                headers: { Authorization: `Bearer ${token}` }
            });
            if (!r.ok) return null;
            const d = await r.json();
            return d.id ? { userId: d.id, username: (d.username || '').toLowerCase() } : null;
        } catch (e) { return null; }
    }
    async function fromSupabase() {
        try {
            const r = await fetch(`${SUPABASE_URL}/auth/v1/user`, {
                headers: {
                    Authorization: `Bearer ${token}`,
                    apikey: process.env.SUPABASE_ANON_KEY || ''
                }
            });
            if (!r.ok) return null;
            const u = await r.json();
            const meta = u.user_metadata || {};
            const uid = meta.provider_id || meta.sub || null;
            const uname = String(meta.user_name || meta.preferred_username || meta.name || meta.full_name || '').toLowerCase();
            return uid ? { userId: uid, username: uname } : null;
        } catch (e) { return null; }
    }

    const looksLikeJwt = (token.match(/\./g) || []).length === 2;
    const ident = looksLikeJwt
        ? (await fromSupabase()) || (await fromDiscord())
        : (await fromDiscord()) || (await fromSupabase());
    if (!ident || !ident.userId) return null;
    const { userId, username } = ident;

    try {
        if (username && ADMIN_USERNAMES.has(username)) {
            return { userId, username };
        }
        // Role check via the BOT's OWN token — reliable regardless of whether
        // the user's OAuth token carries the guilds.members.read scope. Older
        // authorizations don't have it (Discord won't re-prompt on re-login),
        // which is why role-based admins were getting 403 / "session expired"
        // while the username allowlist kept working.
        try {
            const guild = client.guilds.cache.get(SPACEWHLE_GUILD_ID);
            if (guild && userId) {
                let member = guild.members.cache.get(userId);
                if (!member) member = await guild.members.fetch(userId).catch(() => null);
                if (member && [...ADMIN_ROLE_IDS].some(r => member.roles.cache.has(r))) {
                    return { userId, username };
                }
            }
        } catch (e) { /* fall through to the user-token fallback below */ }
        // Fallback: original user-token guild lookup (Discord tokens only; a
        // Supabase JWT won't authenticate against Discord, which is fine — the
        // bot-token check above already covers the role lookup).
        const guildRes = await fetch(`https://discord.com/api/v10/users/@me/guilds/${SPACEWHLE_GUILD_ID}/member`, {
            headers: { Authorization: `Bearer ${token}` }
        });
        if (guildRes.ok) {
            const guildData = await guildRes.json();
            const roles = new Set(guildData.roles || []);
            if ([...ADMIN_ROLE_IDS].some(r => roles.has(r))) {
                return { userId, username };
            }
        }
        return null;
    } catch (e) {
        console.error('verifyAdmin error:', e);
        return null;
    }
}

// ==============================================================================
// === MEMBER DAILY STATS (for history chart) ===
// ==============================================================================
app.get('/member-stats/:discordId/daily', async (req, res) => {
    const discordId = req.params.discordId;
    const days = Math.max(1, Math.min(365, parseInt(req.query.days) || 30));
    try {
        const stats = tracker.getUserStats(discordId, days);
        if (!stats) return res.json({ found: false, daily: [] });
        return res.json({
            found: true,
            days,
            totals: stats.totals,
            daily: stats.daily
        });
    } catch (err) {
        console.error('GET /member-stats/:id/daily error:', err);
        res.status(500).json({ error: 'Failed to fetch daily stats' });
    }
});

// ==============================================================================
// === ADMIN: GUILD INFO (channels, roles, emojis for event creator) ===
// ==============================================================================
app.get('/admin/guild-info', async (req, res) => {
    const admin = await verifyAdmin(req);
    if (!admin) return res.status(403).json({ error: 'Unauthorized' });
    try {
        const guild = client.guilds.cache.get(SPACEWHLE_GUILD_ID);
        if (!guild) return res.status(503).json({ error: 'Bot not in guild' });

        await guild.roles.fetch();
        await guild.channels.fetch();
        await guild.emojis.fetch();

        // Restrict the channel list to the channels the calling admin
        // can actually see in their client. Without this, announcement
        // and event-creator dropdowns expose hidden channels (private
        // staff threads, archived ops). Falls back to "no filter" when
        // we can't resolve the calling admin's roles (rare).
        let callerRoles = null;
        try {
            const callerMember = await guild.members.fetch(admin.userId).catch(() => null);
            if (callerMember) callerRoles = callerMember;
        } catch {}

        const channels = guild.channels.cache
            .filter(c => c.type === ChannelType.GuildText || c.type === ChannelType.GuildAnnouncement)
            .filter(c => {
                if (!callerRoles) return true; // can't check — let them through
                const perms = c.permissionsFor(callerRoles);
                return perms && perms.has(PermissionFlagsBits.ViewChannel);
            })
            .map(c => ({ id: c.id, name: c.name, parent: c.parent ? c.parent.name : null }))
            .sort((a, b) => a.name.localeCompare(b.name));

        const roles = guild.roles.cache
            .filter(r => r.id !== guild.id)
            .map(r => ({ id: r.id, name: r.name, color: r.hexColor, position: r.position }))
            .sort((a, b) => b.position - a.position);

        const emojis = guild.emojis.cache.map(e => ({
            id: e.id,
            name: e.name,
            animated: e.animated,
            url: e.imageURL(),
            string: e.toString()
        }));

        res.json({ channels, roles, emojis });
    } catch (err) {
        console.error('GET /admin/guild-info error:', err);
        res.status(500).json({ error: 'Failed to fetch guild info' });
    }
});

// ==============================================================================
// === ADMIN: ANNOUNCEMENT COMPOSER ===
// ==============================================================================
app.post('/admin/announce', async (req, res) => {
    const admin = await verifyAdmin(req);
    if (!admin) return res.status(403).json({ error: 'Unauthorized' });
    try {
        const { channel_id, message, mention_role_ids, ping_everyone, ping_here } = req.body || {};
        if (!channel_id || !message) return res.status(400).json({ error: 'channel_id and message required' });
        const guild = client.guilds.cache.get(SPACEWHLE_GUILD_ID);
        if (!guild) return res.status(503).json({ error: 'Bot not in guild' });
        const channel = guild.channels.cache.get(channel_id);
        if (!channel || !channel.isTextBased()) return res.status(404).json({ error: 'Channel not found' });

        const roleIds = Array.isArray(mention_role_ids) ? mention_role_ids : [];
        const prefixParts = [];
        if (ping_everyone) prefixParts.push('@everyone');
        else if (ping_here) prefixParts.push('@here');
        for (const id of roleIds) prefixParts.push(`<@&${id}>`);
        const prefix = prefixParts.join(' ');
        const content = prefix ? `${prefix}\n${message}` : message;

        const allowedParse = [];
        if (ping_everyone || ping_here) allowedParse.push('everyone');

        const sent = await channel.send({
            content,
            allowedMentions: { parse: allowedParse, roles: roleIds }
        });
        console.log(`[announce] ${admin.username} sent ${sent.id} to #${channel.name} (everyone=${!!ping_everyone}, here=${!!ping_here}, roles=${roleIds.length})`);
        res.json({ ok: true, message_id: sent.id });
    } catch (err) {
        console.error('POST /admin/announce error:', err);
        res.status(500).json({ error: 'Failed to send announcement: ' + err.message });
    }
});

// ==============================================================================
// === REFERRAL PROCESSOR ===
// ==============================================================================
app.post('/admin/process-referrals', async (req, res) => {
    const admin = await verifyAdmin(req);
    if (!admin) return res.status(403).json({ error: 'Unauthorized' });
    try {
        const supabaseAnon = process.env.SUPABASE_ANON_KEY || '';
        if (!supabaseAnon) return res.status(500).json({ error: 'SUPABASE_ANON_KEY not configured' });

        const refRes = await fetch(`${SUPABASE_URL}/rest/v1/referrals?bonus_awarded=eq.false&select=*`, {
            headers: { apikey: supabaseAnon, Authorization: `Bearer ${supabaseAnon}` }
        });
        if (!refRes.ok) {
            const txt = await refRes.text();
            return res.status(500).json({ error: 'Supabase fetch failed: ' + txt });
        }
        const referrals = await refRes.json();
        if (!referrals.length) return res.json({ ok: true, processed: 0, awarded: 0 });

        let awarded = 0;
        for (const ref of referrals) {
            try {
                const recruitName = ref.recruit_name;
                if (!recruitName) continue;
                const rosterRes = await fetch(`https://api.spacewhle.org/api/roster/${encodeURIComponent(recruitName)}/`);
                if (!rosterRes.ok) continue;
                const roster = await rosterRes.json();
                if (!roster.found) continue;
                const totalOps = (roster.events || 0) + (roster.soma || 0) + (roster.orders || 0);
                if (totalOps < 1) continue;
                const referrerName = ref.referrer_name;
                if (!referrerName) continue;
                const updateRes = await fetch('https://api.spacewhle.org/api/admin/update-member', {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/json',
                        Authorization: req.headers.authorization
                    },
                    body: JSON.stringify({
                        discord_name: referrerName,
                        rsi_handle: '',
                        rank: 'Op',
                        division_operations: '',
                        division_logistics: '',
                        division_medical: '',
                        division_academic: '',
                        points_attendance: 1,
                        points_soma: 0,
                        points_orders: 0
                    })
                });
                if (!updateRes.ok) {
                    console.warn(`Failed to award ${referrerName}: ${await updateRes.text()}`);
                    continue;
                }
                await fetch(`${SUPABASE_URL}/rest/v1/referrals?id=eq.${ref.id}`, {
                    method: 'PATCH',
                    headers: {
                        apikey: supabaseAnon,
                        Authorization: `Bearer ${supabaseAnon}`,
                        'Content-Type': 'application/json'
                    },
                    body: JSON.stringify({ bonus_awarded: true, bonus_awarded_at: new Date().toISOString() })
                });
                awarded++;
            } catch (e) {
                console.warn('Referral process error for one row:', e.message);
            }
        }
        res.json({ ok: true, processed: referrals.length, awarded });
    } catch (err) {
        console.error('POST /admin/process-referrals error:', err);
        res.status(500).json({ error: err.message });
    }
});

// ==============================================================================
// === SESH-STYLE EVENT CREATOR ===
// ==============================================================================
const CUSTOM_EVENTS_FILE = path.join(__dirname, 'events_custom.json');

function loadCustomEvents() {
    try {
        if (!fs.existsSync(CUSTOM_EVENTS_FILE)) return { events: [] };
        return JSON.parse(fs.readFileSync(CUSTOM_EVENTS_FILE, 'utf8'));
    } catch (e) {
        console.error('loadCustomEvents error:', e);
        return { events: [] };
    }
}

function saveCustomEvents(data) {
    try {
        fs.writeFileSync(CUSTOM_EVENTS_FILE, JSON.stringify(data, null, 2));
    } catch (e) {
        console.error('saveCustomEvents error:', e);
    }
}

// --- Local mirror of logistics orders -----------------------------------------
// Supabase RLS hides logistics_orders from the anon role and the bot has no
// service-role key, so it cannot read order history back out of Supabase. To
// make the members-area "Order History" work anyway, every order that arrives
// through the Supabase insert webhook is also appended to this local JSON store,
// which /user/logistics-orders reads from (merged with Supabase if a
// service-role key is ever configured).
const LOGISTICS_ORDERS_FILE = path.join(__dirname, 'logistics_orders_local.json');
const LOGISTICS_LOCAL_MAX = 5000;

function loadLocalLogisticsOrders() {
    try {
        if (!fs.existsSync(LOGISTICS_ORDERS_FILE)) return { orders: [] };
        const data = JSON.parse(fs.readFileSync(LOGISTICS_ORDERS_FILE, 'utf8'));
        return data && Array.isArray(data.orders) ? data : { orders: [] };
    } catch (e) {
        console.error('loadLocalLogisticsOrders error:', e);
        return { orders: [] };
    }
}

function saveLocalLogisticsOrders(data) {
    try {
        fs.writeFileSync(LOGISTICS_ORDERS_FILE, JSON.stringify(data, null, 2));
    } catch (e) {
        console.error('saveLocalLogisticsOrders error:', e);
    }
}

function recordLogisticsOrderLocally(order) {
    try {
        if (!order || !order.user_discord_id) return;
        const data = loadLocalLogisticsOrders();
        data.orders.push({
            id: order.id != null ? order.id : null,
            item: order.item || '',
            quantity: order.quantity != null ? order.quantity : null,
            location: order.location || '',
            user_discord_id: String(order.user_discord_id),
            created_at: order.created_at || new Date().toISOString()
        });
        // Keep the file bounded; newest entries stay at the end.
        if (data.orders.length > LOGISTICS_LOCAL_MAX) {
            data.orders = data.orders.slice(-LOGISTICS_LOCAL_MAX);
        }
        saveLocalLogisticsOrders(data);
    } catch (e) {
        console.error('recordLogisticsOrderLocally error:', e);
    }
}

// ==============================================================================
// === AUTOMATIC VOICE-CHANNEL EVENT ATTENDANCE ===
// ------------------------------------------------------------------------------
// While an event is live (start_time → end_time) the bot passively records how
// long each member sits in any of the designated operation voice channels. A
// member counts as "participating" if they were present for at least 70% of the
// event window. Admins review + confirm participation from the admin page,
// which awards event points to each participant on the roster.
//
// This is completely independent of tracker.js / stats-state.json and never
// touches leaderboard scoring.
// ==============================================================================
const ATTENDANCE_FILE = path.join(__dirname, 'event_attendance.json');
const ATTENDANCE_CHANNEL_IDS = new Set([
    '1366419233319292958',
    '1370728873377005591',
    '1370728909271859340',
    '1378349304640704532',
    '1390697755898417245',
    '1391148928438632630',
    '1425211796175847575',
]);
const ATTENDANCE_THRESHOLD = 0.70; // ≥70% of the event window in VC = attended.
const BOT_SHARED_SECRET = process.env.BOT_SHARED_SECRET || '';

function loadAttendance() {
    try {
        if (!fs.existsSync(ATTENDANCE_FILE)) return { events: {} };
        const d = JSON.parse(fs.readFileSync(ATTENDANCE_FILE, 'utf8'));
        if (!d || typeof d !== 'object') return { events: {} };
        if (!d.events || typeof d.events !== 'object') d.events = {};
        return d;
    } catch (e) {
        console.error('loadAttendance error:', e);
        return { events: {} };
    }
}
function saveAttendance(data) {
    try {
        fs.writeFileSync(ATTENDANCE_FILE, JSON.stringify(data, null, 2));
    } catch (e) {
        console.error('saveAttendance error:', e);
    }
}

function _eventWindowMs(ev) {
    const s = new Date(ev.start_time).getTime();
    const e = ev.end_time
        ? new Date(ev.end_time).getTime()
        : s + (Number(ev.duration_minutes) || 60) * 60 * 1000;
    return { s, e };
}

// userId -> { since: epoch-ms, username, display_name }
const _attendPresence = new Map();

// Roll the elapsed [since → now] window for every currently-present member into
// any event whose window overlaps it, then advance each member's "since" cursor
// to now. Overlap is clamped to each event window, so time spent before start
// or after end is never counted.
function _accrueAttendance(now = Date.now()) {
    if (_attendPresence.size === 0) return;
    let evData;
    try { evData = loadCustomEvents(); } catch { evData = { events: [] }; }
    const events = Array.isArray(evData.events) ? evData.events : [];
    if (events.length === 0) {
        // No events to accrue into — still advance cursors so idle time isn't
        // retro-counted if an event later appears.
        for (const p of _attendPresence.values()) p.since = now;
        return;
    }
    const att = loadAttendance();
    let dirty = false;
    for (const [userId, p] of _attendPresence) {
        const from = p.since;
        if (now <= from) continue;
        for (const ev of events) {
            if (!ev || !ev.id || !ev.start_time) continue;
            const { s, e } = _eventWindowMs(ev);
            const ovStart = Math.max(from, s);
            const ovEnd = Math.min(now, e);
            const overlapSec = Math.floor((ovEnd - ovStart) / 1000);
            if (overlapSec <= 0) continue;
            let erec = att.events[ev.id];
            if (!erec) {
                erec = att.events[ev.id] = {
                    title: ev.title || 'Operation',
                    start_time: ev.start_time,
                    end_time: new Date(e).toISOString(),
                    duration_minutes: Number(ev.duration_minutes) || 60,
                    channel_id: ev.channel_id || null,
                    members: {},
                    points_applied: false,
                    applied_at: null,
                    applied_points: 0
                };
            }
            // Keep metadata fresh in case the event was edited after creation.
            erec.title = ev.title || erec.title;
            erec.start_time = ev.start_time;
            erec.end_time = new Date(e).toISOString();
            erec.duration_minutes = Number(ev.duration_minutes) || erec.duration_minutes || 60;
            let mrec = erec.members[userId];
            if (!mrec) mrec = erec.members[userId] = { username: p.username, display_name: p.display_name, seconds: 0 };
            if (p.username) mrec.username = p.username;
            if (p.display_name) mrec.display_name = p.display_name;
            mrec.seconds = (mrec.seconds || 0) + overlapSec;
            dirty = true;
        }
        p.since = now;
    }
    if (dirty) saveAttendance(att);
}

// Seed presence from whoever is already sitting in the tracked channels — e.g.
// after a bot restart mid-event. Loses the pre-restart slice but keeps counting.
function _seedAttendancePresence() {
    try {
        const guild = client.guilds.cache.get(SPACEWHLE_GUILD_ID);
        if (!guild) return;
        const now = Date.now();
        for (const chId of ATTENDANCE_CHANNEL_IDS) {
            const ch = guild.channels.cache.get(chId);
            if (!ch || !ch.members) continue;
            for (const m of ch.members.values()) {
                if (!m || m.user?.bot) continue;
                _attendPresence.set(m.id, {
                    since: now,
                    username: m.user?.username || 'unknown',
                    display_name: m.displayName || m.user?.globalName || m.user?.username || 'unknown'
                });
            }
        }
    } catch (e) {
        console.error('_seedAttendancePresence error:', e);
    }
}

// Entry/exit tracking for the designated voice channels. Multiple listeners for
// the same gateway event are fine — tracker.js has its own, this is additive.
client.on(Events.VoiceStateUpdate, (oldState, newState) => {
    try {
        const member = newState.member || oldState.member;
        if (!member || member.user?.bot) return;
        const userId = member.id;
        const oldCh = oldState.channelId || null;
        const newCh = newState.channelId || null;
        const isTracked = !!(newCh && ATTENDANCE_CHANNEL_IDS.has(newCh));
        const wasTracked = !!(oldCh && ATTENDANCE_CHANNEL_IDS.has(oldCh));
        if (!isTracked && !wasTracked) return; // churn in an untracked channel

        // Close out everyone's elapsed time at the exact moment of transition.
        _accrueAttendance(Date.now());

        if (isTracked) {
            const username = member.user?.username || 'unknown';
            const display_name = member.displayName || member.user?.globalName || username;
            const existing = _attendPresence.get(userId);
            if (existing) {
                existing.username = username;
                existing.display_name = display_name;
            } else {
                _attendPresence.set(userId, { since: Date.now(), username, display_name });
            }
        } else {
            _attendPresence.delete(userId);
        }
    } catch (e) {
        console.error('attendance voiceStateUpdate error:', e);
    }
});

// Periodic accrual so long uninterrupted sessions still get recorded, and an
// event that ends mid-session captures its final slice.
setInterval(() => { try { _accrueAttendance(); } catch (e) { console.error('attendance tick error:', e); } }, 30 * 1000);

// Build an at-a-glance status of the live attendance tracker for `/event status`:
// whether an event is being recorded right now, who the bot currently sees in the
// operation voice channels, and per-member progress toward the attendance threshold.
function buildEventStatusEmbed() {
    const now = Date.now();
    // Freshen accrued seconds right up to this moment so the report isn't up to 30s stale.
    try { _accrueAttendance(now); } catch {}

    let evData;
    try { evData = loadCustomEvents(); } catch { evData = { events: [] }; }
    const events = Array.isArray(evData.events) ? evData.events : [];

    const live = [], upcoming = [];
    for (const ev of events) {
        if (!ev || !ev.start_time) continue;
        const { s, e } = _eventWindowMs(ev);
        if (now >= s && now < e) live.push({ ev, s, e });
        else if (now < s) upcoming.push({ ev, s, e });
    }
    live.sort((a, b) => a.e - b.e);
    upcoming.sort((a, b) => a.s - b.s);

    const att = loadAttendance();
    const watching = _attendPresence.size;
    const thrPct = Math.round(ATTENDANCE_THRESHOLD * 100);

    const fmtDur = (secs) => {
        secs = Math.max(0, Math.floor(secs || 0));
        const h = Math.floor(secs / 3600), m = Math.floor((secs % 3600) / 60), s = secs % 60;
        if (h) return `${h}h ${m}m`;
        if (m) return `${m}m ${s}s`;
        return `${s}s`;
    };

    const embed = new EmbedBuilder()
        .setColor(live.length ? 0x57f287 : 0x5865f2)
        .setTitle('📡 Event Attendance — Status')
        .setFooter({ text: `Threshold ${thrPct}% · ${ATTENDANCE_CHANNEL_IDS.size} operation channels watched · confirm & award points on the admin page` })
        .setTimestamp(new Date());

    if (live.length) {
        embed.setDescription(`🔴 **Recording now** — ${live.length} live event${live.length > 1 ? 's' : ''}. Watching **${watching}** member${watching !== 1 ? 's' : ''} in the operation voice channels.`);
    } else {
        embed.setDescription(`⚪ **Idle** — no event is live right now, so nothing is being recorded. The bot is watching the operation voice channels and will start automatically when the next event's start time arrives.\nIn those channels right now: **${watching}** member${watching !== 1 ? 's' : ''}.`);
    }

    for (const { ev, s, e } of live.slice(0, 3)) {
        const totalSec = Math.max(1, Math.floor((e - s) / 1000));
        const remainMin = Math.max(0, Math.round((e - now) / 60000));
        const rec = att.events[ev.id];
        const members = rec && rec.members ? rec.members : {};
        const rows = Object.values(members)
            .sort((a, b) => (b.seconds || 0) - (a.seconds || 0))
            .slice(0, 15)
            .map(m => {
                const pct = Math.min(100, Math.round((m.seconds || 0) / totalSec * 100));
                const mark = pct >= thrPct ? '✅' : '⏳';
                return `${mark} **${m.display_name || m.username || 'unknown'}** — ${fmtDur(m.seconds || 0)} · ${pct}%`;
            });
        const body = rows.length ? rows.join('\n') : '_No one recorded in the operation channels yet._';
        embed.addFields({
            name: `🔴 ${(ev.title || 'Operation').slice(0, 240)} — ${remainMin}m left`,
            value: body.slice(0, 1024),
        });
    }

    if (!live.length && watching) {
        const names = [..._attendPresence.values()]
            .map(p => `• ${p.display_name || p.username || 'unknown'}`)
            .slice(0, 25);
        embed.addFields({ name: '👀 In the operation channels now', value: names.join('\n').slice(0, 1024) });
    }

    if (upcoming.length) {
        const { ev, s } = upcoming[0];
        const unix = Math.floor(s / 1000);
        embed.addFields({
            name: '⏭️ Next event',
            value: `**${(ev.title || 'Operation').slice(0, 240)}**\nStarts <t:${unix}:F> (<t:${unix}:R>)`,
        });
    } else if (!live.length) {
        embed.addFields({ name: '⏭️ Next event', value: '_No upcoming events scheduled._' });
    }

    return { embeds: [embed] };
}

// ── Silently attach Discord IDs to roster members ────────────────────────────
// Scans the guild, matches each roster discord_name to a live member's username,
// and pushes {discord_name → discord_id} to Django. Server-to-server via a
// shared secret so it can run unattended. Only writes the discord_id column.
async function attachDiscordIdsToRoster() {
    try {
        if (!BOT_SHARED_SECRET) return { ok: false, error: 'BOT_SHARED_SECRET not set' };
        const guild = client.guilds.cache.get(SPACEWHLE_GUILD_ID);
        if (!guild) return { ok: false, error: 'bot not in guild' };
        await guild.members.fetch().catch(() => {});
        const byUsername = new Map();
        for (const m of guild.members.cache.values()) {
            if (m.user?.bot) continue;
            byUsername.set(m.user.username.toLowerCase(), m.id);
        }
        const rRes = await fetch('https://api.spacewhle.org/api/roster-list');
        if (!rRes.ok) return { ok: false, error: 'roster-list ' + rRes.status };
        const rJson = await rRes.json();
        const mappings = [];
        for (const m of (rJson.members || [])) {
            const name = String(m.discord_name || '').toLowerCase().trim();
            if (!name) continue;
            const id = byUsername.get(name);
            if (id) mappings.push({ discord_name: m.discord_name, discord_id: id });
        }
        if (mappings.length === 0) return { ok: true, attached: 0, matched: 0 };
        const sRes = await fetch('https://api.spacewhle.org/api/admin/attach-discord-ids', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json', 'X-Bot-Secret': BOT_SHARED_SECRET },
            body: JSON.stringify({ mappings })
        });
        if (!sRes.ok) return { ok: false, error: 'attach ' + sRes.status };
        const sJson = await sRes.json().catch(() => ({}));
        return { ok: true, attached: sJson.updated ?? mappings.length, matched: mappings.length };
    } catch (e) {
        console.error('attachDiscordIdsToRoster error:', e);
        return { ok: false, error: e.message };
    }
}
// Re-scan a few times a day. The first run is kicked off from the ready handler.
setInterval(() => { attachDiscordIdsToRoster().catch(() => {}); }, 6 * 60 * 60 * 1000);

function buildEventEmbed(ev) {
    const embed = new EmbedBuilder()
        .setTitle(ev.title)
        .setColor(0x2e8b57)
        .setTimestamp(new Date(ev.start_time));

    const startSec = Math.floor(new Date(ev.start_time).getTime() / 1000);
    let desc = ev.description ? ev.description + '\n\n' : '';
    desc += `🕒 **Starts:** <t:${startSec}:F>\n`;
    desc += `<t:${startSec}:R>`;
    const dur = Number(ev.duration_minutes);
    if (Number.isFinite(dur) && dur > 0) {
        const hours = Math.floor(dur / 60);
        const mins = dur % 60;
        const durLabel = hours && mins ? `${hours}h ${mins}m`
                       : hours          ? `${hours}h`
                       : `${mins}m`;
        const endSec = startSec + dur * 60;
        desc += `\n⏳ **Duration:** ${durLabel} (ends <t:${endSec}:t>)`;
    }
    if (ev.required_role_id) {
        desc += `\n\n🔒 RSVP requires <@&${ev.required_role_id}>`;
    }
    embed.setDescription(desc);

    let attendeeCount = 0;
    for (const opt of (ev.rsvp_options || [])) {
        const users = opt.users || [];
        if (opt.counts_as_rsvp !== false) attendeeCount += users.length;
        const max = Number.isFinite(opt.max_slots) && opt.max_slots > 0 ? opt.max_slots : null;
        const countLabel = max ? `${users.length} / ${max}` : `${users.length}`;
        const value = users.length ? users.map(uid => `<@${uid}>`).join('\n') : '-';
        embed.addFields({
            name: `${opt.emoji} ${opt.label} (${countLabel})`,
            value: value.slice(0, 1024),
            inline: true
        });
    }

    if (ev.image_url) embed.setImage(ev.image_url);
    embed.setFooter({ text: `Event ID: ${ev.id} • Total RSVPs: ${attendeeCount}` });
    return embed;
}

function buildEventComponents(ev) {
    const rows = [];
    let current = new ActionRowBuilder();
    let count = 0;
    (ev.rsvp_options || []).forEach((opt, idx) => {
        if (rows.length >= 5) return;
        if (count === 5) {
            rows.push(current);
            current = new ActionRowBuilder();
            count = 0;
            if (rows.length >= 5) return;
        }
        const btn = new ButtonBuilder()
            .setCustomId(`rsvp:${ev.id}:${idx}`)
            .setLabel((opt.label || 'RSVP').slice(0, 80))
            .setStyle(opt.counts_as_rsvp === false ? ButtonStyle.Danger : ButtonStyle.Secondary);
        try {
            const m = opt.emoji && opt.emoji.match(/<a?:([^:]+):(\d+)>/);
            if (m) {
                btn.setEmoji({ id: m[2], name: m[1], animated: opt.emoji.startsWith('<a:') });
            } else if (opt.emoji) {
                btn.setEmoji(opt.emoji);
            }
        } catch (e) { /* invalid emoji — skip */ }
        current.addComponents(btn);
        count++;
    });
    if (count > 0) rows.push(current);
    if (rows.length < 5) {
        rows.push(new ActionRowBuilder().addComponents(
            new ButtonBuilder()
                .setCustomId(`evnotify:${ev.id}`)
                .setLabel('Notify me 30m before')
                .setEmoji('🔔')
                .setStyle(ButtonStyle.Primary)
        ));
    }
    return rows;
}

function emojiKey(emojiStr) {
    const m = emojiStr && emojiStr.match(/<a?:[^:]+:(\d+)>/);
    return m ? m[1] : emojiStr;
}

async function refreshEventMessage(ev) {
    try {
        const guild = client.guilds.cache.get(SPACEWHLE_GUILD_ID);
        if (!guild) return;
        const channel = guild.channels.cache.get(ev.channel_id);
        if (!channel) return;
        const msg = await channel.messages.fetch(ev.message_id);
        if (!msg) return;
        await msg.edit({ embeds: [buildEventEmbed(ev)], components: buildEventComponents(ev) });
    } catch (e) {
        console.warn('refreshEventMessage failed:', e.message);
    }
}

app.post('/admin/events/create', async (req, res) => {
    const admin = await verifyAdmin(req);
    if (!admin) return res.status(403).json({ error: 'Unauthorized' });
    try {
        const {
            title, description, image_url, start_time,
            channel_id, ping_role_ids, required_role_id,
            ping_everyone, ping_here,
            rsvp_options, create_discord_event,
            duration_minutes
        } = req.body || {};

        const durMin = Number(duration_minutes);
        const safeDuration = Number.isFinite(durMin) && durMin >= 5 && durMin <= 24 * 60
            ? Math.floor(durMin)
            : 60;

        if (!title || !start_time || !channel_id) {
            return res.status(400).json({ error: 'title, start_time, channel_id required' });
        }
        if (!Array.isArray(rsvp_options) || rsvp_options.length === 0) {
            return res.status(400).json({ error: 'At least one rsvp_option required' });
        }

        const guild = client.guilds.cache.get(SPACEWHLE_GUILD_ID);
        if (!guild) return res.status(503).json({ error: 'Bot not in guild' });
        const channel = guild.channels.cache.get(channel_id);
        if (!channel || !channel.isTextBased()) return res.status(404).json({ error: 'Channel not found' });

        const eventId = crypto.randomUUID();
        const ev = {
            id: eventId,
            discord_event_id: null,
            channel_id,
            message_id: null,
            title,
            description: description || '',
            image_url: image_url || '',
            start_time: new Date(start_time).toISOString(),
            duration_minutes: safeDuration,
            end_time: new Date(new Date(start_time).getTime() + safeDuration * 60 * 1000).toISOString(),
            ping_role_ids: Array.isArray(ping_role_ids) ? ping_role_ids : [],
            ping_everyone: !!ping_everyone,
            ping_here:     !!ping_here,
            required_role_id: required_role_id || null,
            rsvp_options: rsvp_options.map(o => {
                const maxRaw = Number(o.max_slots);
                return {
                    label: o.label || 'RSVP',
                    emoji: o.emoji,
                    emoji_key: emojiKey(o.emoji),
                    required_role_id: o.required_role_id || null,
                    counts_as_rsvp: o.counts_as_rsvp !== false,
                    max_slots: Number.isFinite(maxRaw) && maxRaw > 0 ? Math.floor(maxRaw) : null,
                    users: []
                };
            }),
            ping_30m_sent: false,
            ping_start_sent: false,
            created_by: admin.userId,
            created_at: new Date().toISOString()
        };

        if (create_discord_event) {
            try {
                const startDt = new Date(ev.start_time);
                const endDt = new Date(startDt.getTime() + safeDuration * 60 * 1000);
                // Discord caps: name 100 chars, description 1000 chars.
                // Truncate so a long description (e.g. a full Stormbreaker
                // template) doesn't break the scheduled-event creation.
                const safeName = String(title || 'Operation').slice(0, 100);
                const rawDesc  = String(description || '');
                const safeDesc = rawDesc.length > 1000 ? rawDesc.slice(0, 997) + '…' : rawDesc;
                const scheduled = await guild.scheduledEvents.create({
                    name: safeName,
                    scheduledStartTime: startDt,
                    scheduledEndTime: endDt,
                    privacyLevel: 2,
                    entityType: 3,
                    description: safeDesc,
                    entityMetadata: { location: `#${channel.name}` },
                    image: image_url || undefined
                });
                ev.discord_event_id = scheduled.id;
            } catch (e) {
                console.warn('Failed to create Discord scheduled event:', e.message);
            }
        }

        const mParts = [];
        if (ev.ping_everyone) mParts.push('@everyone');
        else if (ev.ping_here) mParts.push('@here');
        for (const id of (ev.ping_role_ids || [])) mParts.push(`<@&${id}>`);
        const prefix = mParts.join(' ');
        const allowedParseEmbed = (ev.ping_everyone || ev.ping_here) ? ['everyone'] : [];
        const sent = await channel.send({
            content: prefix || '',
            embeds: [buildEventEmbed(ev)],
            components: buildEventComponents(ev),
            allowedMentions: { parse: allowedParseEmbed, roles: ev.ping_role_ids || [] }
        });
        ev.message_id = sent.id;

        try {
            const thread = await sent.startThread({
                name: ev.title.slice(0, 100),
                autoArchiveDuration: 1440
            });
            ev.thread_id = thread.id;
        } catch (e) {
            console.warn('Failed to start event thread:', e.message);
        }

        const data = loadCustomEvents();
        data.events.push(ev);
        saveCustomEvents(data);

        res.json({ ok: true, event: ev });
    } catch (err) {
        console.error('POST /admin/events/create error:', err);
        res.status(500).json({ error: err.message });
    }
});

app.get('/admin/events/list', async (req, res) => {
    const admin = await verifyAdmin(req);
    if (!admin) return res.status(403).json({ error: 'Unauthorized' });
    const data = loadCustomEvents();
    const now = Date.now();
    const future = data.events.filter(e => new Date(e.start_time).getTime() > now)
        .sort((a, b) => new Date(a.start_time) - new Date(b.start_time));
    const past = data.events.filter(e => new Date(e.start_time).getTime() <= now)
        .sort((a, b) => new Date(b.start_time) - new Date(a.start_time));
    res.json({ upcoming: future, past: past.slice(0, 20) });
});

// Edit an existing event in place. Only mutable fields are accepted; the
// event id, message id, thread id, discord_event_id, RSVP user lists, and
// timestamps are preserved. The Discord message gets refreshed (embed +
// buttons) on save, and if a Discord scheduled event was created for it,
// that gets patched too.
app.patch('/admin/events/:id', async (req, res) => {
    const admin = await verifyAdmin(req);
    if (!admin) return res.status(403).json({ error: 'Unauthorized' });
    const data = loadCustomEvents();
    const ev = data.events.find(e => e.id === req.params.id);
    if (!ev) return res.status(404).json({ error: 'Not found' });

    try {
        const b = req.body || {};
        if (typeof b.title === 'string')        ev.title = b.title.trim().slice(0, 200);
        if (typeof b.description === 'string')  ev.description = b.description;
        if (typeof b.image_url === 'string')    ev.image_url = b.image_url.trim();
        if (typeof b.start_time === 'string')   ev.start_time = new Date(b.start_time).toISOString();
        if ('required_role_id' in b)            ev.required_role_id = b.required_role_id || null;
        if (Array.isArray(b.ping_role_ids))     ev.ping_role_ids = b.ping_role_ids;
        if (typeof b.ping_everyone === 'boolean') ev.ping_everyone = b.ping_everyone;
        if (typeof b.ping_here === 'boolean')   ev.ping_here = b.ping_here;
        if (Number.isFinite(Number(b.duration_minutes))) {
            const dm = Math.floor(Number(b.duration_minutes));
            if (dm >= 5 && dm <= 24 * 60) {
                ev.duration_minutes = dm;
                ev.end_time = new Date(new Date(ev.start_time).getTime() + dm * 60 * 1000).toISOString();
            }
        }
        if (Array.isArray(b.rsvp_options)) {
            // Preserve existing users[] for options that map to a matching
            // emoji (so editing doesn't drop everyone's RSVPs).
            const oldByEmoji = new Map((ev.rsvp_options || []).map(o => [emojiKey(o.emoji), o.users || []]));
            ev.rsvp_options = b.rsvp_options.map(o => {
                const key = emojiKey(o.emoji);
                const maxRaw = Number(o.max_slots);
                return {
                    label: o.label || 'RSVP',
                    emoji: o.emoji,
                    emoji_key: key,
                    required_role_id: o.required_role_id || null,
                    counts_as_rsvp: o.counts_as_rsvp !== false,
                    max_slots: Number.isFinite(maxRaw) && maxRaw > 0 ? Math.floor(maxRaw) : null,
                    users: oldByEmoji.get(key) || []
                };
            });
        }
        saveCustomEvents(data);

        // Refresh the posted Discord message (embed + buttons).
        try { await refreshEventMessage(ev); } catch (e) { console.warn('refresh after PATCH failed:', e.message); }

        // Patch the Discord scheduled event if one was created.
        if (ev.discord_event_id) {
            try {
                const guild = client.guilds.cache.get(SPACEWHLE_GUILD_ID);
                const sched = guild ? await guild.scheduledEvents.fetch(ev.discord_event_id).catch(() => null) : null;
                if (sched) {
                    const dur = Number(ev.duration_minutes) || 60;
                    const startDt = new Date(ev.start_time);
                    const endDt = new Date(startDt.getTime() + dur * 60 * 1000);
                    const safeDesc = String(ev.description || '').slice(0, 1000);
                    await sched.edit({
                        name: String(ev.title || 'Operation').slice(0, 100),
                        description: safeDesc,
                        scheduledStartTime: startDt,
                        scheduledEndTime: endDt,
                        image: ev.image_url || undefined
                    });
                }
            } catch (e) {
                console.warn('Discord scheduled-event update failed:', e.message);
            }
        }

        res.json({ ok: true, event: ev });
    } catch (err) {
        console.error('PATCH /admin/events/:id error:', err);
        res.status(500).json({ error: err.message });
    }
});

app.delete('/admin/events/:id', async (req, res) => {
    const admin = await verifyAdmin(req);
    if (!admin) return res.status(403).json({ error: 'Unauthorized' });
    const data = loadCustomEvents();
    const ev = data.events.find(e => e.id === req.params.id);
    if (!ev) return res.status(404).json({ error: 'Not found' });

    try {
        const guild = client.guilds.cache.get(SPACEWHLE_GUILD_ID);
        const channel = guild?.channels.cache.get(ev.channel_id);
        if (ev.thread_id && guild) {
            const thread = await guild.channels.fetch(ev.thread_id).catch(() => null);
            if (thread) await thread.delete().catch(() => {});
        }
        if (channel && ev.message_id) {
            const msg = await channel.messages.fetch(ev.message_id).catch(() => null);
            if (msg) await msg.delete().catch(() => {});
        }
        if (ev.discord_event_id && guild) {
            await guild.scheduledEvents.delete(ev.discord_event_id).catch(() => {});
        }
    } catch (e) { console.warn('Cleanup error on delete:', e.message); }

    data.events = data.events.filter(e => e.id !== req.params.id);
    saveCustomEvents(data);
    res.json({ ok: true });
});

// ==============================================================================
// === EVENT ATTENDANCE — admin review + confirm ===
// ==============================================================================

// List recent / live events that have attendance data or have already ended,
// each annotated with a participation summary. Powers the popup's event picker.
app.get('/admin/events/attendance-pending', async (req, res) => {
    res.header('Access-Control-Allow-Origin', '*');
    const admin = await verifyAdmin(req);
    if (!admin) return res.status(403).json({ error: 'Unauthorized' });
    try {
        // Flush any in-flight presence so live events read fresh.
        try { _accrueAttendance(); } catch {}
        const evData = loadCustomEvents();
        const att = loadAttendance();
        const now = Date.now();
        const out = [];
        for (const ev of (evData.events || [])) {
            if (!ev || !ev.id || !ev.start_time) continue;
            const { s, e } = _eventWindowMs(ev);
            const erec = att.events[ev.id];
            const recordedCount = erec ? Object.keys(erec.members || {}).length : 0;
            const started = now >= s;
            const ended = now >= e;
            // Surface anything that's started (live or ended). Skip future events.
            if (!started) continue;
            const durSec = Math.max(1, Math.floor((e - s) / 1000));
            let participated = 0;
            if (erec) {
                for (const m of Object.values(erec.members || {})) {
                    if (Math.min(m.seconds || 0, durSec) / durSec >= ATTENDANCE_THRESHOLD) participated++;
                }
            }
            out.push({
                id: ev.id,
                title: ev.title,
                start_time: ev.start_time,
                end_time: new Date(e).toISOString(),
                duration_minutes: Number(ev.duration_minutes) || Math.round(durSec / 60),
                ended,
                live: started && !ended,
                recorded_count: recordedCount,
                participated_count: participated,
                points_applied: !!(erec && erec.points_applied),
                applied_points: erec ? (erec.applied_points || 0) : 0,
                applied_at: erec ? (erec.applied_at || null) : null
            });
        }
        out.sort((a, b) => new Date(b.start_time) - new Date(a.start_time));
        res.json({ events: out.slice(0, 30), threshold: ATTENDANCE_THRESHOLD });
    } catch (err) {
        console.error('GET /admin/events/attendance-pending error:', err);
        res.status(500).json({ error: err.message });
    }
});

// Per-member attendance breakdown for one event, with roster-match resolution
// so the UI can show who will actually receive points.
app.get('/admin/events/:id/attendance', async (req, res) => {
    res.header('Access-Control-Allow-Origin', '*');
    const admin = await verifyAdmin(req);
    if (!admin) return res.status(403).json({ error: 'Unauthorized' });
    try {
        try { _accrueAttendance(); } catch {}
        const evData = loadCustomEvents();
        const ev = (evData.events || []).find(e => e.id === req.params.id);
        const att = loadAttendance();
        const erec = att.events[req.params.id];
        // Fall back to stored attendance metadata if the event was deleted.
        const meta = ev || (erec ? {
            id: req.params.id, title: erec.title, start_time: erec.start_time,
            end_time: erec.end_time, duration_minutes: erec.duration_minutes
        } : null);
        if (!meta) return res.status(404).json({ error: 'Event not found' });
        const { s, e } = _eventWindowMs(meta);
        const durSec = Math.max(1, Math.floor((e - s) / 1000));

        // Resolve roster matches (Discord-ID first, then username).
        const rosterById = new Map(), rosterByName = new Map();
        try {
            const rRes = await fetch('https://api.spacewhle.org/api/admin/roster', {
                headers: { Authorization: req.headers.authorization || '' }
            });
            if (rRes.ok) {
                const rJson = await rRes.json();
                for (const m of (rJson.members || [])) {
                    if (m.discord_id) rosterById.set(String(m.discord_id), m);
                    if (m.discord) rosterByName.set(String(m.discord).toLowerCase(), m);
                }
            }
        } catch (e2) { console.warn('attendance roster fetch failed:', e2.message); }

        const guild = client.guilds.cache.get(SPACEWHLE_GUILD_ID);
        const members = [];
        const recMembers = (erec && erec.members) ? erec.members : {};
        for (const [userId, m] of Object.entries(recMembers)) {
            const seconds = Math.min(m.seconds || 0, durSec);
            const pct = seconds / durSec;
            let username = m.username || '';
            let display_name = m.display_name || username;
            const gm = guild?.members.cache.get(userId);
            if (gm) { username = gm.user.username; display_name = gm.displayName || gm.user.globalName || username; }
            const rosterRow = rosterById.get(userId) || (username ? rosterByName.get(username.toLowerCase()) : null);
            members.push({
                user_id: userId,
                username,
                display_name,
                seconds,
                minutes: Math.round(seconds / 60),
                pct: Math.round(pct * 1000) / 10,
                participated: pct >= ATTENDANCE_THRESHOLD,
                roster_match: rosterRow ? (rosterRow.discord || null) : null
            });
        }
        members.sort((a, b) => b.seconds - a.seconds);
        res.json({
            event: {
                id: meta.id || req.params.id,
                title: meta.title,
                start_time: meta.start_time,
                end_time: new Date(e).toISOString(),
                duration_minutes: Number(meta.duration_minutes) || Math.round(durSec / 60),
                duration_seconds: durSec
            },
            threshold: ATTENDANCE_THRESHOLD,
            threshold_pct: Math.round(ATTENDANCE_THRESHOLD * 100),
            points_applied: !!(erec && erec.points_applied),
            applied_points: erec ? (erec.applied_points || 0) : 0,
            applied_at: erec ? (erec.applied_at || null) : null,
            members
        });
    } catch (err) {
        console.error('GET /admin/events/:id/attendance error:', err);
        res.status(500).json({ error: err.message });
    }
});

// Confirm participation → award event points to each qualifying participant on
// the roster (minus any manually excluded via the X button). Idempotent: once
// applied, re-confirming is refused.
app.post('/admin/events/:id/confirm-attendance', async (req, res) => {
    res.header('Access-Control-Allow-Origin', '*');
    const admin = await verifyAdmin(req);
    if (!admin) return res.status(403).json({ error: 'Unauthorized' });
    try {
        const eventId = req.params.id;
        const att = loadAttendance();
        const erec = att.events[eventId];
        if (!erec) return res.status(404).json({ error: 'No attendance recorded for this event' });
        if (erec.points_applied) {
            return res.status(409).json({ error: 'Points already applied for this event', already_applied: true, applied_at: erec.applied_at });
        }
        const exclude = new Set((Array.isArray(req.body?.exclude) ? req.body.exclude : []).map(String));
        // Manual override: force-count these members even if they fell below
        // the attendance threshold.
        const include = new Set((Array.isArray(req.body?.include) ? req.body.include : []).map(String));
        let points = Number(req.body?.points);
        if (!Number.isFinite(points)) points = 1;
        points = Math.max(0, Math.min(50, Math.floor(points)));

        const { s, e } = _eventWindowMs({ start_time: erec.start_time, end_time: erec.end_time, duration_minutes: erec.duration_minutes });
        // Optional actual finish time → recompute the attendance window so % is
        // judged against how long the operation really ran, not the scheduled
        // block. Ignored if it isn't a valid instant after the start.
        let effEnd = e;
        if (req.body?.end) {
            const t = new Date(req.body.end).getTime();
            if (Number.isFinite(t) && t > s) effEnd = t;
        }
        const durSec = Math.max(1, Math.floor((effEnd - s) / 1000));

        const participants = [];
        for (const [userId, m] of Object.entries(erec.members || {})) {
            const uid = String(userId);
            if (exclude.has(uid)) continue;
            const pct = Math.min(m.seconds || 0, durSec) / durSec;
            const qualifies = pct >= ATTENDANCE_THRESHOLD || include.has(uid);
            if (qualifies) participants.push({ userId, username: m.username || '', display_name: m.display_name || '' });
        }

        // Pull roster once so we can preserve each row's other fields and match
        // by Discord-ID first, username second.
        const rosterById = new Map(), rosterByName = new Map();
        try {
            const rRes = await fetch('https://api.spacewhle.org/api/admin/roster', {
                headers: { Authorization: req.headers.authorization || '' }
            });
            if (!rRes.ok) return res.status(502).json({ error: 'Roster fetch failed', status: rRes.status });
            const rJson = await rRes.json();
            for (const m of (rJson.members || [])) {
                if (m.discord_id) rosterById.set(String(m.discord_id), m);
                if (m.discord) rosterByName.set(String(m.discord).toLowerCase(), m);
            }
        } catch (e2) {
            return res.status(502).json({ error: 'Roster fetch error: ' + e2.message });
        }

        const guild = client.guilds.cache.get(SPACEWHLE_GUILD_ID);
        const applied = [], skipped = [];
        for (const p of participants) {
            let username = p.username;
            const gm = guild?.members.cache.get(p.userId);
            if (gm) username = gm.user.username;
            const row = rosterById.get(p.userId) || (username ? rosterByName.get(username.toLowerCase()) : null);
            if (!row) {
                skipped.push({ user_id: p.userId, username: username || p.display_name, reason: 'not on roster' });
                continue;
            }
            if (points > 0) {
                try {
                    const upd = await fetch('https://api.spacewhle.org/api/admin/update-member', {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json', Authorization: req.headers.authorization || '' },
                        body: JSON.stringify({
                            discord_name:        row.discord,
                            rsi_handle:          row.rsi_handle || '',
                            rank:                row.rank || 'SpW',
                            division_operations: row.div_ops || '',
                            division_logistics:  row.div_log || '',
                            division_medical:    row.div_med || '',
                            division_academic:   row.div_aca || '',
                            points_attendance:   points,
                            points_soma:         0,
                            points_orders:       0
                        })
                    });
                    if (!upd.ok) { skipped.push({ user_id: p.userId, username, reason: 'update failed (' + upd.status + ')' }); continue; }
                } catch (e3) {
                    skipped.push({ user_id: p.userId, username, reason: 'update error' });
                    continue;
                }
            }
            applied.push({ user_id: p.userId, username, discord_name: row.discord, points });
        }

        erec.points_applied = true;
        erec.applied_at = new Date().toISOString();
        erec.applied_points = points;
        erec.applied_by = admin.username || admin.userId;
        erec.applied_end = new Date(effEnd).toISOString();
        if (include.size) erec.applied_overrides = [...include];
        saveAttendance(att);

        res.json({ ok: true, event_id: eventId, points, applied, skipped, participant_count: participants.length, finish_time: new Date(effEnd).toISOString() });
    } catch (err) {
        console.error('POST /admin/events/:id/confirm-attendance error:', err);
        res.status(500).json({ error: err.message });
    }
});

// Manual trigger for the silent Discord-ID → roster attach scan (also runs
// automatically on startup + every 6h).
app.post('/admin/roster/attach-discord-ids', async (req, res) => {
    res.header('Access-Control-Allow-Origin', '*');
    const admin = await verifyAdmin(req);
    if (!admin) return res.status(403).json({ error: 'Unauthorized' });
    const r = await attachDiscordIdsToRoster();
    res.json(r);
});

// ==============================================================================
// === BUTTON HANDLER for custom event RSVPs ===
// ==============================================================================
// In-memory locks keyed on (eventId|userId). A double-click on a button
// can otherwise fire two parallel runs of the handler, both loading the
// same JSON snapshot, both modifying it, both saving — the second write
// clobbers the first. The lock serialises clicks per-user-per-event.
const _rsvpLocks = new Map(); // key → Promise tail

async function handleRsvpButton(interaction, eventId, idxStr) {
    const userId = interaction.user.id;
    const lockKey = `${eventId}|${userId}`;

    // Acknowledge the click within 3s; we can edit the message after.
    if (!interaction.replied && !interaction.deferred) {
        try { await interaction.deferUpdate(); } catch { /* already acked */ }
    }

    // Serialise: queue this run after any previous in-flight click from
    // the same user on the same event.
    const tail = _rsvpLocks.get(lockKey) || Promise.resolve();
    const run = tail.then(() => _processRsvpClick(interaction, eventId, idxStr)).catch(e => {
        console.warn('RSVP click failed:', e.message);
    });
    _rsvpLocks.set(lockKey, run.then(() => {
        // Clear the lock when this run is the last one queued.
        if (_rsvpLocks.get(lockKey) === run) _rsvpLocks.delete(lockKey);
    }));
    await run;
}

async function _processRsvpClick(interaction, eventId, idxStr) {
    const idx = parseInt(idxStr, 10);
    const data = loadCustomEvents();
    const ev = data.events.find(e => e.id === eventId);
    if (!ev) {
        return interaction.followUp({ content: '❌ This event no longer exists.', ephemeral: true }).catch(() => {});
    }
    const opt = (ev.rsvp_options || [])[idx];
    if (!opt) {
        return interaction.followUp({ content: '❌ That RSVP option no longer exists.', ephemeral: true }).catch(() => {});
    }

    const userId = interaction.user.id;
    let memberRoles = new Set();
    try {
        const guild = client.guilds.cache.get(SPACEWHLE_GUILD_ID);
        const member = await guild.members.fetch(userId);
        memberRoles = new Set(member.roles.cache.map(r => r.id));
    } catch (e) { /* user not in guild */ }

    if (ev.required_role_id && !memberRoles.has(ev.required_role_id)) {
        return interaction.followUp({ content: `❌ This event requires the <@&${ev.required_role_id}> role.`, ephemeral: true, allowedMentions: { parse: [] } }).catch(() => {});
    }
    if (opt.required_role_id && !memberRoles.has(opt.required_role_id)) {
        return interaction.followUp({ content: `❌ This option requires the <@&${opt.required_role_id}> role.`, ephemeral: true, allowedMentions: { parse: [] } }).catch(() => {});
    }

    const wasIn = (opt.users || []).includes(userId);
    let action;
    if (wasIn) {
        opt.users = opt.users.filter(uid => uid !== userId);
        action = 'removed';
    } else {
        const max = Number.isFinite(opt.max_slots) && opt.max_slots > 0 ? opt.max_slots : null;
        if (max && (opt.users || []).length >= max) {
            return interaction.followUp({ content: `❌ **${opt.label}** is full (${max} slots).`, ephemeral: true }).catch(() => {});
        }
        for (const other of ev.rsvp_options) {
            if (other !== opt) {
                other.users = (other.users || []).filter(uid => uid !== userId);
            }
        }
        if (!opt.users) opt.users = [];
        opt.users.push(userId);
        action = 'added';
    }

    saveCustomEvents(data);

    // Already deferred (deferUpdate) by handleRsvpButton; edit the message
    // in place. interaction.update isn't valid post-defer.
    try {
        await interaction.editReply({ embeds: [buildEventEmbed(ev)], components: buildEventComponents(ev) });
    } catch (e) {
        console.warn('RSVP editReply failed; falling back to message edit:', e.message);
        refreshEventMessage(ev);
    }

    // Only DM about "you have RSVPed" when the option actually counts as
    // an RSVP. Two safety nets: the explicit counts_as_rsvp flag (new
    // events) AND a label sniff (Unavailable / Declined / Not going / etc.)
    // for any older event where the flag wasn't set. If either check says
    // "this is a non-RSVP option", suppress the DM.
    const NON_RSVP_LABEL_RE = /^(unavailable|declin|not\s*going|can'?t\s*(make|go|attend)|skip|absent|no\b)/i;
    const isNonRsvpPick = opt.counts_as_rsvp === false || NON_RSVP_LABEL_RE.test(opt.label || '');
    if (!isNonRsvpPick) {
        try {
            const startSec = Math.floor(new Date(ev.start_time).getTime() / 1000);
            const dmText = action === 'added'
                ? `✅ You've RSVPed to **${ev.title}** as **${opt.label}**.\n🕒 Starts <t:${startSec}:F> (<t:${startSec}:R>)`
                : `❌ Your RSVP for **${ev.title}** (**${opt.label}**) has been removed.`;
            await interaction.user.send(dmText);
        } catch (e) {
            // User has DMs disabled — silent
        }
    }

    // Auto-add the RSVPing user to the event thread so they get briefing
    // updates. Only on add — never on remove — AND only when the chosen
    // option actually counts as an RSVP. Picking "Declined" / "Maybe with
    // counts_as_rsvp:false" no longer drags the user into the thread.
    if (action === 'added' && ev.thread_id && opt.counts_as_rsvp !== false) {
        try {
            const guild = client.guilds.cache.get(SPACEWHLE_GUILD_ID);
            const thread = guild ? await guild.channels.fetch(ev.thread_id).catch(() => null) : null;
            if (thread && thread.isThread && thread.isThread() && typeof thread.members?.add === 'function') {
                await thread.members.add(userId);
            }
        } catch (e) {
            console.warn('Failed to add RSVPer to event thread:', e.message);
        }
    }
}

// ==============================================================================
// === EVENT PING SCHEDULER (runs every 60s) ===
// ==============================================================================
setInterval(async () => {
    try {
        const data = loadCustomEvents();
        const now = Date.now();
        let dirty = false;
        for (const ev of data.events) {
            const startMs = new Date(ev.start_time).getTime();
            const guild = client.guilds.cache.get(SPACEWHLE_GUILD_ID);
            if (!guild) continue;
            const channel = guild.channels.cache.get(ev.channel_id);
            if (!channel) continue;

            const rsvpedUserIds = [...new Set(
                (ev.rsvp_options || [])
                    .filter(o => o.counts_as_rsvp !== false)
                    .flatMap(o => o.users || [])
            )];

            // Reminders fire INSIDE the event thread, not the main channel.
            // Use @here — Discord scopes that to "thread members currently
            // online" in a thread context, so we don't ping everyone with
            // the SPACEWHLE role and we don't paste a list of usernames.
            // DMs go out in parallel as a backup for offline / muted thread
            // members so nobody actually misses the reminder.
            const thread = ev.thread_id
                ? await guild.channels.fetch(ev.thread_id).catch(() => null)
                : null;
            const reminderTarget = (thread && thread.isThread && thread.isThread()) ? thread : null;

            if (!ev.ping_30m_sent && startMs - now <= 30 * 60 * 1000 && startMs - now > 0) {
                if (reminderTarget) {
                    await reminderTarget.send({
                        content: `@here ⏰ **${ev.title}** starts in 30 minutes! <t:${Math.floor(startMs/1000)}:R>`,
                        allowedMentions: { parse: ['everyone'] }
                    }).catch(e => console.warn('30m thread ping failed:', e.message));
                }

                // DM only members who explicitly opted in via the 🔔 Notify
                // button on the event message. RSVPed users no longer get
                // an automatic DM — reminders are opt-in by design.
                const dmTargets = Array.isArray(ev.notify_users) ? ev.notify_users : [];
                for (const uid of dmTargets) {
                    try {
                        const u = await client.users.fetch(uid);
                        await u.send(`⏰ **${ev.title}** starts in 30 minutes! <t:${Math.floor(startMs/1000)}:R>`);
                    } catch { /* DMs disabled */ }
                }
                ev.ping_30m_sent = true;
                dirty = true;
            }
            if (!ev.ping_start_sent && startMs <= now) {
                if (reminderTarget) {
                    await reminderTarget.send({
                        content: `@here 🚀 **${ev.title}** is starting NOW!`,
                        allowedMentions: { parse: ['everyone'] }
                    }).catch(e => console.warn('start thread ping failed:', e.message));
                }

                const dmTargets = Array.isArray(ev.notify_users) ? ev.notify_users : [];
                for (const uid of dmTargets) {
                    try {
                        const u = await client.users.fetch(uid);
                        await u.send(`🚀 **${ev.title}** is starting NOW!`);
                    } catch { /* DMs disabled */ }
                }
                ev.ping_start_sent = true;
                dirty = true;
            }
        }
        if (dirty) saveCustomEvents(data);
    } catch (e) {
        console.error('Event scheduler tick error:', e);
    }
}, 60 * 1000);


// ==============================================================================
// === PUBLIC API: SOMA ROLES (id + name list, used for medal detection) ===
// ==============================================================================
app.get('/soma-roles', (req, res) => {
    res.header('Access-Control-Allow-Origin', '*');
    try {
        const guild = client.guilds.cache.get(SPACEWHLE_GUILD_ID);
        if (!guild) return res.status(503).json({ error: 'Bot not in guild' });
        const somaRoles = [...guild.roles.cache.values()]
            .filter(r => /soma/i.test(r.name))
            .map(r => ({ id: r.id, name: r.name }));
        res.json({ roles: somaRoles });
    } catch (err) {
        console.error('GET /soma-roles error:', err);
        res.status(500).json({ error: 'Failed to load roles' });
    }
});

// ==============================================================================
// === PUBLIC API: UPCOMING CUSTOM EVENTS (with Discord deep links) ===
// ==============================================================================
// Used by the members-area Upcoming Operations card. Returns just enough to
// render a card and a deep link back to the bot's event message in Discord.
// Logistics orders proxy: members-area calls this so the dashboard can show
// a user's order history without depending on Supabase RLS (which currently
// blocks anon SELECT). Uses the service role key when configured, otherwise
// falls back to anon. Only returns the orders for the requested discordId.
app.get('/user/logistics-orders/:discordId', async (req, res) => {
    res.header('Access-Control-Allow-Origin', '*');
    try {
        const discordId = String(req.params.discordId || '').trim();
        if (!/^\d{10,25}$/.test(discordId)) {
            return res.status(400).json({ error: 'invalid discordId' });
        }
        const limit = Math.min(parseInt(req.query.limit, 10) || 50, 200);

        // 1. Local mirror — always available, captured from the insert webhook.
        const local = loadLocalLogisticsOrders().orders.filter(
            o => String(o.user_discord_id) === discordId
        );

        // 2. If a service-role key is configured, also read straight from
        //    Supabase (bypasses RLS, includes any pre-existing rows). Anon key
        //    is intentionally NOT used here — RLS returns nothing for it.
        let remote = [];
        const serviceKey = process.env.SUPABASE_SERVICE_ROLE_KEY || '';
        if (serviceKey) {
            try {
                const url = `${SUPABASE_URL}/rest/v1/logistics_orders` +
                    `?user_discord_id=eq.${encodeURIComponent(discordId)}` +
                    `&order=created_at.desc&limit=${limit}` +
                    `&select=id,item,quantity,location,created_at`;
                const r = await fetch(url, {
                    headers: { apikey: serviceKey, Authorization: `Bearer ${serviceKey}` }
                });
                if (r.ok) remote = await r.json();
            } catch (e) { /* fall back to the local mirror only */ }
        }

        // 3. Merge + dedupe (stable id when present, else item|location|time).
        const seen = new Set();
        const merged = [];
        for (const o of [...remote, ...local]) {
            const k = (o.id !== null && o.id !== undefined)
                ? `id:${o.id}`
                : `k:${o.item}|${o.location}|${o.created_at}`;
            if (seen.has(k)) continue;
            seen.add(k);
            merged.push(o);
        }
        merged.sort((a, b) => new Date(b.created_at || 0) - new Date(a.created_at || 0));

        res.json({ orders: merged.slice(0, limit) });
    } catch (err) {
        console.error('GET /user/logistics-orders error:', err);
        res.status(500).json({ error: err.message });
    }
});

// Return the RSVPs (both upcoming and past) for a given user, pulled from
// custom-events.json. Used by members-area's Operations Log and Upcoming
// Operations cards so we don't have to keep a mirror in Supabase.
app.get('/user/rsvps/:discordId', async (req, res) => {
    res.header('Access-Control-Allow-Origin', '*');
    try {
        const discordId = String(req.params.discordId || '').trim();
        if (!discordId) return res.status(400).json({ error: 'discordId required' });
        const data = loadCustomEvents();
        const out = [];
        for (const e of (data.events || [])) {
            const matchingOpts = (e.rsvp_options || []).filter(o => (o.users || []).includes(discordId));
            if (!matchingOpts.length) continue;
            const opt = matchingOpts[0]; // single-choice — there's only ever one
            out.push({
                event_id:       e.id,
                title:          e.title,
                description:    e.description || '',
                start_time:     e.start_time,
                end_time:       e.end_time || null,
                channel_id:     e.channel_id,
                message_id:     e.message_id,
                thread_id:      e.thread_id || null,
                message_link:   (e.channel_id && e.message_id)
                    ? `https://discord.com/channels/${SPACEWHLE_GUILD_ID}/${e.channel_id}/${e.message_id}`
                    : null,
                rsvp_label:     opt.label,
                rsvp_emoji:     opt.emoji,
                counts_as_rsvp: opt.counts_as_rsvp !== false
            });
        }
        // Newest first.
        out.sort((a, b) => new Date(b.start_time) - new Date(a.start_time));
        res.json({ rsvps: out });
    } catch (err) {
        console.error('GET /user/rsvps error:', err);
        res.status(500).json({ error: 'Failed to fetch user RSVPs' });
    }
});

app.get('/events/upcoming', async (req, res) => {
    res.header('Access-Control-Allow-Origin', '*');
    res.header('Cache-Control', 'private, no-store');
    try {
        const data = loadCustomEvents();
        const now = Date.now();

        // Channel-visibility gate: members should only see events posted in
        // Discord channels they can actually view. Mirrors the ViewChannel
        // filter used by /admin/guild-info. The members-area passes the
        // logged-in user's ?discordId; we resolve their guild member and keep
        // only events whose channel they can see. When we can't resolve a
        // viewer (no discordId, bot not in guild) we don't filter, so the card
        // never breaks — the members-area always supplies a discordId.
        const requesterId = String(req.query.discordId || '').trim();
        const guild = client.guilds.cache.get(SPACEWHLE_GUILD_ID);
        let viewer = null;      // GuildMember, or @everyone Role
        let filtering = false;
        if (guild && requesterId && /^\d{10,25}$/.test(requesterId)) {
            // Member in guild → filter by their perms. Not found (left the
            // guild / transient) → fall back to what @everyone can see.
            const member = await guild.members.fetch(requesterId).catch(() => null);
            viewer = member || guild.roles.everyone;
            filtering = true;
        }
        const canSee = (channelId) => {
            if (!filtering) return true;   // no viewer resolved → don't filter
            if (!channelId) return true;   // event not tied to a channel
            const ch = guild.channels.cache.get(channelId);
            if (!ch) return false;         // channel gone/uncached → can't confirm
            const perms = ch.permissionsFor(viewer);
            return !!(perms && perms.has(PermissionFlagsBits.ViewChannel));
        };

        const upcoming = (data.events || [])
            .filter(e => new Date(e.start_time).getTime() > now)
            .sort((a, b) => new Date(a.start_time) - new Date(b.start_time))
            .filter(e => canSee(e.channel_id))
            .slice(0, 8)
            .map(e => ({
                id: e.id,
                title: e.title,
                description: e.description || '',
                image_url: e.image_url || null,
                start_time: e.start_time,
                channel_id: e.channel_id,
                message_id: e.message_id,
                thread_id: e.thread_id || null,
                message_link: (e.channel_id && e.message_id)
                    ? `https://discord.com/channels/${SPACEWHLE_GUILD_ID}/${e.channel_id}/${e.message_id}`
                    : null,
                rsvp_summary: (e.rsvp_options || []).map(o => ({
                    label: o.label,
                    emoji: o.emoji,
                    count: (o.users || []).length,
                    max_slots: o.max_slots || null,
                    counts_as_rsvp: o.counts_as_rsvp !== false
                }))
            }));
        res.json({ events: upcoming });
    } catch (err) {
        console.error('GET /events/upcoming error:', err);
        res.status(500).json({ error: 'Failed to fetch upcoming events' });
    }
});

// ==============================================================================
// === ADMIN: PROMOTE ANNOUNCEMENT (web-triggered) ===
// ==============================================================================
// Called by the admin website's Promote button. Looks up the target by
// username, optionally swaps their rank role, and posts the standard
// promotion announcement in PROMOTIONS_CHANNEL_ID.
app.post('/admin/promote-announce', async (req, res) => {
    const admin = await verifyAdmin(req);
    if (!admin) return res.status(403).json({ error: 'Unauthorized' });

    try {
        const { discord_name, rank_name, rank_role_id, previous_rank_role_id, custom_message, update_roles } = req.body || {};
        if (!discord_name || (!rank_name && !rank_role_id)) {
            return res.status(400).json({ error: 'discord_name and rank_name (or rank_role_id) required' });
        }

        const guild = client.guilds.cache.get(SPACEWHLE_GUILD_ID);
        if (!guild) return res.status(503).json({ error: 'Bot not in guild' });

        // Resolve rank role
        let rankRole = null;
        if (rank_role_id) {
            rankRole = guild.roles.cache.get(rank_role_id);
        }
        if (!rankRole && rank_name) {
            // SPACEWHLE rank abbreviation → full Discord role name. Lets the
            // admin button POST "Lt" or "Lieutenant" interchangeably.
            const ABBR_TO_FULL = {
                spw: 'SPACEWHLE',         op: 'Operator',
                lcpl: 'Lance Corporal',   cpl: 'Corporal',
                sgt: 'Sergeant',          ssgt: 'Staff Sergeant',
                msg: 'Master Sergeant',   sm: 'Sergeant Major',
                ocdt: 'Officer Cadet',    '2lt': 'Second Lieutenant',
                lt: 'Lieutenant',         wgcdr: 'Wing Commander',
                capt: 'Captain',          maj: 'Major',
                ltcol: 'Lieutenant Colonel', col: 'Colonel',
                brig: 'Brigadier',        ltgen: 'Lieutenant General',
                gen: 'General',           fm: 'Field Marshal'
            };
            const norm = String(rank_name).toLowerCase().replace(/[^a-z0-9]/g, '');
            const candidates = [norm];
            if (ABBR_TO_FULL[norm]) candidates.push(
                ABBR_TO_FULL[norm].toLowerCase().replace(/[^a-z0-9]/g, '')
            );
            rankRole = guild.roles.cache.find(r => {
                const rn = r.name.toLowerCase().replace(/[^a-z0-9]/g, '');
                return candidates.some(c => rn === c);
            });
            // Last resort: case-insensitive substring match — pick the
            // shortest matching role name to avoid ambiguity.
            if (!rankRole) {
                const subMatches = guild.roles.cache.filter(r =>
                    r.name.toLowerCase().replace(/[^a-z0-9]/g, '').includes(norm)
                );
                rankRole = [...subMatches.values()].sort((a, b) => a.name.length - b.name.length)[0];
            }
        }
        if (!rankRole) return res.status(404).json({ error: `Rank role not found for "${rank_name}"` });

        // Resolve target member by username (try cache, then full member list)
        const wantedName = String(discord_name).toLowerCase();
        let targetMember = guild.members.cache.find(m =>
            (m.user.username || '').toLowerCase() === wantedName ||
            (m.user.globalName || '').toLowerCase() === wantedName ||
            (m.displayName || '').toLowerCase() === wantedName
        );
        if (!targetMember) {
            await guild.members.fetch().catch(() => {});
            targetMember = guild.members.cache.find(m =>
                (m.user.username || '').toLowerCase() === wantedName ||
                (m.user.globalName || '').toLowerCase() === wantedName ||
                (m.displayName || '').toLowerCase() === wantedName
            );
        }
        if (!targetMember) return res.status(404).json({ error: 'Target member not found in Discord guild' });

        // Update roles (default: yes). Add the new rank role, then strip every
        // OTHER SPACEWHLE rank-ladder role the member still holds so a promotion
        // (e.g. LCpl -> Cpl) cleanly REMOVES the previous rank instead of leaving
        // both stacked. The base SPACEWHLE membership role (SpW) is deliberately
        // NOT in this set, so a promotion never strips someone's org membership.
        const RANK_LADDER_ROLE_IDS = new Set([
            '1308509681613934633', // Op    - Operator
            '1366867228376694824', // LCpl  - Lance Corporal
            '1366867077155262524', // Cpl   - Corporal
            '1366866972159246346', // Sgt   - Sergeant
            '1366866853724553357', // SSgt  - Staff Sergeant
            '1388620399398621194', // MSG   - Master Sergeant
            '1386802432134090783', // SM    - Sergeant Major
            '1429466916157919394', // OCdt  - Officer Cadet
            '1366866733205295214', // 2Lt   - Second Lieutenant
            '1366866565898698915', // Lt    - Lieutenant
            '1388620682145042593', // WgCdr - Wing Commander
            '1308509585874747495', // Capt  - Captain
            '1308509266055008378', // Maj   - Major
            '1366876915083776060', // LtCol - Lieutenant Colonel
            '1308509088807780382', // Col   - Colonel
            '1308508914668535839', // Brig  - Brigadier
            '1354881826178469898', // LtGen - Lieutenant General
            '1308706708683493397', // Gen   - General
            '1308508590809813013', // FM    - Field Marshal
        ]);
        if (update_roles !== false) {
            try {
                await targetMember.roles.add(rankRole, `Promotion announce by ${admin.userId}`);

                // Strip any OTHER ladder rank the member currently has — this is
                // the "remove the previous rank" behaviour. Also honour an explicit
                // previous_rank_role_id if one was passed (backward compatibility).
                const toRemove = new Set();
                for (const id of RANK_LADDER_ROLE_IDS) {
                    if (id !== rankRole.id && targetMember.roles.cache.has(id)) toRemove.add(id);
                }
                if (previous_rank_role_id && previous_rank_role_id !== rankRole.id &&
                    targetMember.roles.cache.has(previous_rank_role_id)) {
                    toRemove.add(previous_rank_role_id);
                }
                for (const id of toRemove) {
                    const prevRole = guild.roles.cache.get(id);
                    if (prevRole) {
                        await targetMember.roles.remove(prevRole, `Promotion announce by ${admin.userId}`).catch(() => {});
                    }
                }
            } catch (e) {
                console.warn('Promotion role update failed:', e.message);
            }
        }

        // Resolve announcement channel
        const channelId = PROMOTIONS_CHANNEL_ID || '1308529431907663872';
        const channel = await guild.channels.fetch(channelId).catch(() => null);
        if (!channel || !channel.isTextBased()) {
            return res.status(404).json({ error: 'Promotions channel not found / not text' });
        }

        const announcement = buildPromotionAnnouncement(targetMember, rankRole, [rankRole], custom_message);
        await channel.send({
            content: announcement,
            allowedMentions: {
                users: [targetMember.id],
                roles: [rankRole.id],
                repliedUser: false
            }
        });

        // Mirror the new rank onto the roster table so the website's
        // Roster Management view reflects the promotion immediately.
        // Best-effort — log a warning if it fails so the announcement
        // path doesn't break. Fetches the current roster row first so we
        // can preserve RSI / divisions (Django update-member overwrites
        // any field it gets, doesn't skip blanks).
        try {
            const NAME_TO_ABBR = {
                'spacewhle': 'SpW', 'operator': 'Op',
                'lance corporal': 'LCpl', 'corporal': 'Cpl',
                'sergeant': 'Sgt', 'staff sergeant': 'SSgt',
                'master sergeant': 'MSG', 'sergeant major': 'SM',
                'officer cadet': 'OCdt', 'second lieutenant': '2Lt',
                'lieutenant': 'Lt', 'wing commander': 'WgCdr',
                'captain': 'Capt', 'major': 'Maj',
                'lieutenant colonel': 'LtCol', 'colonel': 'Col',
                'brigadier': 'Brig', 'lieutenant general': 'LtGen',
                'general': 'Gen', 'field marshal': 'FM'
            };
            const rosterAbbr = NAME_TO_ABBR[(rankRole.name || '').toLowerCase()]
                || (rank_name && NAME_TO_ABBR[String(rank_name).toLowerCase()])
                || rank_name
                || rankRole.name;
            // Pull current roster to find the existing row.
            const rosterRes = await fetch('https://api.spacewhle.org/api/admin/roster', {
                headers: { Authorization: req.headers.authorization }
            });
            const rosterJson = rosterRes.ok ? await rosterRes.json() : { members: [] };
            const wanted = String(discord_name).toLowerCase();
            const existing = (rosterJson.members || []).find(m =>
                (m.discord || '').toLowerCase() === wanted
            ) || {};
            await fetch('https://api.spacewhle.org/api/admin/update-member', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                    Authorization: req.headers.authorization
                },
                body: JSON.stringify({
                    discord_name: existing.discord || discord_name,
                    rank: rosterAbbr,
                    rsi_handle:           existing.rsi_handle || '',
                    division_operations:  existing.div_ops    || '',
                    division_logistics:   existing.div_log    || '',
                    division_medical:     existing.div_med    || '',
                    division_academic:    existing.div_aca    || '',
                    points_attendance: 0, points_soma: 0, points_orders: 0
                })
            }).catch(e => console.warn('Roster rank mirror failed:', e.message));
        } catch (e) {
            console.warn('Roster rank mirror exception:', e.message);
        }

        res.json({ ok: true, message: 'Promotion announcement posted.' });
    } catch (err) {
        console.error('POST /admin/promote-announce error:', err);
        res.status(500).json({ error: err.message });
    }
});

// ==============================================================================
// === EVENT NOTIFY-ME OPT-IN (DM 30 min before start + at start) ===
// ==============================================================================
// Adds a "🔔 Notify" button to event messages. Clicking toggles a member into
// `ev.notify_users`; the scheduler DMs everyone in that list at T-30 and T-0.

function buildEventNotifyRow(ev) {
    return new ActionRowBuilder().addComponents(
        new ButtonBuilder()
            .setCustomId(`evnotify:${ev.id}`)
            .setLabel('Notify me 30m before')
            .setEmoji('🔔')
            .setStyle(ButtonStyle.Primary)
    );
}

async function handleEventNotifyButton(interaction, eventId) {
    const data = loadCustomEvents();
    const ev = data.events.find(e => e.id === eventId);
    if (!ev) {
        return interaction.reply({ content: '❌ This event no longer exists.', ephemeral: true }).catch(() => {});
    }
    if (!Array.isArray(ev.notify_users)) ev.notify_users = [];
    const userId = interaction.user.id;
    let action;
    if (ev.notify_users.includes(userId)) {
        ev.notify_users = ev.notify_users.filter(u => u !== userId);
        action = 'off';
    } else {
        ev.notify_users.push(userId);
        action = 'on';
    }
    saveCustomEvents(data);

    try { await interaction.deferUpdate(); } catch {}

    const ack = action === 'on'
        ? `🔔 You'll get a DM 30 minutes before **${ev.title}** starts (and again at start).`
        : `🔕 Notifications turned off for **${ev.title}**.`;
    try { await interaction.followUp({ content: ack, ephemeral: true }); } catch {}
}

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
