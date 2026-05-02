const DEFAULT_SHIP_DATA = {
  'Aurora Mk II': { cargo: 2, military: false },
  'C8X Pisces Expedition': { cargo: 4, military: false },
  '135c': { cargo: 6, military: false },
  '300i': { cargo: 8, military: false },
  'Avenger Titan': { cargo: 8, military: true },
  '325a': { cargo: 4, military: false },
  '315p': { cargo: 12, military: false },
  'Cutter': { cargo: 4, military: false },
  'Cutter Rambler': { cargo: 4, military: false },
  'Nomad': { cargo: 24, military: false },
  'Reliant Kore': { cargo: 6, military: false },
  'Apollo Medivac': { cargo: 32, military: false },
  'Apollo Triage': { cargo: 32, military: false },
  'Zeus Mk II ES': { cargo: 32, military: false },
  'Zeus Mk II CL': { cargo: 128, military: false },
  'Freelancer DUR': { cargo: 36, military: false },
  'Freelancer': { cargo: 66, military: false },
  'Freelancer MIS': { cargo: 36, military: true },
  '600i Explorer': { cargo: 44, military: false },
  '400i': { cargo: 42, military: false },
  'Cutlass Black': { cargo: 46, military: false },
  'C1 Spirit': { cargo: 64, military: false },
  'Hull A': { cargo: 64, military: false },
  'Corsair': { cargo: 72, military: false },
  'Mercury Star Runner': { cargo: 114, military: false },
  'Freelancer MAX': { cargo: 120, military: false },
  'Constellation Taurus': { cargo: 174, military: false },
  'RAFT': { cargo: 192, military: false },
  'A2 Hercules Starlifter': { cargo: 216, military: true },
  'Starlancer MAX': { cargo: 224, military: false },
  'MPUV Cargo': { cargo: 2, military: false },
  'Hull B': { cargo: 384, military: false },
  'Carrack': { cargo: 456, military: true },
  'M2 Hercules Starlifter': { cargo: 522, military: true },
  'Starfarer': { cargo: 291, military: false },
  'Starfarer Gemini': { cargo: 291, military: true },
  'Polaris': { cargo: 576, military: true },
  'C2 Hercules Starlifter': { cargo: 696, military: false },
  'Hull C': { cargo: 4608, military: false },
};

const SHIP_ALIASES = {
  aurora: 'Aurora Mk II',
  pisces: 'C8X Pisces Expedition',
  c8x: 'C8X Pisces Expedition',
  '135c': '135c',
  '300i': '300i',
  titan: 'Avenger Titan',
  avenger: 'Avenger Titan',
  '325a': '325a',
  '315p': '315p',
  cutter: 'Cutter',
  rambler: 'Cutter Rambler',
  'cutter rambler': 'Cutter Rambler',
  nomad: 'Nomad',
  kore: 'Reliant Kore',
  'reliant kore': 'Reliant Kore',
  apollo: 'Apollo Medivac',
  'apollo medivac': 'Apollo Medivac',
  'apollo triage': 'Apollo Triage',
  'zeus es': 'Zeus Mk II ES',
  'zeus cl': 'Zeus Mk II CL',
  'zeus mk ii cl': 'Zeus Mk II CL',
  'zeus mk ii es': 'Zeus Mk II ES',
  'freelancer dur': 'Freelancer DUR',
  freelancer: 'Freelancer',
  'freelancer mis': 'Freelancer MIS',
  '600i': '600i Explorer',
  '600i explorer': '600i Explorer',
  '400i': '400i',
  cutlass: 'Cutlass Black',
  'cutlass black': 'Cutlass Black',
  c1: 'C1 Spirit',
  'c1 spirit': 'C1 Spirit',
  'hull a': 'Hull A',
  corsair: 'Corsair',
  msr: 'Mercury Star Runner',
  mercury: 'Mercury Star Runner',
  'mercury star runner': 'Mercury Star Runner',
  'freelancer max': 'Freelancer MAX',
  taurus: 'Constellation Taurus',
  'constellation taurus': 'Constellation Taurus',
  raft: 'RAFT',
  a2: 'A2 Hercules Starlifter',
  'starlancer max': 'Starlancer MAX',
  starlancer: 'Starlancer MAX',
  mpuv: 'MPUV Cargo',
  'mpuv cargo': 'MPUV Cargo',
  'hull b': 'Hull B',
  carrack: 'Carrack',
  m2: 'M2 Hercules Starlifter',
  starfarer: 'Starfarer',
  gemini: 'Starfarer Gemini',
  'starfarer gemini': 'Starfarer Gemini',
  polaris: 'Polaris',
  c2: 'C2 Hercules Starlifter',
  'hull c': 'Hull C',
};

const SHIP_STATS_URL = 'https://starcitizen.tools/Ship_cargo_stats';
const CACHE_MS = 24 * 60 * 60 * 1000;
const SHIP_FETCH_TIMEOUT_MS = 8000;
const MILITARY_SHIP_NAMES = new Set([
  'a2 hercules starlifter',
  'avenger titan',
  'carrack',
  'freelancer mis',
  'm2 hercules starlifter',
  'polaris',
  'starfarer gemini',
]);

const state = {
  shipData: { ...DEFAULT_SHIP_DATA },
  lastUpdated: 0,
  sourceLabel: 'bundled fallback data',
};
let refreshPromise = null;

function normalizeText(value) {
  return String(value || '').trim().toLowerCase();
}

function stripHtml(value) {
  return String(value || '')
    .replace(/<script[\s\S]*?<\/script>/gi, '')
    .replace(/<style[\s\S]*?<\/style>/gi, '')
    .replace(/<[^>]+>/g, ' ')
    .replace(/&nbsp;/g, ' ')
    .replace(/&amp;/g, '&')
    .replace(/&#39;/g, "'")
    .replace(/&quot;/g, '"')
    .replace(/\s+/g, ' ')
    .trim();
}

function isMilitaryShipName(name) {
  return MILITARY_SHIP_NAMES.has(normalizeText(name));
}

async function fetchLatestShipData() {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), SHIP_FETCH_TIMEOUT_MS);
  let response;

  try {
    response = await fetch(SHIP_STATS_URL, {
      headers: { 'User-Agent': 'SPACEWHLE Trade Command Bot' },
      signal: controller.signal,
    });
  } finally {
    clearTimeout(timeout);
  }

  if (!response.ok) throw new Error(`Ship stats fetch failed with HTTP ${response.status}`);
  const html = await response.text();

  const rows = [...html.matchAll(/<tr[\s\S]*?<\/tr>/gi)];
  const parsed = {};

  for (const rowMatch of rows) {
    const rowHtml = rowMatch[0];
    const cells = [...rowHtml.matchAll(/<t[dh][\s\S]*?>([\s\S]*?)<\/t[dh]>/gi)].map(match => stripHtml(match[1]));
    if (cells.length < 3) continue;

    const cargoCell = cells.find(cell => /SCU/i.test(cell));
    if (!cargoCell) continue;

    const cargoMatch = cargoCell.match(/(\d+(?:\.\d+)?)\s*SCU/i);
    if (!cargoMatch) continue;

    const name = cells[0];
    if (!name || /name/i.test(name)) continue;

    const cargo = Math.round(Number(cargoMatch[1]));
    if (!Number.isFinite(cargo)) continue;

    parsed[name] = {
      cargo,
      military: isMilitaryShipName(name),
    };
  }

  if (!Object.keys(parsed).length) throw new Error('No ship rows parsed from StarCitizen.tools');

  state.shipData = {
    ...DEFAULT_SHIP_DATA,
    ...parsed,
  };
  state.lastUpdated = Date.now();
  state.sourceLabel = 'live StarCitizen.tools ship cargo stats';
  return state.shipData;
}

async function ensureShipData(force = false) {
  if (!force && state.lastUpdated && Date.now() - state.lastUpdated < CACHE_MS) {
    return state.shipData;
  }

  if (!force && refreshPromise) return refreshPromise;

  refreshPromise = (async () => {
    try {
      await fetchLatestShipData();
    } catch (error) {
      console.error('Live ship data refresh failed, using fallback data:', error.message);
      if (!state.lastUpdated) {
        state.lastUpdated = Date.now();
        state.sourceLabel = 'bundled fallback data';
      }
    } finally {
      refreshPromise = null;
    }

    return state.shipData;
  })();

  return refreshPromise;
}

function normalizeShipName(input) {
  const cleaned = normalizeText(input);
  if (SHIP_ALIASES[cleaned]) return SHIP_ALIASES[cleaned];

  const exact = Object.keys(state.shipData).find(name => normalizeText(name) === cleaned);
  if (exact) return exact;

  const partial = Object.keys(state.shipData).find(name => normalizeText(name).includes(cleaned));
  return partial || null;
}

function getShipProfile(shipName) {
  const name = normalizeShipName(shipName);
  if (!name || !state.shipData[name]) return null;

  const base = state.shipData[name];
  let cargoTier = 'small';
  let shipRiskModifier = 0;

  if (base.cargo <= 8) {
    cargoTier = 'tiny';
    shipRiskModifier = 8;
  } else if (base.cargo <= 32) {
    cargoTier = 'small';
    shipRiskModifier = 4;
  } else if (base.cargo <= 72) {
    cargoTier = 'medium';
    shipRiskModifier = 1;
  } else if (base.cargo <= 192) {
    cargoTier = 'large';
    shipRiskModifier = -2;
  } else if (base.cargo <= 696) {
    cargoTier = 'heavy';
    shipRiskModifier = -4;
  } else {
    cargoTier = 'super-heavy';
    shipRiskModifier = -6;
  }

  if (base.military) shipRiskModifier -= 8;

  return {
    name,
    cargo: base.cargo,
    military: base.military,
    cargoTier,
    shipRiskModifier,
  };
}

function getShipChoices() {
  return Object.keys(state.shipData).sort((a, b) => a.localeCompare(b));
}

function getShipSourceLabel() {
  return state.sourceLabel;
}

module.exports = {
  SHIP_ALIASES,
  ensureShipData,
  getShipChoices,
  getShipProfile,
  getShipSourceLabel,
  normalizeShipName,
};
