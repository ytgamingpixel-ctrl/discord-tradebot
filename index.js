require('dotenv').config();
const fs = require('node:fs');
const path = require('node:path');
const crypto = require('node:crypto');

const {
  Client,
  GatewayIntentBits,
  Events,
  EmbedBuilder,
  ButtonBuilder,
  ButtonStyle,
  ActionRowBuilder,
} = require('discord.js');

const client = new Client({
  intents: [GatewayIntentBits.Guilds],
});

const CACHE_TTL_MS = 15 * 60 * 1000;
const STATE_TTL_MS = 7 * 24 * 60 * 60 * 1000;

const EMBED_THUMBNAIL_URL =
  'https://robertsspaceindustries.com/media/zlgck6fw560rdr/logo/SPACEWHLE-Logo.png';
const EMBED_BANNER_URL =
  'https://s1.cdn.autoevolution.com/images/news/star-citizen-unveils-a-massive-space-hauler-crowdfunding-passes-400-million-175169_1.jpg';

const STATE_FILE = path.join(__dirname, 'route-state.json');

const SHIP_DATA = {
  'Aurora Mk II': { cargo: 2, military: false },
  'C8X Pisces Expedition': { cargo: 4, military: false },
  '135c': { cargo: 6, military: false },
  '300i': { cargo: 8, military: false },
  'Avenger Titan': { cargo: 8, military: false },
  '325a': { cargo: 8, military: false },
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
  'Freelancer MIS': { cargo: 36, military: false },
  '600i Explorer': { cargo: 40, military: false },
  '400i': { cargo: 42, military: false },
  'Cutlass Black': { cargo: 46, military: false },
  'C1 Spirit': { cargo: 64, military: false },
  'Hull A': { cargo: 64, military: false },
  'Corsair': { cargo: 72, military: false },
  'Mercury Star Runner': { cargo: 114, military: false },
  'Freelancer MAX': { cargo: 120, military: false },
  'Constellation Taurus': { cargo: 168, military: false },
  'RAFT': { cargo: 192, military: false },
  'A2 Hercules Starlifter': { cargo: 216, military: true },
  'Starlancer MAX': { cargo: 224, military: false },
  'MPUV Cargo': { cargo: 2, military: false },
  'Hull B': { cargo: 384, military: false },
  'Carrack': { cargo: 456, military: false },
  'M2 Hercules Starlifter': { cargo: 468, military: true },
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

const cache = {
  lastUpdated: 0,
  groups: [],
  shortGroupNames: [],
  routes: [],
  commodityNames: [],
};

const routeStates = new Map();
const activeMessageLocks = new Set();

function normalizeText(value) {
  return String(value || '').trim().toLowerCase();
}

function clamp(value, min, max) {
  return Math.max(min, Math.min(max, value));
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

function normalizeShipName(input) {
  const cleaned = normalizeText(input);
  if (SHIP_ALIASES[cleaned]) return SHIP_ALIASES[cleaned];

  const exact = Object.keys(SHIP_DATA).find(name => normalizeText(name) === cleaned);
  if (exact) return exact;

  const partial = Object.keys(SHIP_DATA).find(name => normalizeText(name).includes(cleaned));
  return partial || null;
}

function getShipProfile(shipName) {
  const name = normalizeShipName(shipName);
  if (!name || !SHIP_DATA[name]) return null;

  const base = SHIP_DATA[name];
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

async function fetchJson(url) {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), 20000);

  try {
    const response = await fetch(url, {
      headers: {
        'User-Agent': 'SPACEWHLE Trade Command Bot',
        Accept: 'application/json',
      },
      signal: controller.signal,
    });

    if (!response.ok) throw new Error(`HTTP ${response.status} for ${url}`);
    return await response.json();
  } finally {
    clearTimeout(timeout);
  }
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
      });
    }

    const terminalEntry = group.terminals.get(terminalId);

    if (Number(row.price_buy) > 0) {
      terminalEntry.sells.push({
        commodity: commodityName,
        price: Number(row.price_buy),
        stock: Number(row.scu_buy ?? row.scu_buy_avg ?? row.scu_buy_stock ?? 0),
      });
    }

    if (Number(row.price_sell) > 0) {
      terminalEntry.buys.push({
        commodity: commodityName,
        price: Number(row.price_sell),
        demand: Number(row.scu_sell ?? row.scu_sell_avg ?? row.scu_sell_stock ?? 0),
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
              commodity: item.commodity,
              price: item.price,
              stock: item.stock,
              terminalName: terminal.name,
              atmospheric: terminal.atmospheric,
            });
          }
        }

        for (const item of terminal.buys) {
          const existing = buys.get(item.commodity);
          if (!existing || item.price > existing.price) {
            buys.set(item.commodity, {
              commodity: item.commodity,
              price: item.price,
              demand: item.demand,
              terminalName: terminal.name,
              atmospheric: terminal.atmospheric,
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
  };
}

function buildRouteIndex(groups) {
  const sellersByCommodity = new Map();
  const buyersByCommodity = new Map();

  for (const group of groups) {
    for (const sell of group.sells) {
      if (!sellersByCommodity.has(sell.commodity)) sellersByCommodity.set(sell.commodity, []);
      sellersByCommodity.get(sell.commodity).push({
        groupName: group.name,
        shortGroupName: group.shortName,
        system: group.system,
        locationType: group.locationType,
        atmospheric: group.atmospheric || sell.atmospheric,
        terminalName: sell.terminalName,
        price: sell.price,
        stock: sell.stock,
      });
    }

    for (const buy of group.buys) {
      if (!buyersByCommodity.has(buy.commodity)) buyersByCommodity.set(buy.commodity, []);
      buyersByCommodity.get(buy.commodity).push({
        groupName: group.name,
        shortGroupName: group.shortName,
        system: group.system,
        locationType: group.locationType,
        atmospheric: group.atmospheric || buy.atmospheric,
        terminalName: buy.terminalName,
        price: buy.price,
        demand: buy.demand,
      });
    }
  }

  const routes = [];

  for (const [commodity, sellersRaw] of sellersByCommodity.entries()) {
    const buyersRaw = buyersByCommodity.get(commodity);
    if (!buyersRaw?.length) continue;

    const sellers = [...sellersRaw].sort((a, b) => a.price - b.price).slice(0, 20);
    const buyers = [...buyersRaw].sort((a, b) => b.price - a.price).slice(0, 20);

    for (const seller of sellers) {
      for (const buyer of buyers) {
        if (seller.groupName === buyer.groupName) continue;

        const profitPerScu = buyer.price - seller.price;
        if (profitPerScu <= 0) continue;

        routes.push({
          commodity,
          buyGroup: seller.groupName,
          buyShortGroup: seller.shortGroupName,
          sellGroup: buyer.groupName,
          sellShortGroup: buyer.shortGroupName,
          buyTerminal: seller.terminalName,
          sellTerminal: buyer.terminalName,
          buySystem: seller.system,
          sellSystem: buyer.system,
          buyLocationType: seller.locationType,
          sellLocationType: buyer.locationType,
          buyRequiresAtmosphere: seller.atmospheric,
          sellRequiresAtmosphere: buyer.atmospheric,
          buyPricePerScu: seller.price,
          sellPricePerScu: buyer.price,
          buyStock: seller.stock,
          sellDemand: buyer.demand,
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

  const [terminalsPayload, pricesPayload] = await Promise.all([
    fetchJson('https://api.uexcorp.space/2.0/terminals?type=commodity'),
    fetchJson('https://api.uexcorp.space/2.0/commodities_prices_all'),
  ]);

  const terminals = getArrayPayload(terminalsPayload).filter(
    terminal => String(terminal.type || '').toLowerCase() === 'commodity'
  );
  const prices = getArrayPayload(pricesPayload);
  const grouped = buildGroupedLocationIndex(terminals, prices);

  cache.lastUpdated = now;
  cache.groups = grouped.groups;
  cache.shortGroupNames = grouped.shortGroupNames;
  cache.routes = buildRouteIndex(grouped.groups);
  cache.commodityNames = grouped.commodityNames;

  return cache;
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

function getRiskScore(route, cargo, cargoValue, shipName) {
  const ship = getShipProfile(shipName);
  let risk = 0;

  if (route.buySystem === 'Pyro' || route.sellSystem === 'Pyro') risk += 50;
  if (route.buySystem === 'Nyx' || route.sellSystem === 'Nyx') risk += 25;
  if (route.interSystem) risk += 15;
  if (cargo > 750) risk += 10;
  if (cargoValue > 10000000) risk += 15;
  else if (cargoValue > 1000000) risk += 5;

  risk += getLocationRiskModifier(route);
  risk += ship.shipRiskModifier;

  return Math.round(clamp(risk, 0, 100));
}

function getRiskReasons(route, cargo, cargoValue, shipName) {
  const ship = getShipProfile(shipName);
  const reasons = [];

  if (route.buySystem === 'Pyro' || route.sellSystem === 'Pyro') reasons.push('Pyro involvement');
  if (route.buySystem === 'Nyx' || route.sellSystem === 'Nyx') reasons.push('Nyx involvement');
  if (route.interSystem) reasons.push('inter-system travel');
  if (cargo > 750) reasons.push('very large cargo load');
  if (cargoValue > 10000000) reasons.push('extremely high cargo value');
  else if (cargoValue > 1000000) reasons.push('high cargo value');
  if (ship.military) reasons.push('military hull lowered risk');
  else if (ship.cargo <= 32) reasons.push('smaller cargo ship');

  if (!reasons.length) reasons.push('no major risk factors');
  return reasons.slice(0, 4).join(', ');
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

function getDockingPreferenceBonus(route) {
  let bonus = 0;

  const buyType = normalizeText(route.buyLocationType);
  const sellType = normalizeText(route.sellLocationType);

  if (buyType.includes('station')) bonus += 500;
  if (sellType.includes('station')) bonus += 500;
  if (buyType.includes('city')) bonus += 150;
  if (sellType.includes('city')) bonus += 150;
  if (buyType.includes('outpost')) bonus -= 700;
  if (sellType.includes('outpost')) bonus -= 700;

  return bonus;
}

function scoreRoute(route, desiredCargo, shipName, budget = null) {
  const ship = getShipProfile(shipName);
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
  const riskScore = getRiskScore(route, effectiveCargo, cargoValue, ship.name);
  const riskReasons = getRiskReasons(route, effectiveCargo, cargoValue, ship.name);
  const time = getFlightTime(route);

  const rankingScore =
    (totalProfit * 12) +
    (fillRatio * 400000) +
    (!route.interSystem ? 4000 : 0) +
    getDockingPreferenceBonus(route) -
    (riskScore * 200) -
    (time.total * 30);

  return {
    ...route,
    effectiveCargo,
    cargoValue,
    totalProfit,
    profitPercent,
    fillRatio,
    riskScore,
    riskReasons,
    time,
    shipProfile: ship,
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

async function findBestRoute({ cargo, shipName, location, finish, budget, previousSignature = null }) {
  await loadMarketData(false);

  const ship = getShipProfile(shipName);
  if (!ship) throw new Error('Invalid ship.');

  const desiredCargo = cargo || ship.cargo;
  const startGroup = location ? findMatchingGroup(location) : null;
  const finishGroup = finish ? findMatchingGroup(finish) : null;

  const filteredRoutes = cache.routes.filter(route => {
    if (startGroup && route.buyGroup !== startGroup.name) return false;
    if (finishGroup && route.sellGroup !== finishGroup.name) return false;
    return true;
  });

  const scoredRoutes = filteredRoutes
    .map(route => scoreRoute(route, desiredCargo, ship.name, budget || null))
    .filter(Boolean)
    .sort((a, b) => b.rankingScore - a.rankingScore);

  return {
    ship,
    desiredCargo,
    route: chooseBestRoute(scoredRoutes, previousSignature),
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
    const shipChoices = Object.keys(SHIP_DATA).sort((a, b) => a.localeCompare(b));
    await interaction.respond(pickAutocompleteChoices(shipChoices, focused.value));
    return;
  }

  if (focused.name === 'location' || focused.name === 'finish') {
    try {
      if (!cache.shortGroupNames.length) await loadMarketData(false);
    } catch (error) {
      console.error('Autocomplete market load error:', error);
    }

    await interaction.respond(pickAutocompleteChoices(cache.shortGroupNames, focused.value));
    return;
  }

  if (focused.name === 'commodity') {
    try {
      if (!cache.commodityNames.length) await loadMarketData(false);
    } catch (error) {
      console.error('Commodity autocomplete load error:', error);
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

function getBudgetStep(currentBudget) {
  if (!currentBudget || currentBudget < 100000) return 25000;
  if (currentBudget < 500000) return 50000;
  if (currentBudget < 2000000) return 100000;
  return 250000;
}

async function buildRouteResponse(params, existingStateId = null) {
  const result = await findBestRoute(params);

  if (!result.route) {
    return {
      content: 'No matching route found for those filters.',
      embeds: [],
      components: [],
      stateId: existingStateId,
    };
  }

  const route = result.route;
  const currentBudget = params.budget || route.cargoValue;

  const nextState = {
    shipName: params.shipName,
    cargo: params.cargo ?? null,
    budget: currentBudget,
    location: params.location ?? null,
    finish: params.finish ?? null,
    previousSignature: getRouteSignature(route),
  };

  const stateId = saveRouteState(nextState, existingStateId);

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
    const currentBudget = state.budget || 100000;
    const step = getBudgetStep(currentBudget);

    if (action === 'refresh') {
      nextState.previousSignature = state.previousSignature || null;
    } else if (action === 'invest_up') {
      nextState.budget = Math.max(1, currentBudget + step);
      nextState.previousSignature = null;
    } else if (action === 'invest_down') {
      nextState.budget = Math.max(1, currentBudget - step);
      nextState.previousSignature = null;
    }

    const response = await buildRouteResponse(nextState, stateId);

    await interaction.editReply({
      content: response.content ?? undefined,
      embeds: response.embeds,
      components: response.components,
    });
  } finally {
    activeMessageLocks.delete(lockKey);
  }
}

client.once(Events.ClientReady, readyClient => {
  loadStateStoreFromDisk();
  console.log(`Logged in as ${readyClient.user.tag}`);
});

client.on(Events.InteractionCreate, async interaction => {
  if (interaction.isAutocomplete()) {
    try {
      await handleAutocomplete(interaction);
    } catch (error) {
      console.error('Autocomplete error:', error);
      try {
        await interaction.respond([]);
      } catch {}
    }
    return;
  }

  if (interaction.isButton()) {
    const [prefix, action, stateId] = interaction.customId.split(':');
    if (prefix !== 'route') return;

    try {
      await interaction.deferUpdate();
      await handleRouteButton(interaction, action, stateId);
    } catch (error) {
      console.error('Route button error:', error);
      try {
        await interaction.followUp({
          content: 'Could not update that route.',
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

    if (interaction.commandName === 'route') {
      await interaction.deferReply();

      const response = await buildRouteResponse({
        shipName: interaction.options.getString('ship', true),
        cargo: interaction.options.getInteger('cargo'),
        budget: interaction.options.getInteger('budget'),
        location: interaction.options.getString('location'),
        finish: interaction.options.getString('finish'),
      });

      await interaction.editReply({
        content: response.content ?? undefined,
        embeds: response.embeds,
        components: response.components,
      });
      return;
    }

    if (interaction.commandName === 'best-routes') {
      await interaction.deferReply();

      const shipName = interaction.options.getString('ship', true);
      const ship = getShipProfile(shipName);

      if (!ship) {
        await interaction.editReply({ content: 'Invalid ship.' });
        return;
      }

      const locationInput = interaction.options.getString('location');
      const finishInput = interaction.options.getString('finish');
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
          text: 'SPACEWHLE Trade Command • bracket summary',
        });

      await interaction.editReply({ embeds: [embed] });
      return;
    }

    if (interaction.commandName === 'location') {
      await interaction.deferReply();
      await loadMarketData(false);

      const locationInput = interaction.options.getString('location', true);
      const group = findMatchingGroup(locationInput);

      if (!group) {
        await interaction.editReply({ content: 'I could not find that location.' });
        return;
      }

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
      await interaction.deferReply();
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
  } catch (error) {
    console.error('Interaction error:', error);

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

client.login(process.env.DISCORD_TOKEN);