require('dotenv').config();
const fs = require('node:fs');
const path = require('node:path');
const {
  ActionRowBuilder,
  AttachmentBuilder,
  ButtonBuilder,
  ButtonStyle,
  EmbedBuilder,
  StringSelectMenuBuilder,
} = require('discord.js');

let CachedResvg = null;
let resvgLoadAttempted = false;

function getResvgConstructor() {
  if (CachedResvg) return CachedResvg;
  if (resvgLoadAttempted) return null;

  resvgLoadAttempted = true;

  try {
    ({ Resvg: CachedResvg } = require('@resvg/resvg-js'));
    return CachedResvg;
  } catch (error) {
    console.warn('Stats image renderer unavailable, falling back to standard embeds:', error.message);
    return null;
  }
}

const STATE_FILE = path.join(__dirname, 'stats-state.json');
const MAX_DAYS = 35;
const ONE_DAY_MS = 24 * 60 * 60 * 1000;
const STAR_CITIZEN_MATCH = /star\s*citizen/i;
const THUMBNAIL_URL = 'https://robertsspaceindustries.com/media/zlgck6fw560rdr/logo/SPACEWHLE-Logo.png';
const TRACKED_ROLE_NAME = (process.env.TRACKED_ROLE_NAME || 'SPACEWHLE').replace(/^@/, '').trim();
const TRACKED_ROLE_ID = process.env.TRACKED_ROLE_ID || null;
const CHART_BG = '#2f3136';
const CHART_TITLE = '#92927c';
const CHART_AXIS = '#3b82f6';
const CHART_LABELS = '#ef4444';
const CHART_DATA = '#22c55e';
const CHART_FILL = 'rgba(34, 197, 94, 0.22)';
const CHART_GRID = '#4b5563';
const PANEL_WIDTH = 1200;
const PANEL_HEIGHT = 820;
const TOP_PANEL_HEIGHT = 990;
const PANEL_RENDER_SCALE = 1;
const PANEL_RENDER_WIDTH = Math.round(PANEL_WIDTH * PANEL_RENDER_SCALE);
const PANEL_BG = '#1f2125';
const PANEL_BG_ACCENT = '#272b33';
const PANEL_CARD = '#2a2d33';
const PANEL_CARD_ALT = '#31343b';
const PANEL_ROW = '#383c45';
const PANEL_STROKE = '#41454d';
const PANEL_TEXT = '#f2f3f5';
const PANEL_MUTED = '#b5bac1';
const PANEL_SUBTLE = '#8e9297';
const PANEL_GREEN = '#43b581';
const PANEL_PINK = '#f06292';
const PANEL_CYAN = '#22d3ee';
const PANEL_GOLD = '#f6c453';
const PANEL_BLUE = '#78a9ff';
const FONT_UI = "'DejaVu Sans', 'Noto Sans', 'Liberation Sans', Arial, sans-serif";
const FONT_DISPLAY = "'DejaVu Sans', 'Noto Sans', 'Liberation Sans', Arial, sans-serif";
const PANEL_FRAME_MARGIN = 10;
const PANEL_FRAME_RADIUS = 34;
const PANEL_CONTENT_X = 40;
const PANEL_CONTENT_WIDTH = PANEL_WIDTH - (PANEL_CONTENT_X * 2);
const PANEL_PILL_GAP = 16;
const PANEL_PILL_WIDTH = 252;
const PANEL_PILL_WIDE_WIDTH = PANEL_CONTENT_WIDTH - (PANEL_PILL_WIDTH * 3) - (PANEL_PILL_GAP * 3);

function escapeSvg(value) {
  return String(value ?? '')
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;');
}

function truncateLabel(value, maxLength = 24) {
  const text = String(value || '').trim();
  if (!text) return 'Unknown';
  return text.length > maxLength ? `${text.slice(0, Math.max(1, maxLength - 3))}...` : text;
}

function truncateToWidth(value, maxWidth, fontSize = 16, minChars = 6) {
  const width = Math.max(1, Number(maxWidth || 0));
  const approximateChars = Math.max(minChars, Math.floor(width / Math.max(6, fontSize * 0.58)));
  return truncateLabel(value, approximateChars);
}

function estimateTextWidth(value, fontSize = 16, weight = 600, letterSpacing = 0) {
  const text = String(value || '');
  const weightFactor = weight >= 800 ? 1.08 : weight >= 700 ? 1.04 : 1;
  let units = 0;

  for (const char of text) {
    if (char === ' ') units += 0.32;
    else if ('il.:,;|!\'`'.includes(char)) units += 0.28;
    else if ('mwWM@#%&'.includes(char)) units += 0.82;
    else if (/[A-Z0-9]/.test(char)) units += 0.66;
    else units += 0.56;
  }

  return (units * fontSize * weightFactor) + (Math.max(0, text.length - 1) * letterSpacing);
}

function fitTextToWidth(value, maxWidth, options = {}) {
  const {
    size = 16,
    minSize = Math.max(10, size - 5),
    weight = 600,
    letterSpacing = 0,
    minChars = 6,
    fallback = 'Unknown',
  } = options;

  let text = String(value || '').trim() || fallback;
  let fontSize = size;
  const width = Math.max(1, Number(maxWidth || 0) - Math.max(4, Math.round(size * 0.18)));

  while (fontSize > minSize && estimateTextWidth(text, fontSize, weight, letterSpacing) > width) {
    fontSize -= 1;
  }

  if (estimateTextWidth(text, fontSize, weight, letterSpacing) <= width) {
    return { text, size: fontSize };
  }

  let candidate = text;
  while (candidate.length > minChars) {
    const next = `${candidate.slice(0, -1).trimEnd()}...`;
    if (estimateTextWidth(next, fontSize, weight, letterSpacing) <= width) {
      return { text: next, size: fontSize };
    }
    candidate = candidate.slice(0, -1);
  }

  return {
    text: truncateToWidth(text, width, fontSize, minChars),
    size: fontSize,
  };
}

function splitTextToLines(value, maxWidth, fontSize, weight, letterSpacing) {
  const text = String(value || '').trim();
  if (!text) return [];

  const words = text.split(/\s+/).filter(Boolean);
  if (!words.length) return [];

  const lines = [];
  let current = words.shift();

  for (const word of words) {
    const next = `${current} ${word}`;
    if (estimateTextWidth(next, fontSize, weight, letterSpacing) <= maxWidth) {
      current = next;
    } else {
      lines.push(current);
      current = word;
    }
  }

  if (current) lines.push(current);
  return lines;
}

function wrapTextToLines(value, maxWidth, maxLines = 2, options = {}) {
  const {
    size = 14,
    minSize = Math.max(10, size - 3),
    weight = 600,
    letterSpacing = 0,
    minChars = 8,
    fallback = 'Unknown',
  } = options;

  const text = String(value || '').trim() || fallback;
  const width = Math.max(1, Number(maxWidth || 0) - Math.max(4, Math.round(size * 0.18)));
  let fontSize = size;

  while (fontSize > minSize) {
    const lines = splitTextToLines(text, width, fontSize, weight, letterSpacing);
    if (lines.length <= maxLines) {
      return {
        lines: lines.map(line => fitTextToWidth(line, width, {
          size: fontSize,
          minSize: fontSize,
          weight,
          letterSpacing,
          minChars,
        }).text),
        size: fontSize,
      };
    }
    fontSize -= 1;
  }

  const lines = splitTextToLines(text, width, fontSize, weight, letterSpacing);
  const trimmedLines = lines.slice(0, maxLines).map((line, index) => {
    if (index < maxLines - 1) {
      return fitTextToWidth(line, width, {
        size: fontSize,
        minSize: fontSize,
        weight,
        letterSpacing,
        minChars,
      }).text;
    }

    return fitTextToWidth(lines.slice(index).join(' '), width, {
      size: fontSize,
      minSize: fontSize,
      weight,
      letterSpacing,
      minChars,
    }).text;
  });

  return {
    lines: trimmedLines,
    size: fontSize,
  };
}

function svgRect(x, y, width, height, radius, fill, stroke = 'none', strokeWidth = 0, opacity = 1) {
  return `<rect x="${x}" y="${y}" width="${width}" height="${height}" rx="${radius}" fill="${fill}" opacity="${opacity}" stroke="${stroke}" stroke-width="${strokeWidth}" />`;
}

function svgCircle(cx, cy, radius, fill, stroke = 'none', strokeWidth = 0, opacity = 1) {
  return `<circle cx="${cx}" cy="${cy}" r="${radius}" fill="${fill}" opacity="${opacity}" stroke="${stroke}" stroke-width="${strokeWidth}" />`;
}

function svgLine(x1, y1, x2, y2, stroke, strokeWidth = 1, opacity = 1, dashArray = '') {
  return `<line x1="${x1}" y1="${y1}" x2="${x2}" y2="${y2}" stroke="${stroke}" stroke-width="${strokeWidth}" opacity="${opacity}"${dashArray ? ` stroke-dasharray="${dashArray}"` : ''} />`;
}

function svgPath(d, fill = 'none', stroke = 'none', strokeWidth = 0, opacity = 1) {
  return `<path d="${d}" fill="${fill}" stroke="${stroke}" stroke-width="${strokeWidth}" stroke-linecap="round" stroke-linejoin="round" opacity="${opacity}" />`;
}

function svgText(value, x, y, options = {}) {
  const {
    size = 18,
    weight = 600,
    fill = PANEL_TEXT,
    anchor = 'start',
    opacity = 1,
    family = FONT_UI,
    style = 'normal',
    letterSpacing = 0,
  } = options;

  return `<text x="${x}" y="${y}" fill="${fill}" font-family="${family}" font-size="${size}" font-weight="${weight}" font-style="${style}" text-anchor="${anchor}" dominant-baseline="middle" opacity="${opacity}" letter-spacing="${letterSpacing}">${escapeSvg(value)}</text>`;
}

function formatHoursVerbose(seconds, decimals = 2) {
  return `${(Number(seconds || 0) / 3600).toFixed(decimals)} hours`;
}

function formatHoursShort(seconds, decimals = 1) {
  return `${(Number(seconds || 0) / 3600).toFixed(decimals)} h`;
}

function formatMetricValue(metricKey, value, decimals = 2) {
  if (metricKey === 'messages') return `${formatNumber(value)} messages`;
  if (metricKey === 'voiceSeconds' || metricKey === 'starCitizenSeconds') return formatHoursVerbose(value, decimals);
  return String(value ?? 0);
}

function buildPolylinePoints(points) {
  return points.map(point => `${point.x},${point.y}`).join(' ');
}

function getTickIndices(total, targetCount = 4) {
  if (total <= 0) return [];
  if (total <= targetCount) return Array.from({ length: total }, (_, index) => index);

  const values = new Set([0, total - 1]);
  for (let step = 1; step < targetCount - 1; step += 1) {
    values.add(Math.round(((total - 1) * step) / (targetCount - 1)));
  }

  return Array.from(values).sort((a, b) => a - b);
}

function renderIconChip(x, y, iconType, color, width = 42, height = 32) {
  const parts = [svgRect(x, y, width, height, 11, color, 'none', 0)];
  const iconFill = PANEL_BG;

  if (iconType === 'messages') {
    parts.push(svgRect(x + 10, y + 8, 18, 12, 4, iconFill));
    parts.push(svgPath(`M ${x + 15} ${y + 20} L ${x + 13} ${y + 25} L ${x + 19} ${y + 20} Z`, iconFill));
  } else if (iconType === 'voice') {
    parts.push(svgPath(`M ${x + 10} ${y + 16} L ${x + 14} ${y + 16} L ${x + 19} ${y + 11} L ${x + 19} ${y + 23} L ${x + 14} ${y + 18} L ${x + 10} ${y + 18} Z`, iconFill));
    parts.push(svgPath(`M ${x + 22} ${y + 13} Q ${x + 26} ${y + 16} ${x + 22} ${y + 19}`, 'none', iconFill, 2));
    parts.push(svgPath(`M ${x + 24} ${y + 10} Q ${x + 30} ${y + 16} ${x + 24} ${y + 22}`, 'none', iconFill, 2));
  } else if (iconType === 'starCitizen') {
    parts.push(svgPath(`M ${x + 19} ${y + 8} L ${x + 24} ${y + 13} L ${x + 21} ${y + 24} L ${x + 17} ${y + 24} L ${x + 14} ${y + 13} Z`, iconFill));
    parts.push(svgPath(`M ${x + 14} ${y + 17} L ${x + 10} ${y + 21} L ${x + 15} ${y + 21} Z`, iconFill));
    parts.push(svgPath(`M ${x + 24} ${y + 17} L ${x + 28} ${y + 21} L ${x + 23} ${y + 21} Z`, iconFill));
    parts.push(svgCircle(x + 19, y + 15, 2.2, color));
    parts.push(svgPath(`M ${x + 17} ${y + 24} L ${x + 14} ${y + 28} L ${x + 19} ${y + 26} L ${x + 24} ${y + 28} L ${x + 21} ${y + 24} Z`, iconFill));
  } else if (iconType === 'leaderboard') {
    parts.push(svgPath(`M ${x + 12} ${y + 9} H ${x + 24} V ${y + 14} C ${x + 24} ${y + 18} ${x + 21} ${y + 21} ${x + 18} ${y + 21} C ${x + 15} ${y + 21} ${x + 12} ${y + 18} ${x + 12} ${y + 14} Z`, iconFill));
    parts.push(svgPath(`M ${x + 12} ${y + 12} C ${x + 8} ${y + 12} ${x + 8} ${y + 18} ${x + 12} ${y + 18}`, 'none', iconFill, 2));
    parts.push(svgPath(`M ${x + 24} ${y + 12} C ${x + 28} ${y + 12} ${x + 28} ${y + 18} ${x + 24} ${y + 18}`, 'none', iconFill, 2));
    parts.push(svgRect(x + 16, y + 21, 4, 5, 1, iconFill));
    parts.push(svgRect(x + 13, y + 27, 10, 3, 1.5, iconFill));
  } else if (iconType === 'route') {
    parts.push(svgCircle(x + 11, y + 16, 3.5, iconFill));
    parts.push(svgCircle(x + 28, y + 16, 3.5, iconFill));
    parts.push(svgPath(`M ${x + 15} ${y + 16} H ${x + 25}`, 'none', iconFill, 2.6));
    parts.push(svgPath(`M ${x + 22} ${y + 12} L ${x + 28} ${y + 16} L ${x + 22} ${y + 20}`, 'none', iconFill, 2.6));
  } else if (iconType === 'cargo') {
    parts.push(svgRect(x + 11, y + 10, 16, 14, 3, iconFill));
    parts.push(svgPath(`M ${x + 11} ${y + 14} H ${x + 27}`, 'none', color, 1.6));
    parts.push(svgPath(`M ${x + 19} ${y + 10} V ${y + 24}`, 'none', color, 1.6));
  } else if (iconType === 'location') {
    parts.push(svgPath(`M ${x + 19} ${y + 8} C ${x + 13} ${y + 8} ${x + 9} ${y + 12} ${x + 9} ${y + 18} C ${x + 9} ${y + 24} ${x + 19} ${y + 29} ${x + 19} ${y + 29} C ${x + 19} ${y + 29} ${x + 29} ${y + 24} ${x + 29} ${y + 18} C ${x + 29} ${y + 12} ${x + 25} ${y + 8} ${x + 19} ${y + 8} Z`, iconFill));
    parts.push(svgCircle(x + 19, y + 18, 4, color));
  } else if (iconType === 'ship') {
    parts.push(svgPath(`M ${x + 10} ${y + 20} L ${x + 15} ${y + 14} H ${x + 23} L ${x + 28} ${y + 20} L ${x + 24} ${y + 24} H ${x + 14} Z`, iconFill));
    parts.push(svgPath(`M ${x + 17} ${y + 14} L ${x + 19} ${y + 9} L ${x + 21} ${y + 14}`, iconFill));
  } else if (iconType === 'players') {
    parts.push(svgCircle(x + 15, y + 14, 4, iconFill));
    parts.push(svgCircle(x + 24, y + 16, 3.5, iconFill, 'none', 0, 0.9));
    parts.push(svgPath(`M ${x + 9} ${y + 26} C ${x + 9} ${y + 21} ${x + 21} ${y + 21} ${x + 21} ${y + 26}`, 'none', iconFill, 2.5));
    parts.push(svgPath(`M ${x + 19} ${y + 26} C ${x + 19} ${y + 22} ${x + 28} ${y + 22} ${x + 28} ${y + 26}`, 'none', iconFill, 2.1, 0.85));
  } else {
    parts.push(svgText('#', x + width / 2, y + height / 2 + 1, {
      size: 16,
      weight: 800,
      fill: iconFill,
      anchor: 'middle',
      family: FONT_DISPLAY,
    }));
  }

  return parts.join('');
}

function renderDataRowsCard({ x, y, width, height, title, chipType, chipColor, rows }) {
  const titleSize = width >= 320 ? 22 : title.length > 14 ? 18 : 22;
  const titleFit = fitTextToWidth(title, width - 92, {
    size: titleSize,
    minSize: Math.max(16, titleSize - 4),
    weight: 700,
    letterSpacing: 0.2,
    minChars: 8,
  });
  const cardParts = [
    svgRect(x, y, width, height, 24, PANEL_CARD, PANEL_STROKE, 1.2),
    renderIconChip(x + 18, y + 14, chipType, chipColor, 38, 28),
    svgText(titleFit.text, x + 66, y + 29, { size: titleFit.size, weight: 700, family: FONT_DISPLAY, letterSpacing: 0.2 }),
  ];

  const rowHeight = 28;
  const rowGap = 8;
  const rowWidth = width - 24;
  const startY = y + 58;

  rows.slice(0, 3).forEach((row, index) => {
    const rowY = startY + index * (rowHeight + rowGap);
    const labelFit = fitTextToWidth(row.label, rowWidth * 0.48, { size: 15, minSize: 12, weight: 700, minChars: 6 });
    const valueFit = fitTextToWidth(row.value, rowWidth * 0.42, { size: 15, minSize: 12, weight: 700, minChars: 6 });
    cardParts.push(svgRect(x + 12, rowY, rowWidth, rowHeight, 8, PANEL_ROW));
    cardParts.push(svgText(labelFit.text, x + 24, rowY + rowHeight / 2, {
      size: labelFit.size,
      weight: 700,
      fill: PANEL_MUTED,
      family: FONT_DISPLAY,
      style: 'italic',
    }));
    cardParts.push(svgText(valueFit.text, x + width - 16, rowY + rowHeight / 2, {
      size: valueFit.size,
      weight: 700,
      anchor: 'end',
      fill: PANEL_TEXT,
      family: FONT_UI,
    }));
  });

  return cardParts.join('');
}

function renderHeaderPill(x, y, width, label, value) {
  const labelFit = fitTextToWidth(label, width - 32, { size: 12, minSize: 10, weight: 700, minChars: 8 });
  const valueFit = fitTextToWidth(value, width - 32, { size: 16, minSize: 12, weight: 700, minChars: 8 });
  return [
    svgRect(x, y, width, 56, 16, PANEL_CARD_ALT, PANEL_STROKE, 1),
    svgText(labelFit.text, x + 16, y + 18, { size: labelFit.size, weight: 700, fill: PANEL_MUTED, family: FONT_DISPLAY }),
    svgText(valueFit.text, x + 16, y + 39, { size: valueFit.size, weight: 700, family: FONT_DISPLAY }),
  ].join('');
}

function renderAvatarBadge(x, y, initials, avatarDataUri = null, clipId = 'user-avatar') {
  if (avatarDataUri) {
    return [
      '<defs>',
      `<clipPath id="${clipId}">`,
      `<circle cx="${x + 32}" cy="${y + 32}" r="32" />`,
      '</clipPath>',
      '</defs>',
      svgCircle(x + 32, y + 32, 32, PANEL_BLUE),
      `<image href="${avatarDataUri}" x="${x}" y="${y}" width="64" height="64" preserveAspectRatio="xMidYMid slice" clip-path="url(#${clipId})" />`,
      svgCircle(x + 32, y + 32, 32, 'none', PANEL_STROKE, 1.5),
    ].join('');
  }

  return [
    svgCircle(x + 32, y + 32, 32, PANEL_BLUE),
    svgText(initials, x + 32, y + 34, {
      size: 24,
      weight: 800,
      fill: PANEL_BG,
      anchor: 'middle',
      family: FONT_DISPLAY,
      style: 'italic',
    }),
  ].join('');
}

function renderLeaderboardSection({ x, y, width, title, chipType, chipColor, rows, valueLabel }) {
  const titleFit = fitTextToWidth(title, width - 92, { size: 28, minSize: 19, weight: 700, letterSpacing: 0.2, minChars: 8 });
  const sectionParts = [
    svgRect(x, y, width, 260, 24, PANEL_CARD, PANEL_STROKE, 1.2),
    renderIconChip(x + 18, y + 14, chipType, chipColor, 38, 28),
    svgText(titleFit.text, x + 66, y + 29, { size: titleFit.size, weight: 700, family: FONT_DISPLAY, letterSpacing: 0.2 }),
  ];

  const rowWidth = width - 32;
  const rowHeight = 50;
  const startY = y + 66;

  if (!rows.length) {
    sectionParts.push(svgRect(x + 16, startY, rowWidth, rowHeight, 12, PANEL_ROW));
    sectionParts.push(svgText('No tracked activity yet', x + 36, startY + rowHeight / 2, {
      size: 20,
      weight: 700,
      fill: PANEL_MUTED,
    }));
    return sectionParts.join('');
  }

  rows.slice(0, 3).forEach((row, index) => {
    const rowY = startY + index * 62;
    const nameWidth = width - 220;
    const valueText = String(row.value || '').trim();
    const valueBoxWidth = Math.max(118, Math.min(180, Math.round(estimateTextWidth(valueText, 20, 800) + 28)));
    const nameFit = fitTextToWidth(row.name, Math.max(100, nameWidth - valueBoxWidth), { size: 23, minSize: 16, weight: 600, minChars: 8 });
    const valueFit = fitTextToWidth(valueText, valueBoxWidth - 20, { size: 20, minSize: 14, weight: 800, minChars: 5 });
    sectionParts.push(svgRect(x + 16, rowY, rowWidth, rowHeight, 12, PANEL_ROW));
    sectionParts.push(svgRect(x + 28, rowY + 7, 42, 34, 10, PANEL_BG_ACCENT));
    sectionParts.push(svgText(String(index + 1), x + 49, rowY + 24, {
      size: 19,
      weight: 800,
      anchor: 'middle',
      family: FONT_DISPLAY,
    }));
    sectionParts.push(svgText(nameFit.text, x + 92, rowY + 24, {
      size: nameFit.size,
      weight: 600,
      family: FONT_DISPLAY,
      style: 'italic',
    }));
    sectionParts.push(svgRect(x + width - valueBoxWidth - 24, rowY + 8, valueBoxWidth, 32, 10, PANEL_CARD_ALT));
    sectionParts.push(svgText(valueFit.text, x + width - 24 - valueBoxWidth / 2, rowY + 24, {
      size: valueFit.size,
      weight: 800,
      anchor: 'middle',
      family: FONT_DISPLAY,
    }));
  });

  sectionParts.push(svgText(valueLabel, x + width - 24, y + 33, {
    size: 15,
    weight: 700,
    fill: PANEL_MUTED,
    anchor: 'end',
    family: FONT_DISPLAY,
  }));

  return sectionParts.join('');
}

function renderLineChartCard({ x, y, width, height, title, subtitle, labels, datasets, yAxisLabel = null, tickFormatter = null }) {
  const titleFit = fitTextToWidth(title, width * 0.36, { size: 28, minSize: 18, weight: 700, letterSpacing: 0.2, minChars: 8 });
  const parts = [
    svgRect(x, y, width, height, 24, PANEL_CARD, PANEL_STROKE, 1.2),
    svgText(titleFit.text, x + 20, y + 28, { size: titleFit.size, weight: 700, family: FONT_DISPLAY, letterSpacing: 0.2 }),
  ];

  if (subtitle) {
    parts.push(svgText(subtitle, x + 20, y + 56, { size: 16, weight: 600, fill: PANEL_MUTED, family: FONT_DISPLAY, style: 'italic' }));
  }

  let legendX = x + width - 28;
  [...datasets].reverse().forEach(dataset => {
    const labelWidth = Math.max(90, Math.min(170, Math.round(estimateTextWidth(dataset.label, 17, 700) + 28)));
    const labelFit = fitTextToWidth(dataset.label, labelWidth - 24, { size: 17, minSize: 12, weight: 700, minChars: 6 });
    legendX -= labelWidth;
    parts.push(svgCircle(legendX, y + 29, 8, dataset.color));
    parts.push(svgText(labelFit.text, legendX + 22, y + 30, { size: labelFit.size, weight: 700, fill: PANEL_MUTED, family: FONT_DISPLAY }));
    legendX -= 40;
  });

  const showYAxis = Boolean(yAxisLabel);
  const plotX = x + (showYAxis ? 84 : 24);
  const plotY = y + 78;
  const plotWidth = width - (showYAxis ? 108 : 48);
  const plotHeight = height - 116;
  const combinedMax = Math.max(
    1,
    ...datasets.flatMap(dataset => dataset.values.map(value => Number(value || 0))),
  );

  for (let step = 0; step <= 4; step += 1) {
    const lineY = plotY + (plotHeight * step) / 4;
    parts.push(svgLine(plotX, lineY, plotX + plotWidth, lineY, PANEL_STROKE, 1, 0.55, '6 10'));
  }

  if (showYAxis) {
    parts.push(svgLine(plotX, plotY, plotX, plotY + plotHeight, PANEL_STROKE, 1.2));

    for (let step = 0; step <= 4; step += 1) {
      const value = (combinedMax * (4 - step)) / 4;
      const lineY = plotY + (plotHeight * step) / 4;
      const label = typeof tickFormatter === 'function'
        ? tickFormatter(value)
        : Number.isInteger(value)
          ? String(value)
          : value.toFixed(1);

      parts.push(svgText(label, plotX - 12, lineY, {
        size: 14,
        weight: 700,
        fill: PANEL_SUBTLE,
        anchor: 'end',
        family: FONT_DISPLAY,
      }));
    }
  }

  const hasData = datasets.some(dataset => dataset.values.some(value => Number(value) > 0));
  if (!labels.length || !hasData) {
    parts.push(svgText('No tracked data for this range yet', x + width / 2, y + height / 2 + 10, {
      size: 24,
      weight: 700,
      fill: PANEL_MUTED,
      anchor: 'middle',
    }));
    return parts.join('');
  }

  datasets.forEach(dataset => {
    const ownMax = Math.max(1, ...dataset.values.map(value => Number(value || 0)));
    const scaleMax = dataset.normalize ? ownMax : combinedMax;
    const points = dataset.values.map((value, index) => {
      const progress = labels.length === 1 ? 0.5 : index / (labels.length - 1);
      const xPos = plotX + progress * plotWidth;
      const yPos = plotY + plotHeight - (Number(value || 0) / scaleMax) * plotHeight;
      return {
        x: Number(xPos.toFixed(2)),
        y: Number(yPos.toFixed(2)),
      };
    });

    parts.push(`<polyline fill="none" stroke="${dataset.color}" stroke-width="5" stroke-linecap="round" stroke-linejoin="round" points="${buildPolylinePoints(points)}" />`);

    const lastPoint = points[points.length - 1];
    if (lastPoint) {
      parts.push(svgCircle(lastPoint.x, lastPoint.y, 5.5, dataset.color));
    }
  });

  getTickIndices(labels.length, 5).forEach(index => {
    const progress = labels.length === 1 ? 0.5 : index / (labels.length - 1);
    const xPos = plotX + progress * plotWidth;
    parts.push(svgText(labels[index], xPos, y + height - 24, {
      size: 15,
      weight: 700,
      fill: PANEL_SUBTLE,
      anchor: index === 0 ? 'start' : index === labels.length - 1 ? 'end' : 'middle',
      family: FONT_DISPLAY,
    }));
  });

  return parts.join('');
}

function renderHorizontalBarCard({ x, y, width, height, title, subtitle, rows, color, valueLabel }) {
  const titleFit = fitTextToWidth(title, width * 0.42, { size: 28, minSize: 18, weight: 700, letterSpacing: 0.2, minChars: 8 });
  const parts = [
    svgRect(x, y, width, height, 24, PANEL_CARD, PANEL_STROKE, 1.2),
    svgText(titleFit.text, x + 20, y + 28, { size: titleFit.size, weight: 700, family: FONT_DISPLAY, letterSpacing: 0.2 }),
  ];

  if (subtitle) {
    parts.push(svgText(subtitle, x + 20, y + 56, { size: 16, weight: 600, fill: PANEL_MUTED, family: FONT_DISPLAY, style: 'italic' }));
  }

  if (!rows.length) {
    parts.push(svgText('No tracked data for this range yet', x + width / 2, y + height / 2, {
      size: 24,
      weight: 700,
      fill: PANEL_MUTED,
      anchor: 'middle',
    }));
    return parts.join('');
  }

  const maxValue = Math.max(1, ...rows.map(row => Number(row.numericValue || 0)));
  const barAreaX = x + 210;
  const barAreaWidth = width - 300;
  const rowHeight = 52;
  const rowGap = 14;
  const startY = y + 84;

  parts.push(svgText(valueLabel, x + width - 28, y + 30, {
    size: 15,
    weight: 700,
    fill: PANEL_MUTED,
    anchor: 'end',
    family: FONT_DISPLAY,
  }));

  rows.slice(0, 8).forEach((row, index) => {
    const rowY = startY + index * (rowHeight + rowGap);
    const labelFit = fitTextToWidth(`${index + 1}. ${row.name}`, 170, { size: 20, minSize: 13, weight: 700, minChars: 8 });
    const valueFit = fitTextToWidth(row.value, 118, { size: 19, minSize: 13, weight: 800, minChars: 5 });
    parts.push(svgText(labelFit.text, x + 22, rowY + rowHeight / 2, {
      size: labelFit.size,
      weight: 700,
      family: FONT_DISPLAY,
      style: 'italic',
    }));
    parts.push(svgRect(barAreaX, rowY + 11, barAreaWidth, 30, 10, PANEL_ROW));
    parts.push(svgRect(barAreaX, rowY + 11, Math.max(26, (barAreaWidth * Number(row.numericValue || 0)) / maxValue), 30, 10, color));
    parts.push(svgText(valueFit.text, x + width - 24, rowY + rowHeight / 2, {
      size: valueFit.size,
      weight: 800,
      anchor: 'end',
      family: FONT_DISPLAY,
    }));
  });

  return parts.join('');
}

function renderMetricPill(x, y, width, label, value, accent = PANEL_BLUE) {
  const labelFit = fitTextToWidth(label, width - 58, { size: 13, minSize: 11, weight: 700, minChars: 8 });
  const preferredValueSize = String(value || '').length > 18 ? 21 : 24;
  const valueFit = fitTextToWidth(value, width - 58, { size: preferredValueSize, minSize: 13, weight: 800, minChars: 5 });
  return [
    svgRect(x, y, width, 74, 18, PANEL_CARD_ALT, PANEL_STROKE, 1),
    svgRect(x + 14, y + 14, 6, 46, 3, accent),
    svgText(labelFit.text, x + 32, y + 24, { size: labelFit.size, weight: 700, fill: PANEL_MUTED, family: FONT_DISPLAY }),
    svgText(valueFit.text, x + 32, y + 48, { size: valueFit.size, weight: 800, family: FONT_DISPLAY }),
  ].join('');
}

function renderKeyValueCard({
  x,
  y,
  width,
  height,
  title,
  subtitle = null,
  chipType = 'messages',
  chipColor = PANEL_BLUE,
  rows = [],
  rowHeight = 38,
}) {
  const titleFit = fitTextToWidth(title, width - 92, {
    size: 26,
    minSize: 18,
    weight: 700,
    letterSpacing: 0.2,
    minChars: 8,
  });
  const parts = [
    svgRect(x, y, width, height, 24, PANEL_CARD, PANEL_STROKE, 1.2),
    renderIconChip(x + 18, y + 14, chipType, chipColor, 38, 28),
    svgText(titleFit.text, x + 66, y + 29, { size: titleFit.size, weight: 700, family: FONT_DISPLAY, letterSpacing: 0.2 }),
  ];

  if (subtitle) {
    parts.push(svgText(subtitle, x + 20, y + 56, {
      size: 15,
      weight: 600,
      fill: PANEL_MUTED,
      family: FONT_DISPLAY,
      style: 'italic',
    }));
  }

  const startY = y + (subtitle ? 74 : 58);
  const rowGap = 10;
  const maxRows = Math.max(1, Math.floor((height - (startY - y) - 16) / (rowHeight + rowGap)));

  rows.slice(0, maxRows).forEach((row, index) => {
    const rowY = startY + index * (rowHeight + rowGap);
    const rowInnerWidth = width - 28;
    const labelWidth = Math.max(84, Math.min(108, Math.round(rowInnerWidth * 0.32)));
    const valueWidth = Math.max(108, rowInnerWidth - labelWidth - 40);
    const labelFit = fitTextToWidth(row.label, labelWidth, { size: 16, minSize: 12, weight: 700, minChars: 6 });
    const valueFit = fitTextToWidth(row.value, valueWidth, { size: 16, minSize: 11, weight: 800, minChars: 6 });
    parts.push(svgRect(x + 14, rowY, width - 28, rowHeight, 10, PANEL_ROW));
    parts.push(svgText(labelFit.text, x + 26, rowY + rowHeight / 2, {
      size: labelFit.size,
      weight: 700,
      fill: PANEL_MUTED,
      family: FONT_DISPLAY,
      style: 'italic',
    }));
    parts.push(svgText(valueFit.text, x + width - 24, rowY + rowHeight / 2, {
      size: valueFit.size,
      weight: 800,
      anchor: 'end',
      family: FONT_DISPLAY,
    }));
  });

  return parts.join('');
}

function renderListRowsCard({
  x,
  y,
  width,
  height,
  title,
  subtitle = null,
  chipType = 'messages',
  chipColor = PANEL_BLUE,
  rows = [],
  rowHeight = 56,
}) {
  const titleFit = fitTextToWidth(title, width - 92, {
    size: 26,
    minSize: 18,
    weight: 700,
    letterSpacing: 0.2,
    minChars: 8,
  });
  const parts = [
    svgRect(x, y, width, height, 24, PANEL_CARD, PANEL_STROKE, 1.2),
    renderIconChip(x + 18, y + 14, chipType, chipColor, 38, 28),
    svgText(titleFit.text, x + 66, y + 29, { size: titleFit.size, weight: 700, family: FONT_DISPLAY, letterSpacing: 0.2 }),
  ];

  if (subtitle) {
    parts.push(svgText(subtitle, x + 20, y + 56, {
      size: 15,
      weight: 600,
      fill: PANEL_MUTED,
      family: FONT_DISPLAY,
      style: 'italic',
    }));
  }

  const startY = y + (subtitle ? 74 : 58);
  const rowGap = 10;
  const maxRows = Math.max(1, Math.floor((height - (startY - y) - 16) / (rowHeight + rowGap)));

  rows.slice(0, maxRows).forEach((row, index) => {
    const rowY = startY + index * (rowHeight + rowGap);
    const valueText = String(row.value || '').trim();
    const innerWidth = width - 28;
    const valueBoxWidth = valueText ? Math.max(104, Math.min(164, Math.round(estimateTextWidth(valueText, 14, 800) + 30))) : 0;
    const tertiaryWidth = row.tertiary
      ? Math.max(132, Math.min(250, Math.round(innerWidth * (rowHeight >= 84 ? 0.34 : 0.24))))
      : 0;
    const valueBoxLeft = valueText ? x + width - valueBoxWidth - 22 : x + width - 24;
    const textLeft = x + 28;
    const tertiaryRight = valueBoxLeft - 14;
    const tertiaryLeft = tertiaryWidth ? tertiaryRight - tertiaryWidth : tertiaryRight;
    const textRight = tertiaryWidth
      ? tertiaryLeft - 18
      : valueText
        ? valueBoxLeft - 18
        : x + width - 24;
    const textWidth = Math.max(84, textRight - textLeft);
    const primaryFit = fitTextToWidth(row.primary, textWidth, { size: 16, minSize: 12, weight: 800, minChars: 8 });
    const secondaryBlock = row.secondary
      ? wrapTextToLines(row.secondary, textWidth, rowHeight >= 84 ? 2 : 1, { size: 13, minSize: 10, weight: 700, minChars: 12 })
      : null;
    const valueFit = valueText ? fitTextToWidth(valueText, valueBoxWidth - 22, { size: 14, minSize: 11, weight: 800, minChars: 5 }) : null;
    const tertiaryFit = row.tertiary
      ? fitTextToWidth(row.tertiary, Math.max(96, tertiaryRight - tertiaryLeft), { size: 12, minSize: 10, weight: 700, minChars: 8 })
      : null;
    parts.push(svgRect(x + 14, rowY, width - 28, rowHeight, 12, PANEL_ROW));
    parts.push(svgText(primaryFit.text, x + 28, row.secondary ? rowY + 18 : rowY + (rowHeight / 2), {
      size: primaryFit.size,
      weight: 800,
      family: FONT_DISPLAY,
      style: 'italic',
    }));

    if (secondaryBlock) {
      const secondaryStartY = rowHeight >= 84 ? rowY + 38 : rowY + 40;
      const lineGap = secondaryBlock.size + (rowHeight >= 84 ? 3 : 0);
      secondaryBlock.lines.forEach((line, lineIndex) => {
        parts.push(svgText(line, x + 28, secondaryStartY + (lineIndex * lineGap), {
          size: secondaryBlock.size,
          weight: 700,
          fill: PANEL_MUTED,
          family: FONT_DISPLAY,
        }));
      });
    }

    if (valueText && valueFit) {
      parts.push(svgRect(valueBoxLeft, rowY + 12, valueBoxWidth, rowHeight - 24, 10, PANEL_CARD_ALT));
      parts.push(svgText(valueFit.text, valueBoxLeft + (valueBoxWidth / 2), rowY + rowHeight / 2, {
        size: valueFit.size,
        weight: 800,
        anchor: 'middle',
        family: FONT_DISPLAY,
      }));
    }

    if (tertiaryFit) {
      parts.push(svgText(tertiaryFit.text, tertiaryRight, row.secondary ? rowY + 40 : rowY + rowHeight / 2, {
        size: tertiaryFit.size,
        weight: 700,
        fill: PANEL_SUBTLE,
        anchor: 'end',
        family: FONT_DISPLAY,
      }));
    }
  });

  return parts.join('');
}

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
    this.avatarCache = new Map();
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
          { label: 'Voice Activity', data: voiceHours, borderColor: '#34d399', yAxisID: 'y', fill: false, tension: 0.3 },
          { label: 'SC Activity', data: playtimeHours, borderColor: '#f59e0b', yAxisID: 'y', fill: false, tension: 0.3 },
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
        color: PANEL_GREEN,
        fillColor: 'rgba(67, 181, 129, 0.18)',
        getLeaderboardValue: row => Number(row.messages || 0),
        getDailyValue: day => Number(day.messages || 0),
        formatValue: value => `${formatNumber(value)} message${Number(value) === 1 ? '' : 's'}`,
      },
      voice: {
        label: 'Voice Activity',
        axisTitle: 'Hours',
        color: PANEL_PINK,
        fillColor: 'rgba(240, 98, 146, 0.18)',
        getLeaderboardValue: row => Number((Number(row.voiceSeconds || 0) / 3600).toFixed(2)),
        getDailyValue: day => Number(day.voiceHours || 0),
        formatValue: value => `${Number(value || 0).toFixed(1)} hours`,
      },
      starCitizen: {
        label: 'SC Activity',
        axisTitle: 'Hours',
        color: PANEL_CYAN,
        fillColor: 'rgba(34, 211, 238, 0.18)',
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
            label: 'Overview',
            value: 'overview',
            description: 'Return to the default overview card.',
            default: activeCategory === 'overview',
          },
          {
            label: 'Players Graph',
            value: 'players',
            description: 'Show the peak player chart for the range.',
            default: activeCategory === 'players',
          },
        ]
      : [
          {
            label: 'Overview',
            value: 'overview',
            description: panel === 'top' ? 'Show the leaderboard overview cards.' : 'Return to the default overview card.',
            default: activeCategory === 'overview',
          },
          {
            label: 'Messages Graph',
            value: 'messages',
            description: panel === 'top' ? 'Compare top users by messages.' : 'Show this member messages.',
            default: activeCategory === 'messages',
          },
          {
            label: 'Voice Activity Graph',
            value: 'voice',
            description: panel === 'top' ? 'Compare top users by voice activity.' : 'Show this member voice activity.',
            default: activeCategory === 'voice',
          },
          {
            label: 'SC Activity Graph',
            value: 'starCitizen',
            description: panel === 'top' ? 'Compare top users by SC activity.' : 'Show this member SC activity.',
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
            backgroundColor: color || CHART_DATA,
            borderColor: color || CHART_DATA,
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
          title: { display: false },
        },
        scales: {
          x: {
            beginAtZero: true,
            grid: { color: CHART_GRID },
            ticks: {
              color: CHART_AXIS,
              font: { size: 16, weight: 'bold' },
            },
            title: {
              display: true,
              text: axisTitle,
              color: CHART_AXIS,
              font: { size: 18, weight: 'bold' },
            },
          },
          y: {
            grid: { display: false },
            ticks: {
              color: CHART_LABELS,
              font: { size: 17, weight: 'bold' },
            },
          },
        },
      },
    };

    return `https://quickchart.io/chart?width=1200&height=560&devicePixelRatio=2&backgroundColor=${encodeURIComponent(CHART_BG)}&version=4&c=${encodeURIComponent(JSON.stringify(config))}`;
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
            borderColor: color || CHART_DATA,
            backgroundColor: fillColor || CHART_FILL,
            fill: true,
            pointBackgroundColor: color || CHART_DATA,
            pointBorderColor: color || CHART_DATA,
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
          title: { display: false },
        },
        scales: {
          x: {
            grid: { color: CHART_GRID },
            ticks: {
              color: CHART_LABELS,
              font: { size: 16, weight: 'bold' },
              maxRotation: 0,
            },
          },
          y: {
            beginAtZero: true,
            grid: { color: CHART_GRID },
            ticks: {
              color: CHART_AXIS,
              font: { size: 16, weight: 'bold' },
            },
            title: {
              display: true,
              text: axisTitle,
              color: CHART_AXIS,
              font: { size: 18, weight: 'bold' },
            },
          },
        },
      },
    };

    return `https://quickchart.io/chart?width=1200&height=560&devicePixelRatio=2&backgroundColor=${encodeURIComponent(CHART_BG)}&version=4&c=${encodeURIComponent(JSON.stringify(config))}`;
  }

  buildBaseEmbed(title) {
    return new EmbedBuilder().setColor(0x2b2d31).setTitle(title).setThumbnail(THUMBNAIL_URL);
  }

  getTopSectionName(category) {
    if (category === 'messages') return 'Messages';
    if (category === 'voice') return 'Voice Activity';
    if (category === 'starCitizen') return 'SC Activity';
    return 'Activity';
  }

  getSummaryWindows() {
    return [1, 7, 14];
  }

  formatFullDate(date) {
    if (!date) return 'Unknown';
    return new Date(date).toLocaleDateString('en-GB', {
      day: 'numeric',
      month: 'long',
      year: 'numeric',
    });
  }

  getMemberContext(userId) {
    for (const guild of this.client.guilds.cache.values()) {
      const member = guild.members.cache.get(userId);
      if (!member) continue;

      const displayName = member.displayName || member.user?.username || 'Unknown User';
      const initials = displayName
        .split(/\s+/)
        .filter(Boolean)
        .slice(0, 2)
        .map(part => part.charAt(0).toUpperCase())
        .join('')
        .slice(0, 2) || displayName.slice(0, 2).toUpperCase();

      return {
        displayName,
        username: member.user?.username || displayName,
        initials,
        createdAt: member.user?.createdAt || null,
        joinedAt: member.joinedAt || null,
        avatarUrl: typeof member.displayAvatarURL === 'function'
          ? member.displayAvatarURL({ extension: 'png', forceStatic: true, size: 128 })
          : typeof member.user?.displayAvatarURL === 'function'
            ? member.user.displayAvatarURL({ extension: 'png', forceStatic: true, size: 128 })
            : null,
      };
    }

    const fallbackName = this.state.users?.[userId]?.username || 'Unknown User';
    return {
      displayName: fallbackName,
      username: fallbackName,
      initials: fallbackName.slice(0, 2).toUpperCase(),
      createdAt: null,
      joinedAt: null,
      avatarUrl: null,
    };
  }

  async getUserAvatarDataUri(userId) {
    const cached = this.avatarCache.get(userId);
    if (cached && cached.expiresAt > now()) return cached.dataUri;

    const context = this.getMemberContext(userId);
    if (!context.avatarUrl) return null;
    if (typeof fetch !== 'function') return null;

    try {
      const response = await fetch(context.avatarUrl);
      if (!response.ok) return null;

      const contentType = response.headers.get('content-type') || 'image/png';
      const buffer = Buffer.from(await response.arrayBuffer());
      const dataUri = `data:${contentType};base64,${buffer.toString('base64')}`;
      this.avatarCache.set(userId, {
        dataUri,
        expiresAt: now() + 6 * 60 * 60 * 1000,
      });
      return dataUri;
    } catch (error) {
      console.warn(`Could not load avatar for ${userId}:`, error.message);
      return null;
    }
  }

  buildPanelSvg(width, height, body) {
    const renderWidth = Math.round(width * PANEL_RENDER_SCALE);
    const renderHeight = Math.round(height * PANEL_RENDER_SCALE);
    const frameX = PANEL_FRAME_MARGIN;
    const frameY = PANEL_FRAME_MARGIN;
    const frameWidth = width - PANEL_FRAME_MARGIN * 2;
    const frameHeight = height - PANEL_FRAME_MARGIN * 2;

    return [
      `<svg xmlns="http://www.w3.org/2000/svg" width="${renderWidth}" height="${renderHeight}" viewBox="0 0 ${width} ${height}">`,
      '<defs>',
      '<linearGradient id="panelBg" x1="0" y1="0" x2="1" y2="1">',
      `<stop offset="0%" stop-color="${PANEL_BG_ACCENT}" />`,
      `<stop offset="100%" stop-color="${PANEL_BG}" />`,
      '</linearGradient>',
      '<filter id="panelShadow" x="-10%" y="-10%" width="120%" height="120%">',
      '<feDropShadow dx="0" dy="14" stdDeviation="14" flood-color="#000000" flood-opacity="0.28" />',
      '</filter>',
      '</defs>',
      `<rect x="${frameX}" y="${frameY}" width="${frameWidth}" height="${frameHeight}" rx="${PANEL_FRAME_RADIUS}" fill="url(#panelBg)" stroke="${PANEL_STROKE}" stroke-width="1.6" filter="url(#panelShadow)" />`,
      `<rect x="${frameX + 1}" y="${frameY + 1}" width="${frameWidth - 2}" height="${frameHeight - 2}" rx="${PANEL_FRAME_RADIUS - 1}" fill="none" stroke="rgba(255,255,255,0.04)" stroke-width="1" />`,
      body,
      '</svg>',
    ].join('');
  }

  renderSvgAttachment(svg, name) {
    const ResvgConstructor = getResvgConstructor();
    if (!ResvgConstructor) return null;

    const resvg = new ResvgConstructor(svg, {
      fitTo: { mode: 'width', value: PANEL_RENDER_WIDTH },
      font: {
        loadSystemFonts: true,
        defaultFontFamily: 'DejaVu Sans',
      },
    });

    return new AttachmentBuilder(resvg.render().asPng(), { name });
  }

  buildImagePanelResponse({ title, svg, attachmentName, components, content, footer, fallbackPayload }) {
    try {
      const attachment = this.renderSvgAttachment(svg, attachmentName);
      if (!attachment) {
        return typeof fallbackPayload === 'function'
          ? fallbackPayload()
          : {
              embeds: [
                new EmbedBuilder()
                  .setColor(0x2b2d31)
                  .setTitle(title || 'Stats')
                  .setDescription('Rendered stats cards are unavailable right now.'),
              ],
              components,
              attachments: [],
              ...(typeof content === 'string' ? { content } : {}),
            };
      }

      const embed = new EmbedBuilder()
        .setColor(0x2b2d31)
        .setImage(`attachment://${attachmentName}`);

      const payload = {
        embeds: [embed],
        components,
        files: [attachment],
        attachments: [],
      };

      if (typeof content === 'string') payload.content = content;
      return payload;
    } catch (error) {
      console.error('Failed to render stats card:', error);
      if (typeof fallbackPayload === 'function') return fallbackPayload();

      const fallback = new EmbedBuilder()
        .setColor(0x2b2d31)
        .setTitle(title || 'Stats')
        .setDescription('The stats card could not be rendered right now.');

      const payload = { embeds: [fallback], components, attachments: [] };
      if (typeof content === 'string') payload.content = content;
      return payload;
    }
  }

  formatTopValue(row, category) {
    if (category === 'messages') return formatNumber(row.messages);
    if (category === 'voice') return formatHoursShort(row.voiceSeconds);
    if (category === 'starCitizen') return formatHoursShort(row.starCitizenSeconds);
    return '0';
  }

  getPanelVisuals(category) {
    if (category === 'messages') return { title: 'Messages', chipType: 'messages', chipColor: PANEL_GREEN, valueLabel: 'Messages' };
    if (category === 'voice') return { title: 'Voice Activity', chipType: 'voice', chipColor: PANEL_PINK, valueLabel: 'Hours' };
    if (category === 'starCitizen') return { title: 'SC Activity', chipType: 'starCitizen', chipColor: PANEL_CYAN, valueLabel: 'Hours' };
    return { title: 'Activity', chipType: 'messages', chipColor: PANEL_BLUE, valueLabel: 'Value' };
  }

  buildTopPanelSvg(days, activeCategory, board) {
    const header = [
      svgText('Top Activity', 36, 44, { size: 44, weight: 800, family: FONT_DISPLAY, letterSpacing: 0.3 }),
      svgText(`Last ${days} Day${days === 1 ? '' : 's'} - UTC`, 36, 82, { size: 18, weight: 700, fill: PANEL_MUTED, family: FONT_DISPLAY, style: 'italic' }),
    ];

    if (activeCategory === 'overview') {
      const sections = [
        {
          y: 112,
          category: 'messages',
          rows: board.messages.slice(0, 3).map(row => ({ name: row.username, value: this.formatTopValue(row, 'messages') })),
        },
        {
          y: 394,
          category: 'voice',
          rows: board.voice.slice(0, 3).map(row => ({ name: row.username, value: this.formatTopValue(row, 'voice') })),
        },
        {
          y: 676,
          category: 'starCitizen',
          rows: board.starCitizen.slice(0, 3).map(row => ({ name: row.username, value: this.formatTopValue(row, 'starCitizen') })),
        },
      ];

      const body = sections.map(section => {
        const visuals = this.getPanelVisuals(section.category);
        return renderLeaderboardSection({
          x: 30,
          y: section.y,
          width: 1140,
          title: visuals.title,
          chipType: visuals.chipType,
          chipColor: visuals.chipColor,
          rows: section.rows,
          valueLabel: visuals.valueLabel,
        });
      }).join('');

      return this.buildPanelSvg(PANEL_WIDTH, TOP_PANEL_HEIGHT, `${header.join('')}${body}`);
    }

    const visuals = this.getPanelVisuals(activeCategory);
    const metric = this.getMetricConfig(activeCategory);
    const rows = this.getBoardRows(board, activeCategory).slice(0, 8).map(row => ({
      name: row.username,
      value: activeCategory === 'messages'
        ? formatNumber(row.messages)
        : `${Number(metric.getLeaderboardValue(row)).toFixed(1)} h`,
      numericValue: metric.getLeaderboardValue(row),
    }));

    const chart = renderHorizontalBarCard({
      x: 30,
      y: 112,
      width: 1140,
      height: 850,
      title: visuals.title,
      subtitle: `Top tracked members - Last ${days} Day${days === 1 ? '' : 's'}`,
      rows,
      color: visuals.chipColor,
      valueLabel: visuals.valueLabel,
    });

    return this.buildPanelSvg(PANEL_WIDTH, TOP_PANEL_HEIGHT, `${header.join('')}${chart}`);
  }

  async buildUserPanelSvg(userId, days, activeCategory, stats, rankings) {
    const context = this.getMemberContext(userId);
    const avatarDataUri = await this.getUserAvatarDataUri(userId);
    const summaryWindows = this.getSummaryWindows();
    const messageRows = summaryWindows.map(windowDays => {
      const windowStats = this.getUserStats(userId, windowDays);
      return {
        label: `${windowDays}d`,
        value: formatMetricValue('messages', windowStats?.totals.messages || 0, 0),
      };
    });
    const voiceRows = summaryWindows.map(windowDays => {
      const windowStats = this.getUserStats(userId, windowDays);
      return {
        label: `${windowDays}d`,
        value: formatMetricValue('voiceSeconds', windowStats?.totals.voiceSeconds || 0),
      };
    });
    const starCitizenRows = summaryWindows.map(windowDays => {
      const windowStats = this.getUserStats(userId, windowDays);
      return {
        label: `${windowDays}d`,
        value: formatMetricValue('starCitizenSeconds', windowStats?.totals.starCitizenSeconds || 0),
      };
    });
    const rankRows = [
      { label: 'Message', value: `#${rankings.messages || '-'}` },
      { label: 'Voice', value: `#${rankings.voice || '-'}` },
      { label: 'SC', value: `#${rankings.starCitizen || '-'}` },
    ];

    const datasets = activeCategory === 'overview'
      ? [
          { label: 'Message', color: PANEL_GREEN, values: stats.daily.map(day => day.messages), normalize: true },
          { label: 'Voice', color: PANEL_PINK, values: stats.daily.map(day => day.voiceHours), normalize: true },
          { label: 'SC Activity', color: PANEL_CYAN, values: stats.daily.map(day => day.starCitizenHours), normalize: true },
        ]
      : [{
          label: this.getTopSectionName(activeCategory),
          color: this.getPanelVisuals(activeCategory).chipColor,
          values: stats.daily.map(day => this.getMetricConfig(activeCategory).getDailyValue(day)),
          normalize: false,
        }];

    const metricConfig = activeCategory !== 'overview' ? this.getMetricConfig(activeCategory) : null;
    const parts = [
      renderAvatarBadge(48, 34, context.initials, avatarDataUri, `avatar-${userId}`),
      svgText(context.displayName, 128, 54, { size: 46, weight: 800, family: FONT_DISPLAY, style: 'italic', letterSpacing: 0.2 }),
      svgText(`Tracked stats profile - Last ${days} Day${days === 1 ? '' : 's'}`, 128, 92, {
        size: 18,
        weight: 700,
        fill: PANEL_MUTED,
        family: FONT_DISPLAY,
        style: 'italic',
      }),
      renderHeaderPill(776, 36, 184, 'Created On', this.formatFullDate(context.createdAt)),
      renderHeaderPill(968, 36, 184, 'Joined On', this.formatFullDate(context.joinedAt)),
      renderDataRowsCard({ x: 48, y: 128, width: 264, height: 160, title: 'Server Ranks', chipType: 'leaderboard', chipColor: PANEL_GOLD, rows: rankRows }),
      renderDataRowsCard({ x: 328, y: 128, width: 264, height: 160, title: 'Messages', chipType: 'messages', chipColor: PANEL_GREEN, rows: messageRows }),
      renderDataRowsCard({ x: 608, y: 128, width: 264, height: 160, title: 'Voice Activity', chipType: 'voice', chipColor: PANEL_PINK, rows: voiceRows }),
      renderDataRowsCard({ x: 888, y: 128, width: 264, height: 160, title: 'SC Activity', chipType: 'starCitizen', chipColor: PANEL_CYAN, rows: starCitizenRows }),
      renderLineChartCard({
        x: 48,
        y: 324,
        width: 1104,
        height: 448,
        title: 'Charts',
        subtitle: `Last ${days} Day${days === 1 ? '' : 's'}`,
        labels: stats.daily.map(day => day.label),
        datasets,
        yAxisLabel: metricConfig ? metricConfig.axisTitle : null,
        tickFormatter: metricConfig?.axisTitle === 'Hours'
          ? value => Number(value || 0).toFixed(1)
          : value => formatNumber(Math.round(Number(value || 0))),
      }),
    ];

    return this.buildPanelSvg(PANEL_WIDTH, PANEL_HEIGHT, parts.join(''));
  }

  buildServerPanelSvg(days, activeCategory, stats) {
    const summaryWindows = this.getSummaryWindows();
    const messageRows = summaryWindows.map(windowDays => {
      const windowStats = this.getServerStats(windowDays);
      return { label: `${windowDays}d`, value: formatMetricValue('messages', windowStats.totals.messages, 0) };
    });
    const voiceRows = summaryWindows.map(windowDays => {
      const windowStats = this.getServerStats(windowDays);
      return { label: `${windowDays}d`, value: formatMetricValue('voiceSeconds', windowStats.totals.voiceSeconds) };
    });
    const starCitizenRows = summaryWindows.map(windowDays => {
      const windowStats = this.getServerStats(windowDays);
      return { label: `${windowDays}d`, value: formatMetricValue('starCitizenSeconds', windowStats.totals.starCitizenSeconds) };
    });

    const datasets = activeCategory === 'overview'
      ? [
          { label: 'Message', color: PANEL_GREEN, values: stats.daily.map(day => day.messages), normalize: true },
          { label: 'Voice', color: PANEL_PINK, values: stats.daily.map(day => day.voiceHours), normalize: true },
          { label: 'SC Activity', color: PANEL_CYAN, values: stats.daily.map(day => day.starCitizenHours), normalize: true },
        ]
      : [{
          label: this.getTopSectionName(activeCategory),
          color: this.getPanelVisuals(activeCategory).chipColor,
          values: stats.daily.map(day => this.getMetricConfig(activeCategory).getDailyValue(day)),
          normalize: false,
        }];

    const metricConfig = activeCategory !== 'overview' ? this.getMetricConfig(activeCategory) : null;
    const parts = [
      svgText('Server Activity', 48, 52, { size: 46, weight: 800, family: FONT_DISPLAY, letterSpacing: 0.2 }),
      svgText(`Tracked totals - Last ${days} Day${days === 1 ? '' : 's'}`, 48, 92, {
        size: 18,
        weight: 700,
        fill: PANEL_MUTED,
        family: FONT_DISPLAY,
        style: 'italic',
      }),
      renderHeaderPill(950, 36, 202, 'Timezone', 'UTC'),
      renderDataRowsCard({ x: 48, y: 128, width: 352, height: 160, title: 'Messages', chipType: 'messages', chipColor: PANEL_GREEN, rows: messageRows }),
      renderDataRowsCard({ x: 424, y: 128, width: 352, height: 160, title: 'Voice Activity', chipType: 'voice', chipColor: PANEL_PINK, rows: voiceRows }),
      renderDataRowsCard({ x: 800, y: 128, width: 352, height: 160, title: 'SC Activity', chipType: 'starCitizen', chipColor: PANEL_CYAN, rows: starCitizenRows }),
      renderLineChartCard({
        x: 48,
        y: 324,
        width: 1104,
        height: 448,
        title: 'Charts',
        subtitle: `Last ${days} Day${days === 1 ? '' : 's'}`,
        labels: stats.daily.map(day => day.label),
        datasets,
        yAxisLabel: metricConfig ? metricConfig.axisTitle : null,
        tickFormatter: metricConfig?.axisTitle === 'Hours'
          ? value => Number(value || 0).toFixed(1)
          : value => formatNumber(Math.round(Number(value || 0))),
      }),
    ];

    return this.buildPanelSvg(PANEL_WIDTH, PANEL_HEIGHT, parts.join(''));
  }

  buildPlayersPanelSvg(guild, days, activeCategory, current, peak, labels, playerSeries) {
    const guildName = guild?.name || 'Server';
    const playerRows = current.players.length
      ? current.players.slice(0, 8).map(player => ({
          primary: player.name,
          secondary: 'Currently online in Star Citizen',
        }))
      : [{
          primary: 'No tracked players online',
          secondary: 'No one is currently detected in Star Citizen',
        }];

    const header = [
      svgText('Players', PANEL_CONTENT_X, 52, { size: 46, weight: 800, family: FONT_DISPLAY, letterSpacing: 0.2 }),
      svgText(`${guildName} - Last ${days} Day${days === 1 ? '' : 's'}`, PANEL_CONTENT_X, 92, {
        size: 18,
        weight: 700,
        fill: PANEL_MUTED,
        family: FONT_DISPLAY,
        style: 'italic',
      }),
      renderMetricPill(PANEL_CONTENT_X, 118, PANEL_PILL_WIDTH, 'Live Now', formatNumber(current.count), PANEL_BLUE),
      renderMetricPill(PANEL_CONTENT_X + PANEL_PILL_WIDTH + PANEL_PILL_GAP, 118, PANEL_PILL_WIDTH, 'Peak', formatNumber(peak.count || 0), '#a78bfa'),
      renderMetricPill(PANEL_CONTENT_X + ((PANEL_PILL_WIDTH + PANEL_PILL_GAP) * 2), 118, PANEL_PILL_WIDTH, 'Peak Time', peak.ts ? new Date(peak.ts).toLocaleDateString('en-GB') : 'No data', PANEL_GOLD),
      renderMetricPill(PANEL_CONTENT_X + ((PANEL_PILL_WIDTH + PANEL_PILL_GAP) * 3), 118, PANEL_PILL_WIDE_WIDTH, 'Range', `${days} day${days === 1 ? '' : 's'}`, PANEL_CYAN),
    ];

    const datasets = [{
      label: 'Players',
      color: '#a78bfa',
      values: playerSeries,
      normalize: false,
    }];

    if (activeCategory === 'players') {
      const body = [
        renderLineChartCard({
          x: PANEL_CONTENT_X,
          y: 214,
          width: PANEL_CONTENT_WIDTH,
          height: 500,
          title: 'Peak Trend',
          subtitle: `Tracked player peaks across the last ${days} day${days === 1 ? '' : 's'}`,
          labels,
          datasets,
          yAxisLabel: 'Players',
          tickFormatter: value => formatNumber(Math.round(Number(value || 0))),
        }),
        renderListRowsCard({
          x: PANEL_CONTENT_X,
          y: 730,
          width: PANEL_CONTENT_WIDTH,
          height: 174,
          title: 'Online Now',
          subtitle: 'Compact live list',
          chipType: 'players',
          chipColor: PANEL_BLUE,
          rows: playerRows.slice(0, 3),
          rowHeight: 46,
        }),
      ].join('');

      return this.buildPanelSvg(PANEL_WIDTH, TOP_PANEL_HEIGHT, `${header.join('')}${body}`);
    }

    const body = [
      renderListRowsCard({
        x: PANEL_CONTENT_X,
        y: 214,
        width: 428,
        height: 736,
        title: 'Online Now',
        subtitle: 'Compact live list',
        chipType: 'players',
        chipColor: PANEL_BLUE,
        rows: playerRows,
        rowHeight: 58,
      }),
      renderLineChartCard({
        x: PANEL_CONTENT_X + 452,
        y: 214,
        width: 668,
        height: 736,
        title: 'Peak Trend',
        subtitle: `Tracked player peaks across the last ${days} day${days === 1 ? '' : 's'}`,
        labels,
        datasets,
        yAxisLabel: 'Players',
        tickFormatter: value => formatNumber(Math.round(Number(value || 0))),
      }),
    ].join('');

    return this.buildPanelSvg(PANEL_WIDTH, TOP_PANEL_HEIGHT, `${header.join('')}${body}`);
  }

  buildTradeRoutePanelSvg(route, historyBundle, rationale = []) {
    const title = `Best Route - ${route.shipProfile.name}`;
    const titleFit = fitTextToWidth(title, PANEL_CONTENT_WIDTH, {
      size: 42,
      minSize: 30,
      weight: 800,
      letterSpacing: 0.2,
      minChars: 12,
    });
    const header = [
      svgText(titleFit.text, PANEL_CONTENT_X, 52, { size: titleFit.size, weight: 800, family: FONT_DISPLAY, letterSpacing: 0.2 }),
      svgText(`${route.buyShortGroup} -> ${route.sellShortGroup}`, PANEL_CONTENT_X, 92, {
        size: 18,
        weight: 700,
        fill: PANEL_MUTED,
        family: FONT_DISPLAY,
        style: 'italic',
      }),
      renderMetricPill(PANEL_CONTENT_X, 118, 208, 'Investment', `${formatNumber(Math.round(route.cargoValue))} aUEC`, PANEL_BLUE),
      renderMetricPill(PANEL_CONTENT_X + 224, 118, 208, 'Gross Profit', `${formatNumber(Math.round(route.totalProfit))} aUEC`, PANEL_GREEN),
      renderMetricPill(PANEL_CONTENT_X + 448, 118, 208, 'Expected Profit', `${formatNumber(Math.round(route.expectedProfit || 0))} aUEC`, PANEL_GOLD),
      renderMetricPill(PANEL_CONTENT_X + 672, 118, 208, 'ROI', `${route.profitPercent.toFixed(1)}%`, PANEL_PINK),
      renderMetricPill(PANEL_CONTENT_X + 896, 118, 224, 'Confidence', `${Math.round((route.confidenceScore || 0) * 100)}% ${route.confidenceLabel}`, PANEL_CYAN),
    ];

    const body = [
      renderKeyValueCard({
        x: PANEL_CONTENT_X,
        y: 214,
        width: 352,
        height: 580,
        title: 'Run Specs',
        chipType: 'route',
        chipColor: PANEL_GREEN,
        rows: [
          { label: 'Commodity', value: route.commodity },
          { label: 'Cargo', value: `${formatNumber(route.effectiveCargo)} SCU` },
          { label: 'Buy', value: `${formatNumber(Math.round(route.buyPricePerScu))} / SCU` },
          { label: 'Sell', value: `${formatNumber(Math.round(route.sellPricePerScu))} / SCU` },
          { label: 'Expected / min', value: `${formatNumber(Math.round(route.profitPerMinute || 0))} aUEC` },
          { label: 'Travel Time', value: route.time.label },
        ],
      }),
      renderKeyValueCard({
        x: PANEL_CONTENT_X + 368,
        y: 214,
        width: 352,
        height: 580,
        title: 'Market Quality',
        chipType: 'cargo',
        chipColor: PANEL_GOLD,
        rows: [
          { label: 'Freshness', value: route.freshnessInfo?.text || 'Unknown' },
          { label: 'Liquidity', value: route.liquidityInfo?.text || 'Unknown' },
          { label: 'Risk', value: `${route.riskScore}/100 ${route.riskLabel || ''}`.trim() },
          { label: 'UEX Route', value: route.routeQuality?.text || 'No route hints' },
          { label: 'Buy Stock', value: route.buyStock ? `${formatNumber(Math.round(route.buyStock))} SCU` : 'Unknown' },
          { label: 'Sell Demand', value: route.sellDemand ? `${formatNumber(Math.round(route.sellDemand))} SCU` : 'Unknown' },
        ],
      }),
      renderListRowsCard({
        x: PANEL_CONTENT_X + 736,
        y: 214,
        width: 384,
        height: 580,
        title: 'Run Notes',
        subtitle: 'Why this route ranks first',
        chipType: 'leaderboard',
        chipColor: PANEL_CYAN,
        rows: [
          {
            primary: 'Confidence',
            secondary: rationale[0] || 'Confidence is based on freshness, liquidity, and route quality.',
            value: route.confidenceLabel,
          },
          {
            primary: 'Commodity Signal',
            secondary: rationale[1] || (route.marketInfo?.text || 'No commodity ranking details'),
            value: route.marketInfo?.illegal ? 'Illegal' : 'Legal',
          },
          {
            primary: 'Price History',
            secondary: rationale[2] || 'Recent price history is unavailable for this route.',
            value: `${historyBundle?.buyHistory?.count || 0}+${historyBundle?.sellHistory?.count || 0}`,
          },
          {
            primary: 'Risk Factors',
            secondary: route.riskReasons || 'No major risk factors recorded.',
            value: route.riskLabel || 'Low',
          },
        ],
        rowHeight: 92,
      }),
    ].join('');

    return this.buildPanelSvg(PANEL_WIDTH, PANEL_HEIGHT, `${header.join('')}${body}`);
  }

  buildBracketRoutesPanelSvg({ location, finish, brackets }) {
    const title = `Bracket Routes - ${location || 'All Starts'}`;
    const titleFit = fitTextToWidth(title, PANEL_CONTENT_WIDTH, {
      size: 42,
      minSize: 30,
      weight: 800,
      letterSpacing: 0.2,
      minChars: 12,
    });
    const rows = brackets.map(bracket => bracket.route
      ? {
          primary: `${bracket.name} | ${bracket.route.commodity}`,
          secondary: `${bracket.route.buyShortGroup} -> ${bracket.route.sellShortGroup}`,
          value: `${formatNumber(Math.round(bracket.route.totalProfit))} aUEC`,
          tertiary: `ROI ${bracket.route.profitPercent.toFixed(1)}% | ${Math.round((bracket.route.confidenceScore || 0) * 100)}% conf`,
        }
      : {
          primary: `${bracket.name} | No route found`,
          secondary: 'No matching commodity run for the current filters',
          value: 'None',
        });

    const header = [
      svgText(titleFit.text, PANEL_CONTENT_X, 52, { size: titleFit.size, weight: 800, family: FONT_DISPLAY, letterSpacing: 0.2 }),
      svgText('Best route per cargo bracket', PANEL_CONTENT_X, 92, {
        size: 18,
        weight: 700,
        fill: PANEL_MUTED,
        family: FONT_DISPLAY,
        style: 'italic',
      }),
      renderMetricPill(PANEL_CONTENT_X, 118, PANEL_PILL_WIDTH, 'Start', location || 'Any', PANEL_BLUE),
      renderMetricPill(PANEL_CONTENT_X + PANEL_PILL_WIDTH + PANEL_PILL_GAP, 118, PANEL_PILL_WIDTH, 'Finish', finish || 'Any', PANEL_PINK),
      renderMetricPill(PANEL_CONTENT_X + ((PANEL_PILL_WIDTH + PANEL_PILL_GAP) * 2), 118, PANEL_PILL_WIDTH, 'Brackets', formatNumber(brackets.length), PANEL_GREEN),
      renderMetricPill(PANEL_CONTENT_X + ((PANEL_PILL_WIDTH + PANEL_PILL_GAP) * 3), 118, PANEL_PILL_WIDE_WIDTH, 'Mode', 'Cargo bracket scan', PANEL_GOLD),
      renderListRowsCard({
        x: PANEL_CONTENT_X,
        y: 214,
        width: PANEL_CONTENT_WIDTH,
        height: 580,
        title: 'Recommended Runs',
        subtitle: 'Ranked with live pricing, route confidence, and liquidity weighting',
        chipType: 'route',
        chipColor: PANEL_GREEN,
        rows,
        rowHeight: 68,
      }),
    ];

    return this.buildPanelSvg(PANEL_WIDTH, PANEL_HEIGHT, header.join(''));
  }

  buildLocationPanelSvg(group) {
    const shopRows = group.terminals.slice(0, 7).map(terminal => ({
      primary: terminal.name,
      secondary: `${terminal.sells.length} sells | ${terminal.buys.length} buys`,
    }));
    const sellRows = group.sells.slice(0, 7).map(item => ({
      primary: item.commodity,
      secondary: item.terminalName,
      value: `${formatNumber(Math.round(item.price))} / SCU`,
    }));
    const buyRows = group.buys.slice(0, 7).map(item => ({
      primary: item.commodity,
      secondary: item.terminalName,
      value: `${formatNumber(Math.round(item.price))} / SCU`,
    }));

    const header = [
      svgText(group.shortName, PANEL_CONTENT_X, 52, { size: 42, weight: 800, family: FONT_DISPLAY, letterSpacing: 0.2 }),
      svgText(`${group.system} | ${group.locationType}`, PANEL_CONTENT_X, 92, {
        size: 18,
        weight: 700,
        fill: PANEL_MUTED,
        family: FONT_DISPLAY,
        style: 'italic',
      }),
      renderMetricPill(PANEL_CONTENT_X, 118, PANEL_PILL_WIDTH, 'Commodity Shops', formatNumber(group.terminals.length), PANEL_BLUE),
      renderMetricPill(PANEL_CONTENT_X + PANEL_PILL_WIDTH + PANEL_PILL_GAP, 118, PANEL_PILL_WIDTH, 'Sells', formatNumber(group.sells.length), PANEL_GREEN),
      renderMetricPill(PANEL_CONTENT_X + ((PANEL_PILL_WIDTH + PANEL_PILL_GAP) * 2), 118, PANEL_PILL_WIDTH, 'Buys', formatNumber(group.buys.length), PANEL_PINK),
      renderMetricPill(PANEL_CONTENT_X + ((PANEL_PILL_WIDTH + PANEL_PILL_GAP) * 3), 118, PANEL_PILL_WIDE_WIDTH, 'Atmosphere', group.atmospheric ? 'Yes' : 'No', PANEL_CYAN),
      renderListRowsCard({
        x: PANEL_CONTENT_X,
        y: 214,
        width: 320,
        height: 580,
        title: 'Shops',
        subtitle: 'Commodity terminals in this area',
        chipType: 'location',
        chipColor: PANEL_BLUE,
        rows: shopRows.length ? shopRows : [{ primary: 'No terminals found', secondary: 'No tracked commodity terminals here' }],
        rowHeight: 56,
      }),
      renderListRowsCard({
        x: PANEL_CONTENT_X + 336,
        y: 214,
        width: 368,
        height: 580,
        title: 'Best Sells',
        subtitle: 'Cheapest local sell offers',
        chipType: 'cargo',
        chipColor: PANEL_GREEN,
        rows: sellRows.length ? sellRows : [{ primary: 'Nothing currently listed', secondary: 'No sell offers are tracked here' }],
        rowHeight: 56,
      }),
      renderListRowsCard({
        x: PANEL_CONTENT_X + 720,
        y: 214,
        width: 400,
        height: 580,
        title: 'Best Buys',
        subtitle: 'Strongest local buy offers',
        chipType: 'leaderboard',
        chipColor: PANEL_PINK,
        rows: buyRows.length ? buyRows : [{ primary: 'Nothing currently listed', secondary: 'No buy offers are tracked here' }],
        rowHeight: 56,
      }),
    ];

    return this.buildPanelSvg(PANEL_WIDTH, PANEL_HEIGHT, header.join(''));
  }

  buildBuyersPanelSvg({ commodity, amount, location, buyers }) {
    const title = `Best Buyers - ${commodity}`;
    const titleFit = fitTextToWidth(title, PANEL_CONTENT_WIDTH, {
      size: 40,
      minSize: 28,
      weight: 800,
      letterSpacing: 0.2,
      minChars: 12,
    });
    const rows = buyers.map((buyer, index) => ({
      primary: `${index + 1}. ${buyer.shortGroupName}`,
      secondary: buyer.terminalName,
      value: `${formatNumber(Math.round(buyer.price))} / SCU`,
      tertiary: amount
        ? `Sellable ${formatNumber(Math.round(buyer.sellableAmount ?? amount))} | ${formatNumber(Math.round(buyer.totalValue || 0))} aUEC`
        : `Demand ${buyer.demand ? formatNumber(Math.round(buyer.demand)) : 'Unknown'} SCU`,
    }));

    const header = [
      svgText(titleFit.text, PANEL_CONTENT_X, 52, { size: titleFit.size, weight: 800, family: FONT_DISPLAY, letterSpacing: 0.2 }),
      svgText('Highest paying buyer destinations', PANEL_CONTENT_X, 92, {
        size: 18,
        weight: 700,
        fill: PANEL_MUTED,
        family: FONT_DISPLAY,
        style: 'italic',
      }),
      renderMetricPill(PANEL_CONTENT_X, 118, PANEL_PILL_WIDTH, 'Commodity', commodity, PANEL_GREEN),
      renderMetricPill(PANEL_CONTENT_X + PANEL_PILL_WIDTH + PANEL_PILL_GAP, 118, PANEL_PILL_WIDTH, 'Amount', amount ? `${formatNumber(amount)} SCU` : 'Not set', PANEL_BLUE),
      renderMetricPill(PANEL_CONTENT_X + ((PANEL_PILL_WIDTH + PANEL_PILL_GAP) * 2), 118, PANEL_PILL_WIDTH, 'Location Filter', location || 'None', PANEL_PINK),
      renderMetricPill(PANEL_CONTENT_X + ((PANEL_PILL_WIDTH + PANEL_PILL_GAP) * 3), 118, PANEL_PILL_WIDE_WIDTH, 'Results', formatNumber(buyers.length), PANEL_GOLD),
      renderListRowsCard({
        x: PANEL_CONTENT_X,
        y: 214,
        width: PANEL_CONTENT_WIDTH,
        height: 580,
        title: 'Buyer Board',
        subtitle: amount ? 'Ranked by total sale value for the requested cargo' : 'Ranked by price per SCU',
        chipType: 'leaderboard',
        chipColor: PANEL_GOLD,
        rows,
        rowHeight: 68,
      }),
    ];

    return this.buildPanelSvg(PANEL_WIDTH, PANEL_HEIGHT, header.join(''));
  }

  buildShipPanelSvg(ship, sourceLabel) {
    const titleFit = fitTextToWidth(ship.name, PANEL_CONTENT_WIDTH, {
      size: 42,
      minSize: 30,
      weight: 800,
      letterSpacing: 0.2,
      minChars: 10,
    });
    const header = [
      svgText(titleFit.text, PANEL_CONTENT_X, 52, { size: titleFit.size, weight: 800, family: FONT_DISPLAY, letterSpacing: 0.2 }),
      svgText('Cargo hauling profile', PANEL_CONTENT_X, 92, {
        size: 18,
        weight: 700,
        fill: PANEL_MUTED,
        family: FONT_DISPLAY,
        style: 'italic',
      }),
      renderMetricPill(PANEL_CONTENT_X, 118, PANEL_PILL_WIDTH, 'Cargo Capacity', `${formatNumber(ship.cargo)} SCU`, PANEL_GREEN),
      renderMetricPill(PANEL_CONTENT_X + PANEL_PILL_WIDTH + PANEL_PILL_GAP, 118, PANEL_PILL_WIDTH, 'Military?', ship.military ? 'Yes' : 'No', PANEL_GOLD),
      renderMetricPill(PANEL_CONTENT_X + ((PANEL_PILL_WIDTH + PANEL_PILL_GAP) * 2), 118, PANEL_PILL_WIDTH, 'Cargo Tier', ship.cargoTier, PANEL_CYAN),
      renderMetricPill(PANEL_CONTENT_X + ((PANEL_PILL_WIDTH + PANEL_PILL_GAP) * 3), 118, PANEL_PILL_WIDE_WIDTH, 'Data Source', sourceLabel, PANEL_BLUE),
      renderKeyValueCard({
        x: PANEL_CONTENT_X,
        y: 214,
        width: 516,
        height: 580,
        title: 'Profile',
        chipType: 'ship',
        chipColor: PANEL_BLUE,
        rows: [
          { label: 'Ship', value: ship.name },
          { label: 'Cargo Capacity', value: `${formatNumber(ship.cargo)} SCU` },
          { label: 'Military?', value: ship.military ? 'Yes' : 'No' },
          { label: 'Cargo Tier', value: ship.cargoTier },
          { label: 'Source', value: sourceLabel },
        ],
      }),
      renderListRowsCard({
        x: PANEL_CONTENT_X + 532,
        y: 214,
        width: 588,
        height: 580,
        title: 'Hauling Notes',
        subtitle: 'Quick take for route planning',
        chipType: 'cargo',
        chipColor: PANEL_GREEN,
        rows: [
          {
            primary: 'Cargo Footprint',
            secondary: ship.cargo >= 500 ? 'Well-suited for large high-yield trade runs.' : ship.cargo >= 100 ? 'Fits medium bracket trade runs comfortably.' : 'Best for compact cargo loops and targeted runs.',
            value: ship.cargoTier,
          },
          {
            primary: 'Hull Risk',
            secondary: ship.military ? 'Military-derived hulls usually survive hot routes better.' : 'Standard cargo hulls benefit more from safer monitored routes.',
            value: ship.military ? 'Reduced' : 'Standard',
          },
          {
            primary: 'Use Case',
            secondary: ship.cargo >= 250 ? 'Aim for strong profit-per-minute routes with healthy stock and demand.' : 'Aim for reliable local routes with fresh data and short approach times.',
          },
        ],
        rowHeight: 96,
      }),
    ];

    return this.buildPanelSvg(PANEL_WIDTH, PANEL_HEIGHT, header.join(''));
  }

  buildTradeRouteEmbed(route, controls = [], historyBundle = null, rationale = []) {
    return this.buildImagePanelResponse({
      title: `Best Route - ${route.shipProfile.name}`,
      footer: 'SPACEWHLE Trade Command - live UEX pricing, route hints, and commodity ranking data',
      svg: this.buildTradeRoutePanelSvg(route, historyBundle, rationale),
      attachmentName: `route-${route.shipProfile.name.toLowerCase().replace(/[^a-z0-9]+/g, '-')}-${route.commodity.toLowerCase().replace(/[^a-z0-9]+/g, '-')}.png`,
      components: controls,
    });
  }

  buildBracketRoutesEmbed(payload) {
    const locationSlug = String(payload.location || 'all-starts').toLowerCase().replace(/[^a-z0-9]+/g, '-');
    return this.buildImagePanelResponse({
      title: `Bracket Routes - ${payload.location || 'All Starts'}`,
      footer: 'SPACEWHLE Trade Command - ranked by profit, confidence, liquidity, and route quality',
      svg: this.buildBracketRoutesPanelSvg(payload),
      attachmentName: `best-routes-${locationSlug}.png`,
      components: [],
    });
  }

  buildLocationLookupEmbed(group) {
    return this.buildImagePanelResponse({
      title: group.shortName,
      footer: 'SPACEWHLE Trade Command - grouped commodity location view',
      svg: this.buildLocationPanelSvg(group),
      attachmentName: `location-${group.shortName.toLowerCase().replace(/[^a-z0-9]+/g, '-')}.png`,
      components: [],
    });
  }

  buildBuyersLookupEmbed(payload) {
    return this.buildImagePanelResponse({
      title: `Best Buyers - ${payload.commodity}`,
      footer: 'SPACEWHLE Trade Command - top buyer board',
      svg: this.buildBuyersPanelSvg(payload),
      attachmentName: `buyers-${payload.commodity.toLowerCase().replace(/[^a-z0-9]+/g, '-')}.png`,
      components: [],
    });
  }

  buildShipLookupEmbed(ship, sourceLabel) {
    return this.buildImagePanelResponse({
      title: ship.name,
      footer: 'Live pull attempted first, then fallback ship data.',
      svg: this.buildShipPanelSvg(ship, sourceLabel),
      attachmentName: `ship-${ship.name.toLowerCase().replace(/[^a-z0-9]+/g, '-')}.png`,
      components: [],
    });
  }

  buildTopEmbed(days = 7, category = 'overview', showTime = false, graphMenuEnabled = false) {
    const activeCategory = this.normalizeCategory('top', category, graphMenuEnabled);
    const board = this.getLeaderboard(days);
    const components = [this.buildStatsControlRow('top', 'global', days, activeCategory, showTime, graphMenuEnabled)];
    if (showTime) components.push(this.buildRangeButtons('top', 'global', days, activeCategory, true, graphMenuEnabled));
    components.push(this.buildCategorySelectRow('top', 'global', days, activeCategory, showTime, graphMenuEnabled));

    return this.buildImagePanelResponse({
      title: `Top Activity - Last ${days} Day${days === 1 ? '' : 's'}`,
      footer: `Server Lookback: Last ${days} Day${days === 1 ? '' : 's'} - Timezone: UTC`,
      svg: this.buildTopPanelSvg(days, activeCategory, board),
      attachmentName: `top-${days}-${activeCategory}.png`,
      components,
    });
  }

  async buildUserStatsEmbed(userId, days = 7, category = 'overview', showTime = false, graphMenuEnabled = false) {
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
        attachments: [],
      };
    }

    const rankings = this.getUserRankings(userId, days);

    return this.buildImagePanelResponse({
      content: `<@${userId}> - Last ${days} Day${days === 1 ? '' : 's'}`,
      svg: await this.buildUserPanelSvg(userId, days, activeCategory, stats, rankings),
      attachmentName: `stats-user-${userId}-${days}-${activeCategory}.png`,
      components,
    });
  }

  buildServerStatsEmbed(days = 7, category = 'overview', showTime = false, graphMenuEnabled = false) {
    const activeCategory = this.normalizeCategory('server', category, graphMenuEnabled);
    const stats = this.getServerStats(days);
    const components = [this.buildStatsControlRow('server', 'global', days, activeCategory, showTime, graphMenuEnabled)];
    if (showTime) components.push(this.buildRangeButtons('server', 'global', days, activeCategory, true, graphMenuEnabled));
    components.push(this.buildCategorySelectRow('server', 'global', days, activeCategory, showTime, graphMenuEnabled));

    return this.buildImagePanelResponse({
      title: `Server Activity - Last ${days} Day${days === 1 ? '' : 's'}`,
      svg: this.buildServerPanelSvg(days, activeCategory, stats),
      attachmentName: `server-${days}-${activeCategory}.png`,
      components,
    });
  }

  buildPlayersEmbed(guild, days = 7, category = 'overview', showTime = false, graphMenuEnabled = false) {
    const activeCategory = this.normalizeCategory('players', category, graphMenuEnabled);
    const current = this.getCurrentPlayers(guild);
    const peak = this.getPeakForRange(days);
    const dayKeys = this.getRangeDayKeys(days);
    const labels = dayKeys.map(formatDisplayDate);
    const playerSeries = dayKeys.map(dayKey => Number(this.state.peaks?.[dayKey]?.count || 0));

    const components = [this.buildStatsControlRow('players', guild?.id || 'global', days, activeCategory, showTime, graphMenuEnabled)];
    if (showTime) components.push(this.buildRangeButtons('players', guild?.id || 'global', days, activeCategory, true, graphMenuEnabled));
    components.push(this.buildCategorySelectRow('players', guild?.id || 'global', days, activeCategory, showTime, graphMenuEnabled));

    return this.buildImagePanelResponse({
      title: `Players - Last ${days} Day${days === 1 ? '' : 's'}`,
      footer: `Server Lookback: Last ${days} Day${days === 1 ? '' : 's'} - Timezone: UTC`,
      svg: this.buildPlayersPanelSvg(guild, days, activeCategory, current, peak, labels, playerSeries),
      attachmentName: `players-${days}-${activeCategory}.png`,
      components,
    });
  }

  async buildPanel(panel, targetId, days, category = 'overview', showTime = false, graphMenuEnabled = false, guild = null) {
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
      await this.buildPanel(
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
        await this.buildPanel(decoded.panel, decoded.targetId, decoded.days, 'overview', false, false, interaction.guild),
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
        await this.buildPanel(panel, targetId, days, activeCategory, showTime, graphMenuEnabled, interaction.guild),
      );
    }

    if (action === 'time') {
      return interaction.editReply(
        await this.buildPanel(panel, targetId, days, activeCategory, !showTime, graphMenuEnabled, interaction.guild),
      );
    }

    if (action === 'graph') {
      const nextGraphMenuEnabled = !graphMenuEnabled;
      const nextCategory = nextGraphMenuEnabled ? activeCategory : 'overview';

      return interaction.editReply(
        await this.buildPanel(panel, targetId, days, nextCategory, showTime, nextGraphMenuEnabled, interaction.guild),
      );
    }

    if (action === 'range') {
      return interaction.editReply(
        await this.buildPanel(panel, targetId, days, activeCategory, true, graphMenuEnabled, interaction.guild),
      );
    }

    return null;
  }
}

module.exports = {
  StatsTracker,
};
