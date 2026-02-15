"use strict";

const fs = require("fs");
const path = require("path");
require("dotenv").config();
const { createClient } = require("@supabase/supabase-js");

const {
  AttachmentBuilder,
  Client,
  GatewayIntentBits,
  Partials,
  ChannelType,
  PermissionsBitField,
  EmbedBuilder,
  ActionRowBuilder,
  ButtonBuilder,
  ButtonStyle,
  ContainerBuilder,
  Events,
  MessageFlags,
  SlashCommandBuilder,
  REST,
  Routes,
  SeparatorBuilder,
  StringSelectMenuBuilder,
  TextDisplayBuilder,
  ModalBuilder,
  TextInputBuilder,
  TextInputStyle,
} = require("discord.js");
const { createCanvas, loadImage } = require("@napi-rs/canvas");

const config = require("./config.json");
const BOT_TOKEN = process.env.DISCORD_TOKEN || process.env.BOT_TOKEN || "";
const PRIMARY_GUILD_ID = process.env.PRIMARY_GUILD_ID || "1259733702280216638";
const FORCED_HELPER_ROLES = {
  ac: "1395663621731651635",
  ap: "1470179253533413522",
  av: "1421548785154129971",
  utd: "1452695019554209945",
};
const SUPABASE_URL = process.env.SUPABASE_URL || "";
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY || process.env.SUPABASE_KEY || "";
function hasPlaceholderSupabaseValues(url, key) {
  const u = String(url || "").toLowerCase();
  const k = String(key || "").toLowerCase();
  return (
    !u.startsWith("https://") ||
    u.includes("your-project.supabase.co") ||
    k.includes("your-supabase") ||
    k === "your-supabase-service-role-key"
  );
}
const USE_SUPABASE = !!(SUPABASE_URL && SUPABASE_SERVICE_ROLE_KEY && !hasPlaceholderSupabaseValues(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY));
const supabase = USE_SUPABASE ? createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY) : null;
if (!USE_SUPABASE && (SUPABASE_URL || SUPABASE_SERVICE_ROLE_KEY)) {
  console.warn("Supabase disabled: check SUPABASE_URL / SUPABASE_SERVICE_ROLE_KEY in .env");
}
const ENABLE_GUILD_MEMBERS_INTENT = String(process.env.ENABLE_GUILD_MEMBERS_INTENT || "").toLowerCase() === "true";
const ENABLE_MESSAGE_CONTENT_INTENT = String(process.env.ENABLE_MESSAGE_CONTENT_INTENT || "").toLowerCase() === "true";
const intents = [
  GatewayIntentBits.Guilds,
  GatewayIntentBits.GuildMessages,
  GatewayIntentBits.DirectMessages,
];
if (ENABLE_GUILD_MEMBERS_INTENT) intents.push(GatewayIntentBits.GuildMembers);
if (ENABLE_MESSAGE_CONTENT_INTENT) intents.push(GatewayIntentBits.MessageContent);

const client = new Client({
  intents,
  partials: [Partials.Channel],
});

process.on("unhandledRejection", (reason) => {
  console.error("UNHANDLED REJECTION:", reason);
});
process.on("uncaughtException", (err) => {
  console.error("UNCAUGHT EXCEPTION:", err);
});


const TICKET_EMBED_COLOR = 0xf07bff;
const EMOJI_HELPER = "1465803484879261739";
const EMOJI_TICKET = "1464644315287654590";
const EMOJI_DESC = "1465794554954518601";
const EMOJI_REVIEWER = "1465794305011617874";
const EMOJI_LEADERBOARD = "1465794496745967721";
const EMOJI_DONE_CLAIM = "1465794356962394285";
const EMOJI_QUESTION = "1464644150384132258";
const EMOJI_STARS = "1465794279380353044";
const EMOJI_RANK = "1465794532976230440";
const EMOJI_VOTES = "1465794902540423422";
const DM_AUTO_DELETE_MS = 5 * 60 * 1000;

function em(id, name = "e") {
  return `<:${name}:${id}>`;
}

function scheduleDmAutoDelete(message, ms = DM_AUTO_DELETE_MS) {
  if (!message || message.guild) return;
  const timer = setTimeout(() => {
    message.delete().catch(() => {});
  }, ms);
  if (typeof timer?.unref === "function") timer.unref();
}

const emojiImageCache = new Map();

async function loadCustomEmojiImage(emojiId) {
  const id = String(emojiId || "").trim();
  if (!id) return null;
  if (emojiImageCache.has(id)) return emojiImageCache.get(id);

  const urls = [
    `https://cdn.discordapp.com/emojis/${id}.png?size=64&quality=lossless`,
    `https://cdn.discordapp.com/emojis/${id}.webp?size=64&quality=lossless`,
  ];
  for (const url of urls) {
    try {
      const img = await loadImage(url);
      emojiImageCache.set(id, img);
      return img;
    } catch {}
  }
  emojiImageCache.set(id, null);
  return null;
}

async function drawLineWithEmoji(ctx, opts) {
  const {
    emojiId,
    text,
    x,
    y,
    font = "600 27px Segoe UI",
    color = "#d8b8f5",
    iconSize = 24,
    gap = 10,
  } = opts;

  ctx.font = font;
  ctx.fillStyle = color;
  let textX = x;
  const icon = await loadCustomEmojiImage(emojiId);
  if (icon) {
    const iconY = y - iconSize + 5;
    ctx.drawImage(icon, x, iconY, iconSize, iconSize);
    textX += iconSize + gap;
  }
  ctx.fillText(text, textX, y);
}

const BANNER_MAIN_AND_CARRY =
  "https://media.discordapp.net/attachments/1405973335979851877/1466056854420455434/RULES_BANNER_2.gif";

const BANNER_HELPER_TICKET =
  "https://media.discordapp.net/attachments/1405973335979851877/1466056776431702108/RULES_BANNER_4.gif";

const BANNER_VOUCH_LOG =
  "https://media.discordapp.net/attachments/1405973335979851877/1466056757578174485/RULES_BANNER_5.gif";

const BANNER_LEADERBOARD =
  "https://media.discordapp.net/attachments/1405973335979851877/1466056737865076859/RULES_BANNER_6.gif";

const BANNER_HELPER_PROFILE =
  "https://media.discordapp.net/attachments/1405973335979851877/1466056723348324566/RULES_BANNER_7.gif";

const DATA_DIR = path.join(__dirname, "data");
if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR);
const INSTANCE_LOCK_PATH = path.join(DATA_DIR, "bot_instance.lock");

function isPidAlive(pid) {
  const n = Number(pid);
  if (!Number.isInteger(n) || n <= 0) return false;
  try {
    process.kill(n, 0);
    return true;
  } catch {
    return false;
  }
}

function setupSingleInstanceLock() {
  try {
    if (fs.existsSync(INSTANCE_LOCK_PATH)) {
      const raw = fs.readFileSync(INSTANCE_LOCK_PATH, "utf8").trim();
      const oldPid = Number(raw);
      if (isPidAlive(oldPid) && oldPid !== process.pid) {
        console.error(`Another bot instance is already running (PID ${oldPid}). Stop it first.`);
        process.exit(1);
      }
      fs.unlinkSync(INSTANCE_LOCK_PATH);
    }
    fs.writeFileSync(INSTANCE_LOCK_PATH, String(process.pid));
  } catch (e) {
    console.warn("Could not initialize instance lock:", e?.message || e);
  }

  const cleanup = () => {
    try {
      if (!fs.existsSync(INSTANCE_LOCK_PATH)) return;
      const raw = fs.readFileSync(INSTANCE_LOCK_PATH, "utf8").trim();
      if (Number(raw) === process.pid) fs.unlinkSync(INSTANCE_LOCK_PATH);
    } catch {}
  };

  process.on("exit", cleanup);
  process.on("SIGINT", () => { cleanup(); process.exit(0); });
  process.on("SIGTERM", () => { cleanup(); process.exit(0); });
}

setupSingleInstanceLock();

const TICKETS_STATE_PATH = path.join(DATA_DIR, "tickets_state.json");
const HELPERS_PATH = path.join(DATA_DIR, "helpers.json");
const MSGCOUNT_PATH = path.join(DATA_DIR, "messages.json");
const COUNTER_PATH = path.join(DATA_DIR, "ticketCounter.json");
const WEEKLY_PATH = path.join(DATA_DIR, "weekly.json");
function readJson(file, fallback = {}) {
  try {
    return JSON.parse(fs.readFileSync(file, "utf8"));
  } catch {
    return fallback;
  }
}
function writeJson(file, data) {
  fs.writeFileSync(file, JSON.stringify(data, null, 2));
}
function isSnowflake(id) {
  return typeof id === "string" && /^\d{17,20}$/.test(id);
}
function guildSettings(guildId) {
  const base = {
    ticketCategoryId: config.ticketCategoryId,
    helperApplyCategoryId: config.helperApplyCategoryId,
    helperRoleId: config.helperRoleId,
    helperRolesByGame: config.helperRolesByGame,
    reviewChannelId: config.reviewChannelId,
    vipRoleId: config.vipRoleId,
    boosterRoleId: config.boosterRoleId,
    robuxLink: config.robuxLink,
    moneyLink: config.moneyLink,
    pointsPerTicket: config.pointsPerTicket,
  };
  const byGuild = config.guilds?.[guildId] || {};
  return { ...base, ...byGuild };
}

function isAllowedGuild(guildId) {
  return String(guildId || "") === PRIMARY_GUILD_ID;
}

function getConfiguredGuildIds() {
  return [PRIMARY_GUILD_ID];
}
async function sbLoadState(key, fallback) {
  if (!supabase) return fallback;
  const { data, error } = await supabase
    .from("bot_state")
    .select("value")
    .eq("key", key)
    .maybeSingle();
  if (error || !data) return fallback;
  return data.value ?? fallback;
}

async function sbSaveState(key, value) {
  if (!supabase) return;
  const { error } = await supabase
    .from("bot_state")
    .upsert(
      { key, value, updated_at: new Date().toISOString() },
      { onConflict: "key" }
    );
  if (error) throw error;
}

function persistState(key, value) {
  if (!supabase) return;
  sbSaveState(key, value).catch((e) => {
    console.error(`Supabase save failed for ${key}:`, e?.message || e);
  });
}

async function hydrateStateFromSupabase() {
  if (!supabase) return;
  try {
    const remoteMsg = await sbLoadState("messages", null);
    const remoteCounter = await sbLoadState("ticketCounter", null);
    const remoteTickets = await sbLoadState("tickets_state", null);
    const remoteHelpers = await sbLoadState("helpers", null);
    const remoteWeekly = await sbLoadState("weekly", null);

    if (remoteMsg && typeof remoteMsg === "object") {
      msgCounts = remoteMsg;
      writeJson(MSGCOUNT_PATH, msgCounts);
    }
    if (remoteCounter && typeof remoteCounter === "object") {
      counter = remoteCounter;
      writeJson(COUNTER_PATH, counter);
    }
    if (remoteTickets && typeof remoteTickets === "object") {
      tickets = remoteTickets;
      writeJson(TICKETS_STATE_PATH, tickets);
    }
    if (remoteHelpers && typeof remoteHelpers === "object") {
      helpers = remoteHelpers;
      writeJson(HELPERS_PATH, helpers);
    }
    if (remoteWeekly && typeof remoteWeekly === "object") {
      weekly = remoteWeekly;
      writeJson(WEEKLY_PATH, weekly);
    }

    if (!remoteMsg) persistState("messages", msgCounts);
    if (!remoteCounter) persistState("ticketCounter", counter);
    if (!remoteTickets) persistState("tickets_state", tickets);
    if (!remoteHelpers) persistState("helpers", helpers);
    if (!remoteWeekly) persistState("weekly", weekly);
  } catch (e) {
    console.error("Supabase hydration failed:", e?.message || e);
  }
}

function inlineBox(text) {
  const s = String(text ?? "").replace(/`/g, "ˋ");
  return `\`${s}\``;
}
function box(text, lang = "") {
  const s = String(text ?? "").replace(/```/g, "ˋˋˋ");
  return `\`\`\`${lang}\n${s}\n\`\`\``;
}
function clamp(str, n) {
  const s = String(str ?? "");
  return s.length > n ? s.slice(0, n - 1) + "…" : s;
}

const GAME_HELPER_ROLES = { ...FORCED_HELPER_ROLES };

const STAFF_CLOSE_ROLES = new Set([
  "1352735543443849246",
  "1441847397683822763",
  "1352735537466839112",
]);

const APPLICATION_VIEW_ROLES = new Set([
  ...(Array.isArray(config.applicationViewRoleIds) ? config.applicationViewRoleIds : []),
  process.env.HELPER_ROLE_ID || "",
  process.env.DEFAULT_HELPER_ROLE_ID || "",
].map((x) => String(x || "")).filter(isSnowflake));

function getApplicationViewRoles(guildId) {
  const g = config.guilds?.[guildId] || {};
  const extra = Array.isArray(g.applicationViewRoleIds) ? g.applicationViewRoleIds : [];
  return Array.from(new Set([...APPLICATION_VIEW_ROLES, ...extra].map((x) => String(x || "")).filter(isSnowflake)));
}
function existingRoleIds(guild, roleIds) {
  if (!guild || !Array.isArray(roleIds)) return [];
  const uniq = Array.from(new Set(roleIds.map((x) => String(x || "")).filter(isSnowflake)));
  return uniq.filter((rid) => guild.roles.cache.has(rid));
}

function helperRoleForGame(guildId, game) {
  const s = guildSettings(guildId);
  const byGame = s?.helperRolesByGame && typeof s.helperRolesByGame === "object" ? s.helperRolesByGame : {};
  const appConfigured = Array.isArray(s?.applicationViewRoleIds)
    ? s.applicationViewRoleIds.map((x) => String(x || "")).find(isSnowflake)
    : null;
  const appRoleFallback = appConfigured || getApplicationViewRoles(guildId).map((x) => String(x || "")).find(isSnowflake) || null;
  const candidate =
    FORCED_HELPER_ROLES[game] ||
    byGame?.[game] ||
    s?.helperRoleId ||
    process.env.HELPER_ROLE_ID ||
    process.env.DEFAULT_HELPER_ROLE_ID ||
    appRoleFallback ||
    GAME_HELPER_ROLES[game] ||
    null;
  const id = String(candidate || "");
  return isSnowflake(id) ? id : null;
}
function hasAnyRole(member, roles) {
  return !!member?.roles?.cache && roles.some((r) => member.roles.cache.has(r));
}
function isGameHelper(member, game) {
  const rid = helperRoleForGame(member?.guild?.id, game);
  return !!rid && member?.roles?.cache?.has(rid);
}
function canStaffClose(member, game) {
  return isGameHelper(member, game) || hasAnyRole(member, [...STAFF_CLOSE_ROLES]);
}

function isHelperMember(member) {
  if (!member || !member.guild) return false;
  const s = guildSettings(member.guild.id);
  const rolePool = [];
  if (s?.helperRoleId) rolePool.push(String(s.helperRoleId));
  if (s?.helperRolesByGame && typeof s.helperRolesByGame === "object") {
    for (const rid of Object.values(s.helperRolesByGame)) rolePool.push(String(rid || ""));
  }
  rolePool.push(String(process.env.HELPER_ROLE_ID || ""));
  rolePool.push(String(process.env.DEFAULT_HELPER_ROLE_ID || ""));
  for (const rid of getApplicationViewRoles(member.guild.id)) rolePool.push(String(rid || ""));
  const valid = Array.from(new Set(rolePool.filter(isSnowflake)));
  return hasAnyRole(member, valid);
}


/* ===================== VIP / BOOSTER ===================== */
function isVipOrBooster(member) {
  const s = guildSettings(member?.guild?.id);
  return (
    (s.vipRoleId && member?.roles?.cache?.has(s.vipRoleId)) ||
    (s.boosterRoleId && member?.roles?.cache?.has(s.boosterRoleId))
  );
}
function reviewerType(member) {
  const s = guildSettings(member?.guild?.id);
  if (s.vipRoleId && member?.roles?.cache?.has(s.vipRoleId)) return "VIP";
  if (s.boosterRoleId && member?.roles?.cache?.has(s.boosterRoleId)) return "Booster";
  return "Member";
}
function strongestReviewerStatus(member) {
  if (isHelperMember(member)) return "Helper";
  return reviewerType(member);
}

/* ===================== MESSAGE COUNT ===================== */
let msgCounts = readJson(MSGCOUNT_PATH, {});
let msgDirty = false;

function bumpMsg(userId) {
  msgCounts[userId] = (msgCounts[userId] || 0) + 1;
  msgDirty = true;
}
setInterval(() => {
  if (!msgDirty) return;
  msgDirty = false;
  writeJson(MSGCOUNT_PATH, msgCounts);
  persistState("messages", msgCounts);
}, 20_000);

/* ===================== TICKET COUNTER ===================== */
let counter = readJson(COUNTER_PATH, { last: 0 });
function nextTicketNumber() {
  counter.last = (counter.last || 0) + 1;
  writeJson(COUNTER_PATH, counter);
  persistState("ticketCounter", counter);
  return counter.last;
}

/* ===================== TICKET STATE ===================== */
let tickets = readJson(TICKETS_STATE_PATH, {});
let ticketsDirty = false;

function saveTickets() {
  if (!ticketsDirty) return;
  ticketsDirty = false;
  writeJson(TICKETS_STATE_PATH, tickets);
  persistState("tickets_state", tickets);
}
setInterval(saveTickets, 10_000);

function getTicket(chId) {
  return tickets[chId] || null;
}
function setTicket(chId, data) {
  tickets[chId] = data;
  ticketsDirty = true;
}
function patchTicket(chId, patch) {
  tickets[chId] = { ...(tickets[chId] || {}), ...patch };
  ticketsDirty = true;
}
function deleteTicket(chId) {
  delete tickets[chId];
  ticketsDirty = true;
}

/* ===================== HELPERS + WEEKLY ===================== */
let helpers = readJson(HELPERS_PATH, {});
let weekly = readJson(WEEKLY_PATH, { weekStart: 0, helpers: {} });

function saveHelpers() {
  writeJson(HELPERS_PATH, helpers);
  persistState("helpers", helpers);
}
function saveWeekly() {
  writeJson(WEEKLY_PATH, weekly);
  persistState("weekly", weekly);
}

function ensureHelper(id) {
  const key = String(id || "");
  if (!helpers[key]) {
    helpers[key] = {
      ticketsCompleted: 0,
      points: 0,
      ratingSum: 0,
      ratingCount: 0,
    };
  }
  return helpers[key];
}

function avgRating(rec) {
  if (!rec || !rec.ratingCount) return null;
  return Number(rec.ratingSum || 0) / Number(rec.ratingCount || 0);
}

function touchWeekly() {
  const now = Date.now();
  const oneWeek = 7 * 24 * 60 * 60 * 1000;
  if (!weekly.weekStart || now - weekly.weekStart > oneWeek) {
    weekly.weekStart = now;
    weekly.helpers = {};
    saveWeekly();
  }
}

const RANKS = [
  { name: "F", minTickets: 0 },
  { name: "E", minTickets: 50 },
  { name: "D", minTickets: 150 },
  { name: "C", minTickets: 300 },
  { name: "B", minTickets: 600 },
  { name: "A", minTickets: 1200 },
  { name: "S", minTickets: 2500 },
  { name: "SS", minTickets: 6000 },
  { name: "SSS", minTickets: 10000 },
];

function rankFromTickets(ticketsDone) {
  let r = "F";
  for (const x of RANKS) if (ticketsDone >= x.minTickets) r = x.name;
  return r;
}

function starsFromRating(rating) {
  if (typeof rating !== "number" || Number.isNaN(rating)) return "\u2606\u2606\u2606\u2606\u2606";
  const full = Math.max(0, Math.min(5, Math.round(rating)));
  return "\u2605".repeat(full) + "\u2606".repeat(5 - full);
}

function fitText(ctx, text, maxWidth) {
  const raw = String(text ?? "");
  if (ctx.measureText(raw).width <= maxWidth) return raw;
  let out = raw;
  while (out.length > 0 && ctx.measureText(`${out}...`).width > maxWidth) {
    out = out.slice(0, -1);
  }
  return `${out}...`;
}

function roundedRectPath(ctx, x, y, w, h, r) {
  const rr = Math.max(0, Math.min(r, Math.floor(Math.min(w, h) / 2)));
  ctx.beginPath();
  ctx.moveTo(x + rr, y);
  ctx.lineTo(x + w - rr, y);
  ctx.arcTo(x + w, y, x + w, y + rr, rr);
  ctx.lineTo(x + w, y + h - rr);
  ctx.arcTo(x + w, y + h, x + w - rr, y + h, rr);
  ctx.lineTo(x + rr, y + h);
  ctx.arcTo(x, y + h, x, y + h - rr, rr);
  ctx.lineTo(x, y + rr);
  ctx.arcTo(x, y, x + rr, y, rr);
  ctx.closePath();
}

function drawParticleField(ctx, width, height, palette, count = 70) {
  for (let i = 0; i < count; i++) {
    const px = Math.random() * width;
    const py = Math.random() * height;
    const pr = 1 + Math.random() * 3;
    const color = palette[i % palette.length];
    const alpha = 0.14 + Math.random() * 0.45;
    ctx.save();
    ctx.shadowColor = `rgba(${color}, ${Math.min(alpha + 0.24, 0.85)})`;
    ctx.shadowBlur = 10 + Math.random() * 12;
    ctx.fillStyle = `rgba(${color}, ${alpha.toFixed(3)})`;
    ctx.beginPath();
    ctx.arc(px, py, pr, 0, Math.PI * 2);
    ctx.fill();
    ctx.restore();
  }
}

async function drawNeonAvatar(ctx, avatarUrl, x, y, size, opts = {}) {
  const innerGlow = opts.innerGlow || "rgba(255,255,255,0.65)";
  const outerGlow = opts.outerGlow || "rgba(255,56,56,0.9)";
  const outerStroke = opts.outerStroke || "#ff4444";
  const fallback = opts.fallback || "#2b1d1d";
  try {
    const img = await loadImage(avatarUrl);
    ctx.save();
    ctx.beginPath();
    ctx.arc(x + size / 2, y + size / 2, size / 2, 0, Math.PI * 2);
    ctx.closePath();
    ctx.clip();
    ctx.drawImage(img, x, y, size, size);
    ctx.restore();
  } catch {
    ctx.fillStyle = fallback;
    ctx.beginPath();
    ctx.arc(x + size / 2, y + size / 2, size / 2, 0, Math.PI * 2);
    ctx.fill();
  }

  ctx.save();
  ctx.shadowColor = innerGlow;
  ctx.shadowBlur = 14;
  ctx.strokeStyle = "rgba(255,255,255,0.9)";
  ctx.lineWidth = 2;
  ctx.beginPath();
  ctx.arc(x + size / 2, y + size / 2, size / 2 + 4, 0, Math.PI * 2);
  ctx.stroke();
  ctx.restore();

  ctx.save();
  ctx.shadowColor = outerGlow;
  ctx.shadowBlur = 22;
  ctx.strokeStyle = outerStroke;
  ctx.lineWidth = 4;
  ctx.beginPath();
  ctx.arc(x + size / 2, y + size / 2, size / 2 + 1, 0, Math.PI * 2);
  ctx.stroke();
  ctx.restore();
}

function drawAvatarOrbit(ctx, cx, cy, baseRadius, opts = {}) {
  const main = opts.main || "rgba(255,255,255,0.92)";
  const alt = opts.alt || "rgba(255,56,56,0.9)";
  const count = opts.count || 3;
  for (let i = 0; i < count; i++) {
    const radius = baseRadius + i * 8;
    ctx.save();
    ctx.strokeStyle = i % 2 === 0 ? main : alt;
    ctx.lineWidth = i === 0 ? 2 : 1.6;
    ctx.shadowColor = i % 2 === 0 ? "rgba(255,255,255,0.65)" : "rgba(255,56,56,0.72)";
    ctx.shadowBlur = 12 + i * 4;
    if (i === 2) ctx.setLineDash([8, 8]);
    ctx.beginPath();
    ctx.arc(cx, cy, radius, Math.PI * (0.12 + i * 0.08), Math.PI * (1.96 - i * 0.05));
    ctx.stroke();
    ctx.restore();
  }
}

async function buildLeaderboardImage(guild, topRows) {
  const width = 1100;
  const headerH = 198;
  const rowH = 148;
  const rowsToRender = Math.max(topRows.length, 1);
  const height = headerH + rowsToRender * rowH + 42;
  const padding = 34;
  const canvas = createCanvas(width, height);
  const ctx = canvas.getContext("2d");
  const FONT_TITLE = "700 56px \"Bahnschrift\", \"Trebuchet MS\", \"Segoe UI\", sans-serif";
  const FONT_SUB = "500 24px \"Bahnschrift\", \"Trebuchet MS\", \"Segoe UI\", sans-serif";
  const FONT_ROW_MAIN = "700 47px \"Bahnschrift\", \"Trebuchet MS\", \"Segoe UI\", sans-serif";
  const FONT_ROW_META = "700 33px \"Bahnschrift\", \"Trebuchet MS\", \"Segoe UI Emoji\", \"Segoe UI Symbol\", sans-serif";

  const bg = ctx.createLinearGradient(0, 0, width, height);
  bg.addColorStop(0, "#050505");
  bg.addColorStop(0.5, "#0d0d0d");
  bg.addColorStop(1, "#151515");
  ctx.fillStyle = bg;
  ctx.fillRect(0, 0, width, height);

  const glow = ctx.createRadialGradient(width * 0.72, 106, 20, width * 0.72, 106, 320);
  glow.addColorStop(0, "rgba(255,255,255,0.26)");
  glow.addColorStop(1, "rgba(255,255,255,0)");
  ctx.fillStyle = glow;
  ctx.fillRect(0, 0, width, height);
  drawParticleField(ctx, width, height, ["255,255,255", "240,240,240", "255,62,62"], 58);

  roundedRectPath(ctx, 16, 16, width - 32, height - 32, 24);
  ctx.fillStyle = "#090909";
  ctx.fill();
  ctx.strokeStyle = "#f3f3f3";
  ctx.lineWidth = 2;
  ctx.stroke();
  roundedRectPath(ctx, 24, 24, width - 48, height - 48, 20);
  ctx.strokeStyle = "rgba(255,64,64,0.62)";
  ctx.lineWidth = 1.2;
  ctx.stroke();

  ctx.fillStyle = "#ffffff";
  ctx.font = FONT_TITLE;
  ctx.shadowColor = "rgba(255,255,255,0.36)";
  ctx.shadowBlur = 16;
  ctx.fillText("Leaderboard of Mohgs Server", padding, 88);
  ctx.shadowBlur = 0;

  await drawLineWithEmoji(ctx, {
    emojiId: EMOJI_TICKET,
    text: "Top helpers by completed tickets",
    x: padding,
    y: 126,
    font: FONT_SUB,
    color: "#dddddd",
    iconSize: 20,
    gap: 8,
  });
  await drawLineWithEmoji(ctx, {
    emojiId: EMOJI_RANK,
    text: `Server: ${guild?.name || "Unknown Guild"}`,
    x: padding,
    y: 154,
    font: FONT_SUB,
    color: "#dddddd",
    iconSize: 20,
    gap: 8,
  });

  for (let i = 0; i < topRows.length; i++) {
    const row = topRows[i];
    const y = headerH + i * rowH + 8;
    const rowX = 28;
    const rowW = width - 56;
    const rowHInner = rowH - 16;

    roundedRectPath(ctx, rowX, y, rowW, rowHInner, 18);
    ctx.fillStyle = i % 2 === 0 ? "#121212" : "#0d0d0d";
    ctx.fill();
    ctx.strokeStyle = i < 3 ? "#ffffff" : "#4e4e4e";
    ctx.lineWidth = i < 3 ? 2 : 1.3;
    ctx.stroke();

    if (i < 3) {
      roundedRectPath(ctx, rowX + 10, y + 12, 8, rowHInner - 24, 4);
      ctx.fillStyle = i === 0 ? "#ffffff" : i === 1 ? "#ff8e59" : "#ffb659";
      ctx.fill();
    }

    const avatarX = rowX + 28;
    const avatarSize = 80;
    const avatarY = y + Math.floor((rowHInner - avatarSize) / 2);
    const rankX = avatarX + avatarSize + 26;
    const nameX = rankX + 98;

    await drawNeonAvatar(ctx, row.avatarUrl, avatarX, avatarY, avatarSize, {
      outerStroke: i < 3 ? "#ffffff" : "#efefef",
      outerGlow: i < 3 ? "rgba(255,255,255,0.88)" : "rgba(255,255,255,0.62)",
    });

    await drawLineWithEmoji(ctx, {
      emojiId: EMOJI_RANK,
      text: `#${i + 1}`,
      x: rankX,
      y: y + 58,
      font: "700 39px \"Bahnschrift\", \"Trebuchet MS\", \"Segoe UI\", sans-serif",
      color: "#f6f6f6",
      iconSize: 22,
      gap: 8,
    });

    ctx.font = FONT_ROW_MAIN;
    ctx.fillStyle = "#f1f1f1";
    ctx.fillText(fitText(ctx, row.name, 360), nameX, y + 60);

    const ticketText = `${row.tickets} tickets`;
    const ratingRaw = typeof row.rating === "number"
      ? `${row.rating.toFixed(2)}/5 ${starsFromRating(row.rating)}`
      : "No rating";
    const ratingText = fitText(ctx, ratingRaw, 330);

    await drawLineWithEmoji(ctx, {
      emojiId: EMOJI_TICKET,
      text: ticketText,
      x: nameX,
      y: y + 110,
      font: FONT_ROW_META,
      color: "#e7e7e7",
      iconSize: 20,
      gap: 8,
    });

    ctx.font = FONT_ROW_META;
    const prefixW = 20 + 8 + ctx.measureText(ticketText).width + 24;
    await drawLineWithEmoji(ctx, {
      emojiId: EMOJI_STARS,
      text: ratingText,
      x: nameX + prefixW,
      y: y + 110,
      font: FONT_ROW_META,
      color: "#e7e7e7",
      iconSize: 20,
      gap: 8,
    });
  }

  if (topRows.length === 0) {
    roundedRectPath(ctx, 28, headerH + 8, width - 56, rowH - 16, 18);
    ctx.fillStyle = "#121212";
    ctx.fill();
    ctx.strokeStyle = "#f3f3f3";
    ctx.lineWidth = 1.5;
    ctx.stroke();
    ctx.fillStyle = "#ececec";
    ctx.font = "600 34px \"Bahnschrift\", \"Trebuchet MS\", \"Segoe UI\", sans-serif";
    ctx.fillText("No helper data yet.", padding + 14, headerH + 94);
  }

  return canvas.toBuffer("image/png");
}

async function buildProfileImage(user, rec) {
  const width = 1200;
  const height = 700;
  const canvas = createCanvas(width, height);
  const ctx = canvas.getContext("2d");

  const ticketsDone = rec?.ticketsCompleted || 0;
  const points = rec?.points || 0;
  const rating = avgRating(rec);
  const rank = rankFromTickets(ticketsDone);
  const ratingCount = rec?.ratingCount || 0;
  const statusLabel = ticketsDone > 0 ? "ACTIVE HELPER" : "NEW HELPER";
  const userTag = String(user?.tag || user?.username || "Unknown User");
  const scoreText = rating ? `${rating.toFixed(2)}/5` : "No rating";
  const starsText = starsFromRating(rating || 0);

  const bg = ctx.createLinearGradient(0, 0, width, height);
  bg.addColorStop(0, "#050505");
  bg.addColorStop(0.55, "#0d0d0d");
  bg.addColorStop(1, "#171717");
  ctx.fillStyle = bg;
  ctx.fillRect(0, 0, width, height);
  drawParticleField(ctx, width, height, ["255,255,255", "240,240,240", "255,52,52"], 95);

  roundedRectPath(ctx, 16, 16, width - 32, height - 32, 24);
  ctx.fillStyle = "#0a0a0a";
  ctx.fill();
  ctx.strokeStyle = "#f7f7f7";
  ctx.lineWidth = 2;
  ctx.stroke();
  roundedRectPath(ctx, 24, 24, width - 48, height - 48, 20);
  ctx.strokeStyle = "rgba(255,60,60,0.62)";
  ctx.lineWidth = 1.3;
  ctx.stroke();

  const leftX = 44;
  const leftW = 340;
  const rightX = 420;
  const rightW = 740;

  ctx.font = "700 56px \"Bahnschrift\", \"Trebuchet MS\", \"Segoe UI\", sans-serif";
  ctx.fillStyle = "#ffffff";
  ctx.fillText("Helper Profile", leftX, 92);
  ctx.font = "500 30px \"Bahnschrift\", \"Trebuchet MS\", \"Segoe UI\", sans-serif";
  ctx.fillStyle = "#dfdfdf";
  ctx.fillText(fitText(ctx, userTag, leftW), leftX, 130);

  const avatarSize = 246;
  const avatarX = leftX + 34;
  const avatarY = 164;
  const avatarUrl = user?.displayAvatarURL
    ? user.displayAvatarURL({ extension: "png", size: 512, forceStatic: true })
    : "https://cdn.discordapp.com/embed/avatars/0.png";
  await drawNeonAvatar(ctx, avatarUrl, avatarX, avatarY, avatarSize, {
    outerStroke: "#ffffff",
    outerGlow: "rgba(255,255,255,0.84)",
  });
  drawAvatarOrbit(ctx, avatarX + avatarSize / 2, avatarY + avatarSize / 2, avatarSize / 2 + 12, {
    main: "rgba(255,255,255,0.86)",
    alt: "rgba(255,58,58,0.92)",
    count: 3,
  });

  roundedRectPath(ctx, leftX, 438, leftW, 160, 16);
  ctx.fillStyle = "rgba(12,12,12,0.95)";
  ctx.fill();
  ctx.strokeStyle = "#f2f2f2";
  ctx.lineWidth = 1.7;
  ctx.stroke();
  await drawLineWithEmoji(ctx, {
    emojiId: EMOJI_HELPER,
    text: fitText(ctx, String(user?.username || "User"), 230),
    x: leftX + 18,
    y: 482,
    font: "700 38px \"Bahnschrift\", \"Trebuchet MS\", \"Segoe UI\", sans-serif",
    color: "#ffffff",
    iconSize: 30,
    gap: 10,
  });
  roundedRectPath(ctx, leftX + 78, 514, 232, 50, 10);
  ctx.fillStyle = "#171717";
  ctx.fill();
  ctx.strokeStyle = "rgba(255,70,70,0.84)";
  ctx.lineWidth = 1.3;
  ctx.stroke();
  ctx.fillStyle = "#f3f3f3";
  ctx.font = "700 26px \"Bahnschrift\", \"Trebuchet MS\", \"Segoe UI\", sans-serif";
  ctx.fillText(statusLabel, leftX + 90, 548);

  roundedRectPath(ctx, rightX, 108, rightW, 560, 20);
  ctx.fillStyle = "rgba(10,10,10,0.96)";
  ctx.fill();
  ctx.strokeStyle = "#f2f2f2";
  ctx.lineWidth = 1.9;
  ctx.stroke();
  roundedRectPath(ctx, rightX + 8, 116, rightW - 16, 544, 16);
  ctx.strokeStyle = "rgba(255,60,60,0.62)";
  ctx.lineWidth = 1.1;
  ctx.stroke();

  ctx.fillStyle = "#ffffff";
  ctx.font = "700 52px \"Bahnschrift\", \"Trebuchet MS\", \"Segoe UI\", sans-serif";
  ctx.fillText("Overview", rightX + 24, 170);

  async function drawStatBox(x, y, w, h, emojiId, label, value) {
    roundedRectPath(ctx, x, y, w, h, 14);
    ctx.fillStyle = "rgba(15,15,15,0.96)";
    ctx.fill();
    ctx.strokeStyle = "rgba(255,70,70,0.74)";
    ctx.lineWidth = 1.3;
    ctx.stroke();
    await drawLineWithEmoji(ctx, {
      emojiId,
      text: label,
      x: x + 14,
      y: y + 32,
      font: "600 28px \"Bahnschrift\", \"Trebuchet MS\", \"Segoe UI\", sans-serif",
      color: "#efefef",
      iconSize: 22,
      gap: 8,
    });
    ctx.font = "700 41px \"Bahnschrift\", \"Trebuchet MS\", \"Segoe UI\", sans-serif";
    ctx.fillStyle = "#ffffff";
    ctx.fillText(fitText(ctx, String(value), w - 28), x + 16, y + 84);
  }

  const boxW = 350;
  const boxH = 108;
  const gapX = 16;
  const c1 = rightX + 22;
  const c2 = c1 + boxW + gapX;
  const r1 = 194;
  const r2 = r1 + boxH + 14;
  await drawStatBox(c1, r1, boxW, boxH, EMOJI_RANK, "Rank", rank);
  await drawStatBox(c2, r1, boxW, boxH, EMOJI_TICKET, "Tickets", ticketsDone);
  await drawStatBox(c1, r2, boxW, boxH, EMOJI_LEADERBOARD, "Points", points);
  await drawStatBox(c2, r2, boxW, boxH, EMOJI_STARS, "Rating", scoreText);

  roundedRectPath(ctx, rightX + 22, r2 + boxH + 14, rightW - 44, 146, 14);
  ctx.fillStyle = "rgba(13,13,13,0.95)";
  ctx.fill();
  ctx.strokeStyle = "rgba(255,70,70,0.74)";
  ctx.lineWidth = 1.3;
  ctx.stroke();
  await drawLineWithEmoji(ctx, {
    emojiId: EMOJI_VOTES,
    text: `Votes: ${ratingCount}`,
    x: rightX + 34,
    y: r2 + boxH + 48,
    font: "600 34px \"Bahnschrift\", \"Trebuchet MS\", \"Segoe UI\", sans-serif",
    color: "#f3f3f3",
    iconSize: 24,
  });
  ctx.font = "600 27px \"Bahnschrift\", \"Trebuchet MS\", \"Segoe UI\", sans-serif";
  ctx.fillStyle = "#efefef";
  ctx.fillText("Visual Score", rightX + 420, r2 + boxH + 46);
  ctx.font = "700 36px \"Segoe UI Emoji\", \"Segoe UI Symbol\", sans-serif";
  ctx.fillStyle = "#fff4f4";
  ctx.fillText(starsText, rightX + 420, r2 + boxH + 85);
  ctx.font = "600 34px \"Bahnschrift\", \"Trebuchet MS\", \"Segoe UI\", sans-serif";
  ctx.fillStyle = "#efefef";
  const uidText = fitText(ctx, `User ID: ${String(user?.id || "-")}`, rightW - 70);
  ctx.fillText(uidText, rightX + 34, r2 + boxH + 123);

  return canvas.toBuffer("image/png");
}

function addCompletion(helperId, ratingOrNull) {
  const rec = ensureHelper(helperId);
  rec.ticketsCompleted += 1;
  rec.points += Number(config.pointsPerTicket || 0);
  if (typeof ratingOrNull === "number") {
    rec.ratingSum += ratingOrNull;
    rec.ratingCount += 1;
  }
  helpers[helperId] = rec;
  saveHelpers();
  touchWeekly();
  weekly.helpers[helperId] = (weekly.helpers[helperId] || 0) + 1;
  saveWeekly();
}

function logCompletionOnce(channelId, helperId, ratingOrNull) {
  const t = getTicket(channelId);
  if (!t) return false;
  if (t.completionLogged) return false;
  if (!isSnowflake(helperId)) return false;
  addCompletion(helperId, ratingOrNull);
  patchTicket(channelId, { completionLogged: true });
  return true;
}

function hasUserVouchedTicket(t, userId) {
  if (!t || !isSnowflake(String(userId || ""))) return false;
  const map = t.vouchedBy && typeof t.vouchedBy === "object" ? t.vouchedBy : {};
  return !!map[String(userId)];
}

function markUserVouchedTicket(channelId, userId) {
  const t = getTicket(channelId);
  if (!t || !isSnowflake(String(userId || ""))) return false;
  const map = t.vouchedBy && typeof t.vouchedBy === "object" ? { ...t.vouchedBy } : {};
  const key = String(userId);
  if (map[key]) return false;
  map[key] = { at: Date.now() };
  patchTicket(channelId, { vouchedBy: map, vouched: true });
  return true;
}

function shouldSendUnvouchedCloseNotice(t) {
  if (!t || t.kind !== "carry") return false;
  if (t.vouched) return false;
  if (t.vouchButtonTouched) return false;
  return true;
}

async function sendUnvouchedCloseNotice(t, closedByUserId) {
  if (!shouldSendUnvouchedCloseNotice(t)) return;
  const ownerId = String(t.owner || "");
  if (!isSnowflake(ownerId)) return;
  const owner = await client.users.fetch(ownerId).catch(() => null);
  if (!owner) return;

  const closerText = isSnowflake(String(closedByUserId || "")) ? `<@${closedByUserId}>` : "staff";
  const helperText = isSnowflake(String(t.claimed || "")) ? `<@${t.claimed}>` : "no helper";
  const game = String(t.game || "").toUpperCase();
  const ticketNo = String(t.ticket || "-");

  await owner.send(
    `${em(EMOJI_QUESTION, "question")} Your ticket #${ticketNo} (${game}) was closed by ${closerText} without a vouch.\n` +
    `Claimed helper: ${helperText}\n` +
    `If your service was completed, open a new ticket and ask staff to submit your feedback.`
  ).catch(() => {});
}

const GAMES = [
  { label: "Anime Vanguards", value: "av" },
  { label: "Universal Tower Defense", value: "utd" },
  { label: "Anime Crusaders", value: "ac" },
  { label: "Anime Paradox", value: "ap" },
];

function safeUserForChannel(str) {
  return (
    String(str || "")
      .toLowerCase()
      .replace(/[^a-z0-9]+/g, "_")
      .replace(/^_+|_+$/g, "")
      .slice(0, 24) || "user"
  );
}

function isTicketChannelName(name) {
  const n = String(name || "").toLowerCase();
  return n.startsWith("carry_") || n.startsWith("helper_");
}

function ticketTitle(t) {
  const tag = t.kind === "helper" ? "Application" : "Ticket";
  return `${t.game.toUpperCase()} - ${tag} #${t.ticket}`;
}

function ticketStatusHuman(t, overrideStatus) {
  if (overrideStatus) return overrideStatus;
  if (t.kind === "helper") return "Pending review";
  if (t.vouched) return "Vouch received";
  if (t.claimed) return `Claimed by <@${t.claimed}>`;
  return "Waiting for claim";
}

function buildTicketActionRows(t) {
  if (t.kind === "helper") {
    return [
      new ActionRowBuilder().addComponents(
        new ButtonBuilder()
          .setCustomId("ticket_staffclose")
          .setLabel("Staff Close")
          .setStyle(ButtonStyle.Secondary)
          .setEmoji(EMOJI_DONE_CLAIM)
      ),
    ];
  }

  const row1 = new ActionRowBuilder().addComponents(
    new ButtonBuilder()
      .setCustomId("ticket_claim")
      .setLabel("Claim")
      .setStyle(ButtonStyle.Secondary)
      .setEmoji(EMOJI_DONE_CLAIM)
      .setDisabled(!!t.claimed),
    new ButtonBuilder()
      .setCustomId("ticket_urgent")
      .setLabel("Urgent")
      .setStyle(ButtonStyle.Danger)
      .setEmoji(EMOJI_QUESTION)
      .setDisabled(!!t.urgent),
    new ButtonBuilder()
      .setCustomId("ticket_vouch")
      .setLabel("Vouch")
      .setStyle(ButtonStyle.Success)
      .setEmoji(EMOJI_HELPER)
      .setDisabled(!t.claimed),
    new ButtonBuilder()
      .setCustomId("ticket_staffclose")
      .setLabel("Staff Close")
      .setStyle(ButtonStyle.Secondary)
      .setEmoji(EMOJI_DONE_CLAIM)
  );

  const rows = [row1];
  if (t.shop) {
    rows.push(
      new ActionRowBuilder().addComponents(
        new ButtonBuilder()
          .setCustomId("ticket_shop")
          .setLabel("Get VIP (soon)")
          .setStyle(ButtonStyle.Primary)
          .setEmoji(EMOJI_TICKET)
      )
    );
  }
  return rows;
}

function buildTicketMessagePayload(t, opts = {}) {
  const status = ticketStatusHuman(t, opts.status);
  const createdAtRaw = t?.createdAt ?? null;
  const createdAtUnix =
    typeof createdAtRaw === "number"
      ? Math.floor(createdAtRaw / 1000)
      : Number.isFinite(Date.parse(String(createdAtRaw || "")))
        ? Math.floor(Date.parse(String(createdAtRaw)) / 1000)
        : null;
  const kindPriority = t.kind === "helper" ? "HELPER APPLICATION" : "CARRY REQUEST";
  const kindReason =
    t.kind === "helper"
      ? `Application for ${String(t.game || "").toUpperCase()}`
      : `Carry request for ${String(t.game || "").toUpperCase()}`;
  const ticketIdLabel = String(t.ticket || 0).padStart(4, "0");
  const handlingLines =
    t.kind === "helper"
      ? [
          `- Owner: <@${t.owner}>`,
          `- Claimed by: ${t.claimed ? `<@${t.claimed}>` : "-"}`,
          `- Review: ${t.vouched ? "Done" : "Pending"}`,
        ]
      : [
          `- Owner: <@${t.owner}>`,
          `- Claimed by: ${t.claimed ? `<@${t.claimed}>` : "-"}`,
          `- Vouch: ${t.vouched ? "Received" : "Pending"}`,
          `- Urgent: ${t.urgent ? "Yes" : "No"}`,
        ];
  const overviewLines = [
    `### ${em(EMOJI_TICKET, "ticket")} Ticket Overview`,
    `- ${em(EMOJI_TICKET, "ticket")} Ticket ID: **${ticketIdLabel}**`,
    `- ${em(EMOJI_HELPER, "helper")} Type: **${t.kind === "helper" ? "Become Helper" : "Ask for Carry"}**`,
    `- ${em(EMOJI_DONE_CLAIM, "done")} Status: **${status}**`,
    `- ${em(EMOJI_QUESTION, "question")} Created: ${createdAtUnix ? `<t:${createdAtUnix}:R>` : "-"}`,
  ];
  const priorityLines = [
    "### Priority",
    `- Level: **${kindPriority}**`,
    `- Reason: ${kindReason}`,
  ];
  const descriptionLines = [
    `### ${em(EMOJI_DESC, "desc")} Description`,
    clamp(t.request || "-", 900),
  ];

  const container = new ContainerBuilder()
    .addTextDisplayComponents(
      new TextDisplayBuilder().setContent(`## New Ticket <@${t.owner}> opened!`)
    )
    .addSeparatorComponents(new SeparatorBuilder())
    .addTextDisplayComponents(
      new TextDisplayBuilder().setContent(
        `${overviewLines.join("\n")}\n\n${priorityLines.join("\n")}\n\n${descriptionLines.join("\n")}\n\n### Handling\n${handlingLines.join("\n")}`
      )
    )
    .addActionRowComponents(...buildTicketActionRows(t));

  return {
    flags: MessageFlags.IsComponentsV2,
    components: [container],
  };
}

function buildMainPanelPayload() {
  const embed = new EmbedBuilder()
    .setColor(TICKET_EMBED_COLOR)
    .setTitle(`${em(EMOJI_TICKET, "ticket")} Ticket Panel`)
    .setDescription(
      "Welcome to our Carrying Service\n\n" +
      "How it works:\n" +
      `${em(EMOJI_QUESTION, "question")} Choose a ticket type from the menu\n` +
      `${em(EMOJI_TICKET, "ticket")} Select your game\n` +
      `${em(EMOJI_DESC, "desc")} Fill your details\n` +
      `${em(EMOJI_HELPER, "helper")} A helper will claim and assist you`
    )
    .addFields({
      name: "Supported games",
      value: `${em(EMOJI_TICKET, "ticket")} Anime Vanguards\n${em(EMOJI_TICKET, "ticket")} Universal Tower Defense\n${em(EMOJI_TICKET, "ticket")} Anime Crusaders\n${em(EMOJI_TICKET, "ticket")} Anime Paradox`,
    })
    .setImage(BANNER_MAIN_AND_CARRY);

  const row = new ActionRowBuilder().addComponents(
    new StringSelectMenuBuilder()
      .setCustomId("panel_entry_select")
      .setPlaceholder("Choose ticket type")
      .addOptions(
        {
          label: "Ask for Carry",
          value: "carry",
          description: "Open a carry ticket",
          emoji: { id: "1472405465257476238", name: "carry" },
        },
        {
          label: "Become Helper",
          value: "helper",
          description: "Submit a helper application",
          emoji: { id: "1472407311388774432", name: "helper" },
        }
      )
  );
  return { embeds: [embed], components: [row] };
}

function buildGameSelectorPayload(mode) {
  const title = mode === "helper" ? "## Helper Application" : "## Create Ticket";
  const desc = mode === "helper"
    ? "Select the game you want to apply for."
    : "Select the game you need help with.";
  const selectId = mode === "helper" ? "helper_select_game" : "carry_select_game";

  const container = new ContainerBuilder()
    .addTextDisplayComponents(new TextDisplayBuilder().setContent(`${title}\n${desc}`))
    .addSeparatorComponents(new SeparatorBuilder())
    .addActionRowComponents(
      new ActionRowBuilder().addComponents(
        new StringSelectMenuBuilder()
          .setCustomId(selectId)
          .setPlaceholder("Select game")
          .addOptions(GAMES)
      )
    );

  return {
    flags: MessageFlags.IsComponentsV2 | MessageFlags.Ephemeral,
    components: [container],
    allowedMentions: { parse: [] },
  };
}

function messageHasTicketPanel(msg) {
  if (!msg || !msg.components?.length) return false;
  const knownIds = new Set([
    "panel_entry_select",
    "carry_select_game",
    "helper_select_game",
    "ticket_open",
    "ticket_open_helper",
  ]);
  for (const row of msg.components) {
    for (const comp of row.components || []) {
      const cid = comp?.customId || comp?.data?.custom_id || null;
      if (cid && knownIds.has(String(cid))) return true;
    }
  }
  return false;
}

async function cleanupOldPanels(channel, botUserId) {
  if (!channel?.messages?.fetch) return 0;
  const recent = await channel.messages.fetch({ limit: 80 }).catch(() => null);
  if (!recent) return 0;
  let removed = 0;
  for (const msg of recent.values()) {
    if (String(msg.author?.id || "") !== String(botUserId || "")) continue;
    if (!messageHasTicketPanel(msg)) continue;
    await msg.delete().catch(() => {});
    removed += 1;
  }
  return removed;
}

async function updateTicketPanelMessage(channel, patch = {}) {
  const t = getTicket(channel.id);
  if (!t?.msg) return;
  const msg = await channel.messages.fetch(t.msg).catch(() => null);
  if (!msg) return;
  await msg.edit(buildTicketMessagePayload(t, { status: patch.status })).catch(() => {});
}

async function buildVouchImage({
  guildName,
  helperUser,
  reviewerUser,
  helperRank,
  reviewerKind,
  rating,
  comment,
  ticketLabel,
}) {
  const width = 1240;
  const height = 760;
  const canvas = createCanvas(width, height);
  const ctx = canvas.getContext("2d");
  const FONT_TITLE = "700 56px \"Bahnschrift\", \"Trebuchet MS\", \"Segoe UI\", sans-serif";
  const FONT_SUBTITLE = "500 24px \"Bahnschrift\", \"Trebuchet MS\", \"Segoe UI\", sans-serif";
  const FONT_SECTION = "700 30px \"Bahnschrift\", \"Trebuchet MS\", \"Segoe UI\", sans-serif";
  const FONT_LABEL = "600 22px \"Bahnschrift\", \"Trebuchet MS\", \"Segoe UI\", sans-serif";
  const FONT_BODY = "500 20px \"Trebuchet MS\", \"Segoe UI\", sans-serif";
  const FONT_BADGE = "700 22px \"Bahnschrift\", \"Trebuchet MS\", \"Segoe UI\", sans-serif";
  const FONT_STAR_TEXT = "600 22px \"Bahnschrift\", \"Trebuchet MS\", \"Segoe UI Emoji\", \"Segoe UI Symbol\", sans-serif";
  const FONT_STAR_GLYPH = "700 20px \"Segoe UI Emoji\", \"Segoe UI Symbol\", \"Trebuchet MS\", sans-serif";

  const bg = ctx.createLinearGradient(0, 0, width, height);
  bg.addColorStop(0, "#020202");
  bg.addColorStop(0.55, "#0a0a0a");
  bg.addColorStop(1, "#151515");
  ctx.fillStyle = bg;
  ctx.fillRect(0, 0, width, height);

  const haze = ctx.createRadialGradient(width * 0.5, 140, 30, width * 0.5, 140, 360);
  haze.addColorStop(0, "rgba(255,255,255,0.32)");
  haze.addColorStop(1, "rgba(255,255,255,0)");
  ctx.fillStyle = haze;
  ctx.fillRect(0, 0, width, height);
  const whiteHaze = ctx.createRadialGradient(width * 0.78, height * 0.2, 20, width * 0.78, height * 0.2, 360);
  whiteHaze.addColorStop(0, "rgba(255, 255, 255, 0.26)");
  whiteHaze.addColorStop(1, "rgba(255, 255, 255, 0)");
  ctx.fillStyle = whiteHaze;
  ctx.fillRect(0, 0, width, height);
  drawParticleField(ctx, width, height, ["255,255,255", "210,210,210", "255,55,55"], 70);

  function drawLightStreak(x, y, len, thickness, angle, colorA, colorB, alpha = 0.75) {
    ctx.save();
    ctx.translate(x, y);
    ctx.rotate(angle);
    const grad = ctx.createLinearGradient(-len / 2, 0, len / 2, 0);
    grad.addColorStop(0, colorA);
    grad.addColorStop(0.5, colorB);
    grad.addColorStop(1, colorA);
    ctx.strokeStyle = grad;
    ctx.globalAlpha = alpha;
    ctx.lineWidth = thickness;
    ctx.lineCap = "round";
    ctx.shadowColor = colorB;
    ctx.shadowBlur = 18;
    ctx.beginPath();
    ctx.moveTo(-len / 2, 0);
    ctx.lineTo(len / 2, 0);
    ctx.stroke();
    ctx.restore();
  }

  for (let i = 0; i < 26; i++) {
    drawLightStreak(
      Math.random() * width,
      Math.random() * (height * 0.92),
      80 + Math.random() * 220,
      1 + Math.random() * 2.6,
      (-0.8 + Math.random() * 1.6),
      "rgba(255,40,40,0)",
      i % 3 === 0 ? "rgba(255,255,255,0.94)" : "rgba(255,55,55,0.88)",
      0.28 + Math.random() * 0.44
    );
  }

  // nebula streak clouds for cinematic space look
  for (let i = 0; i < 14; i++) {
    const cx = Math.random() * width;
    const cy = Math.random() * (height * 0.75);
    const rx = 90 + Math.random() * 180;
    const ry = 24 + Math.random() * 56;
    const rot = (Math.random() - 0.5) * 1.1;
    const cloud = ctx.createRadialGradient(cx, cy, 8, cx, cy, rx);
    cloud.addColorStop(0, i % 2 ? "rgba(255,255,255,0.20)" : "rgba(255,50,50,0.16)");
    cloud.addColorStop(1, "rgba(0,0,0,0)");
    ctx.save();
    ctx.translate(cx, cy);
    ctx.rotate(rot);
    ctx.fillStyle = cloud;
    ctx.beginPath();
    ctx.ellipse(0, 0, rx, ry, 0, 0, Math.PI * 2);
    ctx.fill();
    ctx.restore();
  }

  // snow + black specks mix for dramatic texture
  for (let i = 0; i < 170; i++) {
    const px = Math.random() * width;
    const py = Math.random() * height;
    const pr = 0.6 + Math.random() * 2.2;
    const alpha = 0.16 + Math.random() * 0.55;
    ctx.save();
    ctx.shadowColor = `rgba(255,255,255,${Math.min(alpha + 0.12, 0.9).toFixed(3)})`;
    ctx.shadowBlur = 6 + Math.random() * 10;
    ctx.fillStyle = `rgba(245,245,245,${alpha.toFixed(3)})`;
    ctx.beginPath();
    ctx.arc(px, py, pr, 0, Math.PI * 2);
    ctx.fill();
    ctx.restore();
  }
  for (let i = 0; i < 120; i++) {
    const px = Math.random() * width;
    const py = Math.random() * height;
    const pr = 0.5 + Math.random() * 2.4;
    const alpha = 0.08 + Math.random() * 0.32;
    ctx.save();
    ctx.fillStyle = `rgba(0,0,0,${alpha.toFixed(3)})`;
    ctx.beginPath();
    ctx.arc(px, py, pr, 0, Math.PI * 2);
    ctx.fill();
    ctx.restore();
  }

  // ember layer near bottom for stronger fire atmosphere
  for (let i = 0; i < 120; i++) {
    const px = Math.random() * width;
    const py = height * 0.62 + Math.random() * (height * 0.38);
    const pr = 0.8 + Math.random() * 2.8;
    const alpha = 0.08 + Math.random() * 0.35;
    const emberColor = i % 2 === 0 ? "255,120,40" : "255,70,70";
    ctx.save();
    ctx.shadowColor = `rgba(${emberColor}, ${Math.min(alpha + 0.18, 0.8)})`;
    ctx.shadowBlur = 10 + Math.random() * 14;
    ctx.fillStyle = `rgba(${emberColor}, ${alpha.toFixed(3)})`;
    ctx.beginPath();
    ctx.arc(px, py, pr, 0, Math.PI * 2);
    ctx.fill();
    ctx.restore();
  }

  roundedRectPath(ctx, 16, 16, width - 32, height - 32, 24);
  ctx.fillStyle = "#090909";
  ctx.fill();
  ctx.strokeStyle = "#f1f1f1";
  ctx.lineWidth = 2;
  ctx.stroke();
  roundedRectPath(ctx, 24, 24, width - 48, height - 48, 20);
  ctx.strokeStyle = "rgba(255,70,70,0.65)";
  ctx.lineWidth = 1.2;
  ctx.stroke();
  ctx.save();
  ctx.globalAlpha = 0.35;
  roundedRectPath(ctx, 28, 28, width - 56, 90, 14);
  ctx.fillStyle = "rgba(255,255,255,0.08)";
  ctx.fill();
  ctx.restore();
  drawLightStreak(width / 2, 20, width - 120, 2.8, 0, "rgba(255,255,255,0.0)", "rgba(255,255,255,0.95)", 0.95);

  function drawLensFlare(x, y, radius = 22, alpha = 0.85) {
    const flare = ctx.createRadialGradient(x, y, 0, x, y, radius);
    flare.addColorStop(0, `rgba(255,255,255,${alpha})`);
    flare.addColorStop(0.35, `rgba(255,80,80,${(alpha * 0.75).toFixed(3)})`);
    flare.addColorStop(1, "rgba(255,80,80,0)");
    ctx.save();
    ctx.fillStyle = flare;
    ctx.beginPath();
    ctx.arc(x, y, radius, 0, Math.PI * 2);
    ctx.fill();
    ctx.restore();
  }

  drawLensFlare(30, 30, 18, 0.9);
  drawLensFlare(width - 30, 30, 18, 0.9);
  drawLensFlare(30, height - 30, 18, 0.9);
  drawLensFlare(width - 30, height - 30, 18, 0.9);
  drawLensFlare(width * 0.34, 26, 10, 0.7);
  drawLensFlare(width * 0.66, 26, 10, 0.7);
  drawLensFlare(width * 0.12, height * 0.44, 11, 0.78);
  drawLensFlare(width * 0.88, height * 0.44, 11, 0.78);

  const title = "Vouches for the Mohgs Server";
  ctx.font = FONT_TITLE;
  ctx.fillStyle = "#ffffff";
  ctx.shadowColor = "rgba(255,255,255,0.5)";
  ctx.shadowBlur = 16;
  ctx.fillText(title, (width - ctx.measureText(title).width) / 2, 92);
  ctx.shadowBlur = 0;

  ctx.font = FONT_SUBTITLE;
  ctx.fillStyle = "#d9d9d9";
  const sub = String(guildName || "MOHGS").toUpperCase();
  ctx.fillText(sub, (width - ctx.measureText(sub).width) / 2, 126);

  const score = Number(rating) || 0;
  const stars = starsFromRating(score);
  const reviewerStatus = String(reviewerKind || "Member");
  const helperName = helperUser?.tag || helperUser?.username || "Unknown";
  const reviewerName = reviewerUser?.tag || reviewerUser?.username || "Unknown";
  const safeComment = clamp(String(comment || "-"), 420);

  const leftX = 40;
  const cardY = 170;
  const cardW = 360;
  const cardH = 540;
  const rightX = width - leftX - cardW;

  // dramatic center glow behind avatar
  const centerGlow = ctx.createRadialGradient(width * 0.5, 390, 30, width * 0.5, 390, 230);
  centerGlow.addColorStop(0, "rgba(255,255,255,0.26)");
  centerGlow.addColorStop(1, "rgba(255,255,255,0)");
  ctx.fillStyle = centerGlow;
  ctx.fillRect(width * 0.5 - 240, 170, 480, 460);

  function drawSmokeWisp(cx, cy, w, h) {
    const smoke = ctx.createRadialGradient(cx, cy, 4, cx, cy, Math.max(w, h));
    smoke.addColorStop(0, "rgba(190,190,190,0.16)");
    smoke.addColorStop(1, "rgba(120,120,120,0)");
    ctx.save();
    ctx.fillStyle = smoke;
    ctx.beginPath();
    ctx.ellipse(cx, cy, w, h, Math.random() * 0.5, 0, Math.PI * 2);
    ctx.fill();
    ctx.restore();
  }

  function drawRealisticFlame(cx, baseY, w, h) {
    const sway = (Math.random() - 0.5) * (w * 0.35);
    const tipX = cx + sway;
    const tipY = baseY - h;

    // Outer flame shell (red/orange)
    ctx.save();
    const shell = ctx.createLinearGradient(cx, tipY, cx, baseY + 3);
    shell.addColorStop(0, "rgba(255, 90, 30, 0.85)");
    shell.addColorStop(0.55, "rgba(255, 60, 20, 0.70)");
    shell.addColorStop(1, "rgba(255, 40, 20, 0.08)");
    ctx.fillStyle = shell;
    ctx.shadowColor = "rgba(255, 70, 35, 0.90)";
    ctx.shadowBlur = 34;
    ctx.beginPath();
    ctx.moveTo(cx - w / 2, baseY);
    ctx.bezierCurveTo(cx - w * 0.65, baseY - h * 0.42, tipX - w * 0.28, baseY - h * 0.82, tipX, tipY);
    ctx.bezierCurveTo(tipX + w * 0.26, baseY - h * 0.8, cx + w * 0.64, baseY - h * 0.42, cx + w / 2, baseY);
    ctx.closePath();
    ctx.fill();
    ctx.restore();

    // Hot core (yellow/white)
    ctx.save();
    const coreW = w * 0.48;
    const coreH = h * 0.72;
    const core = ctx.createLinearGradient(cx, tipY + h * 0.22, cx, baseY);
    core.addColorStop(0, "rgba(255, 250, 185, 0.95)");
    core.addColorStop(0.45, "rgba(255, 205, 95, 0.88)");
    core.addColorStop(1, "rgba(255, 125, 40, 0.18)");
    ctx.fillStyle = core;
    ctx.shadowColor = "rgba(255, 200, 100, 0.75)";
    ctx.shadowBlur = 22;
    ctx.beginPath();
    ctx.moveTo(cx - coreW / 2, baseY - 1);
    ctx.bezierCurveTo(cx - coreW * 0.62, baseY - coreH * 0.42, tipX - coreW * 0.18, baseY - coreH * 0.86, tipX, baseY - coreH);
    ctx.bezierCurveTo(tipX + coreW * 0.18, baseY - coreH * 0.86, cx + coreW * 0.62, baseY - coreH * 0.42, cx + coreW / 2, baseY - 1);
    ctx.closePath();
    ctx.fill();
    ctx.restore();

    // Blue-ish base hint for realism
    ctx.save();
    const baseGlow = ctx.createRadialGradient(cx, baseY - 2, 2, cx, baseY - 2, w * 0.34);
    baseGlow.addColorStop(0, "rgba(135, 180, 255, 0.24)");
    baseGlow.addColorStop(1, "rgba(135, 180, 255, 0)");
    ctx.fillStyle = baseGlow;
    ctx.beginPath();
    ctx.ellipse(cx, baseY - 2, w * 0.36, h * 0.09, 0, 0, Math.PI * 2);
    ctx.fill();
    ctx.restore();
  }

  // fire bed shadow
  const fireBed = ctx.createRadialGradient(width * 0.5, 634, 20, width * 0.5, 634, 200);
  fireBed.addColorStop(0, "rgba(255,90,30,0.30)");
  fireBed.addColorStop(1, "rgba(255,90,30,0)");
  ctx.fillStyle = fireBed;
  ctx.fillRect(width * 0.5 - 240, 560, 480, 160);

  // dense layered flames
  for (let layer = 0; layer < 2; layer++) {
    const count = layer === 0 ? 16 : 12;
    for (let i = 0; i < count; i++) {
      const spread = layer === 0 ? 180 : 145;
      const cx = width * 0.5 - spread + (i / (count - 1)) * spread * 2 + (Math.random() - 0.5) * 10;
      const flameH = (layer === 0 ? 34 : 52) + Math.random() * (layer === 0 ? 20 : 34);
      const flameW = (layer === 0 ? 20 : 18) + Math.random() * 10;
      const baseY = (layer === 0 ? 654 : 646) + Math.random() * 7;
      drawRealisticFlame(cx, baseY, flameW, flameH);
      if (Math.random() < 0.52) {
        drawSmokeWisp(cx + (Math.random() - 0.5) * 12, baseY - flameH - 12 - Math.random() * 12, 11 + Math.random() * 13, 7 + Math.random() * 8);
      }
    }
  }

  // rising spark trails
  for (let i = 0; i < 80; i++) {
    const sx = width * 0.5 - 170 + Math.random() * 340;
    const sy = 620 - Math.random() * 230;
    const len = 2 + Math.random() * 9;
    const alpha = 0.18 + Math.random() * 0.58;
    ctx.save();
    ctx.strokeStyle = `rgba(255, ${120 + Math.floor(Math.random() * 90)}, ${40 + Math.floor(Math.random() * 30)}, ${alpha.toFixed(3)})`;
    ctx.lineWidth = 0.8 + Math.random() * 1.4;
    ctx.shadowColor = "rgba(255,140,70,0.85)";
    ctx.shadowBlur = 10;
    ctx.beginPath();
    ctx.moveTo(sx, sy);
    ctx.lineTo(sx + (Math.random() - 0.5) * 4, sy - len);
    ctx.stroke();
    ctx.restore();
  }

  // side glow rails to make cards pop
  const leftRail = ctx.createLinearGradient(leftX, cardY, leftX + 16, cardY + cardH);
  leftRail.addColorStop(0, "rgba(255,255,255,0)");
  leftRail.addColorStop(0.5, "rgba(255,255,255,0.55)");
  leftRail.addColorStop(1, "rgba(255,255,255,0)");
  ctx.fillStyle = leftRail;
  ctx.fillRect(leftX - 10, cardY + 8, 12, cardH - 16);

  const rightRail = ctx.createLinearGradient(rightX + cardW, cardY, rightX + cardW - 16, cardY + cardH);
  rightRail.addColorStop(0, "rgba(255,255,255,0)");
  rightRail.addColorStop(0.5, "rgba(255,255,255,0.55)");
  rightRail.addColorStop(1, "rgba(255,255,255,0)");
  ctx.fillStyle = rightRail;
  ctx.fillRect(rightX + cardW - 2, cardY + 8, 12, cardH - 16);

  roundedRectPath(ctx, leftX, cardY, cardW, cardH, 18);
  const leftCardBg = ctx.createLinearGradient(leftX, cardY, leftX + cardW, cardY + cardH);
  leftCardBg.addColorStop(0, "rgba(12,12,12,0.94)");
  leftCardBg.addColorStop(1, "rgba(5,5,5,0.9)");
  ctx.fillStyle = leftCardBg;
  ctx.fill();
  ctx.strokeStyle = "#f5f5f5";
  ctx.lineWidth = 2;
  ctx.stroke();
  roundedRectPath(ctx, leftX + 8, cardY + 8, cardW - 16, cardH - 16, 14);
  ctx.strokeStyle = "rgba(255,40,40,0.55)";
  ctx.lineWidth = 1.1;
  ctx.stroke();

  roundedRectPath(ctx, rightX, cardY, cardW, cardH, 18);
  const rightCardBg = ctx.createLinearGradient(rightX, cardY, rightX + cardW, cardY + cardH);
  rightCardBg.addColorStop(0, "rgba(12,12,12,0.94)");
  rightCardBg.addColorStop(1, "rgba(5,5,5,0.9)");
  ctx.fillStyle = rightCardBg;
  ctx.fill();
  ctx.strokeStyle = "#f5f5f5";
  ctx.lineWidth = 2;
  ctx.stroke();
  roundedRectPath(ctx, rightX + 8, cardY + 8, cardW - 16, cardH - 16, 14);
  ctx.strokeStyle = "rgba(255,40,40,0.55)";
  ctx.lineWidth = 1.1;
  ctx.stroke();

  // nebula bloom inside side cards
  const leftNebula = ctx.createRadialGradient(leftX + cardW * 0.4, cardY + cardH * 0.82, 20, leftX + cardW * 0.4, cardY + cardH * 0.82, 180);
  leftNebula.addColorStop(0, "rgba(255,255,255,0.16)");
  leftNebula.addColorStop(1, "rgba(255,255,255,0)");
  ctx.fillStyle = leftNebula;
  ctx.fillRect(leftX + 14, cardY + 14, cardW - 28, cardH - 28);
  const rightNebula = ctx.createRadialGradient(rightX + cardW * 0.62, cardY + cardH * 0.82, 20, rightX + cardW * 0.62, cardY + cardH * 0.82, 180);
  rightNebula.addColorStop(0, "rgba(255,255,255,0.16)");
  rightNebula.addColorStop(1, "rgba(255,255,255,0)");
  ctx.fillStyle = rightNebula;
  ctx.fillRect(rightX + 14, cardY + 14, cardW - 28, cardH - 28);

  const centerAvatarSize = 220;
  const centerAvatarX = Math.floor((width - centerAvatarSize) / 2);
  const centerAvatarY = 250;
  const centerAvatarUrl = reviewerUser?.displayAvatarURL
    ? reviewerUser.displayAvatarURL({ extension: "png", size: 512, forceStatic: true })
    : helperUser?.displayAvatarURL
      ? helperUser.displayAvatarURL({ extension: "png", size: 512, forceStatic: true })
    : "https://cdn.discordapp.com/embed/avatars/0.png";
  await drawNeonAvatar(ctx, centerAvatarUrl, centerAvatarX, centerAvatarY, centerAvatarSize, {
    outerStroke: "#ffffff",
    outerGlow: "rgba(255,255,255,0.95)",
  });

  // orbital neon rings around avatar (closer to reference style)
  const ringCx = centerAvatarX + centerAvatarSize / 2;
  const ringCy = centerAvatarY + centerAvatarSize / 2;
  const ringRadii = [132, 146, 160, 172];
  for (let i = 0; i < ringRadii.length; i++) {
    const rr = ringRadii[i];
    ctx.save();
    const whiteRing = i === 3;
    ctx.strokeStyle = i === 1 ? "rgba(255,55,55,0.94)" : whiteRing ? "rgba(255,255,255,0.9)" : "rgba(255,255,255,0.94)";
    ctx.lineWidth = i === 1 ? 1.8 : whiteRing ? 1.8 : 2.8;
    ctx.shadowColor = i === 1 ? "rgba(255,55,55,0.82)" : whiteRing ? "rgba(255,255,255,0.72)" : "rgba(255,255,255,0.86)";
    ctx.shadowBlur = i === 1 ? 12 : whiteRing ? 15 : 22;
    ctx.setLineDash(i === 2 ? [18, 10] : whiteRing ? [10, 8] : []);
    ctx.beginPath();
    ctx.arc(ringCx, ringCy, rr, Math.PI * (0.12 + i * 0.06), Math.PI * (1.96 - i * 0.05));
    ctx.stroke();
    ctx.restore();
  }
  drawLensFlare(ringCx + 148, ringCy - 8, 10, 0.9);
  drawLensFlare(ringCx - 152, ringCy + 16, 9, 0.75);

  roundedRectPath(ctx, centerAvatarX + 28, centerAvatarY + centerAvatarSize + 20, centerAvatarSize - 56, 44, 12);
  ctx.fillStyle = "#121212";
  ctx.fill();
  ctx.strokeStyle = "#f2f2f2";
  ctx.lineWidth = 1.5;
  ctx.stroke();
  ctx.fillStyle = "#ffffff";
  ctx.font = FONT_BADGE;
  const badge = "VOUCH";
  ctx.fillText(badge, centerAvatarX + (centerAvatarSize - ctx.measureText(badge).width) / 2, centerAvatarY + centerAvatarSize + 50);

  // horizontal heat wave glow around center
  const heatWave = ctx.createLinearGradient(centerAvatarX - 70, centerAvatarY + centerAvatarSize + 30, centerAvatarX + centerAvatarSize + 70, centerAvatarY + centerAvatarSize + 30);
  heatWave.addColorStop(0, "rgba(255,80,40,0)");
  heatWave.addColorStop(0.5, "rgba(255,80,40,0.45)");
  heatWave.addColorStop(1, "rgba(255,80,40,0)");
  ctx.fillStyle = heatWave;
  ctx.fillRect(centerAvatarX - 80, centerAvatarY + centerAvatarSize + 56, centerAvatarSize + 160, 10);

  ctx.fillStyle = "#ffffff";
  ctx.font = FONT_SECTION;
  ctx.fillText("Helper Info", leftX + 24, cardY + 50);
  await drawLineWithEmoji(ctx, {
    emojiId: EMOJI_HELPER,
    text: `User: ${fitText(ctx, helperName, 250)}`,
    x: leftX + 24,
    y: cardY + 104,
    font: FONT_LABEL,
    color: "#f0f0f0",
    iconSize: 20,
  });
  await drawLineWithEmoji(ctx, {
    emojiId: EMOJI_RANK,
    text: `Rank: ${helperRank || "-"}`,
    x: leftX + 24,
    y: cardY + 148,
    font: FONT_LABEL,
    color: "#f0f0f0",
    iconSize: 20,
  });
  await drawLineWithEmoji(ctx, {
    emojiId: EMOJI_TICKET,
    text: `Ticket: ${ticketLabel || "-"}`,
    x: leftX + 24,
    y: cardY + 192,
    font: FONT_LABEL,
    color: "#f0f0f0",
    iconSize: 20,
  });

  ctx.fillStyle = "#ffffff";
  ctx.font = "700 27px \"Bahnschrift\", \"Trebuchet MS\", \"Segoe UI\", sans-serif";
  ctx.fillText("Comment", leftX + 24, cardY + 250);
  roundedRectPath(ctx, leftX + 20, cardY + 266, cardW - 40, cardH - 290, 12);
  ctx.fillStyle = "#0d0d0d";
  ctx.fill();
  ctx.strokeStyle = "#9a9a9a";
  ctx.lineWidth = 1.4;
  ctx.stroke();

  ctx.font = FONT_BODY;
  ctx.fillStyle = "#f1f1f1";
  const words = safeComment.split(/\s+/);
  const commentLines = [];
  let current = "";
  for (const w of words) {
    const test = current ? `${current} ${w}` : w;
    if (ctx.measureText(test).width > cardW - 74 && current) {
      commentLines.push(current);
      current = w;
    } else {
      current = test;
    }
  }
  if (current) commentLines.push(current);
  const finalComment = commentLines.slice(0, 7);
  if (commentLines.length > 7) {
    finalComment[6] = fitText(ctx, `${finalComment[6]}...`, cardW - 74);
  }
  for (let i = 0; i < finalComment.length; i++) {
    ctx.fillText(finalComment[i], leftX + 34, cardY + 300 + i * 30);
  }

  ctx.fillStyle = "#ffffff";
  ctx.font = FONT_SECTION;
  ctx.fillText("Reviewer Info", rightX + 24, cardY + 50);
  await drawLineWithEmoji(ctx, {
    emojiId: EMOJI_REVIEWER,
    text: `User: ${fitText(ctx, reviewerName, 250)}`,
    x: rightX + 24,
    y: cardY + 104,
    font: FONT_LABEL,
    color: "#f0f0f0",
    iconSize: 20,
  });
  await drawLineWithEmoji(ctx, {
    emojiId: EMOJI_REVIEWER,
    text: `Status: ${reviewerStatus}`,
    x: rightX + 24,
    y: cardY + 148,
    font: FONT_LABEL,
    color: "#f0f0f0",
    iconSize: 20,
  });
  await drawLineWithEmoji(ctx, {
    emojiId: EMOJI_STARS,
    text: `Rating: ${score}/5`,
    x: rightX + 24,
    y: cardY + 192,
    font: FONT_STAR_TEXT,
    color: "#f0f0f0",
    iconSize: 20,
  });
  await drawLineWithEmoji(ctx, {
    emojiId: EMOJI_STARS,
    text: `Stars`,
    x: rightX + 24,
    y: cardY + 236,
    font: FONT_STAR_TEXT,
    color: "#ffffff",
    iconSize: 20,
  });

  const normalizedScore = Math.max(0, Math.min(5, Number(score) || 0));
  const starBarX = rightX + 24;
  const starBarY = cardY + 274;
  const starBarW = cardW - 48;
  const starBarH = 56;
  roundedRectPath(ctx, starBarX, starBarY, starBarW, starBarH, 14);
  const barBg = ctx.createLinearGradient(starBarX, starBarY, starBarX, starBarY + starBarH);
  barBg.addColorStop(0, "rgba(20,20,20,0.95)");
  barBg.addColorStop(1, "rgba(8,8,8,0.95)");
  ctx.fillStyle = barBg;
  ctx.fill();
  ctx.strokeStyle = "rgba(255,255,255,0.86)";
  ctx.lineWidth = 1.8;
  ctx.stroke();

  const fillW = Math.max(0, Math.min(starBarW, (normalizedScore / 5) * starBarW));
  roundedRectPath(ctx, starBarX + 2, starBarY + 2, Math.max(4, fillW - 4), starBarH - 4, 12);
  const fillGrad = ctx.createLinearGradient(starBarX, starBarY, starBarX + starBarW, starBarY);
  fillGrad.addColorStop(0, "rgba(255,255,255,0.95)");
  fillGrad.addColorStop(0.55, "rgba(255,90,90,0.95)");
  fillGrad.addColorStop(1, "rgba(255,40,40,0.92)");
  ctx.fillStyle = fillGrad;
  ctx.fill();

  ctx.save();
  ctx.shadowColor = "rgba(255,170,80,0.95)";
  ctx.shadowBlur = 20;
  ctx.font = "700 50px \"Segoe UI Emoji\", \"Segoe UI Symbol\", sans-serif";
  ctx.fillStyle = "#ffffff";
  const starsText = "\u2605 \u2605 \u2605 \u2605 \u2605";
  ctx.fillText(starsText, starBarX + 18, starBarY + 42);
  ctx.restore();

  drawLightStreak(starBarX + starBarW / 2, starBarY + starBarH + 2, starBarW - 14, 4, 0, "rgba(255,120,30,0)", "rgba(255,160,60,0.92)", 0.9);

  // reflective floor glow at bottom
  const floorGlow = ctx.createLinearGradient(0, height - 90, 0, height);
  floorGlow.addColorStop(0, "rgba(255,255,255,0)");
  floorGlow.addColorStop(0.5, "rgba(255,255,255,0.16)");
  floorGlow.addColorStop(1, "rgba(255,50,50,0.22)");
  ctx.fillStyle = floorGlow;
  ctx.fillRect(0, height - 90, width, 90);
  drawLightStreak(width / 2, height - 10, width - 120, 3.4, 0, "rgba(255,80,80,0)", "rgba(255,245,245,0.95)", 0.8);

  ctx.font = "600 20px \"Bahnschrift\", \"Trebuchet MS\", \"Segoe UI\", sans-serif";
  ctx.fillStyle = "#ffffff";
  ctx.fillText(`${normalizedScore.toFixed(2)} / 5`, rightX + 24, starBarY + 84);

  return canvas.toBuffer("image/png");
}
/* ===================== READY + MSG COUNT ===================== */
client.once(Events.ClientReady, () => console.log(`✅ Logged in as ${client.user.tag}`));
client.on("error", (e) => console.error("Client error:", e?.message || e));

client.on(Events.MessageCreate, (message) => {
  if (message.guild) return;
  scheduleDmAutoDelete(message);
});

client.on(Events.MessageCreate, (message) => {
  if (!message.guild || message.author.bot || !isAllowedGuild(message.guild.id)) return;
  bumpMsg(message.author.id);
});

/* ===================== SLASH COMMANDS ===================== */
const slashCommands = [
  new SlashCommandBuilder()
    .setName("createembed1")
    .setDescription("Send ticket panel"),
  new SlashCommandBuilder()
    .setName("shop")
    .setDescription("VIP shop links"),
  new SlashCommandBuilder()
    .setName("view")
    .setDescription("View profiles")
    .addSubcommand((sc) =>
      sc
        .setName("profile")
        .setDescription("View helper profile")
        .addUserOption((opt) =>
          opt.setName("user").setDescription("User").setRequired(false)
        )
    ),
  new SlashCommandBuilder()
    .setName("leaderboard")
    .setDescription("Helper leaderboard (all time)"),
  new SlashCommandBuilder()
    .setName("weeklyleaderboard")
    .setDescription("Helper leaderboard (weekly)"),
].map((c) => c.toJSON());

/* ===================== REGISTER COMMANDS ===================== */
client.once(Events.ClientReady, async () => {
  await hydrateStateFromSupabase();
  const rest = new REST({ version: "10", timeout: 30_000 }).setToken(BOT_TOKEN);
  try {
    const targetGuildIds = getConfiguredGuildIds();
    for (const gid of targetGuildIds) {
      if (!client.guilds.cache.has(gid)) continue;
      try {
        await rest.put(
          Routes.applicationGuildCommands(client.user.id, gid),
          { body: slashCommands }
        );
      } catch (e) {
        console.error(`Slash registration failed for guild ${gid}:`, e?.message || e);
      }
    }
  } catch (e) {
    console.error("Slash registration error:", e);
  }
});

async function safeDefer(interaction, ephemeral = true) {
  try {
    if (!interaction.deferred && !interaction.replied) {
      await interaction.deferReply({
        flags: ephemeral ? MessageFlags.Ephemeral : undefined,
      });
    }
  } catch {}
}

async function safeReply(interaction, payload) {
  try {
    if (interaction.deferred || interaction.replied) return await interaction.editReply(payload);
    return await interaction.reply(payload);
  } catch {}
}

async function safeShowModal(interaction, modal) {
  try {
    await interaction.showModal(modal);
    return true;
  } catch (e) {
    if (e?.code === 10062) {
      console.warn(`Interaction expired before modal (${interaction.customId || "unknown_custom_id"}).`);
      return false;
    }
    throw e;
  }
}

/* ===================== COMMAND HANDLER ===================== */
client.on(Events.InteractionCreate, async (interaction) => {
  try {
    if (!interaction.isChatInputCommand()) return;
    if (!isAllowedGuild(interaction.guildId)) {
      return interaction.reply({
        content: "This bot is only active in the configured main guild.",
        flags: MessageFlags.Ephemeral,
      }).catch(() => {});
    }

    const cmd = interaction.commandName;
    const s = guildSettings(interaction.guildId);
    const isPublicCmd =
      cmd === "leaderboard" ||
      cmd === "weeklyleaderboard" ||
      (cmd === "view" && interaction.options.getSubcommand() === "profile");
    await safeDefer(interaction, !isPublicCmd);

    if (cmd === "createembed1") {
      const isOwner = String(interaction.user.id) === String(config.ownerId || "");
      if (!isOwner && !interaction.memberPermissions?.has(PermissionsBitField.Flags.Administrator)) {
        return safeReply(interaction, { content: "⛔ Admin only." });
      }
      const removed = await cleanupOldPanels(interaction.channel, client.user?.id);
      await interaction.channel.send(buildMainPanelPayload());
      return safeReply(interaction, {
        content: removed > 0 ? `✅ Panel sent. Removed ${removed} old panel message(s).` : "✅ Panel sent.",
      });
    }

    if (cmd === "shop") {
      if (!s.robuxLink || !s.moneyLink) {
        return safeReply(interaction, { content: "Shop links are not configured for this guild." });
      }
      const embed = new EmbedBuilder()
        .setColor(TICKET_EMBED_COLOR)
        .setTitle("🛒 VIP Shop")
        .setDescription("Choose a payment option:");
      const row = new ActionRowBuilder().addComponents(
        new ButtonBuilder().setStyle(ButtonStyle.Link).setLabel("Buy with Robux").setURL(s.robuxLink),
        new ButtonBuilder().setStyle(ButtonStyle.Link).setLabel("Buy with Money").setURL(s.moneyLink)
      );
      return safeReply(interaction, { embeds: [embed], components: [row] });
    }

    if (cmd === "view" && interaction.options.getSubcommand() === "profile") {
      const user = interaction.options.getUser("user") || interaction.user;
      const rec = ensureHelper(user.id);
      const png = await buildProfileImage(user, rec);
      const file = new AttachmentBuilder(png, { name: "helper-profile.png" });
      return safeReply(interaction, { files: [file] });
    }

    if (cmd === "leaderboard") {
      const top = Object.entries(helpers)
        .map(([id, r]) => ({
          id,
          tickets: r.ticketsCompleted || 0,
          rating: avgRating(r),
        }))
        .sort((a, b) => b.tickets - a.tickets)
        .slice(0, 10);
      const rows = [];
      for (let i = 0; i < top.length; i++) {
        const member = await interaction.guild.members.fetch(top[i].id).catch(() => null);
        const user = member?.user || null;
        rows.push({
          name: member ? member.displayName : `User ${top[i].id}`,
          avatarUrl: user
            ? user.displayAvatarURL({ extension: "png", size: 128, forceStatic: true })
            : "https://cdn.discordapp.com/embed/avatars/0.png",
          tickets: top[i].tickets,
          rating: top[i].rating,
        });
      }
      const png = await buildLeaderboardImage(interaction.guild, rows);
      const file = new AttachmentBuilder(png, { name: "mohgs-leaderboard.png" });
      return safeReply(interaction, { files: [file] });
    }

    if (cmd === "weeklyleaderboard") {
      touchWeekly();
      const top = Object.entries(weekly.helpers || {})
        .map(([id, count]) => ({ id, tickets: count }))
        .sort((a, b) => b.tickets - a.tickets)
        .slice(0, 10);
      const rows = [];
      for (let i = 0; i < top.length; i++) {
        const member = await interaction.guild.members.fetch(top[i].id).catch(() => null);
        const user = member?.user || null;
        const allTime = helpers[top[i].id] || {};
        rows.push({
          name: member ? member.displayName : `User ${top[i].id}`,
          avatarUrl: user
            ? user.displayAvatarURL({ extension: "png", size: 128, forceStatic: true })
            : "https://cdn.discordapp.com/embed/avatars/0.png",
          tickets: top[i].tickets,
          rating: avgRating(allTime),
        });
      }
      const png = await buildLeaderboardImage(interaction.guild, rows);
      const file = new AttachmentBuilder(png, { name: "mohgs-weekly-leaderboard.png" });
      return safeReply(interaction, { files: [file] });
    }

    return safeReply(interaction, { content: "Unknown command." });
  } catch (e) {
    console.error("COMMAND HANDLER ERROR:", e);
    if (interaction?.isRepliable?.()) {
      try {
        if (interaction.deferred || interaction.replied) {
          await interaction.editReply({ content: "❌ Error occurred. Check bot logs." });
        } else {
          await interaction.reply({
            content: "❌ Error occurred. Check bot logs.",
            flags: MessageFlags.Ephemeral,
          });
        }
      } catch {}
    }
  }
});

/* ===================== BASIC UI INTERACTIONS ===================== */
const selectedCarryGameByUser = new Map();
client.on(Events.InteractionCreate, async (interaction) => {
  try {
    if (interaction.isChatInputCommand()) return;
    if (interaction.guildId && !isAllowedGuild(interaction.guildId)) return;

    if (interaction.isButton() && interaction.customId === "ticket_open") {
      return interaction.reply(buildGameSelectorPayload("carry"));
    }
    if (interaction.isButton() && interaction.customId === "ticket_open_helper") {
      return interaction.reply(buildGameSelectorPayload("helper"));
    }
    if (interaction.isStringSelectMenu() && interaction.customId === "panel_entry_select") {
      const value = interaction.values?.[0];
      if (value === "carry") return interaction.reply(buildGameSelectorPayload("carry"));
      if (value === "helper") return interaction.reply(buildGameSelectorPayload("helper"));
      return interaction.reply({
        content: "❌ Invalid selection.",
        flags: MessageFlags.Ephemeral,
      });
    }
    if (interaction.isStringSelectMenu() && interaction.customId === "carry_select_game") {
      const game = interaction.values?.[0];
      if (!GAMES.some((g) => g.value === game)) {
        return interaction.reply({
          content: "❌ Invalid selection.",
          flags: MessageFlags.Ephemeral,
        });
      }
      selectedCarryGameByUser.set(interaction.user.id, game);
      const modal = new ModalBuilder().setCustomId("carry_ticket_modal").setTitle("Ticket details");
      const nicknameInput = new TextInputBuilder()
        .setCustomId("nickname")
        .setLabel("In-game username")
        .setStyle(TextInputStyle.Short)
        .setRequired(true)
        .setMaxLength(32);
      const helpInput = new TextInputBuilder()
        .setCustomId("help_text")
        .setLabel("Request details")
        .setStyle(TextInputStyle.Paragraph)
        .setRequired(true)
        .setMaxLength(900);
      const psInput = new TextInputBuilder()
        .setCustomId("private_server")
        .setLabel("Private server? (Yes/No)")
        .setStyle(TextInputStyle.Short)
        .setRequired(true)
        .setMaxLength(5);
      modal.addComponents(
        new ActionRowBuilder().addComponents(nicknameInput),
        new ActionRowBuilder().addComponents(helpInput),
        new ActionRowBuilder().addComponents(psInput)
      );
      const ok = await safeShowModal(interaction, modal);
      if (!ok) return;
      return;
    }

    if (interaction.isStringSelectMenu() && interaction.customId === "helper_select_game") {
      await safeDefer(interaction);
      const game = interaction.values?.[0];
      if (!GAMES.some((g) => g.value === game)) {
        return safeReply(interaction, { content: "Invalid selection." });
      }

      const guild = interaction.guild;
      const user = interaction.user;
      const helperRoleId = helperRoleForGame(guild.id, game);
      if (!helperRoleId || !guild.roles.cache.has(helperRoleId)) {
        return safeReply(interaction, { content: "Helper role not configured." });
      }

      const ticketNum = nextTicketNumber();
      const uname = safeUserForChannel(interaction.member?.displayName || user.username);
      const channelName = `helper_${game}_${uname}_${ticketNum}`.slice(0, 95);

      const s = guildSettings(guild.id);
      let parentId = s.helperApplyCategoryId || null;
      if (parentId) {
        const parentCh = guild.channels.cache.get(parentId) || (await guild.channels.fetch(parentId).catch(() => null));
        if (!parentCh || parentCh.type !== ChannelType.GuildCategory) parentId = null;
      }

      const overwrites = [
        { id: guild.id, deny: [PermissionsBitField.Flags.ViewChannel] },
        {
          id: user.id,
          allow: [
            PermissionsBitField.Flags.ViewChannel,
            PermissionsBitField.Flags.SendMessages,
            PermissionsBitField.Flags.ReadMessageHistory,
            PermissionsBitField.Flags.AttachFiles,
          ],
        },
        ...existingRoleIds(guild, getApplicationViewRoles(guild.id)).map((rid) => ({
          id: rid,
          allow: [
            PermissionsBitField.Flags.ViewChannel,
            PermissionsBitField.Flags.SendMessages,
            PermissionsBitField.Flags.ReadMessageHistory,
          ],
        })),
        {
          id: client.user.id,
          allow: [
            PermissionsBitField.Flags.ViewChannel,
            PermissionsBitField.Flags.SendMessages,
            PermissionsBitField.Flags.ReadMessageHistory,
            PermissionsBitField.Flags.ManageChannels,
            PermissionsBitField.Flags.ManageMessages,
          ],
        },
      ];

      const channel = await guild.channels.create({
        name: channelName,
        type: ChannelType.GuildText,
        parent: parentId || undefined,
        permissionOverwrites: overwrites,
      });

      const t = {
        kind: "helper",
        owner: user.id,
        game,
        ticket: ticketNum,
        createdAt: Date.now(),
        msg: null,
        claimed: null,
        urgent: false,
        vouched: false,
        shop: false,
        completionLogged: false,
        request: "Please send screenshots of units + proof. Staff will review soon.",
      };
      const msg = await channel.send(buildTicketMessagePayload(t, { status: "Pending review" }));
      t.msg = msg.id;
      setTicket(channel.id, t);
      return safeReply(interaction, { content: `Application created: <#${channel.id}>` });
    }

    if (interaction.isModalSubmit() && interaction.customId === "carry_ticket_modal") {
      await safeDefer(interaction);
      const guild = interaction.guild;
      const user = interaction.user;
      const s = guildSettings(guild.id);
      let carryParentId = s.ticketCategoryId || null;
      if (carryParentId) {
        const parentCh = guild.channels.cache.get(carryParentId) || (await guild.channels.fetch(carryParentId).catch(() => null));
        if (!parentCh || parentCh.type !== ChannelType.GuildCategory) carryParentId = null;
      }

      const game = selectedCarryGameByUser.get(user.id);
      selectedCarryGameByUser.delete(user.id);
      if (!game) return safeReply(interaction, { content: "Please start again and select a game." });

      const nickname = interaction.fields.getTextInputValue("nickname")?.trim();
      const helpText = interaction.fields.getTextInputValue("help_text")?.trim();
      const psRaw = (interaction.fields.getTextInputValue("private_server") || "").trim();
      const privateServer = psRaw.toUpperCase().startsWith("Y") ? "YES" : "NO";
      if (!nickname || !helpText) return safeReply(interaction, { content: "Please fill in all fields." });

      const helperRoleId = helperRoleForGame(guild.id, game);
      if (!helperRoleId || !guild.roles.cache.has(helperRoleId)) {
        return safeReply(interaction, { content: "Helper role not configured." });
      }
      const staffRoleIds = existingRoleIds(guild, [...STAFF_CLOSE_ROLES]);
      const ticketNum = nextTicketNumber();
      const uname = safeUserForChannel(interaction.member?.displayName || user.username);
      const channelName = `carry_${game}_${uname}_${ticketNum}`.slice(0, 95);
      const showShop = (msgCounts[user.id] || 0) >= 50;

      const overwrites = [
        { id: guild.id, deny: [PermissionsBitField.Flags.ViewChannel] },
        {
          id: user.id,
          allow: [
            PermissionsBitField.Flags.ViewChannel,
            PermissionsBitField.Flags.SendMessages,
            PermissionsBitField.Flags.ReadMessageHistory,
            PermissionsBitField.Flags.AttachFiles,
          ],
        },
        {
          id: helperRoleId,
          allow: [
            PermissionsBitField.Flags.ViewChannel,
            PermissionsBitField.Flags.SendMessages,
            PermissionsBitField.Flags.ReadMessageHistory,
          ],
        },
        ...staffRoleIds.map((rid) => ({
          id: rid,
          allow: [
            PermissionsBitField.Flags.ViewChannel,
            PermissionsBitField.Flags.SendMessages,
            PermissionsBitField.Flags.ReadMessageHistory,
          ],
        })),
        {
          id: client.user.id,
          allow: [
            PermissionsBitField.Flags.ViewChannel,
            PermissionsBitField.Flags.SendMessages,
            PermissionsBitField.Flags.ReadMessageHistory,
            PermissionsBitField.Flags.ManageChannels,
            PermissionsBitField.Flags.ManageMessages,
          ],
        },
      ];

      const channel = await guild.channels.create({
        name: channelName,
        type: ChannelType.GuildText,
        parent: carryParentId || undefined,
        permissionOverwrites: overwrites,
      });

      const requestBlock = `IGN: ${nickname}\nPrivate server: ${privateServer}\n\n${helpText.slice(0, 900)}`;
      const t = {
        kind: "carry",
        owner: user.id,
        game,
        ticket: ticketNum,
        createdAt: Date.now(),
        msg: null,
        claimed: null,
        urgent: false,
        vouched: false,
        shop: showShop,
        completionLogged: false,
        request: requestBlock,
      };
      const msg = await channel.send(buildTicketMessagePayload(t, { status: "Waiting for claim" }));
      t.msg = msg.id;
      setTicket(channel.id, t);
      return safeReply(interaction, { content: `Ticket created: <#${channel.id}>` });
    }

    if (interaction.isButton() && interaction.customId === "ticket_shop") {
      await safeDefer(interaction);
      const s = guildSettings(interaction.guildId);
      if (!s.robuxLink || !s.moneyLink) {
        return safeReply(interaction, { content: "Shop links are not configured for this guild." });
      }
      const embed = new EmbedBuilder().setColor(TICKET_EMBED_COLOR).setTitle("VIP Shop").setDescription("Choose a payment option:");
      const row = new ActionRowBuilder().addComponents(
        new ButtonBuilder().setStyle(ButtonStyle.Link).setLabel("Buy with Robux").setURL(s.robuxLink),
        new ButtonBuilder().setStyle(ButtonStyle.Link).setLabel("Buy with Money").setURL(s.moneyLink)
      );
      return safeReply(interaction, { embeds: [embed], components: [row] });
    }

    if (interaction.isButton() && interaction.customId === "ticket_claim") {
      await safeDefer(interaction);
      const ch = interaction.channel;
      if (!ch || ch.type !== ChannelType.GuildText) return safeReply(interaction, { content: "Not a ticket channel." });
      const t = getTicket(ch.id);
      if (!t || t.kind !== "carry") return safeReply(interaction, { content: "Ticket not found." });
      if (!isGameHelper(interaction.member, t.game)) return safeReply(interaction, { content: "Helpers for this game only." });
      if (t.claimed) return safeReply(interaction, { content: `Already claimed by <@${t.claimed}>.` });
      patchTicket(ch.id, { claimed: interaction.user.id });
      await updateTicketPanelMessage(ch, { status: `Claimed by <@${interaction.user.id}>` });
      return safeReply(interaction, { content: "Ticket claimed." });
    }

    if (interaction.isButton() && interaction.customId === "ticket_urgent") {
      await safeDefer(interaction);
      const ch = interaction.channel;
      if (!ch || ch.type !== ChannelType.GuildText) return safeReply(interaction, { content: "Not a ticket channel." });
      const t = getTicket(ch.id);
      if (!t || t.kind !== "carry") return safeReply(interaction, { content: "Ticket not found." });
      if (!isVipOrBooster(interaction.member)) return safeReply(interaction, { content: "VIP/Booster only." });
      if (t.urgent) return safeReply(interaction, { content: "Urgent already used." });
      patchTicket(ch.id, { urgent: true });
      const pingRole = helperRoleForGame(interaction.guildId, t.game);
      if (pingRole) await ch.send(`URGENT - <@&${pingRole}>`).catch(() => {});
      await updateTicketPanelMessage(ch);
      return safeReply(interaction, { content: "Urgent ping sent." });
    }

    if (interaction.isButton() && interaction.customId === "ticket_staffclose") {
      await safeDefer(interaction);
      const ch = interaction.channel;
      if (!ch || ch.type !== ChannelType.GuildText) return safeReply(interaction, { content: "Not a ticket channel." });
      const t = getTicket(ch.id);
      if (!t) return safeReply(interaction, { content: "Ticket not found." });
      if (!canStaffClose(interaction.member, t.game)) return safeReply(interaction, { content: "No permission." });
      const notifyNoVouch = shouldSendUnvouchedCloseNotice(t);
      if (t.kind === "carry" && t.claimed && isSnowflake(t.claimed)) logCompletionOnce(ch.id, t.claimed, null);
      deleteTicket(ch.id);
      if (notifyNoVouch) {
        await sendUnvouchedCloseNotice(t, interaction.user.id);
      }
      await safeReply(interaction, { content: "Ticket closed." });
      return ch.delete().catch(() => {});
    }

    if (interaction.isButton() && interaction.customId === "ticket_vouch") {
      const ch = interaction.channel;
      if (!ch || ch.type !== ChannelType.GuildText) {
        return interaction
          .reply({ content: "Not a ticket channel.", flags: MessageFlags.Ephemeral })
          .catch(() => {});
      }
      const t = getTicket(ch.id);
      if (!t || t.kind !== "carry") {
        return interaction
          .reply({ content: "Ticket not found.", flags: MessageFlags.Ephemeral })
          .catch(() => {});
      }
      patchTicket(ch.id, { vouchButtonTouched: true });
      if (!t.claimed) {
        return interaction
          .reply({ content: "Vouch only after claim.", flags: MessageFlags.Ephemeral })
          .catch(() => {});
      }
      if (hasUserVouchedTicket(t, interaction.user.id)) {
        return interaction
          .reply({
            content: "You already submitted your vouch for this ticket.",
            flags: MessageFlags.Ephemeral,
          })
          .catch(() => {});
      }

      const modal = new ModalBuilder().setCustomId(`vouch_submit:${ch.id}`).setTitle("Vouch");
      const ratingInput = new TextInputBuilder()
        .setCustomId("rating")
        .setLabel("Rating (1-5)")
        .setStyle(TextInputStyle.Short)
        .setRequired(true)
        .setMinLength(1)
        .setMaxLength(1);
      const commentInput = new TextInputBuilder()
        .setCustomId("comment")
        .setLabel("Comment")
        .setStyle(TextInputStyle.Paragraph)
        .setRequired(true)
        .setMaxLength(800);
      modal.addComponents(
        new ActionRowBuilder().addComponents(ratingInput),
        new ActionRowBuilder().addComponents(commentInput)
      );
      const ok = await safeShowModal(interaction, modal);
      if (!ok) return;
      return;
    }

    if (interaction.isModalSubmit() && interaction.customId.startsWith("vouch_submit:")) {
      await safeDefer(interaction, false);
      const ch = interaction.channel;
      if (!ch || ch.type !== ChannelType.GuildText) return safeReply(interaction, { content: "Not a ticket channel." });
      const t = getTicket(ch.id);
      if (!t || t.kind !== "carry") return safeReply(interaction, { content: "Ticket not found." });
      if (!t.claimed) return safeReply(interaction, { content: "Vouch only after claim." });
      if (hasUserVouchedTicket(t, interaction.user.id)) return safeReply(interaction, { content: "You already submitted your vouch for this ticket." });

      const rating = Number((interaction.fields.getTextInputValue("rating") || "").trim());
      const comment = (interaction.fields.getTextInputValue("comment") || "").trim();
      if (!Number.isInteger(rating) || rating < 1 || rating > 5) {
        return safeReply(interaction, { content: "Rating must be 1-5." });
      }

      const helperRec = ensureHelper(t.claimed);
      const helperRank = rankFromTickets(helperRec.ticketsCompleted || 0);
      const reviewerKind = strongestReviewerStatus(interaction.member);
      const s = guildSettings(interaction.guildId);
      const reviewCh = s.reviewChannelId ? interaction.guild.channels.cache.get(s.reviewChannelId) : null;
      if (reviewCh) {
        const helperMember = interaction.guild.members.cache.get(String(t.claimed)) || (await interaction.guild.members.fetch(String(t.claimed)).catch(() => null));
        const helperUser = helperMember?.user || (await client.users.fetch(String(t.claimed)).catch(() => null));
        const vouchPng = await buildVouchImage({
          guildName: interaction.guild?.name || "Server",
          helperUser,
          reviewerUser: interaction.user,
          helperRank,
          reviewerKind,
          rating,
          comment,
          ticketLabel: `#${ch.name}`,
        }).catch(() => null);
        if (vouchPng) {
          const file = new AttachmentBuilder(vouchPng, { name: `vouch-${ch.id}.png` });
          await reviewCh.send({ files: [file] }).catch(() => {});
        }
      }

      logCompletionOnce(ch.id, t.claimed, rating);
      markUserVouchedTicket(ch.id, interaction.user.id);
      await ch.send(`${em(EMOJI_DONE_CLAIM, "done")} Vouch received.`).catch(() => {});
      await interaction.user.send(`${em(EMOJI_DONE_CLAIM, "done")} Your vouch has been submitted for this ticket.`).catch(() => {});
      await updateTicketPanelMessage(ch, { status: "Vouch received" });
      return safeReply(interaction, { content: "Vouch posted." });
    }
  } catch (e) {
    console.error("UI HANDLER ERROR:", e);
  }
});

/* ===================== !close (MESSAGE COMMAND) ===================== */
client.on(Events.MessageCreate, async (message) => {
  try {
    if (!message.guild || message.author.bot || !isAllowedGuild(message.guild.id)) return;
    if (message.content.trim().toLowerCase() !== "!close") return;
    const ch = message.channel;
    if (!ch || ch.type !== ChannelType.GuildText) return;
    if (!isTicketChannelName(ch.name)) return;
    const t = getTicket(ch.id);
    if (!t) return;
    if (!canStaffClose(message.member, t.game)) return;
    const notifyNoVouch = shouldSendUnvouchedCloseNotice(t);
    if (t.kind === "carry" && t.claimed && isSnowflake(t.claimed)) {
      logCompletionOnce(ch.id, t.claimed, null);
    }
    deleteTicket(ch.id);
    if (notifyNoVouch) {
      await sendUnvouchedCloseNotice(t, message.author.id);
    }
    await ch.delete().catch(() => {});
  } catch (e) {
    console.error("!close ERROR:", e);
  }
});

/* ===================== LOGIN ===================== */
if (!BOT_TOKEN) {
  throw new Error("Missing bot token. Set DISCORD_TOKEN or BOT_TOKEN in .env");
}
client.login(BOT_TOKEN);

