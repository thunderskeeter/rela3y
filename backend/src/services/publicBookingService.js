const crypto = require('crypto');
const { APP_PUBLIC_BASE_URL, CAL_OAUTH_REDIRECT_BASE } = require('../config/runtime');

const SCHEDULING_MODES = new Set(['manual', 'link', 'internal']);
const DEFAULT_TIMEZONE = 'America/New_York';

function randomToken() {
  return crypto.randomBytes(16).toString('hex');
}

function normalizeMode(mode) {
  const m = String(mode || '').toLowerCase();
  return SCHEDULING_MODES.has(m) ? m : 'manual';
}

function ensureSchedulingConfig(account) {
  if (!account || typeof account !== 'object') return null;
  const scheduling = account.scheduling && typeof account.scheduling === 'object'
    ? account.scheduling
    : {};

  const next = {
    mode: normalizeMode(scheduling.mode || (account.bookingUrl ? 'link' : 'manual')),
    url: String(scheduling.url || account.bookingUrl || ''),
    label: String(scheduling.label || 'Book a time'),
    instructions: String(scheduling.instructions || ''),
    publicToken: String(scheduling.publicToken || '').trim(),
    slotIntervalMin: Number(scheduling.slotIntervalMin || 30),
    leadTimeMin: Number(scheduling.leadTimeMin || 60),
    bufferMin: Number(scheduling.bufferMin || 0),
    maxBookingsPerDay: Number(scheduling.maxBookingsPerDay || 0)
  };

  if (!next.publicToken) next.publicToken = randomToken();
  if (!Number.isFinite(next.slotIntervalMin) || next.slotIntervalMin < 10) next.slotIntervalMin = 30;
  if (!Number.isFinite(next.leadTimeMin) || next.leadTimeMin < 0) next.leadTimeMin = 60;
  if (!Number.isFinite(next.bufferMin) || next.bufferMin < 0) next.bufferMin = 0;
  if (!Number.isFinite(next.maxBookingsPerDay) || next.maxBookingsPerDay < 0) next.maxBookingsPerDay = 0;

  account.scheduling = next;
  if (!Array.isArray(account.internalBookings)) account.internalBookings = [];
  return next;
}

function appBaseUrl() {
  return String(APP_PUBLIC_BASE_URL || CAL_OAUTH_REDIRECT_BASE || 'http://127.0.0.1:3001').replace(/\/$/, '');
}

function publicBookingUrlForAccount(account) {
  const scheduling = ensureSchedulingConfig(account);
  if (!scheduling?.publicToken) return '';
  return `${appBaseUrl()}/book/${encodeURIComponent(scheduling.publicToken)}`;
}

function findAccountByBookingToken(data, token) {
  const t = String(token || '').trim();
  if (!t) return null;
  for (const [to, account] of Object.entries(data?.accounts || {})) {
    const scheduling = ensureSchedulingConfig(account);
    if (String(scheduling?.publicToken || '') === t) {
      return { to: String(to), account };
    }
  }
  return null;
}

function readWorkspaceServices(account) {
  const services = account?.workspace?.pricing?.services;
  if (!services || typeof services !== 'object') return {};
  return services;
}

function serviceDurationMin(account, serviceId) {
  const id = String(serviceId || '').trim();
  if (!id) return 60;
  const service = readWorkspaceServices(account)?.[id];
  const hours = Number(service?.hoursMin || 1);
  const mins = Math.max(30, Math.round(hours * 60));
  return mins;
}

function parseDateParts(dateIso) {
  const m = String(dateIso || '').match(/^(\d{4})-(\d{2})-(\d{2})$/);
  if (!m) return null;
  return { y: Number(m[1]), mo: Number(m[2]), d: Number(m[3]) };
}

function getZonedParts(ms, tz) {
  const fmt = new Intl.DateTimeFormat('en-CA', {
    timeZone: tz,
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
    hour12: false
  });
  const parts = fmt.formatToParts(new Date(ms));
  const map = {};
  for (const p of parts) map[p.type] = p.value;
  return {
    y: Number(map.year),
    mo: Number(map.month),
    d: Number(map.day),
    h: Number(map.hour),
    mi: Number(map.minute)
  };
}

function zonedToUtcMs(tz, y, mo, d, h, mi) {
  const desired = Date.UTC(y, mo - 1, d, h, mi, 0, 0);
  let guess = desired;
  for (let i = 0; i < 6; i += 1) {
    const p = getZonedParts(guess, tz);
    const asUtc = Date.UTC(p.y, p.mo - 1, p.d, p.h, p.mi, 0, 0);
    const offset = asUtc - guess;
    const next = desired - offset;
    if (Math.abs(next - guess) < 60000) return next;
    guess = next;
  }
  return guess;
}

function dayKeyForTz(ms, tz) {
  const p = getZonedParts(ms, tz);
  return `${String(p.y).padStart(4, '0')}-${String(p.mo).padStart(2, '0')}-${String(p.d).padStart(2, '0')}`;
}

function weekdayShortForTz(ms, tz) {
  return String(new Intl.DateTimeFormat('en-US', { timeZone: tz, weekday: 'short' }).format(new Date(ms))).toLowerCase().slice(0, 3);
}

function normalizeTimezone(tz) {
  const candidate = String(tz || '').trim() || DEFAULT_TIMEZONE;
  try {
    Intl.DateTimeFormat('en-US', { timeZone: candidate }).format(new Date());
    return candidate;
  } catch {
    return DEFAULT_TIMEZONE;
  }
}

function hhmmToMins(hhmm) {
  const m = String(hhmm || '').match(/^(\d{2}):(\d{2})$/);
  if (!m) return null;
  return Number(m[1]) * 60 + Number(m[2]);
}

function buildBusyRanges(account, dayStartMs, dayEndMs, { bufferMin = 0, ignoreBookingId = '' } = {}) {
  const busy = [];
  const padMs = Math.max(0, Number(bufferMin || 0)) * 60 * 1000;
  for (const ev of Array.isArray(account?.calendarEvents) ? account.calendarEvents : []) {
    const start = Number(ev?.start);
    const end = Number(ev?.end);
    if (!Number.isFinite(start) || !Number.isFinite(end) || end <= start) continue;
    if (end <= dayStartMs || start >= dayEndMs) continue;
    busy.push([Math.max(start - padMs, dayStartMs), Math.min(end + padMs, dayEndMs)]);
  }
  for (const b of Array.isArray(account?.internalBookings) ? account.internalBookings : []) {
    if (ignoreBookingId && String(b?.id || '') === String(ignoreBookingId)) continue;
    const start = Number(b?.start);
    const end = Number(b?.end);
    const status = String(b?.status || 'booked').toLowerCase();
    if (status === 'canceled') continue;
    if (!Number.isFinite(start) || !Number.isFinite(end) || end <= start) continue;
    if (end <= dayStartMs || start >= dayEndMs) continue;
    busy.push([Math.max(start - padMs, dayStartMs), Math.min(end + padMs, dayEndMs)]);
  }
  busy.sort((a, b) => a[0] - b[0]);
  return busy;
}

function buildBusyRangesGlobal(account, { bufferMin = 0, ignoreBookingId = '' } = {}) {
  const busy = [];
  const padMs = Math.max(0, Number(bufferMin || 0)) * 60 * 1000;
  for (const ev of Array.isArray(account?.calendarEvents) ? account.calendarEvents : []) {
    const start = Number(ev?.start);
    const end = Number(ev?.end);
    if (!Number.isFinite(start) || !Number.isFinite(end) || end <= start) continue;
    busy.push([start - padMs, end + padMs]);
  }
  for (const b of Array.isArray(account?.internalBookings) ? account.internalBookings : []) {
    if (ignoreBookingId && String(b?.id || '') === String(ignoreBookingId)) continue;
    const start = Number(b?.start);
    const end = Number(b?.end);
    const status = String(b?.status || 'booked').toLowerCase();
    if (status === 'canceled') continue;
    if (!Number.isFinite(start) || !Number.isFinite(end) || end <= start) continue;
    busy.push([start - padMs, end + padMs]);
  }
  busy.sort((a, b) => a[0] - b[0]);
  return busy;
}

function hasConflict(busyRanges, startMs, endMs) {
  for (const [busyStart, busyEnd] of busyRanges) {
    if (startMs < busyEnd && endMs > busyStart) return true;
  }
  return false;
}

function listAvailability(account, { date, days = 5, durationMin = 60, ignoreBookingId = '' } = {}) {
  const scheduling = ensureSchedulingConfig(account);
  const timezone = normalizeTimezone(account?.workspace?.timezone);
  const startDate = parseDateParts(date) || (() => {
    const p = getZonedParts(Date.now(), timezone);
    return { y: p.y, mo: p.mo, d: p.d };
  })();
  const intervalMin = Math.max(10, Number(scheduling?.slotIntervalMin || 30));
  const leadTimeMs = Math.max(0, Number(scheduling?.leadTimeMin || 60)) * 60 * 1000;
  const now = Date.now();
  const dayOnly = Number(durationMin) >= (8 * 60);
  const out = [];
  const globalBusy = buildBusyRangesGlobal(account, {
    bufferMin: scheduling?.bufferMin || 0,
    ignoreBookingId
  });

  for (let i = 0; i < Math.max(1, Math.min(14, Number(days) || 5)); i += 1) {
    const baseMs = zonedToUtcMs(timezone, startDate.y, startDate.mo, startDate.d + i, 0, 0);
    const dayKey = dayKeyForTz(baseMs, timezone);
    const weekday = weekdayShortForTz(baseMs, timezone);
    const windows = Array.isArray(account?.workspace?.businessHours?.[weekday]) ? account.workspace.businessHours[weekday] : [];
    if (!windows.length) {
      out.push({ date: dayKey, slots: [], status: 'closed', closed: true, full: false });
      continue;
    }

    const baseParts = getZonedParts(baseMs, timezone);
    const dayStartMs = zonedToUtcMs(timezone, baseParts.y, baseParts.mo, baseParts.d, 0, 0);
    const dayEndMs = dayStartMs + 24 * 60 * 60 * 1000;
    const busy = buildBusyRanges(account, dayStartMs, dayEndMs, {
      bufferMin: scheduling?.bufferMin || 0,
      ignoreBookingId
    });
    const maxPerDay = Math.max(0, Number(scheduling?.maxBookingsPerDay || 0));
    if (maxPerDay > 0) {
      const countForDay = (Array.isArray(account?.internalBookings) ? account.internalBookings : [])
        .filter((b) => String(b?.status || '').toLowerCase() !== 'canceled')
        .filter((b) => !ignoreBookingId || String(b?.id || '') !== String(ignoreBookingId))
        .filter((b) => Number.isFinite(Number(b?.start)))
        .filter((b) => dayKeyForTz(Number(b.start), timezone) === dayKey)
        .length;
      if (countForDay >= maxPerDay) {
        out.push({ date: dayKey, slots: [], status: 'full', closed: false, full: true });
        continue;
      }
    }
    const slots = [];

    for (const w of windows) {
      const startMin = hhmmToMins(w?.start);
      const endMin = hhmmToMins(w?.end);
      if (!Number.isFinite(startMin) || !Number.isFinite(endMin) || endMin <= startMin) continue;
      if (dayOnly) {
        const startMs = zonedToUtcMs(timezone, baseParts.y, baseParts.mo, baseParts.d, Math.floor(startMin / 60), startMin % 60);
        const endMs = startMs + durationMin * 60 * 1000;
        if (startMs < now + leadTimeMs) continue;
        if (hasConflict(globalBusy, startMs, endMs)) continue;
        slots.push({
          start: startMs,
          end: endMs,
          label: `Start ${new Date(startMs).toLocaleDateString('en-US', { timeZone: timezone, month: 'short', day: 'numeric' })}`
        });
        break;
      }
      for (let m = startMin; m + durationMin <= endMin; m += intervalMin) {
        const startMs = zonedToUtcMs(timezone, baseParts.y, baseParts.mo, baseParts.d, Math.floor(m / 60), m % 60);
        const endMs = startMs + durationMin * 60 * 1000;
        if (startMs < now + leadTimeMs) continue;
        if (hasConflict(busy, startMs, endMs)) continue;
        slots.push({
          start: startMs,
          end: endMs,
          label: new Date(startMs).toLocaleTimeString('en-US', { timeZone: timezone, hour: 'numeric', minute: '2-digit' })
        });
      }
    }

    out.push({
      date: dayKey,
      slots,
      status: slots.length > 0 ? 'open' : 'full',
      closed: false,
      full: slots.length === 0
    });
  }

  return {
    timezone,
    durationMin,
    dayOnly,
    days: out
  };
}

module.exports = {
  ensureSchedulingConfig,
  publicBookingUrlForAccount,
  findAccountByBookingToken,
  readWorkspaceServices,
  serviceDurationMin,
  listAvailability,
  normalizeTimezone
};
