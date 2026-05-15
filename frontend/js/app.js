/* =========================
   Demo Auth (localStorage)
   ========================= */
   
const API_BASE = (() => {
  if (typeof window === "undefined") return "http://127.0.0.1:3001";
  const origin = String(window.location?.origin || "").trim();
  const protocol = String(window.location?.protocol || "http:");
  const hostname = String(window.location?.hostname || "").trim();
  if (/^(https?:\/\/(127\.0\.0\.1|localhost):3001)$/i.test(origin)) return origin;
  if (/^(127\.0\.0\.1|localhost)$/i.test(hostname)) return `${protocol}//${hostname}:3001`;
  if (/^https?:\/\//i.test(origin) && origin !== "null") return origin;
  return "http://127.0.0.1:3001";
})();
const IS_DEMO_MODE = (() => {
  if (typeof window === "undefined") return false;
  const p = String(window.location?.pathname || "").trim().toLowerCase();
  return p === "/demo" || p.startsWith("/demo/");
})();
const DEMO_TO = "+10000000000";

const UI_KEYS = {
  ACTIVE_TO: "mc_active_to_v1",
  LAST_VIEW: "mc_last_view_v1",
};
const ANALYTICS_RANGE_KEY = "mc_analytics_range_days_v1";
const NOTIF_READ_TS_KEY = "mc_notif_read_ts_v1";
const PROFILE_PREFS_KEY = "mc_profile_prefs_v1";
const SETTINGS_TAB_ROUTE_KEY = "mc_settings_tab_v1";
const relayUiEscape = (value) => String(value ?? "")
  .replace(/&/g, "&amp;")
  .replace(/</g, "&lt;")
  .replace(/>/g, "&gt;")
  .replace(/"/g, "&quot;")
  .replace(/'/g, "&#39;");
const RelayUI = window.RelayUI || {
  renderEmptyState: ({ title = "", text = "", actionsHtml = "", className = "", centered = false } = {}) => `
    <div class="empty-state${centered ? " is-centered" : ""}${className ? ` ${className}` : ""}">
      ${title ? `<div class="h1" style="margin:0;">${relayUiEscape(title)}</div>` : ""}
      ${text ? `<p>${relayUiEscape(text)}</p>` : ""}
      ${actionsHtml ? `<div class="empty-state-actions">${actionsHtml}</div>` : ""}
    </div>
  `,
  renderNoticeCard: ({ title = "", text = "", detail = "", className = "" } = {}) => `
    <div class="card shell-notice${className ? ` ${className}` : ""}">
      ${title ? `<p class="h1">${relayUiEscape(title)}</p>` : ""}
      ${text ? `<p class="p">${relayUiEscape(text)}</p>` : ""}
      ${detail ? `<pre>${relayUiEscape(detail)}</pre>` : ""}
    </div>
  `,
  renderSegmentedControl: ({ activeValue = "", options = [], dataAttr = "", className = "" } = {}) => `
    <div class="ui-segmented-control${className ? ` ${className}` : ""}">
      ${options.map((option) => `<button class="btn${String(option?.value ?? "") === String(activeValue ?? "") ? " active" : ""}" data-${dataAttr}="${relayUiEscape(String(option?.value ?? ""))}">${relayUiEscape(String(option?.label ?? option?.value ?? ""))}</button>`).join("")}
    </div>
  `,
};
const NAV_VIEWS = new Set(["home", "contacts", "messages", "vip", "analytics", "schedule", "schedule-booking", "settings", "onboarding"]);
const authState = { user: null, accounts: [] };
authState.csrfToken = null;
let authExpiredHandledAt = 0;
const BILL_DUE_STATUSES = new Set(["past_due", "unpaid"]);
const navBillDueState = {
  status: "",
  planKey: "",
  loading: false,
  scopeKey: ""
};
function getBillingReturnUrl() {
  const origin = String(window.location?.origin || "").trim();
  if (/^https?:\/\//i.test(origin) && origin !== "null") return `${origin}/settings/billing`;
  return `${API_BASE}/settings/billing`;
}
function isBillDueStatus(status) {
  return BILL_DUE_STATUSES.has(String(status || "").toLowerCase());
}
function normalizeNavPlanKey(planKey) {
  const key = String(planKey || "").toLowerCase();
  if (key === "basic" || key === "starter") return "starter";
  if (key === "pro") return "pro";
  if (key === "growth") return "growth";
  return "";
}
function canAccessDeveloperTools() {
  const role = String(authState?.user?.role || "").toLowerCase();
  return role === "superadmin" || authState?.user?.developerAccess === true;
}
function canAccessDeveloperRoutes() {
  return canAccessDeveloperTools();
}
function canAccessWorkspaceAdmin() {
  const role = String(authState?.user?.role || "").toLowerCase();
  return role === "superadmin" || role === "owner" || role === "admin";
}

async function importContactsFromVcfFile(file, to) {
  const text = await file.text();
  const parsed = parseVCF(text);
  if (!parsed.length) {
    throw new Error("No contacts found in that file. Make sure it's a valid .vcf file.");
  }
  const targetTo = String(to || getActiveTo() || "").trim();
  if (!targetTo) {
    throw new Error("Missing active account number for import.");
  }
  const resp = await apiPost("/api/contacts/import", { to: targetTo, contacts: parsed });
  return {
    parsedCount: parsed.length,
    imported: Number(resp?.imported || 0),
    skipped: Number(resp?.skipped || 0)
  };
}

function parseVCF(text) {
  const contacts = [];
  const cards = String(text || "").split(/(?=BEGIN:VCARD)/i).filter((s) => s.trim());
  for (const card of cards) {
    const lines = unfoldVCFLines(card);
    let name = "";
    let phone = "";
    for (const line of lines) {
      if (/^FN[;:]/i.test(line)) {
        const val = line.replace(/^FN[^:]*:/i, "").trim();
        if (val && !name) name = val;
      }
      if (/^N[;:]/i.test(line) && !name) {
        const val = line.replace(/^N[^:]*:/i, "").trim();
        const parts = val.split(";");
        const first = (parts[1] || "").trim();
        const last = (parts[0] || "").trim();
        if (first || last) name = [first, last].filter(Boolean).join(" ");
      }
      if (/^TEL[;:]/i.test(line)) {
        const val = line.replace(/^TEL[^:]*:/i, "").trim();
        if (val && !phone) phone = val;
      }
    }
    if (phone) contacts.push({ name: name || "", phone });
  }
  return contacts;
}

function unfoldVCFLines(text) {
  return String(text || "")
    .replace(/\r\n[ \t]/g, "")
    .replace(/\r/g, "")
    .split("\n")
    .filter((l) => l.trim());
}

const _nativeFetch = window.fetch.bind(window);
window.fetch = (input, init = {}) => {
  const url = typeof input === "string" ? input : String(input?.url || "");
  const isRelayApi = url.startsWith(API_BASE) || url.startsWith("/api/") || url.startsWith("/webhooks/") || url === "/health";
  if (!isRelayApi) return _nativeFetch(input, init);
  const nextInit = { ...init, credentials: "include" };
  const method = String(nextInit.method || "GET").toUpperCase();
  const isMutating = method === "POST" || method === "PUT" || method === "PATCH" || method === "DELETE";
  if (isMutating && (url.includes("/api/") || url.startsWith("/api/"))) {
    const token = csrfToken();
    if (token) {
      nextInit.headers = { ...(init.headers || {}), "x-csrf-token": token };
    }
  }
  return _nativeFetch(input, nextInit);
};

const DEFERRED_STYLESHEET_HREF = "/styles.min.css?v=20260311-min3";
let deferredStylesPromise = null;

function ensureDeferredStyles() {
  if (typeof document === "undefined") return Promise.resolve();
  const existing = document.getElementById("appDeferredStyles");
  if (existing) return deferredStylesPromise || Promise.resolve();
  if (deferredStylesPromise) return deferredStylesPromise;
  deferredStylesPromise = new Promise((resolve) => {
    const link = document.createElement("link");
    link.id = "appDeferredStyles";
    link.rel = "stylesheet";
    link.href = DEFERRED_STYLESHEET_HREF;
    link.onload = () => resolve();
    link.onerror = () => resolve();
    document.head.appendChild(link);
  });
  return deferredStylesPromise;
}

function viewNeedsDeferredStyles(view = state?.view) {
  void view;
  return true;
}
const THEME_KEY = "mc_theme_v1";

function applyTheme(theme){
  // theme: "dark" | "light"
  document.documentElement.setAttribute("data-theme", theme);
  localStorage.setItem(THEME_KEY, theme);
}

function initTheme(){
  const saved = localStorage.getItem(THEME_KEY);
  // default to dark (your preference)
  applyTheme(saved || "dark");
}

function bindNavDelegated() {
  const host = document.body;
  if (!host) return;

  if (host.__navBound) return;
  host.__navBound = true;

  host.addEventListener("click", (e) => {
    const el = e.target.closest("[data-view]");
    if (!el) return;

    e.preventDefault();
    const view = el.getAttribute("data-view");
    if (!view) return;

    // This fixes the "Messages UI small" problem.
    document.body.classList.toggle("messages-mode", view === "messages");
    document.body.classList.toggle("schedule-mode", view === "schedule" || view === "schedule-booking");

    // Keep active styling consistent across sidebar and dock.
    if (typeof setActiveNav === "function") setActiveNav(view);

    // Collapse dock after navigation tap for a cleaner UI.
    const dock = document.getElementById("arcDock");
    if (dock) {
      dock.classList.remove("is-open");
      if (el.closest("#arcDock")) {
        dock.classList.add("force-closed");
      }
    }

    // Switch views in the most compatible way
    if (typeof renderView === "function") {
      renderView(view);
      return;
    }
    if (typeof setView === "function") {
      setView(view);
      return;
    }

    // Fallback: state.view + render (common in your app)
    if (typeof state === "object") state.view = view;
    if (typeof render === "function") render();
  });
}



function getActiveTo(){
  return localStorage.getItem(UI_KEYS.ACTIVE_TO) || "+18145550001"; // default detailer
}
function setActiveTo(to){
  localStorage.setItem(UI_KEYS.ACTIVE_TO, to);
  window.dispatchEvent(new CustomEvent("relay:active-to-changed", { detail: { to } }));
}

function isLikelyPhoneSelector(value) {
  const v = String(value || "").trim();
  return /^\+?[0-9]{8,16}$/.test(v);
}

function canAccessSensitiveContactsInUi() {
  const role = String(authState?.user?.role || "").trim().toLowerCase();
  return role === "superadmin" || role === "owner" || role === "admin";
}

function resolveActiveWorkspace() {
  const accounts = Array.isArray(authState?.accounts) ? authState.accounts : [];
  const raw = String(getActiveTo() || "").trim();
  if (!accounts.length) {
    return {
      to: raw,
      accountId: "",
      fromAccounts: false
    };
  }
  const byTo = accounts.find((a) => String(a?.to || "").trim() === raw);
  if (byTo) {
    return {
      to: String(byTo?.to || "").trim(),
      accountId: String(byTo?.accountId || "").trim(),
      fromAccounts: true
    };
  }
  const byAccountId = accounts.find((a) => String(a?.accountId || "").trim() === raw);
  if (byAccountId) {
    const mappedTo = String(byAccountId?.to || "").trim();
    if (mappedTo) setActiveTo(mappedTo);
    return {
      to: mappedTo,
      accountId: String(byAccountId?.accountId || "").trim(),
      fromAccounts: true
    };
  }
  const firstWithTo = accounts.find((a) => String(a?.to || "").trim());
  if (firstWithTo) {
    const nextTo = String(firstWithTo.to || "").trim();
    if (nextTo && nextTo !== raw) setActiveTo(nextTo);
    return {
      to: nextTo,
      accountId: String(firstWithTo?.accountId || "").trim(),
      fromAccounts: true
    };
  }
  return {
    to: raw,
    accountId: "",
    fromAccounts: false
  };
}

function getStoredView() {
  const route = parseRoutePath(typeof window !== "undefined" ? window.location.pathname : "");
  if (route?.view && NAV_VIEWS.has(route.view)) {
    if (route.view === "settings" && route.panel) {
      localStorage.setItem(SETTINGS_TAB_ROUTE_KEY, route.panel);
    }
    return route.view;
  }
  const raw = String(localStorage.getItem(UI_KEYS.LAST_VIEW) || "").toLowerCase();
  const normalized = raw === "revenue" ? "analytics" : raw;
  return NAV_VIEWS.has(normalized) ? normalized : "analytics";
}

function persistView(view) {
  if (!NAV_VIEWS.has(view)) return;
  localStorage.setItem(UI_KEYS.LAST_VIEW, view);
}

function normalizePathname(pathname) {
  const raw = String(pathname || "").trim();
  if (!raw) return "/";
  let clean = raw.split("?")[0].split("#")[0] || "/";
  if (!clean.startsWith("/")) clean = `/${clean}`;
  clean = clean.replace(/\/{2,}/g, "/");
  if (clean.length > 1 && clean.endsWith("/")) clean = clean.slice(0, -1);
  return clean || "/";
}

function settingsPanelToRouteSegment(panelId) {
  const map = {
    profile: "profile",
    workspace: "pricing",
    automations: "automations",
    schedule: "schedule",
    "customer-billing": "customer-billing",
    email: "email",
    "call-routing": "call-routing",
    admin: "admin",
    billing: "billing",
    developer: "developer"
  };
  return map[String(panelId || "").trim().toLowerCase()] || "profile";
}

function routeSegmentToSettingsPanel(segment) {
  const map = {
    profile: "profile",
    pricing: "workspace",
    workspace: "workspace",
    messaging: "profile",
    automations: "automations",
    schedule: "schedule",
    "customer-billing": "customer-billing",
    email: "email",
    "call-routing": "call-routing",
    admin: "admin",
    billing: "billing",
    developer: "developer"
  };
  return map[String(segment || "").trim().toLowerCase()] || "profile";
}

function parseRoutePath(pathname) {
  const path = normalizePathname(pathname);
  if (path === "/") return null;
  if (path === "/dashboard" || path === "/login" || path === "/demo") return { view: "home" };
  if (path === "/dashboard/home" || path === "/demo/home") return { view: "home" };
  if (path === "/dashboard/messages" || path === "/demo/messages") return { view: "messages" };
  if (path === "/dashboard/contacts" || path === "/demo/contacts") return { view: "contacts" };
  if (path === "/dashboard/vip" || path === "/demo/vip") return { view: "vip" };
  if (path === "/dashboard/schedule" || path === "/demo/schedule") return { view: "schedule" };
  if (path === "/dashboard/schedule/create-booking" || path === "/demo/schedule/create-booking") return { view: "schedule-booking" };
  if (path === "/dashboard/analytics" || path === "/demo/analytics") return { view: "analytics" };
  if (path === "/dashboard/onboarding" || path === "/demo/onboarding") return { view: "onboarding" };
  if (path === "/dashboard/settings" || path === "/demo/settings") {
    return { view: "settings", panel: routeSegmentToSettingsPanel(localStorage.getItem(SETTINGS_TAB_ROUTE_KEY) || "profile") };
  }
  if (path.startsWith("/dashboard/settings/") || path.startsWith("/demo/settings/")) { const prefix = path.startsWith("/demo/settings/") ? "/demo/settings/" : "/dashboard/settings/"; const seg = path.slice(prefix.length).split("/")[0];
    return { view: "settings", panel: routeSegmentToSettingsPanel(seg) };
  }
  return null;
}

function getCurrentRoutePathForState() { const view = String(state?.view || "").toLowerCase(); const current = normalizePathname(typeof window !== "undefined" ? window.location.pathname : ""); const base = current === "/demo" || current.startsWith("/demo/") ? "/demo" : "/dashboard";
  if (view === "settings") {
    const panel = String(localStorage.getItem(SETTINGS_TAB_ROUTE_KEY) || "profile");
    return `${base}/settings/${settingsPanelToRouteSegment(panel)}`;
  }
  if (view === "home") return base;
  if (view === "messages") return `${base}/messages`;
  if (view === "contacts") return `${base}/contacts`;
  if (view === "vip") return `${base}/vip`;
  if (view === "schedule") return `${base}/schedule`;
  if (view === "schedule-booking") return `${base}/schedule/create-booking`;
  if (view === "analytics") return `${base}/analytics`;
  if (view === "onboarding") return `${base}/onboarding`;
  return base;
}

function isOnboardingView(view = state?.view) {
  return String(view || "").trim().toLowerCase() === "onboarding";
}

function shouldSkipDashboardBootForView(view = state?.view) {
  return isOnboardingView(view);
}

function syncUrlWithState(options = {}) {
  if (typeof window === "undefined" || !window.history || typeof window.history.pushState !== "function") return;
  const replace = options?.replace === true;
  const target = normalizePathname(getCurrentRoutePathForState());
  const current = normalizePathname(window.location.pathname);
  if (!target || target === current) return;
  const payload = { view: String(state?.view || "") };
  if (replace) window.history.replaceState(payload, "", target);
  else window.history.pushState(payload, "", target);
}

function getHomeSnapshot(rangeDays = 1){
  const now = Date.now();
  const start = now - (rangeDays * 24 * 60 * 60 * 1000);

  const threads = state.threads || [];

  // helper: is timestamp within range
  const inRange = (ts) => {
    const n = Number(ts || 0);
    return n && n >= start && n <= now;
  };

  // 1) inbound leads = conversations created in range
  const inbound = threads.filter(t => inRange(t.createdAt || t.updatedAt || t.lastActivityAt)).length;

  // 2) recovered = any activity in range (updated/last activity)
  const recovered = threads.filter(t => inRange(t.lastActivityAt || t.updatedAt)).length;

  // 3) booked = status booked AND activity in range (we don't have bookedAt yet)
  const booked = threads.filter(t => (String(t.status || "").toLowerCase() === "booked") && inRange(t.lastActivityAt || t.updatedAt)).length;

  return { inbound, recovered, booked };
}


function getActionRequiredLeads() {
  const now = Date.now();
  const DAY = 24 * 60 * 60 * 1000;

  const threads = state.threads || [];

  const actionable = threads.filter(t => {
    if (t.status === "new") return true;

    if (t.status === "contacted") {
      const last = t.lastActivityAt || t.updatedAt || 0;
      return now - last > DAY;
    }

    return false;
  });

  const breakdown = {
    new: actionable.filter(t => t.status === "new").length,
    stale: actionable.filter(t => t.status === "contacted").length,
  };

  return {
    total: actionable.length,
    breakdown,
    items: actionable,
  };
}


function loadLS(key, fallback){
  try {
    const raw = localStorage.getItem(key);
    return raw ? JSON.parse(raw) : fallback;
  } catch {
    return fallback;
  }
}
function saveLS(key, value){
  localStorage.setItem(key, JSON.stringify(value));
}

function getSession(){
  return authState.user;
}

function getStoredAuthToken() {
  return null;
}

function getCookie(name) {
  const target = `${String(name || "").trim()}=`;
  const parts = String(document.cookie || "").split(";");
  for (const part of parts) {
    const p = part.trim();
    if (p.startsWith(target)) return decodeURIComponent(p.slice(target.length));
  }
  return "";
}

function csrfToken() {
  return String(authState.csrfToken || getCookie("relay_csrf") || "").trim();
}

function bookingKey(to){
  return `mc_booking_url_v1:${to}`;
}

function cacheBookingUrl(to, url){
  localStorage.setItem(bookingKey(to), url || "");
}

function getCachedBookingUrl(to){
  return localStorage.getItem(bookingKey(to)) || "";
}


function setSession(payload, persist = true){
  bumpAccountScopeVersion();
  authState.user = payload?.user || null;
  authState.accounts = Array.isArray(payload?.accounts) ? payload.accounts : [];
  const role = String(authState?.user?.role || "").toLowerCase();
  document.documentElement.setAttribute("data-user-role", role || "guest");
  authState.csrfToken = String(payload?.csrfToken || authState.csrfToken || getCookie("relay_csrf") || "").trim() || null;
  syncActiveToWithAllowedAccounts();
  populateAllowedAccountOptions();
  syncNavBillDueButton();
  refreshNavBillDueState({ force: true });
  window.dispatchEvent(new CustomEvent("relay:auth-role-changed"));
}

function clearSession(){
  bumpAccountScopeVersion();
  authState.user = null;
  authState.accounts = [];
  document.documentElement.setAttribute("data-user-role", "guest");
  authState.csrfToken = null;
  navBillDueState.status = "";
  navBillDueState.planKey = "";
  navBillDueState.scopeKey = "";
  navBillDueState.loading = false;
  syncNavBillDueButton();
  window.dispatchEvent(new CustomEvent("relay:auth-role-changed"));
}

function handleUnauthorizedSession() {
  const now = Date.now();
  if (now - authExpiredHandledAt < 1200) return;
  authExpiredHandledAt = now;
  clearSession();
  showLoginOverlay();
}

function syncNavBillDueButton() {
  const btn = document.getElementById("navBillDueBtn");
  const topbarDue = document.getElementById("topbarBillDueBar");
  const upgradeBtn = document.getElementById("navUpgradePlanBtn");
  const dockUpgradeBtn = document.getElementById("arcDockUpgradeBtn");
  const role = String(authState?.user?.role || "").toLowerCase();
  const isSuperadmin = role === "superadmin";
  const isDeveloperRole = canAccessDeveloperTools();
  const isStarterPlan = normalizeNavPlanKey(navBillDueState.planKey) === "starter";
  const showUpgrade = isDeveloperRole || isStarterPlan;
  const visible = isSuperadmin && isBillDueStatus(navBillDueState.status);
  if (btn) btn.classList.toggle("hidden", !visible);
  if (topbarDue) topbarDue.classList.toggle("hidden", !visible);
  if (upgradeBtn) upgradeBtn.classList.toggle("hidden", !showUpgrade);
  if (dockUpgradeBtn) dockUpgradeBtn.classList.toggle("hidden", !showUpgrade);
}

async function refreshNavBillDueState({ force = false } = {}) {
  const role = String(authState?.user?.role || "").toLowerCase();
  const isSuperadmin = role === "superadmin";
  const accounts = Array.isArray(authState?.accounts) ? authState.accounts : [];
  const activeTo = String(getActiveTo() || "");
  const activeAccount = accounts.find((acct) => String(acct?.to || "") === activeTo);
  const scopeKey = String(activeAccount?.accountId || activeTo || "");
  if (!scopeKey) {
    navBillDueState.status = "";
    navBillDueState.planKey = "";
    navBillDueState.scopeKey = "";
    syncNavBillDueButton();
    return;
  }
  if (!force && navBillDueState.loading) return;
  if (!force && navBillDueState.scopeKey === scopeKey && navBillDueState.status) {
    syncNavBillDueButton();
    return;
  }
  navBillDueState.loading = true;
  navBillDueState.scopeKey = scopeKey;
  const scopeSnapshot = createUiScopeSnapshot({ to: activeTo });
  try {
    const summaryRes = await apiGet("/api/billing/summary");
    if (!isUiScopeCurrent(scopeSnapshot)) return;
    const planKey = normalizeNavPlanKey(summaryRes?.billing?.plan?.key);
    navBillDueState.planKey = planKey;
    const status = String(summaryRes?.billing?.plan?.status || "").toLowerCase();
    navBillDueState.status = isSuperadmin ? status : "";
  } catch {
    if (!isUiScopeCurrent(scopeSnapshot)) return;
    navBillDueState.status = "";
    navBillDueState.planKey = "";
  } finally {
    if (!isUiScopeCurrent(scopeSnapshot)) return;
    navBillDueState.loading = false;
    syncNavBillDueButton();
    if (typeof window.refreshTopbarNotificationsUI === "function") window.refreshTopbarNotificationsUI();
  }
}

function syncActiveToWithAllowedAccounts(){
  const accounts = Array.isArray(authState.accounts) ? authState.accounts : [];
  if (!accounts.length) return;
  const current = String(getActiveTo() || "").trim();
  if (accounts.some((a) => String(a?.to || '').trim() === current)) return;
  const byAccountId = accounts.find((a) => String(a?.accountId || "").trim() === current);
  if (byAccountId?.to) {
    setActiveTo(String(byAccountId.to || "").trim());
    return;
  }
  const firstWithTo = accounts.find((a) => String(a?.to || "").trim());
  if (firstWithTo?.to) setActiveTo(String(firstWithTo.to || "").trim());
}

function populateAllowedAccountOptions(){
  const simTo = document.getElementById("simTo");
  if (!simTo) return;
  const accounts = Array.isArray(authState.accounts) ? authState.accounts : [];
  const role = String(authState?.user?.role || '').toLowerCase();
  const current = getActiveTo();
  simTo.innerHTML = "";

  accounts.forEach((acct) => {
    const to = String(acct?.to || '').trim();
    if (!to) return;
    const name = String(acct?.businessName || '').trim() || "Account";
    const opt = document.createElement("option");
    opt.value = to;
    opt.textContent = `${name} (To: ${to})`;
    simTo.appendChild(opt);
  });

  if (role === 'superadmin') {
    const custom = document.createElement("option");
    custom.value = "__custom__";
    custom.textContent = "Custom number...";
    simTo.appendChild(custom);
  }

  if (accounts.some((a) => String(a?.to || '') === current)) {
    simTo.value = current;
  } else if (accounts.length) {
    simTo.value = String(accounts[0].to);
  }
}

async function authRequest(path, { method = "GET", body } = {}) {
  const headers = {};
  if (body) headers["Content-Type"] = "application/json";
  const m = String(method || "GET").toUpperCase();
  if (m === "POST" || m === "PUT" || m === "PATCH" || m === "DELETE") {
    const token = csrfToken();
    if (token) headers["x-csrf-token"] = token;
  }
  const res = await fetch(API_BASE + path, {
    method,
    headers: Object.keys(headers).length ? headers : undefined,
    body: body ? JSON.stringify(body) : undefined,
  });
  if (res.status === 401) {
    handleUnauthorizedSession();
    throw new Error("API 401: Unauthorized");
  }
  if (!res.ok) {
    const txt = await res.text();
    throw new Error(`API ${res.status}: ${txt}`);
  }
  const data = await res.json();
  invalidateWorkspaceCacheForPath(path);
  return data;
}

async function logoutAndShowLogin() {
  try {
    await authRequest("/api/auth/logout", { method: "POST" });
  } catch {}
  clearSession();
  setError("");
  showLoginOverlay();
}

async function handlePostLogin(){
  state.activeTo = getActiveTo();
  state.activeThreadId = null;
  state.activeConversation = null;
  state.threads = [];
  state.rules = loadLS(rulesKey(getActiveTo()), []);
  await refreshOnboardingGate({ force: true });
  if (state.onboardingRequired) {
    state.view = "onboarding";
  } else if (state.view === "onboarding") {
    state.view = "home";
  }
  if (!shouldSkipDashboardBootForView()) {
    syncRulesToBackend();
    syncVipToBackend();
  }
  await render();
  if (!shouldSkipDashboardBootForView() && typeof window.refreshTopbarRecoveryStrip === "function") {
    window.refreshTopbarRecoveryStrip({ force: true }).catch(() => {});
  }
}

function setError(msg){
  const el = document.getElementById("error");
  if(el) el.textContent = msg || "";
}

function normalizeLoginErrorMessage(err) {
  const raw = String(err?.message || "").trim();
  if (!raw) return "Login failed.";
  if (raw.includes("API 429")) return "Too many login attempts. Wait 60 seconds and try again.";
  if (raw.includes("API 401")) return "Invalid email or password.";
  if (raw.includes("API 403")) return "Access is pending payment verification.";
  return raw.replace(/^API \d+:\s*/i, "").trim() || "Login failed.";
}

function normalizeInviteErrorMessage(err, fallback = "Invite flow failed.") {
  const raw = String(err?.message || "").trim();
  if (!raw) return fallback;
  if (raw.includes("already exists")) return "An account with this email already exists. Log in instead.";
  if (raw.includes("expired")) return "This invite link has expired. Ask an owner/admin for a new invite.";
  if (raw.includes("already used")) return "This invite link was already used. Ask for a new invite.";
  if (raw.includes("Password must")) return "Password must be 10+ chars with uppercase, lowercase, number, and symbol.";
  return raw.replace(/^API \d+:\s*/i, "").replace(/^[{\[]|[}\]]$/g, "").trim() || fallback;
}

function showLoginOverlay(){
  document.getElementById("loginOverlay")?.classList.remove("hidden");
}
function hideLoginOverlay(){
  document.getElementById("loginOverlay")?.classList.add("hidden");
}

function showWorkspaceRequestModal(){
  document.getElementById("workspaceRequestModal")?.classList.remove("hidden");
  document.getElementById("workspaceRequestForm")?.classList.remove("hidden");
  document.getElementById("loginForm")?.classList.add("hidden");
  document.getElementById("inviteAcceptForm")?.classList.add("hidden");
}
function hideWorkspaceRequestModal(){
  document.getElementById("workspaceRequestModal")?.classList.add("hidden");
  document.getElementById("workspaceRequestForm")?.classList.add("hidden");
  document.getElementById("loginForm")?.classList.remove("hidden");
  document.getElementById("inviteAcceptForm")?.classList.add("hidden");
}

async function playPostLoginIntro() {
  const intro = document.getElementById("postLoginIntro");
  if (!intro) return;
  const reduceMotion = !!window.matchMedia && window.matchMedia("(prefers-reduced-motion: reduce)").matches;
  intro.classList.remove("hidden", "is-playing", "is-done");
  intro.setAttribute("aria-hidden", "false");
  // Force a new animation cycle each login.
  void intro.offsetWidth;
  intro.classList.add("is-playing");
  await new Promise((resolve) => setTimeout(resolve, reduceMotion ? 280 : 1850));
  intro.classList.add("is-done");
  await new Promise((resolve) => setTimeout(resolve, reduceMotion ? 80 : 260));
  intro.classList.remove("is-playing", "is-done");
  intro.classList.add("hidden");
  intro.setAttribute("aria-hidden", "true");
}

async function bootAuthOverlay(){
  const overlay = document.getElementById("loginOverlay");
  if(!overlay) return false;

  const createDemoBtn = document.getElementById("createDemo");
  if (createDemoBtn) createDemoBtn.style.display = "none";
  const authCard = overlay.querySelector(".auth-card-login");
  const requestForm = document.getElementById("workspaceRequestForm");
  const loginForm = document.getElementById("loginForm");
  const inviteForm = document.getElementById("inviteAcceptForm");
  const authMemberIcon = document.getElementById("authMemberIcon");
  const authMemberTitle = document.getElementById("authMemberTitle");
  const authMemberBody = document.getElementById("authMemberBody");
  const authSwitchBtn = document.getElementById("authSwitchBtn");
  const qrSigninPanel = document.getElementById("qrSigninPanel");
  const qrSigninImage = document.getElementById("qrSigninImage");
  const qrSigninLink = document.getElementById("qrSigninLink");
  const qrSigninTimer = document.getElementById("qrSigninTimer");
  const qrSigninCopy = document.getElementById("qrSigninCopy");
  const qrSigninRefreshBtn = document.getElementById("qrSigninRefreshBtn");
  const inviteErrorEl = document.getElementById("inviteAcceptError");
  const inviteBusinessName = document.getElementById("inviteBusinessName");
  const inviteRole = document.getElementById("inviteRole");
  const inviteEmail = document.getElementById("inviteEmail");
  const inviteName = document.getElementById("inviteName");
  const invitePassword = document.getElementById("invitePassword");
  const inviteBackBtn = document.getElementById("inviteBackToLoginBtn");
  const inviteSubtitle = document.getElementById("inviteAcceptSubtitle");
  const inviteSubmitBtn = document.getElementById("inviteAcceptSubmitBtn");
  const qrSigninState = { token: "", approvalUrl: "", expiresAt: 0, pollTimer: null, countdownTimer: null };

  const setInviteError = (msg) => {
    if (inviteErrorEl) inviteErrorEl.textContent = msg || "";
  };
  const updateAuthMemberPanel = (mode) => {
    const loginMode = mode === "login" || mode === "invite";
    if (authCard) authCard.classList.toggle("is-login-mode", loginMode);
    if (authMemberIcon) authMemberIcon.innerHTML = loginMode ? "&#128075;" : "&#128640;";
    if (authMemberTitle) authMemberTitle.textContent = loginMode ? "New Here?" : "Already A Member?";
    if (authMemberBody) authMemberBody.textContent = loginMode
      ? "Make the best decision of the day and begin your journey today!"
      : "Welcome back! Sign in to acces your account";
    if (authSwitchBtn) authSwitchBtn.textContent = loginMode ? "Create An Account" : "Sign In";
  };
  const flipAuthCard = (applyMode) => {
    if (!authCard) {
      if (typeof applyMode === "function") applyMode();
      return;
    }
    authCard.classList.remove("is-flipping-out", "is-flipping-in");
    void authCard.offsetWidth;
    authCard.classList.add("is-flipping-out");
    window.setTimeout(() => {
      if (typeof applyMode === "function") applyMode();
      authCard.classList.remove("is-flipping-out");
      void authCard.offsetWidth;
      authCard.classList.add("is-flipping-in");
      window.setTimeout(() => {
        authCard.classList.remove("is-flipping-in");
      }, 780);
    }, 700);
  };
  const setAuthPanelMode = (mode, { animate = false } = {}) => {
    const applyMode = () => {
      if (requestForm) requestForm.classList.toggle("hidden", mode !== "request");
      if (loginForm) loginForm.classList.toggle("hidden", mode !== "login");
      if (inviteForm) inviteForm.classList.toggle("hidden", mode !== "invite");
      if (mode !== "login") hideQrSigninPanel();
      updateAuthMemberPanel(mode);
      if (mode !== "invite") setInviteError("");
    };
    if (animate) {
      flipAuthCard(applyMode);
      return;
    }
    applyMode();
  };
  const setInviteMode = (enabled) => {
    setAuthPanelMode(enabled ? "invite" : "request");
  };
  const getInviteTokenFromUrl = () => {
    try {
      const params = new URLSearchParams(window.location.search || "");
      return String(params.get("inviteToken") || "").trim();
    } catch {
      return "";
    }
  };
  const hasNewClientIntentInUrl = () => {
    try {
      const params = new URLSearchParams(window.location.search || "");
      const val = String(params.get("newClient") || "").trim().toLowerCase();
      return val === "1" || val === "true" || val === "yes";
    } catch {
      return false;
    }
  };
  const clearNewClientIntentFromUrl = () => {
    try {
      const url = new URL(window.location.href);
      url.searchParams.delete("newClient");
      const next = `${url.pathname}${url.search}${url.hash}`;
      window.history.replaceState({}, "", next);
    } catch {}
  };
  const triggerNewClientFlow = () => {
    const statusEl = document.getElementById("requestStatus");
    if (statusEl) statusEl.textContent = "";
    setAuthPanelMode("request", { animate: true });
    clearNewClientIntentFromUrl();
  };
  const clearInviteTokenFromUrl = () => {
    try {
      const url = new URL(window.location.href);
      url.searchParams.delete("inviteToken");
      const next = `${url.pathname}${url.search}${url.hash}`;
      window.history.replaceState({}, "", next);
    } catch {}
  };
  const fetchPublicJson = async (url, options = {}) => {
    const res = await fetch(url, options);
    const payload = await res.json().catch(() => ({}));
    if (!res.ok) {
      throw new Error(String(payload?.error || `Request failed (${res.status})`));
    }
    return payload;
  };
  const clearQrSigninTimers = () => {
    if (qrSigninState.pollTimer) window.clearInterval(qrSigninState.pollTimer);
    if (qrSigninState.countdownTimer) window.clearInterval(qrSigninState.countdownTimer);
    qrSigninState.pollTimer = null;
    qrSigninState.countdownTimer = null;
  };
  const hideQrSigninPanel = () => {
    clearQrSigninTimers();
    qrSigninState.token = "";
    qrSigninState.approvalUrl = "";
    qrSigninState.expiresAt = 0;
    if (qrSigninPanel) qrSigninPanel.classList.add("hidden");
    if (qrSigninImage) qrSigninImage.removeAttribute("src");
    if (qrSigninLink) qrSigninLink.textContent = "";
    if (qrSigninTimer) qrSigninTimer.textContent = "";
    if (qrSigninCopy) qrSigninCopy.textContent = "Open Relay on your phone and approve this desktop sign-in.";
  };
  const updateQrSigninCountdown = () => {
    if (!qrSigninTimer || !qrSigninState.expiresAt) return;
    const remainingMs = Math.max(0, Number(qrSigninState.expiresAt || 0) - Date.now());
    const remainingSec = Math.ceil(remainingMs / 1000);
    if (remainingSec <= 0) {
      qrSigninTimer.textContent = "Code expired. Refresh to generate a new one.";
      clearQrSigninTimers();
      return;
    }
    qrSigninTimer.textContent = `Code expires in ${remainingSec}s.`;
  };
  const completeQrSignin = async (payload) => {
    clearQrSigninTimers();
    hideQrSigninPanel();
    setSession(payload, true);
    hideLoginOverlay();
    await playPostLoginIntro();
    await handlePostLogin();
  };
  const pollQrSigninStatus = async () => {
    if (!qrSigninState.token) return;
    try {
      const payload = await fetchPublicJson(`${API_BASE}/api/auth/qr/status?token=${encodeURIComponent(qrSigninState.token)}`);
      if (String(payload?.status || "") === "approved") {
        await completeQrSignin(payload);
      }
    } catch (err) {
      const message = String(err?.message || "");
      if (/not found|expired/i.test(message)) {
        if (qrSigninTimer) qrSigninTimer.textContent = "Code expired. Refresh to generate a new one.";
        clearQrSigninTimers();
        return;
      }
      setError(message || "Failed to poll QR sign-in status.");
      clearQrSigninTimers();
    }
  };
  const startQrSignin = async () => {
    setError("");
    hideQrSigninPanel();
    if (qrSigninPanel) qrSigninPanel.classList.remove("hidden");
    if (qrSigninTimer) qrSigninTimer.textContent = "Generating secure QR code...";
    try {
      const payload = await fetchPublicJson(`${API_BASE}/api/auth/qr/start`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({})
      });
      qrSigninState.token = String(payload?.token || "");
      qrSigninState.approvalUrl = String(payload?.approvalUrl || "");
      qrSigninState.expiresAt = Number(payload?.expiresAt || 0);
      if (qrSigninImage) {
        qrSigninImage.src = `https://api.qrserver.com/v1/create-qr-code/?size=220x220&format=svg&data=${encodeURIComponent(qrSigninState.approvalUrl)}`;
      }
      if (qrSigninLink) qrSigninLink.textContent = qrSigninState.approvalUrl;
      if (qrSigninCopy) {
        const host = String(window.location.hostname || "").toLowerCase();
        qrSigninCopy.textContent = (host === "127.0.0.1" || host === "localhost")
          ? "This desktop is running on a local address. Use your public app domain or LAN address for phone scanning outside this machine."
          : "Open Relay on your phone and approve this desktop sign-in.";
      }
      updateQrSigninCountdown();
      qrSigninState.countdownTimer = window.setInterval(updateQrSigninCountdown, 1000);
      qrSigninState.pollTimer = window.setInterval(() => { void pollQrSigninStatus(); }, 2000);
    } catch (err) {
      setError(err?.message || "Failed to start QR sign in.");
      hideQrSigninPanel();
    }
  };
  const loadInvitePreview = async (token) => {
    if (!token) return false;
    setInviteError("");
    const payload = await fetchPublicJson(`${API_BASE}/api/public/onboarding/invitations/${encodeURIComponent(token)}`);
    const invite = payload?.invite || {};
    if (inviteBusinessName) inviteBusinessName.value = String(invite.businessName || "Relay Workspace");
    if (inviteRole) inviteRole.value = String(invite.role || "readonly");
    const lockedEmail = String(invite.email || "").trim();
    if (inviteEmail) {
      inviteEmail.value = lockedEmail;
      inviteEmail.readOnly = Boolean(invite.emailLocked);
    }
    if (inviteSubtitle) {
      const exp = Number(invite.expiresAt || 0);
      const expiryText = exp > 0 ? `Invite expires ${new Date(exp).toLocaleString()}.` : "";
      inviteSubtitle.textContent = expiryText || "Complete your account setup.";
    }
    setInviteMode(true);
    return true;
  };

  if (!overlay.__authBound) {
    overlay.__authBound = true;

    // Password show/hide toggle (UI-only; does not change auth logic)
    const pwInput = document.getElementById("password");
    const pwToggle = document.getElementById("togglePassword");
    if (pwInput && pwToggle && !pwToggle.__bound) {
      pwToggle.__bound = true;
      pwToggle.addEventListener("click", () => {
        const isVisible = pwInput.type === "text";
        pwInput.type = isVisible ? "password" : "text";
        pwToggle.dataset.visible = String(!isVisible);
        pwToggle.setAttribute("aria-label", isVisible ? "Show password" : "Hide password");
        pwToggle.title = isVisible ? "Show password" : "Hide password";
        try { pwInput.focus({ preventScroll: true }); } catch { pwInput.focus(); }
      });
    }
    document.getElementById("logout")?.addEventListener("click", () => {
      logoutAndShowLogin();
    });
    document.getElementById("qrSigninBtn")?.addEventListener("click", () => {
      void startQrSignin();
    });
    qrSigninRefreshBtn?.addEventListener("click", () => {
      void startQrSignin();
    });
    authSwitchBtn?.addEventListener("click", () => {
      const statusEl = document.getElementById("requestStatus");
      if (statusEl) statusEl.textContent = "";
      const nextMode = authCard?.classList.contains("is-login-mode") ? "request" : "login";
      setAuthPanelMode(nextMode, { animate: true });
      if (nextMode === "login") {
        const emailInput = document.getElementById("email");
        if (emailInput) {
          try { emailInput.focus({ preventScroll: true }); } catch { emailInput.focus(); }
        }
      }
    });
    document.getElementById("workspaceRequestForm")?.addEventListener("submit", async (ev) => {
      ev.preventDefault();
      const email = document.getElementById("requestEmail")?.value.trim();
      const businessName = document.getElementById("requestBusinessName")?.value.trim();
      const industry = document.getElementById("requestIndustry")?.value;
      const description = document.getElementById("requestDescription")?.value.trim();
      const statusEl = document.getElementById("requestStatus");
      if (statusEl) statusEl.textContent = "Submitting request...";
      try {
        const response = await fetch(`${API_BASE}/api/public/onboarding/workspace-request`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            email,
            businessName,
            industry,
            description
          })
        });
        const payload = await response.json().catch(() => ({}));
        if (!response.ok) throw new Error(payload?.error || `Request failed (${response.status})`);
        if (statusEl) statusEl.textContent = "Request received. We'll reach out after payment verification.";
      } catch (err) {
        if (statusEl) statusEl.textContent = err?.message || "Failed to submit request. Please try again.";
      }
    });
    document.getElementById("loginForm")?.addEventListener("submit", async (e) => {
      e.preventDefault();
      setError("");

      const email = document.getElementById("email").value.trim();
      const password = document.getElementById("password").value;
      const persist = document.getElementById("staySignedIn")?.checked === true;

      try {
        const payload = await authRequest("/api/auth/login", {
          method: "POST",
          body: { email, password, persist }
        });
        setSession(payload, persist);
        hideLoginOverlay();
        await playPostLoginIntro();
        await handlePostLogin();
      } catch (err) {
        setError(normalizeLoginErrorMessage(err));
      }
    });

    inviteBackBtn?.addEventListener("click", () => {
      clearInviteTokenFromUrl();
      setAuthPanelMode("login", { animate: true });
    });

    inviteForm?.addEventListener("submit", async (e) => {
      e.preventDefault();
      const token = getInviteTokenFromUrl();
      if (!token) {
        setInviteError("Invite token missing. Ask for a new invite link.");
        return;
      }
      setInviteError("");
      if (inviteSubmitBtn) inviteSubmitBtn.disabled = true;
      try {
        const email = String(inviteEmail?.value || "").trim();
        const name = String(inviteName?.value || "").trim();
        const password = String(invitePassword?.value || "");
        const payload = await fetchPublicJson(
          `${API_BASE}/api/public/onboarding/invitations/${encodeURIComponent(token)}/accept`,
          {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ email, name, password })
          }
        );
        const acceptedEmail = String(payload?.user?.email || email || "").trim();
        const loginEmail = document.getElementById("email");
        const loginPassword = document.getElementById("password");
        if (loginEmail) loginEmail.value = acceptedEmail;
        if (loginPassword) loginPassword.value = "";
        if (invitePassword) invitePassword.value = "";
        setError("Invite accepted. Sign in with your email and new password.");
        clearInviteTokenFromUrl();
        setAuthPanelMode("login", { animate: true });
      } catch (err) {
        setInviteError(normalizeInviteErrorMessage(err, "Could not accept invite."));
      } finally {
        if (inviteSubmitBtn) inviteSubmitBtn.disabled = false;
      }
    });
  }

  if (IS_DEMO_MODE) {
    // Demo workspace auto-session (read-only walkthrough mode).
    setSession({
      user: { id: "demo_user", email: "demo@relay.local", role: "owner", name: "Demo Operator" },
      accounts: [{ accountId: "acct_demo", to: DEMO_TO, businessName: "Arc Relay Demo Workspace" }],
      csrfToken: "demo_csrf"
    }, false);
    setActiveTo(DEMO_TO);
    hideLoginOverlay();
    return true;
  }

  try {
    authState.csrfToken = csrfToken();
    const payload = await authRequest("/api/auth/me");
    setSession(payload, true);
    hideLoginOverlay();
    if (hasNewClientIntentInUrl()) {
      triggerNewClientFlow();
    }
    return true;
  } catch {
    clearSession();
    const inviteToken = getInviteTokenFromUrl();
    if (inviteToken) {
      try {
        await loadInvitePreview(inviteToken);
      } catch (err) {
        setInviteMode(true);
        setInviteError(normalizeInviteErrorMessage(err, "Invite link is invalid."));
      }
    } else {
      setAuthPanelMode("request", { animate: true });
    }
    showLoginOverlay();
    if (hasNewClientIntentInUrl()) {
      triggerNewClientFlow();
    }
    return false;
  }
}

function syncSignedInEmailUI() {
  const sess = getSession();
  const emailEl = document.getElementById("signedInEmail");
  if (emailEl) emailEl.textContent = sess?.email || "?";
}

/* ==========
  Storage
========== */
const LS_KEYS = {
  VIP: "mc_vip_list_v1",
};

function rulesKey(to){
  return `mc_rules_v1:${to}`;
}

// Sync rules to backend so the automation engine can evaluate them server-side
function syncRulesToBackend() {
  if (!authState.user || IS_DEMO_MODE) return;
  if (state?.onboardingRequired || shouldSkipDashboardBootForView()) return;
  const to = getActiveTo();
  fetch(`${API_BASE}/api/rules?to=${encodeURIComponent(to)}`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ to, rules: state.rules })
  }).catch(err => console.warn('Rules sync failed:', err));
}

// Sync VIP list to backend so automations respect neverAutoReply contacts
function syncVipToBackend() {
  if (!authState.user || IS_DEMO_MODE) return;
  if (state?.onboardingRequired || shouldSkipDashboardBootForView()) return;
  const to = getActiveTo();
  fetch(`${API_BASE}/api/vip?to=${encodeURIComponent(to)}`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ to, vipList: state.vip })
  }).catch(err => console.warn('VIP sync failed:', err));
}

/* ==========
  Sample data (replace later with real Twilio events)
========== */
const sampleThreads = [
  {
    id: "t1",
    name: "John (Plumbing)",
    phone: "+1 (814) 555-0192",
    tags: ["new"],
    messages: [
      { dir: "in", text: "Hey, do you do emergency service?", ts: "10:12 AM" },
      { dir: "out", text: "Yes  what's the issue and your address?", ts: "10:13 AM" },
      { dir: "in", text: "Water heater leaking. Clearfield.", ts: "10:15 AM" },
    ]
  },
  {
    id: "t2",
    name: "Unknown",
    phone: "+1 (814) 555-0144",
    tags: ["missed-call"],
    messages: [
      { dir: "out", text: "Sorry we missed your call  what service do you need?", ts: "Yesterday" }
    ]
  },
  {
    id: "t3",
    name: "Sarah",
    phone: "+1 (814) 555-0108",
    tags: ["returning"],
    messages: [
      { dir: "in", text: "Can I book for Friday morning?", ts: "Mon" },
      { dir: "out", text: "Yes  does 10:30 AM work?", ts: "Mon" },
    ]
  }
];

const sampleEvents = [
  // last 7 days-ish (dummy)
  { type:"missed_call", recovered:true, day:"Mon" },
  { type:"missed_call", recovered:false, day:"Mon" },
  { type:"missed_call", recovered:true, day:"Tue" },
  { type:"missed_call", recovered:true, day:"Wed" },
  { type:"missed_call", recovered:false, day:"Thu" },
  { type:"missed_call", recovered:true, day:"Fri" },
  { type:"missed_call", recovered:true, day:"Fri" },
  { type:"inbound_sms", recovered:true, day:"Sat" },
  { type:"inbound_sms", recovered:false, day:"Sun" },
];

function getDemoNowMs() {
  return Date.now();
}

function getDemoConversations() {
  const now = getDemoNowMs();
  const mins = (n) => now - (n * 60 * 1000);
  const hours = (n) => now - (n * 60 * 60 * 1000);
  const convoA = {
    id: "demo_c1",
    from: "+18145550101",
    to: DEMO_TO,
    status: "booked",
    stage: "appointment_booked",
    lastText: "Perfect. You are booked for tomorrow at 10:30am.",
    createdAt: hours(30),
    updatedAt: mins(14),
    lastActivityAt: mins(14),
    bookingTime: now + (20 * 60 * 60 * 1000),
    leadData: { service: "Interior + Exterior Detail", vehicle: "2022 Tesla Model 3", amount: 289, booking_time: now + (20 * 60 * 60 * 1000) },
    messages: [
      { dir: "in", text: "Hey, can I get a full detail quote for my Model 3?", ts: hours(30) },
      { dir: "out", text: "Absolutely. Full detail is $289 and takes about 3 hours. Want tomorrow 10:30am?", ts: hours(29) },
      { dir: "in", text: "Yes book it", ts: hours(28) },
      { dir: "out", text: "Perfect. You are booked for tomorrow at 10:30am.", ts: mins(14), meta: { bookingConfirmed: true, bookingTime: now + (20 * 60 * 60 * 1000) } }
    ]
  };
  const convoB = {
    id: "demo_c2",
    from: "+18145550102",
    to: DEMO_TO,
    status: "open",
    stage: "quote_shown",
    lastText: "Following up in case you still want to lock in this week.",
    createdAt: hours(10),
    updatedAt: mins(36),
    lastActivityAt: mins(36),
    leadData: { service: "Paint correction", vehicle: "F-150", amount: 420 },
    messages: [
      { dir: "in", text: "Can you fix swirl marks on black paint?", ts: hours(10) },
      { dir: "out", text: "Yes. Paint correction starts at $420 depending on panel condition.", ts: hours(9) },
      { dir: "out", text: "Following up in case you still want to lock in this week.", ts: mins(36) }
    ]
  };
  const convoC = {
    id: "demo_c3",
    from: "+18145550103",
    to: DEMO_TO,
    status: "new",
    stage: "new_lead",
    lastText: "Need ceramic quote for SUV.",
    createdAt: mins(22),
    updatedAt: mins(22),
    lastActivityAt: mins(22),
    leadData: { service: "Ceramic coating", vehicle: "2024 Tahoe" },
    messages: [
      { dir: "in", text: "Need ceramic quote for SUV.", ts: mins(22) }
    ]
  };
  return [convoA, convoB, convoC];
}

function getDemoOverview() {
  const now = getDemoNowMs();
  return {
    recoveredThisMonth: 124300,
    revenueRecoveryRate: 0.63,
    estimatedLostRevenueCents: 73100,
    recoveredRevenueCents: 124300,
    projectedRecoveryCents: 168200,
    responseTimeAvg: 6,
    missedCallCount: 17,
    quoteShown: 31,
    quoteAccepted: 19,
    revenueEvents: [
      { type: "appointment_booked", signalType: "missed_call", estimatedValueCents: 28900, createdAt: now - (14 * 60 * 1000), status: "won" },
      { type: "opportunity_recovered", signalType: "missed_call", estimatedValueCents: 42000, createdAt: now - (2 * 60 * 60 * 1000), status: "won" },
      { type: "opportunity_created", signalType: "inbound_sms", estimatedValueCents: 26000, createdAt: now - (22 * 60 * 1000), status: "open" }
    ]
  };
}

function getDemoFunnel() {
  return { quoteStarted: 44, quoteReady: 34, quoteShown: 31, quoteAccepted: 19 };
}

function getDemoWins() {
  return {
    today: { bookedJobs: 3, recoveredRevenueCents: 28900, recoveredCalls: 4 },
    week: { responseSlaMinutes: 6 }
  };
}

function getDemoSummary(rangeDays = 7) {
  const days = Number(rangeDays || 7);
  return {
    rangeDays: days,
    totals: {
      inboundLeads: 58,
      respondedConversations: 47,
      bookedLeads: 19,
      responseRate: 81,
      conversionRate: 40
    },
    speed: {
      avgFirstResponseMin: 6,
      buckets: { under5: 18, min5to15: 22, over15: 7 }
    },
    funnel: { inboundLeads: 58, responded: 47, qualified: 31, booked: 19 },
    daily: [
      { day: "2026-03-03", leads: 7, booked: 2 },
      { day: "2026-03-04", leads: 8, booked: 3 },
      { day: "2026-03-05", leads: 9, booked: 2 },
      { day: "2026-03-06", leads: 11, booked: 4 },
      { day: "2026-03-07", leads: 10, booked: 4 },
      { day: "2026-03-08", leads: 7, booked: 2 },
      { day: "2026-03-09", leads: 6, booked: 2 }
    ]
  };
}

function getDemoAccount() {
  return {
    accountId: "acct_demo",
    to: DEMO_TO,
    businessName: "Arc Relay Demo Workspace",
    settings: {
      onboarding: { completed: true },
      compliance: {
        consent: {
          requireForOutbound: true,
          consentCheckboxText: "I confirm I have consent to message this contact.",
          consentSourceOptions: ["verbal", "form", "existing_customer", "other"]
        }
      }
    },
    workspace: { identity: { logoUrl: "/logos/main.png" } }
  };
}

async function sendDashboardMessage() {
  if (IS_DEMO_MODE) {
    alert("Demo mode is read-only. Start with Arc Relay to send live messages.");
    return;
  }
  const input = document.getElementById("chatInput");
  const text = input?.value?.trim();
  if (!text) return;
  if (!state.activeConversation?.id) {
    console.warn("No activeConversation selected");
    return;
  }

  const convoId = state.activeConversation.id;
  const consentWrap = document.getElementById("sendConsentWrap");
  const consentCheck = document.getElementById("sendConsentCheck");
  const consentSource = document.getElementById("sendConsentSource");
  const needsConsentCheck = consentWrap && consentWrap.style.display !== "none";
  const consentConfirmed = needsConsentCheck ? consentCheck?.checked === true : false;

  if (needsConsentCheck && !consentConfirmed) {
    alert("Consent confirmation is required before sending.");
    return;
  }

  // Optimistic UI: show immediately
  state.activeConversation.messages = state.activeConversation.messages || [];
  const optimisticTs = Date.now();
  state.activeConversation.messages.push({ dir: "out", text, ts: optimisticTs });
  input.value = "";
  renderChatFromAPI();

  try {
    console.log("Sending to backend:", convoId, text);

    const res = await fetch(`${API_BASE}/api/conversations/${encodeURIComponent(convoId)}/send?to=${encodeURIComponent(getActiveTo())}`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        text,
        consentConfirmed,
        consentSource: consentSource?.value || "other"
      }),
    });

    if (!res.ok) {
      let errBody = null;
      try {
        errBody = await res.json();
      } catch {
        const errTxt = await res.text();
        throw new Error(`Send failed ${res.status}: ${errTxt}`);
      }
      const msg = errBody?.message || `Send failed (${res.status})`;
      const code = errBody?.code ? ` [${errBody.code}]` : "";
      throw new Error(`${msg}${code}`);
    }

    // Use the updated conversation from the response (already has the new message)
    const data = await res.json();
    console.log("Backend response:", data);
  
    if (data.conversation) {
      state.activeConversation = data.conversation;
      const convoId = String(data.conversation.id || state.activeThreadId || "").trim();
      if (convoId) {
        state.conversationCacheById[getConversationCacheKey(convoId)] = { loadedAt: Date.now(), data: cloneJsonSafe(data.conversation) };
      }
      invalidateWorkspaceCacheForPath("/api/conversations");
      renderChatFromAPI();
    }

  } catch (err) {
    // remove optimistic message if backend rejected
    const msgs = state.activeConversation?.messages || [];
    const idx = msgs.findIndex((m) => m.dir === "out" && m.text === text && m.ts === optimisticTs);
    if (idx >= 0) {
      msgs.splice(idx, 1);
      renderChatFromAPI();
    }
    console.error(err);
    alert(err?.message || "Send failed.");
  }
}




/* ==========
  State
========== */
const state = {
  view: getStoredView(),
  threads: [],
  activeThreadId: null,
  activeTo: getActiveTo(),
  activeConversation: null, // full conversation (messages + leadData)
  activeCompliance: null,
  homeRangeDays: Number(localStorage.getItem("mc_home_range_days_v1") || 1),
  analyticsRange: Number(localStorage.getItem(ANALYTICS_RANGE_KEY) || 1),
  analyticsSummaryCache: {},
  analyticsBookedFallbackCache: {},
  analyticsBookedFallbackLoading: {},
  analyticsThreadsLoading: false,
  analyticsHydrationLoading: false,
  analyticsHydratedAtByScope: {},
  analyticsLoading: false,
  analyticsError: null,
  homeOverviewCache: {},
  homeOverviewLoading: false,
  homeOverviewError: null,
  homeFunnelCache: {},
  homeFunnelLoading: false,
  homeFunnelError: null,
  homeWinsCache: {},
  homeWinsLoading: false,
  homeWinsError: null,
  topbarContactsCacheByScope: {},
  topbarMetricsCacheByScope: {},
  recoveredTotalsByCustomer: {},
  recoveredTotalsLoadingByCustomer: {},
  recoveredTotalsOptimisticApplied: {},
  threadMoneyPillsById: {},
  threadsLastLoadedAt: 0,
  threadListCacheByTo: {},
  threadListPromiseByTo: {},
  conversationCacheById: {},
  conversationPromiseById: {},
  accountSettingsCacheByTo: {},
  accountSettingsPromiseByTo: {},
  contactsCacheByTo: {},
  contactsPromiseByTo: {},
  recentLeadsCache: {},
  recentActivityCache: {},
  revenueCache: {},
  revenueActivityCache: {},
  revenueOptimizationCache: {},
  revenueAgentMetricsCache: {},
  revenuePlaybookPerfCache: {},
  revenueReviewQueueCache: {},
  revenueLoading: false,
  revenueAgentLoading: false,
  revenueError: null,
  revenueAgentError: null,
  revenuePanel: "overview",
  revenueSelectedOpportunityId: null,
  revenueTimelineCache: {},
  revenueRunCache: {},
  outcomePacksCache: null,
  outcomePackLoading: false,
  outcomePackError: null,
  revenueAccountSettings: null,
  revenueAccountLoading: false,
  revenueAccountError: null,
  onboardingRequired: false,
  onboardingLoading: false,
  onboardingError: null,
  onboardingOptions: null,
  onboardingFlows: [],
  onboardingAccountSettings: null,
  onboardingCheckedKey: "",
  onboardingDraft: null,
  onboardingDraftKey: "",
  onboardingStep: 1,
  onboardingTestStatus: "",
  onboardingBookingPreviewKey: "",
  onboardingBookingPreview: null,
  onboardingBookingPreviewLoading: false,
  onboardingBookingPreviewError: null,
  scheduleAccount: null,
  scheduleFocusDate: null,
  chatExpandedByConversation: {},
  simulationActive: false,
  simulationConversationId: null,
  simulationBlockConversationLoad: false,
  vip: loadLS(LS_KEYS.VIP, [
    { id: crypto.randomUUID(), name: "Mom", phone: "+1 (814) 555-0001", neverAutoReply: true },
    { id: crypto.randomUUID(), name: "Best Friend", phone: "+1 (814) 555-0002", neverAutoReply: true },
  ]),
  rules: loadLS(rulesKey(getActiveTo()), [
   {
     id: crypto.randomUUID(),
      name: "Missed call -> instant text",
      enabled: true,
      trigger: "missed_call",
      businessHoursOnly: true,
      firstTimeOnly: true,
      sendText: true,
      template: "Sorry we missed your call - what service do you need?",
    }
  ]),
};

const THREADS_CACHE_TTL_MS = 15 * 1000;
const CONVERSATION_CACHE_TTL_MS = 10 * 1000;
const ACCOUNT_SETTINGS_CACHE_TTL_MS = 30 * 1000;
const CONTACTS_CACHE_TTL_MS = 20 * 1000;
const ANALYTICS_THREAD_HYDRATION_TTL_MS = 60 * 1000;
const SIMULATED_CONVERSATIONS_STORAGE_PREFIX = "mc_simulated_conversations_v1";
const SIMULATED_CONVERSATIONS_GLOBAL_SCOPE = "__developer_simulations__";

function cloneJsonSafe(value) {
  if (value == null) return value;
  return JSON.parse(JSON.stringify(value));
}

let simulationAnimationTimer = null;

const ONBOARDING_DAY_KEYS = ["mon", "tue", "wed", "thu", "fri", "sat", "sun"];
const ONBOARDING_DAY_LABELS = { mon: "Mon", tue: "Tue", wed: "Wed", thu: "Thu", fri: "Fri", sat: "Sat", sun: "Sun" };
const ONBOARDING_TIMEZONE_OPTIONS = [
  "America/New_York",
  "America/Chicago",
  "America/Denver",
  "America/Phoenix",
  "America/Los_Angeles",
  "America/Anchorage",
  "Pacific/Honolulu",
  "UTC"
];
const ONBOARDING_BOOKING_IMPORT_OPTIONS = [
  { value: "none", label: "No external calendar import" },
  { value: "calendly", label: "Calendly" },
  { value: "google_calendar", label: "Google Calendar" },
  { value: "outlook", label: "Outlook / Microsoft 365" },
  { value: "square", label: "Square Appointments" },
  { value: "acuity", label: "Acuity Scheduling" },
  { value: "other", label: "Other platform" }
];
const ONBOARDING_SERVICE_ID_REGEX = /^[a-z0-9_]{2,40}$/;
const DEFAULT_ONBOARDING_SERVICES = [
  { id: "full", name: "Full Detail (Interior + Exterior)", price: "$200-300", hoursMin: 3, hoursMax: 4 },
  { id: "interior", name: "Interior Detail", price: "$120-180", hoursMin: 2, hoursMax: 3 },
  { id: "exterior", name: "Exterior Detail", price: "$90-140", hoursMin: 1, hoursMax: 2 }
];
const ONBOARDING_PRICING_SERVICE_LABELS = {
  full: "Full Detail",
  interior: "Interior Detail",
  exterior: "Exterior Wash & Wax",
  ceramic: "Ceramic Coating",
  tint: "Window Tint",
  headlight: "Headlight Restoration",
  paint_correction: "Paint Correction",
  ppf: "PPF"
};
const ONBOARDING_PRICING_SCOPES = [
  { key: "spot", label: "Spot / Small area" },
  { key: "standard", label: "Single panel typical" },
  { key: "large", label: "Multi-panel / severe" }
];
const ONBOARDING_DEFAULT_SERVICE_SCOPE_TEMPLATES = {
  full: [{ key: "basic", label: "Basic" }, { key: "standard", label: "Standard" }, { key: "premium", label: "Premium" }],
  interior: [
    { key: "light", label: "Light (quick refresh)" },
    { key: "pet_hair", label: "Pet hair removal" },
    { key: "stains_odor", label: "Stains / odor treatment" },
    { key: "heavy", label: "Heavy soil + deep clean" }
  ],
  exterior: [{ key: "basic", label: "Basic" }, { key: "standard", label: "Standard" }, { key: "premium", label: "Premium" }],
  ceramic: [{ key: "one_year", label: "1 Year" }, { key: "two_year", label: "2 Year" }, { key: "five_year", label: "5 Year" }],
  tint: [
    { key: "front_two", label: "Front 2 windows" },
    { key: "rear_two", label: "Rear 2 windows" },
    { key: "back_window", label: "Back windshield (rear glass)" },
    { key: "side_set_four", label: "4 side windows" },
    { key: "full_sides_plus_back", label: "All sides + rear glass" },
    { key: "windshield_full", label: "Full front windshield" },
    { key: "windshield_strip", label: "Windshield brow/strip" },
    { key: "sunroof", label: "Sunroof tint" },
    { key: "remove_old_tint", label: "Old tint removal" },
    { key: "adhesive_cleanup", label: "Adhesive cleanup / glue removal" }
  ],
  headlight: [{ key: "light", label: "Light" }, { key: "moderate", label: "Moderate" }, { key: "heavy", label: "Heavy" }],
  paint_correction: [{ key: "spot", label: "Spot" }, { key: "standard", label: "Standard" }, { key: "large", label: "Large" }],
  ppf: [{ key: "partial", label: "Partial" }, { key: "full", label: "Full" }, { key: "full_vehicle", label: "Full Vehicle" }]
};

function sanitizeOnboardingScopeId(raw, fallback = "scope") {
  const base = String(raw || "")
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "_")
    .replace(/^_+|_+$/g, "")
    .slice(0, 40);
  return base || fallback;
}

function onboardingHumanizeKey(key) {
  const k = String(key || "").trim();
  if (!k) return "";
  if (k.toLowerCase() === "ppf") return "PPF";
  return k.replace(/[_-]+/g, " ").replace(/\b\w/g, (m) => m.toUpperCase());
}

function onboardingPricingLabelForService(serviceKey) {
  const key = String(serviceKey || "").trim();
  return ONBOARDING_PRICING_SERVICE_LABELS[key] || onboardingHumanizeKey(key);
}

function onboardingExtractFlowServiceKeys(flow) {
  const intents = flow?.steps?.detect_intent_ai?.intents || {};
  return Object.keys(intents)
    .map((k) => String(k || "").trim())
    .filter((k) => k && k !== "other" && k !== "escalate");
}

function isDetailerFlow(flow) {
  const id = String(flow?.id || "").toLowerCase();
  const name = String(flow?.name || "").toLowerCase();
  const industry = String(flow?.industry || "").toLowerCase();
  if (id === "detailing_missed_call_v1") return true;
  if (id.includes("detail")) return true;
  if (name.includes("detail")) return true;
  if (industry.includes("detail")) return true;
  if (industry.includes("auto_detail")) return true;
  return false;
}

function getOnboardingDetailerFlows(flows) {
  const list = Array.isArray(flows) ? flows : [];
  const filtered = list.filter((f) => isDetailerFlow(f));
  return filtered.length ? filtered : list;
}

function normalizeOnboardingFlowLabel(flow) {
  const raw = String(flow?.name || flow?.id || "Flow").trim();
  return raw.replace(/\s*:\s*missed\s*call.*$/i, "").trim() || raw;
}

function onboardingGetPricingServiceList(flowId, flows = [], pricingByFlow = {}) {
  const flow = Array.isArray(flows) ? flows.find((f) => String(f?.id || "") === String(flowId || "")) : null;
  const fromFlow = onboardingExtractFlowServiceKeys(flow);
  const fromPricing = Object.keys(pricingByFlow?.[flowId]?.services || {});
  const ids = Array.from(new Set([...fromFlow, ...fromPricing]));
  if (!ids.length) return DEFAULT_ONBOARDING_SERVICES.map((s) => ({ key: s.id, label: onboardingPricingLabelForService(s.id) }));
  return ids.map((id) => ({ key: id, label: onboardingPricingLabelForService(id) }));
}

function onboardingGetScopeItemsForService(serviceKey, pricing) {
  const existingKeys = Object.keys(pricing?.serviceScopes?.[serviceKey] || {});
  if (existingKeys.length) {
    return existingKeys.map((k) => ({ key: k, label: onboardingHumanizeKey(k) }));
  }
  return ONBOARDING_DEFAULT_SERVICE_SCOPE_TEMPLATES[serviceKey] || [];
}

function onboardingNormalizePricingConfig(value, serviceKeys = []) {
  const src = value && typeof value === "object" ? value : {};
  const out = { services: {}, paintScopes: {}, serviceScopes: {} };
  const uniqueServiceKeys = Array.from(new Set([
    ...DEFAULT_ONBOARDING_SERVICES.map((x) => x.id),
    ...Object.keys(ONBOARDING_PRICING_SERVICE_LABELS),
    ...serviceKeys,
    ...Object.keys(src?.services || {})
  ]));
  for (const key of uniqueServiceKeys) {
    const cur = src?.services?.[key] || {};
    const fallback = DEFAULT_ONBOARDING_SERVICES.find((x) => x.id === key) || { price: "$0-0", hoursMin: 1, hoursMax: 1 };
    const hoursMin = Math.max(0, Number(cur.hoursMin ?? fallback.hoursMin) || 0);
    const hoursMax = Math.max(hoursMin, Number(cur.hoursMax ?? fallback.hoursMax) || hoursMin);
    out.services[key] = {
      name: String(cur.name || onboardingPricingLabelForService(key)).trim(),
      price: String(cur.price || fallback.price || "$0-0").trim(),
      hoursMin,
      hoursMax
    };
  }
  for (const item of ONBOARDING_PRICING_SCOPES) {
    const cur = src?.paintScopes?.[item.key] || {};
    const hoursMin = Math.max(0, Number(cur.hoursMin ?? 1) || 0);
    const hoursMax = Math.max(hoursMin, Number(cur.hoursMax ?? 1) || hoursMin);
    out.paintScopes[item.key] = {
      price: String(cur.price || "$0-0").trim(),
      hoursMin,
      hoursMax
    };
  }
  const scopeServices = Array.from(new Set([
    ...uniqueServiceKeys,
    ...Object.keys(src?.serviceScopes || {})
  ]));
  for (const serviceKey of scopeServices) {
    const scopes = onboardingGetScopeItemsForService(serviceKey, src);
    if (!scopes.length) continue;
    out.serviceScopes[serviceKey] = {};
    for (const scope of scopes) {
      const cur = src?.serviceScopes?.[serviceKey]?.[scope.key] || {};
      const fallback = out.services?.[serviceKey] || { price: "$0-0", hoursMin: 1, hoursMax: 1 };
      const hoursMin = Math.max(0, Number(cur.hoursMin ?? fallback.hoursMin) || 0);
      const hoursMax = Math.max(hoursMin, Number(cur.hoursMax ?? fallback.hoursMax) || hoursMin);
      out.serviceScopes[serviceKey][scope.key] = {
        name: String(cur.name || `${onboardingPricingLabelForService(serviceKey)} (${scope.label})`).trim(),
        price: String(cur.price || fallback.price || "$0-0").trim(),
        hoursMin,
        hoursMax
      };
    }
  }
  return out;
}

function sanitizeOnboardingServiceId(raw, fallback = "service") {
  const base = String(raw || "")
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "_")
    .replace(/^_+|_+$/g, "")
    .slice(0, 40);
  const safe = base || String(fallback || "service").toLowerCase().replace(/[^a-z0-9]+/g, "_").replace(/^_+|_+$/g, "") || "service";
  return ONBOARDING_SERVICE_ID_REGEX.test(safe) ? safe : "service";
}

function normalizeOnboardingServices(input) {
  const list = Array.isArray(input) ? input : [];
  const out = [];
  const seen = new Set();
  for (const row of list) {
    const name = String(row?.name || "").trim();
    const price = String(row?.price || "").trim();
    const hoursMin = Math.max(0, Number(row?.hoursMin || 0) || 0);
    const hoursMaxRaw = Math.max(0, Number(row?.hoursMax || 0) || 0);
    const hoursMax = hoursMaxRaw >= hoursMin ? hoursMaxRaw : hoursMin;
    if (!name && !price) continue;
    let id = sanitizeOnboardingServiceId(row?.id || name, "service");
    let n = 2;
    while (seen.has(id)) {
      id = `${id}_${n}`;
      n += 1;
    }
    seen.add(id);
    out.push({ id, name: name || humanizeKey(id), price: price || "$0-0", hoursMin, hoursMax });
  }
  if (out.length) return out;
  return DEFAULT_ONBOARDING_SERVICES.map((s) => ({ ...s }));
}

function defaultOnboardingBusinessHours() {
  return {
    mon: [{ start: "09:00", end: "17:00" }],
    tue: [{ start: "09:00", end: "17:00" }],
    wed: [{ start: "09:00", end: "17:00" }],
    thu: [{ start: "09:00", end: "17:00" }],
    fri: [{ start: "09:00", end: "17:00" }],
    sat: [],
    sun: []
  };
}

function cloneJson(value) {
  try {
    return JSON.parse(JSON.stringify(value ?? null));
  } catch {
    return null;
  }
}

function normalizeOnboardingBusinessHours(input) {
  const fallback = defaultOnboardingBusinessHours();
  const src = input && typeof input === "object" ? input : {};
  const out = {};
  ONBOARDING_DAY_KEYS.forEach((day) => {
    const slots = Array.isArray(src?.[day]) ? src[day] : fallback[day];
    const normalized = [];
    for (const slot of slots) {
      const start = String(slot?.start || "").trim();
      const end = String(slot?.end || "").trim();
      if (!/^\d{2}:\d{2}$/.test(start) || !/^\d{2}:\d{2}$/.test(end)) continue;
      if (start >= end) continue;
      normalized.push({ start, end });
    }
    out[day] = normalized;
  });
  return out;
}

function buildOnboardingDraft(account, options = null) {
  const onboarding = account?.settings?.onboarding || {};
  const finance = account?.settings?.finance || {};
  const workspace = account?.workspace || {};
  const identity = workspace?.identity || {};
  const packs = Array.isArray(options?.packs) ? options.packs : [];
  const selectedFromAccount = Array.isArray(onboarding?.selectedPacks) ? onboarding.selectedPacks : [];
  const selectedFromPacks = packs.filter((p) => p?.enabled === true).map((p) => String(p?.id || "")).filter(Boolean);
  const selectedPacks = selectedFromAccount.length ? selectedFromAccount : selectedFromPacks;
  const avgTicketCents = Number(finance?.averageTicketValueCents || 0);
  const defaultFlowId = String(account?.defaults?.defaultFlowId || "").trim();
  const pricingByFlow = workspace?.pricingByFlow && typeof workspace.pricingByFlow === "object"
    ? cloneJson(workspace.pricingByFlow) || {}
    : {};
  if (defaultFlowId && !pricingByFlow[defaultFlowId]) {
    pricingByFlow[defaultFlowId] = {
      services: cloneJson(workspace?.pricing?.services || {})
    };
  }
  const onboardingServices = normalizeOnboardingServices(
    Object.entries(workspace?.pricing?.services || {}).map(([id, svc]) => ({
      id: String(id || ""),
      name: String(svc?.name || "").trim(),
      price: String(svc?.price || "").trim(),
      hoursMin: Number(svc?.hoursMin || 0),
      hoursMax: Number(svc?.hoursMax || 0)
    }))
  );
  return {
    businessType: String(identity?.industry || "").trim(),
    businessName: String(identity?.businessName || account?.businessName || "").trim(),
    businessEmail: String(identity?.businessEmail || "").trim(),
    businessLogoUrl: String(identity?.logoUrl || "").trim(),
    businessPhone: String(identity?.businessPhone || "").trim(),
    avgTicketValueDollars: avgTicketCents > 0 ? Math.round(avgTicketCents / 100) : 150,
    bookingMode: String(account?.scheduling?.mode || "internal").trim() === "link"
      ? "external_scheduler"
      : String(account?.scheduling?.mode || "internal").trim() === "manual"
        ? "manual_follow_up"
        : "relay_scheduler",
    bookingImportSource: String(workspace?.settings?.bookingImportSource || "none").trim() || "none",
    bookingUrl: String(account?.scheduling?.url || account?.bookingUrl || "").trim(),
    phoneConnected: onboarding?.phoneConnected === true || account?.integrations?.twilio?.enabled === true,
    calendarConnected: onboarding?.calendarConnected === true || account?.integrations?.calendar?.enabled === true,
    outcomePacks: selectedPacks,
    services: onboardingServices,
    defaultFlowId,
    pricingByFlow,
    timezone: String(workspace?.timezone || "America/New_York").trim() || "America/New_York",
    businessHours: normalizeOnboardingBusinessHours(workspace?.businessHours),
    testMessage: "Hi, this is a test message from onboarding."
  };
}

function readOnboardingDraftFromForm(form, currentDraft) {
  const data = new FormData(form);
  const next = { ...(currentDraft || {}) };
  next.businessType = String(data.get("businessType") || "").trim();
  next.businessName = String(data.get("businessName") || "").trim();
  next.businessEmail = String(data.get("businessEmail") || "").trim();
  next.businessLogoUrl = String(data.get("businessLogoUrl") || "").trim();
  next.businessPhone = String(data.get("businessPhone") || "").trim();
  next.avgTicketValueDollars = Math.max(0, Number(data.get("avgTicketValueDollars") || 0));
  next.bookingMode = String(data.get("bookingMode") || "relay_scheduler").trim() || "relay_scheduler";
  next.bookingImportSource = String(data.get("bookingImportSource") || "none").trim() || "none";
  next.bookingUrl = String(data.get("bookingUrl") || "").trim();
  next.phoneConnected = currentDraft?.phoneConnected !== false;
  next.calendarConnected = String(data.get("calendarConnected") || "").trim() === "1";
  next.timezone = String(data.get("timezone") || "America/New_York").trim() || "America/New_York";
  next.testMessage = String(data.get("testMessage") || "").trim() || "Hi, this is a test message from onboarding.";
  const selectedPacks = data.getAll("outcomePacks").map((x) => String(x || "").trim()).filter(Boolean);
  next.outcomePacks = selectedPacks;
  next.defaultFlowId = String(data.get("pricingFlowId") || currentDraft?.defaultFlowId || "").trim();
  const hours = {};
  ONBOARDING_DAY_KEYS.forEach((day) => {
    const isOpen = data.has(`open_${day}`);
    if (!isOpen) {
      hours[day] = [];
      return;
    }
    const start = String(data.get(`start_${day}`) || "").trim();
    const end = String(data.get(`end_${day}`) || "").trim();
    if (/^\d{2}:\d{2}$/.test(start) && /^\d{2}:\d{2}$/.test(end) && start < end) {
      hours[day] = [{ start, end }];
      return;
    }
    hours[day] = [];
  });
  next.businessHours = normalizeOnboardingBusinessHours(hours);
  const flowId = String(next.defaultFlowId || "").trim();
  const pricingByFlow = currentDraft?.pricingByFlow && typeof currentDraft.pricingByFlow === "object"
    ? cloneJson(currentDraft.pricingByFlow) || {}
    : {};
  if (flowId) {
    const serviceList = onboardingGetPricingServiceList(flowId, state.onboardingFlows || [], pricingByFlow);
    const serviceKeys = serviceList.map((x) => x.key);
    const flowPricing = onboardingNormalizePricingConfig(pricingByFlow[flowId] || {}, serviceKeys);
    const inputs = Array.from(form.querySelectorAll("[data-onb-pricing-kind][data-onb-pricing-key][data-onb-pricing-field]"));
    for (const el of inputs) {
      const kind = String(el.getAttribute("data-onb-pricing-kind") || "").trim();
      const key = String(el.getAttribute("data-onb-pricing-key") || "").trim();
      const field = String(el.getAttribute("data-onb-pricing-field") || "").trim();
      if (!kind || !key || !field) continue;
      if (kind === "serviceScopes") {
        const scopeKey = String(el.getAttribute("data-onb-pricing-scope") || "").trim();
        if (!scopeKey) continue;
        flowPricing.serviceScopes[key] = flowPricing.serviceScopes[key] || {};
        flowPricing.serviceScopes[key][scopeKey] = flowPricing.serviceScopes[key][scopeKey] || {
          name: `${onboardingPricingLabelForService(key)} (${onboardingHumanizeKey(scopeKey)})`,
          price: "$0-0",
          hoursMin: 1,
          hoursMax: 1
        };
        if (field === "name") flowPricing.serviceScopes[key][scopeKey].name = String(el.value || "").trim() || `${onboardingPricingLabelForService(key)} (${onboardingHumanizeKey(scopeKey)})`;
        if (field === "price") flowPricing.serviceScopes[key][scopeKey].price = String(el.value || "").trim();
        if (field === "hoursMin" || field === "hoursMax") flowPricing.serviceScopes[key][scopeKey][field] = Math.max(0, Number(el.value || 0) || 0);
        continue;
      }
      flowPricing[kind] = flowPricing[kind] || {};
      flowPricing[kind][key] = flowPricing[kind][key] || { name: onboardingPricingLabelForService(key), price: "$0-0", hoursMin: 1, hoursMax: 1 };
      if (field === "name") flowPricing[kind][key].name = String(el.value || "").trim() || onboardingPricingLabelForService(key);
      if (field === "price") flowPricing[kind][key].price = String(el.value || "").trim();
      if (field === "hoursMin" || field === "hoursMax") flowPricing[kind][key][field] = Math.max(0, Number(el.value || 0) || 0);
    }
    for (const [svcKey, row] of Object.entries(flowPricing.services || {})) {
      if (!row) continue;
      row.name = String(row.name || onboardingPricingLabelForService(svcKey)).trim();
      row.hoursMax = Math.max(Number(row.hoursMin || 0), Number(row.hoursMax || 0));
    }
    for (const item of ONBOARDING_PRICING_SCOPES) {
      const row = flowPricing.paintScopes?.[item.key];
      if (!row) continue;
      row.hoursMax = Math.max(Number(row.hoursMin || 0), Number(row.hoursMax || 0));
    }
    for (const [svcKey, scopes] of Object.entries(flowPricing.serviceScopes || {})) {
      for (const [scopeKey, row] of Object.entries(scopes || {})) {
        if (!row) continue;
        row.name = String(row.name || `${onboardingPricingLabelForService(svcKey)} (${onboardingHumanizeKey(scopeKey)})`).trim();
        row.hoursMax = Math.max(Number(row.hoursMin || 0), Number(row.hoursMax || 0));
      }
    }
    pricingByFlow[flowId] = flowPricing;
  }
  next.pricingByFlow = pricingByFlow;
  const effectiveFlowId = String(flowId || "").trim();
  const effectivePricing = effectiveFlowId ? onboardingNormalizePricingConfig(pricingByFlow?.[effectiveFlowId] || {}, onboardingGetPricingServiceList(effectiveFlowId, state.onboardingFlows || [], pricingByFlow).map((x) => x.key)) : { services: {} };
  next.services = normalizeOnboardingServices(Object.entries(effectivePricing.services || {}).map(([id, row]) => ({
    id,
    name: row?.name || onboardingPricingLabelForService(id),
    price: row?.price || "$0-0",
    hoursMin: Number(row?.hoursMin || 0),
    hoursMax: Number(row?.hoursMax || 0)
  })));
  return next;
}

async function refreshOnboardingGate({ force = false } = {}) {
  const scopeKey = getAnalyticsScopeKey();
  const scopeSnapshot = createUiScopeSnapshot({ to: getActiveTo() });
  if (!force && state.onboardingCheckedKey === scopeKey && state.onboardingAccountSettings) return;
  state.onboardingLoading = true;
  state.onboardingError = null;
  try {
    const account = await loadAccountSettings(getActiveTo());
    if (!isUiScopeCurrent(scopeSnapshot)) return;
    state.onboardingAccountSettings = account || null;
    const onboarding = account?.settings?.onboarding || {};
    state.onboardingRequired = Boolean(account) && onboarding.completed !== true;
    state.onboardingCheckedKey = scopeKey;

    const shouldLoadOnboardingArtifacts = state.onboardingRequired || isOnboardingView();
    if (shouldLoadOnboardingArtifacts) {
      const [options, flowRes] = await Promise.all([
        apiGetOptional("/api/onboarding/options", { packs: [], onboarding: {} }),
        apiGetOptional("/api/flows", { flows: [] })
      ]);
      if (!isUiScopeCurrent(scopeSnapshot)) return;
      state.onboardingOptions = options || { packs: [], onboarding: {} };
      state.onboardingFlows = Array.isArray(flowRes?.flows) ? flowRes.flows : [];
      if (!state.onboardingDraft || state.onboardingDraftKey !== scopeKey || force) {
        state.onboardingDraft = buildOnboardingDraft(account, state.onboardingOptions);
        state.onboardingDraftKey = scopeKey;
        state.onboardingStep = 1;
        state.onboardingTestStatus = "";
      }
    } else {
      if (!isUiScopeCurrent(scopeSnapshot)) return;
      state.onboardingOptions = { packs: [], onboarding: onboarding || {} };
      state.onboardingFlows = [];
    }
  } catch (err) {
    if (!isUiScopeCurrent(scopeSnapshot)) return;
    state.onboardingError = err?.message || "Failed to load onboarding.";
    state.onboardingRequired = false;
  } finally {
    if (!isUiScopeCurrent(scopeSnapshot)) return;
    state.onboardingLoading = false;
  }
}

function readFileAsDataUrl(file) {
  return new Promise((resolve, reject) => {
    const reader = new FileReader();
    reader.onload = () => resolve(String(reader.result || ""));
    reader.onerror = () => reject(new Error("Failed to read file"));
    reader.readAsDataURL(file);
  });
}

async function uploadOnboardingLogoFile(file, activeTo) {
  const maxBytes = 750 * 1024;
  const maxWidth = 3000;
  const maxHeight = 3000;
  const mime = String(file?.type || "").toLowerCase();
  const allowed = new Set(["image/png", "image/jpeg", "image/jpg", "image/webp", "image/gif"]);
  if (!allowed.has(mime)) {
    throw new Error("Logo must be PNG, JPG, WEBP, or GIF.");
  }
  if (Number(file?.size || 0) > maxBytes) {
    throw new Error("Logo must be 750KB or smaller.");
  }
  try {
    const blobUrl = URL.createObjectURL(file);
    const dims = await new Promise((resolve, reject) => {
      const img = new Image();
      img.onload = () => resolve({ width: Number(img.naturalWidth || 0), height: Number(img.naturalHeight || 0) });
      img.onerror = () => reject(new Error("Invalid image file"));
      img.src = blobUrl;
    });
    URL.revokeObjectURL(blobUrl);
    if (!dims.width || !dims.height) throw new Error("Could not read image dimensions");
    if (dims.width > maxWidth || dims.height > maxHeight) {
      throw new Error(`Logo dimensions must be <= ${maxWidth}x${maxHeight}.`);
    }
  } catch (err) {
    throw new Error(err?.message || "Unable to validate image dimensions");
  }
  const dataUrl = await readFileAsDataUrl(file);
  const to = String(activeTo || "").trim();
  const path = to ? `/api/account/logo?to=${encodeURIComponent(to)}` : "/api/account/logo";
  const result = await apiPost(path, {
    dataUrl,
    fileName: String(file?.name || "").slice(0, 160)
  });
  return result;
}

async function sendOnboardingTestMessage() {
  const active = resolveActiveWorkspace();
  const to = String(active?.to || "").trim();
  if (!to || !isLikelyPhoneSelector(to)) {
    throw new Error("No valid workspace phone number selected. Pick a workspace in Connected and try again.");
  }
  const body = String(state.onboardingDraft?.testMessage || "").trim() || "Hi, this is a test message from onboarding.";
  const res = await fetch(`${API_BASE}/webhooks/sms`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ To: to, From: "+15555550123", Body: body })
  });
  if (!res.ok) {
    const txt = await res.text().catch(() => "");
    throw new Error(`Test failed (${res.status})${txt ? `: ${txt}` : ""}`);
  }
  return res.json().catch(() => ({}));
}

function onboardingBookingTokenFromUrl(url) {
  const raw = String(url || "").trim();
  if (!raw) return "";
  try {
    const u = new URL(raw, window.location.origin);
    const m = String(u.pathname || "").match(/\/book\/([^/]+)/i);
    return m ? decodeURIComponent(String(m[1] || "").trim()) : "";
  } catch {
    const m = raw.match(/\/book\/([^/?#]+)/i);
    return m ? decodeURIComponent(String(m[1] || "").trim()) : "";
  }
}

async function fetchOnboardingBookingPreview(publicUrl) {
  const token = onboardingBookingTokenFromUrl(publicUrl);
  if (!token) return null;
  const dateIso = new Date().toISOString().slice(0, 10);
  const [configRes, availRes] = await Promise.all([
    fetch(`${API_BASE}/api/public/booking/${encodeURIComponent(token)}/config`),
    fetch(`${API_BASE}/api/public/booking/${encodeURIComponent(token)}/availability?date=${encodeURIComponent(dateIso)}&days=3`)
  ]);
  const cfg = await configRes.json().catch(() => ({}));
  const availability = await availRes.json().catch(() => ({}));
  if (!configRes.ok) throw new Error(String(cfg?.error || `Preview failed (${configRes.status})`));
  if (!availRes.ok) throw new Error(String(availability?.error || `Availability failed (${availRes.status})`));
  return {
    token,
    businessName: String(cfg?.businessName || "").trim(),
    timezone: String(availability?.timezone || cfg?.timezone || "").trim(),
    days: Array.isArray(availability?.days) ? availability.days : []
  };
}

async function ensureOnboardingRelayBookingUrl() {
  const active = resolveActiveWorkspace();
  const to = String(active?.to || "").trim();
  const accountId = String(active?.accountId || "").trim();
  if (!to && !accountId) {
    throw new Error("No workspace selected. Pick a workspace in Connected and try again.");
  }
  const result = await apiPost("/api/account/scheduling", {
    scheduling: { mode: "internal", url: "" }
  });
  let account = result?.account || {};
  // Some response paths may not include publicUrl yet. Refresh and derive from token.
  try {
    const refreshed = await apiGet("/api/account");
    if (refreshed?.account) account = refreshed.account;
  } catch {}
  const publicToken = String(account?.scheduling?.publicToken || "").trim();
  const derivedPublicUrl = publicToken ? `${API_BASE}/book/${encodeURIComponent(publicToken)}` : "";
  const resolvedPublicUrl = String(account?.scheduling?.publicUrl || derivedPublicUrl || "").trim();
  if (resolvedPublicUrl && (!account?.scheduling || !String(account?.scheduling?.publicUrl || "").trim())) {
    account.scheduling = {
      ...(account?.scheduling && typeof account.scheduling === "object" ? account.scheduling : {}),
      publicUrl: resolvedPublicUrl
    };
  }
  state.onboardingAccountSettings = account;
  state.revenueAccountSettings = account;
  state.onboardingDraft = {
    ...(state.onboardingDraft || {}),
    bookingMode: "relay_scheduler",
    bookingUrl: "",
    bookingImportSource: "none"
  };
  return resolvedPublicUrl;
}

async function completeOnboardingFromDraft() {
  const draft = state.onboardingDraft || buildOnboardingDraft(state.onboardingAccountSettings, state.onboardingOptions);
  const active = resolveActiveWorkspace();
  const activeTo = String(active?.to || "").trim();
  const activeAccountId = String(active?.accountId || "").trim();
  const account = state.onboardingAccountSettings || {};
  const allowedAccounts = Array.isArray(authState?.accounts) ? authState.accounts : [];
  if (!activeTo && !activeAccountId) throw new Error("No workspace selected. Select a workspace and try again.");
  if (allowedAccounts.length && !allowedAccounts.some((a) => {
    const toMatch = activeTo && String(a?.to || "").trim() === activeTo;
    const idMatch = activeAccountId && String(a?.accountId || "").trim() === activeAccountId;
    return Boolean(toMatch || idMatch);
  })) {
    throw new Error("Selected workspace is not available for this account. Pick a workspace and retry.");
  }

  const selectedFlowId = String(draft?.defaultFlowId || account?.defaults?.defaultFlowId || "").trim();
  const bookingMode = String(draft?.bookingMode || "relay_scheduler").trim() || "relay_scheduler";
  const schedulingMode = bookingMode === "external_scheduler"
    ? "link"
    : bookingMode === "manual_follow_up"
      ? "manual"
      : "internal";
  const bookingUrl = schedulingMode === "link" ? String(draft?.bookingUrl || "").trim() : "";
  const incomingPricingByFlow = draft?.pricingByFlow && typeof draft.pricingByFlow === "object"
    ? cloneJson(draft.pricingByFlow) || {}
    : {};
  const selectedServiceKeys = onboardingGetPricingServiceList(selectedFlowId, state.onboardingFlows || [], incomingPricingByFlow).map((x) => x.key);
  const normalizedSelectedPricing = selectedFlowId
    ? onboardingNormalizePricingConfig(incomingPricingByFlow[selectedFlowId] || {}, selectedServiceKeys)
    : { services: {} };
  const logoUrl = String(draft.businessLogoUrl || "").trim();
  if (selectedFlowId) incomingPricingByFlow[selectedFlowId] = normalizedSelectedPricing;
  const servicePricing = cloneJson(normalizedSelectedPricing?.services || {}) || {};
  await apiPost("/api/account/scheduling", {
    scheduling: {
      mode: schedulingMode,
      url: bookingUrl
    }
  });
  await apiPatch("/api/account/workspace", {
    defaults: selectedFlowId ? { defaultFlowId: selectedFlowId } : undefined,
    workspace: {
      identity: {
        businessName: String(draft.businessName || "").trim(),
        businessEmail: String(draft.businessEmail || "").trim(),
        businessPhone: String(draft.businessPhone || "").trim(),
        industry: String(draft.businessType || "").trim(),
        logoUrl
      },
      timezone: String(draft.timezone || "America/New_York"),
      businessHours: cloneJson(draft.businessHours) || defaultOnboardingBusinessHours(),
      pricing: {
        ...(account?.workspace?.pricing && typeof account.workspace.pricing === "object" ? account.workspace.pricing : {}),
        services: cloneJson(servicePricing)
      },
      settings: {
        ...(account?.workspace?.settings && typeof account.workspace.settings === "object" ? account.workspace.settings : {}),
        bookingImportSource: String(draft.bookingImportSource || "none").trim() || "none"
      },
      pricingByFlow: incomingPricingByFlow
    }
  });
  const payload = {
    businessType: draft.businessType,
    outcomePacks: Array.isArray(draft.outcomePacks) ? draft.outcomePacks : [],
    avgTicketValue: Math.max(0, Number(draft.avgTicketValueDollars || 0)),
    avgTicketValueCents: Math.round(Math.max(0, Number(draft.avgTicketValueDollars || 0)) * 100),
    bookingUrl: bookingUrl || undefined,
    phoneConnected: true,
    calendarConnected: draft.calendarConnected === true,
    goLive: true
  };
  const result = await apiPost("/api/onboarding/setup", payload);
  state.onboardingAccountSettings = result?.account || state.onboardingAccountSettings;
  state.revenueAccountSettings = result?.account || state.revenueAccountSettings;
  state.onboardingRequired = false;
  state.onboardingError = null;
  state.onboardingTestStatus = "";
}

/* ==========
  Helpers
========== */

function openAddContactModal(onSave){
  const existing = document.getElementById("addContactModal");
  if (existing) existing.remove();

  const modal = document.createElement("div");
  modal.id = "addContactModal";
  modal.className = "overlay";
  modal.style.background = "rgba(0,0,0,0.55)";

  modal.innerHTML = `
    <div class="card" style="max-width:560px; width:100%; box-shadow: var(--shadow);">
      <div class="row" style="justify-content:space-between; align-items:center;">
        <div class="col" style="gap:2px;">
          <div class="h1" style="margin:0;">Add contact</div>
          <div class="p">Create a contact manually (phone required).</div>
        </div>
        <button class="btn" type="button" id="closeAddContact">Close</button>
      </div>

      <div style="height:14px;"></div>

      <div class="grid2">
        <div class="col">
          <label class="p">Phone (E.164 preferred)</label>
          <input class="input" id="acPhone" placeholder="+18145559999" />
        </div>
        <div class="col">
          <label class="p">Name (optional)</label>
          <input class="input" id="acName" placeholder="John Smith" />
        </div>
      </div>

      <div style="height:10px;"></div>

      <div class="row" style="gap:14px; flex-wrap:wrap;">
        <label class="toggle">
          <input type="checkbox" id="acVip" />
          VIP
        </label>
        <label class="toggle">
          <input type="checkbox" id="acDnr" />
          do not auto-reply
        </label>
      </div>

      <div style="height:10px;"></div>

      <div class="col">
        <label class="p">Notes (optional)</label>
        <textarea class="input" id="acNotes" rows="4" style="resize:vertical;" placeholder="Anything important..."></textarea>
      </div>

      <div style="height:14px;"></div>

      <div class="row" style="justify-content:flex-end; gap:10px;">
        <button class="btn" type="button" id="cancelAddContact">Cancel</button>
        <button class="btn primary" type="button" id="saveAddContact">Save contact</button>
      </div>

      <div style="height:10px;"></div>
      <div class="p" id="acError" style="color:#ffb4b4; min-height:18px;"></div>
    </div>
  `;

  document.body.appendChild(modal);

  function close(){
    modal.remove();
  }

  modal.addEventListener("click", (e) => {
    // click backdrop closes
    if (e.target === modal) close();
  });

  document.getElementById("closeAddContact")?.addEventListener("click", close);
  document.getElementById("cancelAddContact")?.addEventListener("click", close);

  document.getElementById("saveAddContact")?.addEventListener("click", () => {
    const err = document.getElementById("acError");
    if (err) err.textContent = "";

    const to = getActiveTo();
    const phoneRaw = (document.getElementById("acPhone")?.value || "").trim();
    const name = (document.getElementById("acName")?.value || "").trim();
    const vip = document.getElementById("acVip")?.checked === true;
    const dnr = document.getElementById("acDnr")?.checked === true;
    const notes = (document.getElementById("acNotes")?.value || "").trim();

    const phone = normalizePhone(phoneRaw);

    // basic validation
    if (!phone || phone.length < 8) {
      if (err) err.textContent = "Enter a valid phone number.";
      return;
    }

    // load contacts, upsert, save
    const contacts = loadContacts(to) || [];

    const patch = {
      accountTo: to,
      phone: phoneRaw,
      leadStatus: "new",
      firstSeenAt: Date.now(),
      lastSeenAt: Date.now(),
    };

    const c = upsertContact(contacts, patch);

    // apply manual fields
    c.name = name || c.name || "";
    c.flags.vip = vip;
    c.flags.doNotAutoReply = dnr;
    c.summary.notes = notes || c.summary.notes || "";

    saveContacts(to, contacts);

    close();
    if (typeof onSave === "function") onSave(c);
  });
}


function scrollChatToBottom() {
  const bubbles = document.getElementById("bubbles");
  if (!bubbles) return;
  bubbles.scrollTop = bubbles.scrollHeight;
}

async function getVipSetForActiveTo(){
  try{
    const to = getActiveTo();
    const contacts = await apiGetContacts(to); // <-- returns array
    const vip = new Set();
    (Array.isArray(contacts) ? contacts : []).forEach(c => {
      if (c?.flags?.vip) vip.add(normalizePhone(c.phone));
    });
    return vip;
  }catch(err){
    console.warn("VIP lookup failed, continuing without VIP badges:", err?.message || err);
    return new Set();
  }
}




function isVipThread(vipSet, fromPhone){
  return vipSet.has(normalizePhone(fromPhone));
}



function normalizeE164(phone){
  return normalizePhone(phone); // you already have normalizePhone() at bottom
}

function upsertContact(list, patch){
  const phone = normalizeE164(patch.phone);
  let c = list.find(x => normalizeE164(x.phone) === phone);

  if (!c) {
    c = {
      id: crypto.randomUUID(),
      accountTo: patch.accountTo,
      phone,
      name: "",
      tags: [],
      flags: { vip: false, doNotAutoReply: false, blocked: false },
      lifecycle: {
        leadStatus: patch.leadStatus || "new",
        firstSeenAt: patch.firstSeenAt || Date.now(),
        lastSeenAt: patch.lastSeenAt || Date.now(),
        lastConversationId: patch.lastConversationId || "",
      },
      summary: { intent: "", vehicle: { year: "", make: "", model: "" }, request: "", notes: "" },
      metrics: { totalConversations: 0, totalInbound: 0, totalOutbound: 0, missedCalls: 0, recovered: 0 },
      audit: { createdAt: Date.now(), updatedAt: Date.now() }
    };
    list.push(c);
  }

  // merge patch safely
  c.accountTo = patch.accountTo || c.accountTo;
  c.lifecycle.leadStatus = patch.leadStatus || c.lifecycle.leadStatus;
  c.lifecycle.lastSeenAt = Math.max(c.lifecycle.lastSeenAt || 0, patch.lastSeenAt || 0);
  c.lifecycle.firstSeenAt = c.lifecycle.firstSeenAt || patch.firstSeenAt || Date.now();
  c.lifecycle.lastConversationId = patch.lastConversationId || c.lifecycle.lastConversationId;

  if (patch.intent) c.summary.intent = patch.intent;
  if (patch.request) c.summary.request = patch.request;

  if (patch.vehicle) {
    c.summary.vehicle = {
      year: patch.vehicle.year || c.summary.vehicle.year,
      make: patch.vehicle.make || c.summary.vehicle.make,
      model: patch.vehicle.model || c.summary.vehicle.model,
    };
  }

  c.audit.updatedAt = Date.now();
  return c;
}

function syncContactsFromThreads(){
  const to = getActiveTo();
  const saved = loadContacts(to); // preserves vip/notes/name

  const threads = state.threads || [];
  for (const t of threads) {
    const ld = t.leadData || {};
    const vehicle = {
      year: ld.vehicle_year || "",
      make: ld.vehicle_make || "",
      model: ld.vehicle_model || "",
    };

    upsertContact(saved, {
      accountTo: to,
      phone: t.from,
      leadStatus: t.status || "new",
      firstSeenAt: t.createdAt || t.updatedAt || t.lastActivityAt || Date.now(),
      lastSeenAt: t.lastActivityAt || t.updatedAt || Date.now(),
      lastConversationId: t.id,
      intent: ld.intent || "",
      request: ld.request || ld.issue || "",
      vehicle,
    });
  }

  // sort newest activity first
  saved.sort((a, b) => (b.lifecycle.lastSeenAt || 0) - (a.lifecycle.lastSeenAt || 0));

  saveContacts(to, saved);
  return saved;
}

function formatPhoneNice(p){
  // simple formatting; keep your normalizePhone elsewhere
  return p || "Unknown";
}



function contactsKey(to){
  return `mc_contacts_v1:${to}`;
}

async function loadContactsFromBackend(options = {}){
  const to = getActiveTo();
  const force = options && options.force === true;
  const cacheEntry = state.contactsCacheByTo?.[to] || null;
  if (!force && cacheEntry && (Date.now() - Number(cacheEntry.loadedAt || 0)) < CONTACTS_CACHE_TTL_MS) {
    return cloneJsonSafe(cacheEntry.data || []);
  }
  if (!force && state.contactsPromiseByTo?.[to]) {
    return state.contactsPromiseByTo[to];
  }
  const loadPromise = apiGetContacts(to)
    .then((contacts) => {
      state.contactsCacheByTo[to] = { loadedAt: Date.now(), data: cloneJsonSafe(contacts || []) };
      return cloneJsonSafe(contacts || []);
    })
    .finally(() => {
      delete state.contactsPromiseByTo[to];
    });
  state.contactsPromiseByTo[to] = loadPromise;
  return loadPromise;
}


async function saveContactToBackend(contact){
  const to = getActiveTo();
  const saved = await apiUpsertContact(to, contact);
  const existing = Array.isArray(state.contactsCacheByTo?.[to]?.data) ? state.contactsCacheByTo[to].data.slice() : [];
  const next = existing.filter((item) => String(item?.phone || "") !== String(saved?.phone || ""));
  next.unshift(saved);
  state.contactsCacheByTo[to] = { loadedAt: Date.now(), data: cloneJsonSafe(next) };
  return saved;
}


function getRecentActivity(limit = 6){
  const activeTo = getActiveTo();
  const cacheKey = `${activeTo}::${Number(limit || 0)}::${Number(state.threadsLastLoadedAt || 0)}`;
  const cached = state.recentActivityCache?.[cacheKey];
  if (Array.isArray(cached)) return cached;

  const threads = (state.threads || []).filter(t => {
    return !t.to || String(t.to) === String(activeTo);
  });

  const sorted = threads.slice().sort((a, b) => {
    const ta = Number(a.lastActivityAt || a.updatedAt || a.createdAt || 0);
    const tb = Number(b.lastActivityAt || b.updatedAt || b.createdAt || 0);
    return tb - ta;
  });

  const items = [];

  for (const t of sorted) {
    const when = Number(t.lastActivityAt || t.updatedAt || t.createdAt || 0);
    const from = t.from || "Unknown";
    const status = String(t.status || "new").toLowerCase();

    if (status === "new") {
      items.push({ id:`new:${t.id}`, convoId:t.id, when, title:`New lead from ${from}`, detail:"Awaiting follow-up" });
    } else if (status === "contacted") {
      items.push({ id:`contacted:${t.id}`, convoId:t.id, when, title:`Follow-up started: ${from}`, detail:"Marked as contacted" });
    } else if (status === "booked") {
      items.push({ id:`booked:${t.id}`, convoId:t.id, when, title:`Lead booked: ${from}`, detail:"Marked as booked" });
    } else if (status === "closed") {
      items.push({ id:`closed:${t.id}`, convoId:t.id, when, title:`Lead closed: ${from}`, detail:"Marked as closed" });
    }

    const lastText = String(t.lastText || "").trim();
    if (lastText) {
      items.push({
        id: `msg:${t.id}:${when}`,
        convoId: t.id,
        when,
        title: `Latest message: ${from}`,
        detail: lastText.length > 70 ? lastText.slice(0, 70) : lastText,
      });
    }

    if (items.length >= limit * 2) break;
  }

  items.sort((a, b) => b.when - a.when);

  const seen = new Set();
  const out = [];
  for (const it of items) {
    if (seen.has(it.id)) continue;
    seen.add(it.id);
    out.push(it);
    if (out.length >= limit) break;
  }

  state.recentActivityCache = { [cacheKey]: out };
  return out;
}

function getNotificationScopeKey(){
  const accounts = Array.isArray(authState?.accounts) ? authState.accounts : [];
  const activeTo = String(getActiveTo() || "");
  const activeAccount = accounts.find((acct) => String(acct?.to || "") === activeTo);
  return String(activeAccount?.accountId || activeTo || "default");
}

function getNotificationReadMap(){
  try {
    const raw = localStorage.getItem(NOTIF_READ_TS_KEY);
    const parsed = raw ? JSON.parse(raw) : {};
    return (parsed && typeof parsed === "object") ? parsed : {};
  } catch {
    return {};
  }
}

function getNotificationReadTs(scopeKey){
  const map = getNotificationReadMap();
  return Number(map?.[scopeKey] || 0);
}

function setNotificationReadTs(scopeKey, ts){
  const map = getNotificationReadMap();
  map[scopeKey] = Number(ts || Date.now());
  localStorage.setItem(NOTIF_READ_TS_KEY, JSON.stringify(map));
}

function getNotificationItems(limit = 24){
  const activeTo = getActiveTo();
  const now = Date.now();
  const threads = (state.threads || [])
    .filter((t) => !t.to || String(t.to) === String(activeTo))
    .slice()
    .sort((a, b) => Number(b.lastActivityAt || b.updatedAt || b.createdAt || 0) - Number(a.lastActivityAt || a.updatedAt || a.createdAt || 0));

  const items = [];
  for (const t of threads) {
    const when = Number(t.lastActivityAt || t.updatedAt || t.createdAt || 0) || now;
    const status = String(t.status || "new").toLowerCase();
    const from = String(t.from || t.name || t.phone || "Unknown");
    const convoId = String(t.id || "");

    if (status === "new") {
      items.push({ id: `lead-new:${convoId}`, when, severity: "high", title: `New lead: ${from}`, detail: "Needs first response", action: "open-convo", convoId });
    }
    if (status === "booked") {
      items.push({ id: `lead-booked:${convoId}`, when, severity: "normal", title: `Booking confirmed: ${from}`, detail: "Lead moved to booked", action: "open-convo", convoId });
    }
    if (status === "contacted" && (now - when) > (24 * 60 * 60 * 1000)) {
      items.push({ id: `followup:${convoId}`, when, severity: "normal", title: `Follow-up due: ${from}`, detail: "No activity in 24h", action: "open-convo", convoId });
    }

    const lastText = String(t.lastText || "").trim();
    if (lastText) {
      items.push({
        id: `msg:${convoId}:${when}`,
        when,
        severity: "low",
        title: `Latest message: ${from}`,
        detail: lastText.length > 84 ? `${lastText.slice(0, 84)}...` : lastText,
        action: "open-convo",
        convoId
      });
    }

    if (items.length >= (limit * 2)) break;
  }

  if (isBillDueStatus(navBillDueState.status)) {
    items.push({
      id: "billing-due",
      when: now,
      severity: "critical",
      title: "Billing requires attention",
      detail: "Payment is due. Open billing to prevent interruption.",
      action: "open-billing"
    });
  }

  if (state.analyticsError) {
    items.push({
      id: "analytics-alert",
      when: now - 1000,
      severity: "normal",
      title: "Analytics sync warning",
      detail: "Latest analytics refresh failed. Retry from Analytics.",
      action: "open-analytics"
    });
  }

  const seen = new Set();
  const deduped = [];
  for (const item of items.sort((a, b) => b.when - a.when)) {
    if (seen.has(item.id)) continue;
    seen.add(item.id);
    deduped.push(item);
    if (deduped.length >= limit) break;
  }
  return deduped;
}



function getAutomationHealth(){
  const activeTo = getActiveTo();

  // Rules (if you later add per-number keys, this still works)
  const rules = state.rules || [];
  const enabled = rules.filter(r => r.enabled);

  const missedCallOn = enabled.some(r => r.trigger === "missed_call");
  const inboundSmsOn  = enabled.some(r => r.trigger === "inbound_sms");

  // Scheduling configured (MVP: booking link in cache)
  const bookingUrl = (getCachedBookingUrl(activeTo) || "").trim();
  const schedulingSet = bookingUrl.startsWith("http://") || bookingUrl.startsWith("https://");

  // Last inbound lead activity (per active number)
  const threads = (state.threads || []).filter(t => {
    return String(t.to || "") === String(activeTo || "");
  });

  // If your backend returns normalized +to, this should match.
  // If it doesn't, remove the filter above and just use all threads for now.

  let lastInboundAt = 0;
  for (const t of threads) {
    const ts = Number(t.lastActivityAt || t.updatedAt || t.createdAt || 0);
    if (ts > lastInboundAt) lastInboundAt = ts;
  }

  return {
    activeTo,
    enabledRulesCount: enabled.length,
    missedCallOn,
    inboundSmsOn,
    schedulingSet,
    bookingUrl,
    lastInboundAt,
  };
}


function getLeadPreviewLines(convo){
  const ld = convo.leadData || {};

  // Line 1: intent / service
  const line1 =
    ld.intent
      ? ld.intent.charAt(0).toUpperCase() + ld.intent.slice(1)
      : "New lead";

  // Line 2: vehicle
  const vehicle = [ld.vehicle_year, ld.vehicle_make, ld.vehicle_model]
    .filter(Boolean)
    .join(" ");
  const line2 = vehicle || "Vehicle not provided";

  // Line 3: notes or last inbound message
  let line3 = "";

  if (ld.notes) {
    line3 = ld.notes;
  } else {
    const inbound = getLastInboundText(convo);
    line3 = inbound || "Awaiting customer reply";
  }

  return [line1, line2, line3];
}

function humanizeServiceLabel(value) {
  const raw = String(value || "").trim();
  if (!raw) return "";
  if (raw.includes(" ")) return raw;
  return raw
    .replace(/_/g, " ")
    .split(" ")
    .filter(Boolean)
    .map((w) => w.charAt(0).toUpperCase() + w.slice(1))
    .join(" ");
}

function getServiceLabelsFromLeadData(ld) {
  const leadData = ld || {};
  const fromList = Array.isArray(leadData.services_list)
    ? leadData.services_list.map(humanizeServiceLabel).filter(Boolean)
    : [];
  if (fromList.length) return Array.from(new Set(fromList)).slice(0, 6);
  const fallback = [
    leadData.intent,
    leadData.service_required,
    leadData.request,
    leadData.issue,
    leadData.service
  ]
    .map(humanizeServiceLabel)
    .filter(Boolean);
  return Array.from(new Set(fallback)).slice(0, 4);
}

function inferServiceLabelsFromMessages(messages = []) {
  const labels = [];
  const safe = Array.isArray(messages) ? messages : [];
  for (const m of safe) {
    const text = String(m?.text || m?.body || "").trim();
    if (!text) continue;
    const pricePattern = text.match(/\bour\s+(.+?)\s+package\s+starts\s+at\s+\$/i);
    if (pricePattern?.[1]) labels.push(humanizeServiceLabel(pricePattern[1]));
    const summaryPattern = text.match(/\bservice summary\s*\(([^)]+)\)/i);
    if (summaryPattern?.[1]) {
      const parts = String(summaryPattern[1]).split(/\+|,| and /i).map((x) => humanizeServiceLabel(x)).filter(Boolean);
      labels.push(...parts);
    }
  }
  return Array.from(new Set(labels.filter(Boolean))).slice(0, 6);
}

function resolveLeadVehicleText(ld = {}, convo = {}) {
  const basic = [ld.vehicle_year, ld.vehicle_make, ld.vehicle_model].filter(Boolean).join(" ").trim();
  if (basic) return basic;
  const fallback = [
    ld.vehicle,
    ld.vehicle_model,
    ld.vehicleName,
    ld.car,
    ld.auto
  ].map((x) => String(x || "").trim()).find(Boolean);
  if (fallback) return fallback;
  const msgs = Array.isArray(convo?.messages) ? convo.messages : [];
  for (let i = msgs.length - 1; i >= 0; i--) {
    const text = String(msgs[i]?.text || msgs[i]?.body || "");
    const m = text.match(/\b(19|20)\d{2}\s+[A-Za-z0-9\-]+\s+[A-Za-z0-9\- ]{2,40}\b/);
    if (m?.[0]) return m[0].trim();
  }
  return "";
}

function looksLikeYearMakeModel(raw) {
  const text = String(raw || "").trim().replace(/\s+/g, " ");
  if (!text) return false;
  // Require a leading year and at least make + model words after it.
  return /^(19|20)\d{2}\s+[A-Za-z0-9][A-Za-z0-9-]*\s+[A-Za-z0-9][A-Za-z0-9\- ]{1,60}$/i.test(text);
}

function normalizeBookedVehicleValue(...candidates) {
  for (const value of candidates) {
    const text = String(value || "").trim().replace(/\s+/g, " ");
    if (!text) continue;
    if (looksLikeYearMakeModel(text)) return text;
  }
  return "";
}

function resolveLeadAvailabilityText(ld = {}, convo = {}) {
  const status = String(convo?.status || "").toLowerCase();
  const stage = String(convo?.stage || "").toLowerCase();
  const bookedMs = getLatestBookedConfirmationTime(convo);
  const isBooked = status === "booked" || /booked|appointment_booked|scheduled/.test(stage) || (Number.isFinite(bookedMs) && bookedMs > 0);
  if (isBooked && Number.isFinite(bookedMs) && bookedMs > 0) {
    return formatBookedMarkerLabel(bookedMs);
  }

  const direct = String(ld.availability || "").trim();
  if (direct) return direct;
  const bookingFallbackMs = Number(
    convo?.bookingTime
    || ld?.booking_time
    || ld?.bookingTime
    || 0
  );
  if (Number.isFinite(bookingFallbackMs) && bookingFallbackMs > 0) {
    return formatBookedMarkerLabel(bookingFallbackMs);
  }
  return "";
}

function renderServiceChipsHtml(labels = []) {
  const items = Array.isArray(labels) ? labels.filter(Boolean) : [];
  if (!items.length) return "";
  return `<div style="display:flex;gap:6px;flex-wrap:wrap;margin-top:6px;">${
    items.map((x) => `<span class="badge">${escapeHtml(String(x))}</span>`).join("")
  }</div>`;
}

function humanizeGuardrailReason(reason) {
  const raw = String(reason || "").trim();
  if (!raw) return "";
  if (raw.startsWith("missing_required_fields:")) return "Missing required intake details";
  if (raw.startsWith("unpriced_services:")) return "One or more services need pricing config";
  if (raw.startsWith("missing_configured_price:")) return "Configured pricing missing for one or more services";
  if (raw === "photo_or_range_required") return "Need photos or range-only confirmation";
  if (raw === "range_only_quote") return "Range estimate only (final after inspection)";
  if (raw === "photos_pending") return "Waiting on customer photos";
  if (raw === "no_service_detected") return "Service not clearly detected yet";
  if (raw === "low_context_for_scope") return "Not enough scope context yet";
  return raw.replace(/_/g, " ");
}


function getLastInboundText(convo){
  if (!convo?.messages?.length) return "";

  // walk from newest  oldest
  for (let i = convo.messages.length - 1; i >= 0; i--) {
    const m = convo.messages[i];
    if (m.dir === "in" && m.text) {
      return m.text;
    }
  }
  return "";
}


function getRecentLeads(limit = 5){
  const activeTo = getActiveTo();
  const cacheKey = `${activeTo}::${Number(limit || 0)}::${Number(state.threadsLastLoadedAt || 0)}`;
  const cached = state.recentLeadsCache?.[cacheKey];
  if (Array.isArray(cached)) return cached;
  const leads = (state.threads || [])
    .slice()
    .sort((a, b) => {
      const ta = a.lastActivityAt || a.updatedAt || a.createdAt || 0;
      const tb = b.lastActivityAt || b.updatedAt || b.createdAt || 0;
      return tb - ta;
    })
    .slice(0, limit);
  state.recentLeadsCache = { [cacheKey]: leads };
  return leads;
}


// Shared view helpers
function headerCard(title, desc){
  const wrap = document.createElement("div");
  wrap.className = "card";
  wrap.innerHTML = `
    <p class="h1">${title}</p>
    <p class="p">${desc}</p>
  `;
  return wrap;
}


function formatTimeAgo(ms){
  if (!ms) return "?";
  const s = Math.floor((Date.now() - ms) / 1000);
  if (s < 60) return `${s}s ago`;
  const m = Math.floor(s / 60);
  if (m < 60) return `${m}m ago`;
  const h = Math.floor(m / 60);
  if (h < 24) return `${h}h ago`;
  const d = Math.floor(h / 24);
  return `${d}d ago`;
}

function formatThreadTimestamp(ms) {
  const value = Number(ms || 0);
  if (!Number.isFinite(value) || value <= 0) return "";
  const now = Date.now();
  const delta = Math.abs(now - value);
  if (delta < (7 * 24 * 60 * 60 * 1000)) return formatTimeAgo(value);
  return new Date(value).toLocaleDateString("en-US", { month: "short", day: "numeric" });
}

function prettyLifecycleLabel(value) {
  const raw = String(value || "").trim();
  if (!raw) return "";
  return raw
    .replace(/[_-]+/g, " ")
    .replace(/\s+/g, " ")
    .replace(/\b\w/g, (m) => m.toUpperCase());
}

function getMessageStatusTone(status) {
  const normalized = String(status || "").trim().toLowerCase();
  if (!normalized) return "";
  if (normalized === "delivered") return "is-delivered";
  if (normalized === "failed" || normalized === "undelivered") return "is-failed";
  if (normalized === "sending") return "is-sending";
  if (normalized === "sent") return "is-sent";
  if (normalized === "simulated") return "is-simulated";
  return "is-neutral";
}

const $ = (sel) => document.querySelector(sel);
const $$ = (sel) => Array.from(document.querySelectorAll(sel));
const debounce = (typeof RelayUI?.debounce === "function")
  ? RelayUI.debounce.bind(RelayUI)
  : (fn, wait = 0) => {
      let timeoutId = 0;
      return (...args) => {
        if (timeoutId) clearTimeout(timeoutId);
        timeoutId = setTimeout(() => {
          timeoutId = 0;
          fn(...args);
        }, Math.max(0, Number(wait || 0)));
      };
    };
const uiRequestScope = {
  accountVersion: 0,
};

function bumpAccountScopeVersion() {
  uiRequestScope.accountVersion += 1;
  return uiRequestScope.accountVersion;
}

function createUiScopeSnapshot({ view = state?.view, to = getActiveTo(), panel = "", renderId = _renderId } = {}) {
  return {
    accountVersion: uiRequestScope.accountVersion,
    view: String(view || "").trim(),
    to: String(to || "").trim(),
    panel: String(panel || "").trim(),
    renderId: Number(renderId || 0),
  };
}

function isUiScopeCurrent(snapshot, options = {}) {
  if (!snapshot) return true;
  if (Number(snapshot.accountVersion || 0) !== Number(uiRequestScope.accountVersion || 0)) return false;
  if (snapshot.renderId && snapshot.renderId !== _renderId) return false;
  if (snapshot.view && String(state?.view || "").trim() !== snapshot.view) return false;
  if (snapshot.to && String(getActiveTo() || "").trim() !== snapshot.to) return false;
  if (snapshot.panel) {
    const currentPanel = String(localStorage.getItem(SETTINGS_TAB_ROUTE_KEY) || localStorage.getItem("mc_settings_tab_v1") || "").trim();
    if (currentPanel !== snapshot.panel) return false;
  }
  if (options.element && !options.element.isConnected) return false;
  if (options.activeThreadId != null && String(state.activeThreadId || "").trim() !== String(options.activeThreadId || "").trim()) return false;
  if (options.activeContactPhone != null && String(options.activeContactPhone || "").trim()) {
    const currentPhone = String(options.getActiveContactPhone ? options.getActiveContactPhone() : "").trim();
    if (currentPhone !== String(options.activeContactPhone || "").trim()) return false;
  }
  return true;
}

async function runGuardedButtonAction(button, task, options = {}) {
  if (!button || typeof task !== "function") return false;
  if (button.dataset.pending === "1") return false;
  const originalText = button.textContent;
  const pendingText = String(options.pendingText || "").trim();
  button.dataset.pending = "1";
  button.disabled = true;
  if (pendingText) button.textContent = pendingText;
  try {
    await task();
    return true;
  } finally {
    button.disabled = false;
    button.dataset.pending = "0";
    if (pendingText) button.textContent = originalText;
  }
}

function closeTransientOverlays() {
  document.querySelectorAll(".overlay:not(.hidden), .billing-modal-overlay:not(.hidden)").forEach((el) => {
    el.classList.add("hidden");
    if (el.hasAttribute("aria-hidden")) el.setAttribute("aria-hidden", "true");
  });
  document.getElementById("threadContextMenu")?.classList.add("hidden");
  document.getElementById("msgContextMenu")?.classList.add("hidden");
}

function resetWorkspaceScopedTransientState() {
  state.activeThreadId = null;
  state.activeConversation = null;
  state.activeCompliance = null;
  state.threads = [];
  state.threadsLastLoadedAt = 0;
  state.scheduleAccount = null;
  state.revenueSelectedOpportunityId = null;
  state.chatExpandedByConversation = {};
  state.simulationConversationId = null;
  state.simulationBlockConversationLoad = false;
  state.onboardingDraft = null;
  state.onboardingDraftKey = "";
  state.onboardingStep = 1;
  state.onboardingTestStatus = "";
  state.onboardingBookingPreviewKey = "";
  state.onboardingBookingPreview = null;
  state.onboardingBookingPreviewLoading = false;
  state.onboardingBookingPreviewError = null;
}

function hasFreshThreadState(ttlMs = THREADS_CACHE_TTL_MS) {
  const loadedAt = Number(state.threadsLastLoadedAt || 0);
  return Array.isArray(state.threads)
    && state.threads.length > 0
    && Number.isFinite(loadedAt)
    && loadedAt > 0
    && (Date.now() - loadedAt) < ttlMs;
}

function getConversationCacheKey(convoId, to = getActiveTo()) {
  const id = String(convoId || "").trim();
  const scopeTo = String(to || "").trim();
  return `${scopeTo}::${id}`;
}

function hasFreshConversationState(convoId, ttlMs = CONVERSATION_CACHE_TTL_MS) {
  const id = String(convoId || "").trim();
  if (!id) return false;
  const cacheKey = getConversationCacheKey(id);
  const cacheEntry = state.conversationCacheById?.[cacheKey] || null;
  const loadedAt = Number(cacheEntry?.loadedAt || 0);
  return Number.isFinite(loadedAt)
    && loadedAt > 0
    && (Date.now() - loadedAt) < ttlMs;
}

function positionNavGhostHighlight({ instant = false } = {}) {
  const sidebar = document.querySelector(".sidebar");
  const ghost = document.getElementById("navGhostHighlight");
  if (!sidebar || !ghost || sidebar.offsetParent === null) return;

  const activeBtn = sidebar.querySelector(".nav-btn.active");
  if (!activeBtn) {
    ghost.style.opacity = "0";
    return;
  }

  // Anchor ghost to the nav container, then translate within that container.
  const navContainer = sidebar.querySelector(".nav-track") || sidebar;
  const offsetParent =
    ghost.offsetParent instanceof HTMLElement ? ghost.offsetParent : sidebar;
  const offsetParentTop = offsetParent.getBoundingClientRect().top;
  const containerTop = navContainer.getBoundingClientRect().top;
  const btnTop = activeBtn.getBoundingClientRect().top;
  const y = btnTop - containerTop;
  ghost.style.top = `${containerTop - offsetParentTop}px`;
  ghost.style.height = `${activeBtn.offsetHeight}px`;

  const first = ghost.dataset.ready !== "true";
  const shouldInstant = instant || first;
  if (shouldInstant) {
    const prev = ghost.style.transition;
    ghost.style.transition = "none";
    ghost.style.transform = `translateY(${y}px)`;
    ghost.style.opacity = "1";
    // Force style flush so the next transition animates cleanly.
    void ghost.offsetHeight;
    ghost.style.transition = prev || "";
    ghost.dataset.ready = "true";
    return;
  }

  ghost.style.transform = `translateY(${y}px)`;
  ghost.style.opacity = "1";
}

function setActiveNav(view){
  $$(".nav-btn[data-view], .arc-dock-btn[data-view]").forEach((b) => {
    b.classList.toggle("active", b.dataset.view === view);
  });
  positionNavGhostHighlight();
}

let _renderId = 0;  // guard against concurrent async renders

async function render(){
  const thisRender = ++_renderId;
  if (!authState.user) {
    await ensureDeferredStyles();
    showLoginOverlay();
    return;
  }
  if (viewNeedsDeferredStyles(state.view)) {
    await ensureDeferredStyles();
  }
  if (state.view === "automations") {
    localStorage.setItem("mc_settings_tab_v1", "automations");
    state.view = "settings";
  }
  if (state.view === "revenue") {
    state.view = "analytics";
  }
  if (!NAV_VIEWS.has(state.view)) {
    state.view = "home";
  }
  if (state.onboardingRequired && state.view !== "onboarding") {
    state.view = "onboarding";
  }
  persistView(state.view);
  const skipRouteSyncOnce = window.__suppressRouteSyncOnce === true;
  if (skipRouteSyncOnce) {
    window.__suppressRouteSyncOnce = false;
  } else {
    syncUrlWithState({ replace: window.__routeSyncedOnce !== true });
    window.__routeSyncedOnce = true;
  }
  if (!shouldSkipDashboardBootForView() && !hasFreshThreadState()) {
    try {
      await loadThreads();
    } catch {}
  }
  if (thisRender !== _renderId) return; // stale render, abort

  setActiveNav(state.view);
  document.body.classList.toggle("messages-mode", state.view === "messages");
  document.body.classList.toggle("schedule-mode", state.view === "schedule" || state.view === "schedule-booking");
  const content = $("#content");
  content.innerHTML = "";

  if(state.view === "messages"){
    content.innerHTML = skeletonInbox();

    try{
      if (!hasFreshThreadState()) {
        await loadThreads({ skipTopbarRefresh: true });
      }
      if (thisRender !== _renderId) return;
      const shouldSkipConversationLoad = state.simulationBlockConversationLoad && state.simulationConversationId === state.activeThreadId;
      if (!shouldSkipConversationLoad && !hasFreshConversationState(state.activeThreadId)) {
        await loadConversation(state.activeThreadId);
        if (thisRender !== _renderId) return;
      }
      content.innerHTML = "";
      content.appendChild(viewMessages());
    }catch(e){
      if (thisRender !== _renderId) return;
      content.innerHTML = RelayUI.renderNoticeCard({
        title: "Messages",
        text: `Messages could not load. Make sure the backend is running on ${API_BASE}, then refresh this page.`,
        detail: e.message,
        className: "shell-notice-error"
      });
    }
    return;
  }

  if(state.view === "home") {
    const scopeKey = getAnalyticsScopeKey();
    const needsOverview = !state.homeOverviewCache?.[scopeKey];
    const needsFunnel = !state.homeFunnelCache?.[scopeKey];
    const needsWins = !state.homeWinsCache?.[scopeKey];
    if ((needsOverview || needsFunnel || needsWins) && !state.homeOverviewLoading && !state.homeFunnelLoading && !state.homeWinsLoading) {
      state.homeOverviewLoading = true;
      state.homeOverviewError = null;
      state.homeFunnelLoading = true;
      state.homeFunnelError = null;
      state.homeWinsLoading = true;
      state.homeWinsError = null;
      Promise.all([
        needsOverview ? apiGet("/api/analytics/revenue-overview") : Promise.resolve(state.homeOverviewCache?.[scopeKey] || {}),
        needsFunnel ? apiGet("/api/analytics/funnel") : Promise.resolve(state.homeFunnelCache?.[scopeKey] || {}),
        needsWins ? apiGetOptional("/api/analytics/todays-wins", {}) : Promise.resolve(state.homeWinsCache?.[scopeKey] || {})
      ])
        .then(([overview, funnel, wins]) => {
          state.homeOverviewCache[scopeKey] = overview || {};
          state.homeFunnelCache[scopeKey] = funnel || {};
          state.homeWinsCache[scopeKey] = wins || {};
        })
        .catch((err) => {
          const message = err?.message || "Unable to load revenue overview";
          state.homeOverviewError = message;
          state.homeFunnelError = message;
          state.homeWinsError = message;
          if (needsOverview) state.homeOverviewCache[scopeKey] = {};
          if (needsFunnel) state.homeFunnelCache[scopeKey] = {};
          if (needsWins) state.homeWinsCache[scopeKey] = {};
        })
        .finally(() => {
          state.homeOverviewLoading = false;
          state.homeFunnelLoading = false;
          state.homeWinsLoading = false;
          if (state.view === "home") render();
        });
    }
    try {
      if (!hasFreshThreadState()) {
        await loadThreads({ skipTopbarRefresh: true });
      }
    } catch {}
    if (thisRender !== _renderId) return;
    content.appendChild(viewHome());
  } else if(state.view === "contacts") {
    content.appendChild(viewContacts());
  } else if(state.view === "vip") {
    content.appendChild(viewVIP());
  } else if(state.view === "analytics") {
    content.appendChild(viewAnalyticsModern());
  } else if(state.view === "schedule") {
    try {
      if (!hasFreshThreadState()) {
        await loadThreads({ skipTopbarRefresh: true });
      }
    } catch {}
    try {
      state.scheduleAccount = await loadAccountSettings(getActiveTo());
    } catch {
      state.scheduleAccount = null;
    }
    if (thisRender !== _renderId) return;
    content.appendChild(viewSchedule());
  } else if (state.view === "schedule-booking") {
    content.appendChild(viewScheduleBooking());
  } else if(state.view === "settings") {
    content.appendChild(viewSettings());
  } else if (state.view === "onboarding") {
    content.appendChild(viewOnboarding());
  }
}



/* ==========
  Views
========== */
function viewOnboarding() {
  const wrap = document.createElement("div");
  wrap.className = "analytics-page revenue-page app-view app-view-onboarding";
  const currentStep = Math.max(1, Math.min(5, Number(state.onboardingStep || 1)));
  const account = state.onboardingAccountSettings || null;
  const draft = state.onboardingDraft || buildOnboardingDraft(account, state.onboardingOptions);
  const saveError = String(state.onboardingError || "").trim();
  const testStatus = String(state.onboardingTestStatus || "").trim();
  const businessPhoneValue = String(draft?.businessPhone || "").trim();

  const stepClass = (step) => (currentStep === step ? "btn btn-primary" : "btn");
  const renderHoursRows = () => ONBOARDING_DAY_KEYS.map((day) => {
    const slots = Array.isArray(draft?.businessHours?.[day]) ? draft.businessHours[day] : [];
    const first = slots[0] || null;
    const isOpen = Boolean(first);
    const start = String(first?.start || "09:00");
    const end = String(first?.end || "17:00");
    const openId = `onb_open_${day}`;
    return `
      <div class="form-row onb-hours-row">
        <label class="onb-day-toggle" for="${openId}">
          <input type="checkbox" id="${openId}" name="open_${day}" ${isOpen ? "checked" : ""}>
          <span>${ONBOARDING_DAY_LABELS[day]}</span>
        </label>
        <label class="onb-hours-field">Start <input type="time" name="start_${day}" value="${escapeAttr(start)}"></label>
        <label class="onb-hours-field">End <input type="time" name="end_${day}" value="${escapeAttr(end)}"></label>
      </div>
    `;
  }).join("");
  const timezoneValue = String(draft.timezone || "America/New_York").trim() || "America/New_York";
  const timezoneOptions = Array.from(new Set([...ONBOARDING_TIMEZONE_OPTIONS, timezoneValue]));
  const relayPublicBookingUrl = String(account?.scheduling?.publicUrl || "").trim();
  const bookingMode = String(draft.bookingMode || "relay_scheduler").trim() || "relay_scheduler";
  const bookingImportSource = String(draft.bookingImportSource || "none").trim() || "none";
  const flows = getOnboardingDetailerFlows(state.onboardingFlows);
  const fallbackFlowId = String(flows?.[0]?.id || "detailing_missed_call_v1").trim();
  const requestedFlowId = String(draft?.defaultFlowId || "").trim();
  const selectedFlowId = flows.some((f) => String(f?.id || "").trim() === requestedFlowId) ? requestedFlowId : fallbackFlowId;
  const pricingByFlow = draft?.pricingByFlow && typeof draft.pricingByFlow === "object"
    ? draft.pricingByFlow
    : {};
  const flowServiceList = onboardingGetPricingServiceList(selectedFlowId, flows, pricingByFlow);
  const flowServiceKeys = flowServiceList.map((x) => x.key);
  const flowPricing = selectedFlowId
    ? onboardingNormalizePricingConfig(pricingByFlow?.[selectedFlowId] || {}, flowServiceKeys)
    : { services: {}, paintScopes: {}, serviceScopes: {} };
  const renderOnbPricingRows = (list, kind, values, { removable = false } = {}) => list.map((item) => {
    const row = values?.[item.key] || { name: onboardingPricingLabelForService(item.key), price: "$0-0", hoursMin: 1, hoursMax: 1 };
    const displayName = String(row.name || onboardingPricingLabelForService(item.key)).trim();
    return `
      <div class="pricing-row">
        <div class="grid2" style="gap:12px; align-items:flex-end;">
          <div class="col">
            <label class="p">Service name</label>
            <input class="input" type="text" data-onb-pricing-kind="${escapeAttr(kind)}" data-onb-pricing-key="${escapeAttr(item.key)}" data-onb-pricing-field="name" value="${escapeAttr(displayName)}" placeholder="Service name">
          </div>
          <div class="col">
            <label class="p">Price range</label>
            <input class="input" type="text" data-onb-pricing-kind="${escapeAttr(kind)}" data-onb-pricing-key="${escapeAttr(item.key)}" data-onb-pricing-field="price" value="${escapeAttr(String(row.price || ""))}" placeholder="$120-260">
          </div>
          <div class="row pricing-row-hours" style="gap:8px; align-items:flex-end;">
            <div class="col" style="min-width:120px;">
              <label class="p">Hours min</label>
              <input class="input" type="number" min="0" step="0.25" data-onb-pricing-kind="${escapeAttr(kind)}" data-onb-pricing-key="${escapeAttr(item.key)}" data-onb-pricing-field="hoursMin" value="${escapeAttr(String(row.hoursMin ?? 0))}">
            </div>
            <div class="col" style="min-width:120px;">
              <label class="p">Hours max</label>
              <input class="input" type="number" min="0" step="0.25" data-onb-pricing-kind="${escapeAttr(kind)}" data-onb-pricing-key="${escapeAttr(item.key)}" data-onb-pricing-field="hoursMax" value="${escapeAttr(String(row.hoursMax ?? 0))}">
            </div>
          </div>
          ${removable ? `<div class="col" style="flex:0 0 auto;"><button type="button" class="btn pricing-row-remove" data-onb-remove-service="${escapeAttr(String(item.key))}">Remove</button></div>` : ""}
        </div>
      </div>
    `;
  }).join("");
  const serviceRows = renderOnbPricingRows(flowServiceList, "services", flowPricing.services, { removable: true });
  const serviceScopeRows = flowServiceList.map((svc) => {
    const scopes = onboardingGetScopeItemsForService(svc.key, flowPricing);
    if (!scopes.length) return "";
    return `
      <div class="card" style="background:var(--panel);">
        <div class="p"><b>${escapeHtml(svc.label)} scopes</b></div>
        <div class="col" style="gap:8px; margin-top:8px;">
          ${scopes.map((scope) => {
            const row = flowPricing?.serviceScopes?.[svc.key]?.[scope.key] || {};
            const scopeName = String(row.name || scope.label || onboardingHumanizeKey(scope.key)).trim();
            return `
              <div class="pricing-row">
                <div class="grid2" style="gap:12px; align-items:flex-end;">
                  <div class="col">
                    <label class="p">Scope name</label>
                    <input class="input" type="text" data-onb-pricing-kind="serviceScopes" data-onb-pricing-key="${escapeAttr(svc.key)}" data-onb-pricing-scope="${escapeAttr(scope.key)}" data-onb-pricing-field="name" value="${escapeAttr(scopeName)}" placeholder="e.g. Pet hair removal">
                  </div>
                  <div class="col">
                    <label class="p">Price</label>
                    <input class="input" type="text" data-onb-pricing-kind="serviceScopes" data-onb-pricing-key="${escapeAttr(svc.key)}" data-onb-pricing-scope="${escapeAttr(scope.key)}" data-onb-pricing-field="price" value="${escapeAttr(String(row.price || ""))}" placeholder="$120-260">
                  </div>
                  <div class="row pricing-row-hours" style="gap:8px; align-items:flex-end;">
                    <div class="col" style="min-width:120px;">
                      <label class="p">Hours min</label>
                      <input class="input" type="number" min="0" step="0.25" data-onb-pricing-kind="serviceScopes" data-onb-pricing-key="${escapeAttr(svc.key)}" data-onb-pricing-scope="${escapeAttr(scope.key)}" data-onb-pricing-field="hoursMin" value="${escapeAttr(String(row.hoursMin ?? 0))}">
                    </div>
                    <div class="col" style="min-width:120px;">
                      <label class="p">Hours max</label>
                      <input class="input" type="number" min="0" step="0.25" data-onb-pricing-kind="serviceScopes" data-onb-pricing-key="${escapeAttr(svc.key)}" data-onb-pricing-scope="${escapeAttr(scope.key)}" data-onb-pricing-field="hoursMax" value="${escapeAttr(String(row.hoursMax ?? 0))}">
                    </div>
                  </div>
                  <div class="col" style="flex:0 0 auto;">
                    <button type="button" class="btn pricing-row-remove" data-onb-remove-scope="${escapeAttr(`${svc.key}::${scope.key}`)}">Remove</button>
                  </div>
                </div>
              </div>
            `;
          }).join("")}
          <div class="row" style="justify-content:flex-end;">
            <button type="button" class="btn" data-onb-add-scope="${escapeAttr(svc.key)}">Add scope</button>
          </div>
        </div>
      </div>
    `;
  }).filter(Boolean).join("");
  const flowOptions = flows.map((flow) => {
    const id = String(flow?.id || "").trim();
    const label = normalizeOnboardingFlowLabel(flow);
    return `<option value="${escapeAttr(id)}" ${id === selectedFlowId ? "selected" : ""}>${escapeHtml(label)}</option>`;
  }).join("");

  const selectedFlowLabel = normalizeOnboardingFlowLabel(flows.find((f) => String(f?.id || "").trim() === selectedFlowId) || {});
  const openDaysCount = ONBOARDING_DAY_KEYS.reduce((count, day) => {
    const slots = Array.isArray(draft?.businessHours?.[day]) ? draft.businessHours[day] : [];
    return count + (slots.length ? 1 : 0);
  }, 0);
  const bookingSetupLabel = bookingMode === "external_scheduler"
    ? "External scheduler URL"
    : bookingMode === "manual_follow_up"
      ? "Manual follow-up (no direct link)"
      : "Relay scheduler page";
  const bookingImportOptions = ONBOARDING_BOOKING_IMPORT_OPTIONS
    .map((opt) => `<option value="${escapeAttr(opt.value)}" ${opt.value === bookingImportSource ? "selected" : ""}>${escapeHtml(opt.label)}</option>`)
    .join("");

  wrap.innerHTML = `
    <div class="analytics-header">
      <div>
        <h1>5-Minute Onboarding</h1>
        <div class="p">Finish setup before using the workspace.</div>
      </div>
      <div class="row">
        <button type="button" class="${stepClass(1)}" data-onb-step="1">1. Business</button>
        <button type="button" class="${stepClass(2)}" data-onb-step="2">2. Hours</button>
        <button type="button" class="${stepClass(3)}" data-onb-step="3">3. Pricing</button>
        <button type="button" class="${stepClass(4)}" data-onb-step="4">4. Booking</button>
        <button type="button" class="${stepClass(5)}" data-onb-step="5">5. Review + Go Live</button>
      </div>
    </div>
    <div class="analytics-card outcome-onboarding-card">
      ${state.onboardingLoading ? '<div class="p">Loading onboarding...</div>' : `
        <form class="outcome-onboarding-form" data-onboarding-wizard-form>
          <div class="${currentStep === 1 ? "" : "hidden"}" data-onb-panel="1">
            <div class="form-row">
              <label>
                Business type
                <input name="businessType" type="text" value="${escapeAttr(String(draft.businessType || ""))}" placeholder="e.g. auto_detailing">
              </label>
              <label>
                Business name
                <input name="businessName" type="text" value="${escapeAttr(String(draft.businessName || ""))}" placeholder="e.g. Mike's Detailing">
              </label>
            </div>
            <div class="form-row">
              <label>
                Business email
                <input name="businessEmail" type="email" value="${escapeAttr(String(draft.businessEmail || ""))}" placeholder="owner@business.com">
              </label>
              <label>
                Business logo
                <input name="businessLogoUrl" type="url" value="${escapeAttr(String(draft.businessLogoUrl || ""))}" placeholder="Logo URL will auto-fill after upload" readonly>
                <div class="row" style="gap:8px;">
                  <input type="file" accept="image/png,image/jpeg,image/webp,image/gif" class="hidden" data-onb-logo-file>
                  <button type="button" class="btn" data-onb-logo-pick>Attach file</button>
                  <button type="button" class="btn" data-onb-logo-remove ${draft.businessLogoUrl ? "" : "disabled"}>Remove logo</button>
                  <span class="p muted" data-onb-logo-status></span>
                </div>
              </label>
              <label>
                Business phone
                <input name="businessPhone" type="text" value="${escapeAttr(String(businessPhoneValue || ""))}" placeholder="(814) 555-1212">
                <div class="p muted">This is the business's existing public phone number from before Relay.</div>
              </label>
            </div>
            ${draft.businessLogoUrl ? `
              <div class="row">
                <img src="${escapeAttr(String(draft.businessLogoUrl || ""))}" alt="Business logo preview" style="display:block; width:auto; height:auto; max-width:220px; max-height:64px; object-fit:contain; border:1px solid var(--border); border-radius:8px; padding:6px; background:rgba(255,255,255,0.03);">
              </div>
            ` : ""}
          </div>
          <div class="${currentStep === 2 ? "" : "hidden"}" data-onb-panel="2">
            <div class="form-row">
              <label>
                Timezone
                <select name="timezone" class="select">
                  ${timezoneOptions.map((tz) => `<option value="${escapeAttr(tz)}" ${tz === timezoneValue ? "selected" : ""}>${escapeHtml(tz)}</option>`).join("")}
                </select>
              </label>
            </div>
            ${renderHoursRows()}
          </div>
          <div class="${currentStep === 3 ? "" : "hidden"}" data-onb-panel="3">
            <input type="hidden" name="pricingFlowId" value="${escapeAttr(selectedFlowId)}">
            <div class="form-row">
              <label>
                Pricing profile flow
                <select name="pricingFlowIdSelect" class="select" data-onb-pricing-flow-select>
                  ${flowOptions || '<option value="">No flows</option>'}
                </select>
              </label>
              <label>
                Avg ticket value ($)
                <input name="avgTicketValueDollars" type="number" min="0" step="10" value="${escapeAttr(String(draft.avgTicketValueDollars || 0))}">
              </label>
            </div>
            <div class="pricing-card onboarding-pricing-card">
              <section class="pricing-section">
                <div class="pricing-section-head">
                  <div>
                    <div class="pricing-section-title">Services + Pricing</div>
                    <p class="p muted" style="margin:0;">Define the core services, their price ranges, and time estimates.</p>
                  </div>
                  <button class="btn pricing-section-action" type="button" data-onb-add-service ${selectedFlowId ? "" : "disabled"}>Add service</button>
                </div>
                <div class="pricing-section-body" data-onb-services>
                  ${serviceRows}
                </div>
              </section>
              <section class="pricing-section">
                <div class="pricing-section-head">
                  <div>
                    <div class="pricing-section-title">Service scope pricing</div>
                    <p class="p muted" style="margin:0;">Capture the scoped add-ons tied to each primary service.</p>
                  </div>
                </div>
                <div class="pricing-section-body" data-onb-service-scopes>
                  ${serviceScopeRows || '<div class="p">No scope profiles for this flow yet.</div>'}
                </div>
              </section>
            </div>
          </div>
          <div class="${currentStep === 4 ? "" : "hidden"}" data-onb-panel="4">
            <div class="p"><b>Booking runs on Relay</b></div>
            <div class="p">Your Relay booking URL</div>
            <div class="card" style="background:var(--panel); padding:10px 12px;">
              <code>${escapeHtml(relayPublicBookingUrl || "Not generated yet")}</code>
            </div>
            ${relayPublicBookingUrl ? "" : '<div class="p" style="color:#d3a94b;">(Will be generated after setup)</div>'}
            <div class="row" style="gap:8px; margin-top:8px;">
              <button type="button" class="btn" data-onb-action="generate-booking-url">Generate Relay booking URL</button>
              ${relayPublicBookingUrl ? `<button type="button" class="btn" data-onb-action="copy-booking-url">Copy URL</button>` : ""}
              ${relayPublicBookingUrl ? `<a class="btn" href="${escapeAttr(relayPublicBookingUrl)}" target="_blank" rel="noopener noreferrer">Open booking page</a>` : ""}
            </div>
            <div class="p muted">Customers book through your Relay page so everything stays in one place.</div>
            <input type="hidden" name="bookingMode" value="relay_scheduler">
            <input type="hidden" name="bookingUrl" value="">
            <input type="hidden" name="bookingImportSource" value="none">
            <input type="hidden" name="calendarConnected" value="${draft.calendarConnected ? "1" : "0"}">
            <div class="p muted">Using Calendly/Square/Google Calendar? Configure that later in Settings.</div>
            ${relayPublicBookingUrl ? `
              <div class="card" style="background:var(--panel); margin-top:10px;">
                <div class="p"><b>Booking page preview</b></div>
                <iframe src="${escapeAttr(relayPublicBookingUrl)}" title="Relay booking preview" style="width:100%; height:320px; border:1px solid var(--border); border-radius:8px; background:#fff;"></iframe>
              </div>
            ` : ""}
          </div>
          <div class="${currentStep === 5 ? "" : "hidden"}" data-onb-panel="5">
            <div class="p"><b>Review + Go Live</b></div>
            <div class="card" style="background:var(--panel);">
              <div class="p"><b>Business info</b></div>
              <div class="p muted">${escapeHtml(String(draft.businessName || "Not set"))}</div>
              <div class="p muted">${escapeHtml(String(draft.businessType || "Not set"))}</div>
              <div class="p muted">${escapeHtml(String(draft.businessEmail || "No email"))}</div>
              <div class="p muted">${escapeHtml(String(draft.businessPhone || "No phone"))}</div>
            </div>
            <div class="card" style="background:var(--panel);">
              <div class="p"><b>Hours/timezone</b></div>
              <div class="p muted">${escapeHtml(String(timezoneValue || "America/New_York"))}</div>
              <div class="p muted">${escapeHtml(String(openDaysCount))} open days configured</div>
            </div>
            <div class="card" style="background:var(--panel);">
              <div class="p"><b>Pricing/profile</b></div>
              <div class="p muted">${escapeHtml(String(selectedFlowLabel || "Not set"))}</div>
              <div class="p muted">${escapeHtml(String(flowServiceList.length || 0))} services configured</div>
              <div class="p muted">Avg ticket: $${escapeHtml(String(Number(draft.avgTicketValueDollars || 0)))}</div>
            </div>
            <div class="card" style="background:var(--panel);">
              <div class="p"><b>Booking setup</b></div>
              <div class="p muted">${escapeHtml(bookingSetupLabel)}</div>
              <div class="p muted">${escapeHtml(String(bookingMode === "external_scheduler" ? (draft.bookingUrl || "No URL set") : (relayPublicBookingUrl || "Will be generated after setup")))}</div>
              <div class="p muted">Calendar connected: ${draft.calendarConnected ? "Yes" : "No"}</div>
            </div>
            <div class="row" style="justify-content:flex-end;">
              <button type="button" class="btn btn-primary" data-onb-action="finish">Finish Setup & Go Live</button>
            </div>
          </div>
          <div class="row" style="justify-content:space-between;">
            <button type="button" class="btn" data-onb-action="back" ${currentStep <= 1 ? "disabled" : ""}>Back</button>
            <button type="button" class="btn btn-primary" data-onb-action="next" ${currentStep >= 5 ? "disabled" : ""}>Next</button>
          </div>
        </form>
      `}
      <div class="outcome-onboarding-status">${escapeHtml(saveError || testStatus || "")}</div>
    </div>
  `;

  const form = wrap.querySelector("[data-onboarding-wizard-form]");
  const setDraftFromForm = () => {
    if (!form) return;
    state.onboardingDraft = readOnboardingDraftFromForm(form, state.onboardingDraft || draft);
  };
  const logoFileInput = wrap.querySelector("[data-onb-logo-file]");
  const logoPickBtn = wrap.querySelector("[data-onb-logo-pick]");
  const logoRemoveBtn = wrap.querySelector("[data-onb-logo-remove]");
  const logoStatus = wrap.querySelector("[data-onb-logo-status]");
  logoPickBtn?.addEventListener("click", () => {
    logoFileInput?.click();
  });
  logoFileInput?.addEventListener("change", async () => {
    const file = logoFileInput?.files?.[0];
    if (!file) return;
    const active = resolveActiveWorkspace();
    const to = String(active?.to || "").trim();
    if (logoStatus) logoStatus.textContent = "Uploading...";
    if (logoPickBtn) logoPickBtn.disabled = true;
    try {
      const uploaded = await uploadOnboardingLogoFile(file, to);
      setDraftFromForm();
      state.onboardingDraft = {
        ...(state.onboardingDraft || draft),
        businessLogoUrl: String(uploaded?.logoUrl || "").trim()
      };
      if (logoStatus) logoStatus.textContent = "Uploaded";
      render();
    } catch (err) {
      if (logoStatus) logoStatus.textContent = err?.message || "Upload failed";
    } finally {
      if (logoPickBtn) logoPickBtn.disabled = false;
      if (logoFileInput) logoFileInput.value = "";
    }
  });
  logoRemoveBtn?.addEventListener("click", async () => {
    const active = resolveActiveWorkspace();
    const to = String(active?.to || "").trim();
    if (logoStatus) logoStatus.textContent = "Removing...";
    if (logoRemoveBtn) logoRemoveBtn.disabled = true;
    try {
      const path = to ? `/api/account/logo?to=${encodeURIComponent(to)}` : "/api/account/logo";
      await apiDelete(path);
      setDraftFromForm();
      state.onboardingDraft = {
        ...(state.onboardingDraft || draft),
        businessLogoUrl: ""
      };
      if (logoStatus) logoStatus.textContent = "Removed";
      render();
    } catch (err) {
      if (logoStatus) logoStatus.textContent = err?.message || "Remove failed";
    } finally {
      if (logoRemoveBtn) logoRemoveBtn.disabled = false;
    }
  });

  wrap.querySelectorAll("[data-onb-step]").forEach((btn) => {
    btn.addEventListener("click", () => {
      setDraftFromForm();
      const step = Number(btn.getAttribute("data-onb-step") || 1);
      state.onboardingStep = Math.max(1, Math.min(5, step));
      render();
    });
  });
  wrap.querySelectorAll('input[name="bookingMode"]').forEach((el) => {
    el.addEventListener("change", () => {
      setDraftFromForm();
      render();
    });
  });

  wrap.querySelector("[data-onb-pricing-flow-select]")?.addEventListener("change", (ev) => {
    setDraftFromForm();
    const selected = String(ev?.target?.value || "").trim();
    state.onboardingDraft = {
      ...(state.onboardingDraft || draft),
      defaultFlowId: selected
    };
    render();
  });
  wrap.querySelector("[data-onb-add-service]")?.addEventListener("click", () => {
    setDraftFromForm();
    const nextDraft = { ...(state.onboardingDraft || draft) };
    const flowId = String(nextDraft?.defaultFlowId || "").trim();
    if (!flowId) return;
    const pricingByFlow = nextDraft?.pricingByFlow && typeof nextDraft.pricingByFlow === "object"
      ? cloneJson(nextDraft.pricingByFlow) || {}
      : {};
    const serviceList = onboardingGetPricingServiceList(flowId, state.onboardingFlows || [], pricingByFlow);
    const serviceKeys = serviceList.map((x) => x.key);
    const pricing = onboardingNormalizePricingConfig(pricingByFlow[flowId] || {}, serviceKeys);
    let serviceId = sanitizeOnboardingServiceId(`service_${serviceList.length + 1}`, "service");
    let counter = 2;
    while (pricing?.services?.[serviceId]) {
      serviceId = sanitizeOnboardingServiceId(`service_${serviceList.length + counter}`, "service");
      counter += 1;
    }
    pricing.services[serviceId] = { name: onboardingPricingLabelForService(serviceId), price: "$0-0", hoursMin: 1, hoursMax: 1 };
    pricing.serviceScopes[serviceId] = {};
    pricingByFlow[flowId] = pricing;
    nextDraft.pricingByFlow = pricingByFlow;
    state.onboardingDraft = nextDraft;
    render();
  });
  wrap.querySelectorAll("[data-onb-remove-service]").forEach((btn) => {
    btn.addEventListener("click", () => {
      setDraftFromForm();
      const serviceId = String(btn.getAttribute("data-onb-remove-service") || "").trim();
      const nextDraft = { ...(state.onboardingDraft || draft) };
      const flowId = String(nextDraft?.defaultFlowId || "").trim();
      if (!flowId || !serviceId) return;
      const pricingByFlow = nextDraft?.pricingByFlow && typeof nextDraft.pricingByFlow === "object"
        ? cloneJson(nextDraft.pricingByFlow) || {}
        : {};
      const serviceList = onboardingGetPricingServiceList(flowId, state.onboardingFlows || [], pricingByFlow);
      if (serviceList.length <= 1) return;
      const pricing = onboardingNormalizePricingConfig(pricingByFlow[flowId] || {}, serviceList.map((x) => x.key));
      if (pricing.services?.[serviceId]) delete pricing.services[serviceId];
      if (pricing.serviceScopes?.[serviceId]) delete pricing.serviceScopes[serviceId];
      pricingByFlow[flowId] = pricing;
      nextDraft.pricingByFlow = pricingByFlow;
      state.onboardingDraft = nextDraft;
      render();
    });
  });
  wrap.querySelectorAll("[data-onb-add-scope]").forEach((btn) => {
    btn.addEventListener("click", () => {
      setDraftFromForm();
      const serviceKey = String(btn.getAttribute("data-onb-add-scope") || "").trim();
      const nextDraft = { ...(state.onboardingDraft || draft) };
      const flowId = String(nextDraft?.defaultFlowId || "").trim();
      if (!flowId || !serviceKey) return;
      const pricingByFlow = nextDraft?.pricingByFlow && typeof nextDraft.pricingByFlow === "object"
        ? cloneJson(nextDraft.pricingByFlow) || {}
        : {};
      const serviceList = onboardingGetPricingServiceList(flowId, state.onboardingFlows || [], pricingByFlow);
      const pricing = onboardingNormalizePricingConfig(pricingByFlow[flowId] || {}, serviceList.map((x) => x.key));
      pricing.serviceScopes = pricing.serviceScopes || {};
      pricing.serviceScopes[serviceKey] = pricing.serviceScopes[serviceKey] || {};
      const existingKeys = Object.keys(pricing.serviceScopes[serviceKey] || {});
      let idx = existingKeys.length + 1;
      let scopeKey = sanitizeOnboardingScopeId(`scope_${idx}`, "scope");
      while (pricing.serviceScopes[serviceKey][scopeKey]) {
        idx += 1;
        scopeKey = sanitizeOnboardingScopeId(`scope_${idx}`, "scope");
      }
      pricing.serviceScopes[serviceKey][scopeKey] = {
        name: "New scope",
        price: "$0-0",
        hoursMin: 1,
        hoursMax: 1
      };
      pricingByFlow[flowId] = pricing;
      nextDraft.pricingByFlow = pricingByFlow;
      state.onboardingDraft = nextDraft;
      render();
    });
  });
  wrap.querySelectorAll("[data-onb-remove-scope]").forEach((btn) => {
    btn.addEventListener("click", () => {
      setDraftFromForm();
      const token = String(btn.getAttribute("data-onb-remove-scope") || "").trim();
      const [serviceKey, scopeKey] = token.split("::");
      const nextDraft = { ...(state.onboardingDraft || draft) };
      const flowId = String(nextDraft?.defaultFlowId || "").trim();
      if (!flowId || !serviceKey || !scopeKey) return;
      const pricingByFlow = nextDraft?.pricingByFlow && typeof nextDraft.pricingByFlow === "object"
        ? cloneJson(nextDraft.pricingByFlow) || {}
        : {};
      const serviceList = onboardingGetPricingServiceList(flowId, state.onboardingFlows || [], pricingByFlow);
      const pricing = onboardingNormalizePricingConfig(pricingByFlow[flowId] || {}, serviceList.map((x) => x.key));
      if (pricing?.serviceScopes?.[serviceKey]?.[scopeKey]) {
        delete pricing.serviceScopes[serviceKey][scopeKey];
        if (!Object.keys(pricing.serviceScopes[serviceKey]).length) {
          delete pricing.serviceScopes[serviceKey];
        }
      }
      pricingByFlow[flowId] = pricing;
      nextDraft.pricingByFlow = pricingByFlow;
      state.onboardingDraft = nextDraft;
      render();
    });
  });

  wrap.querySelectorAll("[data-onb-action]").forEach((btn) => {
    btn.addEventListener("click", async () => {
      const action = String(btn.getAttribute("data-onb-action") || "");
      setDraftFromForm();
      if (action === "back") {
        state.onboardingStep = Math.max(1, Number(state.onboardingStep || 1) - 1);
        render();
        return;
      }
      if (action === "next") {
        state.onboardingStep = Math.min(5, Number(state.onboardingStep || 1) + 1);
        render();
        return;
      }
      if (action === "generate-booking-url") {
        btn.disabled = true;
        state.onboardingError = "";
        state.onboardingTestStatus = "Generating Relay booking URL...";
        render();
        try {
          const generatedUrl = await ensureOnboardingRelayBookingUrl();
          state.onboardingBookingPreviewKey = "";
          state.onboardingBookingPreview = null;
          state.onboardingBookingPreviewLoading = false;
          state.onboardingBookingPreviewError = null;
          state.onboardingTestStatus = generatedUrl ? "Relay booking URL generated." : "Relay booking URL saved.";
        } catch (err) {
          state.onboardingTestStatus = "";
          state.onboardingError = err?.message || "Failed to generate booking URL.";
        } finally {
          btn.disabled = false;
          render();
        }
        return;
      }
      if (action === "copy-booking-url") {
        const url = String(relayPublicBookingUrl || "").trim();
        if (!url) {
          state.onboardingError = "No Relay booking URL to copy yet.";
          render();
          return;
        }
        try {
          await navigator.clipboard.writeText(url);
          state.onboardingError = null;
          state.onboardingTestStatus = "Booking URL copied.";
        } catch {
          state.onboardingTestStatus = "";
          state.onboardingError = "Unable to copy URL. Copy it manually from the field above.";
        }
        render();
        return;
      }
      if (action === "test") {
        btn.disabled = true;
        state.onboardingTestStatus = "Sending test message...";
        render();
        try {
          await sendOnboardingTestMessage();
          state.onboardingError = null;
          state.onboardingTestStatus = "Test message sent.";
        } catch (err) {
          state.onboardingTestStatus = "";
          state.onboardingError = err?.message || "Failed to send test message.";
        } finally {
          btn.disabled = false;
          render();
        }
        return;
      }
      if (action === "finish") {
        btn.disabled = true;
        state.onboardingError = "";
        state.onboardingTestStatus = "Saving onboarding...";
        render();
        try {
          await completeOnboardingFromDraft();
          state.onboardingTestStatus = "Onboarding complete.";
          state.view = "home";
          state.onboardingStep = 1;
          state.onboardingDraft = null;
          state.onboardingDraftKey = "";
          state.homeOverviewCache = {};
          state.homeFunnelCache = {};
          state.homeWinsCache = {};
        } catch (err) {
          state.onboardingTestStatus = "";
          state.onboardingError = err?.message || "Failed to complete onboarding.";
        } finally {
          btn.disabled = false;
          render();
        }
      }
    });
  });

  return wrap;
}

function viewHome(){
  const wrap = document.createElement("div");
  wrap.className = "dashboard home-neo app-view app-view-home";

  const snap = getHomeSnapshot(1);
  const action = getActionRequiredLeads();
  const health = getAutomationHealth();
  const recent = getRecentLeads(5);
  const activity = getRecentActivity(5);
  const scopeKey = getAnalyticsScopeKey();
  const overview = state.homeOverviewCache?.[scopeKey] || {};
  const funnel = state.homeFunnelCache?.[scopeKey] || {};
  const wins = state.homeWinsCache?.[scopeKey] || {};

  // Calculate conversion rate
  const conversionRate = snap.inbound > 0 ? Math.round((snap.booked / snap.inbound) * 100) : 0;
  const nowMs = Date.now();
  const threadRows = safeArray(state.threads).map((thread) => {
    const ld = thread?.leadData || {};
    const status = String(thread?.status || "").toLowerCase();
    const stage = String(thread?.stage || "").toLowerCase();
    const bookedMs = coerceTimestampMs(
      thread?.bookingTime || ld?.booking_time || ld?.bookingTime || getLatestBookedConfirmationTime(thread) || thread?.updatedAt || thread?.lastActivityAt,
      0
    );
    const bookedLike = status === "booked"
      || status === "closed"
      || /booked|appointment_booked|scheduled/.test(stage)
      || (Number.isFinite(bookedMs) && bookedMs > 0);
    const amount = Number(resolveConversationAmount(thread) || 0);
    return {
      bookedLike,
      bookedMs,
      amountCents: Number.isFinite(amount) && amount > 0 ? Math.round(amount * 100) : 0
    };
  });
  const allBookedRows = threadRows.filter((row) => row.bookedLike);
  const appointmentsBookedFromThreads = allBookedRows.length;
  const recoveredRevenueFromThreadsCents = allBookedRows.reduce((sum, row) => sum + Number(row.amountCents || 0), 0);
  const bookedRevenueFromOverviewCents = Number(overview?.recoveredThisMonth || 0);
  const bookedRevenueCents = Math.max(bookedRevenueFromOverviewCents, recoveredRevenueFromThreadsCents);
  const revenueRecoveryRate = Math.round(Number(overview?.revenueRecoveryRate || 0) * 100);
  const estimatedLostRevenueCents = Math.max(0, Number(overview?.estimatedLostRevenueCents || 0));
  const recoveredRevenueCents = Math.max(0, Number(overview?.recoveredRevenueCents || 0), recoveredRevenueFromThreadsCents);
  const projectedRecoveryCents = Math.max(0, Number(overview?.projectedRecoveryCents || 0));
  const totalLeakPool = recoveredRevenueCents + estimatedLostRevenueCents;
  const leakPct = totalLeakPool > 0 ? Math.round((estimatedLostRevenueCents / totalLeakPool) * 100) : 0;
  const rescuedPct = totalLeakPool > 0 ? Math.round((recoveredRevenueCents / totalLeakPool) * 100) : 0;

  const revenueEvents = Array.isArray(overview?.revenueEvents) ? overview.revenueEvents : [];
  const missedCallsRecovered = revenueEvents.filter((e) => {
    const signal = String(e?.signalType || "").toLowerCase();
    const type = String(e?.type || "").toLowerCase();
    const status = String(e?.status || "").toLowerCase();
    const fromMissed = signal.includes("missed_call");
    const recovered = ["opportunity_recovered", "appointment_booked", "sale_closed"].includes(type) || status === "won";
    return fromMissed && recovered;
  }).length;
  const appointmentsBookedFromEvents = revenueEvents.filter((e) => String(e?.type || "").toLowerCase() === "appointment_booked").length;
  const appointmentsBooked = Math.max(appointmentsBookedFromThreads, appointmentsBookedFromEvents);
  const todayStart = new Date(new Date(nowMs).getFullYear(), new Date(nowMs).getMonth(), new Date(nowMs).getDate()).getTime();
  const contactedTodayFromEvents = revenueEvents.filter((e) => {
    const ts = Number(e?.createdAt || 0);
    if (!Number.isFinite(ts) || ts < todayStart || ts > nowMs) return false;
    const type = String(e?.type || "").toLowerCase();
    const signal = String(e?.signalType || "").toLowerCase();
    return type === "opportunity_created" && signal.includes("missed_call");
  }).length;
  const contactedTodayFromThreads = safeArray(state.threads).filter((thread) => {
    const msgs = Array.isArray(thread?.messages) ? thread.messages : [];
    if (!msgs.length) return false;
    const firstOutbound = msgs.find((m) => {
      const dir = String(m?.dir || m?.direction || "").toLowerCase();
      return dir === "out" || dir === "outbound";
    });
    if (!firstOutbound) return false;
    const ts = Number(firstOutbound?.ts || firstOutbound?.createdAt || 0);
    if (!Number.isFinite(ts) || ts < todayStart || ts > nowMs) return false;
    const text = String(firstOutbound?.text || firstOutbound?.body || "").toLowerCase();
    return /sorry.*missed your call|sorry we missed your call|sorry i missed your call/.test(text);
  }).length;
  const contactedToday = Math.max(contactedTodayFromEvents, contactedTodayFromThreads);
  const bookedRevenueText = moneyFromCents(bookedRevenueCents || 0);
  const missedCallsRecoveredText = String(missedCallsRecovered || 0);
  const appointmentsBookedText = String(appointmentsBooked || 0);
  const leakSummaryText = estimatedLostRevenueCents > 0
    ? `${leakPct}% still leaking`
    : "Leak controlled";
  const quoteStarted = Number(funnel?.quoteStarted || 0);
  const quoteReady = Number(funnel?.quoteReady || 0);
  const quoteShown = Number(funnel?.quoteShown || 0);
  const quoteAccepted = Number(funnel?.quoteAccepted || 0);
  const quoteAcceptRate = quoteShown > 0 ? Math.round((quoteAccepted / quoteShown) * 100) : 0;
  const quoteReadyRate = quoteStarted > 0 ? Math.round((quoteReady / quoteStarted) * 100) : 0;
  const quoteShownRate = quoteStarted > 0 ? Math.round((quoteShown / quoteStarted) * 100) : 0;
  const todayWins = wins?.today || {};
  const weekWins = wins?.week || {};

  const todayBooked = Number(todayWins.bookedJobs || 0);
  const todayRecoveredRevenue = moneyFromCents(Number(todayWins.recoveredRevenueCents || 0));
  const todayRecoveredCalls = Number(todayWins.recoveredCalls || 0);
  const slaValue = weekWins.responseSlaMinutes == null ? "-" : `${Number(weekWins.responseSlaMinutes)}m`;

  wrap.innerHTML = `
    <section class="home-neo-kpis">
      <article class="home-neo-kpi">
        <span class="home-neo-kpi-label">Revenue Booked Today</span>
        <strong class="home-neo-kpi-value">${todayRecoveredRevenue}</strong>
        <span class="home-neo-kpi-meta">${revenueRecoveryRate}% recovery rate</span>
      </article>
      <article class="home-neo-kpi">
        <span class="home-neo-kpi-label">Appointments Booked Today</span>
        <strong class="home-neo-kpi-value">${todayBooked}</strong>
        <span class="home-neo-kpi-meta">Quote accept ${quoteAcceptRate}% (${quoteAccepted}/${quoteShown})</span>
      </article>
      <article class="home-neo-kpi">
        <span class="home-neo-kpi-label">Contacted Today</span>
        <strong class="home-neo-kpi-value">${contactedToday}</strong>
        <span class="home-neo-kpi-meta">First auto-texts sent after missed calls today</span>
      </article>
      <article class="home-neo-kpi ${estimatedLostRevenueCents > 0 ? "is-alert" : ""}">
        <span class="home-neo-kpi-label">Revenue Leak</span>
        <strong class="home-neo-kpi-value">${moneyFromCents(estimatedLostRevenueCents)}</strong>
        <span class="home-neo-kpi-meta">${leakSummaryText}</span>
      </article>
    </section>

    <section class="home-neo-grid">
      <article class="home-neo-panel" style="grid-column: 1 / -1;">
        <div class="home-neo-panel-head">
          <h2>Monthly Revenue Flow</h2>
          <button class="btn btn-sm" id="goToRevenueBoardSecondary">Open Analytics</button>
        </div>
        <div class="home-neo-flow">
          <div class="home-neo-meter" role="img" aria-label="Recovered versus leaking revenue">
            <div class="home-neo-meter-segment recovered" style="width:${Math.max(0, Math.min(100, rescuedPct))}%"></div>
            <div class="home-neo-meter-segment leaking" style="width:${Math.max(0, Math.min(100, leakPct))}%"></div>
          </div>
          <div class="home-neo-flow-legend">
            <div><span class="dot recovered"></span>Recovered ${moneyFromCents(recoveredRevenueCents)}</div>
            <div><span class="dot leaking"></span>Leaking ${moneyFromCents(estimatedLostRevenueCents)}</div>
            <div><span class="dot projected"></span>Projected ${moneyFromCents(projectedRecoveryCents)}</div>
          </div>
          <div class="home-neo-conversion">
            <div class="row"><span>Quote started</span><strong>${quoteStarted}</strong></div>
            <div class="row"><span>Quote ready</span><strong>${quoteReadyRate}%</strong></div>
            <div class="row"><span>Quote shown</span><strong>${quoteShownRate}%</strong></div>
            <div class="row"><span>Quote accepted</span><strong>${quoteAcceptRate}%</strong></div>
          </div>
        </div>
      </article>

      <article id="homeRecentLeadsPanel" class="home-neo-panel home-neo-panel-wide">
        <div id="demoRecentLeadsAnchor" class="home-neo-panel-head">
          <h2>Recent Leads</h2>
          <button class="btn btn-sm" id="viewAllLeads">View Inbox</button>
        </div>
        <div id="homeRecentLeadsTable" class="home-neo-table-wrap">
          ${recent.length === 0
            ? RelayUI.renderEmptyState({ text: "No recent leads yet." })
            : `<table class="home-neo-table">
                <thead>
                  <tr>
                    <th>Contact</th>
                    <th>Request</th>
                    <th>Status</th>
                    <th>Updated</th>
                  </tr>
                </thead>
                <tbody>
                  ${recent.map((t) => {
                    const [l1, l2] = getLeadPreviewLines(t);
                    const chips = renderServiceChipsHtml(getServiceLabelsFromLeadData(t.leadData || {}));
                    const requestLabel = getServiceLabelsFromLeadData(t.leadData || {})[0] || l1 || "Awaiting customer reply";
                    return `
                      <tr class="lead-row" data-convo="${t.id}">
                        <td>
                          <div class="home-neo-contact">
                            <strong>${escapeHtml(t.from)}</strong>
                            <span>${escapeHtml(l2 || "Vehicle not provided")}</span>
                          </div>
                        </td>
                        <td>
                          <div class="home-neo-request">
                            <span>${escapeHtml(requestLabel)}</span>
                            ${chips}
                          </div>
                        </td>
                        <td><span class="status-badge status-${(t.status || "new").toLowerCase()}">${escapeHtml(t.status || "new")}</span></td>
                        <td>${escapeHtml(formatTimeAgo(t.lastActivityAt || t.updatedAt || t.createdAt))}</td>
                      </tr>
                    `;
                  }).join("")}
                </tbody>
              </table>`
          }
        </div>
      </article>

      <article class="home-neo-panel">
        <div class="home-neo-panel-head">
          <h2>System Health</h2>
        </div>
        <div class="home-neo-stat-list">
          <div class="item"><span>Auto-Reply</span><strong>Off</strong></div>
          <div class="item"><span>AI Intent</span><strong>Active</strong></div>
          <div class="item"><span>Booking Link</span><strong>Set</strong></div>
        </div>
        <p class="home-neo-note">Inbox is caught up. Keep response speed tight to protect recovery momentum.</p>
      </article>
    </section>
  `;

  // Event listeners
  wrap.querySelector('#viewAllLeads')?.addEventListener('click', async () => {
    state.view = 'messages';
    await render();
  });
  wrap.querySelector('#goToRevenueBoard')?.addEventListener('click', async () => {
    state.view = 'analytics';
    await render();
  });
  wrap.querySelector('#goToRevenueBoardSecondary')?.addEventListener('click', async () => {
    state.view = 'analytics';
    await render();
  });

  wrap.querySelector('#goToMessages')?.addEventListener('click', async () => {
    state.view = 'messages';
    await render();
  });

  wrap.querySelector('#goToContacts')?.addEventListener('click', async () => {
    state.view = 'contacts';
    await render();
  });

  wrap.querySelector('#goToAutomations')?.addEventListener('click', async () => {
    localStorage.setItem("mc_settings_tab_v1", "automations");
    state.view = 'settings';
    await render();
  });

  wrap.querySelector('#goToSettings')?.addEventListener('click', async () => {
    state.view = 'settings';
    await render();
  });

  wrap.querySelectorAll('.lead-row').forEach(el => {
    el.addEventListener('click', async () => {
      state.activeThreadId = el.dataset.convo;
      state.view = 'messages';
      await render();
    });
  });

  wrap.querySelectorAll('.activity-item').forEach(el => {
    el.addEventListener('click', async () => {
      if (el.dataset.convo) {
        state.activeThreadId = el.dataset.convo;
        state.view = 'messages';
        await render();
      }
    });
  });

  return wrap;
}




/* Contacts (placeholder) */
function viewContacts(){
  const wrap = document.createElement("div");
  wrap.className = "col contacts-view app-view app-view-contacts";
  const contactsScope = createUiScopeSnapshot({ view: "contacts", to: getActiveTo(), renderId: _renderId });
  if (!canAccessSensitiveContactsInUi()) {
    const locked = document.createElement("div");
    locked.className = "card";
    locked.innerHTML = `
      <div class="h1" style="margin:0;">Contacts Locked</div>
      <div class="p" style="margin-top:8px;">Customer contact records are protected. Only workspace owner/admin roles can access full contact details.</div>
      <div class="p" style="margin-top:6px;">Ask your owner/admin for access if required.</div>
    `;
    wrap.appendChild(locked);
    return wrap;
  }

  const card = document.createElement("div");
  card.className = "card contacts-shell";

  card.innerHTML = `
    <div class="contacts-toolbar ops-toolbar">
      <div class="contacts-toolbar-main ops-toolbar-main">
        <label class="contacts-search-wrap" for="contactSearch">
          <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" aria-hidden="true">
            <circle cx="11" cy="11" r="7"></circle>
            <path d="m20 20-3.5-3.5"></path>
          </svg>
          <input class="contacts-search-input" id="contactSearch" placeholder="Search name or number" />
        </label>

        <div class="contacts-segment" role="tablist" aria-label="Contact filters">
          <button type="button" class="contacts-segment-btn is-active" data-cfilter="all">All</button>
          <button type="button" class="contacts-segment-btn" data-cfilter="vip">VIP</button>
          <button type="button" class="contacts-segment-btn" data-cfilter="new">New</button>
          <button type="button" class="contacts-segment-btn" data-cfilter="booked">Booked</button>
          <button type="button" class="contacts-segment-btn" data-cfilter="dnr">Do Not Reply</button>
        </div>
      </div>

      <div class="contacts-toolbar-actions ops-toolbar-actions">
        <button type="button" class="btn primary" id="addContactBtn">+ Add contact</button>
        <button type="button" class="btn help-btn" id="contactsHelpBtn" title="Help">
          <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" width="18" height="18">
            <circle cx="12" cy="12" r="10"/>
            <path d="M9.09 9a3 3 0 0 1 5.83 1c0 2-3 3-3 3"/>
            <circle cx="12" cy="17" r="0.5" fill="currentColor"/>
          </svg>
        </button>
      </div>
    </div>

    <div style="height:14px;"></div>

    <div class="inbox contacts-inbox" style="grid-template-columns: 420px 1fr; gap:12px;">
      <div class="panel">
        <div class="panel-body" id="contactList"></div>
      </div>

      <div class="panel ops-detail-pane">
        <div class="panel-head">
          <div class="row" style="justify-content:space-between; align-items:center;">
            <div class="ops-panel-copy">
              <div class="h1" style="margin:0;">Contact</div>
              <div class="p muted" style="margin:0;">Details, lifecycle, and contact controls.</div>
            </div>
            <button type="button" class="btn" id="openConvoBtn" disabled>Open conversation</button>
          </div>
        </div>
        <div class="panel-body" id="contactDetail">
          <div class="p">Select a contact to view details.</div>
        </div>
      </div>
    </div>

    <!-- Simple modal -->
    <div id="contactModal" class="overlay hidden">
      <div class="auth-card" style="width:min(560px, 92vw);">
        <div class="auth-title" style="margin-bottom:10px;">
          <h1 style="margin:0;">Add contact</h1>
          <p style="margin:6px 0 0;" class="p">Saved to this account (To: ${escapeHtml(getActiveTo())}).</p>
        </div>

        <div class="auth-form" style="gap:12px;">
          <label class="field">
            <span>Phone</span>
            <input class="input" id="newContactPhone" placeholder="+18145559999" />
          </label>

          <label class="field">
            <span>Name</span>
            <input class="input" id="newContactName" placeholder="John Smith" />
          </label>

          <label class="toggle" style="justify-content:flex-start;">
            <input type="checkbox" id="newContactVip" />
            VIP
          </label>

          <label class="toggle" style="justify-content:flex-start;">
            <input type="checkbox" id="newContactDnr" />
            Do not auto-reply
          </label>

          <label class="field">
            <span>Notes (optional)</span>
            <textarea class="input" id="newContactNotes" rows="4" placeholder="Anything you want to remember..."></textarea>
          </label>

          <div class="row" style="justify-content:flex-end; gap:10px;">
            <button type="button" class="btn" id="cancelContactBtn">Cancel</button>
            <button type="button" class="btn primary" id="saveContactBtn">Save contact</button>
          </div>

          <p class="error" id="contactModalErr" role="alert" aria-live="polite"></p>
        </div>
      </div>
    </div>

    <!-- Help Modal -->
    <div id="contactsHelpModal" class="overlay hidden">
      <div class="auth-card help-modal" style="width:min(640px, 94vw); max-height:90vh; overflow-y:auto;">
        <div class="help-modal-header">
          <h1>Contacts Help</h1>
          <button type="button" class="btn btn-icon" id="closeHelpBtn">
            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" width="20" height="20">
              <path d="M18 6L6 18M6 6l12 12"/>
            </svg>
          </button>
        </div>

        <div class="help-section">
          <h2>? VIP Contacts</h2>
          <p>VIP contacts are your most important customers. When a contact is marked as VIP:</p>
          <ul>
            <li>They appear with a star badge in your contact list</li>
            <li>You can quickly filter to see only VIPs</li>
            <li>Use this for repeat customers, high-value clients, or anyone you want to prioritize</li>
          </ul>
        </div>

        <div class="help-section">
          <h2>?? Do Not Reply (DNR)</h2>
          <p>DNR contacts will <strong>never</strong> receive automated messages from Relay. Use this for:</p>
          <ul>
            <li>Personal contacts (family, friends) who might call your business line</li>
            <li>Vendors or suppliers</li>
            <li>Anyone who has requested not to receive automated texts</li>
            <li>Wrong numbers or spam</li>
          </ul>
          <p class="help-note">?? DNR contacts can still receive manual messages you send yourself.</p>
        </div>

        <div class="help-section">
          <h2>?? Import Contacts</h2>
          <p>You can bulk import contacts from your phone. Here's how to export them:</p>

          <div class="help-platform">
            <h3>?? iPhone / iOS</h3>
            <p>iOS doesn't export directly from the Contacts app, but there's an easy workaround:</p>
            <ol>
              <li>Go to <a href="https://icloud.com" target="_blank" rel="noopener">iCloud.com</a> on a computer</li>
              <li>Sign in with your Apple ID</li>
              <li>Click <strong>Contacts</strong></li>
              <li>Press <kbd>Ctrl+A</kbd> (or <kbd>R+A</kbd> on Mac) to select all</li>
              <li>Click the gear icon ?? -> <strong>Export vCard</strong></li>
              <li>Upload the downloaded <code>.vcf</code> file here</li>
            </ol>
          </div>

          <div class="help-platform">
            <h3>?? Android</h3>
            <p>Much easier! Direct export from your phone:</p>
            <ol>
              <li>Open the <strong>Contacts</strong> app</li>
              <li>Tap <strong>Menu</strong> (three dots or lines)</li>
              <li>Go to <strong>Settings</strong> -> <strong>Import/Export</strong></li>
              <li>Choose <strong>Export to .VCF file</strong></li>
              <li>Save the file and upload it here</li>
            </ol>
          </div>

          <div class="help-note" style="margin-top:16px;">
            <strong>Ready to import?</strong> Close this help window and use the <strong>Import .vcf</strong> button at the top of the page.
          </div>
        </div>

        <div class="help-section">
          <h2>?? Quick Tips</h2>
          <ul>
            <li><strong>Search:</strong> Type any part of a name or phone number to filter</li>
            <li><strong>Filter buttons:</strong> Quickly view VIPs, DNRs, new leads, or booked customers</li>
            <li><strong>Notes:</strong> Add notes to any contact for context your team can see</li>
          </ul>
        </div>
      </div>
    </div>

    <div id="contactsImportInfoModal" class="overlay hidden" aria-hidden="true">
      <div class="auth-card help-modal" style="width:min(760px, 94vw); max-height:90vh; overflow-y:auto;">
        <div class="help-modal-header">
          <h1 style="margin:0;">Contact Settings</h1>
          <div class="row" style="gap:8px; margin-left:auto;">
            <button type="button" class="btn" id="contactsImportInfoCloseBtn">Close</button>
            <button type="button" class="btn primary" id="contactsImportInfoUploadBtn">Upload</button>
          </div>
          <button type="button" class="btn btn-icon" id="closeContactsImportInfoBtn" aria-label="Close">
            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" width="20" height="20">
              <path d="M18 6L6 18M6 6l12 12"/>
            </svg>
          </button>
          <input type="file" id="contactsImportInfoFileInput" accept=".vcf,text/vcard,text/x-vcard" class="hidden" />
        </div>
        <p class="p" style="margin-top:8px;">Import contacts and manage contact behavior guidelines for this workspace.</p>

        <div class="help-section">
          <h2 style="margin:0 0 8px;">VIP + DNR Guidelines</h2>
          <ul>
            <li>VIP contacts are priority customers shown with star badges in Contacts.</li>
            <li>DNR contacts never receive automated outbound messages.</li>
            <li>DNR still allows manual messages sent directly by your team.</li>
          </ul>
        </div>

        <div class="help-section">
          <h2 style="margin:0 0 8px;">Quick Tips</h2>
          <ul>
            <li>Use search and filters on Contacts to find VIP, DNR, New, and Booked leads quickly.</li>
            <li>Add notes to contacts for team context and handoff clarity.</li>
            <li>Imports are scoped to the active workspace To number shown in the top bar.</li>
          </ul>
        </div>

        <div class="help-section">
          <h2 style="margin:0 0 8px;">Import Contacts (.vcf)</h2>

          <div class="help-platform">
            <h3 style="margin:0 0 6px;">iPhone / iOS</h3>
            <ul>
              <li>Open iCloud.com on desktop and sign in.</li>
              <li>Open Contacts, select all, then use gear menu -> Export vCard.</li>
              <li>Upload the downloaded .vcf file here.</li>
            </ul>
          </div>

          <div class="help-platform">
            <h3 style="margin:0 0 6px;">Android</h3>
            <ul>
              <li>Open Contacts app and go to Menu/Settings.</li>
              <li>Use Import/Export and choose Export to .VCF.</li>
              <li>Upload the saved .vcf file here.</li>
            </ul>
          </div>
        </div>

        <div class="p" id="contactsImportInfoStatus" style="min-height:18px; margin-top:8px;"></div>
      </div>
    </div>
  `;

  wrap.appendChild(card);
  const listEl = card.querySelector("#contactList");
  const detailEl = card.querySelector("#contactDetail");
  const openConvoBtn = card.querySelector("#openConvoBtn");
  const contactSearchEl = card.querySelector("#contactSearch");
  const filterGroupEl = card.querySelector(".contacts-segment");

  // local view state (per render)
  const pageState = {
    contacts: [],
    filter: "all",
    q: "",
    selectedPhone: null
  };

  function normPhone(p){
    return String(p || "").trim().replace(/[^\d+]/g, "");
  }

  function matchesFilter(c){
    const f = pageState.filter;
    if (f === "all") return true;
    if (f === "vip") return !!c?.flags?.vip;
    if (f === "dnr") return !!c?.flags?.doNotAutoReply;
    if (f === "new") return (c?.lifecycle?.leadStatus || "new") === "new";
    if (f === "booked") return (c?.lifecycle?.leadStatus || "") === "booked";
    return true;
  }

  function matchesQuery(c){
    const q = (pageState.q || "").toLowerCase();
    if (!q) return true;
    const phone = String(c.phone || "").toLowerCase();
    const name = String(c.name || "").toLowerCase();
    return phone.includes(q) || name.includes(q);
  }

  function getVisibleContacts(){
    return (pageState.contacts || [])
      .filter(matchesFilter)
      .filter(matchesQuery)
      .sort((a,b) => (b?.lifecycle?.lastSeenAt || b?.updatedAt || 0) - (a?.lifecycle?.lastSeenAt || a?.updatedAt || 0));
  }

  function syncVisibleSelection(){
    const visible = getVisibleContacts();
    if (!pageState.selectedPhone) return;
    if (!visible.some((item) => item.phone === pageState.selectedPhone)) {
      pageState.selectedPhone = visible[0]?.phone || null;
      renderContactDetail();
    }
  }

  function contactStatusChipClass(status) {
    const value = String(status || "new").trim().toLowerCase();
    if (value === "booked" || value === "closed") return "ops-status-chip is-success";
    if (value === "contacted") return "ops-status-chip is-warning";
    return "ops-status-chip is-muted";
  }

  function renderContactList(){
    if (!listEl) return;

    const rows = getVisibleContacts();

    if (!rows.length){
      listEl.innerHTML = RelayUI.renderEmptyState({
        title: "No contacts found",
        centered: true,
        className: "is-compact",
        actionsHtml: '<button type="button" class="btn primary" id="contactEmptyImportBtn">Import Contacts</button>'
      });
      return;
    }

    listEl.innerHTML = `
      <div class="list">
        ${rows.map(c => {
          const phone = escapeHtml(c.phone || "");
          const name = escapeHtml(c.name || "");
          const vip = c?.flags?.vip ? " " : "";
          const status = escapeHtml(c?.lifecycle?.leadStatus || "new");
          const last = c?.lifecycle?.lastSeenAt || c?.updatedAt || c?.createdAt;
          const statusClass = contactStatusChipClass(c?.lifecycle?.leadStatus || "new");
          return `
            <div class="list-item contact-row ${pageState.selectedPhone === c.phone ? "active" : ""}" data-phone="${escapeHtml(c.phone)}" style="cursor:pointer;">
              <div class="list-left">
                <b>${name ? name : phone}${vip}</b>
                <span>${phone}</span>
              </div>
              <div class="contact-row-meta">
                <span class="${statusClass}">${status}</span>
                <span class="p">${last ? escapeHtml(formatTimeAgo(last)) : ""}</span>
              </div>
            </div>
          `;
        }).join("")}
      </div>
    `;
  }

  function renderContactDetail(){
    if (!detailEl) return;

    const phone = pageState.selectedPhone;
    const c = (pageState.contacts || []).find(x => x.phone === phone);

    if (!c){
      detailEl.innerHTML = RelayUI.renderEmptyState({
        text: "Select a contact to view details.",
        className: "is-compact"
      });
      if (openConvoBtn) openConvoBtn.disabled = true;
      return;
    }

    const name = escapeHtml(c.name || "");
    const vipChecked = c?.flags?.vip ? "checked" : "";
    const dnrChecked = c?.flags?.doNotAutoReply ? "checked" : "";
    const notes = escapeHtml(c?.summary?.notes || "");
    const status = escapeHtml(c?.lifecycle?.leadStatus || "new");
    const statusClass = contactStatusChipClass(c?.lifecycle?.leadStatus || "new");

    detailEl.innerHTML = `
      <div class="ops-detail-pane">
        <div class="ops-detail-card">
          <div class="row" style="justify-content:space-between; align-items:flex-start;">
            <div class="col" style="gap:4px;">
              <div class="h1" style="margin:0;">${name ? name : escapeHtml(c.phone)}</div>
              <div class="p">${escapeHtml(c.phone)}</div>
            </div>

            <div class="row" style="gap:14px;">
              <label class="toggle" style="justify-content:flex-start;">
                <input type="checkbox" id="contactVipToggle" ${vipChecked} />
                VIP
              </label>
              <label class="toggle" style="justify-content:flex-start;">
                <input type="checkbox" id="contactDnrToggle" ${dnrChecked} />
                do not auto-reply
              </label>
            </div>
          </div>

          <div class="ops-detail-section">
            <div class="grid2">
              <div class="kv">
                <label>Status</label>
                <div class="value">
                  <select class="select" id="contactStatusSelect">
                    <option value="new" ${status==="new"?"selected":""}>new</option>
                    <option value="contacted" ${status==="contacted"?"selected":""}>contacted</option>
                    <option value="booked" ${status==="booked"?"selected":""}>booked</option>
                    <option value="closed" ${status==="closed"?"selected":""}>closed</option>
                  </select>
                </div>
              </div>

              <div class="kv">
                <label>Name</label>
                <div class="value">
                  <input class="input" id="contactNameInput" value="${name}" placeholder="Name (optional)" />
                </div>
              </div>
            </div>

            <div class="row" style="justify-content:space-between; align-items:center; gap:10px; flex-wrap:wrap;">
              <span class="${statusClass}">${status}</span>
              <span class="p muted" style="margin:0;">Last active: ${escapeHtml(formatTimeAgo(c?.lifecycle?.lastSeenAt || c?.updatedAt || c?.createdAt || Date.now()))}</span>
            </div>
          </div>

          <div class="ops-detail-section">
            <div class="kv">
              <label>Notes</label>
              <div class="value">
                <textarea class="input" id="contactNotesInput" rows="6" placeholder="Notes...">${notes}</textarea>
              </div>
            </div>
          </div>

          <div class="ops-detail-actions">
            <button type="button" class="btn" id="saveContactEditsBtn">Save changes</button>
          </div>

          <p class="error" id="contactDetailErr" role="alert" aria-live="polite"></p>
        </div>
      </div>
    `;

    if (openConvoBtn) openConvoBtn.disabled = false;

    detailEl.querySelector("#saveContactEditsBtn")?.addEventListener("click", async () => {
      const errEl = detailEl.querySelector("#contactDetailErr");
      if (errEl) errEl.textContent = "";

      try{
        const next = {
          phone: c.phone,
          name: detailEl.querySelector("#contactNameInput")?.value?.trim() || "",
          flags: {
            vip: !!detailEl.querySelector("#contactVipToggle")?.checked,
            doNotAutoReply: !!detailEl.querySelector("#contactDnrToggle")?.checked
          },
          summary: {
            notes: detailEl.querySelector("#contactNotesInput")?.value || ""
          },
          lifecycle: {
            leadStatus: detailEl.querySelector("#contactStatusSelect")?.value || "new"
          }
        };

        const saved = await saveContactToBackend(next);
        if (!isUiScopeCurrent(contactsScope, { element: wrap, activeContactPhone: c.phone, getActiveContactPhone: () => pageState.selectedPhone })) return;
        pageState.contacts = (pageState.contacts || []).map((item) => item.phone === c.phone ? saved : item);
        renderContactList();
        renderContactDetail();
      }catch(e){
        if (errEl) errEl.textContent = e?.message || String(e);
      }
    });

    // Open conversation (best-effort: jump to messages + select thread by phone if exists)
    openConvoBtn?.addEventListener("click", async () => {
      state.view = "messages";
      await render();
      // optional: you can attempt to auto-select thread by phone here if your thread ids match
    });
  }

  function openModal(){
    const modal = card.querySelector("#contactModal");
    const errEl = card.querySelector("#contactModalErr");
    if (errEl) errEl.textContent = "";
    if (modal) modal.classList.remove("hidden");
  }

  function closeModal(){
    const modal = card.querySelector("#contactModal");
    if (modal) modal.classList.add("hidden");
  }

  // wire buttons
  card.querySelector("#addContactBtn")?.addEventListener("click", openModal);
  card.querySelector("#cancelContactBtn")?.addEventListener("click", closeModal);

  function openContactsImportInfoModal() {
    if (!contactsImportInfoModal) return;
    contactsImportInfoModal.classList.remove("hidden");
    contactsImportInfoModal.setAttribute("aria-hidden", "false");
  }
  card.querySelector("#importVcfBtn")?.addEventListener("click", openContactsImportInfoModal);
  card.querySelector("#contactsHelpBtn")?.addEventListener("click", openContactsImportInfoModal);
  const contactsImportInfoModal = card.querySelector("#contactsImportInfoModal");
  const contactsImportInfoStatus = card.querySelector("#contactsImportInfoStatus");
  const contactsImportInfoUploadBtn = card.querySelector("#contactsImportInfoUploadBtn");
  const contactsImportInfoFileInput = card.querySelector("#contactsImportInfoFileInput");
  const closeContactsImportInfo = () => {
    if (!contactsImportInfoModal) return;
    contactsImportInfoModal.classList.add("hidden");
    contactsImportInfoModal.setAttribute("aria-hidden", "true");
    if (contactsImportInfoStatus) contactsImportInfoStatus.textContent = "";
  };
  contactsImportInfoModal?.addEventListener("click", (event) => {
    if (event.target === contactsImportInfoModal) closeContactsImportInfo();
  });
  card.querySelector("#closeContactsImportInfoBtn")?.addEventListener("click", closeContactsImportInfo);
  card.querySelector("#contactsImportInfoCloseBtn")?.addEventListener("click", closeContactsImportInfo);
  contactsImportInfoUploadBtn?.addEventListener("click", () => {
    contactsImportInfoFileInput?.click();
  });
  contactsImportInfoFileInput?.addEventListener("change", async (event) => {
    const inputEl = event.target;
    const file = inputEl?.files?.[0];
    if (!file) return;
    try {
      await runGuardedButtonAction(contactsImportInfoUploadBtn, async () => {
        if (contactsImportInfoStatus) contactsImportInfoStatus.textContent = "Importing contacts...";
        const res = await importContactsFromVcfFile(file, getActiveTo());
        if (!isUiScopeCurrent(contactsScope, { element: wrap })) return;
        if (contactsImportInfoStatus) {
          contactsImportInfoStatus.textContent = `Imported ${res.imported} new, skipped ${res.skipped}, parsed ${res.parsedCount}.`;
        }
        const contacts = await loadContactsFromBackend({ force: true });
        if (!isUiScopeCurrent(contactsScope, { element: wrap })) return;
        pageState.contacts = contacts;
        renderContactList();
        syncVisibleSelection();
      }, { pendingText: "Uploading..." });
    } catch (err) {
      if (contactsImportInfoStatus) contactsImportInfoStatus.textContent = err?.message || "Failed to import contacts.";
    } finally {
      if (contactsImportInfoFileInput) contactsImportInfoFileInput.value = "";
    }
  });
  if (!window.__relayContactsEscHandler) {
    window.__relayContactsEscHandler = (event) => {
      if (event.key !== "Escape") return;
      document.getElementById("contactsImportInfoModal")?.classList.add("hidden");
      document.getElementById("contactsImportInfoModal")?.setAttribute("aria-hidden", "true");
      document.getElementById("contactsHelpModal")?.classList.add("hidden");
      document.getElementById("contactModal")?.classList.add("hidden");
    };
    document.addEventListener("keydown", window.__relayContactsEscHandler);
  }

  // Save contact happens HERE (async), NOT in viewContacts()
  card.querySelector("#saveContactBtn")?.addEventListener("click", async () => {
    const errEl = card.querySelector("#contactModalErr");
    if (errEl) errEl.textContent = "";

    const phoneRaw = card.querySelector("#newContactPhone")?.value || "";
    const name = card.querySelector("#newContactName")?.value || "";
    const vip = !!card.querySelector("#newContactVip")?.checked;
    const dnr = !!card.querySelector("#newContactDnr")?.checked;
    const notes = card.querySelector("#newContactNotes")?.value || "";

    const phone = normPhone(phoneRaw);
    if (!phone){
      if (errEl) errEl.textContent = "Phone is required.";
      return;
    }
    if (!String(name || "").trim()) {
      if (errEl) errEl.textContent = "Name is required before saving a contact.";
      return;
    }

    try{
      await runGuardedButtonAction(card.querySelector("#saveContactBtn"), async () => {
        const saved = await saveContactToBackend({
          phone,
          name: name.trim(),
          flags: { vip, doNotAutoReply: dnr },
          summary: { notes },
          lifecycle: { leadStatus: "new" }
        });
        if (!isUiScopeCurrent(contactsScope, { element: wrap })) return;
        closeModal();
        pageState.contacts = [saved, ...(pageState.contacts || []).filter((item) => item.phone !== saved.phone)];
        pageState.selectedPhone = saved.phone;
        renderContactList();
        renderContactDetail();
      }, { pendingText: "Saving..." });
    }catch(e){
      if (errEl) errEl.textContent = e?.message || String(e);
    }
  });

  listEl?.addEventListener("click", (event) => {
    const importBtn = event.target.closest("#contactEmptyImportBtn");
    if (importBtn && listEl.contains(importBtn)) {
      openContactsImportInfoModal();
      return;
    }
    const row = event.target.closest(".contact-row[data-phone]");
    if (!row || !listEl.contains(row)) return;
    pageState.selectedPhone = row.dataset.phone;
    renderContactList();
    renderContactDetail();
  });

  filterGroupEl?.addEventListener("click", (event) => {
    const btn = event.target.closest("button[data-cfilter]");
    if (!btn || !filterGroupEl.contains(btn)) return;
    filterGroupEl.querySelectorAll("button[data-cfilter]").forEach((node) => node.classList.remove("is-active"));
    btn.classList.add("is-active");
    pageState.filter = btn.dataset.cfilter || "all";
    renderContactList();
    syncVisibleSelection();
  });

  const debouncedContactSearch = debounce((value) => {
    pageState.q = String(value || "").trim();
    renderContactList();
    syncVisibleSelection();
  }, 90);
  contactSearchEl?.addEventListener("input", (e) => {
    debouncedContactSearch(e.target?.value || "");
  });

  // initial load (async) AFTER render
  setTimeout(async () => {
    try{
      const contacts = await loadContactsFromBackend();
      if (!isUiScopeCurrent(contactsScope, { element: wrap })) return;
      pageState.contacts = contacts;
      renderContactList();
      syncVisibleSelection();
    }catch(e){
      if (!isUiScopeCurrent(contactsScope, { element: wrap })) return;
      const listEl = card.querySelector("#contactList");
      if (listEl) listEl.innerHTML = `<div class="p">Failed to load contacts. Is backend running?</div><pre>${escapeHtml(e?.message || String(e))}</pre>`;
    }
  }, 0);

  return wrap;
}




function renderLeadDetails(){
  const box = document.getElementById("leadDetails");
  const sub = document.getElementById("leadSub");
  if (!box || !sub) return;

  const convo = state.activeConversation;

  if (!convo){
    sub.textContent = "";
    box.innerHTML = `
      ${RelayUI.renderEmptyState({
        title: "No lead selected",
        text: "Select a conversation to review lead details, recovery status, and compliance actions.",
        className: "messages-empty-state lead-empty-state"
      })}
      <div class="row lead-compliance-row">
        <button type="button" class="btn" data-open-lead-compliance="1">Compliance</button>
      </div>
    `;
    box.querySelector('[data-open-lead-compliance="1"]')?.addEventListener("click", () => {
      openLeadComplianceModal();
    });
    return;
  }

  const ld = convo.leadData || {};
  const servicesFromLead = getServiceLabelsFromLeadData(ld);
  const services = servicesFromLead.length ? servicesFromLead : inferServiceLabelsFromMessages(convo.messages || []);
  const vehicle = resolveLeadVehicleText(ld, convo);
  const request = String(ld.request || ld.issue || ld.service_required || "").trim() || (services.length ? services.join(" + ") : "");
  const availability = resolveLeadAvailabilityText(ld, convo);

  const currentStatus = String(convo.status || "new").toLowerCase();
  const pretty = (value) => {
    const raw = String(value || "").trim();
    if (!raw) return "--";
    return raw
      .replace(/[_-]+/g, " ")
      .replace(/\s+/g, " ")
      .replace(/\b\w/g, (m) => m.toUpperCase());
  };

  sub.textContent = String(convo.from || "--");

  const badges = [];
  if (ld.intent) badges.push(`Intent: ${pretty(ld.intent)}`);
  if (ld.drivable === true) badges.push("Drivable: YES");
  if (ld.drivable === false) badges.push("Drivable: NO");

  const stageRaw = String(convo.stage || "").toLowerCase();
  const hasBookedAudit = Array.isArray(convo.audit) && convo.audit.some((a) => {
    const t = String(a?.type || "").toLowerCase();
    const statusMeta = String(a?.meta?.status || a?.meta?.newStatus || "").toLowerCase();
    return (t === "status_change" || t === "status_changed") && statusMeta === "booked";
  });
  const hasBookingLeadData = Boolean(
    String(ld?.booking_id || "").trim()
    || (Number.isFinite(Number(ld?.booking_time || 0)) && Number(ld?.booking_time) > 0)
  );
  const hasBookedMessage = Array.isArray(convo.messages) && convo.messages.some((m) => {
    const bookingConfirmed = Boolean(m?.meta?.bookingConfirmed || m?.payload?.bookingConfirmed);
    const bookingTime = Number(m?.meta?.bookingTime || m?.payload?.bookingTime || m?.bookingTime || 0);
    return bookingConfirmed || (Number.isFinite(bookingTime) && bookingTime > 0);
  });
  const bookingInProgress = Boolean(
    ld?.booking_link_sent === true
    || ld?.booking_url_sent === true
    || /booking|book_link|send_booking|schedule|slot|time/.test(stageRaw)
  );
  const bookedOrScheduled = currentStatus === "booked"
    || /booked|scheduled|appointment_booked|confirm/.test(stageRaw)
    || hasBookedAudit
    || hasBookingLeadData
    || hasBookedMessage
    || (Number.isFinite(Number(convo?.bookingTime || 0)) && Number(convo.bookingTime) > 0);
  const closed = currentStatus === "closed";
  const contacted = currentStatus === "contacted" || (!closed && !bookedOrScheduled && !bookingInProgress && stageRaw && stageRaw !== "ask_service");
  const journeyProgressPct = closed
    ? 100
    : bookedOrScheduled
      ? 66
      : 12;
  const journeyStep = closed ? 3 : bookedOrScheduled ? 2 : 1;
  const journeyLabel = closed
    ? "Closed"
    : bookedOrScheduled
      ? "Scheduled / Booked"
      : bookingInProgress
        ? "Booking in progress"
        : contacted
          ? "Conversation active"
          : "New lead";
  const safeVehicle = vehicle ? escapeHtml(vehicle) : "Not provided";
  const safeRequest = request ? escapeHtml(request) : "Not provided";
  const safeAvailability = availability ? escapeHtml(availability) : "Not provided";
  const adminUnlocked = isAdminUnlockedForActiveAccount();
  const recoveredTotal = resolveRecoveredTotalForConversation(convo);
  const recoveredLabel = formatUsdAmount(recoveredTotal);
  if (adminUnlocked) {
    ensureRecoveredTotalForConversation(convo);
  }

  box.innerHTML = `
    <div class="lead-progress-wrap">
      <div class="lead-progress-head">
        <span>Conversation Journey</span>
        <strong>${escapeHtml(journeyLabel)}</strong>
      </div>
      <div class="lead-progress-track">
        <div class="lead-progress-fill" style="width:${journeyProgressPct}%;"></div>
      </div>
      <div class="lead-progress-steps">
        ${[
          { id: 1, label: "New" },
          { id: 2, label: "Booked" },
          { id: 3, label: "Closed" }
        ].map((step) => `
          <span class="lead-progress-step ${journeyStep >= step.id ? "is-active" : ""} ${journeyStep === step.id ? "is-current" : ""}">${escapeHtml(step.label)}</span>
        `).join("")}
      </div>
    </div>

    ${badges.length ? `
    <div class="badges">
      ${badges.map(b => `<span class="badge-green"><span class="badge-dot"></span>${escapeHtml(b)}</span>`).join("")}
    </div>
    ` : ""}

    <div class="lead-section">
      <div class="lead-section-title">Customer Request</div>
      <div class="lead-field">
        <label>Vehicle</label>
        <div class="value">${safeVehicle}</div>
      </div>
      <div class="lead-field">
        <label>Request</label>
        <div class="value">${safeRequest}</div>
      </div>
      <div class="lead-field">
        <label>Availability</label>
        <div class="value">${safeAvailability}</div>
      </div>
    </div>

    <div class="lead-section">
      <div class="lead-section-title">Services</div>
      <div class="value">${services.length ? renderServiceChipsHtml(services) : "No services captured yet."}</div>
    </div>

    <div class="lead-section">
      <div class="lead-section-title">Recovered Amount</div>
      <div class="value">
        ${adminUnlocked
          ? `<div>${escapeHtml(recoveredLabel)}</div><div class="p muted" style="margin-top:6px;">All-time recovered for this customer</div>`
          : `<button type="button" class="lead-lock-btn" data-lead-admin-unlock="1" aria-label="Unlock admin to view recovered amount" title="Unlock admin to view recovered amount">??</button>`
        }
      </div>
    </div>

    <div class="row lead-compliance-row">
      <button type="button" class="btn" data-open-lead-compliance="1">Compliance</button>
    </div>
  `;

  const unlockBtn = box.querySelector('[data-lead-admin-unlock="1"]');
  if (unlockBtn) {
    unlockBtn.addEventListener("click", () => {
      openAdminUnlockFromLeadDetails();
    });
  }
  box.querySelector('[data-open-lead-compliance="1"]')?.addEventListener("click", () => {
    openLeadComplianceModal();
  });

}

let leadComplianceBaseline = "";
const leadComplianceFieldIds = [
  "leadCmpStopEnabled", "leadCmpStopAutoReply", "leadCmpStopKeywords", "leadCmpHelpKeywords", "leadCmpStopAutoReplyText",
  "leadCmpOptOutEnforce", "leadCmpAllowTransactional", "leadCmpStoreAsTag", "leadCmpResubKeywords",
  "leadCmpConsentRequired", "leadCmpConsentCheckboxText", "leadCmpConsentSourceOptions",
  "leadCmpRetentionEnabled", "leadCmpRetentionSchedule", "leadCmpRetentionDays"
];

function parseLeadComplianceCsvList(value) {
  return String(value || "")
    .split(",")
    .map((part) => part.trim())
    .filter(Boolean);
}

function readLeadCompliancePatchFromUI() {
  const getChecked = (id) => document.getElementById(id)?.checked === true;
  const getValue = (id) => (document.getElementById(id)?.value || "").trim();
  const sourceOpts = parseLeadComplianceCsvList(getValue("leadCmpConsentSourceOptions"));
  const stopKeywords = parseLeadComplianceCsvList(getValue("leadCmpStopKeywords"));
  const helpKeywords = parseLeadComplianceCsvList(getValue("leadCmpHelpKeywords"));
  const resubKeywords = parseLeadComplianceCsvList(getValue("leadCmpResubKeywords"));

  return {
    stopKeywords: stopKeywords.length ? stopKeywords : ["STOP", "UNSUBSCRIBE", "CANCEL", "END", "QUIT"],
    helpKeywords: helpKeywords.length ? helpKeywords : ["HELP", "INFO"],
    stopBehavior: {
      enabled: getChecked("leadCmpStopEnabled"),
      autoReply: getChecked("leadCmpStopAutoReply"),
      autoReplyText: getValue("leadCmpStopAutoReplyText")
    },
    optOut: {
      enforce: getChecked("leadCmpOptOutEnforce"),
      allowTransactional: getChecked("leadCmpAllowTransactional"),
      storeAsTag: getValue("leadCmpStoreAsTag") || "DNR",
      resubscribeKeywords: resubKeywords.length ? resubKeywords : ["START", "UNSTOP", "YES"]
    },
    consent: {
      requireForOutbound: getChecked("leadCmpConsentRequired"),
      consentCheckboxText: getValue("leadCmpConsentCheckboxText"),
      consentSourceOptions: sourceOpts.length ? sourceOpts : ["verbal", "form", "existing_customer", "other"]
    },
    retention: {
      enabled: getChecked("leadCmpRetentionEnabled"),
      purgeOnSchedule: getChecked("leadCmpRetentionSchedule"),
      messageLogDays: Number(getValue("leadCmpRetentionDays") || 90)
    }
  };
}

function renderLeadComplianceStatusCard(comp) {
  const statusCard = document.getElementById("leadCmpStatusCard");
  if (!statusCard) return;
  const enforce = comp?.optOut?.enforce ? "Enabled" : "Disabled";
  const days = comp?.retention?.messageLogDays ?? "";
  const lastPurge = comp?.retention?.lastPurgeAt
    ? new Date(comp.retention.lastPurgeAt).toLocaleString()
    : "Never";
  statusCard.innerHTML = `
    <div class="p">Outbound enforcement: <b>${escapeHtml(enforce)}</b></div>
    <div class="p">Retention days: <b>${escapeHtml(String(days))}</b></div>
    <div class="p">Last purge: <b>${escapeHtml(lastPurge)}</b></div>
  `;
}

function syncLeadComplianceDirtyState() {
  const saveBtn = document.getElementById("leadCmpSaveBtn");
  if (!saveBtn) return;
  const dirty = JSON.stringify(readLeadCompliancePatchFromUI()) !== leadComplianceBaseline;
  saveBtn.setAttribute("data-dirty", dirty ? "1" : "0");
}

async function loadLeadComplianceUI() {
  const to = getActiveTo();
  const acct = await loadAccountSettings(to);
  const comp = acct?.compliance || {};
  state.activeCompliance = comp;

  const setChecked = (id, val) => {
    const el = document.getElementById(id);
    if (el) el.checked = !!val;
  };
  const setValue = (id, val) => {
    const el = document.getElementById(id);
    if (el) el.value = val ?? "";
  };

  setChecked("leadCmpStopEnabled", comp?.stopBehavior?.enabled);
  setChecked("leadCmpStopAutoReply", comp?.stopBehavior?.autoReply);
  setValue("leadCmpStopKeywords", (comp?.stopKeywords || []).join(","));
  setValue("leadCmpHelpKeywords", (comp?.helpKeywords || []).join(","));
  setValue("leadCmpStopAutoReplyText", comp?.stopBehavior?.autoReplyText || "");
  setChecked("leadCmpOptOutEnforce", comp?.optOut?.enforce);
  setChecked("leadCmpAllowTransactional", comp?.optOut?.allowTransactional);
  setValue("leadCmpStoreAsTag", comp?.optOut?.storeAsTag || "DNR");
  setValue("leadCmpResubKeywords", (comp?.optOut?.resubscribeKeywords || []).join(","));
  setChecked("leadCmpConsentRequired", comp?.consent?.requireForOutbound);
  setValue("leadCmpConsentCheckboxText", comp?.consent?.consentCheckboxText || "");
  setValue("leadCmpConsentSourceOptions", (comp?.consent?.consentSourceOptions || []).join(","));
  setChecked("leadCmpRetentionEnabled", comp?.retention?.enabled);
  setChecked("leadCmpRetentionSchedule", comp?.retention?.purgeOnSchedule);
  setValue("leadCmpRetentionDays", comp?.retention?.messageLogDays || 90);
  renderLeadComplianceStatusCard(comp);
  leadComplianceBaseline = JSON.stringify(readLeadCompliancePatchFromUI());
  syncLeadComplianceDirtyState();
}

function closeLeadComplianceModal() {
  const modal = document.getElementById("leadComplianceModal");
  if (!modal) return;
  modal.classList.add("hidden");
  modal.setAttribute("aria-hidden", "true");
  const status = document.getElementById("leadCmpSaveStatus");
  if (status) status.textContent = "";
}

async function openLeadComplianceModal() {
  const modal = document.getElementById("leadComplianceModal");
  const status = document.getElementById("leadCmpSaveStatus");
  if (!modal) return;
  initLeadComplianceModal();
  if (status) status.textContent = "";
  modal.classList.remove("hidden");
  modal.setAttribute("aria-hidden", "false");
  try {
    await loadLeadComplianceUI();
  } catch (err) {
    if (status) status.textContent = "Failed to load compliance.";
    console.error("Compliance modal load failed:", err);
  }
}

function initLeadComplianceModal() {
  const modal = document.getElementById("leadComplianceModal");
  if (!modal || modal.dataset.bound === "1") return;

  modal.dataset.bound = "1";
  const closeBtn = document.getElementById("leadComplianceCloseBtn");
  const saveBtn = document.getElementById("leadCmpSaveBtn");
  const purgeBtn = document.getElementById("leadCmpRunPurgeNowBtn");

  closeBtn?.addEventListener("click", closeLeadComplianceModal);
  modal.addEventListener("click", (event) => {
    if (event.target === modal) closeLeadComplianceModal();
  });
  if (!window.__relayLeadComplianceEscBound) {
    window.__relayLeadComplianceEscBound = true;
    document.addEventListener("keydown", (event) => {
      if (event.key !== "Escape") return;
      const activeModal = document.getElementById("leadComplianceModal");
      if (!activeModal || activeModal.classList.contains("hidden")) return;
      closeLeadComplianceModal();
    });
  }

  leadComplianceFieldIds.forEach((id) => {
    const el = document.getElementById(id);
    if (!el) return;
    el.addEventListener("input", syncLeadComplianceDirtyState);
    el.addEventListener("change", syncLeadComplianceDirtyState);
  });

  saveBtn?.addEventListener("click", async () => {
    const to = getActiveTo();
    const status = document.getElementById("leadCmpSaveStatus");
    if (status) status.textContent = "";
    const patch = readLeadCompliancePatchFromUI();
    try {
      const res = await apiPatch(`/api/account/compliance?to=${encodeURIComponent(to)}`, patch);
      state.activeCompliance = res?.compliance || patch;
      if (status) status.textContent = "Saved";
      renderLeadComplianceStatusCard(state.activeCompliance);
      leadComplianceBaseline = JSON.stringify(readLeadCompliancePatchFromUI());
      syncLeadComplianceDirtyState();
      await refreshComposeComplianceUI(state.activeConversation);
    } catch (err) {
      if (status) status.textContent = "Failed to save.";
      console.error("Compliance modal save failed:", err);
    }
  });

  purgeBtn?.addEventListener("click", async () => {
    const to = getActiveTo();
    const status = document.getElementById("leadCmpSaveStatus");
    if (status) status.textContent = "";
    try {
      await apiPost(`/api/account/compliance/purge-now?to=${encodeURIComponent(to)}`, {});
      await loadLeadComplianceUI();
      if (status) status.textContent = "Purge complete";
      await refreshComposeComplianceUI(state.activeConversation);
    } catch (err) {
      if (status) status.textContent = "Purge failed.";
      console.error("Compliance modal purge failed:", err);
    }
  });
}

function openAdminUnlockFromLeadDetails() {
  localStorage.setItem("mc_settings_tab_v1", "admin");
  state.view = "settings";
  render();
  setTimeout(() => {
    document.querySelector('[data-settings-tab="admin"]')?.click();
  }, 0);
}

function getAdminUnlockStorageKeyForActiveAccount() {
  const active = resolveActiveWorkspace();
  const accountId = String(active?.accountId || "").trim();
  return accountId ? `mc_admin_unlock_v1:${accountId}` : "";
}

function isAdminUnlockedForActiveAccount() {
  const key = getAdminUnlockStorageKeyForActiveAccount();
  return Boolean(key && sessionStorage.getItem(key) === "1");
}

function formatUsdAmount(value) {
  const n = Number(value || 0);
  const safe = Number.isFinite(n) && n > 0 ? n : 0;
  return new Intl.NumberFormat("en-US", {
    style: "currency",
    currency: "USD",
    maximumFractionDigits: 2
  }).format(safe);
}

function resolveRecoveredTotalForConversation(convo) {
  const key = customerRecoveredCacheKey(convo?.from || "");
  if (!key) return 0;
  const val = Number(state.recoveredTotalsByCustomer?.[key] || 0);
  return Number.isFinite(val) && val > 0 ? val : 0;
}

function customerRecoveredCacheKey(fromValue) {
  const active = resolveActiveWorkspace();
  const accountId = String(active?.accountId || "").trim();
  const from = normalizePhone(String(fromValue || "").trim());
  if (!accountId || !from) return "";
  return `${accountId}__${from}`;
}

function revenueLedgerScopeKeys() {
  const active = resolveActiveWorkspace();
  const accountId = String(active?.accountId || "").trim();
  const to = String(active?.to || getActiveTo() || "").trim();
  const keys = [];
  if (accountId) keys.push(accountId);
  if (to && !keys.includes(to)) keys.push(to);
  if (!keys.length) keys.push("default");
  return keys;
}

function revenueLedgerStorageKeys() {
  return revenueLedgerScopeKeys().map((scope) => `mc_revenue_ledger_v1:${scope}`);
}

function loadRevenueLedgerRows() {
  const keys = revenueLedgerStorageKeys();
  const merged = new Map();
  keys.forEach((key) => {
    const rows = loadLS(key, []);
    if (!Array.isArray(rows)) return;
    rows.forEach((row) => {
      const id = String(row?.id || "").trim();
      if (!id) return;
      const prev = merged.get(id);
      if (!prev || Number(row?.updatedAt || 0) >= Number(prev?.updatedAt || 0)) {
        merged.set(id, row);
      }
    });
  });
  return Array.from(merged.values());
}

function saveRevenueLedgerRows(rows) {
  const safe = Array.isArray(rows) ? rows : [];
  revenueLedgerStorageKeys().forEach((key) => saveLS(key, safe));
}

function upsertRevenueLedgerRow({ threadId, customer, bookedMs, amountCents }) {
  const id = `${String(threadId || "").trim()}:${Number(bookedMs || 0)}`;
  if (!id || !Number.isFinite(Number(bookedMs || 0)) || Number(bookedMs || 0) <= 0) return;
  const cents = Number(amountCents || 0);
  if (!Number.isFinite(cents) || cents <= 0) return;
  const rows = loadRevenueLedgerRows();
  const idx = rows.findIndex((r) => String(r?.id || "") === id);
  const row = {
    id,
    threadId: String(threadId || "").trim(),
    customer: String(customer || "").trim().toLowerCase(),
    bookedMs: Number(bookedMs),
    amountCents: Math.round(cents),
    updatedAt: Date.now()
  };
  if (idx >= 0) rows[idx] = { ...(rows[idx] || {}), ...row };
  else rows.push(row);
  saveRevenueLedgerRows(rows);
}

function removeRevenueLedgerRows({ threadId = "", bookedMs = 0 } = {}) {
  const targetThreadId = String(threadId || "").trim();
  const targetBookedMs = Number(bookedMs || 0);
  if (!targetThreadId && !(Number.isFinite(targetBookedMs) && targetBookedMs > 0)) return;
  const rows = loadRevenueLedgerRows();
  const next = rows.filter((row) => {
    const sameThread = targetThreadId ? String(row?.threadId || "").trim() === targetThreadId : false;
    const sameBooked = Number.isFinite(targetBookedMs) && targetBookedMs > 0
      ? Number(row?.bookedMs || 0) === targetBookedMs
      : false;
    if (targetThreadId && Number.isFinite(targetBookedMs) && targetBookedMs > 0) {
      return !(sameThread && sameBooked);
    }
    if (targetThreadId) return !sameThread;
    return !sameBooked;
  });
  saveRevenueLedgerRows(next);
}

function rebuildRevenueLedgerFromThreads(threadsInput) {
  const threads = Array.isArray(threadsInput) ? threadsInput : [];
  const rows = [];
  const now = Date.now();
  for (const thread of threads) {
    if (isSimulatedConversationLike(thread)) continue;
    const status = String(thread?.status || "").toLowerCase();
    const stage = String(thread?.stage || "").toLowerCase();
    const ld = thread?.leadData || {};
    const snippet = String(thread?.lastText || "").toLowerCase();
    const bookingMs = coerceTimestampMs(
      thread?.bookingTime || ld?.booking_time || ld?.bookingTime || thread?.updatedAt || thread?.lastActivityAt,
      0
    );
    const bookedLike = status === "booked"
      || status === "closed"
      || /booked|appointment_booked|scheduled/.test(stage)
      || /booked\\s*:|i see you booked|appointment booked|scheduled/.test(snippet)
      || (Number.isFinite(bookingMs) && bookingMs > 0);
    if (!bookedLike || !(Number.isFinite(bookingMs) && bookingMs > 0)) continue;
    const pill = buildConversationMoneyPill(thread);
    const amountCents = parseMoneyLabelToCents(pill?.label);
    if (!(Number.isFinite(amountCents) && amountCents > 0)) continue;
    const threadId = String(thread?.id || "").trim();
    if (!threadId) continue;
    rows.push({
      id: `${threadId}:${bookingMs}`,
      threadId,
      customer: String(thread?.from || thread?.phone || "").trim().toLowerCase(),
      bookedMs: Number(bookingMs),
      amountCents: Math.round(amountCents),
      updatedAt: now
    });
  }
  saveRevenueLedgerRows(rows);
}

function parseBookedTextToMs(text, fallbackTs = 0) {
  const raw = String(text || "").trim();
  if (!/^booked\s*:/i.test(raw)) return 0;
  const label = raw.replace(/^booked\s*:\s*/i, "").trim();
  if (!label) return Number.isFinite(Number(fallbackTs || 0)) ? Number(fallbackTs) : 0;
  let parsed = Date.parse(label);
  if (Number.isNaN(parsed)) {
    const year = new Date().getFullYear();
    parsed = Date.parse(`${label}, ${year}`);
  }
  if (!Number.isNaN(parsed) && parsed > 0) return parsed;
  const fb = Number(fallbackTs || 0);
  return Number.isFinite(fb) && fb > 0 ? fb : 0;
}

function coerceTimestampMs(value, fallback = 0) {
  if (typeof value === "number" && Number.isFinite(value) && value > 0) return value;
  if (typeof value === "string") {
    const trimmed = value.trim();
    if (trimmed) {
      const n = Number(trimmed);
      if (Number.isFinite(n) && n > 0) return n;
      const parsed = Date.parse(trimmed);
      if (!Number.isNaN(parsed) && parsed > 0) return parsed;
    }
  }
  const fb = Number(fallback || 0);
  return Number.isFinite(fb) && fb > 0 ? fb : 0;
}

function simulatedConversationsStorageKey(to = getActiveTo()) {
  return `${SIMULATED_CONVERSATIONS_STORAGE_PREFIX}:${String(to || SIMULATED_CONVERSATIONS_GLOBAL_SCOPE).trim()}`;
}

function isSimulatedConversationLike(item) {
  if (!item || typeof item !== "object") return false;
  if (item.isSimulated === true) return true;
  if (String(item.source || "").toLowerCase() === "simulated") return true;
  if (String(item.status || "").toLowerCase() === "simulated") return true;
  if (String(item.stage || "").toLowerCase() === "simulated") return true;
  return /^sim-/i.test(String(item.id || ""));
}

function sanitizeSimulatedMessages(messages) {
  return (Array.isArray(messages) ? messages : []).map((message) => {
    const next = {
      ...message,
      status: "simulated",
      providerMeta: { ...(message?.providerMeta || {}), provider: "simulator" }
    };
    const meta = { ...(message?.meta || {}) };
    const payload = { ...(message?.payload || {}) };
    delete meta.bookingConfirmed;
    delete meta.bookingTime;
    delete meta.booking_time;
    delete payload.bookingConfirmed;
    delete payload.bookingTime;
    delete payload.booking_time;
    delete next.bookingTime;
    delete next.booking_time;
    next.meta = { ...meta, simulated: true };
    next.payload = { ...payload, simulated: true };
    return next;
  });
}

function normalizeSimulatedConversation(item) {
  if (!item || typeof item !== "object") return null;
  const id = String(item.id || "").trim();
  if (!id) return null;
  const messages = sanitizeSimulatedMessages(item.messages);
  const leadData = item.leadData && typeof item.leadData === "object" ? { ...item.leadData } : {};
  delete leadData.booking_time;
  delete leadData.bookingTime;
  delete leadData.payment_status;
  delete leadData.paymentStatus;
  return {
    ...item,
    id,
    source: "simulated",
    isSimulated: true,
    to: String(item.to || "").trim(),
    stage: "simulated",
    status: "simulated",
    bookingTime: null,
    bookedAt: null,
    paymentStatus: null,
    payment_status: null,
    leadData,
    messages,
    lastText: String(item.lastText || messages[messages.length - 1]?.text || messages[messages.length - 1]?.body || "Simulated conversation"),
    updatedAt: Number(item.updatedAt || Date.now()),
    lastActivityAt: Number(item.lastActivityAt || item.updatedAt || Date.now())
  };
}

function loadSimulatedRowsFromStorage(storage, key) {
  try {
    const raw = storage?.getItem?.(key);
    if (!raw) return [];
    const rows = JSON.parse(raw);
    return Array.isArray(rows) ? rows : [];
  } catch {
    return [];
  }
}

function saveSimulatedRowsToStorage(storage, key, rows) {
  try {
    storage?.setItem?.(key, JSON.stringify(rows));
  } catch {}
}

function dedupeSimulatedConversations(rows, to = getActiveTo(), options = {}) {
  const scopedTo = String(to || "").trim();
  const requireScopeMatch = options.requireScopeMatch !== false;
  const out = [];
  const seen = new Set();
  for (const row of rows) {
    const normalized = normalizeSimulatedConversation(row);
    if (!normalized) continue;
    if (requireScopeMatch && normalized.to && scopedTo && normalized.to !== scopedTo) continue;
    if (seen.has(normalized.id)) continue;
    seen.add(normalized.id);
    out.push(normalized);
  }
  return out;
}

function loadSimulatedConversations(to = getActiveTo()) {
  const scopedKey = simulatedConversationsStorageKey(to);
  const globalKey = simulatedConversationsStorageKey(SIMULATED_CONVERSATIONS_GLOBAL_SCOPE);
  const rows = [
    ...loadSimulatedRowsFromStorage(localStorage, scopedKey),
    ...loadSimulatedRowsFromStorage(localStorage, globalKey),
    ...loadSimulatedRowsFromStorage(sessionStorage, scopedKey),
    ...loadSimulatedRowsFromStorage(sessionStorage, globalKey)
  ];
  const scoped = dedupeSimulatedConversations(rows, to, { requireScopeMatch: true });
  return scoped.length ? scoped : dedupeSimulatedConversations(rows, to, { requireScopeMatch: false });
}

function saveSimulatedConversation(to, conversation) {
  const normalized = normalizeSimulatedConversation({ ...conversation, to: String(to || "").trim() });
  if (!normalized) return null;
  const existing = dedupeSimulatedConversations([
    ...loadSimulatedRowsFromStorage(localStorage, simulatedConversationsStorageKey(to)),
    ...loadSimulatedRowsFromStorage(sessionStorage, simulatedConversationsStorageKey(to)),
    ...loadSimulatedRowsFromStorage(localStorage, simulatedConversationsStorageKey(SIMULATED_CONVERSATIONS_GLOBAL_SCOPE)),
    ...loadSimulatedRowsFromStorage(sessionStorage, simulatedConversationsStorageKey(SIMULATED_CONVERSATIONS_GLOBAL_SCOPE))
  ], to);
  const next = [normalized, ...existing.filter((row) => String(row?.id || "") !== normalized.id)].slice(0, 25);
  for (const key of [simulatedConversationsStorageKey(to), simulatedConversationsStorageKey(SIMULATED_CONVERSATIONS_GLOBAL_SCOPE)]) {
    saveSimulatedRowsToStorage(localStorage, key, next);
    saveSimulatedRowsToStorage(sessionStorage, key, next);
  }
  return normalized;
}

function findSimulatedConversation(convoId, to = getActiveTo()) {
  const id = String(convoId || "").trim();
  if (!id) return null;
  return loadSimulatedConversations(to).find((row) => String(row?.id || "") === id) || null;
}

function mergeSimulatedConversations(threads, to = getActiveTo()) {
  const base = Array.isArray(threads) ? threads : [];
  const simulated = loadSimulatedConversations(to);
  if (!simulated.length) return base;
  const simulatedIds = new Set(simulated.map((row) => String(row?.id || "")));
  return [...simulated, ...base.filter((row) => !simulatedIds.has(String(row?.id || "")))];
}

async function ensureRecoveredTotalForConversation(convo, options = {}) {
  const force = options?.force === true;
  const from = String(convo?.from || "").trim();
  const key = customerRecoveredCacheKey(from);
  if (!key) return;
  if (!force && state.recoveredTotalsLoadingByCustomer?.[key]) return;
  if (!force && Object.prototype.hasOwnProperty.call(state.recoveredTotalsByCustomer || {}, key)) return;

  state.recoveredTotalsLoadingByCustomer[key] = true;
  try {
    const resp = await apiGet(`/api/analytics/customer-recovered?from=${encodeURIComponent(from)}`);
    const total = Number(resp?.totalRecovered || 0);
    state.recoveredTotalsByCustomer[key] = Number.isFinite(total) && total > 0 ? total : 0;
  } catch (err) {
    console.warn("Failed to load customer recovered total:", err?.message || err);
    if (!Object.prototype.hasOwnProperty.call(state.recoveredTotalsByCustomer || {}, key)) {
      state.recoveredTotalsByCustomer[key] = 0;
    }
  } finally {
    state.recoveredTotalsLoadingByCustomer[key] = false;
    if (state.activeConversation?.id && state.activeConversation.id === convo?.id) {
      renderLeadDetails();
    }
  }
}

function getLatestBookedConfirmationTime(convo) {
  const messages = Array.isArray(convo?.messages) ? convo.messages : [];
  let latest = Number(convo?.bookingTime || 0);
  for (const m of messages) {
    const confirmed = Boolean(m?.meta?.bookingConfirmed || m?.payload?.bookingConfirmed);
    const bookingMs = Number(m?.meta?.bookingTime || m?.payload?.bookingTime || m?.bookingTime || 0);
    if (confirmed && Number.isFinite(bookingMs) && bookingMs > 0) {
      if (!Number.isFinite(latest) || bookingMs > latest) latest = bookingMs;
    }
    const bookedFromText = parseBookedTextToMs(m?.text || m?.body || "", messageTimestampMs(m));
    if (Number.isFinite(bookedFromText) && bookedFromText > 0) {
      if (!Number.isFinite(latest) || bookedFromText > latest) latest = bookedFromText;
    }
  }
  return Number.isFinite(latest) && latest > 0 ? latest : 0;
}

function applyRealtimeBookedState(convo, options = {}) {
  if (!convo || typeof convo !== "object") return false;
  if (isSimulatedConversationLike(convo)) return false;
  const refreshRecovered = options?.refreshRecovered === true;
  const bookedMs = getLatestBookedConfirmationTime(convo);
  if (!Number.isFinite(bookedMs) || bookedMs <= 0) return false;

  let changed = false;
  if (String(convo.status || "").toLowerCase() !== "booked") {
    convo.status = "booked";
    changed = true;
  }
  if (!/booked|appointment_booked|scheduled/.test(String(convo.stage || "").toLowerCase())) {
    convo.stage = "booked";
    changed = true;
  }
  if (!Number.isFinite(Number(convo.bookingTime || 0)) || Number(convo.bookingTime) <= 0) {
    convo.bookingTime = bookedMs;
    changed = true;
  }
  const ld = convo.leadData && typeof convo.leadData === "object" ? convo.leadData : {};
  if (!convo.leadData || convo.leadData !== ld) convo.leadData = ld;
  if (!Number.isFinite(Number(ld.booking_time || 0)) || Number(ld.booking_time) <= 0) {
    ld.booking_time = bookedMs;
    changed = true;
  }
  if (!isPaidState(ld.payment_status || ld.paymentStatus || convo.paymentStatus || convo.payment_status)) {
    ld.payment_status = "paid";
    convo.paymentStatus = "paid";
    changed = true;
  }
  const inferredAmount = resolveConversationAmount(convo);
  if (Number.isFinite(inferredAmount) && inferredAmount > 0) {
    if (!Number.isFinite(Number(convo.amount || 0)) || Number(convo.amount) <= 0) {
      convo.amount = inferredAmount;
      changed = true;
    }
    if (!Number.isFinite(Number(ld.amount || 0)) || Number(ld.amount) <= 0) {
      ld.amount = inferredAmount;
      changed = true;
    }
  }

  const threadId = String(convo.id || "").trim();
  const thread = (state.threads || []).find((t) => String(t?.id || "").trim() === threadId);
  if (thread) {
    if (String(thread.status || "").toLowerCase() !== "booked") thread.status = "booked";
    if (!/booked|appointment_booked|scheduled/.test(String(thread.stage || "").toLowerCase())) thread.stage = "booked";
    if (!Number.isFinite(Number(thread.bookingTime || 0)) || Number(thread.bookingTime) <= 0) thread.bookingTime = bookedMs;
    thread.leadData = { ...(thread.leadData || {}), booking_time: Number(thread.bookingTime || bookedMs), payment_status: "paid" };
    if (Number.isFinite(inferredAmount) && inferredAmount > 0) {
      if (!Number.isFinite(Number(thread.amount || 0)) || Number(thread.amount) <= 0) thread.amount = inferredAmount;
      if (!Number.isFinite(Number(thread.leadData?.amount || 0)) || Number(thread.leadData?.amount) <= 0) {
        thread.leadData.amount = inferredAmount;
      }
    }
  }

  // Optimistically apply the booked amount once so lead details updates instantly.
  const key = customerRecoveredCacheKey(convo.from || "");
  const bookingKey = `${threadId}:${bookedMs}`;
  if (key && Number.isFinite(inferredAmount) && inferredAmount > 0 && state.recoveredTotalsOptimisticApplied?.[bookingKey] !== true) {
    const existing = Number(state.recoveredTotalsByCustomer?.[key] || 0);
    state.recoveredTotalsByCustomer[key] = Number((existing + inferredAmount).toFixed(2));
    state.recoveredTotalsOptimisticApplied[bookingKey] = true;
  }
  const isSimulated = /^sim-/i.test(threadId);
  if (refreshRecovered && !isSimulated) {
    ensureRecoveredTotalForConversation(convo, { force: true }).catch(() => {});
  }
  return changed;
}

function stopConversationAnimation(){
  if (simulationAnimationTimer) {
    clearTimeout(simulationAnimationTimer);
    simulationAnimationTimer = null;
  }
  state.simulationAnimation = null;
  state.simulationActive = false;
  state.simulationBlockConversationLoad = false;
  state.simulationConversationId = null;
}

function startConversationAnimation({
  from,
  conversation = [],
  service = "",
  detail = "",
  extra = "",
  bookingTime = 0,
  price = 0,
  conversationId = ""
}){
  stopConversationAnimation();
  const tenantTo = getActiveTo();
  const fallbackFrom = String(from || "+10000000000").trim();
  const resolvedConversationId = String(conversationId || "").trim();
  const convoId = /^.+__.+$/.test(resolvedConversationId)
    ? resolvedConversationId
    : `${tenantTo}__${fallbackFrom}`;
  const simulatedMessages = (conversation || []).map((item) => {
    const dir = item.role === "ai" ? "out" : "in";
    return {
      id: `sim_${Math.random().toString(36).slice(2, 8)}`,
      direction: dir === "out" ? "outbound" : "inbound",
      dir,
      text: item.text || "",
      body: item.text || "",
      meta: item.meta || {},
      payload: item.payload || {},
      ts: Date.now(),
      status: "simulated",
      providerMeta: { provider: "simulator" }
    };
  });
  const safeSimulatedMessages = sanitizeSimulatedMessages(simulatedMessages);
  const bookingMs = Number(bookingTime || 0);
  const serviceName = String(service || "").trim();
  const extraName = String(extra || "").trim();
  const detailText = String(detail || "").trim();
  const servicesList = [serviceName, extraName].filter(Boolean);
  const availabilityText = Number.isFinite(bookingMs) && bookingMs > 0
    ? new Date(bookingMs).toLocaleString()
    : "";
  const requestText = servicesList.length ? servicesList.join(" + ") : (serviceName || "");
  const leadData = {
    intent: serviceName || "inquiry",
    request: requestText,
    service_required: requestText,
    services_list: servicesList,
    services_summary: servicesList.map((s) => `- ${s}`).join("\n"),
    availability: availabilityText,
    vehicle_model: detailText || undefined
  };
  state.activeConversation = {
    id: convoId,
    from,
    to: tenantTo,
    messages: safeSimulatedMessages,
    source: "simulated",
    isSimulated: true,
    stage: "simulated",
    status: "simulated",
    bookingTime: null,
    amount: Number.isFinite(Number(price || 0)) && Number(price) > 0 ? Number(price) : null,
    leadData
  };
  state.activeThreadId = convoId;
  state.view = "messages";
  state.simulationActive = true;
  state.simulationBlockConversationLoad = true;
  state.simulationConversationId = convoId;
  state.threadsLastLoadedAt = Date.now();
  const simulatedThread = {
    id: convoId,
    from,
    name: from,
    phone: from,
    source: "simulated",
    isSimulated: true,
    stage: "simulated",
    status: "simulated",
    bookingTime: null,
    amount: Number.isFinite(Number(price || 0)) && Number(price) > 0 ? Number(price) : null,
    leadData,
    lastText: safeSimulatedMessages[safeSimulatedMessages.length - 1]?.text || "Simulated conversation",
    messages: safeSimulatedMessages,
    updatedAt: Date.now(),
    lastActivityAt: Date.now()
  };
  const persistedSimulatedThread = saveSimulatedConversation(tenantTo, simulatedThread) || simulatedThread;
  state.activeConversation = {
    ...state.activeConversation,
    ...persistedSimulatedThread,
    to: tenantTo,
    messages: safeSimulatedMessages,
    leadData
  };
  state.conversationCacheById[getConversationCacheKey(convoId, tenantTo)] = {
    loadedAt: Date.now(),
    data: cloneJsonSafe(state.activeConversation)
  };
  state.threads = [persistedSimulatedThread, ...state.threads.filter((t) => t.id !== convoId)];
  try { localStorage.removeItem(`mc_schedule_sim_bookings_v1:${tenantTo}`); } catch {}
  rebuildRevenueLedgerFromThreads(state.threads);
  const hasMountedMessagesView = Boolean(document.getElementById("threadList") && document.getElementById("chatHead") && document.getElementById("bubbles"));
  if (hasMountedMessagesView) {
    renderThreadListFromAPI("").catch((err) => console.error("simulation thread paint failed:", err));
    renderChatFromAPI();
    renderLeadDetails();
    scrollChatToBottom();
  } else {
    render();
  }
}

async function runDeveloperConversationSimulation() {
  if (!canAccessDeveloperRoutes()) {
    throw new Error("Conversation simulation is only available in developer mode.");
  }
  const res = await apiPost("/api/dev/revenue/simulate", { scenario: "detailing_conversation" });
  if (res) {
    const tenantTo = String(res.to || getActiveTo() || "").trim();
    const customerFrom = String(res.from || "+10000000000").trim();
    if (tenantTo && tenantTo !== getActiveTo()) setActiveTo(tenantTo);
    startConversationAnimation({
      from: customerFrom,
      conversation: res.conversation || [],
      service: res.service || "",
      detail: res.detail || "",
      extra: res.extra || "",
      bookingTime: Number(res.bookingTime || 0) || 0,
      price: Number(res.price || 0) || 0,
      conversationId: res.convoKey || res.conversationId || `${tenantTo}__${customerFrom}`
    });
  }
  const serviceText = res?.service || "detailing request";
  const bookingTimeText = res?.bookingTime ? new Date(Number(res.bookingTime)).toLocaleString() : "your next slot";
  return {
    response: res,
    message: `Simulated ${serviceText}. Booking link sent for ${bookingTimeText}.`
  };
}


function formatMessageTimestamp(message) {
  const raw = message?.ts;
  if (typeof raw === "number" && Number.isFinite(raw)) return new Date(raw).toLocaleString();
  if (typeof raw === "string") {
    const trimmed = raw.trim();
    if (!trimmed) return "";
    const n = Number(trimmed);
    if (Number.isFinite(n) && n > 1_000_000_000) return new Date(n).toLocaleString();
    const parsed = Date.parse(trimmed);
    if (!Number.isNaN(parsed)) return new Date(parsed).toLocaleString();
    return trimmed;
  }
  const createdAt = Number(message?.createdAt || message?.timestamp || 0);
  if (Number.isFinite(createdAt) && createdAt > 0) return new Date(createdAt).toLocaleString();
  return "";
}


 
document.querySelectorAll(".status-btn").forEach(btn => {
  btn.addEventListener("click", async (e) => {
    e.preventDefault();
    e.stopPropagation();

    const newStatus = btn.dataset.status;
    if (!newStatus) return;

    c.lifecycle.leadStatus = newStatus;
    saveContacts(getActiveTo(), pageState.contacts);

    await apiPost("/api/conversations/status", {
      convoId: c.lifecycle.lastConversationId,
      status: newStatus
    }).catch(()=>{});

    render(); // refresh UI
  });
});



async function renderThreadListFromAPI(query){
  const list = $("#threadList");
  if(!list) return;
  try {

  const vipSet = state.simulationBlockConversationLoad ? new Set() : await getVipSetForActiveTo();

  const q = (query || "").toLowerCase();
  const threads = (state.threads || []).filter(t => {
    const hay = `${t.from} ${t.lastText || ""} ${t.stage || ""}`.toLowerCase();
    return hay.includes(q);
  });

  if (!threads.length) {
    list.innerHTML = RelayUI.renderEmptyState({
      title: "No threads yet",
      text: q ? "No conversations match your current search." : "New conversations will appear here as leads respond.",
      className: "messages-empty-state"
    });
    return;
  }

  list.innerHTML = threads.map(t => {
    const activeSnapshot = (state.activeConversation && String(state.activeConversation.id || "") === String(t?.id || ""))
      ? state.activeConversation
      : null;
    const threadForPill = activeSnapshot
      ? {
          ...t,
          ...activeSnapshot,
          leadData: { ...(t?.leadData || {}), ...(activeSnapshot?.leadData || {}) }
        }
      : t;
    applyRealtimeBookedState(threadForPill, { refreshRecovered: false });
    t.status = threadForPill?.status || t.status;
    t.stage = threadForPill?.stage || t.stage;
    t.bookingTime = coerceTimestampMs(threadForPill?.bookingTime || t.bookingTime || t?.leadData?.booking_time, t.bookingTime || 0) || t.bookingTime || 0;
    t.leadData = { ...(t?.leadData || {}), ...(threadForPill?.leadData || {}) };
    t.lastText = String(threadForPill?.lastText || t?.lastText || "");
    t.updatedAt = threadForPill?.updatedAt || t.updatedAt;
    t.lastActivityAt = threadForPill?.lastActivityAt || t.lastActivityAt;
    const threadTimestamp = formatThreadTimestamp(t.lastActivityAt || t.updatedAt || 0);
    const active = t.id === state.activeThreadId ? "active" : "";
    const from = t.from || "Unknown";
    const snippet = t.lastText || "";
    const vip = isVipThread(vipSet, from) ? " " : "";
    const lifecycleLabel = prettyLifecycleLabel(threadForPill?.status || threadForPill?.stage || "");
    const serviceLabels = getServiceLabelsFromLeadData(t.leadData || {});
    const serviceChips = renderServiceChipsHtml(
      serviceLabels.length ? serviceLabels : inferServiceLabelsFromMessages(threadForPill?.messages || [])
    );
    const threadMoneyPill = buildConversationMoneyPill(threadForPill);
    state.threadMoneyPillsById = state.threadMoneyPillsById || {};
    state.threadMoneyPillsById[String(t?.id || "")] = {
      label: String(threadMoneyPill?.label || ""),
      tone: String(threadMoneyPill?.tone || ""),
      updatedAt: Date.now()
    };
    return `
      <div class="thread ${active}" data-thread="${t.id}">
        <div class="thread-top">
          <div class="thread-title-stack">
            <div class="thread-name">${escapeHtml(from)}${vip}</div>
            ${lifecycleLabel ? `<div class="thread-meta thread-meta-status">${escapeHtml(lifecycleLabel)}</div>` : ""}
          </div>
          <div class="thread-top-right">
            ${threadTimestamp ? `<div class="thread-meta thread-meta-time">${escapeHtml(threadTimestamp)}</div>` : ""}
            <span class="badge chat-money-pill ${escapeAttr(threadMoneyPill.tone)}">${escapeHtml(threadMoneyPill.label)}</span>
          </div>
        </div>
        <div class="thread-snippet">${escapeHtml(snippet)}</div>
        ${serviceChips}
      </div>
    `;
  }).join("");

  } catch (err) {
    console.error("Thread list render failed:", err);
    list.innerHTML = RelayUI.renderNoticeCard({
      title: "Threads unavailable",
      text: "The conversation list could not render.",
      detail: err?.message || String(err),
      className: "shell-notice-error"
    });
  }
}

function focusScheduleOnDate(dateValue) {
  const ms = Number(dateValue);
  if (!Number.isFinite(ms) || ms <= 0) return;
  state.scheduleFocusDate = ms;
  state.view = "schedule";
  render();
}

function findSlotSnippet(text) {
  if (!text) return null;
  const sanitized = String(text || "").replace(/\s+/g, " ").trim();
  const patterns = [
    /\bfor\s+([^.,\n]+)/i,
    /\b(?:this|next|today|tomorrow)\s+([^.,\n]+)/i,
    /\b([A-Za-z]+day(?:\s+(?:morning|afternoon|evening|night))?(?:\s+at\s+\d{1,2}(?::\d{2})?\s*(?:am|pm))?)/i,
    /\bat\s+\d{1,2}(?::\d{2})?\s*(?:am|pm)/i
  ];
  for (const regex of patterns) {
    const match = sanitized.match(regex);
    if (match) {
      return String(match[1] || match[0] || "").trim();
    }
  }
  return null;
}

function getBookingActionForMessage(message) {
  if (!message || String(message.dir || "").toLowerCase() !== "out") return null;
  const confirmed = Boolean(message.meta?.bookingConfirmed || message.payload?.bookingConfirmed);
  if (!confirmed) return null;
  const bookingTimeMeta = message.meta?.bookingTime || message.bookingTime || message.payload?.bookingTime || message.payload?.booking_time;
  const normalizedMs = Number(bookingTimeMeta);
  if (!Number.isFinite(normalizedMs) || normalizedMs <= 0) return null;
  const text = String(message.text || "");
  const slotLabel = findSlotSnippet(text) || `Confirmed slot`;
  const formattedDate = new Date(normalizedMs).toLocaleDateString("en-US", { month: "short", day: "numeric" });
  const customLabel = String(message.meta?.bookingLabel || message.payload?.bookingLabel || "").trim();
  const label = customLabel || (slotLabel ? `${slotLabel} (${formattedDate})` : `Jump to ${formattedDate}`);
  return { dateMs: normalizedMs, label };
}

function messageTimestampMs(message) {
  const raw = message?.ts;
  if (typeof raw === "number" && Number.isFinite(raw) && raw > 0) return raw;
  if (typeof raw === "string") {
    const trimmed = raw.trim();
    if (trimmed) {
      const n = Number(trimmed);
      if (Number.isFinite(n) && n > 0) return n;
      const parsed = Date.parse(trimmed);
      if (!Number.isNaN(parsed)) return parsed;
    }
  }
  const createdAt = Number(message?.createdAt || message?.timestamp || 0);
  return Number.isFinite(createdAt) && createdAt > 0 ? createdAt : null;
}

function dayKey(ms) {
  if (!Number.isFinite(ms) || ms <= 0) return "";
  const d = new Date(ms);
  const y = d.getFullYear();
  const m = String(d.getMonth() + 1).padStart(2, "0");
  const day = String(d.getDate()).padStart(2, "0");
  return `${y}-${m}-${day}`;
}

function formatTimelineDayLabel(ms) {
  if (!Number.isFinite(ms) || ms <= 0) return "";
  const d = new Date(ms);
  const now = new Date();
  const todayStart = new Date(now.getFullYear(), now.getMonth(), now.getDate()).getTime();
  const yesterdayStart = todayStart - (24 * 60 * 60 * 1000);
  if (ms >= todayStart) return "Today";
  if (ms >= yesterdayStart && ms < todayStart) return "Yesterday";
  return d.toLocaleDateString("en-US", { month: "short", day: "numeric", year: "numeric" });
}

function formatBookedMarkerLabel(ms) {
  if (!Number.isFinite(ms) || ms <= 0) return "Booked";
  return `Booked: ${new Date(ms).toLocaleString("en-US", { month: "short", day: "numeric", hour: "numeric", minute: "2-digit" })}`;
}

function parseNumericAmount(value) {
  let n = Number(value);
  if ((!Number.isFinite(n) || n <= 0) && typeof value === "string") {
    const raw = String(value || "").trim();
    const numericLike = /^\$?\s*\d+(?:\.\d{1,2})?$/;
    const commaNumericLike = /^\$?\s*\d{1,3}(?:,\d{3})+(?:\.\d{1,2})?$/;
    if (!(numericLike.test(raw) || commaNumericLike.test(raw))) return null;
    const cleaned = raw.replace(/[$,\s]/g, "");
    n = Number(cleaned);
  }
  if (!Number.isFinite(n) || n <= 0) return null;
  // Heuristic: message/payment payloads sometimes store cents.
  if (n >= 10000 && Number.isInteger(n) && n % 100 === 0) return n / 100;
  return n;
}

function extractDollarAmountFromText(text) {
  const s = String(text || "");
  const m = s.match(/\$\s*(\d{1,3}(?:,\d{3})*(?:\.\d{1,2})?|\d{1,6}(?:\.\d{1,2})?)/);
  if (!m) return null;
  const n = Number(String(m[1] || "").replace(/,/g, ""));
  return Number.isFinite(n) && n > 0 ? n : null;
}

function normalizePaymentMethod(value) {
  const raw = String(value || "").trim().toLowerCase();
  if (!raw) return "";
  if (/cash/.test(raw)) return "cash";
  if (/card|credit|debit|stripe|visa|mastercard|amex|discover/.test(raw)) return "card";
  return raw;
}

function isPaidState(value) {
  const raw = String(value || "").trim().toLowerCase();
  if (!raw) return false;
  return /paid|succeed|success|captured|complete|settled|confirmed/.test(raw);
}

function isPendingCardState(value) {
  const raw = String(value || "").trim().toLowerCase();
  if (!raw) return false;
  return /pending|authorize|processing|hold|initiated|in_progress/.test(raw);
}

function resolveConversationAmount(convo) {
  const ld = convo?.leadData || {};
  const directCandidates = [
    convo?.resolvedAmount,
    convo?.resolved_amount,
    convo?.resolvedAmountCents,
    convo?.resolved_amount_cents,
    convo?.amount,
    convo?.bookingAmount,
    convo?.booking_amount,
    ld?.amount,
    ld?.price,
    ld?.quoted_amount,
    ld?.estimate_amount,
    ld?.booking_amount,
    ld?.invoice_amount,
    ld?.final_amount,
    ld?.total,
    ld?.total_price
  ];
  for (const c of directCandidates) {
    const n = parseNumericAmount(c);
    if (Number.isFinite(n) && n > 0) return n;
  }
  const msgs = Array.isArray(convo?.messages) ? convo.messages : [];
  for (let i = msgs.length - 1; i >= 0; i--) {
    const m = msgs[i] || {};
    const payloadCandidates = [
      m?.amount,
      m?.meta?.amount,
      m?.payload?.amount,
      m?.meta?.amountCents,
      m?.payload?.amountCents
    ];
    for (const c of payloadCandidates) {
      const n = parseNumericAmount(c);
      if (Number.isFinite(n) && n > 0) return n;
    }
    const fromText = extractDollarAmountFromText(m?.text || m?.body || "");
    if (Number.isFinite(fromText) && fromText > 0) return fromText;
  }
  return null;
}

function resolvePaymentSignals(convo) {
  const ld = convo?.leadData || {};
  const msgs = Array.isArray(convo?.messages) ? convo.messages : [];
  let method = normalizePaymentMethod(ld?.payment_method || ld?.paymentMethod || convo?.paymentMethod || convo?.payment_method);
  let paid = isPaidState(ld?.payment_status || ld?.paymentStatus || convo?.paymentStatus || convo?.payment_status);
  let pendingCard = isPendingCardState(ld?.payment_status || ld?.paymentStatus || convo?.paymentStatus || convo?.payment_status);

  for (let i = msgs.length - 1; i >= 0; i--) {
    const m = msgs[i] || {};
    const msgMethod = normalizePaymentMethod(
      m?.meta?.paymentMethod
      || m?.payload?.paymentMethod
      || m?.meta?.payment_method
      || m?.payload?.payment_method
    );
    if (!method && msgMethod) method = msgMethod;
    const statusRaw =
      m?.meta?.paymentStatus
      || m?.payload?.paymentStatus
      || m?.meta?.payment_status
      || m?.payload?.payment_status
      || "";
    if (!paid) {
      paid = m?.meta?.paid === true || m?.payload?.paid === true || m?.meta?.paymentSucceeded === true || m?.payload?.paymentSucceeded === true || isPaidState(statusRaw);
    }
    if (!pendingCard && isPendingCardState(statusRaw)) pendingCard = true;
  }
  return { method, paid, pendingCard };
}

function formatMoneyLabel(amount) {
  if (!Number.isFinite(amount) || amount <= 0) return "$???";
  const rounded = Math.round(amount * 100) / 100;
  if (Number.isInteger(rounded)) return `$${rounded}`;
  return `$${rounded.toFixed(2)}`;
}

function parseMoneyLabelToCents(label) {
  const raw = String(label || "").trim();
  if (!raw || raw.includes("?")) return 0;
  const n = Number(raw.replace(/[^0-9.]/g, ""));
  if (!Number.isFinite(n) || n <= 0) return 0;
  return Math.round(n * 100);
}

function buildConversationMoneyPill(convo) {
  const status = String(convo?.status || "").trim().toLowerCase();
  const stage = String(convo?.stage || "").trim().toLowerCase();
  const isBookedLifecycle = status === "booked" || /booked|appointment_booked|scheduled/.test(stage);
  const amount = resolveConversationAmount(convo);
  if ((!Number.isFinite(amount) || amount <= 0) && isBookedLifecycle) {
    return { label: "$???", tone: "is-paid" };
  }
  if (!Number.isFinite(amount) || amount <= 0) {
    return { label: "$???", tone: "is-unknown" };
  }
  const { method, paid, pendingCard } = resolvePaymentSignals(convo);
  if (isBookedLifecycle) {
    return { label: formatMoneyLabel(amount), tone: "is-paid" };
  }
  if (method === "cash") {
    return { label: formatMoneyLabel(amount), tone: "is-paid" };
  }
  if (method === "card" && paid) {
    return { label: formatMoneyLabel(amount), tone: "is-paid" };
  }
  if (method === "card" && !paid) {
    return { label: formatMoneyLabel(amount), tone: "is-pending" };
  }
  if (paid) {
    return { label: formatMoneyLabel(amount), tone: "is-paid" };
  }
  if (pendingCard) {
    return { label: formatMoneyLabel(amount), tone: "is-pending" };
  }
  return { label: formatMoneyLabel(amount), tone: "is-pending" };
}

function buildTimelineItems(messages) {
  const out = [];
  let lastRenderedDay = "";
  const safe = Array.isArray(messages) ? messages : [];
  safe.forEach((m, i) => {
    const tsMs = messageTimestampMs(m);
    const dKey = dayKey(tsMs || 0);
    if (dKey && dKey !== lastRenderedDay) {
      out.push({ type: "day_separator", key: `day_${dKey}_${i}`, label: formatTimelineDayLabel(tsMs) });
      lastRenderedDay = dKey;
    }
    const bookedMs = Number(m?.meta?.bookingTime || m?.bookingTime || m?.payload?.bookingTime || m?.payload?.booking_time || 0);
    const bookingConfirmed = Boolean(m?.meta?.bookingConfirmed || m?.payload?.bookingConfirmed) && Number.isFinite(bookedMs) && bookedMs > 0;
    if (bookingConfirmed) {
      out.push({
        type: "booked_separator",
        key: `booked_${bookedMs}_${i}`,
        label: formatBookedMarkerLabel(bookedMs)
      });
    }
    out.push({ type: "message", key: `msg_${i}`, index: i, message: m });
  });
  return out;
}


function renderChatFromAPI(){
  const convo = state.activeConversation;
  const chatHead = $("#chatHead");
  const bubbles = $("#bubbles");
  if (!chatHead || !bubbles) {
    return;
  }
  if(!convo){
    chatHead.innerHTML = `
      <div class="chat-title">
        <b>Select a conversation</b>
        <span>Choose a thread to review the timeline and lead context.</span>
      </div>
    `;
    bubbles.innerHTML = RelayUI.renderEmptyState({
      title: "No conversation selected",
      text: "Select a thread to view messages, timestamps, and operational status.",
      className: "messages-empty-state"
    });
    const wrap = document.getElementById("sendConsentWrap");
    if (wrap) wrap.style.display = "none";
    renderLeadDetails();
    return;
  }

  const bookedStateChanged = applyRealtimeBookedState(convo, { refreshRecovered: true });
  if (bookedStateChanged) {
    const query = $("#threadSearch")?.value?.trim() || "";
    renderThreadListFromAPI(query).catch(() => {});
  }

  const expandedIndex = Number(state.chatExpandedByConversation?.[convo.id]);
  const moneyPill = buildConversationMoneyPill(convo);
  const convoStatusLabel = prettyLifecycleLabel(convo.status || convo.stage || "");
  chatHead.innerHTML = `
    <div class="chat-head-main">
      <div class="chat-title">
        <b>${escapeHtml(convo.from)}</b>
        <span>${escapeHtml([convo.to, convoStatusLabel].filter(Boolean).join(" · "))}</span>
      </div>
      <div class="chat-head-badges">
        <span class="badge chat-money-pill ${escapeAttr(moneyPill.tone)}">${escapeHtml(moneyPill.label)}</span>
      </div>
    </div>
  `;

  const timelineItems = buildTimelineItems(convo.messages);
  const renderedMessages = timelineItems.map((item) => {
    if (item.type === "day_separator") {
      return `
        <div class="chat-separator" data-separator="day">
          <span>${escapeHtml(item.label || "")}</span>
        </div>
      `;
    }
    if (item.type === "booked_separator") {
      return `
        <div class="chat-separator chat-separator-booked" data-separator="booked">
          <span>${escapeHtml(item.label || "Booked")}</span>
        </div>
      `;
    }
    const m = item.message;
    const i = Number(item.index);
    const cls =
      (m.dir === "event" || m.dir === "system") ? "system" :
      (m.dir === "out") ? "out" : "in";
    const ts = formatMessageTimestamp(m);
    const isExpanded = i === expandedIndex;
    const bookingAction = getBookingActionForMessage(m);
    const bookingLink = bookingAction
      ? `<button type="button" class="bubble-slot-link bubble-date-link" data-schedule-ms="${escapeAttr(String(bookingAction.dateMs || ""))}">${escapeHtml(bookingAction.label)}</button>`
      : "";
    const messageStatus = String(m?.status || "").trim().toLowerCase();
    const statusLabel = cls === "out" ? prettyLifecycleLabel(messageStatus) : "";
    const statusTone = getMessageStatusTone(messageStatus);
    const bubbleMeta = (ts || statusLabel) ? `
      <div class="bubble-meta ${cls}">
        ${ts ? `<span class="bubble-ts-inline">${escapeHtml(ts)}</span>` : ""}
        ${statusLabel ? `<span class="bubble-status ${escapeAttr(statusTone)}">${escapeHtml(statusLabel)}</span>` : ""}
      </div>
    ` : "";
    const bubbleContent = `
      <div class="bubble ${cls} ${isExpanded ? "is-expanded" : ""}">
        <div class="bubble-text">${escapeHtml(m.text)}</div>
        ${bookingLink}
      </div>
    `;
    return `
      <div class="bubble-wrapper ${cls} ${isExpanded ? "is-expanded" : ""}" data-msg-index="${i}">
        ${bubbleContent}
        ${bubbleMeta}
      </div>
    `;
  }).join("");
  bubbles.innerHTML = renderedMessages;

  bubbles.scrollTop = bubbles.scrollHeight;
  // Enable input + bind send handlers (safe re-bind)
  const input = document.getElementById("chatInput");
  const btn = document.getElementById("sendBtn");
  if (input) input.disabled = false;
  if (btn) btn.disabled = false;
  if (input) {
    input.onkeydown = (e) => {
      if (e.key === "Enter") sendDashboardMessage();
    };
  }
  refreshComposeComplianceUI(convo);
  renderLeadDetails(); //  RIGHT HERE

}

async function refreshComposeComplianceUI(convo){
  const wrap = document.getElementById("sendConsentWrap");
  const check = document.getElementById("sendConsentCheck");
  const text = document.getElementById("sendConsentText");
  const source = document.getElementById("sendConsentSource");
  if (!wrap || !convo) return;

  try {
    const acct = await loadAccountSettings(convo.to);
    const comp = acct?.compliance || {};
    state.activeCompliance = comp;

    const requireConsent = comp?.consent?.requireForOutbound === true;
    wrap.style.display = requireConsent ? "" : "none";
    if (text) text.textContent = comp?.consent?.consentCheckboxText || "I confirm I have consent to text this contact.";
    if (source && Array.isArray(comp?.consent?.consentSourceOptions) && comp.consent.consentSourceOptions.length > 0) {
      const options = comp.consent.consentSourceOptions.map((v) => `<option value="${escapeAttr(v)}">${escapeHtml(v)}</option>`);
      source.innerHTML = options.join("");
      if (!comp.consent.consentSourceOptions.includes(source.value)) {
        source.value = comp.consent.consentSourceOptions[0];
      }
    }
    if (check) check.checked = false;
  } catch (err) {
    console.error("Failed to load compliance settings:", err);
    wrap.style.display = "none";
  }
}



function renderThreadList(query){
  const list = $("#threadList");
  if(!list) return;

  const q = query.toLowerCase();
  const threads = state.threads.filter(t => {
    const hay = `${t.name} ${t.phone} ${(t.tags||[]).join(" ")}`.toLowerCase();
    return hay.includes(q);
  });

  list.innerHTML = threads.map(t => {
    const last = t.messages[t.messages.length - 1];
    const active = t.id === state.activeThreadId ? "active" : "";
    const tags = (t.tags||[]).map(x => `<span class="badge">${x}</span>`).join(" ");
    return `
      <div class="thread ${active}" data-thread="${t.id}">
        <div class="thread-top">
          <div class="thread-name">${escapeHtml(t.name)}</div>
          <div class="thread-meta">${escapeHtml(last?.ts || "")}</div>
        </div>
        <div class="thread-snippet">${escapeHtml(last?.text || "")}</div>
        <div class="row" style="gap:6px; flex-wrap:wrap;">${tags}</div>
      </div>
    `;
  }).join("");

  $$(".thread").forEach(el => {
    el.addEventListener("click", () => {
      state.activeThreadId = el.dataset.thread;
      renderThreadList($("#threadSearch")?.value?.trim() || "");
      renderChat();
    });
  });
}

function skeletonInbox(){
  return `
    <div class="inbox">
      <div class="panel">
        <div class="panel-body">
          ${Array.from({length:6}).map(() => `
            <div class="thread">
              <div class="skeleton" style="height:14px;width:60%;margin-bottom:6px;"></div>
              <div class="skeleton" style="height:12px;width:90%;"></div>
            </div>
          `).join("")}
        </div>
      </div>

      <div class="panel">
        <div class="panel-body">
          ${Array.from({length:4}).map(() => `
            <div class="skeleton" style="height:34px;width:70%;margin-bottom:10px;"></div>
          `).join("")}
        </div>
      </div>
    </div>
  `;
}


/* VIP */
function viewVIP(){
  const wrap = document.createElement("div");
  wrap.className = "col";
  wrap.appendChild(headerCard("VIP", "Add/remove/search numbers that should NEVER receive auto-replies."));

  const card = document.createElement("div");
  card.className = "card";
  card.innerHTML = `
    <div class="grid2">
      <div class="col">
        <label class="p">Search</label>
        <input class="input" id="vipSearch" placeholder="Name or phone..." />
      </div>
      <div class="col">
        <label class="p">Add VIP</label>
        <div class="row">
          <input class="input" id="vipName" placeholder="Name (optional)" />
          <input class="input" id="vipPhone" placeholder="Phone (ex: 814-555-1234)" />
          <button class="btn primary" id="vipAdd">Add</button>
        </div>
      </div>
    </div>
    <div style="height:12px;"></div>
    <div class="list" id="vipList"></div>
  `;
  wrap.appendChild(card);

  setTimeout(() => {
    renderVipList("");
    $("#vipSearch").addEventListener("input", (e) => renderVipList(e.target.value.trim()));
    $("#vipAdd").addEventListener("click", addVipFromInputs);
  }, 0);

  return wrap;
}

function addVipFromInputs(){
  const name = $("#vipName").value.trim() || "VIP";
  const phone = $("#vipPhone").value.trim();
  if(!phone) return;

  const phoneKey = normalizePhone(phone);
  if(state.vip.some(v => normalizePhone(v.phone) === phoneKey)){
    $("#vipPhone").value = "";
    return;
  }

  state.vip.unshift({ id: crypto.randomUUID(), name, phone, neverAutoReply: true });
  saveLS(LS_KEYS.VIP, state.vip);
  syncVipToBackend();

  $("#vipName").value = "";
  $("#vipPhone").value = "";
  renderVipList($("#vipSearch").value.trim());
}

function renderVipList(query){
  const list = $("#vipList");
  if(!list) return;

  const q = query.toLowerCase();
  const items = state.vip.filter(v => {
    const hay = `${v.name} ${v.phone}`.toLowerCase();
    return hay.includes(q);
  });

  list.innerHTML = items.map(v => `
    <div class="list-item" data-id="${v.id}">
      <div class="list-left">
        <b>${escapeHtml(v.name)}</b>
        <span>${escapeHtml(v.phone)}</span>
      </div>

      <div class="row">
        <label class="toggle" title="If checked, this number will NEVER get auto replies.">
          <input type="checkbox" ${v.neverAutoReply ? "checked" : ""} data-toggle="${v.id}" />
          never auto-reply
        </label>
        <button class="btn" data-remove="${v.id}">Remove</button>
      </div>
    </div>
  `).join("");

  $$("input[data-toggle]").forEach(inp => {
    inp.addEventListener("change", () => {
      const id = inp.dataset.toggle;
      const item = state.vip.find(x => x.id === id);
      if(!item) return;
      item.neverAutoReply = inp.checked;
      saveLS(LS_KEYS.VIP, state.vip);
      syncVipToBackend();
    });
  });

  $$("button[data-remove]").forEach(btn => {
    btn.addEventListener("click", () => {
      const id = btn.dataset.remove;
      state.vip = state.vip.filter(x => x.id !== id);
      saveLS(LS_KEYS.VIP, state.vip);
      syncVipToBackend();
      renderVipList($("#vipSearch").value.trim());
    });
  });
}

/* Automations */
function viewAutomations(){
  const wrap = document.createElement("div");
  wrap.className = "automations-page";

  // Define all available automation templates
  const automationTemplates = {
    leadCapture: [
      {
        id: 'high-intent-qualifier',
        name: 'High-Intent Lead Qualifier',
        description: 'Capture scope fast so you can quote and book sooner',
        icon: '??',
        trigger: 'inbound_sms',
        firstTimeOnly: true,
        template: "Awesome ï¿½ I can get you a fast quote. What service do you need, what vehicle is it for, and what day works best?",
        category: 'leadCapture'
      },
      {
        id: 'quick-booking-nudge-15m',
        name: '15-Min Booking Nudge',
        description: 'Recover warm leads quickly if they go quiet',
        icon: '??',
        trigger: 'no_response',
        delayMinutes: 15,
        template: "Quick follow-up ï¿½ I can hold a priority slot for you today. Want me to send the booking link?",
        category: 'leadCapture'
      },
      {
        id: 'new-text-greeting',
        name: 'New Text Lead Greeting',
        description: 'Auto-greet new customers who text in',
        icon: '??',
        trigger: 'inbound_sms',
        firstTimeOnly: true,
        template: "Hey! Thanks for reaching out to [Business Name]. What can we help you with today?",
        category: 'leadCapture'
      }
    ],
    followUp: [
      {
        id: 'no-response-1hr',
        name: '1 Hour Follow-Up',
        description: 'Nudge leads who haven\'t responded in 1 hour',
        icon: '?',
        trigger: 'no_response',
        delayMinutes: 60,
        template: "Hey! Just following up - still interested in getting your vehicle detailed? Let me know what works for your schedule.",
        category: 'followUp'
      },
      {
        id: 'no-response-24hr',
        name: '24 Hour Follow-Up',
        description: 'Second follow-up for unresponsive leads',
        icon: '??',
        trigger: 'no_response',
        delayMinutes: 1440,
        template: "Hi again! Wanted to make sure you saw my message. We'd love to help with your detailing needs. Any questions I can answer?",
        category: 'followUp'
      },
      {
        id: 'quote-follow-up',
        name: 'Quote Follow-Up',
        description: 'Follow up after sending a quote',
        icon: '??',
        trigger: 'quote_sent',
        delayMinutes: 240,
        template: "Hey! Just checking in on that quote I sent. Any questions about the pricing or services? Happy to explain anything.",
        category: 'followUp'
      }
    ],
    booking: [
      {
        id: 'booking-confirmation',
        name: 'Booking Confirmation',
        description: 'Confirm when appointment is scheduled',
        icon: '?',
        trigger: 'booking_created',
        template: "Awesome! You're all booked for {{service}} on {{date}} at {{time}}. We'll send a reminder the day before. See you soon!",
        category: 'booking'
      },
      {
        id: 'booking-reminder-24hr',
        name: '24 Hour Reminder',
        description: 'Remind customers about tomorrow\'s appointment',
        icon: '??',
        trigger: 'booking_reminder',
        delayMinutes: -1440,
        template: "Hey! Just a reminder - your {{service}} appointment is tomorrow at {{time}}. Reply YES to confirm or let us know if you need to reschedule.",
        category: 'booking'
      },
      {
        id: 'booking-reminder-2hr',
        name: '2 Hour Reminder',
        description: 'Final reminder before appointment',
        icon: '??',
        trigger: 'booking_reminder',
        delayMinutes: -120,
        template: "See you in 2 hours! Your {{service}} appointment is at {{time}}. Address: [Your Address]",
        category: 'booking'
      }
    ],
    postService: [
      {
        id: 'thank-you',
        name: 'Thank You Message',
        description: 'Thank customers after service completion',
        icon: '??',
        trigger: 'service_completed',
        delayMinutes: 30,
        template: "Thanks for choosing us! Hope you love how your {{vehicle}} turned out. If you have a minute, a Google review would mean the world to us: [Review Link]",
        category: 'postService'
      },
      {
        id: 'feedback-request',
        name: 'Feedback Request',
        description: 'Ask for feedback a day after service',
        icon: '?',
        trigger: 'service_completed',
        delayMinutes: 1440,
        template: "Hey! How's your {{vehicle}} looking? We'd love your honest feedback. Anything we could have done better?",
        category: 'postService'
      },
      {
        id: 'maintenance-reminder',
        name: 'Maintenance Reminder',
        description: 'Remind about ceramic coating maintenance',
        icon: '???',
        trigger: 'service_completed',
        delayMinutes: 43200, // 30 days
        serviceType: 'ceramic',
        template: "Hey! It's been about a month since your ceramic coating. How's it holding up? Remember to use pH-neutral soap when washing. Book a maintenance wash anytime!",
        category: 'postService'
      }
    ],
    winBack: [
      {
        id: 'inactive-30-days',
        name: '30 Day Win-Back',
        description: 'Re-engage customers after 30 days',
        icon: '??',
        trigger: 'inactive_customer',
        delayDays: 30,
        template: "Hey! It's been a while since your last detail. Ready for a refresh? Book this week and get 10% off!",
        category: 'winBack'
      },
      {
        id: 'seasonal-reminder',
        name: 'Seasonal Reminder',
        description: 'Seasonal detailing reminders',
        icon: '??',
        trigger: 'seasonal',
        template: "Spring cleaning time! Your car took a beating this winter. Book a full detail to get rid of salt, grime, and protect your paint for summer.",
        category: 'winBack'
      },
      {
        id: 'lost-lead-recovery',
        name: 'Lost Lead Recovery',
        description: 'Try to recover leads that went cold',
        icon: '??',
        trigger: 'lead_lost',
        delayDays: 7,
        template: "Hey! We chatted last week about detailing. Still looking for someone? We'd love to earn your business. Any questions I can answer?",
        category: 'winBack'
      }
    ]
  };

  const categoryInfo = {
    leadCapture: { name: 'Lead Capture', icon: '??', desc: 'Capture and respond to new leads instantly' },
    followUp: { name: 'Follow-Up', icon: '??', desc: 'Automated follow-ups for unresponsive leads' },
    booking: { name: 'Booking', icon: '??', desc: 'Confirmations and reminders for appointments' },
    postService: { name: 'Post-Service', icon: '?', desc: 'Thank you messages and review requests' },
    winBack: { name: 'Win-Back', icon: '??', desc: 'Re-engage inactive customers' }
  };
  const automationDisplayOverrides = {
    'high-intent-qualifier': {
      icon: '\u{1F3AF}',
      template: "Awesome \u2014 I can get you a fast quote. What service do you need, what vehicle is it for, and what day works best?"
    },
    'quick-booking-nudge-15m': {
      icon: '\u{23F1}',
      template: "Quick follow-up \u2014 I can hold a priority slot for you today. Want me to send the booking link?"
    },
    'new-text-greeting': { icon: '\u{1F4AC}' },
    'no-response-1hr': { icon: '\u{23F0}' },
    'no-response-24hr': { icon: '\u{1F4E9}' },
    'quote-follow-up': { icon: '\u{1F4DD}' },
    'booking-confirmation': { icon: '\u{2705}' },
    'booking-reminder-24hr': { icon: '\u{1F4C5}' },
    'booking-reminder-2hr': { icon: '\u{23F3}' },
    'thank-you': { icon: '\u{1F64F}' },
    'feedback-request': { icon: '\u{2B50}' },
    'maintenance-reminder': { icon: '\u{1F6E1}' },
    'inactive-30-days': { icon: '\u{1F504}' },
    'seasonal-reminder': { icon: '\u{1F343}' },
    'lost-lead-recovery': { icon: '\u{1F9F2}' }
  };
  Object.values(automationTemplates).forEach((group) => {
    group.forEach((automation) => {
      const override = automationDisplayOverrides[automation.id];
      if (override) Object.assign(automation, override);
    });
  });
  Object.assign(categoryInfo, {
    leadCapture: { ...categoryInfo.leadCapture, icon: '\u{1F3AF}' },
    followUp: { ...categoryInfo.followUp, icon: '\u{1F4AC}' },
    booking: { ...categoryInfo.booking, icon: '\u{1F4C5}' },
    postService: { ...categoryInfo.postService, icon: '\u{2728}' },
    winBack: { ...categoryInfo.winBack, icon: '\u{1F504}' }
  });
  // Count active automations per category
  const getActiveCount = (category) => {
    return state.rules.filter(r => r.category === category && r.enabled).length;
  };

  const getTotalCount = (category) => {
    return automationTemplates[category]?.length || 0;
  };

  wrap.innerHTML = `
    <div class="automations-header">
      <div>
        <h1>Automations</h1>
        <p class="text-muted">Set up automated responses, follow-ups, and reminders to never miss a lead.</p>
      </div>
      <div class="automations-stats">
        <div class="auto-stat">
          <span class="auto-stat-value">${state.rules.filter(r => r.enabled).length}</span>
          <span class="auto-stat-label">Active</span>
        </div>
        <div class="auto-stat">
          <span class="auto-stat-value">${state.rules.length}</span>
          <span class="auto-stat-label">Total Rules</span>
        </div>
      </div>
    </div>

    <!-- Quick Stats -->
    <div class="auto-overview">
      ${Object.entries(categoryInfo).map(([key, info]) => `
        <div class="auto-category-card" data-category="${key}">
          <div class="auto-cat-icon">${info.icon}</div>
          <div class="auto-cat-info">
            <h3>${info.name}</h3>
            <p>${info.desc}</p>
          </div>
          <div class="auto-cat-status">
            <span class="auto-cat-count">${getActiveCount(key)}/${getTotalCount(key)}</span>
            <span class="auto-cat-label">active</span>
          </div>
        </div>
      `).join('')}
    </div>

    <!-- Automation Categories -->
    ${Object.entries(categoryInfo).map(([categoryKey, info]) => `
      <div class="auto-section" id="section-${categoryKey}">
        <div class="auto-section-header">
          <div class="auto-section-title">
            <span class="auto-section-icon">${info.icon}</span>
            <h2>${info.name}</h2>
          </div>
          <span class="auto-section-count">${getActiveCount(categoryKey)} of ${getTotalCount(categoryKey)} active</span>
        </div>
        <div class="auto-cards">
          ${automationTemplates[categoryKey].map(auto => {
            const existingRule = state.rules.find(r => r.templateId === auto.id);
            const isActive = existingRule?.enabled || false;
            return `
              <div class="auto-card ${isActive ? 'active' : ''}" data-template-id="${auto.id}">
                <div class="auto-card-header">
                  <span class="auto-card-icon">${auto.icon}</span>
                  <div class="auto-card-title">
                    <h4>${auto.name}</h4>
                    <p>${auto.description}</p>
                  </div>
                  <label class="toggle auto-toggle">
                    <input type="checkbox" ${isActive ? 'checked' : ''} data-toggle="${auto.id}" />
                    <span class="toggle-slider"></span>
                  </label>
                </div>
                <div class="auto-card-preview">
                  <div class="auto-message-preview">
                    <span class="preview-label">Message:</span>
                    <p>"${escapeHtml(existingRule?.template || auto.template)}"</p>
                  </div>
                  <div class="auto-card-meta">
                    ${auto.trigger === 'missed_call' ? '<span class="auto-tag">\u{1F4DE} Missed Call</span>' : ''}
                    ${auto.trigger === 'inbound_sms' ? '<span class="auto-tag">\u{1F4AC} New Text</span>' : ''}
                    ${auto.trigger === 'no_response' ? `<span class="auto-tag">\u{23F1} ${auto.delayMinutes >= 60 ? Math.round(auto.delayMinutes/60) + 'hr' : auto.delayMinutes + 'min'} delay</span>` : ''}
                    ${auto.businessHoursOnly ? '<span class="auto-tag">Business Hours</span>' : ''}
                    ${auto.afterHoursOnly ? '<span class="auto-tag">After Hours</span>' : ''}
                    ${auto.firstTimeOnly ? '<span class="auto-tag">First Contact</span>' : ''}
                  </div>
                </div>
                <div class="auto-card-actions">
                  <button class="btn btn-sm" data-edit="${auto.id}">Edit Message</button>
                  <button class="btn btn-sm" data-test="${auto.id}">Test</button>
                </div>
              </div>
            `;
          }).join('')}
        </div>
      </div>
    `).join('')}

    <!-- Custom Rules Section -->
    <div class="auto-section">
      <div class="auto-section-header">
        <div class="auto-section-title">
          <span class="auto-section-icon">\u{2699}</span>
          <h2>Custom Rules</h2>
        </div>
        <button class="btn primary" id="newCustomRule">+ New Custom Rule</button>
      </div>
      <div class="auto-custom-list" id="customRulesList">
        ${state.rules.filter(r => !r.templateId).length === 0
          ? RelayUI.renderEmptyState({ text: "No custom rules yet. Create one to build your own automation." })
          : state.rules.filter(r => !r.templateId).map(r => `
              <div class="auto-custom-rule">
                <div class="auto-custom-info">
                  <span class="auto-custom-icon">\u{2699}</span>
                  <div>
                    <h4>${escapeHtml(r.name)}</h4>
                    <p>${escapeHtml(r.trigger)} | ${r.businessHoursOnly ? 'business hours' : '24/7'}</p>
                  </div>
                </div>
                <div class="auto-custom-actions">
                  <label class="toggle">
                    <input type="checkbox" ${r.enabled ? 'checked' : ''} data-custom-toggle="${r.id}" />
                  </label>
                  <button class="btn btn-sm" data-custom-edit="${r.id}">Edit</button>
                  <button class="btn btn-sm danger" data-custom-del="${r.id}">Delete</button>
                </div>
              </div>
            `).join('')
        }
      </div>
    </div>

    <!-- Edit Modal -->
    <div id="autoEditModal" class="overlay hidden">
      <div class="auth-card auto-modal">
        <div class="auto-modal-header">
          <h2>Edit Automation</h2>
          <button class="btn btn-icon" id="closeAutoModal">
            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" width="20" height="20">
              <path d="M18 6L6 18M6 6l12 12"/>
            </svg>
          </button>
        </div>
        <div class="auto-modal-body" id="autoModalBody">
          <!-- Populated dynamically -->
        </div>
      </div>
    </div>
  `;

  // Store templates for reference
  wrap._templates = automationTemplates;

  // Event Listeners
  setTimeout(() => {
    // Toggle automation on/off
    wrap.querySelectorAll('[data-toggle]').forEach(toggle => {
      toggle.addEventListener('change', (e) => {
        const templateId = e.target.dataset.toggle;
        const isEnabled = e.target.checked;
        const card = e.target.closest('.auto-card');

        // Find template
        let template = null;
        Object.values(automationTemplates).forEach(cat => {
          const found = cat.find(t => t.id === templateId);
          if (found) template = found;
        });

        if (!template) return;

        // Find or create rule
        let rule = state.rules.find(r => r.templateId === templateId);

        if (isEnabled) {
          if (!rule) {
            rule = {
              id: crypto.randomUUID(),
              templateId: template.id,
              name: template.name,
              enabled: true,
              trigger: template.trigger,
              businessHoursOnly: template.businessHoursOnly || false,
              afterHoursOnly: template.afterHoursOnly || false,
              firstTimeOnly: template.firstTimeOnly || false,
              delayMinutes: template.delayMinutes || 0,
              template: template.template,
              category: template.category
            };
            state.rules.push(rule);
          } else {
            rule.enabled = true;
          }
          card.classList.add('active');
        } else {
          if (rule) {
            rule.enabled = false;
          }
          card.classList.remove('active');
        }

        saveLS(rulesKey(getActiveTo()), state.rules);
        syncRulesToBackend();
        updateAutomationCounts();
      });
    });

    // Edit automation message
    wrap.querySelectorAll('[data-edit]').forEach(btn => {
      btn.addEventListener('click', () => {
        const templateId = btn.dataset.edit;
        openAutoEditModal(templateId, automationTemplates);
      });
    });

    // Test automation
    wrap.querySelectorAll('[data-test]').forEach(btn => {
      btn.addEventListener('click', () => {
        const templateId = btn.dataset.test;
        alert('Test message would be sent to your phone. (Implement with real Twilio in production)');
      });
    });

    // Custom rule toggle
    wrap.querySelectorAll('[data-custom-toggle]').forEach(toggle => {
      toggle.addEventListener('change', (e) => {
        const ruleId = e.target.dataset.customToggle;
        const rule = state.rules.find(r => r.id === ruleId);
        if (rule) {
          rule.enabled = e.target.checked;
          saveLS(rulesKey(getActiveTo()), state.rules);
          syncRulesToBackend();
          updateAutomationCounts();
        }
      });
    });

    // Custom rule edit
    wrap.querySelectorAll('[data-custom-edit]').forEach(btn => {
      btn.addEventListener('click', () => {
        const ruleId = btn.dataset.customEdit;
        openCustomRuleModal(ruleId);
      });
    });

    // Custom rule delete
    wrap.querySelectorAll('[data-custom-del]').forEach(btn => {
      btn.addEventListener('click', () => {
        if (!confirm('Delete this custom rule?')) return;
        const ruleId = btn.dataset.customDel;
        state.rules = state.rules.filter(r => r.id !== ruleId);
        saveLS(rulesKey(getActiveTo()), state.rules);
        syncRulesToBackend();
        render();
      });
    });

    // New custom rule
    wrap.querySelector('#newCustomRule')?.addEventListener('click', () => {
      openCustomRuleModal(null);
    });

    // Close modal
    wrap.querySelector('#closeAutoModal')?.addEventListener('click', closeAutoModal);
    wrap.querySelector('#autoEditModal')?.addEventListener('click', (e) => {
      if (e.target.id === 'autoEditModal') closeAutoModal();
    });

    // Category card click to scroll
    wrap.querySelectorAll('.auto-category-card').forEach(card => {
      card.addEventListener('click', () => {
        const category = card.dataset.category;
        const section = wrap.querySelector(`#section-${category}`);
        if (section) section.scrollIntoView({ behavior: 'smooth' });
      });
    });

  }, 0);

  function updateAutomationCounts() {
    // Update header stats
    wrap.querySelector('.auto-stat-value').textContent = state.rules.filter(r => r.enabled).length;
    wrap.querySelectorAll('.auto-stat')[1].querySelector('.auto-stat-value').textContent = state.rules.length;

    // Update category counts
    Object.keys(categoryInfo).forEach(key => {
      const countEl = wrap.querySelector(`.auto-category-card[data-category="${key}"] .auto-cat-count`);
      const sectionCountEl = wrap.querySelector(`#section-${key} .auto-section-count`);
      if (countEl) countEl.textContent = `${getActiveCount(key)}/${getTotalCount(key)}`;
      if (sectionCountEl) sectionCountEl.textContent = `${getActiveCount(key)} of ${getTotalCount(key)} active`;
    });
  }

  function openAutoEditModal(templateId, templates) {
    const modal = wrap.querySelector('#autoEditModal');
    const body = wrap.querySelector('#autoModalBody');

    let template = null;
    Object.values(templates).forEach(cat => {
      const found = cat.find(t => t.id === templateId);
      if (found) template = found;
    });

    if (!template) return;

    const existingRule = state.rules.find(r => r.templateId === templateId);
    const currentMessage = existingRule?.template || template.template;

    body.innerHTML = `
      <div class="auto-edit-form">
        <div class="auto-edit-info">
          <span class="auto-card-icon">${template.icon}</span>
          <div>
            <h3>${template.name}</h3>
            <p>${template.description}</p>
          </div>
        </div>

        <label class="field">
          <span>Message Template</span>
          <textarea class="input" id="editAutoMessage" rows="4">${escapeHtml(currentMessage)}</textarea>
          <span class="field-hint">Variables: {{service}}, {{date}}, {{time}}, {{vehicle}}</span>
        </label>

        <div class="auto-edit-options">
          <label class="toggle">
            <input type="checkbox" id="editBizHours" ${template.businessHoursOnly ? 'checked' : ''} />
            <span>Business hours only</span>
          </label>
          <label class="toggle">
            <input type="checkbox" id="editFirstTime" ${template.firstTimeOnly ? 'checked' : ''} />
            <span>First-time contacts only</span>
          </label>
        </div>

        <div class="auto-edit-actions">
          <button class="btn" id="resetAutoMessage">Reset to Default</button>
          <button class="btn primary" id="saveAutoMessage">Save Changes</button>
        </div>
      </div>
    `;

    modal.classList.remove('hidden');

    // Save handler
    body.querySelector('#saveAutoMessage').addEventListener('click', () => {
      const newMessage = body.querySelector('#editAutoMessage').value.trim();
      const bizHours = body.querySelector('#editBizHours').checked;
      const firstTime = body.querySelector('#editFirstTime').checked;

      let rule = state.rules.find(r => r.templateId === templateId);
      if (!rule) {
        rule = {
          id: crypto.randomUUID(),
          templateId: template.id,
          name: template.name,
          enabled: false,
          trigger: template.trigger,
          businessHoursOnly: bizHours,
          firstTimeOnly: firstTime,
          delayMinutes: template.delayMinutes || 0,
          template: newMessage,
          category: template.category
        };
        state.rules.push(rule);
      } else {
        rule.template = newMessage;
        rule.businessHoursOnly = bizHours;
        rule.firstTimeOnly = firstTime;
      }

      saveLS(rulesKey(getActiveTo()), state.rules);
      syncRulesToBackend();

      // Update preview in card
      const card = wrap.querySelector(`[data-template-id="${templateId}"]`);
      if (card) {
        card.querySelector('.auto-message-preview p').textContent = `"${newMessage}"`;
      }

      closeAutoModal();
    });

    // Reset handler
    body.querySelector('#resetAutoMessage').addEventListener('click', () => {
      body.querySelector('#editAutoMessage').value = template.template;
    });
  }

  function openCustomRuleModal(ruleId) {
    const modal = wrap.querySelector('#autoEditModal');
    const body = wrap.querySelector('#autoModalBody');

    const rule = ruleId ? state.rules.find(r => r.id === ruleId) : null;
    const isNew = !rule;

    body.innerHTML = `
      <div class="auto-edit-form">
        <h3>${isNew ? 'Create Custom Rule' : 'Edit Custom Rule'}</h3>

        <label class="field">
          <span>Rule Name</span>
          <input class="input" id="customRuleName" value="${escapeHtml(rule?.name || '')}" placeholder="e.g., Weekend Special Offer" />
        </label>

        <label class="field">
          <span>Trigger</span>
          <select class="select" id="customRuleTrigger">
            <option value="missed_call" ${rule?.trigger === 'missed_call' ? 'selected' : ''}>Missed Call</option>
            <option value="inbound_sms" ${rule?.trigger === 'inbound_sms' ? 'selected' : ''}>Inbound SMS</option>
            <option value="no_response" ${rule?.trigger === 'no_response' ? 'selected' : ''}>No Response (follow-up)</option>
          </select>
        </label>

        <label class="field">
          <span>Message Template</span>
          <textarea class="input" id="customRuleMessage" rows="4">${escapeHtml(rule?.template || '')}</textarea>
        </label>

        <div class="auto-edit-options">
          <label class="toggle">
            <input type="checkbox" id="customBizHours" ${rule?.businessHoursOnly ? 'checked' : ''} />
            <span>Business hours only</span>
          </label>
          <label class="toggle">
            <input type="checkbox" id="customFirstTime" ${rule?.firstTimeOnly ? 'checked' : ''} />
            <span>First-time contacts only</span>
          </label>
          <label class="toggle">
            <input type="checkbox" id="customEnabled" ${rule?.enabled !== false ? 'checked' : ''} />
            <span>Enabled</span>
          </label>
        </div>

        <div class="auto-edit-actions">
          <button class="btn" id="cancelCustomRule">Cancel</button>
          <button class="btn primary" id="saveCustomRule">${isNew ? 'Create Rule' : 'Save Changes'}</button>
        </div>
      </div>
    `;

    modal.classList.remove('hidden');

    body.querySelector('#saveCustomRule').addEventListener('click', () => {
      const name = body.querySelector('#customRuleName').value.trim() || 'Untitled Rule';
      const trigger = body.querySelector('#customRuleTrigger').value;
      const template = body.querySelector('#customRuleMessage').value.trim();
      const bizHours = body.querySelector('#customBizHours').checked;
      const firstTime = body.querySelector('#customFirstTime').checked;
      const enabled = body.querySelector('#customEnabled').checked;

      if (!template) {
        alert('Please enter a message template');
        return;
      }

      if (isNew) {
        state.rules.push({
          id: crypto.randomUUID(),
          name,
          enabled,
          trigger,
          businessHoursOnly: bizHours,
          firstTimeOnly: firstTime,
          template
        });
      } else {
        const idx = state.rules.findIndex(r => r.id === ruleId);
        if (idx !== -1) {
          state.rules[idx] = { ...state.rules[idx], name, trigger, template, businessHoursOnly: bizHours, firstTimeOnly: firstTime, enabled };
        }
      }

      saveLS(rulesKey(getActiveTo()), state.rules);
      syncRulesToBackend();
      closeAutoModal();
      render();
    });

    body.querySelector('#cancelCustomRule').addEventListener('click', closeAutoModal);
  }

  function closeAutoModal() {
    wrap.querySelector('#autoEditModal')?.classList.add('hidden');
  }

  return wrap;
}

function renderRuleList(){
  const list = $("#ruleList");
  if(!list) return;

  list.innerHTML = state.rules.map(r => `
    <div class="list-item" data-rule="${r.id}">
      <div class="list-left">
        <b>${escapeHtml(r.name || "Untitled rule")}</b>
        <span>${escapeHtml(r.trigger)} | ${r.businessHoursOnly ? "business hours" : "24/7"} | ${r.firstTimeOnly ? "first-time only" : "all callers"}</span>
      </div>

      <div class="row">
        <label class="toggle">
          <input type="checkbox" ${r.enabled ? "checked" : ""} data-enable="${r.id}" />
          enabled
        </label>
        <button class="btn" data-edit="${r.id}">Edit</button>
        <button class="btn" data-del="${r.id}">Delete</button>
      </div>
    </div>
  `).join("");

  $$("input[data-enable]").forEach(inp => {
    inp.addEventListener("change", () => {
      const r = state.rules.find(x => x.id === inp.dataset.enable);
      if(!r) return;
      r.enabled = inp.checked;
      saveLS(LS_KEYS.RULES, state.rules);
      syncRulesToBackend();
    });
  });

  $$("button[data-edit]").forEach(btn => {
    btn.addEventListener("click", () => {
      const r = state.rules.find(x => x.id === btn.dataset.edit);
      renderRuleEditor(structuredClone(r), false);
    });
  });

  $$("button[data-del]").forEach(btn => {
    btn.addEventListener("click", () => {
      const id = btn.dataset.del;
      state.rules = state.rules.filter(x => x.id !== id);
      saveLS(LS_KEYS.RULES, state.rules);
      syncRulesToBackend();
      renderRuleList();
      renderRuleEditor(null);
    });
  });
}

function renderRuleEditor(rule, isNew=false){
  const editor = $("#ruleEditor");
  if(!editor) return;

  if(!rule){
    editor.innerHTML = `<div class="p">Select a rule to edit, or click "New Rule".</div>`;
    return;
  }

  editor.innerHTML = `
    <div class="row" style="justify-content:space-between; flex-wrap:wrap;">
      <div class="col" style="gap:4px;">
        <div class="h1" style="margin:0;">${isNew ? "Create Rule" : "Edit Rule"}</div>
        <div class="p">Trigger -> conditions -> action (send SMS)</div>
      </div>
      <div class="row">
        <button class="btn" id="cancelRule">Cancel</button>
        <button class="btn primary" id="saveRule">Save</button>
      </div>
    </div>

    <div style="height:12px;"></div>

    <div class="grid2">
      <div class="col">
        <label class="p">Rule name</label>
        <input class="input" id="rName" value="${escapeAttr(rule.name)}" placeholder="Missed call -> instant text" />
      </div>

      <div class="col">
        <label class="p">Trigger</label>
        <select class="select" id="rTrigger">
          <option value="missed_call" ${rule.trigger==="missed_call"?"selected":""}>Missed call</option>
          <option value="inbound_sms" ${rule.trigger==="inbound_sms"?"selected":""}>Inbound SMS</option>
        </select>
      </div>
    </div>

    <div style="height:10px;"></div>

    <div class="grid3">
      <label class="toggle" style="align-items:center; justify-content:flex-start;">
        <input type="checkbox" id="rEnabled" ${rule.enabled ? "checked":""} />
        enabled
      </label>

      <label class="toggle">
        <input type="checkbox" id="rBizHours" ${rule.businessHoursOnly ? "checked":""} />
        business hours only
      </label>

      <label class="toggle">
        <input type="checkbox" id="rFirstTime" ${rule.firstTimeOnly ? "checked":""} />
        first-time number only
      </label>
    </div>

    <div style="height:10px;"></div>

    <div class="col">
      <label class="p">Action: SMS template</label>
      <textarea class="input" id="rTemplate" rows="4" style="resize:vertical;">${escapeHtml(rule.template)}</textarea>
      <div class="p">Tip: keep it short. Ask 1 question max.</div>
    </div>
  `;

  $("#cancelRule").addEventListener("click", () => renderRuleEditor(null));

  $("#saveRule").addEventListener("click", () => {
    const updated = {
      ...rule,
      name: $("#rName").value.trim(),
      trigger: $("#rTrigger").value,
      enabled: $("#rEnabled").checked,
      businessHoursOnly: $("#rBizHours").checked,
      firstTimeOnly: $("#rFirstTime").checked,
      sendText: true,
      template: $("#rTemplate").value.trim()
    };

    if(!updated.name) updated.name = "Untitled rule";

    const idx = state.rules.findIndex(x => x.id === updated.id);
    if(idx === -1) state.rules.unshift(updated);
    else state.rules[idx] = updated;

    saveLS(LS_KEYS.RULES, state.rules);
    syncRulesToBackend();
    renderRuleList();
    renderRuleEditor(null);
  });
}

/* Analytics */
/* Analytics */
function defaultAnalyticsSummary(rangeDays = 1){
  const days = [];
  const now = Date.now();
  for (let i = rangeDays - 1; i >= 0; i -= 1) {
    days.push({
      day: new Date(now - (i * 24 * 60 * 60 * 1000)).toISOString().slice(0, 10),
      inboundLeads: 0,
      bookedLeads: 0
    });
  }
  return {
    rangeDays,
    totals: { inboundLeads: 0, respondedConversations: 0, bookedLeads: 0, responseRate: 0, conversionRate: 0 },
    speed: { avgFirstResponseMinutes: null, buckets: { under5: 0, min5to15: 0, over15: 0 } },
    daily: days,
    funnel: { inboundLeads: 0, responded: 0, qualified: 0, booked: 0 }
  };
}

function normalizeAnalyticsSummary(raw, rangeDays){
  const base = defaultAnalyticsSummary(rangeDays);
  const src = raw && typeof raw === "object" ? raw : {};
  const srcDaily = Array.isArray(src.daily) ? src.daily : [];
  const srcDailyByDay = new Map(srcDaily.map((d) => [String(d?.day || ""), d]));
  const safeDaily = base.daily.map((d) => {
    const row = srcDailyByDay.get(String(d.day)) || {};
    return {
      day: String(d.day),
      inboundLeads: Number(row?.inboundLeads || 0),
      bookedLeads: Number(row?.bookedLeads || 0)
    };
  });
  const inboundLeads = Number(src?.totals?.inboundLeads || 0);
  const respondedConversations = Number(src?.totals?.respondedConversations || 0);
  const bookedLeads = Number(src?.totals?.bookedLeads || 0);
  const responseRateRaw = Number(src?.totals?.responseRate);
  const conversionRateRaw = Number(src?.totals?.conversionRate);
  const responseRate = Number.isFinite(responseRateRaw)
    ? responseRateRaw
    : (inboundLeads > 0 ? (respondedConversations / inboundLeads) * 100 : 0);
  const conversionRate = Number.isFinite(conversionRateRaw)
    ? conversionRateRaw
    : (inboundLeads > 0 ? (bookedLeads / inboundLeads) * 100 : 0);
  return {
    rangeDays,
    totals: {
      inboundLeads,
      respondedConversations,
      bookedLeads,
      responseRate: Math.max(0, Math.min(100, responseRate)),
      conversionRate: Math.max(0, Math.min(100, conversionRate))
    },
    speed: {
      avgFirstResponseMinutes: src?.speed?.avgFirstResponseMinutes == null ? null : Number(src.speed.avgFirstResponseMinutes || 0),
      buckets: {
        under5: Number(src?.speed?.buckets?.under5 || 0),
        min5to15: Number(src?.speed?.buckets?.min5to15 || 0),
        over15: Number(src?.speed?.buckets?.over15 || 0)
      }
    },
    daily: safeDaily,
    funnel: {
      inboundLeads: Number(src?.funnel?.inboundLeads || 0),
      responded: Number(src?.funnel?.responded || 0),
      qualified: Number(src?.funnel?.qualified || 0),
      booked: Number(src?.funnel?.booked || 0)
    }
  };
}

function buildAnalyticsStats(summary){
  const inbound = Number(summary?.totals?.inboundLeads || 0);
  const responded = Number(summary?.totals?.respondedConversations || 0);
  const booked = Number(summary?.totals?.bookedLeads || 0);
  const qualified = Number(summary?.funnel?.qualified || responded);
  const maxDaily = Math.max(1, ...summary.daily.map((d) => Math.max(d.inboundLeads, d.bookedLeads)));
  return {
    inbound,
    responded,
    booked,
    responseRate: Math.max(0, Math.min(100, Number(summary?.totals?.responseRate || (inbound ? (responded / inbound) * 100 : 0)))),
    conversionRate: Math.max(0, Math.min(100, Number(summary?.totals?.conversionRate || (inbound ? (booked / inbound) * 100 : 0)))),
    avgResponseTime: summary?.speed?.avgFirstResponseMinutes == null ? null : Number(summary.speed.avgFirstResponseMinutes || 0),
    funnel: {
      leads: inbound,
      responded,
      qualified,
      booked,
      respondedPct: inbound ? Math.round((responded / inbound) * 100) : 0,
      qualifiedPct: inbound ? Math.round((qualified / inbound) * 100) : 0,
      bookedPct: inbound ? Math.round((booked / inbound) * 100) : 0
    },
    dailyPerformance: summary.daily.map((d) => ({
      label: String(d.day || '').slice(5),
      leads: Number(d.inboundLeads || 0),
      booked: Number(d.bookedLeads || 0),
      leadsPct: (Number(d.inboundLeads || 0) / maxDaily) * 100,
      bookedPct: (Number(d.bookedLeads || 0) / maxDaily) * 100
    }))
  };
}

function getAnalyticsScopeKey() {
  const accounts = Array.isArray(authState?.accounts) ? authState.accounts : [];
  const activeTo = String(getActiveTo() || "");
  const activeAccount = accounts.find((acct) => String(acct?.to || "") === activeTo);
  return String(activeAccount?.accountId || activeTo || "default");
}

function viewAnalyticsLegacy(){
  const wrap = document.createElement("div");
  wrap.className = "analytics-page app-view app-view-analytics";
  const rangeDaysRaw = Number(state.analyticsRange || 1);
  const rangeDays = [1, 7, 30].includes(rangeDaysRaw) ? rangeDaysRaw : 1;
  if (rangeDays !== rangeDaysRaw) state.analyticsRange = rangeDays;
  const scopeKey = getAnalyticsScopeKey();
  const activeTo = String(getActiveTo() || "");
  const scopeText = activeTo ? `Scope: ${activeTo}` : `Scope: ${scopeKey}`;
  const cacheKey = `${scopeKey}:${rangeDays}`;
  const cached = state.analyticsSummaryCache?.[cacheKey] || null;
  const summary = cached || defaultAnalyticsSummary(rangeDays);
  let stats = buildAnalyticsStats(summary);
  if ((!Array.isArray(state.threads) || state.threads.length === 0) && !state.analyticsThreadsLoading) {
    state.analyticsThreadsLoading = true;
    loadThreads()
      .catch(() => {})
      .finally(() => {
        state.analyticsThreadsLoading = false;
        if (state.view === "analytics") render();
      });
  }
  state.analyticsBookedFallbackCache = state.analyticsBookedFallbackCache || {};
  state.analyticsBookedFallbackLoading = state.analyticsBookedFallbackLoading || {};
  if ((!Array.isArray(state.threads) || state.threads.length === 0) && !state.analyticsThreadsLoading) {
    state.analyticsThreadsLoading = true;
    loadThreads()
      .catch(() => {})
      .finally(() => {
        state.analyticsThreadsLoading = false;
        if (state.view === "analytics") render();
      });
  }
  if ((!Array.isArray(state.threads) || state.threads.length === 0) && !state.analyticsThreadsLoading) {
    state.analyticsThreadsLoading = true;
    loadThreads()
      .catch(() => {})
      .finally(() => {
        state.analyticsThreadsLoading = false;
        if (state.view === "analytics") render();
      });
  }

  if ((!Array.isArray(state.threads) || state.threads.length === 0) && !state.analyticsThreadsLoading) {
    state.analyticsThreadsLoading = true;
    loadThreads()
      .catch(() => {})
      .finally(() => {
        state.analyticsThreadsLoading = false;
        if (state.view === "analytics") render();
      });
  }

  if (!cached && !state.analyticsLoading) {
    state.analyticsLoading = true;
    state.analyticsError = null;
    apiGet(`/api/analytics/summary?range=${encodeURIComponent(rangeDays)}`)
      .then((res) => {
        state.analyticsSummaryCache[cacheKey] = normalizeAnalyticsSummary(res, rangeDays);
      })
      .catch((err) => {
        state.analyticsSummaryCache[cacheKey] = defaultAnalyticsSummary(rangeDays);
        state.analyticsError = err?.message || "Failed to load analytics";
      })
      .finally(() => {
        state.analyticsLoading = false;
        if (state.view === "analytics") render();
      });
  }

  const revenueCache = state.revenueCache?.[scopeKey] || null;
  const revenueOverview = revenueCache?.overview || {};
  if (!revenueCache && !state.revenueLoading) {
    state.revenueLoading = true;
    state.revenueError = null;
    apiGet("/api/analytics/revenue-overview")
      .then((overview) => {
        state.revenueCache[scopeKey] = { overview };
      })
      .catch((err) => {
        state.revenueError = err?.message || "Failed to load revenue intelligence";
      })
      .finally(() => {
        state.revenueLoading = false;
        if (state.view === "analytics") render();
      });
  }

  const nowMs = Date.now();
  const monthStart = new Date();
  monthStart.setDate(1);
  monthStart.setHours(0, 0, 0, 0);
  const monthStartMs = monthStart.getTime();
  const realtimeThreads = safeArray(state.threads).filter((thread) => {
    const threadTo = String(thread?.to || "").trim();
    return !threadTo || threadTo === activeTo;
  });
  const realtimeBookedRows = realtimeThreads.map((thread) => {
    const status = String(thread?.status || "").toLowerCase();
    const stage = String(thread?.stage || "").toLowerCase();
    const bookedMs = getLatestBookedConfirmationTime(thread);
    const isBooked = status === "booked" || /booked|appointment_booked|scheduled/.test(stage) || (Number.isFinite(bookedMs) && bookedMs > 0);
    if (!isBooked) return null;
    const amount = Number(resolveConversationAmount(thread) || 0);
    if (!Number.isFinite(amount) || amount <= 0) return null;
    const ts = Number(thread?.bookingTime || thread?.leadData?.booking_time || bookedMs || thread?.updatedAt || thread?.lastActivityAt || nowMs) || nowMs;
    return { ts, amountCents: Math.round(amount * 100) };
  }).filter(Boolean);
  const realtimeRecoveredThisMonthCents = realtimeBookedRows.reduce((acc, row) => {
    const ts = Number(row?.ts || 0);
    if (!Number.isFinite(ts) || ts < monthStartMs || ts > nowMs) return acc;
    return acc + Number(row?.amountCents || 0);
  }, 0);

  let bookedRevenueCents = Number(revenueOverview?.recoveredThisMonth || 0);
  if ((!Number.isFinite(bookedRevenueCents) || bookedRevenueCents <= 0) && realtimeRecoveredThisMonthCents > 0) {
    bookedRevenueCents = realtimeRecoveredThisMonthCents;
  }
  const estimatedLostRevenueCents = Math.max(0, Number(revenueOverview?.estimatedLostRevenueCents || 0));
  const recoveredRevenueCents = Math.max(0, Number(revenueOverview?.recoveredRevenueCents || 0));
  const totalLeakPool = estimatedLostRevenueCents + recoveredRevenueCents;
  const leakPct = totalLeakPool > 0 ? Math.round((estimatedLostRevenueCents / totalLeakPool) * 100) : 0;
  const recoveryRate = Number.isFinite(Number(revenueOverview?.revenueRecoveryRate))
    ? Math.round(Number(revenueOverview.revenueRecoveryRate) * 100)
    : 0;
  const responseTimeValue = Number(revenueOverview?.responseTimeAvg || 0);
  const missedCalls = Number(revenueOverview?.missedCallCount || 0);
  const quoteShown = Number(revenueOverview?.quoteShown || 0);
  const quoteAccepted = Number(revenueOverview?.quoteAccepted || 0);
  const allRevenueEventsRaw = safeArray(revenueOverview?.revenueEvents);
  const fallbackRevenueEvents = realtimeBookedRows.map((row) => ({
    type: "appointment_booked",
    status: "won",
    estimatedValueCents: Number(row?.amountCents || 0),
    createdAt: Number(row?.ts || nowMs)
  }));
  const allRevenueEvents = allRevenueEventsRaw.length ? allRevenueEventsRaw : fallbackRevenueEvents;
  const revenueSignals = allRevenueEvents.slice(0, 4);
  const bookedJobsCount = allRevenueEvents.filter((e) => {
    const type = String(e?.type || "").toLowerCase();
    return type === "appointment_booked" || type === "sale_closed";
  }).length;
  const quoteAcceptRate = quoteShown > 0 ? Math.round((quoteAccepted / quoteShown) * 100) : 0;

  const dailyRevenueMap = new Map((summary.daily || []).map((d) => [String(d?.day || ""), { recovered: 0, atRisk: 0 }]));
  allRevenueEvents.forEach((event) => {
    const ts = Number(event?.createdAt || 0);
    const day = Number.isFinite(ts) && ts > 0 ? new Date(ts).toISOString().slice(0, 10) : "";
    if (!day || !dailyRevenueMap.has(day)) return;
    const row = dailyRevenueMap.get(day);
    const valueCents = Math.max(0, Number(event?.estimatedValueCents || 0));
    const type = String(event?.type || "").toLowerCase();
    const status = String(event?.status || "").toLowerCase();
    const recovered = type === "appointment_booked" || type === "sale_closed" || type === "opportunity_recovered" || status === "won";
    if (recovered) row.recovered += valueCents;
    else row.atRisk += valueCents;
  });
  const dailyRevenueRows = (summary.daily || []).map((d) => {
    const day = String(d?.day || "");
    const row = dailyRevenueMap.get(day) || { recovered: 0, atRisk: 0 };
    return {
      label: day.slice(5),
      recoveredCents: Number(row.recovered || 0),
      atRiskCents: Number(row.atRisk || 0)
    };
  });
  const maxDailyRevenueCents = Math.max(1, ...dailyRevenueRows.map((r) => Math.max(r.recoveredCents, 0)));
  const bookedCustomersCount = Math.max(
    new Set(
      threadPillBookedInRangeRows
        .filter((row) => Number(row?.amountCents || 0) > 0)
        .map((row) => String(row?.customerKey || ""))
        .filter(Boolean)
    ).size,
    customerRecoveredInRange.size,
    Math.max(0, Number(detailedFallback?.customerCount || 0)),
    ledgerCustomerCount
  );
  const avgTicketCents = bookedJobsCount > 0 ? Math.round(bookedRevenueCents / bookedJobsCount) : 0;
  const heatmapDayLabels = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"];
  const heatmapBuckets = [
    { label: "6a-9a", start: 6, end: 9 },
    { label: "9a-12p", start: 9, end: 12 },
    { label: "12p-3p", start: 12, end: 15 },
    { label: "3p-6p", start: 15, end: 18 },
    { label: "6p-9p", start: 18, end: 21 },
    { label: "9p-12a", start: 21, end: 24 }
  ];
  const heatmapRowsSource = threadPillRows.filter((row) => {
    const ts = Number(row?.ts || 0);
    const effectiveTs = Number.isFinite(ts) ? Math.min(ts, nowMs) : 0;
    return row?.bookedLike && Number(row?.amountCents || 0) > 0 && Number.isFinite(effectiveTs) && effectiveTs >= rangeStartMs && effectiveTs <= nowMs;
  });
  const heatmapValues = heatmapBuckets.map(() => heatmapDayLabels.map(() => 0));
  heatmapRowsSource.forEach((row) => {
    const ts = Number(row?.ts || 0);
    const effectiveTs = Number.isFinite(ts) ? Math.min(ts, nowMs) : 0;
    if (!(Number.isFinite(effectiveTs) && effectiveTs > 0)) return;
    const dt = new Date(effectiveTs);
    const dayIdx = dt.getDay();
    const hour = dt.getHours();
    const bucketIdx = heatmapBuckets.findIndex((b) => hour >= b.start && hour < b.end);
    if (bucketIdx < 0 || dayIdx < 0 || dayIdx > 6) return;
    heatmapValues[bucketIdx][dayIdx] += Math.max(0, Number(row?.amountCents || 0));
  });
  const heatmapMax = Math.max(1, ...heatmapValues.flat());
  const heatmapEmpty = heatmapRowsSource.length === 0;
  const heatmapGridHtml = `
    <div class="revenue-heatmap-grid">
      <div class="revenue-heatmap-corner"></div>
      ${heatmapDayLabels.map((d) => `<div class="revenue-heatmap-day">${d}</div>`).join("")}
      ${heatmapBuckets.map((bucket, bucketIdx) => `
        <div class="revenue-heatmap-time">${bucket.label}</div>
        ${heatmapDayLabels.map((day, dayIdx) => {
          const cents = Number(heatmapValues[bucketIdx]?.[dayIdx] || 0);
          const pct = Math.max(0, Math.min(1, cents / heatmapMax));
          const alpha = (cents > 0) ? (0.16 + (pct * 0.78)) : 0.06;
          const title = `${day} ${bucket.label}: ${moneyFromCents(cents)}`;
          return `<div class="revenue-heatmap-cell" style="background: rgba(10, 132, 255, ${alpha.toFixed(3)});" title="${escapeAttr(title)}"></div>`;
        }).join("")}
      `).join("")}
    </div>
  `;
  stats = {
    ...stats,
    inbound: Math.round(bookedRevenueCents / 100),
    responseRate: leakPct,
    booked: bookedJobsCount,
    conversionRate: quoteAcceptRate,
    dailyPerformance: dailyRevenueRows.map((r) => ({
      label: r.label,
      leads: Math.round(r.recoveredCents / 100),
      booked: Math.round(r.atRiskCents / 100),
      leadsPct: (r.recoveredCents / maxDailyRevenueCents) * 100,
      bookedPct: (r.atRiskCents / maxDailyRevenueCents) * 100,
      leadsText: moneyFromCents(r.recoveredCents),
      bookedText: moneyFromCents(r.atRiskCents)
    }))
  };
  const empty = stats.inbound === 0 && stats.booked === 0;

  const revenueKpiCards = [
    { label: "Recovered this period", value: moneyFromCents(bookedRevenueCents), meta: `${Math.max(customerRecoveredInRange.size, Number(detailedFallback?.customerCount || 0))} customer totals | ${recoveryRate}% recovery rate` },
    { label: "Revenue at risk", value: moneyFromCents(estimatedLostRevenueCents), meta: `${leakPct}% of the pool` },
    { label: "Avg response time", value: responseTimeValue ? `${responseTimeValue} min` : "ï¿½", meta: `${missedCalls} missed calls` },
    { label: "Quotes shown", value: String(quoteShown), meta: `${quoteAccepted} accepted` }
  ];

  const revenueSignalsHtml = revenueSignals.length
    ? revenueSignals.map((event) => {
      const label = escapeHtml(event.type || event.signalType || "Signal");
      const when = new Date(Number(event.createdAt || 0));
      const timeText = Number(when.getTime()) ? when.toLocaleString() : "soon";
      const valueText = event.estimatedValueCents ? moneyFromCents(Number(event.estimatedValueCents)) : "ï¿½";
      return `
        <div class="revenue-signal">
          <div>
            <div class="revenue-signal-label">${label}</div>
            <div class="muted">${escapeHtml(timeText)}</div>
          </div>
          <div class="revenue-signal-value">${escapeHtml(valueText)}</div>
        </div>
      `;
    }).join("")
    : `<div class="p muted">No revenue signals yet.</div>`;

  wrap.innerHTML = `
    <div class="analytics-header">
      <div class="ops-toolbar-meta">
        <h1>Analytics</h1>
        <p class="text-muted">${escapeHtml(scopeText)}</p>
      </div>
      <div class="analytics-controls ops-toolbar-actions">
        ${RelayUI.renderSegmentedControl({
          activeValue: rangeDays,
          dataAttr: "arange",
          className: "btn-group",
          options: [
            { value: 1, label: "Today" },
            { value: 7, label: "7 days" },
            { value: 30, label: "30 days" }
          ]
        })}
      </div>
    </div>

    <div class="analytics-kpis">
      <div class="analytics-kpi"><div class="kpi-header"><span class="kpi-label">Inbound Leads</span></div><div class="kpi-value" data-count="${stats.inbound}" data-suffix="">0</div></div>
      <div class="analytics-kpi"><div class="kpi-header"><span class="kpi-label">Response Rate</span></div><div class="kpi-value" data-count="${stats.responseRate}" data-suffix="%">0%</div></div>
      <div class="analytics-kpi"><div class="kpi-header"><span class="kpi-label">Booked Leads</span></div><div class="kpi-value" data-count="${stats.booked}" data-suffix="">0</div></div>
      <div class="analytics-kpi"><div class="kpi-header"><span class="kpi-label">Conversion Rate</span></div><div class="kpi-value" data-count="${stats.conversionRate}" data-suffix="%">0%</div></div>
    </div>

    ${(state.analyticsLoading || state.analyticsError || empty) ? `
      <div class="analytics-card" style="margin-bottom:14px;">
        ${RelayUI.renderEmptyState({
          text: state.analyticsLoading
            ? "Loading analytics..."
            : state.analyticsError
              ? state.analyticsError
              : "No activity yet run a webhook simulation or send a test SMS",
          className: "is-compact"
        })}
      </div>
    ` : ''}

    <div class="analytics-grid">
      <div class="analytics-card">
        <div class="card-header"><h2>Lead Funnel</h2><span class="text-muted">Conversion stages</span></div>
        <div class="funnel-chart">
          <div class="funnel-stage"><div class="funnel-bar-wrap"><div class="funnel-bar" style="width:${stats.funnel.leads > 0 ? 100 : 0}%"><span>${stats.funnel.leads}</span></div></div><span class="funnel-label">Inbound Leads</span></div>
          <div class="funnel-stage"><div class="funnel-bar-wrap"><div class="funnel-bar responded" style="width:${stats.funnel.respondedPct}%"><span>${stats.funnel.responded}</span></div></div><span class="funnel-label">Responded (${stats.funnel.respondedPct}%)</span></div>
          <div class="funnel-stage"><div class="funnel-bar-wrap"><div class="funnel-bar qualified" style="width:${stats.funnel.qualifiedPct}%"><span>${stats.funnel.qualified}</span></div></div><span class="funnel-label">Qualified (${stats.funnel.qualifiedPct}%)</span></div>
          <div class="funnel-stage"><div class="funnel-bar-wrap"><div class="funnel-bar booked" style="width:${stats.funnel.bookedPct}%"><span>${stats.funnel.booked}</span></div></div><span class="funnel-label">Booked (${stats.funnel.bookedPct}%)</span></div>
        </div>
      </div>

      <div class="analytics-card">
        <div class="card-header"><h2>Avg Response Time</h2><span class="text-muted">Time to first reply</span></div>
        <div class="response-time-display">
          <div class="response-time-value" data-count="${stats.avgResponseTime == null ? 0 : stats.avgResponseTime}">${stats.avgResponseTime == null ? "" : "0"}</div>
          <div class="response-time-unit">minutes</div>
          <div class="response-breakdown">
            <div class="breakdown-item"><span class="breakdown-dot excellent"></span><span>&lt; 5 min</span><span class="breakdown-value">${summary.speed.buckets.under5}</span></div>
            <div class="breakdown-item"><span class="breakdown-dot good"></span><span>5-15 min</span><span class="breakdown-value">${summary.speed.buckets.min5to15}</span></div>
            <div class="breakdown-item"><span class="breakdown-dot slow"></span><span>&gt; 15 min</span><span class="breakdown-value">${summary.speed.buckets.over15}</span></div>
          </div>
        </div>
      </div>

      <div class="analytics-card wide">
        <div class="card-header"><h2>Daily Performance</h2><div class="chart-legend"><span class="legend-item"><span class="legend-dot leads"></span> Leads</span><span class="legend-item"><span class="legend-dot booked"></span> Booked</span></div></div>
        <div class="daily-chart">
          ${stats.dailyPerformance.map(d => `
            <div class="daily-bar-group"><div class="daily-bars"><div class="daily-bar leads" style="height:${d.leadsPct}%" title="${d.leads} leads"></div><div class="daily-bar booked" style="height:${d.bookedPct}%" title="${d.booked} booked"></div></div><span class="daily-label">${d.label}</span></div>
          `).join('')}
        </div>
      </div>
    </div>
    <div style="height:14px;"></div>
    <div class="analytics-card wide">
      <div class="card-header">
        <h2>Revenue Intelligence</h2>
        <span class="text-muted">${state.revenueLoading ? "Loading..." : (state.revenueError ? escapeHtml(state.revenueError) : `${revenueSignals.length} signals`)}</span>
      </div>
      ${state.revenueLoading && !revenueCache ? `
        <div class="p">Loading revenue intelligence...</div>
      ` : `
        <div class="revenue-kpi-stack">
          ${revenueKpiCards.map((card) => `
            <div class="revenue-kpi-card">
              <div class="summary-label">${escapeHtml(card.label)}</div>
              <div class="summary-value">${escapeHtml(card.value)}</div>
              <div class="summary-meta">${escapeHtml(card.meta)}</div>
            </div>
          `).join("")}
        </div>
        <div style="height:10px;"></div>
        <div class="revenue-signal-list">
          ${revenueSignalsHtml}
        </div>
      `}
    </div>
  `;

  wrap.querySelectorAll('[data-arange]').forEach(btn => {
    btn.addEventListener('click', () => {
      state.analyticsRange = Number(btn.dataset.arange);
      localStorage.setItem(ANALYTICS_RANGE_KEY, String(state.analyticsRange));
      state.analyticsError = null;
      render();
    });
  });

  setTimeout(() => {
    wrap.querySelectorAll('.kpi-value[data-count]').forEach(el => {
      animateCounter(el, 0, parseFloat(el.dataset.count), 800, el.dataset.suffix || '');
    });
    if (stats.avgResponseTime != null) {
      wrap.querySelectorAll('.response-time-value[data-count]').forEach(el => {
        animateCounter(el, 0, parseFloat(el.dataset.count), 1000, '');
      });
    }
  }, 300);

  return wrap;
}

// Counter animation function
function animateCounter(element, start, end, duration, suffix) {
  const startTime = performance.now();
  const isFloat = !Number.isInteger(end);

  function update(currentTime) {
    const elapsed = currentTime - startTime;
    const progress = Math.min(elapsed / duration, 1);

    // Easing function (ease-out cubic)
    const easeOut = 1 - Math.pow(1 - progress, 3);

    const current = start + (end - start) * easeOut;
    element.textContent = (isFloat ? current.toFixed(0) : Math.floor(current)) + suffix;

    if (progress < 1) {
      requestAnimationFrame(update);
    } else {
      element.textContent = (isFloat ? end.toFixed(0) : end) + suffix;
    }
  }

  requestAnimationFrame(update);
}

function computeAnalyticsData(){
  const rangeDaysRaw = Number(state.analyticsRange || 1);
  const rangeDays = [1, 7, 30].includes(rangeDaysRaw) ? rangeDaysRaw : 1;
  const cacheKey = `${getAnalyticsScopeKey()}:${rangeDays}`;
  return buildAnalyticsStats(state.analyticsSummaryCache?.[cacheKey] || defaultAnalyticsSummary(rangeDays));
}

function moneyFromCents(cents) {
  const n = Number(cents || 0);
  if (!Number.isFinite(n)) return "$0";
  return `$${(n / 100).toLocaleString(undefined, { maximumFractionDigits: 0 })}`;
}

function deriveRecoveryMetricsFromOverview(overview = {}) {
  const recoveredThisMonthCents = Math.max(0, Number(overview?.recoveredThisMonth || 0));
  const revenueEvents = Array.isArray(overview?.revenueEvents) ? overview.revenueEvents : [];
  const missedCallsConverted = revenueEvents.filter((e) => {
    const signal = String(e?.signalType || "").toLowerCase();
    const type = String(e?.type || "").toLowerCase();
    const status = String(e?.status || "").toLowerCase();
    const fromMissedCall = signal.includes("missed_call");
    const converted = ["opportunity_recovered", "appointment_booked", "sale_closed"].includes(type) || status === "won";
    return fromMissedCall && converted;
  }).length;
  const bookedAppointments = revenueEvents.filter((e) => String(e?.type || "").toLowerCase() === "appointment_booked").length;
  return {
    recoveredThisMonthText: moneyFromCents(recoveredThisMonthCents),
    missedCallsConvertedText: String(missedCallsConverted),
    bookedAppointmentsText: String(bookedAppointments)
  };
}

function deriveTopbarMetrics(overview = {}, options = {}) {
  const nowMs = Date.now();
  const now = new Date(nowMs);
  const rangeDays = 30;
  const rangeStartMs = nowMs - ((rangeDays - 1) * 24 * 60 * 60 * 1000);
  const todayStartMs = new Date(now.getFullYear(), now.getMonth(), now.getDate()).getTime();
  const tomorrowStartMs = todayStartMs + (24 * 60 * 60 * 1000);
  const activeTo = String(getActiveTo() || "").trim();
  const bookedRows = safeArray(state.threads).filter((thread) => {
    const to = String(thread?.to || "").trim();
    return !activeTo || !to || to === activeTo;
  }).map((thread) => {
    const normalizedThread = {
      ...thread,
      leadData: { ...(thread?.leadData || {}) }
    };
    applyRealtimeBookedState(normalizedThread, { refreshRecovered: false });
    const ld = normalizedThread?.leadData || {};
    const status = String(normalizedThread?.status || "").toLowerCase();
    const stage = String(normalizedThread?.stage || "").toLowerCase();
    const snippet = String(normalizedThread?.lastText || "").toLowerCase();
    const bookedDividerMs = getLatestBookedConfirmationTime(normalizedThread);
    const bookedPersistedMs = coerceTimestampMs(normalizedThread?.bookingTime || ld?.booking_time, 0);
    const amount = Number(resolveConversationAmount(normalizedThread) || 0);
    const amountCents = Number.isFinite(amount) && amount > 0 ? Math.round(amount * 100) : 0;
    const bookedLike = status === "booked"
      || status === "closed"
      || /booked|appointment_booked|scheduled/.test(stage)
      || /booked\s*:|i see you booked|appointment booked|scheduled/.test(snippet)
      || (Number.isFinite(bookedDividerMs) && bookedDividerMs > 0)
      || (Number.isFinite(bookedPersistedMs) && bookedPersistedMs > 0);
    if (!bookedLike || !(amountCents > 0)) return null;
    const ts = coerceTimestampMs(
      bookedPersistedMs || bookedDividerMs || normalizedThread?.updatedAt || normalizedThread?.lastActivityAt || normalizedThread?.bookingTime || ld?.booking_time,
      0
    );
    if (!(Number.isFinite(ts) && ts > 0)) return null;
    return {
      ts: Math.min(ts, nowMs),
      amountCents: Math.max(0, Number(amountCents || 0)),
      customerKey: String(normalizedThread?.from || normalizedThread?.phone || "").trim().toLowerCase()
    };
  }).filter(Boolean);

  const recoveredInRangeCents = bookedRows.reduce((acc, row) => {
    const ts = Number(row?.ts || 0);
    if (!Number.isFinite(ts) || ts < rangeStartMs || ts > nowMs) return acc;
    return acc + Math.max(0, Number(row?.amountCents || 0));
  }, 0);
  const allBookedRecoveredCents = bookedRows.reduce((acc, row) => acc + Math.max(0, Number(row?.amountCents || 0)), 0);
  const bookedTodayCount = bookedRows.filter((row) => {
    const ts = Number(row?.ts || 0);
    return Number.isFinite(ts) && ts >= todayStartMs && ts < tomorrowStartMs;
  }).length;
  const bookedInRangeCount = bookedRows.filter((row) => {
    const ts = Number(row?.ts || 0);
    return Number.isFinite(ts) && ts >= rangeStartMs && ts <= nowMs;
  }).length;
  const allBookedCount = bookedRows.length;
  const bookedCountDisplay = bookedInRangeCount > 0 ? bookedInRangeCount : allBookedCount;
  const savedContactSet = options?.savedContactSet instanceof Set
    ? options.savedContactSet
    : new Set(safeArray(options?.savedContactPhones).map((p) => normalizePhone(String(p || ""))).filter(Boolean));
  const todayTexterSet = new Set();
  const allTexterSet = new Set();
  safeArray(state.threads).forEach((thread) => {
    const to = String(thread?.to || "").trim();
    if (activeTo && to && to !== activeTo) return;
    const from = normalizePhone(String(thread?.from || thread?.phone || "").trim());
    if (!from) return;
    allTexterSet.add(from);
    const ts = coerceTimestampMs(thread?.updatedAt || thread?.lastActivityAt || thread?.createdAt, 0);
    if (!(Number.isFinite(ts) && ts >= todayStartMs && ts < tomorrowStartMs)) return;
    todayTexterSet.add(from);
  });
  const newCustomersTodayCountRaw = Array.from(todayTexterSet).filter((phone) => !savedContactSet.has(phone)).length;
  const newCustomersFallback = Array.from(allTexterSet).filter((phone) => !savedContactSet.has(phone)).length;
  const newCustomersTodayCount = newCustomersTodayCountRaw > 0 ? newCustomersTodayCountRaw : newCustomersFallback;
  const fallbackRecovery = deriveRecoveryMetricsFromOverview(overview || {});
  return {
    line1Value: (recoveredInRangeCents > 0 ? moneyFromCents(recoveredInRangeCents) : (allBookedRecoveredCents > 0 ? moneyFromCents(allBookedRecoveredCents) : fallbackRecovery.recoveredThisMonthText)),
    line1Label: "recovered this month",
    line2Value: String(bookedCountDisplay),
    line2Label: "booked this month",
    line3Value: String(newCustomersTodayCount),
    line3Label: "new customers this month"
  };
}
function safeArray(value) {
  return Array.isArray(value) ? value : [];
}

function computeAnalyticsBookedFallbackFromThreads({ threads, activeTo, rangeStartMs, nowMs }) {
  const source = safeArray(threads).filter((thread) => {
    const to = String(thread?.to || "").trim();
    return !activeTo || !to || to === activeTo;
  });
  let sumCents = 0;
  let bookedCount = 0;
  const customers = new Set();
  source.forEach((thread) => {
    const snapshot = {
      ...thread,
      leadData: { ...(thread?.leadData || {}) }
    };
    applyRealtimeBookedState(snapshot, { refreshRecovered: false });
    const status = String(snapshot?.status || "").toLowerCase();
    const stage = String(snapshot?.stage || "").toLowerCase();
    const bookedDividerMs = getLatestBookedConfirmationTime(snapshot);
    const bookedMs = coerceTimestampMs(
      snapshot?.bookingTime || snapshot?.leadData?.booking_time || bookedDividerMs || snapshot?.updatedAt || snapshot?.lastActivityAt,
      0
    );
    const snippet = String(snapshot?.lastText || "").toLowerCase();
    const bookedLike = status === "booked"
      || status === "closed"
      || /booked|appointment_booked|scheduled/.test(stage)
      || /booked\s*:|i see you booked|appointment booked|scheduled/.test(snippet)
      || (Number.isFinite(bookedDividerMs) && bookedDividerMs > 0)
      || (Number.isFinite(bookedMs) && bookedMs > 0);
    if (!bookedLike) return;
    if (!Number.isFinite(bookedMs) || bookedMs < rangeStartMs || bookedMs > nowMs) return;
    const amount = Number(resolveConversationAmount(snapshot) || 0);
    let amountCents = Number.isFinite(amount) && amount > 0 ? Math.round(amount * 100) : 0;
    if (!(amountCents > 0)) {
      const pill = buildConversationMoneyPill(snapshot);
      amountCents = parseMoneyLabelToCents(pill?.label);
    }
    if (amountCents > 0) sumCents += amountCents;
    bookedCount += 1;
    const customerKey = String(snapshot?.from || snapshot?.phone || "").trim().toLowerCase();
    if (customerKey) customers.add(customerKey);
  });
  return {
    sumCents,
    bookedCount,
    customerCount: customers.size,
    computedAt: Date.now(),
    threadCount: source.length,
    hasData: bookedCount > 0 || sumCents > 0 || customers.size > 0
  };
}

function viewRevenue() {
  const wrap = document.createElement("div");
  wrap.className = "revenue-simple-page app-view app-view-revenue";
  const scopeKey = getAnalyticsScopeKey();
  const cache = state.revenueCache?.[scopeKey] || null;
  const loading = state.revenueLoading && !cache;
  const error = state.revenueError;

  if (!cache && !state.revenueLoading) {
    state.revenueLoading = true;
    state.revenueError = null;
    apiGet("/api/analytics/revenue-overview")
      .then((overview) => {
        state.revenueCache[scopeKey] = { overview };
      })
      .catch((err) => {
        state.revenueError = err?.message || "Failed to load revenue intelligence";
      })
      .finally(() => {
        state.revenueLoading = false;
        if (state.view === "revenue") render();
      });
  }

  const overview = cache?.overview || {};
  const bookedRevenueCents = Number(overview?.recoveredThisMonth || 0);
  const estimatedLostRevenueCents = Math.max(0, Number(overview?.estimatedLostRevenueCents || 0));
  const recoveredRevenueCents = Math.max(0, Number(overview?.recoveredRevenueCents || 0));
  const totalLeakPool = estimatedLostRevenueCents + recoveredRevenueCents;
  const leakPct = totalLeakPool > 0 ? Math.round((estimatedLostRevenueCents / totalLeakPool) * 100) : 0;
  const recoveryRate = Number.isFinite(Number(overview?.revenueRecoveryRate))
    ? Math.round(Number(overview.revenueRecoveryRate) * 100)
    : 0;
  const responseTimeValue = Number(overview?.responseTimeAvg || 0);
  const missedCalls = Number(overview?.missedCallCount || 0);
  const quoteShown = Number(overview?.quoteShown || 0);
  const quoteAccepted = Number(overview?.quoteAccepted || 0);
  const signals = safeArray(overview?.revenueEvents).slice(0, 3);

  const kpiCards = [
    { label: "Recovered this month", value: moneyFromCents(bookedRevenueCents), meta: `${recoveryRate}% recovery rate` },
    { label: "Revenue at risk", value: moneyFromCents(estimatedLostRevenueCents), meta: `${leakPct}% of the pool` },
    { label: "Avg response time", value: responseTimeValue ? `${responseTimeValue} min` : "ï¿½", meta: `${missedCalls} missed calls` },
    { label: "Quotes shown", value: String(quoteShown), meta: `${quoteAccepted} accepted` }
  ];

  const signalsHtml = signals.length
    ? signals.map((event) => {
      const label = escapeHtml(event.type || event.signalType || "Signal");
      const when = new Date(Number(event.createdAt || 0));
      const timeText = Number(when.getTime()) ? when.toLocaleString() : "soon";
      const valueText = event.estimatedValueCents ? moneyFromCents(Number(event.estimatedValueCents)) : "ï¿½";
      return `
        <div class="revenue-signal">
          <div>
            <div class="revenue-signal-label">${label}</div>
            <div class="muted">${escapeHtml(timeText)}</div>
          </div>
          <div class="revenue-signal-value">${escapeHtml(valueText)}</div>
        </div>
      `;
    }).join("")
    : `<div class="p muted">No revenue signals yet.</div>`;

  const kpiHtml = kpiCards.map((card) => `
    <div class="revenue-kpi-card">
      <div class="summary-label">${escapeHtml(card.label)}</div>
      <div class="summary-value">${escapeHtml(card.value)}</div>
      <div class="summary-meta">${escapeHtml(card.meta)}</div>
    </div>
  `).join("");

  const scopeLabel = scopeKey ? `Scope: ${escapeHtml(scopeKey)}` : "Workspace scope";
  const headerMeta = loading
    ? '<span class="badge">Refreshing</span>'
    : error
      ? `<span class="muted small">${escapeHtml(error)}</span>`
      : '<span class="badge" style="background:rgba(175,255,187,0.25);">Signals ready</span>';
  const onboardingNotice = state.onboardingRequired
    ? `
      <div class="revenue-simple-section revenue-onboarding-section">
        <div class="revenue-section-head">
          <h2>Complete setup</h2>
          <button class="btn btn-sm" type="button" data-go-to-onboarding>Continue onboarding</button>
        </div>
        <div class="p muted">Connect your booking link and automate missed calls to capture more jobs.</div>
      </div>
    `
    : "";

  wrap.innerHTML = `
    <div class="revenue-simple-header">
      <div>
        <h1>Revenue</h1>
        <p class="text-muted">Clean booking signals (${escapeHtml(scopeLabel)}).</p>
      </div>
      <div class="revenue-header-meta">
        ${headerMeta}
      </div>
    </div>
    ${loading ? `<div class="p">Loading revenue intelligenceï¿½</div>` : error ? `<div class="p muted">${escapeHtml(error)}</div>` : `
      <div class="revenue-kpi-stack">
        ${kpiHtml}
      </div>
      <div class="revenue-simple-section">
        <div class="revenue-section-head">
          <h2>Recent revenue signals</h2>
          <span class="muted small">${signals.length} captured</span>
        </div>
        <div class="revenue-signal-list">
          ${signalsHtml}
        </div>
      </div>
    `}
    ${onboardingNotice}
  `;
  if (state.onboardingRequired) {
    wrap.querySelector("[data-go-to-onboarding]")?.addEventListener("click", () => {
      state.view = "onboarding";
      render();
    });
  }
  return wrap;
}

function viewMessages(){
  const wrap = document.createElement("div");
  wrap.className = "col app-view app-view-messages";
  const showDevSimulatorQuickBtn = canAccessDeveloperRoutes();

  const inbox = document.createElement("div");
  inbox.className = "inbox";

  inbox.innerHTML = `
    <div class="panel">
      <div class="panel-head">
        <div class="row">
          <input class="input" id="threadSearch" placeholder="Search threads..." />
          <button class="btn" id="refreshThreads" type="button">Refresh</button>
        </div>
      </div>
      <div class="panel-body thread-list-panel" id="threadList">
        <div class="thread-list-skeleton" aria-hidden="true">
          <div class="thread-skeleton-row skeleton"></div>
          <div class="thread-skeleton-row skeleton"></div>
          <div class="thread-skeleton-row skeleton"></div>
          <div class="thread-skeleton-row skeleton"></div>
        </div>
      </div>
      ${showDevSimulatorQuickBtn ? `
      <div class="panel-head" style="border-top:1px solid var(--border); justify-content:flex-end;">
        <button class="btn" id="messagesDevSimBtn" type="button">Simulate Conversation</button>
      </div>
      ` : ""}
    </div>

    <div class="panel chat">
      <div class="chat-head" id="chatHead">
        <div class="chat-title">
          <b>Select a conversation</b>
          <span>Choose a thread to review the timeline and lead context.</span>
        </div>
      </div>
      <div class="bubbles" id="bubbles">
        ${RelayUI.renderEmptyState({
          title: "No conversation selected",
          text: "Select a thread to review messages, booking signals, and lead details.",
          className: "messages-empty-state"
        })}
      </div>
      <div class="chat-foot">
        <input class="input" id="chatInput" placeholder="Type a message..." />
        <button class="btn primary" id="sendBtn" type="button" >Send</button>
      </div>
      <div class="row" id="sendConsentWrap" style="display:none; gap:10px; align-items:center; padding:8px 12px;">
        <label class="toggle" style="flex:1;">
          <input type="checkbox" id="sendConsentCheck" />
          <span id="sendConsentText">I confirm I have consent to text this contact.</span>
        </label>
        <select class="select" id="sendConsentSource" style="max-width:200px;">
          <option value="verbal">verbal</option>
          <option value="form">form</option>
          <option value="existing_customer">existing_customer</option>
          <option value="other">other</option>
        </select>
      </div>
    </div>

    <div class="lead-panel">
      <div class="lead-head">
        <p class="lead-title">Lead Details</p>
        <p class="lead-sub" id="leadSub">?</p>
      </div>
      <div class="lead-body" id="leadDetails">
        ${RelayUI.renderEmptyState({
          title: "No lead selected",
          text: "Select a conversation to review lead details, recovery status, and compliance actions.",
          className: "messages-empty-state lead-empty-state"
        })}
      </div>
    </div>

    <div id="leadComplianceModal" class="billing-modal-overlay hidden" aria-hidden="true">
      <div class="billing-modal" role="dialog" aria-modal="true" aria-labelledby="leadComplianceTitle">
        <div class="row" style="justify-content:space-between; align-items:flex-start; gap:10px;">
          <div class="col" style="gap:4px;">
            <div class="h1" id="leadComplianceTitle" style="margin:0;">Compliance</div>
            <div class="p">Per-tenant SMS compliance controls for opt-out, consent, and retention.</div>
          </div>
          <button class="btn" id="leadComplianceCloseBtn" type="button" aria-label="Close compliance modal">Close</button>
        </div>

        <div style="height:14px;"></div>

        <div class="grid2">
          <div class="card" style="background:var(--panel);">
            <div class="h1" style="margin:0;">STOP behavior</div>
            <label class="toggle"><input type="checkbox" id="leadCmpStopEnabled" /> Enable STOP keyword handling</label>
            <label class="toggle"><input type="checkbox" id="leadCmpStopAutoReply" /> Auto-reply on opt-out</label>
            <div class="col">
              <label class="p">STOP keywords (comma-separated)</label>
              <input class="input" id="leadCmpStopKeywords" placeholder="STOP,UNSUBSCRIBE,CANCEL,END,QUIT" />
            </div>
            <div class="col">
              <label class="p">HELP keywords (comma-separated)</label>
              <input class="input" id="leadCmpHelpKeywords" placeholder="HELP,INFO" />
            </div>
            <div class="col">
              <label class="p">Auto-reply text</label>
              <input class="input" id="leadCmpStopAutoReplyText" />
            </div>
          </div>

          <div class="card" style="background:var(--panel);">
            <div class="h1" style="margin:0;">Opt-out enforcement</div>
            <label class="toggle"><input type="checkbox" id="leadCmpOptOutEnforce" /> Block outbound to opted-out contacts</label>
            <label class="toggle"><input type="checkbox" id="leadCmpAllowTransactional" /> Allow transactional messages to opted-out contacts</label>
            <div class="col">
              <label class="p">DNR tag</label>
              <input class="input" id="leadCmpStoreAsTag" placeholder="DNR" />
            </div>
            <div class="col">
              <label class="p">Resubscribe keywords (comma-separated)</label>
              <input class="input" id="leadCmpResubKeywords" placeholder="START,UNSTOP,YES" />
            </div>
          </div>
        </div>

        <div style="height:12px;"></div>

        <div class="grid2">
          <div class="card" style="background:var(--panel);">
            <div class="h1" style="margin:0;">Consent</div>
            <label class="toggle"><input type="checkbox" id="leadCmpConsentRequired" /> Require consent for outbound messages</label>
            <div class="col">
              <label class="p">Compose checkbox text</label>
              <input class="input" id="leadCmpConsentCheckboxText" />
            </div>
            <div class="col">
              <label class="p">Consent source options (comma-separated)</label>
              <input class="input" id="leadCmpConsentSourceOptions" placeholder="verbal,form,existing_customer,other" />
            </div>
          </div>

          <div class="card" style="background:var(--panel);">
            <div class="h1" style="margin:0;">Retention</div>
            <label class="toggle"><input type="checkbox" id="leadCmpRetentionEnabled" /> Enable message log retention</label>
            <label class="toggle"><input type="checkbox" id="leadCmpRetentionSchedule" /> Purge on schedule</label>
            <div class="col">
              <label class="p">Message log days (7-365)</label>
              <input class="input" id="leadCmpRetentionDays" type="number" min="7" max="365" />
            </div>
            <div class="row" style="justify-content:flex-end;">
              <button class="btn" id="leadCmpRunPurgeNowBtn" type="button">Run purge now</button>
            </div>
          </div>
        </div>

        <div style="height:12px;"></div>

        <div class="card" style="background:var(--panel);">
          <div class="h1" style="margin:0;">Compliance status</div>
          <div class="p" id="leadCmpStatusCard">Loading...</div>
        </div>

        <div style="height:12px;"></div>
        <div class="row" style="justify-content:flex-end; align-items:center; gap:10px;">
          <div class="p" id="leadCmpSaveStatus" style="min-height:18px; margin-right:auto;"></div>
          <button class="btn primary" id="leadCmpSaveBtn" type="button">Save compliance settings</button>
        </div>
      </div>
    </div>
  `;

  wrap.appendChild(inbox);
  const threadListEl = inbox.querySelector("#threadList");
  const bubblesEl = inbox.querySelector("#bubbles");
  const threadSearchEl = inbox.querySelector("#threadSearch");
  const refreshThreadsBtn = inbox.querySelector("#refreshThreads");
  const sendBtn = inbox.querySelector("#sendBtn");
  const chatInput = inbox.querySelector("#chatInput");
  const devSimBtn = inbox.querySelector("#messagesDevSimBtn");
  const debouncedThreadSearch = debounce((value) => {
    renderThreadListFromAPI(String(value || "").trim());
  }, 90);

  renderThreadListFromAPI("").catch((err) => console.error("initial thread list render failed:", err));
  renderChatFromAPI();

  sendBtn?.addEventListener("click", sendDashboardMessage);
  chatInput?.addEventListener("keydown", (e) => {
    if (e.key !== "Enter") return;
    e.preventDefault();
    sendDashboardMessage(e);
  });

  threadSearchEl?.addEventListener("input", (e) => {
    debouncedThreadSearch(e.target?.value || "");
  });

  threadListEl?.addEventListener("click", async (event) => {
    const threadEl = event.target.closest(".thread[data-thread]");
    if (!threadEl || !threadListEl.contains(threadEl)) return;
    const nextThreadId = String(threadEl.dataset.thread || "").trim();
    if (!nextThreadId) return;
    state.activeThreadId = nextThreadId;
    if (!hasFreshConversationState(state.activeThreadId)) {
      await loadConversation(state.activeThreadId);
    }
    renderThreadListFromAPI(threadSearchEl?.value?.trim() || "");
    renderChatFromAPI();
  });

  bubblesEl?.addEventListener("click", (event) => {
    const slotLink = event.target.closest(".bubble-slot-link[data-schedule-ms]");
    if (slotLink && bubblesEl.contains(slotLink)) {
      event.stopPropagation();
      const targetMs = Number(slotLink.dataset.scheduleMs || 0);
      if (Number.isFinite(targetMs) && targetMs > 0) focusScheduleOnDate(targetMs);
      return;
    }
    const bubbleEl = event.target.closest(".bubble-wrapper[data-msg-index]");
    if (!bubbleEl || !bubblesEl.contains(bubbleEl)) return;
    const convoId = String(state.activeConversation?.id || "").trim();
    const idx = Number(bubbleEl.getAttribute("data-msg-index"));
    if (!convoId || !Number.isFinite(idx)) return;
    const current = Number(state.chatExpandedByConversation?.[convoId]);
    state.chatExpandedByConversation = {
      ...(state.chatExpandedByConversation || {}),
      [convoId]: current === idx ? -1 : idx
    };
    renderChatFromAPI();
  });

  refreshThreadsBtn?.addEventListener("click", async () => {
    const searchValue = threadSearchEl?.value?.trim() || "";
    const originalText = refreshThreadsBtn?.textContent || "Refresh";
    try {
      refreshThreadsBtn.disabled = true;
      refreshThreadsBtn.textContent = "Refreshing...";
      await loadThreads({ force: true });
      if (state.activeThreadId) {
        await loadConversation(state.activeThreadId, { force: true });
      }
      renderThreadListFromAPI(searchValue);
      renderChatFromAPI();
    } finally {
      refreshThreadsBtn.disabled = false;
      refreshThreadsBtn.textContent = originalText;
    }
  });

  devSimBtn?.addEventListener("click", async () => {
    const originalText = devSimBtn?.textContent || "Simulate Conversation";
    if (devSimBtn) {
      devSimBtn.disabled = true;
      devSimBtn.textContent = "Simulating...";
    }
    try {
      await runDeveloperConversationSimulation();
    } catch (err) {
      console.error("simulate conversation failed:", err);
    } finally {
      if (devSimBtn) {
        devSimBtn.disabled = false;
        devSimBtn.textContent = originalText;
      }
    }
  });

  initLeadComplianceModal();

  return wrap;
}



function renderChat(){
  const t = state.threads.find(x => x.id === state.activeThreadId);
  if(!t) return;

  const chatHead = $("#chatHead");
  const bubbles = $("#bubbles");
  if (!chatHead || !bubbles) {
    return;
  }

  const isVip = state.vip.some(v => normalizePhone(v.phone) === normalizePhone(t.phone) && v.neverAutoReply);

  chatHead.innerHTML = `
    <div class="chat-title">
      <b>${escapeHtml(t.name)}</b>
      <span>${escapeHtml(t.phone)} ${isVip ? " | VIP (never auto-reply)" : ""}</span>
    </div>
    <div class="row">
      <button class="btn" id="markVipBtn">${isVip ? "Remove VIP" : "Add VIP"}</button>
    </div>
  `;

  bubbles.innerHTML = t.messages.map((m, i) => {
    const cls = m.dir === "out" ? "out" : "in";
    const ts = formatMessageTimestamp(m);
    const bookingAction = getBookingActionForMessage(m);
    const bookingButton = bookingAction
      ? `<button type="button" class="bubble-slot-link" data-schedule-ms="${escapeAttr(String(bookingAction.dateMs || ""))}">${escapeHtml(bookingAction.label)}</button>`
      : "";
    const messageStatus = String(m?.status || "").trim().toLowerCase();
    const statusLabel = cls === "out" ? prettyLifecycleLabel(messageStatus) : "";
    const statusTone = getMessageStatusTone(messageStatus);
    const bubbleMeta = (ts || statusLabel) ? `
      <div class="bubble-meta ${cls}">
        ${ts ? `<span class="bubble-ts-inline">${escapeHtml(ts)}</span>` : ""}
        ${statusLabel ? `<span class="bubble-status ${escapeAttr(statusTone)}">${escapeHtml(statusLabel)}</span>` : ""}
      </div>
    ` : "";
    return `
      <div class="bubble-wrapper ${cls}" data-msg-index="${i}">
        <div class="bubble ${cls} ${m.isNew ? "new" : ""}">
          <div class="bubble-text">${escapeHtml(m.text)}</div>
          ${bookingButton}
        </div>
        ${bubbleMeta}
      </div>
    `;
  }).join("");

  bubbles.scrollTop = bubbles.scrollHeight;

  // Clear new flags so they don't re-animate on re-render
  t.messages.forEach(m => delete m.isNew);

  $("#markVipBtn").addEventListener("click", () => {
    toggleVIPFromThread(t);
    renderChat();
  });
}




function sendChatMessage(){
  const input = $("#chatInput");
  const text = input.value.trim();
  if(!text) return;

  const t = state.threads.find(x => x.id === state.activeThreadId);
  if(!t) return;
  
  t.messages.push({ dir:"out", text, ts:"Now", isNew:true });
  input.value = "";
  renderThreadList($("#threadSearch")?.value?.trim() || "");
  renderChat();
}

function toggleVIPFromThread(thread){
  const phoneKey = normalizePhone(thread.phone);
  const existing = state.vip.find(v => normalizePhone(v.phone) === phoneKey);

  if(existing){
    // toggle neverAutoReply; if turning off, remove from list
    if(existing.neverAutoReply){
      state.vip = state.vip.filter(v => v.id !== existing.id);
    }else{
      existing.neverAutoReply = true;
    }
  }else{
    state.vip.unshift({
      id: crypto.randomUUID(),
      name: thread.name === "Unknown" ? "VIP" : thread.name,
      phone: thread.phone,
      neverAutoReply: true
    });
  }
  saveLS(LS_KEYS.VIP, state.vip);
  syncVipToBackend();
}

/* VIP */
function viewVIP(){
  const wrap = document.createElement("div");
  wrap.className = "col";
  wrap.appendChild(headerCard("VIP", "Add/remove/search numbers that should NEVER receive auto-replies."));

  const card = document.createElement("div");
  card.className = "card";
  card.innerHTML = `
    <div class="grid2">
      <div class="col">
        <label class="p">Search</label>
        <input class="input" id="vipSearch" placeholder="Name or phone..." />
      </div>
      <div class="col">
        <label class="p">Add VIP</label>
        <div class="row">
          <input class="input" id="vipName" placeholder="Name (optional)" />
          <input class="input" id="vipPhone" placeholder="Phone (ex: 814-555-1234)" />
          <button class="btn primary" id="vipAdd">Add</button>
        </div>
      </div>
    </div>
    <div style="height:12px;"></div>
    <div class="list" id="vipList"></div>
  `;
  wrap.appendChild(card);

  setTimeout(() => {
    renderVipList("");
    $("#vipSearch").addEventListener("input", (e) => renderVipList(e.target.value.trim()));
    $("#vipAdd").addEventListener("click", addVipFromInputs);
  }, 0);

  return wrap;
}

function addVipFromInputs(){
  const name = $("#vipName").value.trim() || "VIP";
  const phone = $("#vipPhone").value.trim();
  if(!phone) return;

  const phoneKey = normalizePhone(phone);
  if(state.vip.some(v => normalizePhone(v.phone) === phoneKey)){
    $("#vipPhone").value = "";
    return;
  }

  state.vip.unshift({ id: crypto.randomUUID(), name, phone, neverAutoReply: true });
  saveLS(LS_KEYS.VIP, state.vip);
  syncVipToBackend();

  $("#vipName").value = "";
  $("#vipPhone").value = "";
  renderVipList($("#vipSearch").value.trim());
}

function renderVipList(query){
  const list = $("#vipList");
  if(!list) return;

  const q = query.toLowerCase();
  const items = state.vip.filter(v => {
    const hay = `${v.name} ${v.phone}`.toLowerCase();
    return hay.includes(q);
  });

  list.innerHTML = items.map(v => `
    <div class="list-item" data-id="${v.id}">
      <div class="list-left">
        <b>${escapeHtml(v.name)}</b>
        <span>${escapeHtml(v.phone)}</span>
      </div>

      <div class="row">
        <label class="toggle" title="If checked, this number will NEVER get auto replies.">
          <input type="checkbox" ${v.neverAutoReply ? "checked" : ""} data-toggle="${v.id}" />
          never auto-reply
        </label>
        <button class="btn" data-remove="${v.id}">Remove</button>
      </div>
    </div>
  `).join("");

  $$("input[data-toggle]").forEach(inp => {
    inp.addEventListener("change", () => {
      const id = inp.dataset.toggle;
      const item = state.vip.find(x => x.id === id);
      if(!item) return;
      item.neverAutoReply = inp.checked;
      saveLS(LS_KEYS.VIP, state.vip);
      syncVipToBackend();
    });
  });

  $$("button[data-remove]").forEach(btn => {
    btn.addEventListener("click", () => {
      const id = btn.dataset.remove;
      state.vip = state.vip.filter(x => x.id !== id);
      saveLS(LS_KEYS.VIP, state.vip);
      syncVipToBackend();
      renderVipList($("#vipSearch").value.trim());
    });
  });
}

/* Automations */
function viewAutomations(){
  const wrap = document.createElement("div");
  wrap.className = "automations-page";

  // Define all available automation templates
  const automationTemplates = {
    leadCapture: [
      {
        id: 'high-intent-qualifier',
        name: 'High-Intent Lead Qualifier',
        description: 'Capture scope fast so you can quote and book sooner',
        icon: '??',
        trigger: 'inbound_sms',
        firstTimeOnly: true,
        template: "Awesome ï¿½ I can get you a fast quote. What service do you need, what vehicle is it for, and what day works best?",
        category: 'leadCapture'
      },
      {
        id: 'quick-booking-nudge-15m',
        name: '15-Min Booking Nudge',
        description: 'Recover warm leads quickly if they go quiet',
        icon: '??',
        trigger: 'no_response',
        delayMinutes: 15,
        template: "Quick follow-up ï¿½ I can hold a priority slot for you today. Want me to send the booking link?",
        category: 'leadCapture'
      },
      {
        id: 'new-text-greeting',
        name: 'New Text Lead Greeting',
        description: 'Auto-greet new customers who text in',
        icon: '??',
        trigger: 'inbound_sms',
        firstTimeOnly: true,
        template: "Hey! Thanks for reaching out to [Business Name]. What can we help you with today?",
        category: 'leadCapture'
      }
    ],
    followUp: [
      {
        id: 'no-response-1hr',
        name: '1 Hour Follow-Up',
        description: 'Nudge leads who haven\'t responded in 1 hour',
        icon: '?',
        trigger: 'no_response',
        delayMinutes: 60,
        template: "Hey! Just following up - still interested in getting your vehicle detailed? Let me know what works for your schedule.",
        category: 'followUp'
      },
      {
        id: 'no-response-24hr',
        name: '24 Hour Follow-Up',
        description: 'Second follow-up for unresponsive leads',
        icon: '??',
        trigger: 'no_response',
        delayMinutes: 1440,
        template: "Hi again! Wanted to make sure you saw my message. We'd love to help with your detailing needs. Any questions I can answer?",
        category: 'followUp'
      },
      {
        id: 'quote-follow-up',
        name: 'Quote Follow-Up',
        description: 'Follow up after sending a quote',
        icon: '??',
        trigger: 'quote_sent',
        delayMinutes: 240,
        template: "Hey! Just checking in on that quote I sent. Any questions about the pricing or services? Happy to explain anything.",
        category: 'followUp'
      }
    ],
    booking: [
      {
        id: 'booking-confirmation',
        name: 'Booking Confirmation',
        description: 'Confirm when appointment is scheduled',
        icon: '?',
        trigger: 'booking_created',
        template: "Awesome! You're all booked for {{service}} on {{date}} at {{time}}. We'll send a reminder the day before. See you soon!",
        category: 'booking'
      },
      {
        id: 'booking-reminder-24hr',
        name: '24 Hour Reminder',
        description: 'Remind customers about tomorrow\'s appointment',
        icon: '??',
        trigger: 'booking_reminder',
        delayMinutes: -1440,
        template: "Hey! Just a reminder - your {{service}} appointment is tomorrow at {{time}}. Reply YES to confirm or let us know if you need to reschedule.",
        category: 'booking'
      },
      {
        id: 'booking-reminder-2hr',
        name: '2 Hour Reminder',
        description: 'Final reminder before appointment',
        icon: '??',
        trigger: 'booking_reminder',
        delayMinutes: -120,
        template: "See you in 2 hours! Your {{service}} appointment is at {{time}}. Address: [Your Address]",
        category: 'booking'
      }
    ],
    postService: [
      {
        id: 'thank-you',
        name: 'Thank You Message',
        description: 'Thank customers after service completion',
        icon: '??',
        trigger: 'service_completed',
        delayMinutes: 30,
        template: "Thanks for choosing us! Hope you love how your {{vehicle}} turned out. If you have a minute, a Google review would mean the world to us: [Review Link]",
        category: 'postService'
      },
      {
        id: 'feedback-request',
        name: 'Feedback Request',
        description: 'Ask for feedback a day after service',
        icon: '?',
        trigger: 'service_completed',
        delayMinutes: 1440,
        template: "Hey! How's your {{vehicle}} looking? We'd love your honest feedback. Anything we could have done better?",
        category: 'postService'
      },
      {
        id: 'maintenance-reminder',
        name: 'Maintenance Reminder',
        description: 'Remind about ceramic coating maintenance',
        icon: '???',
        trigger: 'service_completed',
        delayMinutes: 43200, // 30 days
        serviceType: 'ceramic',
        template: "Hey! It's been about a month since your ceramic coating. How's it holding up? Remember to use pH-neutral soap when washing. Book a maintenance wash anytime!",
        category: 'postService'
      }
    ],
    winBack: [
      {
        id: 'inactive-30-days',
        name: '30 Day Win-Back',
        description: 'Re-engage customers after 30 days',
        icon: '??',
        trigger: 'inactive_customer',
        delayDays: 30,
        template: "Hey! It's been a while since your last detail. Ready for a refresh? Book this week and get 10% off!",
        category: 'winBack'
      },
      {
        id: 'seasonal-reminder',
        name: 'Seasonal Reminder',
        description: 'Seasonal detailing reminders',
        icon: '??',
        trigger: 'seasonal',
        template: "Spring cleaning time! Your car took a beating this winter. Book a full detail to get rid of salt, grime, and protect your paint for summer.",
        category: 'winBack'
      },
      {
        id: 'lost-lead-recovery',
        name: 'Lost Lead Recovery',
        description: 'Try to recover leads that went cold',
        icon: '??',
        trigger: 'lead_lost',
        delayDays: 7,
        template: "Hey! We chatted last week about detailing. Still looking for someone? We'd love to earn your business. Any questions I can answer?",
        category: 'winBack'
      }
    ]
  };

  const categoryInfo = {
    leadCapture: { name: 'Lead Capture', icon: '??', desc: 'Capture and respond to new leads instantly' },
    followUp: { name: 'Follow-Up', icon: '??', desc: 'Automated follow-ups for unresponsive leads' },
    booking: { name: 'Booking', icon: '??', desc: 'Confirmations and reminders for appointments' },
    postService: { name: 'Post-Service', icon: '?', desc: 'Thank you messages and review requests' },
    winBack: { name: 'Win-Back', icon: '??', desc: 'Re-engage inactive customers' }
  };
  const automationDisplayOverrides = {
    'high-intent-qualifier': {
      icon: '\u{1F3AF}',
      template: "Awesome \u2014 I can get you a fast quote. What service do you need, what vehicle is it for, and what day works best?"
    },
    'quick-booking-nudge-15m': {
      icon: '\u{23F1}',
      template: "Quick follow-up \u2014 I can hold a priority slot for you today. Want me to send the booking link?"
    },
    'new-text-greeting': { icon: '\u{1F4AC}' },
    'no-response-1hr': { icon: '\u{23F0}' },
    'no-response-24hr': { icon: '\u{1F4E9}' },
    'quote-follow-up': { icon: '\u{1F4DD}' },
    'booking-confirmation': { icon: '\u{2705}' },
    'booking-reminder-24hr': { icon: '\u{1F4C5}' },
    'booking-reminder-2hr': { icon: '\u{23F3}' },
    'thank-you': { icon: '\u{1F64F}' },
    'feedback-request': { icon: '\u{2B50}' },
    'maintenance-reminder': { icon: '\u{1F6E1}' },
    'inactive-30-days': { icon: '\u{1F504}' },
    'seasonal-reminder': { icon: '\u{1F343}' },
    'lost-lead-recovery': { icon: '\u{1F9F2}' }
  };
  Object.values(automationTemplates).forEach((group) => {
    group.forEach((automation) => {
      const override = automationDisplayOverrides[automation.id];
      if (override) Object.assign(automation, override);
    });
  });
  Object.assign(categoryInfo, {
    leadCapture: { ...categoryInfo.leadCapture, icon: '\u{1F3AF}' },
    followUp: { ...categoryInfo.followUp, icon: '\u{1F4AC}' },
    booking: { ...categoryInfo.booking, icon: '\u{1F4C5}' },
    postService: { ...categoryInfo.postService, icon: '\u{2728}' },
    winBack: { ...categoryInfo.winBack, icon: '\u{1F504}' }
  });
  // Count active automations per category
  const getActiveCount = (category) => {
    return state.rules.filter(r => r.category === category && r.enabled).length;
  };

  const getTotalCount = (category) => {
    return automationTemplates[category]?.length || 0;
  };

  wrap.innerHTML = `
    <div class="automations-header">
      <div>
        <h1>Automations</h1>
        <p class="text-muted">Set up automated responses, follow-ups, and reminders to never miss a lead.</p>
      </div>
      <div class="automations-stats">
        <div class="auto-stat">
          <span class="auto-stat-value">${state.rules.filter(r => r.enabled).length}</span>
          <span class="auto-stat-label">Active</span>
        </div>
        <div class="auto-stat">
          <span class="auto-stat-value">${state.rules.length}</span>
          <span class="auto-stat-label">Total Rules</span>
        </div>
      </div>
    </div>

    <!-- Quick Stats -->
    <div class="auto-overview">
      ${Object.entries(categoryInfo).map(([key, info]) => `
        <div class="auto-category-card" data-category="${key}">
          <div class="auto-cat-icon">${info.icon}</div>
          <div class="auto-cat-info">
            <h3>${info.name}</h3>
            <p>${info.desc}</p>
          </div>
          <div class="auto-cat-status">
            <span class="auto-cat-count">${getActiveCount(key)}/${getTotalCount(key)}</span>
            <span class="auto-cat-label">active</span>
          </div>
        </div>
      `).join('')}
    </div>

    <!-- Automation Categories -->
    ${Object.entries(categoryInfo).map(([categoryKey, info]) => `
      <div class="auto-section" id="section-${categoryKey}">
        <div class="auto-section-header">
          <div class="auto-section-title">
            <span class="auto-section-icon">${info.icon}</span>
            <h2>${info.name}</h2>
          </div>
          <span class="auto-section-count">${getActiveCount(categoryKey)} of ${getTotalCount(categoryKey)} active</span>
        </div>
        <div class="auto-cards">
          ${automationTemplates[categoryKey].map(auto => {
            const existingRule = state.rules.find(r => r.templateId === auto.id);
            const isActive = existingRule?.enabled || false;
            return `
              <div class="auto-card ${isActive ? 'active' : ''}" data-template-id="${auto.id}">
                <div class="auto-card-header">
                  <span class="auto-card-icon">${auto.icon}</span>
                  <div class="auto-card-title">
                    <h4>${auto.name}</h4>
                    <p>${auto.description}</p>
                  </div>
                  <label class="toggle auto-toggle">
                    <input type="checkbox" ${isActive ? 'checked' : ''} data-toggle="${auto.id}" />
                    <span class="toggle-slider"></span>
                  </label>
                </div>
                <div class="auto-card-preview">
                  <div class="auto-message-preview">
                    <span class="preview-label">Message:</span>
                    <p>"${escapeHtml(existingRule?.template || auto.template)}"</p>
                  </div>
                  <div class="auto-card-meta">
                    ${auto.trigger === 'missed_call' ? '<span class="auto-tag">\u{1F4DE} Missed Call</span>' : ''}
                    ${auto.trigger === 'inbound_sms' ? '<span class="auto-tag">\u{1F4AC} New Text</span>' : ''}
                    ${auto.trigger === 'no_response' ? `<span class="auto-tag">\u{23F1} ${auto.delayMinutes >= 60 ? Math.round(auto.delayMinutes/60) + 'hr' : auto.delayMinutes + 'min'} delay</span>` : ''}
                    ${auto.businessHoursOnly ? '<span class="auto-tag">Business Hours</span>' : ''}
                    ${auto.afterHoursOnly ? '<span class="auto-tag">After Hours</span>' : ''}
                    ${auto.firstTimeOnly ? '<span class="auto-tag">First Contact</span>' : ''}
                  </div>
                </div>
                <div class="auto-card-actions">
                  <button class="btn btn-sm" data-edit="${auto.id}">Edit Message</button>
                  <button class="btn btn-sm" data-test="${auto.id}">Test</button>
                </div>
              </div>
            `;
          }).join('')}
        </div>
      </div>
    `).join('')}

    <!-- Custom Rules Section -->
    <div class="auto-section">
      <div class="auto-section-header">
        <div class="auto-section-title">
          <span class="auto-section-icon">\u{2699}</span>
          <h2>Custom Rules</h2>
        </div>
        <button class="btn primary" id="newCustomRule">+ New Custom Rule</button>
      </div>
      <div class="auto-custom-list" id="customRulesList">
        ${state.rules.filter(r => !r.templateId).length === 0
          ? RelayUI.renderEmptyState({ text: "No custom rules yet. Create one to build your own automation." })
          : state.rules.filter(r => !r.templateId).map(r => `
              <div class="auto-custom-rule">
                <div class="auto-custom-info">
                  <span class="auto-custom-icon">\u{2699}</span>
                  <div>
                    <h4>${escapeHtml(r.name)}</h4>
                    <p>${escapeHtml(r.trigger)} | ${r.businessHoursOnly ? 'business hours' : '24/7'}</p>
                  </div>
                </div>
                <div class="auto-custom-actions">
                  <label class="toggle">
                    <input type="checkbox" ${r.enabled ? 'checked' : ''} data-custom-toggle="${r.id}" />
                  </label>
                  <button class="btn btn-sm" data-custom-edit="${r.id}">Edit</button>
                  <button class="btn btn-sm danger" data-custom-del="${r.id}">Delete</button>
                </div>
              </div>
            `).join('')
        }
      </div>
    </div>

    <!-- Edit Modal -->
    <div id="autoEditModal" class="overlay hidden">
      <div class="auth-card auto-modal">
        <div class="auto-modal-header">
          <h2>Edit Automation</h2>
          <button class="btn btn-icon" id="closeAutoModal">
            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" width="20" height="20">
              <path d="M18 6L6 18M6 6l12 12"/>
            </svg>
          </button>
        </div>
        <div class="auto-modal-body" id="autoModalBody">
          <!-- Populated dynamically -->
        </div>
      </div>
    </div>
  `;

  // Store templates for reference
  wrap._templates = automationTemplates;

  // Event Listeners
  setTimeout(() => {
    // Toggle automation on/off
    wrap.querySelectorAll('[data-toggle]').forEach(toggle => {
      toggle.addEventListener('change', (e) => {
        const templateId = e.target.dataset.toggle;
        const isEnabled = e.target.checked;
        const card = e.target.closest('.auto-card');

        // Find template
        let template = null;
        Object.values(automationTemplates).forEach(cat => {
          const found = cat.find(t => t.id === templateId);
          if (found) template = found;
        });

        if (!template) return;

        // Find or create rule
        let rule = state.rules.find(r => r.templateId === templateId);

        if (isEnabled) {
          if (!rule) {
            rule = {
              id: crypto.randomUUID(),
              templateId: template.id,
              name: template.name,
              enabled: true,
              trigger: template.trigger,
              businessHoursOnly: template.businessHoursOnly || false,
              afterHoursOnly: template.afterHoursOnly || false,
              firstTimeOnly: template.firstTimeOnly || false,
              delayMinutes: template.delayMinutes || 0,
              template: template.template,
              category: template.category
            };
            state.rules.push(rule);
          } else {
            rule.enabled = true;
          }
          card.classList.add('active');
        } else {
          if (rule) {
            rule.enabled = false;
          }
          card.classList.remove('active');
        }

        saveLS(rulesKey(getActiveTo()), state.rules);
        syncRulesToBackend();
        updateAutomationCounts();
      });
    });

    // Edit automation message
    wrap.querySelectorAll('[data-edit]').forEach(btn => {
      btn.addEventListener('click', () => {
        const templateId = btn.dataset.edit;
        openAutoEditModal(templateId, automationTemplates);
      });
    });

    // Test automation
    wrap.querySelectorAll('[data-test]').forEach(btn => {
      btn.addEventListener('click', () => {
        const templateId = btn.dataset.test;
        alert('Test message would be sent to your phone. (Implement with real Twilio in production)');
      });
    });

    // Custom rule toggle
    wrap.querySelectorAll('[data-custom-toggle]').forEach(toggle => {
      toggle.addEventListener('change', (e) => {
        const ruleId = e.target.dataset.customToggle;
        const rule = state.rules.find(r => r.id === ruleId);
        if (rule) {
          rule.enabled = e.target.checked;
          saveLS(rulesKey(getActiveTo()), state.rules);
          syncRulesToBackend();
          updateAutomationCounts();
        }
      });
    });

    // Custom rule edit
    wrap.querySelectorAll('[data-custom-edit]').forEach(btn => {
      btn.addEventListener('click', () => {
        const ruleId = btn.dataset.customEdit;
        openCustomRuleModal(ruleId);
      });
    });

    // Custom rule delete
    wrap.querySelectorAll('[data-custom-del]').forEach(btn => {
      btn.addEventListener('click', () => {
        if (!confirm('Delete this custom rule?')) return;
        const ruleId = btn.dataset.customDel;
        state.rules = state.rules.filter(r => r.id !== ruleId);
        saveLS(rulesKey(getActiveTo()), state.rules);
        syncRulesToBackend();
        render();
      });
    });

    // New custom rule
    wrap.querySelector('#newCustomRule')?.addEventListener('click', () => {
      openCustomRuleModal(null);
    });

    // Close modal
    wrap.querySelector('#closeAutoModal')?.addEventListener('click', closeAutoModal);
    wrap.querySelector('#autoEditModal')?.addEventListener('click', (e) => {
      if (e.target.id === 'autoEditModal') closeAutoModal();
    });

    // Category card click to scroll
    wrap.querySelectorAll('.auto-category-card').forEach(card => {
      card.addEventListener('click', () => {
        const category = card.dataset.category;
        const section = wrap.querySelector(`#section-${category}`);
        if (section) section.scrollIntoView({ behavior: 'smooth' });
      });
    });

  }, 0);

  function updateAutomationCounts() {
    // Update header stats
    wrap.querySelector('.auto-stat-value').textContent = state.rules.filter(r => r.enabled).length;
    wrap.querySelectorAll('.auto-stat')[1].querySelector('.auto-stat-value').textContent = state.rules.length;

    // Update category counts
    Object.keys(categoryInfo).forEach(key => {
      const countEl = wrap.querySelector(`.auto-category-card[data-category="${key}"] .auto-cat-count`);
      const sectionCountEl = wrap.querySelector(`#section-${key} .auto-section-count`);
      if (countEl) countEl.textContent = `${getActiveCount(key)}/${getTotalCount(key)}`;
      if (sectionCountEl) sectionCountEl.textContent = `${getActiveCount(key)} of ${getTotalCount(key)} active`;
    });
  }

  function openAutoEditModal(templateId, templates) {
    const modal = wrap.querySelector('#autoEditModal');
    const body = wrap.querySelector('#autoModalBody');

    let template = null;
    Object.values(templates).forEach(cat => {
      const found = cat.find(t => t.id === templateId);
      if (found) template = found;
    });

    if (!template) return;

    const existingRule = state.rules.find(r => r.templateId === templateId);
    const currentMessage = existingRule?.template || template.template;

    body.innerHTML = `
      <div class="auto-edit-form">
        <div class="auto-edit-info">
          <span class="auto-card-icon">${template.icon}</span>
          <div>
            <h3>${template.name}</h3>
            <p>${template.description}</p>
          </div>
        </div>

        <label class="field">
          <span>Message Template</span>
          <textarea class="input" id="editAutoMessage" rows="4">${escapeHtml(currentMessage)}</textarea>
          <span class="field-hint">Variables: {{service}}, {{date}}, {{time}}, {{vehicle}}</span>
        </label>

        <div class="auto-edit-options">
          <label class="toggle">
            <input type="checkbox" id="editBizHours" ${template.businessHoursOnly ? 'checked' : ''} />
            <span>Business hours only</span>
          </label>
          <label class="toggle">
            <input type="checkbox" id="editFirstTime" ${template.firstTimeOnly ? 'checked' : ''} />
            <span>First-time contacts only</span>
          </label>
        </div>

        <div class="auto-edit-actions">
          <button class="btn" id="resetAutoMessage">Reset to Default</button>
          <button class="btn primary" id="saveAutoMessage">Save Changes</button>
        </div>
      </div>
    `;

    modal.classList.remove('hidden');

    // Save handler
    body.querySelector('#saveAutoMessage').addEventListener('click', () => {
      const newMessage = body.querySelector('#editAutoMessage').value.trim();
      const bizHours = body.querySelector('#editBizHours').checked;
      const firstTime = body.querySelector('#editFirstTime').checked;

      let rule = state.rules.find(r => r.templateId === templateId);
      if (!rule) {
        rule = {
          id: crypto.randomUUID(),
          templateId: template.id,
          name: template.name,
          enabled: false,
          trigger: template.trigger,
          businessHoursOnly: bizHours,
          firstTimeOnly: firstTime,
          delayMinutes: template.delayMinutes || 0,
          template: newMessage,
          category: template.category
        };
        state.rules.push(rule);
      } else {
        rule.template = newMessage;
        rule.businessHoursOnly = bizHours;
        rule.firstTimeOnly = firstTime;
      }

      saveLS(rulesKey(getActiveTo()), state.rules);
      syncRulesToBackend();

      // Update preview in card
      const card = wrap.querySelector(`[data-template-id="${templateId}"]`);
      if (card) {
        card.querySelector('.auto-message-preview p').textContent = `"${newMessage}"`;
      }

      closeAutoModal();
    });

    // Reset handler
    body.querySelector('#resetAutoMessage').addEventListener('click', () => {
      body.querySelector('#editAutoMessage').value = template.template;
    });
  }

  function openCustomRuleModal(ruleId) {
    const modal = wrap.querySelector('#autoEditModal');
    const body = wrap.querySelector('#autoModalBody');

    const rule = ruleId ? state.rules.find(r => r.id === ruleId) : null;
    const isNew = !rule;

    body.innerHTML = `
      <div class="auto-edit-form">
        <h3>${isNew ? 'Create Custom Rule' : 'Edit Custom Rule'}</h3>

        <label class="field">
          <span>Rule Name</span>
          <input class="input" id="customRuleName" value="${escapeHtml(rule?.name || '')}" placeholder="e.g., Weekend Special Offer" />
        </label>

        <label class="field">
          <span>Trigger</span>
          <select class="select" id="customRuleTrigger">
            <option value="missed_call" ${rule?.trigger === 'missed_call' ? 'selected' : ''}>Missed Call</option>
            <option value="inbound_sms" ${rule?.trigger === 'inbound_sms' ? 'selected' : ''}>Inbound SMS</option>
            <option value="no_response" ${rule?.trigger === 'no_response' ? 'selected' : ''}>No Response (follow-up)</option>
          </select>
        </label>

        <label class="field">
          <span>Message Template</span>
          <textarea class="input" id="customRuleMessage" rows="4">${escapeHtml(rule?.template || '')}</textarea>
        </label>

        <div class="auto-edit-options">
          <label class="toggle">
            <input type="checkbox" id="customBizHours" ${rule?.businessHoursOnly ? 'checked' : ''} />
            <span>Business hours only</span>
          </label>
          <label class="toggle">
            <input type="checkbox" id="customFirstTime" ${rule?.firstTimeOnly ? 'checked' : ''} />
            <span>First-time contacts only</span>
          </label>
          <label class="toggle">
            <input type="checkbox" id="customEnabled" ${rule?.enabled !== false ? 'checked' : ''} />
            <span>Enabled</span>
          </label>
        </div>

        <div class="auto-edit-actions">
          <button class="btn" id="cancelCustomRule">Cancel</button>
          <button class="btn primary" id="saveCustomRule">${isNew ? 'Create Rule' : 'Save Changes'}</button>
        </div>
      </div>
    `;

    modal.classList.remove('hidden');

    body.querySelector('#saveCustomRule').addEventListener('click', () => {
      const name = body.querySelector('#customRuleName').value.trim() || 'Untitled Rule';
      const trigger = body.querySelector('#customRuleTrigger').value;
      const template = body.querySelector('#customRuleMessage').value.trim();
      const bizHours = body.querySelector('#customBizHours').checked;
      const firstTime = body.querySelector('#customFirstTime').checked;
      const enabled = body.querySelector('#customEnabled').checked;

      if (!template) {
        alert('Please enter a message template');
        return;
      }

      if (isNew) {
        state.rules.push({
          id: crypto.randomUUID(),
          name,
          enabled,
          trigger,
          businessHoursOnly: bizHours,
          firstTimeOnly: firstTime,
          template
        });
      } else {
        const idx = state.rules.findIndex(r => r.id === ruleId);
        if (idx !== -1) {
          state.rules[idx] = { ...state.rules[idx], name, trigger, template, businessHoursOnly: bizHours, firstTimeOnly: firstTime, enabled };
        }
      }

      saveLS(rulesKey(getActiveTo()), state.rules);
      syncRulesToBackend();
      closeAutoModal();
      render();
    });

    body.querySelector('#cancelCustomRule').addEventListener('click', closeAutoModal);
  }

  function closeAutoModal() {
    wrap.querySelector('#autoEditModal')?.classList.add('hidden');
  }

  return wrap;
}

function renderRuleList(){
  const list = $("#ruleList");
  if(!list) return;

  list.innerHTML = state.rules.map(r => `
    <div class="list-item" data-rule="${r.id}">
      <div class="list-left">
        <b>${escapeHtml(r.name || "Untitled rule")}</b>
        <span>${escapeHtml(r.trigger)} | ${r.businessHoursOnly ? "business hours" : "24/7"} | ${r.firstTimeOnly ? "first-time only" : "all callers"}</span>
      </div>

      <div class="row">
        <label class="toggle">
          <input type="checkbox" ${r.enabled ? "checked" : ""} data-enable="${r.id}" />
          enabled
        </label>
        <button class="btn" data-edit="${r.id}">Edit</button>
        <button class="btn" data-del="${r.id}">Delete</button>
      </div>
    </div>
  `).join("");

  $$("input[data-enable]").forEach(inp => {
    inp.addEventListener("change", () => {
      const r = state.rules.find(x => x.id === inp.dataset.enable);
      if(!r) return;
      r.enabled = inp.checked;
      saveLS(LS_KEYS.RULES, state.rules);
      syncRulesToBackend();
    });
  });

  $$("button[data-edit]").forEach(btn => {
    btn.addEventListener("click", () => {
      const r = state.rules.find(x => x.id === btn.dataset.edit);
      renderRuleEditor(structuredClone(r), false);
    });
  });

  $$("button[data-del]").forEach(btn => {
    btn.addEventListener("click", () => {
      const id = btn.dataset.del;
      state.rules = state.rules.filter(x => x.id !== id);
      saveLS(LS_KEYS.RULES, state.rules);
      syncRulesToBackend();
      renderRuleList();
      renderRuleEditor(null);
    });
  });
}

function renderRuleEditor(rule, isNew=false){
  const editor = $("#ruleEditor");
  if(!editor) return;

  if(!rule){
    editor.innerHTML = `<div class="p">Select a rule to edit, or click "New Rule".</div>`;
    return;
  }

  editor.innerHTML = `
    <div class="row" style="justify-content:space-between; flex-wrap:wrap;">
      <div class="col" style="gap:4px;">
        <div class="h1" style="margin:0;">${isNew ? "Create Rule" : "Edit Rule"}</div>
        <div class="p">Trigger -> conditions -> action (send SMS)</div>
      </div>
      <div class="row">
        <button class="btn" id="cancelRule">Cancel</button>
        <button class="btn primary" id="saveRule">Save</button>
      </div>
    </div>

    <div style="height:12px;"></div>

    <div class="grid2">
      <div class="col">
        <label class="p">Rule name</label>
        <input class="input" id="rName" value="${escapeAttr(rule.name)}" placeholder="Missed call -> instant text" />
      </div>

      <div class="col">
        <label class="p">Trigger</label>
        <select class="select" id="rTrigger">
          <option value="missed_call" ${rule.trigger==="missed_call"?"selected":""}>Missed call</option>
          <option value="inbound_sms" ${rule.trigger==="inbound_sms"?"selected":""}>Inbound SMS</option>
        </select>
      </div>
    </div>

    <div style="height:10px;"></div>

    <div class="grid3">
      <label class="toggle" style="align-items:center; justify-content:flex-start;">
        <input type="checkbox" id="rEnabled" ${rule.enabled ? "checked":""} />
        enabled
      </label>

      <label class="toggle">
        <input type="checkbox" id="rBizHours" ${rule.businessHoursOnly ? "checked":""} />
        business hours only
      </label>

      <label class="toggle">
        <input type="checkbox" id="rFirstTime" ${rule.firstTimeOnly ? "checked":""} />
        first-time number only
      </label>
    </div>

    <div style="height:10px;"></div>

    <div class="col">
      <label class="p">Action: SMS template</label>
      <textarea class="input" id="rTemplate" rows="4" style="resize:vertical;">${escapeHtml(rule.template)}</textarea>
      <div class="p">Tip: keep it short. Ask 1 question max.</div>
    </div>
  `;

  $("#cancelRule").addEventListener("click", () => renderRuleEditor(null));

  $("#saveRule").addEventListener("click", () => {
    const updated = {
      ...rule,
      name: $("#rName").value.trim(),
      trigger: $("#rTrigger").value,
      enabled: $("#rEnabled").checked,
      businessHoursOnly: $("#rBizHours").checked,
      firstTimeOnly: $("#rFirstTime").checked,
      sendText: true,
      template: $("#rTemplate").value.trim()
    };

    if(!updated.name) updated.name = "Untitled rule";

    const idx = state.rules.findIndex(x => x.id === updated.id);
    if(idx === -1) state.rules.unshift(updated);
    else state.rules[idx] = updated;

    saveLS(LS_KEYS.RULES, state.rules);
    syncRulesToBackend();
    renderRuleList();
    renderRuleEditor(null);
  });
}

/* Analytics */
/* Analytics */
function defaultAnalyticsSummary(rangeDays = 1){
  const days = [];
  const now = Date.now();
  for (let i = rangeDays - 1; i >= 0; i -= 1) {
    days.push({
      day: new Date(now - (i * 24 * 60 * 60 * 1000)).toISOString().slice(0, 10),
      inboundLeads: 0,
      bookedLeads: 0
    });
  }
  return {
    rangeDays,
    totals: { inboundLeads: 0, respondedConversations: 0, bookedLeads: 0, responseRate: 0, conversionRate: 0 },
    speed: { avgFirstResponseMinutes: null, buckets: { under5: 0, min5to15: 0, over15: 0 } },
    daily: days,
    funnel: { inboundLeads: 0, responded: 0, qualified: 0, booked: 0 }
  };
}

function normalizeAnalyticsSummary(raw, rangeDays){
  const base = defaultAnalyticsSummary(rangeDays);
  const src = raw && typeof raw === "object" ? raw : {};
  const srcDaily = Array.isArray(src.daily) ? src.daily : [];
  const srcDailyByDay = new Map(srcDaily.map((d) => [String(d?.day || ""), d]));
  const safeDaily = base.daily.map((d) => {
    const row = srcDailyByDay.get(String(d.day)) || {};
    return {
      day: String(d.day),
      inboundLeads: Number(row?.inboundLeads || 0),
      bookedLeads: Number(row?.bookedLeads || 0)
    };
  });
  const inboundLeads = Number(src?.totals?.inboundLeads || 0);
  const respondedConversations = Number(src?.totals?.respondedConversations || 0);
  const bookedLeads = Number(src?.totals?.bookedLeads || 0);
  const responseRateRaw = Number(src?.totals?.responseRate);
  const conversionRateRaw = Number(src?.totals?.conversionRate);
  const responseRate = Number.isFinite(responseRateRaw)
    ? responseRateRaw
    : (inboundLeads > 0 ? (respondedConversations / inboundLeads) * 100 : 0);
  const conversionRate = Number.isFinite(conversionRateRaw)
    ? conversionRateRaw
    : (inboundLeads > 0 ? (bookedLeads / inboundLeads) * 100 : 0);
  return {
    rangeDays,
    totals: {
      inboundLeads,
      respondedConversations,
      bookedLeads,
      responseRate: Math.max(0, Math.min(100, responseRate)),
      conversionRate: Math.max(0, Math.min(100, conversionRate))
    },
    speed: {
      avgFirstResponseMinutes: src?.speed?.avgFirstResponseMinutes == null ? null : Number(src.speed.avgFirstResponseMinutes || 0),
      buckets: {
        under5: Number(src?.speed?.buckets?.under5 || 0),
        min5to15: Number(src?.speed?.buckets?.min5to15 || 0),
        over15: Number(src?.speed?.buckets?.over15 || 0)
      }
    },
    daily: safeDaily,
    funnel: {
      inboundLeads: Number(src?.funnel?.inboundLeads || 0),
      responded: Number(src?.funnel?.responded || 0),
      qualified: Number(src?.funnel?.qualified || 0),
      booked: Number(src?.funnel?.booked || 0)
    }
  };
}

function buildAnalyticsStats(summary){
  const inbound = Number(summary?.totals?.inboundLeads || 0);
  const responded = Number(summary?.totals?.respondedConversations || 0);
  const booked = Number(summary?.totals?.bookedLeads || 0);
  const qualified = Number(summary?.funnel?.qualified || responded);
  const maxDaily = Math.max(1, ...summary.daily.map((d) => Math.max(d.inboundLeads, d.bookedLeads)));
  return {
    inbound,
    responded,
    booked,
    responseRate: Math.max(0, Math.min(100, Number(summary?.totals?.responseRate || (inbound ? (responded / inbound) * 100 : 0)))),
    conversionRate: Math.max(0, Math.min(100, Number(summary?.totals?.conversionRate || (inbound ? (booked / inbound) * 100 : 0)))),
    avgResponseTime: summary?.speed?.avgFirstResponseMinutes == null ? null : Number(summary.speed.avgFirstResponseMinutes || 0),
    funnel: {
      leads: inbound,
      responded,
      qualified,
      booked,
      respondedPct: inbound ? Math.round((responded / inbound) * 100) : 0,
      qualifiedPct: inbound ? Math.round((qualified / inbound) * 100) : 0,
      bookedPct: inbound ? Math.round((booked / inbound) * 100) : 0
    },
    dailyPerformance: summary.daily.map((d) => ({
      label: String(d.day || '').slice(5),
      leads: Number(d.inboundLeads || 0),
      booked: Number(d.bookedLeads || 0),
      leadsPct: (Number(d.inboundLeads || 0) / maxDaily) * 100,
      bookedPct: (Number(d.bookedLeads || 0) / maxDaily) * 100
    }))
  };
}

function getAnalyticsScopeKey() {
  const accounts = Array.isArray(authState?.accounts) ? authState.accounts : [];
  const activeTo = String(getActiveTo() || "");
  const activeAccount = accounts.find((acct) => String(acct?.to || "") === activeTo);
  return String(activeAccount?.accountId || activeTo || "default");
}

async function hydrateAnalyticsBookedThreads(activeTo, options = {}) {
  const force = options && options.force === true;
  const scopeKey = getAnalyticsScopeKey();
  const hydratedAt = Number(state.analyticsHydratedAtByScope?.[scopeKey] || 0);
  const ttlMs = 15000;
  if (state.analyticsHydrationLoading && !force) return;
  if (!force && Number.isFinite(hydratedAt) && (Date.now() - hydratedAt) < ttlMs) return;
  state.analyticsHydrationLoading = true;
  try {
    const scopedThreads = safeArray(state.threads).filter((thread) => {
      const to = String(thread?.to || "").trim();
      if (activeTo && to && to !== activeTo) return false;
      return true;
    });
    let candidates = scopedThreads.filter((thread) => {
      const status = String(thread?.status || "").toLowerCase();
      const stage = String(thread?.stage || "").toLowerCase();
      const snippet = String(thread?.lastText || "").toLowerCase();
      const ld = thread?.leadData || {};
      const lifecycle = String(thread?.lifecycle?.leadStatus || ld?.lifecycle?.leadStatus || "").toLowerCase();
      const availability = String(ld?.availability || "").toLowerCase();
      const bookedLike = status === "booked"
        || status === "closed"
        || /booked|appointment_booked|scheduled/.test(stage)
        || /booked\s*:|i see you booked|appointment booked|scheduled/.test(snippet)
        || lifecycle === "booked"
        || /booked|scheduled/.test(availability)
        || coerceTimestampMs(thread?.bookingTime || ld?.booking_time || 0) > 0;
      const amount = Number(resolveConversationAmount(thread) || 0);
      const amountMissing = !(Number.isFinite(amount) && amount > 0);
      const serviceLabels = getServiceLabelsFromLeadData(ld);
      const serviceMissing = serviceLabels.length === 0;
      // Hydrate any likely-booked thread that is missing amount in summary.
      return bookedLike && (amountMissing || serviceMissing);
    }).slice(0, force ? 200 : 25);

    if (!candidates.length) {
      const hasAnyKnownAmount = scopedThreads.some((thread) => {
        const n = Number(resolveConversationAmount(thread) || 0);
        return Number.isFinite(n) && n > 0;
      });
      if (!hasAnyKnownAmount) {
        // Last-resort hydration: pull a few recent conversations so metrics can be derived
        // from persisted messages/lead data without requiring manual thread opens.
        candidates = scopedThreads.slice(0, force ? 120 : 10);
      }
    }

    if (!candidates.length) {
      state.analyticsHydratedAtByScope[scopeKey] = Date.now();
      return;
    }

    const details = await Promise.all(candidates.map(async (thread) => {
      const id = String(thread?.id || "").trim();
      if (!id) return null;
      try {
        const res = await apiGet(`/api/conversations/${encodeURIComponent(id)}`);
        return res?.conversation || null;
      } catch {
        return null;
      }
    }));

    const byId = new Map(safeArray(state.threads).map((t) => [String(t?.id || ""), t]));
    details.forEach((detail) => {
      const id = String(detail?.id || "").trim();
      if (!id || !byId.has(id)) return;
      const prev = byId.get(id) || {};
      const merged = {
        ...prev,
        ...detail,
        leadData: { ...(prev?.leadData || {}), ...(detail?.leadData || {}) }
      };
      const mergedLead = merged.leadData && typeof merged.leadData === "object" ? merged.leadData : {};
      const hasServiceLabel = getServiceLabelsFromLeadData(mergedLead).length > 0;
      if (!hasServiceLabel) {
        const inferred = inferServiceLabelsFromMessages(Array.isArray(detail?.messages) ? detail.messages : []);
        if (inferred.length > 0) {
          mergedLead.services_list = Array.from(new Set(inferred.map((x) => String(x || "").trim()).filter(Boolean))).slice(0, 6);
          const requestText = mergedLead.services_list.join(" + ");
          if (!String(mergedLead.service_required || "").trim()) mergedLead.service_required = requestText;
          if (!String(mergedLead.request || "").trim()) mergedLead.request = requestText;
        }
      }
      merged.leadData = mergedLead;
      applyRealtimeBookedState(merged, { refreshRecovered: false });
      byId.set(id, merged);
    });
    state.threads = Array.from(byId.values());
    state.analyticsHydratedAtByScope[scopeKey] = Date.now();
  } finally {
    state.analyticsHydrationLoading = false;
  }
}

function viewAnalyticsModern(){
  const wrap = document.createElement("div");
  wrap.className = "analytics-page app-view app-view-analytics";
  const analyticsScopeKey = getAnalyticsScopeKey();
  const threadRefreshTtlMs = 6000;
  const threadsLoadedAt = Number(state.threadsLastLoadedAt || 0);
  const shouldRefreshThreads = !state.analyticsThreadsLoading && (
    !Array.isArray(state.threads)
    || state.threads.length === 0
    || !Number.isFinite(threadsLoadedAt)
    || (Date.now() - threadsLoadedAt) > threadRefreshTtlMs
  );
  if (shouldRefreshThreads) {
    state.analyticsThreadsLoading = true;
    loadThreads({ skipTopbarRefresh: true })
      .catch(() => {})
      .finally(() => {
        state.analyticsThreadsLoading = false;
        if (state.view === "analytics") render();
      });
  } else {
    const hydratedAt = Number(state.analyticsHydratedAtByScope?.[analyticsScopeKey] || 0);
    const shouldHydrate = !state.analyticsHydrationLoading && (
      !Number.isFinite(hydratedAt) || (Date.now() - hydratedAt) > 15000
    );
    if (shouldHydrate) {
      hydrateAnalyticsBookedThreads(String(getActiveTo() || ""))
        .finally(() => {
          if (state.view === "analytics") render();
        });
    }
  }
  const rangeDaysRaw = Number(state.analyticsRange || 1);
  const rangeDays = [1, 7, 30].includes(rangeDaysRaw) ? rangeDaysRaw : 1;
  if (rangeDays !== rangeDaysRaw) state.analyticsRange = rangeDays;
  const scopeKey = getAnalyticsScopeKey();
  const activeTo = String(getActiveTo() || "");
  const scopeText = activeTo ? `Scope: ${activeTo}` : `Scope: ${scopeKey}`;
  const cacheKey = `${scopeKey}:${rangeDays}`;
  const cached = state.analyticsSummaryCache?.[cacheKey] || null;
  const summary = cached || defaultAnalyticsSummary(rangeDays);
  let stats = buildAnalyticsStats(summary);

  if (!cached && !state.analyticsLoading) {
    state.analyticsLoading = true;
    state.analyticsError = null;
    apiGet(`/api/analytics/summary?range=${encodeURIComponent(rangeDays)}`)
      .then((res) => {
        state.analyticsSummaryCache[cacheKey] = normalizeAnalyticsSummary(res, rangeDays);
      })
      .catch((err) => {
        state.analyticsSummaryCache[cacheKey] = defaultAnalyticsSummary(rangeDays);
        state.analyticsError = err?.message || "Failed to load analytics";
      })
      .finally(() => {
        state.analyticsLoading = false;
        if (state.view === "analytics") render();
      });
  }

  const revenueCache = state.revenueCache?.[scopeKey] || null;
  const revenueOverview = revenueCache?.overview || {};
  if (!revenueCache && !state.revenueLoading) {
    state.revenueLoading = true;
    state.revenueError = null;
    apiGet("/api/analytics/revenue-overview")
      .then((overview) => {
        state.revenueCache[scopeKey] = { overview };
      })
      .catch((err) => {
        state.revenueError = err?.message || "Failed to load revenue intelligence";
      })
      .finally(() => {
        state.revenueLoading = false;
        if (state.view === "analytics") render();
      });
  }

  const nowMs = Date.now();
  const defaultRangeStart = nowMs - ((rangeDays - 1) * 24 * 60 * 60 * 1000);
  const rangeStartMs = (() => {
    const firstDay = String(summary?.daily?.[0]?.day || "").trim();
    const parsed = firstDay ? Date.parse(`${firstDay}T00:00:00`) : NaN;
    return Number.isFinite(parsed) && parsed > 0 ? parsed : defaultRangeStart;
  })();
  const cachedFallback = state.analyticsBookedFallbackCache[cacheKey] || null;
  const localThreadCount = safeArray(state.threads).length;
  const fallbackStaleMs = 8000;
  const shouldRefreshFallback = (
    !cachedFallback
    || !Number.isFinite(Number(cachedFallback.computedAt || 0))
    || (Date.now() - Number(cachedFallback.computedAt || 0)) > fallbackStaleMs
    || Number(cachedFallback.threadCount || 0) !== Number(localThreadCount || 0)
  );
  const localFallback = computeAnalyticsBookedFallbackFromThreads({
    threads: state.threads,
    activeTo,
    rangeStartMs,
    nowMs
  });
  if (localFallback.hasData) {
    state.analyticsBookedFallbackCache[cacheKey] = localFallback;
  } else if (shouldRefreshFallback && !state.analyticsBookedFallbackLoading[cacheKey]) {
    state.analyticsBookedFallbackLoading[cacheKey] = true;
    (async () => {
      try {
        const listRes = activeTo
          ? await apiGet(`/api/conversations?to=${encodeURIComponent(activeTo)}`)
          : await apiGet("/api/conversations");
        const rows = safeArray(listRes?.conversations);
        const details = await Promise.all(rows.map(async (thread) => {
          const id = String(thread?.id || "").trim();
          if (!id) return null;
          try {
            const res = await apiGet(`/api/conversations/${encodeURIComponent(id)}`);
            return res?.conversation || null;
          } catch {
            return null;
          }
        }));
        const localOnlyConvos = safeArray(state.threads)
          .filter((thread) => {
            const id = String(thread?.id || "").trim();
            return /^sim-/i.test(id) || Array.isArray(thread?.messages);
          });
        const combined = [...details, ...localOnlyConvos];
        let sumCents = 0;
        let bookedCount = 0;
        const customers = new Set();
        combined.forEach((convo) => {
          if (!convo) return;
          const ld = convo?.leadData || {};
          const status = String(convo?.status || "").toLowerCase();
          const stage = String(convo?.stage || "").toLowerCase();
          const bookedMs = coerceTimestampMs(
            convo?.bookingTime || ld?.booking_time || getLatestBookedConfirmationTime(convo) || convo?.updatedAt || convo?.lastActivityAt,
            0
          );
          const bookedLike = status === "booked" || /booked|appointment_booked|scheduled/.test(stage) || (Number.isFinite(bookedMs) && bookedMs > 0);
          if (!bookedLike) return;
          if (!Number.isFinite(bookedMs) || bookedMs < rangeStartMs || bookedMs > nowMs) return;
          const amount = Number(resolveConversationAmount(convo) || 0);
          let amountCents = Number.isFinite(amount) && amount > 0 ? Math.round(amount * 100) : 0;
          if (!(amountCents > 0)) {
            const pill = buildConversationMoneyPill(convo);
            const n = Number(String(pill?.label || "").replace(/[^0-9.]/g, ""));
            if (Number.isFinite(n) && n > 0) amountCents = Math.round(n * 100);
          }
          if (amountCents > 0) sumCents += amountCents;
          bookedCount += 1;
          const customerKey = String(convo?.from || convo?.phone || "").trim().toLowerCase();
          if (customerKey) customers.add(customerKey);
        });
        state.analyticsBookedFallbackCache[cacheKey] = {
          sumCents,
          bookedCount,
          customerCount: customers.size,
          computedAt: Date.now(),
          threadCount: localThreadCount
        };
      } catch {
        state.analyticsBookedFallbackCache[cacheKey] = { sumCents: 0, bookedCount: 0, customerCount: 0, computedAt: Date.now(), threadCount: localThreadCount };
      } finally {
        state.analyticsBookedFallbackLoading[cacheKey] = false;
        if (state.view === "analytics") render();
      }
    })();
  }
  const detailedFallback = state.analyticsBookedFallbackCache[cacheKey] || null;
  const ledgerRowsAll = loadRevenueLedgerRows();
  const ledgerRows = ledgerRowsAll.filter((row) => {
    const ts = Number(row?.bookedMs || 0);
    return Number.isFinite(ts) && ts >= rangeStartMs && ts <= nowMs;
  });
  const safeCents = (v) => {
    const n = Number(v || 0);
    return Number.isFinite(n) && n > 0 ? Math.round(n) : 0;
  };
  const ledgerRecoveredRangeCents = ledgerRows.reduce((acc, row) => acc + safeCents(row?.amountCents), 0);
  const ledgerBookedCount = ledgerRows.length;
  const ledgerCustomerCount = new Set(ledgerRows.map((r) => String(r?.customer || "")).filter(Boolean)).size;
  let bookedRevenueCents = 0;
  const estimatedLostRevenueCents = safeCents(revenueOverview?.estimatedLostRevenueCents);
  const recoveredRevenueCents = safeCents(revenueOverview?.recoveredRevenueCents);
  const totalLeakPool = estimatedLostRevenueCents + recoveredRevenueCents;
  const leakPct = totalLeakPool > 0 ? Math.round((estimatedLostRevenueCents / totalLeakPool) * 100) : 0;
  const recoveryRate = Number.isFinite(Number(revenueOverview?.revenueRecoveryRate))
    ? Math.round(Number(revenueOverview.revenueRecoveryRate) * 100)
    : 0;
  const responseTimeValue = Number(revenueOverview?.responseTimeAvg || 0);
  const missedCalls = Number(revenueOverview?.missedCallCount || 0);
  const quoteShown = Number(revenueOverview?.quoteShown || 0);
  const quoteAccepted = Number(revenueOverview?.quoteAccepted || 0);
  const realtimeBookedRows = safeArray(state.threads).filter((thread) => {
    const to = String(thread?.to || "").trim();
    return !activeTo || !to || to === activeTo;
  }).map((thread) => {
    const status = String(thread?.status || "").toLowerCase();
    const stage = String(thread?.stage || "").toLowerCase();
    const ld = thread?.leadData || {};
    const snippet = String(thread?.lastText || "").toLowerCase();
    const bookedDividerMs = getLatestBookedConfirmationTime(thread);
    const isBooked = status === "booked"
      || status === "closed"
      || /booked|appointment_booked|scheduled/.test(stage)
      || (Number.isFinite(bookedDividerMs) && bookedDividerMs > 0)
      || /booked\s*:|i see you booked|appointment booked|scheduled/.test(snippet);
    if (!isBooked) return null;
    const amount = Number(resolveConversationAmount(thread) || 0);
    const ts = coerceTimestampMs(
      thread?.updatedAt || thread?.lastActivityAt || bookedDividerMs || thread?.bookingTime || thread?.leadData?.booking_time,
      0
    );
    if (!(Number.isFinite(ts) && ts > 0)) return null;
    const customerKey = String(thread?.from || thread?.phone || "").trim().toLowerCase();
    return {
      ts,
      customerKey,
      amountCents: Number.isFinite(amount) && amount > 0 ? Math.round(amount * 100) : 0
    };
  }).filter(Boolean);
  const threadPillRows = safeArray(state.threads).filter((thread) => {
    const to = String(thread?.to || "").trim();
    return !activeTo || !to || to === activeTo;
  }).map((thread) => {
    const normalizedThread = {
      ...thread,
      leadData: { ...(thread?.leadData || {}) }
    };
    applyRealtimeBookedState(normalizedThread, { refreshRecovered: false });
    const ld = normalizedThread?.leadData || {};
    const bookedDividerMs = getLatestBookedConfirmationTime(normalizedThread);
    const ts = coerceTimestampMs(
      normalizedThread?.updatedAt || normalizedThread?.lastActivityAt || bookedDividerMs || normalizedThread?.bookingTime || ld?.booking_time,
      0
    );
    if (!(Number.isFinite(ts) && ts > 0)) return null;
    const status = String(normalizedThread?.status || "").toLowerCase();
    const stage = String(normalizedThread?.stage || "").toLowerCase();
    const snippet = String(normalizedThread?.lastText || "").toLowerCase();
    const pill = buildConversationMoneyPill(normalizedThread);
    const bookedLike = status === "booked"
      || status === "closed"
      || /booked|appointment_booked|scheduled/.test(stage)
      || /booked\s*:|i see you booked|appointment booked|scheduled/.test(snippet)
      || (Number.isFinite(bookedDividerMs) && bookedDividerMs > 0)
      || String(pill?.tone || "") === "is-paid";
    return {
      ts,
      customerKey: String(normalizedThread?.from || normalizedThread?.phone || "").trim().toLowerCase(),
      bookedLike,
      amountCents: parseMoneyLabelToCents(pill?.label)
    };
  });
  const realtimeRecoveredRangeCents = realtimeBookedRows.reduce((acc, row) => {
    const ts = Number(row?.ts || 0);
    const effectiveTs = Number.isFinite(ts) ? Math.min(ts, nowMs) : 0;
    if (!Number.isFinite(effectiveTs) || effectiveTs < rangeStartMs || effectiveTs > nowMs) return acc;
    return acc + Math.max(0, Number(row?.amountCents || 0));
  }, 0);
  const threadPillRecoveredRangeCents = threadPillRows.reduce((acc, row) => {
    const ts = Number(row?.ts || 0);
    const effectiveTs = Number.isFinite(ts) ? Math.min(ts, nowMs) : 0;
    if (!row?.bookedLike || !Number.isFinite(effectiveTs) || effectiveTs < rangeStartMs || effectiveTs > nowMs) return acc;
    return acc + Math.max(0, Number(row?.amountCents || 0));
  }, 0);
  const detailedRecoveredRangeCents = safeCents(detailedFallback?.sumCents);
  const customerRecoveredInRange = new Map();
  realtimeBookedRows.forEach((row) => {
    const ts = Number(row?.ts || 0);
    const effectiveTs = Number.isFinite(ts) ? Math.min(ts, nowMs) : 0;
    if (!Number.isFinite(effectiveTs) || effectiveTs < rangeStartMs || effectiveTs > nowMs) return;
    const key = String(row?.customerKey || "");
    if (!key) return;
    customerRecoveredInRange.set(key, (customerRecoveredInRange.get(key) || 0) + Math.max(0, Number(row?.amountCents || 0)));
  });
  threadPillRows.forEach((row) => {
    const ts = Number(row?.ts || 0);
    const effectiveTs = Number.isFinite(ts) ? Math.min(ts, nowMs) : 0;
    if (!row?.bookedLike || !Number.isFinite(effectiveTs) || effectiveTs < rangeStartMs || effectiveTs > nowMs) return;
    const key = String(row?.customerKey || "");
    if (!key) return;
    customerRecoveredInRange.set(key, (customerRecoveredInRange.get(key) || 0) + Math.max(0, Number(row?.amountCents || 0)));
  });
  const realtimeBookedInRange = realtimeBookedRows.filter((row) => {
    const ts = Number(row?.ts || 0);
    const effectiveTs = Number.isFinite(ts) ? Math.min(ts, nowMs) : 0;
    return Number.isFinite(effectiveTs) && effectiveTs >= rangeStartMs && effectiveTs <= nowMs;
  });
  const strictThreadPillRecoveredCents = threadPillRows.reduce((acc, row) => {
    const ts = Number(row?.ts || 0);
    const effectiveTs = Number.isFinite(ts) ? Math.min(ts, nowMs) : 0;
    if (!row?.bookedLike || !Number.isFinite(effectiveTs) || effectiveTs < rangeStartMs || effectiveTs > nowMs) return acc;
    return acc + Math.max(0, Number(row?.amountCents || 0));
  }, 0);
  const authoritativeRecoveredCents = Math.max(
    strictThreadPillRecoveredCents,
    realtimeRecoveredRangeCents,
    detailedRecoveredRangeCents,
    ledgerRecoveredRangeCents
  );
  bookedRevenueCents = authoritativeRecoveredCents;
  const apiRevenueEvents = safeArray(revenueOverview?.revenueEvents);
  const fallbackRevenueEvents = realtimeBookedInRange.map((row) => ({
    type: "appointment_booked",
    status: "won",
    estimatedValueCents: Math.max(0, Number(row?.amountCents || 0)),
    createdAt: Number(row?.ts || nowMs)
  }));
  const allRevenueEvents = apiRevenueEvents.length ? apiRevenueEvents : fallbackRevenueEvents;
  const revenueSignals = allRevenueEvents.slice(0, 4);
  const bookedJobsFromEvents = allRevenueEvents.filter((e) => {
    const type = String(e?.type || "").toLowerCase();
    return type === "appointment_booked" || type === "sale_closed";
  }).length;
  const threadPillBookedInRangeRows = threadPillRows.filter((row) => {
    const ts = Number(row?.ts || 0);
    const effectiveTs = Number.isFinite(ts) ? Math.min(ts, nowMs) : 0;
    return row?.bookedLike && Number.isFinite(effectiveTs) && effectiveTs >= rangeStartMs && effectiveTs <= nowMs;
  });
  const threadPillBookedInRangeCount = threadPillBookedInRangeRows.length;
  const detailedBookedCount = Math.max(0, Number(detailedFallback?.bookedCount || 0));
  const bookedJobsCount = Math.max(
    threadPillBookedInRangeCount,
    detailedBookedCount,
    ledgerBookedCount,
    Number(summary?.totals?.bookedLeads || 0)
  );
  const quoteAcceptRate = quoteShown > 0 ? Math.round((quoteAccepted / quoteShown) * 100) : 0;

  const dailyRevenueMap = new Map((summary.daily || []).map((d) => [String(d?.day || ""), { recovered: 0, atRisk: 0 }]));
  const authoritativeThreadPillRows = threadPillRows.filter((row) => {
    const ts = Number(row?.ts || 0);
    const effectiveTs = Number.isFinite(ts) ? Math.min(ts, nowMs) : 0;
    return row?.bookedLike && row?.amountCents > 0 && Number.isFinite(effectiveTs) && effectiveTs >= rangeStartMs && effectiveTs <= nowMs;
  });
  allRevenueEvents.forEach((event) => {
    const ts = Number(event?.createdAt || 0);
    const day = Number.isFinite(ts) && ts > 0 ? new Date(ts).toISOString().slice(0, 10) : "";
    if (!day || !dailyRevenueMap.has(day)) return;
    const row = dailyRevenueMap.get(day);
    const valueCents = Math.max(0, Number(event?.estimatedValueCents || 0));
    const type = String(event?.type || "").toLowerCase();
    const status = String(event?.status || "").toLowerCase();
    const recovered = type === "appointment_booked" || type === "sale_closed" || type === "opportunity_recovered" || status === "won";
    if (recovered) row.recovered += valueCents;
    else row.atRisk += valueCents;
  });
  if (authoritativeThreadPillRows.length) {
    dailyRevenueMap.forEach((slot) => { slot.recovered = 0; slot.atRisk = 0; });
    authoritativeThreadPillRows.forEach((row) => {
      const ts = Number(row?.ts || 0);
      const day = Number.isFinite(ts) && ts > 0 ? new Date(ts).toISOString().slice(0, 10) : "";
      if (!day || !dailyRevenueMap.has(day)) return;
      const slot = dailyRevenueMap.get(day);
      slot.recovered += safeCents(row?.amountCents);
    });
  } else {
    ledgerRows.forEach((row) => {
      const ts = Number(row?.bookedMs || 0);
      const day = Number.isFinite(ts) && ts > 0 ? new Date(ts).toISOString().slice(0, 10) : "";
      if (!day || !dailyRevenueMap.has(day)) return;
      const slot = dailyRevenueMap.get(day);
      slot.recovered += safeCents(row?.amountCents);
    });
  }
  const dailyRevenueRows = (summary.daily || []).map((d) => {
    const day = String(d?.day || "");
    const row = dailyRevenueMap.get(day) || { recovered: 0, atRisk: 0 };
    return {
      label: day.slice(5),
      recoveredCents: Number(row.recovered || 0),
      atRiskCents: Number(row.atRisk || 0)
    };
  });
  const maxDailyRevenueCents = Math.max(1, ...dailyRevenueRows.map((r) => Math.max(r.recoveredCents, 0)));
  const bookedCustomersCount = Math.max(
    new Set(
      threadPillBookedInRangeRows
        .filter((row) => Number(row?.amountCents || 0) > 0)
        .map((row) => String(row?.customerKey || ""))
        .filter(Boolean)
    ).size,
    customerRecoveredInRange.size,
    Math.max(0, Number(detailedFallback?.customerCount || 0)),
    ledgerCustomerCount
  );
  const avgTicketCents = bookedJobsCount > 0 ? Math.round(bookedRevenueCents / bookedJobsCount) : 0;
  const heatmapDayLabels = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"];
  const heatmapBuckets = [
    { label: "6a-9a", start: 6, end: 9 },
    { label: "9a-12p", start: 9, end: 12 },
    { label: "12p-3p", start: 12, end: 15 },
    { label: "3p-6p", start: 15, end: 18 },
    { label: "6p-9p", start: 18, end: 21 },
    { label: "9p-12a", start: 21, end: 24 }
  ];
  const heatmapRowsSource = threadPillRows.filter((row) => {
    const ts = Number(row?.ts || 0);
    const effectiveTs = Number.isFinite(ts) ? Math.min(ts, nowMs) : 0;
    return row?.bookedLike && Number(row?.amountCents || 0) > 0 && Number.isFinite(effectiveTs) && effectiveTs >= rangeStartMs && effectiveTs <= nowMs;
  });
  const heatmapValues = heatmapBuckets.map(() => heatmapDayLabels.map(() => 0));
  heatmapRowsSource.forEach((row) => {
    const ts = Number(row?.ts || 0);
    const effectiveTs = Number.isFinite(ts) ? Math.min(ts, nowMs) : 0;
    if (!(Number.isFinite(effectiveTs) && effectiveTs > 0)) return;
    const dt = new Date(effectiveTs);
    const dayIdx = dt.getDay();
    const hour = dt.getHours();
    const bucketIdx = heatmapBuckets.findIndex((b) => hour >= b.start && hour < b.end);
    if (bucketIdx < 0 || dayIdx < 0 || dayIdx > 6) return;
    heatmapValues[bucketIdx][dayIdx] += Math.max(0, Number(row?.amountCents || 0));
  });
  const heatmapMax = Math.max(1, ...heatmapValues.flat());
  const heatmapEmpty = heatmapRowsSource.length === 0;
  const heatmapGridHtml = `
    <div class="revenue-heatmap-grid">
      <div class="revenue-heatmap-corner"></div>
      ${heatmapDayLabels.map((d) => `<div class="revenue-heatmap-day">${d}</div>`).join("")}
      ${heatmapBuckets.map((bucket, bucketIdx) => `
        <div class="revenue-heatmap-time">${bucket.label}</div>
        ${heatmapDayLabels.map((day, dayIdx) => {
          const cents = Number(heatmapValues[bucketIdx]?.[dayIdx] || 0);
          const pct = Math.max(0, Math.min(1, cents / heatmapMax));
          const alpha = (cents > 0) ? (0.16 + (pct * 0.78)) : 0.06;
          const title = `${day} ${bucket.label}: ${moneyFromCents(cents)}`;
          return `<div class="revenue-heatmap-cell" style="background: rgba(10, 132, 255, ${alpha.toFixed(3)});" title="${escapeAttr(title)}"></div>`;
        }).join("")}
      `).join("")}
    </div>
  `;

  stats = {
    ...stats,
    inbound: Math.round(bookedRevenueCents / 100),
    responseRate: Math.round(avgTicketCents / 100),
    booked: bookedJobsCount,
    conversionRate: bookedCustomersCount,
    dailyPerformance: dailyRevenueRows.map((r) => ({
      label: r.label,
      leads: Math.round(r.recoveredCents / 100),
      booked: 0,
      leadsPct: (r.recoveredCents / maxDailyRevenueCents) * 100,
      bookedPct: 0,
      leadsText: moneyFromCents(r.recoveredCents),
      bookedText: "$0"
    }))
  };
  const empty = stats.inbound === 0 && stats.booked === 0;

  wrap.innerHTML = `
    <div class="analytics-header">
      <div>
        <h1>Analytics</h1>
      </div>
      <div class="analytics-controls">
        ${RelayUI.renderSegmentedControl({
          activeValue: rangeDays,
          dataAttr: "arange",
          className: "btn-group",
          options: [
            { value: 1, label: "Today" },
            { value: 7, label: "7 days" },
            { value: 30, label: "30 days" }
          ]
        })}
      </div>
    </div>

    <div class="analytics-kpis">
      <div class="analytics-kpi"><div class="kpi-header"><span class="kpi-label">Recovered (USD)</span></div><div class="kpi-value" data-count="${stats.inbound}" data-suffix="">0</div></div>
      <div class="analytics-kpi"><div class="kpi-header"><span class="kpi-label">Booked Jobs</span></div><div class="kpi-value" data-count="${stats.booked}" data-suffix="">0</div></div>
      <div class="analytics-kpi"><div class="kpi-header"><span class="kpi-label">Avg Ticket (USD)</span></div><div class="kpi-value" data-count="${stats.responseRate}" data-suffix="">0</div></div>
      <div class="analytics-kpi"><div class="kpi-header"><span class="kpi-label">Booked Customers</span></div><div class="kpi-value" data-count="${stats.conversionRate}" data-suffix="">0</div></div>
    </div>

    ${(state.analyticsLoading || state.analyticsError || empty) ? `
      <div class="analytics-card" style="margin-bottom:14px;">
        ${RelayUI.renderEmptyState({
          text: state.analyticsLoading
            ? "Loading analytics..."
            : state.analyticsError
              ? state.analyticsError
              : "No activity yet run a webhook simulation or send a test SMS",
          className: "is-compact"
        })}
      </div>
    ` : ''}

    <div class="analytics-grid">
      <div class="analytics-card wide">
        <div class="card-header"><h2>Daily Recovered Revenue</h2><div class="chart-legend"><span class="legend-item"><span class="legend-dot leads"></span> Recovered</span></div></div>
        <div class="daily-chart">
          ${stats.dailyPerformance.map(d => `
            <div class="daily-bar-group"><div class="daily-bars"><div class="daily-bar leads" style="height:${d.leadsPct}%" title="${d.leadsText || d.leads} recovered"></div></div><span class="daily-label">${d.label}</span></div>
          `).join('')}
        </div>
      </div>
      <div class="analytics-card wide">
        <div class="card-header"><h2>Day/Hour Revenue Heatmap</h2><span class="text-muted">Booked revenue by weekday and time</span></div>
        ${heatmapEmpty ? `
          <div class="p muted">No booked revenue in this time range yet.</div>
        ` : `
          ${heatmapGridHtml}
          <div class="revenue-heatmap-legend">
            <span>Lower</span>
            <div class="revenue-heatmap-gradient"></div>
            <span>Higher</span>
          </div>
        `}
      </div>
    </div>
  `;

  wrap.querySelectorAll('[data-arange]').forEach(btn => {
    btn.addEventListener('click', () => {
      state.analyticsRange = Number(btn.dataset.arange);
      localStorage.setItem(ANALYTICS_RANGE_KEY, String(state.analyticsRange));
      state.analyticsError = null;
      render();
    });
  });

  setTimeout(() => {
    wrap.querySelectorAll('.kpi-value[data-count]').forEach(el => {
      animateCounter(el, 0, parseFloat(el.dataset.count), 800, el.dataset.suffix || '');
    });
    if (stats.avgResponseTime != null) {
      wrap.querySelectorAll('.response-time-value[data-count]').forEach(el => {
        animateCounter(el, 0, parseFloat(el.dataset.count), 1000, '');
      });
    }
  }, 300);

  return wrap;
}

// Counter animation function
function animateCounter(element, start, end, duration, suffix) {
  const startTime = performance.now();
  const isFloat = !Number.isInteger(end);

  function update(currentTime) {
    const elapsed = currentTime - startTime;
    const progress = Math.min(elapsed / duration, 1);

    // Easing function (ease-out cubic)
    const easeOut = 1 - Math.pow(1 - progress, 3);

    const current = start + (end - start) * easeOut;
    element.textContent = (isFloat ? current.toFixed(0) : Math.floor(current)) + suffix;

    if (progress < 1) {
      requestAnimationFrame(update);
    } else {
      element.textContent = (isFloat ? end.toFixed(0) : end) + suffix;
    }
  }

  requestAnimationFrame(update);
}

function computeAnalyticsData(){
  const rangeDaysRaw = Number(state.analyticsRange || 1);
  const rangeDays = [1, 7, 30].includes(rangeDaysRaw) ? rangeDaysRaw : 1;
  const cacheKey = `${getAnalyticsScopeKey()}:${rangeDays}`;
  return buildAnalyticsStats(state.analyticsSummaryCache?.[cacheKey] || defaultAnalyticsSummary(rangeDays));
}

function moneyFromCents(cents) {
  const n = Number(cents || 0);
  if (!Number.isFinite(n)) return "$0";
  return `$${(n / 100).toLocaleString(undefined, { maximumFractionDigits: 0 })}`;
}

function deriveRecoveryMetricsFromOverview(overview = {}) {
  const recoveredThisMonthCents = Math.max(0, Number(overview?.recoveredThisMonth || 0));
  const revenueEvents = Array.isArray(overview?.revenueEvents) ? overview.revenueEvents : [];
  const missedCallsConverted = revenueEvents.filter((e) => {
    const signal = String(e?.signalType || "").toLowerCase();
    const type = String(e?.type || "").toLowerCase();
    const status = String(e?.status || "").toLowerCase();
    const fromMissedCall = signal.includes("missed_call");
    const converted = ["opportunity_recovered", "appointment_booked", "sale_closed"].includes(type) || status === "won";
    return fromMissedCall && converted;
  }).length;
  const bookedAppointments = revenueEvents.filter((e) => String(e?.type || "").toLowerCase() === "appointment_booked").length;
  return {
    recoveredThisMonthText: moneyFromCents(recoveredThisMonthCents),
    missedCallsConvertedText: String(missedCallsConverted),
    bookedAppointmentsText: String(bookedAppointments)
  };
}

function deriveTopbarMetrics(overview = {}, options = {}) {
  const nowMs = Date.now();
  const now = new Date(nowMs);
  const rangeDays = 30;
  const rangeStartMs = nowMs - ((rangeDays - 1) * 24 * 60 * 60 * 1000);
  const todayStartMs = new Date(now.getFullYear(), now.getMonth(), now.getDate()).getTime();
  const tomorrowStartMs = todayStartMs + (24 * 60 * 60 * 1000);
  const activeTo = String(getActiveTo() || "").trim();
  const bookedRows = safeArray(state.threads).filter((thread) => {
    const to = String(thread?.to || "").trim();
    return !activeTo || !to || to === activeTo;
  }).map((thread) => {
    const normalizedThread = {
      ...thread,
      leadData: { ...(thread?.leadData || {}) }
    };
    applyRealtimeBookedState(normalizedThread, { refreshRecovered: false });
    const ld = normalizedThread?.leadData || {};
    const status = String(normalizedThread?.status || "").toLowerCase();
    const stage = String(normalizedThread?.stage || "").toLowerCase();
    const snippet = String(normalizedThread?.lastText || "").toLowerCase();
    const bookedDividerMs = getLatestBookedConfirmationTime(normalizedThread);
    const bookedPersistedMs = coerceTimestampMs(normalizedThread?.bookingTime || ld?.booking_time, 0);
    const amount = Number(resolveConversationAmount(normalizedThread) || 0);
    const amountCents = Number.isFinite(amount) && amount > 0 ? Math.round(amount * 100) : 0;
    const bookedLike = status === "booked"
      || status === "closed"
      || /booked|appointment_booked|scheduled/.test(stage)
      || /booked\s*:|i see you booked|appointment booked|scheduled/.test(snippet)
      || (Number.isFinite(bookedDividerMs) && bookedDividerMs > 0)
      || (Number.isFinite(bookedPersistedMs) && bookedPersistedMs > 0);
    if (!bookedLike || !(amountCents > 0)) return null;
    const ts = coerceTimestampMs(
      bookedPersistedMs || bookedDividerMs || normalizedThread?.updatedAt || normalizedThread?.lastActivityAt || normalizedThread?.bookingTime || ld?.booking_time,
      0
    );
    if (!(Number.isFinite(ts) && ts > 0)) return null;
    return {
      ts: Math.min(ts, nowMs),
      amountCents: Math.max(0, Number(amountCents || 0)),
      customerKey: String(normalizedThread?.from || normalizedThread?.phone || "").trim().toLowerCase()
    };
  }).filter(Boolean);

  const recoveredInRangeCents = bookedRows.reduce((acc, row) => {
    const ts = Number(row?.ts || 0);
    if (!Number.isFinite(ts) || ts < rangeStartMs || ts > nowMs) return acc;
    return acc + Math.max(0, Number(row?.amountCents || 0));
  }, 0);
  const allBookedRecoveredCents = bookedRows.reduce((acc, row) => acc + Math.max(0, Number(row?.amountCents || 0)), 0);
  const bookedTodayCount = bookedRows.filter((row) => {
    const ts = Number(row?.ts || 0);
    return Number.isFinite(ts) && ts >= todayStartMs && ts < tomorrowStartMs;
  }).length;
  const bookedInRangeCount = bookedRows.filter((row) => {
    const ts = Number(row?.ts || 0);
    return Number.isFinite(ts) && ts >= rangeStartMs && ts <= nowMs;
  }).length;
  const allBookedCount = bookedRows.length;
  const bookedCountDisplay = bookedInRangeCount > 0 ? bookedInRangeCount : allBookedCount;
  const savedContactSet = options?.savedContactSet instanceof Set
    ? options.savedContactSet
    : new Set(safeArray(options?.savedContactPhones).map((p) => normalizePhone(String(p || ""))).filter(Boolean));
  const todayTexterSet = new Set();
  const allTexterSet = new Set();
  safeArray(state.threads).forEach((thread) => {
    const to = String(thread?.to || "").trim();
    if (activeTo && to && to !== activeTo) return;
    const from = normalizePhone(String(thread?.from || thread?.phone || "").trim());
    if (!from) return;
    allTexterSet.add(from);
    const ts = coerceTimestampMs(thread?.updatedAt || thread?.lastActivityAt || thread?.createdAt, 0);
    if (!(Number.isFinite(ts) && ts >= todayStartMs && ts < tomorrowStartMs)) return;
    todayTexterSet.add(from);
  });
  const newCustomersTodayCountRaw = Array.from(todayTexterSet).filter((phone) => !savedContactSet.has(phone)).length;
  const newCustomersFallback = Array.from(allTexterSet).filter((phone) => !savedContactSet.has(phone)).length;
  const newCustomersTodayCount = newCustomersTodayCountRaw > 0 ? newCustomersTodayCountRaw : newCustomersFallback;
  const fallbackRecovery = deriveRecoveryMetricsFromOverview(overview || {});
  return {
    line1Value: (recoveredInRangeCents > 0 ? moneyFromCents(recoveredInRangeCents) : (allBookedRecoveredCents > 0 ? moneyFromCents(allBookedRecoveredCents) : fallbackRecovery.recoveredThisMonthText)),
    line1Label: "recovered this month",
    line2Value: String(bookedCountDisplay),
    line2Label: "booked this month",
    line3Value: String(newCustomersTodayCount),
    line3Label: "new customers this month"
  };
}
function viewScheduleBooking() {
  const wrap = document.createElement("div");
  wrap.className = "col app-view app-view-schedule-booking";
  const today = new Date();
  const isoDate = `${today.getFullYear()}-${String(today.getMonth() + 1).padStart(2, "0")}-${String(today.getDate()).padStart(2, "0")}`;
  wrap.innerHTML = `
    <div class="card">
      <div class="row" style="justify-content:space-between; align-items:center; gap:8px; flex-wrap:wrap;">
        <div>
          <div class="h1" style="margin:0;">One-Time Booking</div>
          <div class="p">Manually book a customer. This updates schedule, conversation lifecycle, analytics, and invoice flow.</div>
        </div>
        <button type="button" class="btn" id="manualBookingBackBtn">Back to Schedule</button>
      </div>
      <div style="height:10px;"></div>
      <form id="manualBookingForm" class="col" style="gap:10px;">
        <div class="grid2">
          <div class="col">
            <label class="p">Customer Name</label>
            <input class="input" id="manualBookingCustomerName" required maxlength="160" placeholder="John Smith" />
          </div>
          <div class="col">
            <label class="p">Customer Phone</label>
            <input class="input" id="manualBookingCustomerPhone" required maxlength="32" placeholder="+18145551234" />
          </div>
        </div>
        <div class="grid2">
          <div class="col">
            <label class="p">Customer Email</label>
            <input class="input" id="manualBookingCustomerEmail" type="email" maxlength="254" placeholder="john@example.com" />
          </div>
          <div class="col">
            <label class="p">Service</label>
            <input class="input" id="manualBookingService" maxlength="160" placeholder="Interior Detail" />
          </div>
        </div>
        <div class="grid2">
          <div class="col">
            <label class="p">Vehicle</label>
            <input class="input" id="manualBookingVehicle" maxlength="160" placeholder="2022 Tesla Model 3" />
          </div>
          <div class="col">
            <label class="p">Amount (USD)</label>
            <input class="input" id="manualBookingAmount" type="number" min="0" step="1" placeholder="250" />
          </div>
        </div>
        <div class="grid2">
          <div class="col">
            <label class="p">Date</label>
            <input class="input" id="manualBookingDate" type="date" value="${isoDate}" required />
          </div>
          <div class="col">
            <label class="p">Location</label>
            <input class="input" id="manualBookingLocation" maxlength="240" placeholder="123 Main St" />
          </div>
        </div>
        <div class="grid2">
          <div class="col">
            <label class="p">Start Time</label>
            <input class="input" id="manualBookingStartTime" type="time" value="09:00" required />
          </div>
          <div class="col">
            <label class="p">End Time</label>
            <input class="input" id="manualBookingEndTime" type="time" value="10:00" required />
          </div>
        </div>
        <div class="col">
          <label class="p">Notes</label>
          <textarea class="input" id="manualBookingNotes" rows="4" maxlength="2000" placeholder="Any booking details..."></textarea>
        </div>
        <div class="row" style="justify-content:flex-end; align-items:center; gap:10px;">
          <div class="p" id="manualBookingStatus" style="min-height:18px; margin-right:auto;"></div>
          <button type="submit" class="btn primary" id="manualBookingSubmitBtn">Book Now</button>
        </div>
      </form>
    </div>
  `;

  setTimeout(() => {
    const backBtn = document.getElementById("manualBookingBackBtn");
    const form = document.getElementById("manualBookingForm");
    const statusEl = document.getElementById("manualBookingStatus");
    const submitBtn = document.getElementById("manualBookingSubmitBtn");
    backBtn?.addEventListener("click", () => {
      state.view = "schedule";
      render();
    });

    form?.addEventListener("submit", async (event) => {
      event.preventDefault();
      if (!submitBtn) return;
      const name = String(document.getElementById("manualBookingCustomerName")?.value || "").trim();
      const phone = String(document.getElementById("manualBookingCustomerPhone")?.value || "").trim();
      const email = String(document.getElementById("manualBookingCustomerEmail")?.value || "").trim();
      const service = String(document.getElementById("manualBookingService")?.value || "").trim();
      const vehicle = String(document.getElementById("manualBookingVehicle")?.value || "").trim();
      const amountRaw = String(document.getElementById("manualBookingAmount")?.value || "").trim();
      const date = String(document.getElementById("manualBookingDate")?.value || "").trim();
      const startTime = String(document.getElementById("manualBookingStartTime")?.value || "").trim();
      const endTime = String(document.getElementById("manualBookingEndTime")?.value || "").trim();
      const location = String(document.getElementById("manualBookingLocation")?.value || "").trim();
      const notes = String(document.getElementById("manualBookingNotes")?.value || "").trim();

      if (!name || !phone || !date || !startTime || !endTime) {
        if (statusEl) statusEl.textContent = "Name, phone, date, and start/end time are required.";
        return;
      }

      const bookingStart = new Date(`${date}T${startTime}:00`).getTime();
      const bookingEnd = new Date(`${date}T${endTime}:00`).getTime();
      if (!Number.isFinite(bookingStart) || !Number.isFinite(bookingEnd) || bookingEnd <= bookingStart) {
        if (statusEl) statusEl.textContent = "Invalid booking time range.";
        return;
      }
      const amount = amountRaw ? Number(amountRaw) : null;
      if (amountRaw && (!Number.isFinite(amount) || amount < 0)) {
        if (statusEl) statusEl.textContent = "Amount must be 0 or greater.";
        return;
      }

      submitBtn.disabled = true;
      if (statusEl) statusEl.textContent = "Creating booking...";
      try {
        await apiPost("/api/bookings/manual", {
          customerName: name,
          customerPhone: phone,
          customerEmail: email,
          service,
          vehicle,
          notes,
          amount: amount != null ? amount : undefined,
          bookingTime: bookingStart,
          bookingEndTime: bookingEnd,
          location
        });
        state.analyticsSummaryCache = {};
        state.analyticsError = null;
        state.homeOverviewCache = {};
        state.homeOverviewError = null;
        state.homeFunnelCache = {};
        state.homeFunnelError = null;
        state.homeWinsCache = {};
        state.homeWinsError = null;
        state.scheduleFocusDate = bookingStart;
        try { await loadThreads(); } catch {}
        try { state.scheduleAccount = await loadAccountSettings(getActiveTo()); } catch {}
        state.view = "schedule";
        await render();
      } catch (err) {
        if (statusEl) statusEl.textContent = err?.message || "Failed to create booking.";
        submitBtn.disabled = false;
      }
    });
  }, 0);

  return wrap;
}

function viewSchedule() {
  const wrap = document.createElement("div");
  wrap.className = "schedule-page app-view app-view-schedule";
  function syncScheduleTopOffset() {
    const topbar = document.querySelector(".topbar");
    const measured = Math.max(88, Math.round(topbar?.getBoundingClientRect()?.height || 112));
    document.documentElement.style.setProperty("--schedule-top-offset", `${measured}px`);
  }
  const hiddenEventsKey = `mc_schedule_hidden_events_v1:${getActiveTo()}`;
  const customEventsKey = `mc_schedule_custom_events_v1:${getActiveTo()}`;
  const simulatedBookingsKey = `mc_schedule_sim_bookings_v1:${getActiveTo()}`;
  const hiddenEventIds = new Set(loadLS(hiddenEventsKey, []));
  function persistHiddenEvents() {
    saveLS(hiddenEventsKey, Array.from(hiddenEventIds));
  }
  function toStoredEvent(event) {
    if (!event || !event.id || !(event.start instanceof Date) || !(event.end instanceof Date)) return null;
    return {
      id: String(event.id),
      title: String(event.title || "").trim(),
      start: event.start.getTime(),
      end: event.end.getTime(),
      allDay: Boolean(event.allDay),
      color: String(event.color || ""),
      calendar: String(event.calendar || "Calendar"),
      meta: event.meta && typeof event.meta === "object" ? event.meta : {},
    };
  }
  function fromStoredEvent(row) {
    const startMs = Number(row?.start);
    const endMs = Number(row?.end);
    if (!(Number.isFinite(startMs) && Number.isFinite(endMs) && endMs > startMs)) return null;
    const start = new Date(startMs);
    const end = new Date(endMs);
    return {
      id: String(row?.id || `stored-${startMs}`),
      title: String(row?.title || "Event"),
      start,
      end,
      allDay: Boolean(row?.allDay),
      color: String(row?.color || calendarColorById[String(row?.calendar || "")] || "#5fa8ff"),
      calendar: String(row?.calendar || "Calendar"),
      meta: row?.meta && typeof row.meta === "object" ? row.meta : {},
    };
  }
  function loadStoredEvents(key) {
    const raw = loadLS(key, []);
    if (!Array.isArray(raw)) return [];
    return raw.map(fromStoredEvent).filter(Boolean);
  }
  function saveStoredEvents(key, events) {
    const payload = (Array.isArray(events) ? events : [])
      .map(toStoredEvent)
      .filter(Boolean)
      .slice(-500);
    saveLS(key, payload);
  }
  function upsertStoredEvent(key, event) {
    const next = loadStoredEvents(key).filter((row) => String(row?.id || "") !== String(event?.id || ""));
    next.push(event);
    saveStoredEvents(key, next);
  }
  function removeStoredEventById(eventId) {
    const id = String(eventId || "").trim();
    if (!id) return;
    for (const key of [customEventsKey, simulatedBookingsKey]) {
      const next = loadStoredEvents(key).filter((row) => String(row?.id || "") !== id);
      saveStoredEvents(key, next);
    }
  }
  const dayNames = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"];
  const monthNames = ["January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December"];
  const calendarSections = [
    {
      title: "ARCRELAY",
      items: [
        { id: "Calendar", label: "Calendar", color: "#5fa8ff" },
        { id: "Booked", label: "Booked", color: "#7f1d1d" },
        { id: "Bills Due", label: "Bills Due", color: "#f97316" },
        { id: "Holidays", label: "Holidays", color: "#ef4444" },
        { id: "Scheduled Reminders", label: "Scheduled Reminders", color: "#93c5fd" },
        { id: "Birthdays", label: "Birthdays", color: "#fb7185" },
        { id: "Vacations", label: "Vacations", color: "#14b8a6" },
      ],
    },
  ];
  const calendarColorById = {};
  for (const section of calendarSections) {
    for (const item of section.items) {
      calendarColorById[item.id] = item.color;
    }
  }

  function startOfDay(value) {
    return new Date(value.getFullYear(), value.getMonth(), value.getDate());
  }

  function toIsoDay(value) {
    return `${value.getFullYear()}-${String(value.getMonth() + 1).padStart(2, "0")}-${String(value.getDate()).padStart(2, "0")}`;
  }

  function isSameDay(a, b) {
    return a.getFullYear() === b.getFullYear()
      && a.getMonth() === b.getMonth()
      && a.getDate() === b.getDate();
  }

  function shiftDate(base, amount, unit) {
    const next = new Date(base);
    if (unit === "day") next.setDate(next.getDate() + amount);
    if (unit === "week") next.setDate(next.getDate() + amount * 7);
    if (unit === "month") next.setMonth(next.getMonth() + amount);
    if (unit === "year") next.setFullYear(next.getFullYear() + amount);
    return next;
  }

  function startOfWeek(value) {
    return shiftDate(startOfDay(value), -startOfDay(value).getDay(), "day");
  }

  function endOfWeek(value) {
    return shiftDate(startOfWeek(value), 6, "day");
  }

  function formatRangeTitle(start, end) {
    const sameMonth = start.getMonth() === end.getMonth();
    const sameYear = start.getFullYear() === end.getFullYear();
    if (sameMonth && sameYear) {
      return `${monthNames[start.getMonth()]} ${start.getDate()}-${end.getDate()}, ${start.getFullYear()}`;
    }
    if (sameYear) {
      return `${monthNames[start.getMonth()]} ${start.getDate()} - ${monthNames[end.getMonth()]} ${end.getDate()}, ${start.getFullYear()}`;
    }
    return `${monthNames[start.getMonth()]} ${start.getDate()}, ${start.getFullYear()} - ${monthNames[end.getMonth()]} ${end.getDate()}, ${end.getFullYear()}`;
  }

  function observedHoliday(date) {
    const d = startOfDay(date);
    if (d.getDay() === 0) return shiftDate(d, 1, "day");
    if (d.getDay() === 6) return shiftDate(d, -1, "day");
    return d;
  }

  function nthWeekdayOfMonth(year, month, weekday, nth) {
    const first = new Date(year, month, 1);
    const offset = (7 + weekday - first.getDay()) % 7;
    return new Date(year, month, 1 + offset + ((nth - 1) * 7));
  }

  function lastWeekdayOfMonth(year, month, weekday) {
    const last = new Date(year, month + 1, 0);
    const offset = (7 + last.getDay() - weekday) % 7;
    return new Date(year, month, last.getDate() - offset);
  }

  function holidayEvent(name, date) {
    const start = startOfDay(date);
    return {
      id: `holiday-${name}-${toIsoDay(start)}`,
      title: name,
      start,
      end: shiftDate(start, 1, "day"),
      allDay: true,
      color: "#ef4444",
      calendar: "Holidays",
    };
  }

  function buildHolidayEventsForYear(year) {
    const items = [];
    items.push(holidayEvent("New Year's Day", observedHoliday(new Date(year, 0, 1))));
    items.push(holidayEvent("Martin Luther King Jr. Day", nthWeekdayOfMonth(year, 0, 1, 3)));
    items.push(holidayEvent("Presidents' Day", nthWeekdayOfMonth(year, 1, 1, 3)));
    items.push(holidayEvent("Memorial Day", lastWeekdayOfMonth(year, 4, 1)));
    items.push(holidayEvent("Juneteenth", observedHoliday(new Date(year, 5, 19))));
    items.push(holidayEvent("Independence Day", observedHoliday(new Date(year, 6, 4))));
    items.push(holidayEvent("Labor Day", nthWeekdayOfMonth(year, 8, 1, 1)));
    items.push(holidayEvent("Columbus Day", nthWeekdayOfMonth(year, 9, 1, 2)));
    items.push(holidayEvent("Veterans Day", observedHoliday(new Date(year, 10, 11))));
    items.push(holidayEvent("Thanksgiving", nthWeekdayOfMonth(year, 10, 4, 4)));
    items.push(holidayEvent("Christmas Day", observedHoliday(new Date(year, 11, 25))));
    return items;
  }

  function buildEvents() {
    const result = [];
    const internalBookings = Array.isArray(state?.scheduleAccount?.internalBookings)
      ? state.scheduleAccount.internalBookings
      : [];
    const seen = new Set();

    function pushEvent(event) {
      if (!event || hiddenEventIds.has(String(event.id || ""))) return;
      const key = `${event.calendar}|${event.title}|${event.start.toISOString()}`;
      if (!seen.has(key)) {
        seen.add(key);
        result.push(event);
      }
    }

    for (const booking of internalBookings) {
      const status = String(booking?.status || '').toLowerCase();
      if (status === 'canceled') continue;
      const startMs = Number(booking?.start);
      const endMs = Number(booking?.end);
      if (!Number.isFinite(startMs) || !Number.isFinite(endMs) || endMs <= startMs) continue;
      const start = new Date(startMs);
      const end = new Date(endMs);
      const customerName = String(booking?.customerName || '').trim();
      const customerPhone = String(booking?.customerPhone || '').trim();
      const serviceName = String(booking?.serviceName || booking?.serviceId || 'Appointment').trim();
      const notes = String(booking?.notes || '').trim();
      const serviceRequired = notes ? `${serviceName} - ${notes}` : serviceName;
      const title = customerName
        ? `Booked: ${customerName}`
        : (customerPhone ? `Booked: ${customerPhone}` : `Booked: ${serviceName}`);

      pushEvent({
        id: `internal-booking-${String(booking?.id || startMs)}`,
        title,
        start,
        end,
        allDay: false,
        color: "#7f1d1d",
        calendar: "Booked",
        meta: {
          bookingId: String(booking?.id || "").trim() || undefined,
          jobType: String(booking?.jobType || serviceName || "").trim() || undefined,
          customer: customerName || undefined,
          phone: customerPhone || undefined,
          email: String(booking?.customerEmail || "").trim() || undefined,
          service: serviceName || undefined,
          serviceRequired: serviceRequired || undefined,
          notes: notes || undefined,
          vehicle: String(booking?.vehicleSize || booking?.vehicle || "").trim() || undefined,
          condition: String(booking?.conditionLevel || "").trim() || undefined,
          addOns: Array.isArray(booking?.addOns)
            ? booking.addOns.map((x) => String(x || "").trim()).filter(Boolean)
            : (String(booking?.addOns || "").trim()
              ? String(booking.addOns).split(",").map((x) => x.trim()).filter(Boolean)
              : []),
          tintConfig: String(booking?.tintConfig || "").trim() || undefined,
          agreedPrice: booking?.agreedPrice != null ? booking.agreedPrice : undefined,
          depositStatus: String(booking?.depositStatus || "").trim() || undefined,
          collectedAnswers: String(booking?.collectedAnswers || "").trim() || undefined,
          confidence: String(booking?.confidence || "").trim() || undefined
        }
      });
    }

    const threads = Array.isArray(state?.threads) ? state.threads : [];
    for (const thread of threads) {
      if (isSimulatedConversationLike(thread)) continue;
      const status = String(thread?.status || "").toLowerCase();
      const stage = String(thread?.stage || "").toLowerCase();
      const ld = thread?.leadData || {};
      let bookingMs = Number(thread?.bookingTime || ld?.booking_time || ld?.bookingTime || 0);
      if (!(Number.isFinite(bookingMs) && bookingMs > 0) && Array.isArray(thread?.messages)) {
        for (const m of thread.messages) {
          const candidate = Number(m?.meta?.bookingTime || m?.payload?.bookingTime || m?.bookingTime || m?.payload?.booking_time || 0);
          if (Number.isFinite(candidate) && candidate > 0) {
            bookingMs = Math.max(bookingMs || 0, candidate);
          }
        }
      }
      const bookedLike = status === "booked"
        || /booked|appointment_booked|scheduled/.test(stage)
        || (Number.isFinite(bookingMs) && bookingMs > 0);
      if (!bookedLike || !(Number.isFinite(bookingMs) && bookingMs > 0)) continue;

      const start = new Date(bookingMs);
      const durationMin = Number(thread?.durationMin || ld?.duration_min || 60);
      const end = new Date(start.getTime() + Math.max(15, Number.isFinite(durationMin) ? durationMin : 60) * 60 * 1000);
      const customerName = String(thread?.name || ld?.customer_name || "").trim();
      const customerPhone = String(thread?.from || "").trim();
      const serviceName = String(ld?.service_required || ld?.request || "Appointment").trim();
      const normalizedVehicle = normalizeBookedVehicleValue(
        ld?.vehicle_year && ld?.vehicle_make && ld?.vehicle_model
          ? `${ld.vehicle_year} ${ld.vehicle_make} ${ld.vehicle_model}`
          : "",
        ld?.vehicle,
        ld?.vehicle_model,
        ld?.vehicleName
      );
      const addOns = Array.isArray(ld?.addOns)
        ? ld.addOns.map((x) => String(x || "").trim()).filter(Boolean)
        : (String(ld?.add_ons || "").trim()
          ? String(ld.add_ons).split(",").map((x) => x.trim()).filter(Boolean)
          : []);
      const title = customerName
        ? `Booked: ${customerName}`
        : (customerPhone ? `Booked: ${customerPhone}` : `Booked: ${serviceName}`);

      pushEvent({
        id: `thread-booking-${String(thread?.id || bookingMs)}`,
        title,
        start,
        end,
        allDay: false,
        color: "#7f1d1d",
        calendar: "Booked",
        meta: {
          bookingId: String(thread?.id || "").trim() || undefined,
          jobType: String(ld?.jobType || ld?.job_type || serviceName || "").trim() || undefined,
          customer: customerName || undefined,
          phone: customerPhone || undefined,
          email: String(ld?.customer_email || ld?.email || "").trim() || undefined,
          service: serviceName || undefined,
          serviceRequired: String(ld?.service_required || "").trim() || undefined,
          notes: String(ld?.notes || "").trim() || undefined,
          vehicle: normalizedVehicle || undefined,
          condition: String(ld?.conditionLevel || ld?.condition_level || "").trim() || undefined,
          addOns,
          tintConfig: String(ld?.tintConfig || ld?.tint_config || "").trim() || undefined,
          agreedPrice: Number(thread?.amount || ld?.amount || 0) || undefined,
          depositStatus: String(ld?.depositStatus || ld?.deposit_status || "").trim() || undefined,
          collectedAnswers: String(ld?.collectedAnswers || ld?.collected_answers || "").trim() || undefined,
          confidence: String(ld?.confidence || "").trim() || undefined,
          source: String(thread?.id || "").startsWith("sim-") ? "simulated" : "thread_fallback"
        }
      });
    }

    for (const event of loadStoredEvents(customEventsKey)) {
      pushEvent(event);
    }
    return result;
  }

  const today = startOfDay(new Date());
  const focusMs = Number(state.scheduleFocusDate || 0);
  const focusDate = (Number.isFinite(focusMs) && focusMs > 0) ? new Date(focusMs) : null;
  const selectedCalendars = {};
  for (const section of calendarSections) {
    for (const item of section.items) selectedCalendars[item.id] = true;
  }

  const baseDate = focusDate || today;
  if (focusDate) {
    state.scheduleFocusDate = null;
  }
  const calState = {
    currentView: "month",
    currentDate: new Date(baseDate.getFullYear(), baseDate.getMonth(), 1),
    selectedDate: new Date(baseDate),
    selectedCalendars,
    events: buildEvents(),
    drawerOpen: false,
    drawerEventId: "",
    holidayYears: new Set(),
    showAddEventModal: false,
    showImportHelpModal: false,
    monthTransitionDir: 0,
    wheelDeltaY: 0,
    wheelLockUntil: 0,
  };

  function pushUniqueEvents(events) {
    const existing = new Set(calState.events.map((ev) => ev.id));
    for (const event of events) {
      if (!existing.has(event.id)) {
        existing.add(event.id);
        calState.events.push(event);
      }
    }
  }

  function ensureHolidayYearsForDate(anchorDate) {
    const year = anchorDate.getFullYear();
    for (let y = year - 1; y <= year + 1; y += 1) {
      if (calState.holidayYears.has(y)) continue;
      pushUniqueEvents(buildHolidayEventsForYear(y));
      calState.holidayYears.add(y);
    }
  }

  ensureHolidayYearsForDate(calState.currentDate);

  function eventIntersectsDay(event, date) {
    const dayStart = startOfDay(date).getTime();
    const dayEnd = dayStart + (24 * 60 * 60 * 1000);
    const eventStart = Number(event?.start?.getTime?.() || 0);
    const eventEndRaw = Number(event?.end?.getTime?.() || 0);
    const eventEnd = eventEndRaw > eventStart ? eventEndRaw : (eventStart + 60 * 1000);
    return eventStart < dayEnd && eventEnd > dayStart;
  }

  function getVisibleEventsForDate(date, allDayOnly = null) {
    return calState.events.filter((event) => {
      if (!calState.selectedCalendars[event.calendar]) return false;
      if (allDayOnly === true && !event.allDay) return false;
      if (allDayOnly === false && event.allDay) return false;
      return eventIntersectsDay(event, date);
    });
  }

  function getVisibleEventsInRange(rangeStart, rangeEnd, allDayOnly = null) {
    const start = startOfDay(rangeStart).getTime();
    const end = startOfDay(rangeEnd).getTime();
    return calState.events.filter((event) => {
      if (!calState.selectedCalendars[event.calendar]) return false;
      if (allDayOnly === true && !event.allDay) return false;
      if (allDayOnly === false && event.allDay) return false;
      const cursor = new Date(start);
      while (cursor.getTime() <= end) {
        if (eventIntersectsDay(event, cursor)) return true;
        cursor.setDate(cursor.getDate() + 1);
      }
      return false;
    });
  }

  function EventChip(event, { interactive = false } = {}) {
    const title = escapeHtml(event.title || "");
    const holidayClass = event.calendar === "Holidays" ? " is-holiday" : "";
    const chipClass = event.allDay ? `mc-event-chip is-all-day${holidayClass}` : `mc-event-chip${holidayClass}`;
    const openAttrs = interactive ? ` data-mc-open-event="${escapeAttr(String(event?.id || ""))}"` : "";
    if (event.calendar === "Booked") {
      const customer = String(event?.meta?.customer || "").trim();
      const phone = String(event?.meta?.phone || "").trim();
      const bookedTitle = customer ? `Booked: ${escapeHtml(customer)}` : title;
      const phoneLine = interactive && phone ? `<div style="font-size:11px;opacity:.9;line-height:1.2;margin-top:2px;">${escapeHtml(phone)}</div>` : "";
      if (interactive) {
        return `<button type="button" class="${chipClass} mc-event-chip-btn" style="--chip-color:${event.color};" title="${title}"${openAttrs}><div>${bookedTitle}</div>${phoneLine}</button>`;
      }
      return `<div class="${chipClass}" style="--chip-color:${event.color};" title="${title}">${bookedTitle}</div>`;
    }
    if (interactive) {
      return `<button type="button" class="${chipClass} mc-event-chip-btn" style="--chip-color:${event.color};" title="${title}"${openAttrs}>${title}</button>`;
    }
    return `<div class="${chipClass}" style="--chip-color:${event.color};" title="${title}">${title}</div>`;
  }

  function getEventEndInclusiveDay(event) {
    const start = startOfDay(event.start);
    const endRaw = startOfDay(event.end);
    if (!(endRaw instanceof Date) || Number.isNaN(endRaw.getTime())) return start;
    if (event.allDay) {
      if (endRaw.getTime() <= start.getTime()) return start;
      return shiftDate(endRaw, -1, "day");
    }
    if (endRaw.getTime() < start.getTime()) return start;
    return endRaw;
  }

  function isVacationSpanEvent(event) {
    const eventType = String(event?.meta?.eventType || "").toLowerCase();
    if (eventType !== "vacation") return false;
    const start = startOfDay(event.start);
    const endInclusive = getEventEndInclusiveDay(event);
    return endInclusive.getTime() > start.getTime();
  }

  function EventChipForDate(event, cellDate, { interactive = false } = {}) {
    if (!isVacationSpanEvent(event)) return EventChip(event, { interactive });
    const day = startOfDay(cellDate);
    const start = startOfDay(event.start);
    const endInclusive = getEventEndInclusiveDay(event);
    const atTrueStart = isSameDay(day, start);
    const atTrueEnd = isSameDay(day, endInclusive);
    const weekStart = day.getDay() === 0;
    const weekEnd = day.getDay() === 6;
    const capLeft = atTrueStart;
    const capRight = atTrueEnd;
    const showLabel = atTrueStart;
    const cls = [
      "mc-event-chip",
      "is-all-day",
      "is-span-segment",
      capLeft ? "is-span-start" : "is-span-flat-left",
      capRight ? "is-span-end" : "is-span-flat-right",
      weekStart && !capLeft ? "is-span-week-start" : "",
      weekEnd && !capRight ? "is-span-week-end" : "",
    ].filter(Boolean).join(" ");
    const title = escapeHtml(event.title || "");
    const body = showLabel ? title : "&nbsp;";
    if (interactive) {
      return `<button type="button" class="${cls} mc-event-chip-btn" style="--chip-color:${event.color};" title="${title}" data-mc-open-event="${escapeAttr(String(event?.id || ""))}">${body}</button>`;
    }
    return `<div class="${cls}" style="--chip-color:${event.color};" title="${title}">${body}</div>`;
  }

  function normalizePersonName(value) {
    return String(value || "").toLowerCase().replace(/[^a-z0-9]+/g, " ").replace(/\s+/g, " ").trim();
  }

  const customerPhoneByName = (() => {
    const map = {};
    const internalBookings = Array.isArray(state?.scheduleAccount?.internalBookings) ? state.scheduleAccount.internalBookings : [];
    for (const b of internalBookings) {
      const name = normalizePersonName(b?.customerName);
      const phone = String(b?.customerPhone || "").trim();
      if (name && phone && !map[name]) map[name] = phone;
    }
    for (const t of (state.threads || [])) {
      const name = normalizePersonName(t?.leadData?.customer_name || "");
      const phone = String(t?.from || "").trim();
      if (name && phone && !map[name]) map[name] = phone;
    }
    return map;
  })();

  function DayCell(cellDate, inCurrentMonth) {
    const selected = isSameDay(cellDate, calState.selectedDate);
    const isToday = isSameDay(cellDate, today);
    const events = getVisibleEventsForDate(cellDate, null);
    const firstEvent = events[0] || null;
    const secondEvent = events[1] || null;
    const extraCount = Math.max(events.length - 2, 0);

    return `
      <button type="button" class="mc-day-cell${inCurrentMonth ? "" : " is-outside"}${selected ? " is-selected" : ""}${isToday ? " is-today" : ""}" data-mc-day="${toIsoDay(cellDate)}">
        <div class="mc-day-head mc-day-head-inline">
          <span class="mc-day-number">${cellDate.getDate()}</span>
          <div class="mc-day-head-chip">${firstEvent ? EventChipForDate(firstEvent, cellDate) : ""}</div>
        </div>
        <div class="mc-day-events">
          ${secondEvent ? EventChipForDate(secondEvent, cellDate) : ""}
          ${extraCount > 0 ? `<div class="mc-more-events">${extraCount} more</div>` : ""}
        </div>
      </button>
    `;
  }

  function MonthGrid() {
    const firstOfMonth = new Date(calState.currentDate.getFullYear(), calState.currentDate.getMonth(), 1);
    const gridStart = shiftDate(firstOfMonth, -firstOfMonth.getDay(), "day");
    const dayCells = [];

    for (let i = 0; i < 42; i++) {
      const date = shiftDate(gridStart, i, "day");
      const inCurrentMonth = date.getMonth() === calState.currentDate.getMonth();
      dayCells.push(DayCell(date, inCurrentMonth));
    }

    const fxClass = calState.monthTransitionDir > 0
      ? " is-transition-next"
      : (calState.monthTransitionDir < 0 ? " is-transition-prev" : "");
    return `
      <div class="mc-month-view${fxClass}">
        <div class="mc-weekday-row">
          ${dayNames.map((name) => `<div class="mc-weekday">${name}</div>`).join("")}
        </div>
        <div class="mc-month-grid">
          ${dayCells.join("")}
        </div>
      </div>
    `;
  }

  function DayView() {
    const date = startOfDay(calState.selectedDate);
    const events = getVisibleEventsForDate(date, null).sort((a, b) => a.start - b.start);

    function formatAmount(value) {
      const n = Number(value);
      if (!Number.isFinite(n)) return "";
      return new Intl.NumberFormat("en-US", { style: "currency", currency: "USD", maximumFractionDigits: 0 }).format(n);
    }

    function eventMetaLine(event) {
      const meta = event.meta || {};
      const parts = [];
      const normalize = (v) => String(v || "").toLowerCase().replace(/\s+/g, " ").trim();
      const service = String(meta.service || "").trim();
      let serviceRequired = String(meta.serviceRequired || "").trim();
      const notes = String(meta.notes || "").trim();
      const resolvedPhone = String(meta.phone || customerPhoneByName[normalizePersonName(meta.customer)] || "").trim();
      // Back-compat: older records may store "Service - notes" in serviceRequired.
      // If so, keep only the notes portion to avoid duplicated service labeling.
      if (service && serviceRequired) {
        const svcNorm = normalize(service);
        const reqNorm = normalize(serviceRequired);
        if (reqNorm.startsWith(`${svcNorm} -`) || reqNorm.startsWith(`${svcNorm}:`)) {
          const splitIdx = serviceRequired.indexOf(" - ") >= 0
            ? serviceRequired.indexOf(" - ")
            : serviceRequired.indexOf(":");
          if (splitIdx >= 0) {
            serviceRequired = String(serviceRequired.slice(splitIdx + (serviceRequired.includes(" - ") ? 3 : 1)) || "").trim();
          }
        }
      }
      if (meta.customer) parts.push(`Customer: ${meta.customer}`);
      if (resolvedPhone) parts.push(`Number: ${resolvedPhone}`);
      if (serviceRequired) {
        parts.push(`Service required: ${serviceRequired}`);
      } else if (service) {
        parts.push(`Service: ${service}`);
      }
      if (meta.vehicle) parts.push(`Vehicle: ${meta.vehicle}`);
      if (meta.amount != null && meta.amount !== "") {
        const formatted = formatAmount(meta.amount);
        if (formatted) parts.push(`Price: ${formatted}`);
      }
      const notesDupServiceRequired = serviceRequired && notes && normalize(serviceRequired).includes(normalize(notes));
      if (notes && !notesDupServiceRequired) parts.push(`Notes: ${notes}`);
      return parts.length ? `<div class="mc-event-meta">${parts.map((line) => `<div>${escapeHtml(line)}</div>`).join("")}</div>` : "";
    }

    function formatTimeLabel(event) {
      if (event.allDay) return "No time";
      return event.start.toLocaleTimeString("en-US", { hour: "numeric", minute: "2-digit" });
    }

    return `
      <div class="mc-day-view">
        <div class="mc-day-title-row">
          <div class="mc-day-title">${escapeHtml(date.toLocaleDateString("en-US", { weekday: "long", month: "long", day: "numeric", year: "numeric" }))}</div>
        </div>
        <section class="mc-day-panel">
          <h3>Schedule</h3>
          <div class="mc-day-list">
            ${events.length ? events.map((event) => `
              <div class="mc-day-row" data-mc-event-id="${escapeAttr(String(event.id || ""))}">
                <div class="mc-day-time">${escapeHtml(formatTimeLabel(event))}</div>
                <div class="mc-day-event-wrap">
                  ${event.calendar !== "Holidays" ? `<button type="button" class="mc-day-delete-btn" data-mc-delete-event="${escapeAttr(String(event.id || ""))}" title="Delete from schedule" aria-label="Delete from schedule">
                    <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" aria-hidden="true">
                      <path d="M3 6h18"></path>
                      <path d="M8 6V4h8v2"></path>
                      <path d="M19 6l-1 14H6L5 6"></path>
                      <path d="M10 11v6"></path>
                      <path d="M14 11v6"></path>
                    </svg>
                  </button>` : ""}
                  ${EventChip(event, { interactive: true })}
                  ${eventMetaLine(event)}
                </div>
              </div>
            `).join("") : `<div class="mc-empty-note">No scheduled events</div>`}
          </div>
        </section>
      </div>
    `;
  }

  function WeekView() {
    const weekStart = startOfWeek(calState.selectedDate);
    const cols = [];

    for (let i = 0; i < 7; i += 1) {
      const date = shiftDate(weekStart, i, "day");
      const allDayEvents = getVisibleEventsForDate(date, true);
      const timedEvents = getVisibleEventsForDate(date, false).slice(0, 6);
      const isTodayCol = isSameDay(date, today);
      const isSelectedCol = isSameDay(date, calState.selectedDate);
      cols.push(`
        <button type="button" class="mc-week-col${isTodayCol ? " is-today" : ""}${isSelectedCol ? " is-selected" : ""}" data-mc-day="${toIsoDay(date)}">
          <div class="mc-week-col-head">
            <span class="mc-week-col-name">${dayNames[date.getDay()]}</span>
            <span class="mc-week-col-date">${date.getDate()}</span>
          </div>
          <div class="mc-week-col-events">
            ${allDayEvents.map((event) => EventChipForDate(event, date)).join("")}
            ${timedEvents.map((event) => EventChip(event)).join("")}
          </div>
        </button>
      `);
    }

    return `
      <div class="mc-week-view">
        <div class="mc-week-grid">
          ${cols.join("")}
        </div>
      </div>
    `;
  }

  function YearMonthMini(monthIndex) {
    const year = calState.currentDate.getFullYear();
    const first = new Date(year, monthIndex, 1);
    const gridStart = shiftDate(first, -first.getDay(), "day");
    const cells = [];
    let visibleCount = 0;

    for (let i = 0; i < 42; i += 1) {
      const date = shiftDate(gridStart, i, "day");
      const inMonth = date.getMonth() === monthIndex;
      if (inMonth) {
        visibleCount += getVisibleEventsForDate(date).length;
      }
      const isSelected = isSameDay(date, calState.selectedDate);
      const isTodayCell = isSameDay(date, today);
      const hasHoliday = getVisibleEventsForDate(date, true).some((event) => event.calendar === "Holidays");
      cells.push(`
        <button type="button" class="mc-year-day${inMonth ? "" : " is-outside"}${isSelected ? " is-selected" : ""}${isTodayCell ? " is-today" : ""}${hasHoliday ? " has-holiday" : ""}" data-mc-day="${toIsoDay(date)}">${date.getDate()}</button>
      `);
    }

    return `
      <section class="mc-year-month">
        <header class="mc-year-month-head">
          <h3>${monthNames[monthIndex]}</h3>
          <span>${visibleCount} events</span>
        </header>
        <div class="mc-year-weekdays">${dayNames.map((d) => `<span>${d.charAt(0)}</span>`).join("")}</div>
        <div class="mc-year-grid">${cells.join("")}</div>
      </section>
    `;
  }

  function YearView() {
    return `
      <div class="mc-year-view">
        ${Array.from({ length: 12 }, (_, idx) => YearMonthMini(idx)).join("")}
      </div>
    `;
  }

  function getTitleForCurrentView() {
    if (calState.currentView === "day") {
      return calState.selectedDate.toLocaleDateString("en-US", { weekday: "long", month: "long", day: "numeric", year: "numeric" });
    }
    if (calState.currentView === "week") {
      const start = startOfWeek(calState.selectedDate);
      const end = endOfWeek(calState.selectedDate);
      return formatRangeTitle(start, end);
    }
    if (calState.currentView === "year") {
      return String(calState.currentDate.getFullYear());
    }
    return calState.currentDate.toLocaleDateString("en-US", { month: "long", year: "numeric" });
  }

  function alignCurrentDateToView() {
    const sel = startOfDay(calState.selectedDate);
    if (calState.currentView === "month") {
      calState.currentDate = new Date(sel.getFullYear(), sel.getMonth(), 1);
      return;
    }
    if (calState.currentView === "year") {
      calState.currentDate = new Date(sel.getFullYear(), 0, 1);
      return;
    }
    calState.currentDate = new Date(sel);
  }

  function Toolbar() {
    const title = getTitleForCurrentView();
    const viewButtons = [
      { id: "day", label: "Day" },
      { id: "week", label: "Week" },
      { id: "month", label: "Month" },
      { id: "year", label: "Year" },
    ];

    return `
      <header class="mc-toolbar">
        <div class="mc-toolbar-left">
          <h1 class="mc-title">${escapeHtml(title)}</h1>
        </div>
        <div class="mc-toolbar-center" role="tablist" aria-label="Calendar views">
          <div class="mc-segmented">
            ${viewButtons.map((view) => `
              <button type="button" role="tab" data-mc-view="${view.id}" class="mc-segment-btn${calState.currentView === view.id ? " is-active" : ""}" aria-selected="${calState.currentView === view.id ? "true" : "false"}">${view.label}</button>
            `).join("")}
          </div>
        </div>
        <div class="mc-toolbar-right">
          <label class="mc-search">
            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" aria-hidden="true">
              <circle cx="11" cy="11" r="7"></circle>
              <path d="m20 20-3.5-3.5"></path>
            </svg>
            <input type="search" placeholder="Search" aria-label="Search events" />
          </label>
          <button type="button" class="btn mc-toolbar-btn" data-mc-nav="today">Today</button>
          <div class="mc-nav-pair">
            <button type="button" class="btn mc-toolbar-btn" data-mc-nav="prev" aria-label="Previous period">&lsaquo;</button>
            <button type="button" class="btn mc-toolbar-btn" data-mc-nav="next" aria-label="Next period">&rsaquo;</button>
            <button type="button" class="btn mc-toolbar-btn mc-import-help-btn" data-mc-nav="import-help" aria-label="Import help">
              <span class="mc-info-glyph" aria-hidden="true"></span>
              <span>Import help</span>
            </button>
          </div>
        </div>
      </header>
    `;
  }

  function AddEventModal() {
    if (!calState.showAddEventModal) return "";
    const defaultDate = toIsoDay(calState.selectedDate);
    return `
      <div class="billing-modal-overlay" data-mc-modal-overlay="true" aria-hidden="false">
        <div class="billing-modal billing-modal-sm mc-add-event-modal" role="dialog" aria-modal="true" aria-labelledby="mcAddEventTitle">
          <div class="row" style="justify-content:space-between; align-items:center; gap:10px;">
            <div class="h1" id="mcAddEventTitle" style="margin:0;">Add Event</div>
            <button type="button" class="btn" data-mc-modal-close="true">Close</button>
          </div>
          <div style="height:10px;"></div>
          <form id="mcAddEventForm" class="mc-add-event-form">
            <label class="mc-form-field">
              <span>Event Title</span>
              <input class="input" name="title" type="text" required maxlength="120" placeholder="Event title" />
            </label>
            <label class="mc-form-field">
              <span>Event Type</span>
              <select class="select" name="eventType">
                <option value="appointment">Appointment</option>
                <option value="bill">Bill Due</option>
                <option value="birthday">Birthday</option>
                <option value="reminder">Reminder</option>
                <option value="holiday">Holiday</option>
                <option value="vacation">Vacations</option>
              </select>
            </label>
            <div class="mc-form-grid" data-mc-type-show="base-date-calendar">
              <label class="mc-form-field">
                <span>Date</span>
                <input class="input" name="date" type="date" required value="${defaultDate}" />
              </label>
              <label class="mc-form-field">
                <span>Calendar</span>
                <select class="select" name="calendar">
                  ${Object.keys(calendarColorById).map((id) => `<option value="${escapeHtml(id)}">${escapeHtml(id)}</option>`).join("")}
                </select>
              </label>
            </div>
            <div class="mc-form-grid" data-mc-type-show="timed">
              <label class="mc-form-field">
                <span>Start</span>
                <input class="input" name="startTime" type="time" value="09:00" />
              </label>
              <label class="mc-form-field">
                <span>End</span>
                <input class="input" name="endTime" type="time" value="10:00" />
              </label>
            </div>
            <div class="mc-form-grid hidden" data-mc-type-panel="bill">
              <label class="mc-form-field">
                <span>Bill For</span>
                <input class="input" name="billFor" type="text" maxlength="120" placeholder="Shop Rent" />
              </label>
              <label class="mc-form-field">
                <span>Amount (USD)</span>
                <input class="input" name="billAmount" type="number" min="0" step="1" placeholder="1250" />
              </label>
            </div>
            <label class="mc-form-field hidden" data-mc-type-panel="birthday">
              <span>Birthday Person</span>
              <input class="input" name="birthdayPerson" type="text" maxlength="120" placeholder="Jane Doe" />
            </label>
            <label class="mc-form-field hidden" data-mc-type-panel="reminder">
              <span>Reminder Notes</span>
              <textarea class="input" name="reminderNotes" rows="3" maxlength="400" placeholder="What needs to be done?"></textarea>
            </label>
            <div class="mc-form-grid hidden" data-mc-type-panel="vacation">
              <label class="mc-form-field">
                <span>Start Date</span>
                <input class="input" name="vacationStartDate" type="date" value="${defaultDate}" />
              </label>
              <label class="mc-form-field">
                <span>End Date</span>
                <input class="input" name="vacationEndDate" type="date" value="${defaultDate}" />
              </label>
            </div>
            <div class="row" style="justify-content:flex-end; gap:8px; margin-top:12px;">
              <button type="button" class="btn" data-mc-modal-close="true">Cancel</button>
              <button type="submit" class="btn btn-brand">Create</button>
            </div>
          </form>
        </div>
      </div>
    `;
  }

  function ImportHelpModal() {
    if (!calState.showImportHelpModal) return "";
    return `
      <div class="billing-modal-overlay" data-mc-import-overlay="true" aria-hidden="false">
        <div class="billing-modal billing-modal-sm mc-import-help-modal" role="dialog" aria-modal="true" aria-labelledby="mcImportHelpTitle">
          <div class="row" style="justify-content:space-between; align-items:center; gap:10px;">
            <div class="h1" id="mcImportHelpTitle" style="margin:0;">Import Calendar Help</div>
            <button type="button" class="btn" data-mc-import-close="true">Close</button>
          </div>
          <div style="height:10px;"></div>
          <p class="p" style="margin:0;">Bring your existing calendar into Relay using one of these options:</p>
          <ul class="mc-import-help-list">
            <li><b>Upload .ics file</b>: Import an exported calendar file from your current provider.</li>
            <li><b>Subscribe via ICS link</b>: Paste a read-only ICS URL to keep events synced.</li>
            <li><b>Connect Google / Outlook</b>: Use direct provider integration when available.</li>
          </ul>
          <div class="row" style="justify-content:flex-end; gap:8px; margin-top:8px;">
            <button type="button" class="btn" data-mc-import-close="true">Dismiss</button>
            <button type="button" class="btn btn-brand" data-mc-open-calendar-settings="true">Open Calendar Settings</button>
          </div>
        </div>
      </div>
    `;
  }

  function formatJobPrice(value) {
    if (value == null || value === "") return "";
    if (typeof value === "string" && value.trim().startsWith("$")) return value.trim();
    const n = Number(value);
    if (!Number.isFinite(n)) return String(value);
    return new Intl.NumberFormat("en-US", { style: "currency", currency: "USD", maximumFractionDigits: 0 }).format(n);
  }

  function EventDetailDrawer() {
    if (!calState.drawerOpen || !calState.drawerEventId) return "";
    const event = calState.events.find((x) => String(x?.id || "") === String(calState.drawerEventId || ""));
    if (!event) return "";
    const meta = event.meta || {};
    const addOns = Array.isArray(meta.addOns) ? meta.addOns : [];
    const rows = [
      ["Job type", meta.jobType || meta.service || event.title || "Appointment"],
      ["Vehicle", meta.vehicle || "Not provided"],
      ["Condition", meta.condition || "Not provided"],
      ["Add-ons", addOns.length ? addOns.join(", ") : "None"],
      ["Tint config", meta.tintConfig || "Not provided"],
      ["Agreed price", formatJobPrice(meta.agreedPrice) || "Not provided"],
      ["Deposit status", meta.depositStatus || "Not provided"],
      ["Collected answers", meta.collectedAnswers || "Unknown"],
      ["Confidence", meta.confidence || "Unknown"]
    ];
    return `
      <div class="mc-job-drawer-overlay" data-mc-close-drawer="true"></div>
      <aside class="mc-job-drawer" role="dialog" aria-modal="true" aria-label="Scheduled job details">
        <div class="mc-job-drawer-head">
          <div>
            <div class="h1" style="margin:0;">Scheduled Job</div>
            <div class="p">${escapeHtml(event.start.toLocaleString())}</div>
          </div>
          <button type="button" class="btn" data-mc-close-drawer="true">Close</button>
        </div>
        <div class="mc-job-drawer-body">
          ${rows.map(([k, v]) => `
            <div class="mc-job-row">
              <div class="mc-job-key">${escapeHtml(k)}</div>
              <div class="mc-job-val">${escapeHtml(String(v || ""))}</div>
            </div>
          `).join("")}
          ${event.calendar !== "Holidays" ? `
            <div class="mc-job-drawer-actions">
              <button type="button" class="btn mc-job-delete-btn" data-mc-delete-event="${escapeAttr(String(event.id || ""))}">Delete</button>
            </div>
          ` : ""}
        </div>
      </aside>
    `;
  }

  function openCalendarSettings() {
    const targetPath = "/settings/calendar";
    try {
      if (window.location.pathname !== targetPath) {
        history.pushState({ view: "settings", section: "calendar" }, "", targetPath);
      }
    } catch {}
    localStorage.setItem("mc_settings_tab_v1", "schedule");
    calState.showImportHelpModal = false;
    state.view = "settings";
    if (typeof render === "function") render();
  }

  function getAddEventTypeConfig(eventType) {
    const type = String(eventType || "").toLowerCase();
    const calendarByType = {
      appointment: "Calendar",
      bill: "Bills Due",
      birthday: "Birthdays",
      reminder: "Scheduled Reminders",
      holiday: "Holidays",
      vacation: "Vacations",
    };
    return {
      type: calendarByType[type] ? type : "appointment",
      calendar: calendarByType[type] || "Calendar",
      allDay: type !== "appointment",
    };
  }

  function syncAddEventTypeUI(form, eventType) {
    if (!(form instanceof HTMLFormElement)) return;
    const cfg = getAddEventTypeConfig(eventType);
    const panels = form.querySelectorAll("[data-mc-type-panel]");
    for (const panel of panels) {
      const type = String(panel.getAttribute("data-mc-type-panel") || "").trim();
      panel.classList.toggle("hidden", type !== cfg.type);
    }
    const timedRows = form.querySelectorAll("[data-mc-type-show=\"timed\"]");
    for (const row of timedRows) row.classList.toggle("hidden", cfg.type !== "appointment");
    const baseRows = form.querySelectorAll("[data-mc-type-show=\"base-date-calendar\"]");
    for (const row of baseRows) row.classList.toggle("hidden", cfg.type === "vacation");
    const calendarSelect = form.querySelector('select[name="calendar"]');
    if (calendarSelect instanceof HTMLSelectElement && calendarColorById[cfg.calendar]) {
      calendarSelect.value = cfg.calendar;
    }
  }

  function resetScheduleDerivedCaches() {
    state.homeFunnelCache = {};
    state.homeWinsCache = {};
    state.analyticsBookedFallbackCache = {};
    state.analyticsHydratedAtByScope = {};
    state.recoveredTotalsByCustomer = {};
    state.recoveredTotalsLoadingByCustomer = {};
    state.recoveredTotalsOptimisticApplied = {};
  }

  function stripBookedStateFromThread(thread) {
    if (!thread || typeof thread !== "object") return;
    thread.status = "contacted";
    thread.stage = "contacted";
    thread.bookingTime = null;
    thread.amount = null;
    thread.leadData = { ...(thread.leadData || {}) };
    delete thread.leadData.booking_time;
    delete thread.leadData.bookingTime;
    delete thread.leadData.amount;
    thread.leadData.payment_status = "unpaid";
  }

  function removeBookedEventFromSources(event) {
    if (!event || String(event.calendar || "") !== "Booked") return false;
    const eventId = String(event.id || "").trim();
    const meta = event.meta && typeof event.meta === "object" ? event.meta : {};
    const bookedMs = Number(event?.start?.getTime?.() || 0);
    const internalId = eventId.startsWith("internal-booking-") ? eventId.slice("internal-booking-".length) : "";
    const threadIdFromEvent = eventId.startsWith("thread-booking-") ? eventId.slice("thread-booking-".length) : "";
    const simThreadIdFromEvent = eventId.startsWith("sim-booking-") ? eventId.slice("sim-booking-".length) : "";
    const threadId = String(meta.bookingId || meta.threadId || threadIdFromEvent || simThreadIdFromEvent || "").trim();
    let changed = false;

    if (Array.isArray(state?.scheduleAccount?.internalBookings)) {
      const before = state.scheduleAccount.internalBookings.length;
      state.scheduleAccount.internalBookings = state.scheduleAccount.internalBookings.filter((b) => {
        const bid = String(b?.id || "").trim();
        if (internalId && bid === internalId) return false;
        if (threadId && bid === threadId) return false;
        if (Number.isFinite(bookedMs) && bookedMs > 0 && Number(b?.start || 0) === bookedMs) return false;
        return true;
      });
      if (state.scheduleAccount.internalBookings.length !== before) changed = true;
    }

    if (threadId) {
      const thread = (state.threads || []).find((t) => String(t?.id || "").trim() === threadId);
      if (thread) {
        stripBookedStateFromThread(thread);
        changed = true;
      }
      if (String(state?.activeConversation?.id || "").trim() === threadId) {
        stripBookedStateFromThread(state.activeConversation);
        changed = true;
      }
      removeRevenueLedgerRows({ threadId, bookedMs });
    } else if (Number.isFinite(bookedMs) && bookedMs > 0) {
      removeRevenueLedgerRows({ bookedMs });
    }

    return changed;
  }

  function Sidebar() {
    const miniMonthTitle = calState.selectedDate.toLocaleDateString("en-US", { month: "long", year: "numeric" });
    const miniStart = new Date(calState.selectedDate.getFullYear(), calState.selectedDate.getMonth(), 1);
    const miniGridStart = shiftDate(miniStart, -miniStart.getDay(), "day");
    const miniCells = [];

    for (let i = 0; i < 42; i++) {
      const date = shiftDate(miniGridStart, i, "day");
      const isOutside = date.getMonth() !== calState.currentDate.getMonth();
      const isSelected = isSameDay(date, calState.selectedDate);
      miniCells.push(`
        <button type="button" class="mc-mini-day${isOutside ? " is-outside" : ""}${isSelected ? " is-selected" : ""}" data-mc-mini-day="${toIsoDay(date)}">${date.getDate()}</button>
      `);
    }

    return `
      <aside class="mc-sidebar">
        <div class="mc-sidebar-scroll">
          ${calendarSections.map((section) => `
            <section class="mc-sidebar-section">
              <h2 class="mc-section-title">${section.title}</h2>
              <ul class="mc-cal-list">
                ${section.items.map((item) => `
                  <li>
                    <label class="mc-cal-item">
                      <input type="checkbox" data-mc-cal="${escapeHtml(item.id)}" ${calState.selectedCalendars[item.id] ? "checked" : ""} />
                      <span class="mc-cal-dot" style="--dot-color:${item.color};"></span>
                      <span class="mc-cal-name">${escapeHtml(item.label)}</span>
                    </label>
                  </li>
                `).join("")}
              </ul>
            </section>
          `).join("")}

          <section class="mc-sidebar-section">
            <button type="button" class="btn mc-sidebar-create-btn" data-mc-nav="create-booking-page">Create Booking</button>
            <button type="button" class="btn mc-sidebar-create-btn" data-mc-nav="create">Create Event</button>
          </section>

          <section class="mc-mini-month">
            <div class="mc-mini-head">
              <button type="button" class="btn mc-mini-nav-btn" data-mc-mini-nav="prev" aria-label="Previous month">&lsaquo;</button>
              <div class="mc-mini-title">${escapeHtml(miniMonthTitle)}</div>
              <button type="button" class="btn mc-mini-nav-btn" data-mc-mini-nav="next" aria-label="Next month">&rsaquo;</button>
            </div>
            <div class="mc-mini-weekdays">
              ${dayNames.map((name) => `<span>${name.charAt(0)}</span>`).join("")}
            </div>
            <div class="mc-mini-grid">${miniCells.join("")}</div>
          </section>
        </div>
      </aside>
    `;
  }

  function MainContent() {
    if (calState.currentView === "day") return DayView();
    if (calState.currentView === "week") return WeekView();
    if (calState.currentView === "year") return YearView();
    return MonthGrid();
  }

  function shiftCurrentPeriod(direction) {
    const unit = calState.currentView === "day"
      ? "day"
      : calState.currentView === "week"
        ? "week"
        : calState.currentView === "year"
          ? "year"
          : "month";
    calState.currentDate = shiftDate(calState.currentDate, direction, unit);
    calState.selectedDate = shiftDate(calState.selectedDate, direction, unit);
    alignCurrentDateToView();
    ensureHolidayYearsForDate(calState.selectedDate);
  }

  function shiftCurrentPeriodWithFx(direction) {
    const dir = Number(direction) >= 0 ? 1 : -1;
    if (calState.currentView === "month") calState.monthTransitionDir = dir;
    shiftCurrentPeriod(dir);
    renderCalendarLayout();
  }

  function renderCalendarLayout() {
    syncScheduleTopOffset();
    wrap.innerHTML = `
      <div class="mc-shell">
        ${Sidebar()}
        <section class="mc-main">
          ${Toolbar()}
          <div class="mc-body">
            ${MainContent()}
          </div>
        </section>
      </div>
      ${AddEventModal()}
      ${ImportHelpModal()}
      ${EventDetailDrawer()}
    `;
  }

  wrap.addEventListener("click", (event) => {
    const deleteBtn = event.target.closest("[data-mc-delete-event]");
    if (deleteBtn) {
      event.preventDefault();
      event.stopPropagation();
      const eventId = String(deleteBtn.getAttribute("data-mc-delete-event") || "").trim();
      if (eventId) {
        const existingEvent = calState.events.find((ev) => String(ev?.id || "") === eventId);
        const sourceChanged = removeBookedEventFromSources(existingEvent);
        if (!sourceChanged) {
          hiddenEventIds.add(eventId);
          persistHiddenEvents();
        }
        calState.events = calState.events.filter((ev) => String(ev?.id || "") !== eventId);
        removeStoredEventById(eventId);
        if (sourceChanged) {
          resetScheduleDerivedCaches();
          const threadId = String(existingEvent?.meta?.bookingId || existingEvent?.meta?.threadId || "").trim();
          if (threadId && !/^sim-/i.test(threadId)) {
            apiPost("/api/conversations/status", { convoId: threadId, status: "contacted" }).catch(() => {});
          }
        }
        calState.drawerOpen = false;
        calState.drawerEventId = "";
      }
      renderCalendarLayout();
      return;
    }

    const rawTarget = event.target;
    if (rawTarget instanceof HTMLElement && rawTarget.getAttribute("data-mc-modal-overlay") === "true") {
      calState.showAddEventModal = false;
      renderCalendarLayout();
      return;
    }
    if (rawTarget instanceof HTMLElement && rawTarget.getAttribute("data-mc-import-overlay") === "true") {
      calState.showImportHelpModal = false;
      renderCalendarLayout();
      return;
    }
    if (rawTarget instanceof HTMLElement && rawTarget.getAttribute("data-mc-close-drawer") === "true") {
      calState.drawerOpen = false;
      calState.drawerEventId = "";
      renderCalendarLayout();
      return;
    }

    const target = event.target.closest("button");
    if (!target) return;

    const openEventId = String(target.getAttribute("data-mc-open-event") || "").trim();
    if (openEventId) {
      calState.drawerOpen = true;
      calState.drawerEventId = openEventId;
      renderCalendarLayout();
      return;
    }
    if (target.getAttribute("data-mc-close-drawer") === "true") {
      calState.drawerOpen = false;
      calState.drawerEventId = "";
      renderCalendarLayout();
      return;
    }

    if (target.getAttribute("data-mc-modal-close") === "true") {
      calState.showAddEventModal = false;
      renderCalendarLayout();
      return;
    }
    if (target.getAttribute("data-mc-import-close") === "true") {
      calState.showImportHelpModal = false;
      renderCalendarLayout();
      return;
    }
    if (target.getAttribute("data-mc-open-calendar-settings") === "true") {
      openCalendarSettings();
      return;
    }

    const view = target.getAttribute("data-mc-view");
    if (view) {
      calState.currentView = view;
      if (view !== "month") calState.monthTransitionDir = 0;
      calState.drawerOpen = false;
      calState.drawerEventId = "";
      alignCurrentDateToView();
      ensureHolidayYearsForDate(calState.selectedDate);
      renderCalendarLayout();
      return;
    }

    const navAction = target.getAttribute("data-mc-nav");
    if (navAction === "today") {
      calState.monthTransitionDir = 0;
      calState.selectedDate = new Date(today);
      alignCurrentDateToView();
      ensureHolidayYearsForDate(calState.selectedDate);
      renderCalendarLayout();
      return;
    }
    if (navAction === "create") {
      calState.showAddEventModal = true;
      renderCalendarLayout();
      return;
    }
    if (navAction === "create-booking-page") {
      state.view = "schedule-booking";
      render();
      return;
    }
    if (navAction === "import-help") {
      calState.showImportHelpModal = true;
      renderCalendarLayout();
      return;
    }
    if (navAction === "prev") {
      shiftCurrentPeriodWithFx(-1);
      return;
    }
    if (navAction === "next") {
      shiftCurrentPeriodWithFx(1);
      return;
    }

    const dayIso = target.getAttribute("data-mc-day");
    if (dayIso) {
      calState.monthTransitionDir = 0;
      const next = new Date(`${dayIso}T00:00:00`);
      calState.selectedDate = next;
      calState.currentView = "day";
      calState.drawerOpen = false;
      calState.drawerEventId = "";
      alignCurrentDateToView();
      ensureHolidayYearsForDate(next);
      renderCalendarLayout();
      return;
    }

    const miniIso = target.getAttribute("data-mc-mini-day");
    if (miniIso) {
      calState.monthTransitionDir = 0;
      const next = new Date(`${miniIso}T00:00:00`);
      calState.selectedDate = next;
      calState.currentView = "day";
      calState.drawerOpen = false;
      calState.drawerEventId = "";
      alignCurrentDateToView();
      ensureHolidayYearsForDate(next);
      renderCalendarLayout();
      return;
    }

    const miniNav = target.getAttribute("data-mc-mini-nav");
    if (miniNav === "prev") {
      if (calState.currentView === "month") calState.monthTransitionDir = -1;
      calState.selectedDate = shiftDate(calState.selectedDate, -1, "month");
      alignCurrentDateToView();
      ensureHolidayYearsForDate(calState.selectedDate);
      renderCalendarLayout();
      return;
    }
    if (miniNav === "next") {
      if (calState.currentView === "month") calState.monthTransitionDir = 1;
      calState.selectedDate = shiftDate(calState.selectedDate, 1, "month");
      alignCurrentDateToView();
      ensureHolidayYearsForDate(calState.selectedDate);
      renderCalendarLayout();
    }
  });

  wrap.addEventListener("wheel", (event) => {
    if (calState.currentView !== "month") return;
    const target = event.target;
    if (!(target instanceof HTMLElement)) return;
    if (!target.closest(".mc-month-view")) return;

    event.preventDefault();

    const now = Date.now();
    if (now < Number(calState.wheelLockUntil || 0)) return;

    calState.wheelDeltaY += Number(event.deltaY || 0);
    if (Math.abs(calState.wheelDeltaY) < 46) return;

    const dir = calState.wheelDeltaY > 0 ? 1 : -1;
    calState.wheelDeltaY = 0;
    calState.wheelLockUntil = now + 240;
    shiftCurrentPeriodWithFx(dir);
  }, { passive: false });

  wrap.addEventListener("change", (event) => {
    const target = event.target;
    if (target instanceof HTMLSelectElement && target.name === "eventType") {
      const form = target.closest("form");
      if (form instanceof HTMLFormElement && form.id === "mcAddEventForm") {
        syncAddEventTypeUI(form, target.value);
      }
      return;
    }
    if (!(target instanceof HTMLInputElement)) return;
    const calendarKey = target.getAttribute("data-mc-cal");
    if (!calendarKey) return;
    calState.selectedCalendars[calendarKey] = target.checked;
    renderCalendarLayout();
  });

  wrap.addEventListener("submit", (event) => {
    const form = event.target;
    if (!(form instanceof HTMLFormElement) || form.id !== "mcAddEventForm") return;
    event.preventDefault();
    const fd = new FormData(form);
    const title = String(fd.get("title") || "").trim();
    const cfg = getAddEventTypeConfig(String(fd.get("eventType") || "appointment"));
    const date = String(fd.get("date") || "").trim();
    const fallbackCalendar = cfg.calendar;
    const rawCalendar = String(fd.get("calendar") || "").trim();
    const calendar = calendarColorById[rawCalendar] ? rawCalendar : fallbackCalendar;
    const startTime = cfg.allDay ? "00:00" : String(fd.get("startTime") || "09:00");
    const endTime = cfg.allDay ? "23:59" : String(fd.get("endTime") || "10:00");
    const billFor = String(fd.get("billFor") || "").trim();
    const billAmountRaw = String(fd.get("billAmount") || "").trim();
    const billAmountParsed = billAmountRaw === "" ? null : Number(billAmountRaw);
    const billAmount = Number.isFinite(billAmountParsed) ? billAmountParsed : null;
    const birthdayPerson = String(fd.get("birthdayPerson") || "").trim();
    const reminderNotes = String(fd.get("reminderNotes") || "").trim();
    const vacationStartDate = String(fd.get("vacationStartDate") || "").trim();
    const vacationEndDate = String(fd.get("vacationEndDate") || "").trim();
    if (!title || (cfg.type !== "vacation" && !date)) return;

    const effectiveDate = cfg.type === "vacation" ? (vacationStartDate || "") : date;
    if (!effectiveDate) return;
    const start = new Date(`${effectiveDate}T${startTime || "09:00"}:00`);
    let end = new Date(`${effectiveDate}T${endTime || "10:00"}:00`);
    if (Number.isNaN(end.getTime()) || end <= start) {
      end = cfg.allDay ? shiftDate(startOfDay(start), 1, "day") : new Date(start.getTime() + (60 * 60 * 1000));
    }
    if (cfg.type === "vacation") {
      const rangeEnd = new Date(`${vacationEndDate || effectiveDate}T00:00:00`);
      if (!Number.isNaN(rangeEnd.getTime())) {
        const normalizedStart = startOfDay(start);
        const normalizedEnd = startOfDay(rangeEnd);
        end = shiftDate(normalizedEnd < normalizedStart ? normalizedStart : normalizedEnd, 1, "day");
      }
    }
    const eventMeta = { eventType: cfg.type };
    if (cfg.type === "bill") {
      if (billFor) eventMeta.billFor = billFor;
      if (billAmount != null) eventMeta.amount = billAmount;
      eventMeta.kind = "bill_due";
    } else if (cfg.type === "birthday") {
      if (birthdayPerson) eventMeta.person = birthdayPerson;
      eventMeta.kind = "birthday";
    } else if (cfg.type === "reminder") {
      if (reminderNotes) eventMeta.notes = reminderNotes;
      eventMeta.kind = "reminder";
    } else if (cfg.type === "holiday") {
      eventMeta.kind = "holiday";
    } else if (cfg.type === "vacation") {
      eventMeta.kind = "vacation";
      eventMeta.startDate = vacationStartDate || effectiveDate;
      eventMeta.endDate = vacationEndDate || vacationStartDate || effectiveDate;
    }

    const createdEvent = {
      id: `manual-${Date.now()}-${Math.floor(Math.random() * 1000)}`,
      title,
      start,
      end,
      allDay: cfg.allDay,
      color: calendarColorById[calendar] || "#5fa8ff",
      calendar,
      meta: eventMeta,
    };
    calState.events.push(createdEvent);
    upsertStoredEvent(customEventsKey, createdEvent);
    calState.selectedDate = startOfDay(start);
    alignCurrentDateToView();
    ensureHolidayYearsForDate(calState.selectedDate);
    calState.showAddEventModal = false;
    renderCalendarLayout();
  });

  renderCalendarLayout();

  return wrap;
}

/* Settings (simple MVP settings screen) */
function viewSettings(){
  const wrap = document.createElement("div");
  wrap.className = "col app-view app-view-settings";

  const card = document.createElement("div");
  card.className = "card settings-shell-card";

  card.innerHTML = `
    <div class="settings-sidebar-head">
      <div class="h1" style="margin:0;">Settings</div>
      <div class="p">General and account controls</div>
    </div>
    <div id="settingsSubnav" class="settings-subnav" role="tablist" aria-label="Settings sections">
      <button class="btn settings-tab" type="button" role="tab" data-settings-tab="profile">Profile</button>
      <button class="btn settings-tab" type="button" role="tab" data-settings-tab="automations">Automations</button>
      <button class="btn settings-tab" type="button" role="tab" data-settings-tab="customer-billing">Customer Billing</button>
      <button class="btn settings-tab" type="button" role="tab" data-settings-tab="workspace">Pricing</button>
      <button class="btn settings-tab" type="button" role="tab" data-settings-tab="schedule">Schedule</button>
      <button class="btn settings-tab" type="button" role="tab" data-settings-tab="email">Email</button>
      <button class="btn settings-tab" type="button" role="tab" data-settings-tab="call-routing">Call Routing</button>
      <button class="btn settings-tab" type="button" role="tab" data-settings-tab="admin">Admin</button>
      <button class="btn settings-tab" type="button" role="tab" data-settings-tab="billing">Subscription</button>
      <button class="btn settings-tab" type="button" role="tab" data-settings-tab="developer">Developer</button>
    </div>

    <div class="settings-main-head">
      <div id="settingsBreadcrumb" class="p shell-breadcrumb">Settings / General</div>
      <div id="settingsToast" class="hidden settings-toast"></div>
    </div>
    <div id="adminPasscodeModal" class="billing-modal-overlay hidden" aria-hidden="true">
      <div class="billing-modal billing-modal-sm" role="dialog" aria-modal="true" aria-labelledby="adminPasscodeTitle">
        <div class="h1" id="adminPasscodeTitle" style="margin:0;">Admin passcode required</div>
        <div class="p" id="adminPasscodeSubtitle" style="margin-top:6px;">Enter admin passcode to unlock protected actions for this account.</div>
        <div style="height:10px;"></div>
        <div class="col" id="adminCurrentPasscodeWrap" style="display:none;">
          <label class="p">Current passcode</label>
          <input class="input" id="adminCurrentPasscodeInput" type="password" inputmode="numeric" maxlength="12" placeholder="Current 4-12 digit passcode" />
        </div>
        <div style="height:8px;"></div>
        <div class="col">
          <label class="p" id="adminPasscodeInputLabel">Passcode</label>
          <input class="input" id="adminPasscodeInput" type="password" inputmode="numeric" maxlength="12" placeholder="Enter 4-12 digit passcode" />
        </div>
        <div style="height:8px;"></div>
        <div class="col" id="adminConfirmPasscodeWrap" style="display:none;">
          <label class="p">Confirm new passcode</label>
          <input class="input" id="adminConfirmPasscodeInput" type="password" inputmode="numeric" maxlength="12" placeholder="Re-enter passcode" />
        </div>
        <div class="p" id="adminPasscodeStatus" style="min-height:18px; margin-top:8px;"></div>
        <div class="row" style="justify-content:flex-end; gap:8px; margin-top:10px;">
          <button class="btn" id="adminPasscodeCancelBtn" type="button">Cancel</button>
          <button class="btn primary" id="adminPasscodeSubmitBtn" type="button">Unlock</button>
        </div>
      </div>
    </div>
    <div id="settingsLockedGate" class="card" style="display:none;">
      <div class="h1" style="margin:0;">Settings Locked</div>
      <div class="p" style="margin-top:8px;">Admin passcode is required to access Settings.</div>
      <div style="height:10px;"></div>
      <div class="row" style="justify-content:flex-end;">
        <button class="btn primary" id="settingsLockedGateUnlockBtn" type="button">Unlock Settings</button>
      </div>
    </div>

    <section data-settings-panel="profile" class="settings-panel" style="display:none;">
      <div class="card">
        <div class="h1" style="margin:0;">My Profile</div>
        <div class="p">Manage your personal account profile and preferences.</div>
        <div style="height:10px;"></div>
        <div class="grid2">
          <div class="col">
            <label class="p">Display name</label>
            <input class="input" id="profileDisplayNameInput" placeholder="Your name" />
          </div>
          <div class="col">
            <label class="p">Email</label>
            <input class="input" id="profileEmailInput" readonly />
          </div>
        </div>
        <div style="height:10px;"></div>
        <div class="grid2">
          <div class="col">
            <label class="p">Role</label>
            <input class="input" id="profileRoleInput" readonly />
          </div>
        </div>
        <div style="height:14px;"></div>
        <div class="h1" style="margin:0;">Business Identity</div>
        <div class="p">Business profile and branding used across messaging and automations.</div>
        <div style="height:10px;"></div>
        <div class="grid2">
          <div class="col">
            <label class="p">Business name</label>
            <input class="input" id="businessNameInput" placeholder="e.g. Mike's Detailing" />
          </div>
          <div class="col">
            <label class="p">Industry</label>
            <input class="input" id="workspaceIndustryInput" placeholder="e.g. Auto detailing" />
          </div>
        </div>
        <div style="height:10px;"></div>
        <div class="col">
          <label class="p">Logo URL</label>
          <input class="input" id="workspaceLogoInput" placeholder="https://cdn.example.com/logo.png" />
          <div style="height:8px;"></div>
          <div class="row" style="gap:8px; flex-wrap:wrap;">
            <input type="file" id="workspaceLogoFileInput" accept="image/png,image/jpeg,image/webp,image/gif" class="hidden" />
            <button class="btn" id="workspaceLogoUploadBtn" type="button">Upload Logo</button>
            <button class="btn" id="workspaceLogoRemoveBtn" type="button">Remove Logo</button>
            <span class="p" id="workspaceLogoUploadStatus" style="min-height:18px;"></span>
          </div>
          <img id="workspaceLogoPreview" alt="Business logo preview" style="display:none; width:auto; height:auto; max-width:220px; max-height:64px; object-fit:contain; margin-top:8px; border:1px solid var(--border); border-radius:8px; padding:6px; background:rgba(255,255,255,0.03);" />
        </div>
        <div style="height:10px;"></div>
        <div class="row" style="justify-content:flex-end; align-items:center; gap:10px;">
          <div class="p" id="saveBusinessNameStatus" style="min-height:18px; margin-right:auto;"></div>
          <button class="btn primary" id="saveBusinessNameBtn" type="button">Save identity</button>
        </div>
      </div>

      <div style="height:14px;"></div>

      <div class="card">
        <div class="h1" style="margin:0;">Business Hours</div>
        <div class="p">Weekly availability used for automation timing and response policies.</div>
        <div style="height:10px;"></div>
        <div id="workspaceHoursGrid" class="col" style="gap:8px;"></div>
        <div style="height:10px;"></div>
        <div class="row" style="justify-content:space-between; gap:8px; flex-wrap:wrap;">
          <button class="btn" id="workspaceCopyHoursBtn" type="button">Copy Monday to all weekdays</button>
          <button class="btn primary" id="workspaceSaveHoursBtn" type="button">Save hours</button>
        </div>
      </div>

      <div style="height:14px;"></div>

      <div class="card" id="workspacePasswordResetCard">
        <div class="h1" style="margin:0;">User Password Reset</div>
        <div class="p">Owners and admins can reset passwords for users in this workspace.</div>
        <div style="height:10px;"></div>
        <div class="grid2">
          <div class="col">
            <label class="p">User</label>
            <select class="select" id="workspaceResetUserSelect"></select>
          </div>
          <div class="col">
            <label class="p">Code</label>
            <input class="input" id="workspaceResetCodeInput" type="text" placeholder="Enter code from email" />
          </div>
        </div>
        <div style="height:10px;"></div>
        <div class="col" id="workspaceResetNewPasswordRow" style="display:none;">
          <label class="p">New password</label>
          <input class="input" id="workspaceResetUserPasswordInput" type="password" placeholder="Enter strong password" />
          <div class="p">Password must be 10+ characters and include uppercase, lowercase, number, and symbol.</div>
        </div>
        <div style="height:10px;"></div>
        <div class="row" style="justify-content:flex-end; gap:8px;">
          <div class="p" id="workspaceResetUserStatus" style="min-height:18px; margin-right:auto;"></div>
          <button class="btn" id="workspaceSendResetCodeBtn" type="button">Send code to email</button>
          <button class="btn" id="workspaceVerifyResetCodeBtn" type="button">Verify code</button>
          <button class="btn primary" id="workspaceResetUserBtn" type="button" style="display:none;">Reset password</button>
        </div>
        <div class="p" style="margin-top:8px;">TIP: All sessions will be logged out upon password change.</div>
      </div>

      <div style="height:14px;"></div>

      <div class="card">
        <div class="h1" style="margin:0;">Preferences</div>
        <div class="p">Set defaults for your own dashboard experience.</div>
        <div style="height:10px;"></div>
        <div class="grid2">
          <div class="col">
            <label class="p">Default landing page</label>
            <select class="select" id="profileDefaultViewSelect">
              <option value="home">Home</option>
              <option value="messages">Messages</option>
              <option value="contacts">Contacts</option>
              <option value="analytics">Analytics</option>
              <option value="schedule">Schedule</option>
              <option value="settings">Settings</option>
            </select>
          </div>
          <div class="col">
            <label class="p">Keyboard hint style</label>
            <select class="select" id="profileShortcutHintSelect">
              <option value="auto">Auto</option>
              <option value="mac">Mac</option>
              <option value="windows">Windows</option>
            </select>
          </div>
        </div>
        <div style="height:10px;"></div>
        <div class="row" style="justify-content:flex-end; gap:8px;">
          <div class="p" id="profileSaveStatus" style="min-height:18px; margin-right:auto;"></div>
          <button class="btn primary" id="profileSaveBtn" type="button">Save profile</button>
        </div>
      </div>

      <div style="height:14px;"></div>

      <div class="card">
        <div class="h1" style="margin:0;">Timezone</div>
        <div class="p">Set your workspace timezone for scheduling, follow-ups, and business hours.</div>
        <div style="height:10px;"></div>
        <div class="grid2">
          <div class="col">
            <label class="p">Timezone</label>
            <select class="select" id="workspaceTimezoneSelect"></select>
          </div>
          <div class="col">
            <label class="p">Local time preview</label>
            <div class="badge" id="workspaceLocalTimePreview">?</div>
          </div>
        </div>
        <div style="height:10px;"></div>
        <div class="row" style="justify-content:flex-end;">
          <button class="btn primary" id="workspaceSaveTimezoneBtn" type="button">Save timezone</button>
        </div>
      </div>

      <div style="height:14px;"></div>

      <div class="card">
        <div class="row" style="justify-content:space-between; align-items:center;">
          <div class="col" style="gap:4px;">
            <div class="h1" style="margin:0;">Theme</div>
            <div class="p">Toggle dark mode (saved on this device).</div>
          </div>
          <label class="toggle" style="gap:10px;">
            <span class="p" style="margin:0;">Dark mode</span>
            <input type="checkbox" id="themeToggle" />
          </label>
        </div>
      </div>

      <div style="height:14px;"></div>

      <div class="card">
        <div class="row" style="justify-content:space-between; align-items:flex-start; flex-wrap:wrap;">
          <div class="col" style="gap:4px;">
            <div class="h1" style="margin:0;">Notification Settings</div>
            <div class="p">Configure where alerts are sent and what events trigger them.</div>
          </div>
          <span class="badge">Alerts</span>
        </div>

        <div style="height:14px;"></div>

        <div class="row" style="justify-content:space-between; align-items:flex-start; flex-wrap:wrap; gap:18px;">
          <div class="col" style="gap:10px; min-width:240px;">
            <div class="p" style="font-weight:600;">Channels</div>
            <label class="toggle"><input type="checkbox" id="notifChannelEmail" /> Email alerts</label>
            <label class="toggle"><input type="checkbox" id="notifChannelSms" /> SMS alerts</label>
            <label class="toggle"><input type="checkbox" id="notifChannelDesktop" /> Desktop notifications</label>
          </div>

          <div class="col" style="gap:10px; min-width:240px;">
            <div class="p" style="font-weight:600;">Triggers</div>
            <label class="toggle"><input type="checkbox" id="notifTriggerVipMessage" /> VIP message received</label>
            <label class="toggle"><input type="checkbox" id="notifTriggerMissedCall" /> Missed call</label>
            <label class="toggle"><input type="checkbox" id="notifTriggerNewBooking" /> New booking</label>
          </div>
        </div>

        <div style="height:12px;"></div>
        <details class="card" style="background:var(--panel);">
          <summary class="p" style="cursor:pointer; font-weight:600;">Quiet hours + dedupe</summary>
          <div style="height:10px;"></div>
          <label class="toggle"><input type="checkbox" id="notifQuietHoursEnabled" /> Enable quiet hours (<span id="notifQuietHoursTz">America/New_York</span>)</label>
          <div class="row" style="gap:10px; flex-wrap:wrap;">
            <div class="col">
              <label class="p">Start</label>
              <input class="input" type="time" id="notifQuietHoursStart" value="21:00" />
            </div>
            <div class="col">
              <label class="p">End</label>
              <input class="input" type="time" id="notifQuietHoursEnd" value="08:00" />
            </div>
            <div class="col">
              <label class="p">Dedupe window</label>
              <select class="select" id="notifDedupeMinutes">
                <option value="5">5 minutes</option>
                <option value="10">10 minutes</option>
                <option value="15">15 minutes</option>
                <option value="30">30 minutes</option>
              </select>
            </div>
          </div>
        </details>

        <div style="height:14px;"></div>

        <div class="row" style="justify-content:flex-end; align-items:center; gap:10px;">
          <div class="p" id="notifSettingsStatus" style="min-height:18px; margin-right:auto;"></div>
          <button class="btn primary" id="saveNotifSettingsBtn" type="button">Save notifications</button>
        </div>
      </div>

    </section>

    <section data-settings-panel="workspace" class="settings-panel">

        <div class="row" style="justify-content:space-between; align-items:flex-start; flex-wrap:wrap;">
        <div class="col" style="gap:6px;">
          <div class="h1" style="margin:0;">Pricing</div>
          <p class="p">Signed in as: <b id="signedInEmail">?</b></p>
        </div>
        <button class="btn" id="logoutSettingsBtn" type="button">Log out</button>
      </div>

      <div style="height:14px;"></div>

      <div class="card" style="display:none;">
        <div class="h1" style="margin:0;">Defaults</div>
        <div class="p">Workspace default configuration for new leads and automations.</div>
        <div style="height:10px;"></div>
        <div class="col">
          <label class="p">Default automation flow</label>
          <select class="select" id="workspaceDefaultFlowSelect">
            <option value="">No default flow</option>
          </select>
        </div>
        <div style="height:10px;"></div>
        <div class="row" style="justify-content:flex-end;">
          <button class="btn primary" id="workspaceSaveDefaultsBtn" type="button">Save defaults</button>
        </div>
      </div>

      <div style="height:14px;"></div>

      <div class="card pricing-card">
        <div class="pricing-card-head">
          <div>
            <div class="h1" style="margin:0;">Pricing</div>
            <div class="p muted">Craft quote bands that match your auto-detail offerings.</div>
          </div>
          <div class="pricing-profile-select">
            <label class="p" style="margin:0 0 6px;">Pricing profile flow</label>
            <select class="select" id="workspacePricingFlowSelect"></select>
          </div>
        </div>
        <div class="pricing-card-body">
          <section class="pricing-section" data-pricing-section="services">
            <div class="pricing-section-head">
              <div>
                <div class="pricing-section-title">Services</div>
                <p class="p muted" style="margin:0;">Editable rows control the service name, price range, and estimated hours.</p>
              </div>
              <button class="btn pricing-section-action" type="button" data-workspace-add-service>+ Add service</button>
            </div>
            <div class="pricing-section-body" id="workspacePricingRows"></div>
          </section>
          <section class="pricing-section" data-pricing-section="paint-scopes">
            <div class="pricing-section-head">
              <div>
                <div class="pricing-section-title">Paint correction scopes</div>
                <p class="p muted" style="margin:0;">Scope detail for paint-specific jobs.</p>
              </div>
            </div>
            <div class="pricing-section-body" id="workspacePaintScopeRows"></div>
          </section>
          <section class="pricing-section" data-pricing-section="service-scopes">
            <div class="pricing-section-head">
              <div>
                <div class="pricing-section-title">Service scopes</div>
                <p class="p muted" style="margin:0;">Drill into add-on scopes associated with each service.</p>
              </div>
            </div>
            <div class="pricing-section-body" id="workspaceServiceScopeRows"></div>
          </section>
        </div>
        <div class="pricing-card-footer">
          <div class="p" id="workspacePricingStatus" style="min-height:18px;"></div>
          <button class="btn primary" id="workspaceSavePricingBtn" type="button">Save pricing</button>
        </div>
      </div>

      <div style="height:14px;"></div>

    </section>

    <section data-settings-panel="messaging" class="settings-panel" style="display:none;">
      <div class="card">
        <div class="row" style="justify-content:space-between; align-items:flex-start; flex-wrap:wrap;">
          <div class="col" style="gap:4px;">
            <div class="h1" style="margin:0;">Compliance</div>
            <div class="p">Per-tenant SMS compliance controls for opt-out, consent, and retention.</div>
          </div>
          <span class="badge">Legal</span>
        </div>

        <div style="height:14px;"></div>

        <div class="grid2">
          <div class="card" style="background:var(--panel);">
            <div class="h1" style="margin:0;">STOP behavior</div>
            <label class="toggle"><input type="checkbox" id="cmpStopEnabled" /> Enable STOP keyword handling</label>
            <label class="toggle"><input type="checkbox" id="cmpStopAutoReply" /> Auto-reply on opt-out</label>
            <div class="col">
              <label class="p">STOP keywords (comma-separated)</label>
              <input class="input" id="cmpStopKeywords" placeholder="STOP,UNSUBSCRIBE,CANCEL,END,QUIT" />
            </div>
            <div class="col">
              <label class="p">HELP keywords (comma-separated)</label>
              <input class="input" id="cmpHelpKeywords" placeholder="HELP,INFO" />
            </div>
            <div class="col">
              <label class="p">Auto-reply text</label>
              <input class="input" id="cmpStopAutoReplyText" />
            </div>
          </div>

          <div class="card" style="background:var(--panel);">
            <div class="h1" style="margin:0;">Opt-out enforcement</div>
            <label class="toggle"><input type="checkbox" id="cmpOptOutEnforce" /> Block outbound to opted-out contacts</label>
            <label class="toggle"><input type="checkbox" id="cmpAllowTransactional" /> Allow transactional messages to opted-out contacts</label>
            <div class="col">
              <label class="p">DNR tag</label>
              <input class="input" id="cmpStoreAsTag" placeholder="DNR" />
            </div>
            <div class="col">
              <label class="p">Resubscribe keywords (comma-separated)</label>
              <input class="input" id="cmpResubKeywords" placeholder="START,UNSTOP,YES" />
            </div>
          </div>
        </div>

        <div style="height:12px;"></div>

        <div class="grid2">
          <div class="card" style="background:var(--panel);">
            <div class="h1" style="margin:0;">Consent</div>
            <label class="toggle"><input type="checkbox" id="cmpConsentRequired" /> Require consent for outbound messages</label>
            <div class="col">
              <label class="p">Compose checkbox text</label>
              <input class="input" id="cmpConsentCheckboxText" />
            </div>
            <div class="col">
              <label class="p">Consent source options (comma-separated)</label>
              <input class="input" id="cmpConsentSourceOptions" placeholder="verbal,form,existing_customer,other" />
            </div>
          </div>

          <div class="card" style="background:var(--panel);">
            <div class="h1" style="margin:0;">Retention</div>
            <label class="toggle"><input type="checkbox" id="cmpRetentionEnabled" /> Enable message log retention</label>
            <label class="toggle"><input type="checkbox" id="cmpRetentionSchedule" /> Purge on schedule</label>
            <div class="col">
              <label class="p">Message log days (7-365)</label>
              <input class="input" id="cmpRetentionDays" type="number" min="7" max="365" />
            </div>
            <div class="row" style="justify-content:flex-end;">
              <button class="btn" id="cmpRunPurgeNowBtn" type="button">Run purge now</button>
            </div>
          </div>
        </div>

        <div style="height:12px;"></div>

        <div class="card" style="background:var(--panel);">
          <div class="h1" style="margin:0;">Compliance status</div>
          <div class="p" id="cmpStatusCard">Loading...</div>
        </div>

        <div style="height:12px;"></div>
        <div class="row" style="justify-content:flex-end; align-items:center; gap:10px;">
          <div class="p" id="cmpSaveStatus" style="min-height:18px; margin-right:auto;"></div>
          <button class="btn primary" id="cmpSaveBtn" type="button">Save compliance settings</button>
        </div>
      </div>
    </section>

    <section data-settings-panel="automations" class="settings-panel" style="display:none;">
      <div id="settingsAutomationsLegacyMount"></div>
      <div style="height:12px;"></div>
      <div class="card">
        <div class="h1" style="margin:0;">Automation Settings</div>
        <div class="p">Workspace-level automation controls.</div>
        <div style="height:10px;"></div>
        <label class="toggle"><input type="checkbox" id="workspaceAutomationsEnabled" checked /> Enable automations for this workspace</label>
        <label class="toggle"><input type="checkbox" id="workspaceAutomationsQuietHours" /> Quiet hours mode (uses workspace timezone: <span id="workspaceAutomationsQuietHoursTz">America/New_York</span>)</label>
        <label class="toggle"><input type="checkbox" id="workspaceAutomationsSafeOptOut" checked /> Never auto-reply to DNR / opted-out / STOP contacts</label>
        <div class="col" style="margin-top:8px;">
          <label class="p">Default automation profile</label>
          <select class="select" id="workspaceAutomationsProfile">
            <option value="balanced">Balanced</option>
            <option value="aggressive">Aggressive</option>
            <option value="conservative">Conservative</option>
            <option value="custom">Custom</option>
          </select>
        </div>
        <div style="height:10px;"></div>
        <div class="row" style="justify-content:flex-end; align-items:center; gap:10px;">
          <div class="p" id="workspaceAutomationsStatus" style="min-height:18px; margin-right:auto;"></div>
          <button class="btn primary" id="workspaceAutomationsSaveBtn" type="button">Save automation settings</button>
        </div>
      </div>
    </section>

    <section data-settings-panel="schedule" class="settings-panel" style="display:none;">
      <div class="card">
        <div class="row" style="justify-content:space-between; align-items:flex-start; flex-wrap:wrap;">
          <div class="col" style="gap:6px; max-width:560px;">
            <div class="h1" style="margin:0;">Scheduling</div>
            <div class="p">Relay booking link is enabled by default. Add an external URL only if you want link-mode.</div>
            <div class="grid2">
              <label class="col" style="gap:4px;">
                <span class="p">Slot interval (min)</span>
                <input class="input" id="slotIntervalMin" type="number" min="10" step="5" value="30" />
              </label>
              <label class="col" style="gap:4px;">
                <span class="p">Lead time (min)</span>
                <input class="input" id="leadTimeMin" type="number" min="0" step="5" value="60" />
              </label>
            </div>
            <div class="grid2">
              <label class="col" style="gap:4px;">
                <span class="p">Buffer between bookings (min)</span>
                <input class="input" id="bufferMin" type="number" min="0" step="5" value="0" />
              </label>
              <label class="col" style="gap:4px;">
                <span class="p">Max bookings per day (0 = unlimited)</span>
                <input class="input" id="maxBookingsPerDay" type="number" min="0" step="1" value="0" />
              </label>
            </div>
            <div class="p">Service duration used for availability comes from each service's hoursMin in Pricing editor.</div>
            <div class="p">Custom external booking URL (optional)</div>
            <input class="input" id="calendlyUrl" placeholder="https://BOOKING-LINK.com (optional)" />
            <div class="row" style="justify-content:flex-end;">
              <button class="btn primary" id="saveCalendlyBtn" type="button">Save scheduling</button>
            </div>
            <div class="p" id="saveCalendlyStatus" style="min-height:18px;"></div>
          </div>
          <div class="col" style="gap:6px; min-width:320px; flex:1;">
            <div class="p">Generated booking link</div>
            <a class="badge" id="generatedBookingUrl" href="#" target="_blank" rel="noopener" style="display:inline-block; max-width:100%; overflow:hidden; text-overflow:ellipsis; white-space:nowrap;">Generating...</a>
            <div class="p">Live booking page preview</div>
            <iframe id="bookingUrlPreviewFrame" title="Booking page preview" style="width:100%; min-height:380px; border:1px solid var(--line); border-radius:12px; background:var(--panel);"></iframe>
            <a class="p" id="bookingUrlPreviewOpen" href="#" target="_blank" rel="noopener" style="display:none;">Open booking page in new tab</a>
          </div>
        </div>
      </div>

      <div style="height:14px;"></div>

      <div class="card">
        <div class="h1" style="margin:0;">Calendar Import</div>
        <div class="p">Bring existing calendars into Relay and control scheduling sync behavior.</div>
        <div style="height:10px;"></div>
        <div class="grid2">
          <div class="card" style="background:var(--panel);">
            <div class="h1" style="margin:0;">Import Methods</div>
            <div style="height:8px;"></div>
            <ul class="p" style="margin:0; padding-left:18px; display:grid; gap:6px;">
              <li>Upload an <b>.ics</b> calendar export file</li>
              <li>Subscribe with an <b>ICS link</b> for ongoing sync</li>
              <li>Connect <b>Google</b> or <b>Outlook</b> when available</li>
            </ul>
          </div>
          <div class="card" style="background:var(--panel);">
            <div class="h1" style="margin:0;">Quick Actions</div>
            <div style="height:8px;"></div>
            <div class="col" style="gap:8px;">
              <button class="btn primary" id="settingsOpenScheduleViewBtn" type="button">Open Schedule View</button>
            </div>
          </div>
        </div>
      </div>
    </section>

    <section data-settings-panel="integrations" class="settings-panel" style="display:none;">
      <div class="integrations-grid">
        <div class="card integrations-main-card">
          <div class="row" style="justify-content:space-between; align-items:flex-start; gap:12px; flex-wrap:wrap;">
            <div class="col" style="gap:4px;">
              <div class="h1" style="margin:0;">Integrations</div>
              <div class="p">Connect the tools you already use.</div>
            </div>
            <div class="col" style="gap:6px; min-width:240px;">
              <label class="p" for="integrationsSearchInput">Search integrations...</label>
              <input class="input" id="integrationsSearchInput" placeholder="Search integrations..." />
            </div>
          </div>

          <div style="height:10px;"></div>
          <div class="integrations-security-row">
            Connections are tenant-isolated by accountId. Tokens are stored securely.
          </div>

          <div style="height:12px;"></div>
          <div class="integrations-categories" role="tablist" aria-label="Integration categories">
            <button class="btn integrations-cat-btn active" type="button" data-int-cat="payments">Payments</button>
          </div>

          <div style="height:12px;"></div>
          <div id="integrationsCards" class="integrations-cards-grid"></div>
        </div>

        <div class="col" style="gap:12px;">
          <div class="card integrations-health-card">
            <div class="h1" style="margin:0;">Connection Health</div>
            <div style="height:8px;"></div>
            <div id="intHealthConnected" class="p">Connected: 0</div>
            <div id="intHealthLastSync" class="p">Last sync: --</div>
            <div id="intHealthErrors" class="p">Errors: none</div>
          </div>

          <div class="card integrations-events-card">
            <div class="h1" style="margin:0;">Imported events</div>
            <div class="p">Latest calendar events imported for this tenant.</div>
            <div style="height:8px;"></div>
            <div id="integrationsImportedEvents" class="integrations-log-list"></div>
          </div>
        </div>
      </div>

      <div style="height:12px;"></div>
      <div class="card integrations-activity-card">
        <div class="h1" style="margin:0;">Activity Log</div>
        <div class="p">Recent integration events for this workspace.</div>
        <div style="height:8px;"></div>
        <div id="integrationsActivityLog" class="integrations-log-list"></div>
      </div>

      <div class="billing-note-footer">
        <span>Integration SLA: Sync health monitored. Errors are logged and visible per tenant.</span>
      </div>

      <div id="integrationLearnModal" class="billing-modal-overlay hidden" aria-hidden="true">
        <div class="billing-modal billing-modal-sm" role="dialog" aria-modal="true" aria-labelledby="integrationLearnModalTitle">
          <div class="h1" id="integrationLearnModalTitle" style="margin:0;">Integration</div>
          <div class="p" id="integrationLearnModalBody" style="margin-top:8px;">Details</div>
          <div class="row" style="justify-content:flex-end; margin-top:12px;">
            <button class="btn primary" id="integrationLearnModalCloseBtn" type="button">Close</button>
          </div>
        </div>
      </div>

      <div id="integrationComingSoonModal" class="billing-modal-overlay hidden" aria-hidden="true">
        <div class="billing-modal billing-modal-sm" role="dialog" aria-modal="true" aria-labelledby="integrationComingSoonTitle">
          <div class="h1" id="integrationComingSoonTitle" style="margin:0;">Coming soon</div>
          <div class="p" id="integrationComingSoonBody" style="margin-top:8px;">This integration is in progress.</div>
          <div class="row" style="justify-content:flex-end; margin-top:12px;">
            <button class="btn primary" id="integrationComingSoonCloseBtn" type="button">Close</button>
          </div>
        </div>
      </div>

      <div id="integrationIcsModal" class="billing-modal-overlay hidden" aria-hidden="true">
        <div class="billing-modal" role="dialog" aria-modal="true" aria-labelledby="integrationIcsModalTitle">
          <div class="row" style="justify-content:space-between; align-items:flex-start; gap:10px;">
            <div class="col" style="gap:4px;">
              <div class="h1" id="integrationIcsModalTitle" style="margin:0;">Universal Calendar Sync</div>
              <div class="p">Works with TimeTree, Google, Outlook, Calendly, Square, Acuity, and more.</div>
            </div>
            <button class="btn" id="integrationIcsCloseBtn" type="button">Close</button>
          </div>

          <div style="height:10px;"></div>
          <div class="integrations-wizard-steps">
            <span class="badge">Step 1: Provider</span>
            <span class="badge">Step 2: Feed</span>
            <span class="badge">Step 3: Sync</span>
          </div>

          <div style="height:10px;"></div>
          <div class="grid2">
            <div class="card" style="background:var(--panel);">
              <div class="h1" style="margin:0;">1) Choose provider</div>
              <div style="height:8px;"></div>
              <label class="toggle"><input type="radio" name="icsProvider" value="timetree" checked /> TimeTree</label>
              <label class="toggle"><input type="radio" name="icsProvider" value="google" /> Google Calendar</label>
              <label class="toggle"><input type="radio" name="icsProvider" value="outlook" /> Outlook</label>
              <label class="toggle"><input type="radio" name="icsProvider" value="calendly" /> Calendly / Scheduling Tool</label>
              <label class="toggle"><input type="radio" name="icsProvider" value="other" /> Other (iCal link)</label>
            </div>

            <div class="card" style="background:var(--panel);">
              <div class="h1" style="margin:0;">2) Paste ICS feed URL</div>
              <div style="height:8px;"></div>
              <label class="p">Calendar feed URL (ends with .ics)</label>
              <input class="input" id="icsUrlInput" placeholder="https://example.com/calendar.ics" />
              <label class="toggle"><input type="checkbox" id="icsPrivacyModeInput" checked /> Privacy mode (import busy blocks only; hide event titles)</label>
              <label class="toggle"><input type="checkbox" id="icsIncludeDetailsInput" disabled /> Include event details if available</label>
              <div style="height:6px;"></div>
              <div class="p" style="font-weight:600;">What we access</div>
              <ul class="p" style="margin:0; padding-left:18px;">
                <li>Read-only calendar event times from your ICS feed</li>
                <li>Event title/location only when privacy mode is off</li>
                <li>No write access to your calendar provider</li>
              </ul>
              <div style="height:8px;"></div>
              <div class="row" style="justify-content:flex-end;">
                <button class="btn" id="icsTestBtn" type="button">Test connection</button>
              </div>
              <div class="p" id="icsTestStatus" style="min-height:18px;"></div>
            </div>
          </div>

          <div style="height:10px;"></div>
          <div class="card" style="background:var(--panel);">
            <div class="h1" style="margin:0;">3) Sync settings</div>
            <div style="height:8px;"></div>
            <div class="grid2">
              <div class="col">
                <label class="p">Sync frequency</label>
                <select class="select" id="icsSyncMinutesSelect">
                  <option value="5">5 min (dev)</option>
                  <option value="15">15 min</option>
                  <option value="60" selected>60 min (default)</option>
                  <option value="0">manual only</option>
                </select>
              </div>
              <div class="col">
                <label class="p">Workspace timezone</label>
                <div class="badge" id="icsTimezoneBadge">--</div>
              </div>
            </div>
            <div style="height:8px;"></div>
            <div class="row" style="justify-content:space-between; align-items:center; flex-wrap:wrap;">
              <div class="p" id="icsManageDetails">Not connected</div>
              <div class="row" style="justify-content:flex-end; gap:8px;">
                <button class="btn" id="icsSyncNowBtn" type="button">Sync now</button>
                <button class="btn" id="icsDisconnectBtn" type="button">Disconnect</button>
                <button class="btn primary" id="icsSaveBtn" type="button">Connect & Sync</button>
              </div>
            </div>
            <div class="p">ICS URL is masked in this view. Reveal is only available in Developer workflows.</div>
          </div>
        </div>
      </div>

      <div id="integrationStripeModal" class="billing-modal-overlay hidden" aria-hidden="true">
        <div class="billing-modal" role="dialog" aria-modal="true" aria-labelledby="integrationStripeModalTitle">
          <div class="row" style="justify-content:space-between; align-items:flex-start; gap:10px;">
            <div class="col" style="gap:4px;">
              <div class="h1" id="integrationStripeModalTitle" style="margin:0;">Payments (Stripe)</div>
              <div class="p">Connect Stripe to sync invoices and billing status into Relay.</div>
            </div>
            <button class="btn" id="integrationStripeCloseBtn" type="button">Close</button>
          </div>

          <div style="height:10px;"></div>
          <div class="grid2">
            <div class="col">
              <label class="p">Stripe Secret Key</label>
              <input class="input" id="stripeSecretKeyInput" type="password" placeholder="sk_test_..." />
            </div>
            <div class="col">
              <label class="p">Stripe Publishable Key (optional)</label>
              <input class="input" id="stripePublishableKeyInput" placeholder="pk_test_..." />
            </div>
          </div>
          <div style="height:8px;"></div>
          <div class="grid2">
            <div class="col">
              <label class="p">Webhook Signing Secret (optional)</label>
              <input class="input" id="stripeWebhookSecretInput" type="password" placeholder="whsec_..." />
            </div>
            <div class="col">
              <label class="p">Customer ID (optional, for invoice sync)</label>
              <input class="input" id="stripeCustomerIdInput" placeholder="cus_..." />
            </div>
          </div>
          <div style="height:8px;"></div>
          <div class="p" id="stripeWebhookUrlLine">Webhook URL: --</div>

          <div style="height:10px;"></div>
          <div class="card" style="background:var(--panel);">
            <div class="p" id="stripeManageDetails">Not connected</div>
            <div class="p" id="stripeTestStatus" style="min-height:18px;"></div>
            <div class="row" style="justify-content:flex-end; gap:8px; flex-wrap:wrap;">
              <button class="btn" id="stripeTestBtn" type="button">Test</button>
              <button class="btn" id="stripeSyncBtn" type="button">Sync invoices</button>
              <button class="btn" id="stripeDisconnectBtn" type="button">Disconnect</button>
              <button class="btn primary" id="stripeSaveBtn" type="button">Save Stripe</button>
            </div>
          </div>
        </div>
      </div>

      <div id="integrationDisconnectModal" class="billing-modal-overlay hidden" aria-hidden="true">
        <div class="billing-modal billing-modal-sm" role="dialog" aria-modal="true" aria-labelledby="integrationDisconnectTitle">
          <div class="h1" id="integrationDisconnectTitle" style="margin:0;">Disconnect calendar?</div>
          <div class="p" style="margin-top:8px;">This will disable ICS sync for this tenant and clear imported ICS events.</div>
          <div class="row" style="justify-content:flex-end; margin-top:12px; gap:8px;">
            <button class="btn" id="integrationDisconnectCancelBtn" type="button">Cancel</button>
            <button class="btn primary" id="integrationDisconnectConfirmBtn" type="button">Disconnect</button>
          </div>
        </div>
      </div>
    </section>

    <section data-settings-panel="customer-billing" class="settings-panel" style="display:none;">
      <div class="card">
        <div class="row" style="justify-content:space-between; align-items:flex-start; gap:10px; flex-wrap:wrap;">
          <div class="col" style="gap:4px;">
            <div class="h1" style="margin:0;">Customer Billing</div>
            <div class="p">Track customer-side recovered revenue and booking value from your conversations.</div>
          </div>
          <div class="row" style="gap:8px;">
            <button class="btn" id="customerBillingRefreshBtn" type="button">Refresh</button>
            <button class="btn" id="customerBillingOpenRevenueBtn" type="button">Open Revenue Board</button>
          </div>
        </div>
        <div style="height:12px;"></div>
        <div class="grid2">
          <div class="card" style="background:var(--panel);">
            <div class="p">Recovered Revenue</div>
            <div class="h1" id="customerBillingRecovered" style="margin:0;">$0</div>
          </div>
          <div class="card" style="background:var(--panel);">
            <div class="p">Booked Value This Month</div>
            <div class="h1" id="customerBillingMonth" style="margin:0;">$0</div>
          </div>
          <div class="card" style="background:var(--panel);">
            <div class="p">Open Opportunities</div>
            <div class="h1" id="customerBillingOpenOpps" style="margin:0;">0</div>
          </div>
          <div class="card" style="background:var(--panel);">
            <div class="p">At-Risk Value</div>
            <div class="h1" id="customerBillingAtRisk" style="margin:0;">$0</div>
          </div>
        </div>
        <div style="height:12px;"></div>
        <div class="card" style="background:var(--panel);">
          <div class="h1" style="margin:0;">Recent Customer Billing Signals</div>
          <div style="height:8px;"></div>
          <div id="customerBillingSignals" class="col" style="gap:6px;"></div>
        </div>
        <div style="height:12px;"></div>
        <div class="card" style="background:var(--panel);">
          <div class="row" style="justify-content:space-between; align-items:center; gap:10px; flex-wrap:wrap;">
            <div class="h1" style="margin:0;">Customer Invoices</div>
            <div class="p" id="customerBillingInvoiceSummary">Loading invoices...</div>
          </div>
          <div style="height:8px;"></div>
          <div class="billing-table-wrap">
            <table class="billing-table">
              <thead>
                <tr>
                  <th>Date</th>
                  <th>Invoice</th>
                  <th>Customer</th>
                  <th>Service</th>
                  <th>Method</th>
                  <th>Status</th>
                  <th>Amount</th>
                  <th>PDF</th>
                </tr>
              </thead>
              <tbody id="customerBillingInvoicesBody"></tbody>
            </table>
          </div>
        </div>
      </div>
    </section>

    <section data-settings-panel="email" class="settings-panel" style="display:none;">
      <div class="card">
        <div class="row" style="justify-content:space-between; align-items:flex-start; flex-wrap:wrap; gap:10px;">
          <div class="col" style="gap:4px;">
            <div class="h1" style="margin:0;">Email Campaigns</div>
            <div class="p">Use booking emails for promos, holiday offers, and follow-up deals.</div>
          </div>
          <span class="badge">Protected</span>
        </div>

        <div style="height:12px;"></div>
        <div class="card" style="background:var(--panel);">
          <div class="row" style="justify-content:space-between; align-items:center; gap:10px; flex-wrap:wrap;">
            <div class="h1" style="margin:0;">Recipients from Bookings</div>
            <div class="row" style="gap:8px; flex-wrap:wrap;">
              <button class="btn" id="emailSelectAllRecipientsBtn" type="button">Select all</button>
              <button class="btn" id="emailClearRecipientsBtn" type="button">Clear</button>
              <button class="btn" id="emailRefreshRecipientsBtn" type="button">Refresh list</button>
            </div>
          </div>
          <div class="p" id="emailRecipientsSummary" style="margin-top:8px;">Loading recipients...</div>
          <div class="p" id="emailSelectedSummary" style="margin-top:4px;">0 selected</div>
          <div style="height:8px;"></div>
          <div id="emailRecipientList" class="col" style="gap:6px; max-height:240px; overflow:auto;"></div>
        </div>

        <div style="height:12px;"></div>
        <div class="card" style="background:var(--panel);">
          <div class="row" style="justify-content:space-between; align-items:flex-start; gap:10px; flex-wrap:wrap;">
            <div class="col" style="gap:4px;">
              <div class="h1" style="margin:0;">Compose campaign</div>
              <div class="p">Templates autofill subject/body. Edit before sending.</div>
            </div>
            <div class="row" style="gap:8px; flex-wrap:wrap;">
              <button class="btn" type="button" data-email-template="holiday_offer">Holiday Deal</button>
              <button class="btn" type="button" data-email-template="flash_sale">Flash Sale</button>
              <button class="btn" type="button" data-email-template="maintenance_followup">Maintenance Follow-up</button>
            </div>
          </div>
          <div style="height:10px;"></div>
          <div class="col">
            <label class="p">Subject</label>
            <input class="input" id="emailCampaignSubject" placeholder="Seasonal offer from your detail shop" />
          </div>
          <div style="height:8px;"></div>
          <div class="col">
            <label class="p">Message</label>
            <textarea class="input" id="emailCampaignBody" rows="8" placeholder="Write your campaign email..."></textarea>
          </div>
          <div style="height:10px;"></div>
          <div class="row" style="justify-content:flex-end; align-items:center; gap:10px;">
            <div class="p" id="emailCampaignStatus" style="min-height:18px; margin-right:auto;"></div>
            <button class="btn primary" id="emailSendCampaignBtn" type="button">Send campaign</button>
          </div>
        </div>

        <div style="height:12px;"></div>
        <div class="card" style="background:var(--panel);">
          <div class="row" style="justify-content:space-between; align-items:center; gap:10px; flex-wrap:wrap;">
            <div class="h1" style="margin:0;">Recent campaigns</div>
            <button class="btn" id="emailRefreshCampaignsBtn" type="button">Refresh history</button>
          </div>
          <div style="height:8px;"></div>
          <div id="emailCampaignHistory" class="col" style="gap:6px;"></div>
        </div>
      </div>
    </section>

    <section data-settings-panel="call-routing" class="settings-panel" style="display:none;">
      <div class="card">
        <div class="row" style="justify-content:space-between; align-items:flex-start; gap:10px; flex-wrap:wrap;">
          <div class="col" style="gap:4px;">
            <div class="h1" style="margin:0;">Call Routing</div>
            <div class="p">Set up carrier forwarding from your business number to your Relay number.</div>
          </div>
          <span class="badge" id="callRoutingStatusBadge">Not setup</span>
        </div>
        <div style="height:12px;"></div>
        <div class="grid2">
          <div class="col">
            <label class="p">Client business number</label>
            <input class="input" id="callRoutingBusinessNumber" placeholder="+18145550123" />
          </div>
          <div class="col">
            <label class="p">Relay forwarding number (Twilio)</label>
            <input class="input" id="callRoutingRelayNumber" readonly />
          </div>
        </div>
        <div style="height:10px;"></div>
        <div class="grid2">
          <div class="col">
            <label class="p">Carrier</label>
            <select class="select" id="callRoutingCarrier">
              <option value="generic">Carrier: Generic</option>
              <option value="att">AT&T</option>
              <option value="verizon">Verizon</option>
              <option value="tmobile">T-Mobile</option>
            </select>
          </div>
          <div class="col">
            <label class="p">Current status</label>
            <div class="badge" id="callRoutingStatusLine">Not setup</div>
          </div>
        </div>
        <div style="height:12px;"></div>
        <div class="card" style="background:var(--panel);">
          <div class="h1" style="margin:0;">Carrier Steps</div>
          <div class="p" style="margin-top:6px;">Defaults are *72 and *73. Update these if your carrier uses different codes.</div>
          <div style="height:8px;"></div>
          <div class="row" style="justify-content:space-between; gap:10px; flex-wrap:wrap; align-items:center;">
            <div class="col" style="gap:4px;">
              <div class="p">Enable forwarding code (default *72)</div>
              <input class="input" id="callRoutingEnableCode" value="*72" />
            </div>
            <button class="btn" id="callRoutingCopyEnableBtn" type="button">Copy enable code</button>
          </div>
          <div style="height:8px;"></div>
          <div class="row" style="justify-content:space-between; gap:10px; flex-wrap:wrap; align-items:center;">
            <div class="col" style="gap:4px;">
              <div class="p">Disable forwarding code (default *73)</div>
              <input class="input" id="callRoutingDisableCode" value="*73" />
            </div>
            <button class="btn" id="callRoutingCopyDisableBtn" type="button">Copy disable code</button>
          </div>
        </div>
        <div style="height:12px;"></div>
        <div class="row" style="justify-content:flex-end; gap:8px; flex-wrap:wrap;">
          <div class="p" id="callRoutingStatus" style="min-height:18px; margin-right:auto;"></div>
          <button class="btn" id="callRoutingMarkCompleteBtn" type="button">Mark complete</button>
          <button class="btn" id="callRoutingRunTestBtn" type="button">Run test call</button>
          <button class="btn primary" id="callRoutingSaveBtn" type="button">Save call routing</button>
        </div>
      </div>
    </section>

    <section data-settings-panel="admin" class="settings-panel" style="display:none;">
      <div class="card">
        <div class="row" style="justify-content:space-between; align-items:flex-start; flex-wrap:wrap; gap:10px;">
          <div class="col" style="gap:4px;">
            <div class="h1" style="margin:0;">Admin Lock</div>
            <div class="p">Protect sensitive workspace actions with an account admin passcode.</div>
          </div>
          <span class="badge">Protected</span>
        </div>
        <div style="height:10px;"></div>
        <div class="p" id="adminGateStatus">Status: Locked</div>
        <div style="height:10px;"></div>
        <div class="row" style="justify-content:flex-end; gap:8px;">
          <button class="btn" id="adminLockBtn" type="button">Lock now</button>
          <button class="btn" id="adminUnlockBtn" type="button">Unlock</button>
          <button class="btn primary" id="adminChangePasscodeBtn" type="button">Change passcode</button>
        </div>
      </div>
    </section>

    <section data-settings-panel="billing" class="settings-panel" style="display:none;">
      <div id="billingHealthBanner" class="billing-health-banner hidden"></div>
      <div style="height:8px;"></div>
      <div class="row">
        <button class="btn danger hidden" id="billingDueCtaBtn" type="button">BILL DUE!!!</button>
      </div>

      <div class="card billing-card" id="billingMasterCard" style="display:none;">
        <div class="row" style="justify-content:space-between; align-items:flex-start; gap:10px; flex-wrap:wrap;">
          <div class="col" style="gap:4px;">
            <div class="h1" style="margin:0;">Master Billing Overview</div>
            <div class="p">Superadmin rollup across all client workspaces.</div>
          </div>
          <span class="badge" id="billingMasterAsOf">As of --</span>
        </div>
        <div style="height:10px;"></div>
        <div class="grid2">
          <div class="col">
            <div class="p">Total paid revenue</div>
            <div class="h1" id="billingMasterPaidRevenue" style="margin:0;">$0</div>
          </div>
          <div class="col">
            <div class="p">Outstanding invoices</div>
            <div class="h1" id="billingMasterOutstanding" style="margin:0;">$0</div>
          </div>
          <div class="col">
            <div class="p">Monthly run-rate</div>
            <div class="h1" id="billingMasterRunRate" style="margin:0;">$0</div>
          </div>
          <div class="col">
            <div class="p">Connected Stripe workspaces</div>
            <div class="h1" id="billingMasterConnected" style="margin:0;">0</div>
          </div>
        </div>
        <div style="height:10px;"></div>
        <div class="p" id="billingMasterStatsLine">Loading...</div>
        <div style="height:8px;"></div>
        <div class="billing-table-wrap">
          <table class="billing-table">
            <thead>
              <tr>
                <th>Workspace</th>
                <th>To Number</th>
                <th>Paid Revenue</th>
              </tr>
            </thead>
            <tbody id="billingMasterTopBody"></tbody>
          </table>
        </div>
      </div>

      <div style="height:12px;"></div>

      <div class="billing-grid">
        <div class="billing-col">
          <div class="card billing-card">
            <div class="row" style="justify-content:space-between; align-items:flex-start; gap:10px; flex-wrap:wrap;">
              <div class="col" style="gap:4px;">
                <div class="h1" style="margin:0;">Subscription</div>
                <div class="p" id="billingPlanLine">Pro plan</div>
              </div>
              <div class="row" style="gap:8px; align-items:center;">
                <span id="billingDemoBadge" class="badge">Demo billing</span>
                <span id="billingStatusPill" class="billing-status-pill">Active</span>
              </div>
            </div>
            <div style="height:10px;"></div>
            <div class="billing-price-line" id="billingPriceLine">$129 / month</div>
            <div class="p" id="billingNextBillLine">Next billing date: ?</div>
            <div class="p" id="billingSeatsLine">Seats: ?</div>
            <div class="p" id="billingLockLine">Access: Active</div>
            <div class="p" id="billingDunningLine">Dunning: No retries pending</div>
            <div class="billing-card-foot">
              <button class="btn primary" id="billingPortalBtn" type="button">Billing Portal</button>
              <button class="btn" id="billingCompareBtn" type="button">Compare plans</button>
              <button class="btn" id="billingStartTrialBtn" type="button">Start trial</button>
              <button class="btn" id="billingCancelBtn" type="button" disabled title="Demo mode">Cancel</button>
            </div>
          </div>

          <div class="card billing-card">
            <div class="row" style="justify-content:space-between; align-items:flex-start; gap:10px;">
              <div class="col" style="gap:4px;">
                <div class="h1" style="margin:0;">Invoices</div>
                <div class="p">Downloadable invoices and payment status.</div>
              </div>
              <button class="btn" id="billingViewAllInvoicesBtn" type="button" disabled title="Demo mode">View all invoices</button>
            </div>
            <div style="height:10px;"></div>
            <div class="billing-table-wrap">
              <table class="billing-table">
                <thead>
                  <tr>
                    <th>Date</th>
                    <th>Invoice</th>
                    <th>Amount</th>
                    <th>Status</th>
                    <th>Actions</th>
                  </tr>
                </thead>
                <tbody id="billingInvoicesTbody"></tbody>
              </table>
            </div>
            <div class="p" id="billingInvoicesFooter" style="margin-top:8px;">Showing 0 of 0</div>
          </div>
        </div>

        <div class="billing-col">
          <div class="card billing-card">
            <div class="h1" style="margin:0;">Payments Connection</div>
            <div class="p">Connect Stripe to sync invoices and payment outcomes into Relay.</div>
            <div style="height:10px;"></div>
            <div class="row" style="justify-content:space-between; align-items:center; gap:10px; flex-wrap:wrap;">
              <div class="p" id="billingStripeConnectionStatus">Checking Stripe connection...</div>
              <div class="row" style="gap:8px;">
                <button class="btn" id="billingStripeRefreshBtn" type="button">Refresh</button>
                <button class="btn primary" id="billingStripeConnectBtn" type="button">Connect</button>
              </div>
            </div>
          </div>

          <div class="card billing-card">
            <div class="h1" style="margin:0;">Payment Method</div>
            <div class="p" id="billingPaymentMethodLine">No payment method on file</div>
            <div style="height:10px;"></div>
            <div class="row" style="justify-content:flex-end;">
              <button class="btn" id="billingUpdatePaymentBtn" type="button">Update payment method</button>
            </div>
          </div>

          <div class="card billing-card">
            <div class="h1" style="margin:0;">Current usage</div>
            <div class="p" id="billingUsageResetLine">Resets on ?</div>
            <div class="billing-usage-item">
              <div class="row" style="justify-content:space-between;"><span class="p">Revenue recovered this month</span><span class="p" id="billingUsageRevenue">0 / 0</span></div>
              <div class="billing-usage-bar"><span id="billingUsageRevenueBar"></span></div>
            </div>
            <div class="billing-usage-item">
              <div class="row" style="justify-content:space-between;"><span class="p">Automations run</span><span class="p" id="billingUsageAutomations">0 / 0</span></div>
              <div class="billing-usage-bar"><span id="billingUsageAutomationsBar"></span></div>
            </div>
            <div class="billing-usage-item">
              <div class="row" style="justify-content:space-between;"><span class="p">Active conversations</span><span class="p" id="billingUsageConversations">0 / 0</span></div>
              <div class="billing-usage-bar"><span id="billingUsageConversationsBar"></span></div>
            </div>
          </div>

          <div class="card billing-card">
            <div class="h1" style="margin:0;">Billing Details</div>
            <div class="p">Company profile and invoice recipients.</div>
            <div style="height:10px;"></div>
            <div class="billing-form-grid">
              <div class="col">
                <label class="p">Company name</label>
                <input class="input" id="billingCompanyName" />
              </div>
              <div class="col">
                <label class="p">Billing email</label>
                <input class="input" id="billingEmail" />
              </div>
            </div>
            <div style="height:8px;"></div>
            <div class="billing-form-grid">
              <div class="col">
                <label class="p">Address line 1</label>
                <input class="input" id="billingAddress1" />
              </div>
              <div class="col">
                <label class="p">Address line 2</label>
                <input class="input" id="billingAddress2" />
              </div>
            </div>
            <div style="height:8px;"></div>
            <div class="billing-form-grid">
              <div class="col">
                <label class="p">City</label>
                <input class="input" id="billingCity" />
              </div>
              <div class="col">
                <label class="p">State / Region</label>
                <input class="input" id="billingState" />
              </div>
            </div>
            <div style="height:8px;"></div>
            <div class="billing-form-grid">
              <div class="col">
                <label class="p">Postal code</label>
                <input class="input" id="billingPostalCode" />
              </div>
              <div class="col">
                <label class="p">Country</label>
                <input class="input" id="billingCountry" />
              </div>
            </div>
            <div style="height:8px;"></div>
            <div class="col">
              <label class="p">Tax ID (optional)</label>
              <input class="input" id="billingTaxId" />
            </div>
            <div style="height:10px;"></div>
            <div class="row" style="justify-content:flex-end; align-items:center; gap:10px;">
              <div class="p" id="billingDetailsStatus" style="margin-right:auto; min-height:18px;"></div>
              <button class="btn primary" id="billingDetailsSaveBtn" type="button">Save billing details</button>
            </div>
          </div>
        </div>
      </div>

      <div style="height:14px;"></div>

      <div class="card billing-card">
        <div class="h1" style="margin:0;">Billing activity</div>
        <div class="p">Recent billing events for this workspace.</div>
        <div style="height:10px;"></div>
        <div class="billing-table-wrap">
          <table class="billing-table">
            <thead>
              <tr>
                <th>When</th>
                <th>Event</th>
                <th>Details</th>
              </tr>
            </thead>
            <tbody id="billingActivityTbody"></tbody>
          </table>
        </div>
      </div>

      <div class="billing-note-footer">
        <span>Payments secured. Access is tenant-isolated by accountId.</span>
        <span id="billingUpdatedAt">Last billing update: ?</span>
      </div>

      <div id="billingPlanModal" class="billing-modal-overlay hidden" aria-hidden="true">
        <div class="billing-modal" role="dialog" aria-modal="true" aria-labelledby="billingPlanModalTitle">
          <div class="row" style="justify-content:space-between; align-items:flex-start; gap:10px; flex-wrap:wrap;">
            <div class="col" style="gap:4px;">
              <div class="h1" id="billingPlanModalTitle" style="margin:0;">Upgrade to Pro</div>
              <div class="p">Unlock advanced automations and team features.</div>
            </div>
            <button class="btn" id="billingPlanModalCloseBtn" type="button" aria-label="Close">X</button>
          </div>
          <div style="height:10px;"></div>
          <div class="row" style="justify-content:flex-end;">
            <div class="billing-cadence-toggle" role="tablist" aria-label="Billing cadence">
              <button class="btn is-active" type="button" id="billingCadenceMonthlyBtn" data-billing-cadence="monthly">Monthly</button>
              <button class="btn" type="button" id="billingCadenceAnnualBtn" data-billing-cadence="annual">Annual</button>
            </div>
          </div>
          <div style="height:12px;"></div>
          <div class="billing-plan-grid" id="billingPlanGrid"></div>
          <div style="height:10px;"></div>
          <div class="billing-note-footer" style="padding:0;">
            <span>Cancel anytime. Prorated changes. No hidden fees.</span>
          </div>
          <div style="height:12px;"></div>
          <div class="card billing-upgrade-faq">
            <details>
              <summary>Will I lose data if I downgrade?</summary>
              <div class="p">No. Historical workspace data remains accessible based on your plan limits.</div>
            </details>
            <details>
              <summary>Can I change plans later?</summary>
              <div class="p">Yes. Plan changes can be made any time from billing settings.</div>
            </details>
            <details>
              <summary>Do you offer invoices?</summary>
              <div class="p">Yes. Invoice support is available for monthly and annual subscriptions.</div>
            </details>
          </div>
        </div>
      </div>

      <div id="billingInfoModal" class="billing-modal-overlay hidden" aria-hidden="true">
        <div class="billing-modal billing-modal-sm" role="dialog" aria-modal="true" aria-labelledby="billingInfoModalTitle">
          <div class="h1" id="billingInfoModalTitle" style="margin:0;">Billing</div>
          <div class="p" id="billingInfoModalBody" style="margin-top:8px;">Message</div>
          <div class="row" style="justify-content:flex-end; margin-top:12px;">
            <button class="btn primary" id="billingInfoModalCloseBtn" type="button">Close</button>
          </div>
        </div>
      </div>
      <div id="billingConfirmModal" class="billing-modal-overlay hidden" aria-hidden="true">
        <div class="billing-modal billing-modal-sm" role="dialog" aria-modal="true" aria-labelledby="billingConfirmModalTitle">
          <div class="h1" id="billingConfirmModalTitle" style="margin:0;">Confirm upgrade</div>
          <div class="p" id="billingConfirmModalBody" style="margin-top:8px;">This would update your subscription.</div>
          <div class="row" style="justify-content:flex-end; margin-top:12px; gap:8px;">
            <button class="btn" id="billingConfirmCancelBtn" type="button">Cancel</button>
            <button class="btn primary" id="billingConfirmActionBtn" type="button">Confirm</button>
          </div>
        </div>
      </div>
    </section>

    <section data-settings-panel="developer" class="settings-panel settings-panel-developer" style="display:none;">
      <div class="card developer-controls-card">
        <div class="h1 developer-card-title">Developer Controls</div>
        <div class="p">Local runtime switches for tenant development behavior.</div>
        <div style="height:10px;"></div>
        <div class="card developer-warning">
          <div class="p"><b>Warning:</b> Developer mode should be OFF in production.</div>
        </div>
        <div style="height:10px;"></div>
        <label class="toggle developer-toggle"><input type="checkbox" id="devModeEnabled" /> Developer Mode (local only)</label>
        <label class="toggle developer-toggle"><input type="checkbox" id="devAutoCreateTenants" /> Auto-create Tenants</label>
        <label class="toggle developer-toggle"><input type="checkbox" id="devVerboseTenantLogs" /> Verbose Tenant Logs</label>
        <label class="toggle developer-toggle"><input type="checkbox" id="devSimulateOutbound" /> Simulate Outbound Messages (no Twilio send)</label>
        <div style="height:10px;"></div>
        <div class="row developer-actions" style="justify-content:flex-end; gap:8px;">
          <div class="p developer-status" id="devSettingsStatus" style="min-height:18px; margin-right:auto;"></div>
          <button class="btn primary" id="devSettingsSaveBtn" type="button">Save Developer Settings</button>
        </div>
      </div>

        <div style="height:14px;"></div>

      <div class="card">
        <div class="h1" style="margin:0;">Simulate Conversation</div>
        <div class="p">Generate a mock detailing conversation so you can inspect the AI replies without using real leads.</div>
        <div style="height:10px;"></div>
        <div class="row" style="justify-content:flex-end; gap:8px;">
          <div class="p developer-status" id="simulateConversationStatus" style="min-height:18px; margin-right:auto;"></div>
          <button class="btn primary" id="simulateConversationBtn" type="button">Simulate Conversation</button>
        </div>
      </div>

      <div class="card">
        <div class="h1" style="margin:0;">Phone Numbers</div>
        <div class="p">Manage inbound business numbers and set a primary routing number.</div>
        <div style="height:10px;"></div>
        <div id="workspaceNumbersList" class="col" style="gap:8px;"></div>
        <div style="height:10px;"></div>
        <div class="grid2">
          <div class="col">
            <label class="p">Add number (E.164)</label>
            <input class="input" id="workspaceNewNumberInput" placeholder="+18145550003" />
          </div>
          <div class="col">
            <label class="p">Label</label>
            <input class="input" id="workspaceNewNumberLabelInput" placeholder="Support line" />
          </div>
        </div>
        <div style="height:10px;"></div>
        <div class="row" style="justify-content:flex-end; gap:8px;">
          <div class="p" id="workspaceNumbersStatus" style="min-height:18px; margin-right:auto;"></div>
          <button class="btn" id="workspaceAddNumberBtn" type="button">Add number</button>
          <button class="btn primary" id="workspaceSaveNumbersBtn" type="button">Save numbers</button>
        </div>
      </div>

      <div style="height:14px;"></div>

      <div class="card developer-admin-card" id="adminSuperPanel">
        <div class="h1" style="margin:0;">Superadmin: Accounts & Users</div>
        <div class="p">Monitor created client accounts and manage login users.</div>
        <div style="height:10px;"></div>
        <div class="card" style="background:var(--panel);">
          <div class="h1" style="margin:0;">All Created Accounts</div>
          <div style="height:8px;"></div>
        <div class="p" id="adminAccountsSummary">Loading accounts...</div>
        <div style="height:8px;"></div>
        <div class="row" style="justify-content:flex-end; gap:8px;">
          <div class="p" id="adminQuickCreateStatus" style="min-height:18px; margin-right:auto;"></div>
          <select class="select" id="adminQuickCreateTemplate" style="max-width:190px;">
            <option value="detailer">Detailer</option>
          </select>
          <button class="btn" id="adminQuickCreateBtn" type="button">Create test account</button>
        </div>
        <div style="height:8px;"></div>
          <div style="border:1px solid var(--border); border-radius:12px; overflow:auto; max-height:240px; background:var(--panel-solid);">
            <table style="width:100%; border-collapse:collapse; min-width:640px;">
              <thead>
                <tr style="text-align:left;">
                  <th style="padding:10px; border-bottom:1px solid var(--border);">Business</th>
                  <th style="padding:10px; border-bottom:1px solid var(--border);">Account ID</th>
                  <th style="padding:10px; border-bottom:1px solid var(--border);">To Number</th>
                  <th style="padding:10px; border-bottom:1px solid var(--border);">Created</th>
                </tr>
              </thead>
              <tbody id="adminAccountsTableBody"></tbody>
            </table>
          </div>
        </div>
        <div style="height:10px;"></div>
        <div class="row" style="justify-content:flex-end; gap:8px;">
          <button class="btn" id="adminRefreshBtn" type="button">Refresh lists</button>
        </div>
        <div style="height:10px;"></div>
        <div class="card" style="background:var(--panel);">
          <div class="h1" style="margin:0;">Reset User Password</div>
          <div style="height:8px;"></div>
          <div class="grid2">
            <div class="col">
              <label class="p">User</label>
              <select class="select" id="adminResetUserId"></select>
            </div>
            <div class="col">
              <label class="p">New temporary password</label>
              <input class="input" id="adminResetPasswordInput" type="text" placeholder="TempPass123!" />
            </div>
          </div>
          <div style="height:10px;"></div>
          <div class="row" style="justify-content:flex-end; gap:8px;">
            <div class="p" id="adminResetPasswordStatus" style="min-height:18px; margin-right:auto;"></div>
            <button class="btn primary" id="adminResetPasswordBtn" type="button">Reset password</button>
          </div>
        </div>
        <div style="height:8px;"></div>
        <div>
          <div class="p"><b>Users</b></div>
          <pre id="adminUsersOut" style="margin:6px 0 0; padding:10px; border:1px solid var(--border); border-radius:12px; background:var(--panel-solid); overflow:auto; max-height:220px;"></pre>
        </div>
      </div>

      <div style="height:14px;"></div>

      <div class="card developer-admin-card" id="superadminOpsPanel">
        <div class="row" style="justify-content:space-between; align-items:flex-start; gap:10px; flex-wrap:wrap;">
          <div class="col" style="gap:4px;">
            <div class="h1" style="margin:0;">Superadmin: Twilio, Numbers, Assignments, Billing</div>
            <div class="p">Main Twilio account view across clients, phone number ownership, assigned users, and billing status.</div>
          </div>
          <div class="row" style="gap:8px; align-items:center;">
            <span class="badge" id="superadminOpsAsOf">As of --</span>
            <button class="btn" id="superadminOpsRefreshBtn" type="button">Refresh</button>
          </div>
        </div>
        <div style="height:10px;"></div>
        <div class="grid2">
          <div class="card" style="background:var(--panel);">
            <div class="p">Workspaces tracked</div>
            <div class="h1" id="superadminOpsWorkspaceCount" style="margin:0;">0</div>
          </div>
          <div class="card" style="background:var(--panel);">
            <div class="p">Twilio connected</div>
            <div class="h1" id="superadminOpsTwilioConnected" style="margin:0;">0</div>
          </div>
          <div class="card" style="background:var(--panel);">
            <div class="p">Main Twilio account</div>
            <div class="h1" id="superadminOpsMainSid" style="margin:0;">--</div>
          </div>
          <div class="card" style="background:var(--panel);">
            <div class="p">Used by workspaces</div>
            <div class="h1" id="superadminOpsMainSidCount" style="margin:0;">0</div>
          </div>
        </div>
        <div style="height:10px;"></div>
        <div class="billing-table-wrap">
          <table class="billing-table">
            <thead>
              <tr>
                <th>Workspace</th>
                <th>Numbers</th>
                <th>Assigned To</th>
                <th>Twilio</th>
                <th>Billing</th>
              </tr>
            </thead>
            <tbody id="superadminOpsTableBody"></tbody>
          </table>
        </div>
        <div style="height:10px;"></div>
        <div class="card" style="background:var(--panel);">
          <div class="h1" style="margin:0;">Platform Stripe (Superadmin Billing)</div>
          <div class="p">This Stripe connection is for dashboard subscription renewal only. Client booking Stripe stays in each workspace Integrations tab.</div>
          <div style="height:8px;"></div>
          <div class="grid2">
            <div class="col">
              <label class="p">Secret Key (sk_...)</label>
              <input class="input" id="superadminPlatformStripeSecretKeyInput" type="password" placeholder="sk_live_..." />
            </div>
            <div class="col">
              <label class="p">Publishable Key (optional)</label>
              <input class="input" id="superadminPlatformStripePublishableKeyInput" placeholder="pk_live_..." />
            </div>
          </div>
          <div style="height:8px;"></div>
          <div class="grid2">
            <div class="col">
              <label class="p">Webhook Secret (optional)</label>
              <input class="input" id="superadminPlatformStripeWebhookSecretInput" type="password" placeholder="whsec_..." />
            </div>
            <div class="col">
              <label class="p">Status</label>
              <div class="badge" id="superadminPlatformStripeStatusBadge">Not connected</div>
            </div>
          </div>
          <div style="height:8px;"></div>
          <div class="p" id="superadminPlatformStripeDetails">Not connected</div>
          <div style="height:10px;"></div>
          <div class="row" style="justify-content:flex-end; gap:8px; flex-wrap:wrap;">
            <div class="p" id="superadminPlatformStripeActionStatus" style="min-height:18px; margin-right:auto;"></div>
            <button class="btn" id="superadminPlatformStripeTestBtn" type="button">Test</button>
            <button class="btn" id="superadminPlatformStripeDisconnectBtn" type="button">Disconnect</button>
            <button class="btn primary" id="superadminPlatformStripeSaveBtn" type="button">Save Platform Stripe</button>
          </div>
        </div>
        <div style="height:10px;"></div>
        <div class="card" style="background:var(--panel);">
          <div class="row" style="justify-content:space-between; align-items:flex-start; gap:10px; flex-wrap:wrap;">
            <div class="col" style="gap:4px;">
              <div class="h1" style="margin:0;">Twilio Number Inventory</div>
              <div class="p">Numbers owned in Twilio and which workspaces they are assigned to.</div>
            </div>
            <span class="badge" id="superadminTwilioInventorySummary">Loading...</span>
          </div>
          <div style="height:8px;"></div>
          <div class="billing-table-wrap">
            <table class="billing-table">
              <thead>
                <tr>
                  <th>Twilio Number</th>
                  <th>Friendly Name</th>
                  <th>Assigned Workspaces</th>
                  <th>Discovered From</th>
                </tr>
              </thead>
              <tbody id="superadminTwilioInventoryBody"></tbody>
            </table>
          </div>
          <div class="p" id="superadminTwilioInventoryError" style="min-height:18px; margin-top:8px;"></div>
        </div>
        <div style="height:10px;"></div>
        <div class="card" style="background:var(--panel);">
          <div class="h1" style="margin:0;">Buy + Assign Twilio Number (Superadmin)</div>
          <div class="p">Search available numbers from the selected workspace Twilio account, then purchase and auto-attach.</div>
          <div style="height:8px;"></div>
          <div class="grid2">
            <div class="col">
              <label class="p">Country</label>
              <select class="select" id="superadminNumberSearchCountry">
                <option value="US">US</option>
              </select>
            </div>
            <div class="col">
              <label class="p">Area code (optional)</label>
              <input class="input" id="superadminNumberSearchAreaCode" placeholder="814" />
            </div>
          </div>
          <div style="height:8px;"></div>
          <div class="grid2">
            <div class="col">
              <label class="p">Contains pattern (optional)</label>
              <input class="input" id="superadminNumberSearchContains" placeholder="+1814%%%%###" />
            </div>
            <div class="col">
              <label class="p">Result limit</label>
              <input class="input" id="superadminNumberSearchLimit" type="number" min="1" max="50" value="20" />
            </div>
          </div>
          <div style="height:8px;"></div>
          <div class="grid2">
            <div class="col">
              <label class="p">Friendly label for workspace</label>
              <input class="input" id="superadminNumberPurchaseLabel" placeholder="Main line" />
            </div>
            <div class="col">
              <label class="p">Public backend base URL (optional, sets voice webhook)</label>
              <input class="input" id="superadminNumberWebhookBaseUrl" placeholder="https://your-domain.com" />
            </div>
          </div>
          <div style="height:10px;"></div>
          <div class="row" style="justify-content:flex-end; gap:8px;">
            <div class="p" id="superadminNumberSearchStatus" style="min-height:18px; margin-right:auto;"></div>
            <button class="btn primary" id="superadminNumberSearchBtn" type="button">Search available numbers</button>
          </div>
          <div style="height:8px;"></div>
          <div class="billing-table-wrap">
            <table class="billing-table">
              <thead>
                <tr>
                  <th>Number</th>
                  <th>Location</th>
                  <th>Capabilities</th>
                  <th>Action</th>
                </tr>
              </thead>
              <tbody id="superadminNumberSearchResultsBody"></tbody>
            </table>
          </div>
        </div>
        <div style="height:10px;"></div>
        <div class="card" style="background:var(--panel);">
          <div class="h1" style="margin:0;">Adjust Twilio for Workspace</div>
          <div style="height:8px;"></div>
          <div class="grid2">
            <div class="col">
              <label class="p">Workspace</label>
              <select class="select" id="superadminTwilioAccountSelect"></select>
            </div>
            <div class="col">
              <label class="p">Status</label>
              <div class="badge" id="superadminTwilioStatusBadge">Not connected</div>
            </div>
          </div>
          <div style="height:8px;"></div>
          <div class="grid2">
            <div class="col">
              <label class="p">Account SID (AC...)</label>
              <input class="input" id="superadminTwilioAccountSidInput" placeholder="ACxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx" />
            </div>
            <div class="col">
              <label class="p">API Key SID (SK...)</label>
              <input class="input" id="superadminTwilioApiKeySidInput" placeholder="SKxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx" />
            </div>
          </div>
          <div style="height:8px;"></div>
          <div class="grid2">
            <div class="col">
              <label class="p">API Key Secret</label>
              <input class="input" id="superadminTwilioApiKeySecretInput" type="password" placeholder="Leave blank to keep current" />
            </div>
            <div class="col">
              <label class="p">Webhook Auth Token</label>
              <input class="input" id="superadminTwilioWebhookTokenInput" type="password" placeholder="Optional (recommended)" />
            </div>
          </div>
          <div style="height:8px;"></div>
          <div class="grid2">
            <div class="col">
              <label class="p">Messaging Service SID (optional)</label>
              <input class="input" id="superadminTwilioMessagingServiceSidInput" placeholder="MGxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx" />
            </div>
            <div class="col">
              <label class="p">Phone Number (optional)</label>
              <input class="input" id="superadminTwilioPhoneNumberInput" placeholder="+18145551234" />
            </div>
          </div>
          <div style="height:8px;"></div>
          <div class="grid2">
            <div class="col">
              <label class="p">Forward unanswered calls to (E.164)</label>
              <input class="input" id="superadminTwilioVoiceForwardToInput" placeholder="+18145557654" />
            </div>
            <div class="col">
              <label class="p">Ring timeout seconds (10-60)</label>
              <input class="input" id="superadminTwilioVoiceDialTimeoutInput" type="number" min="10" max="60" value="20" />
            </div>
          </div>
          <div style="height:10px;"></div>
          <div class="row" style="justify-content:flex-end; gap:8px; flex-wrap:wrap;">
            <div class="p" id="superadminTwilioActionStatus" style="min-height:18px; margin-right:auto;"></div>
            <button class="btn" id="superadminTwilioTestBtn" type="button">Test</button>
            <button class="btn" id="superadminTwilioDisconnectBtn" type="button">Disconnect</button>
            <button class="btn primary" id="superadminTwilioSaveBtn" type="button">Save Twilio</button>
          </div>
        </div>
      </div>

      <div style="height:14px;"></div>

      <!-- Webhook Simulator -->
      <div class="card developer-webhook-card">
        <div class="row" style="justify-content:space-between; flex-wrap:wrap;">
          <div class="col" style="gap:4px;">
            <div class="h1" style="margin:0;">Webhook Simulator</div>
            <div class="p">Send test events to your local backend (no curl).</div>
          </div>
          <span class="badge">DEV</span>
        </div>

        <div style="height:12px;"></div>

        <div class="grid2">
          <div class="col">
            <label class="p">Account "To" number</label>
            <select class="select" id="simTo">
              <option value="+18145550001">Detailer (To: +18145550001)</option>
              <option value="+18145550002">Mechanic (To: +18145550002)</option>
              <option value="__custom__">Custom number...</option>
            </select>
            <div id="simToCustomWrap" class="col hidden" style="gap:6px; margin-top:8px;">
              <input class="input" id="simToCustom" type="text" placeholder="+18145550003" />
              <div id="simToCustomError" class="p hidden" style="color:#d97373; margin:0;">Enter a valid E.164 US number (+1XXXXXXXXXX).</div>
            </div>
          </div>

          <div class="col">
            <label class="p">Event type</label>
            <select class="select" id="simType">
              <option value="missed-call">Missed Call</option>
              <option value="sms">Inbound SMS</option>
            </select>
          </div>
        </div>

        <div style="height:10px;"></div>

        <div class="grid2">
          <div class="col">
            <label class="p">From (customer number)</label>
            <input class="input" id="simFrom" value="+18145559999" />
          </div>

          <div class="col" id="bodyWrap">
            <label class="p">Body (text message)</label>
            <input class="input" id="simBody" value="I want a detail on my truck" />
          </div>
        </div>

        <div style="height:10px;"></div>

        <div class="row" style="justify-content:flex-end;">
          <button class="btn" id="simClear" type="button">Clear</button>
          <button class="btn primary" id="simSend" type="button">Send</button>
        </div>

        <div style="height:12px;"></div>

        <div class="p">Response</div>
        <pre id="simOut" class="developer-output"></pre>
      </div>
    </section>
  `;

  wrap.appendChild(card);

  setTimeout(async () => {
    const SETTINGS_TAB_KEY = "mc_settings_tab_v1";
    const panelEls = Array.from(wrap.querySelectorAll("[data-settings-panel]"));
    const tabEls = Array.from(wrap.querySelectorAll("[data-settings-tab]"));
    const breadcrumbEl = document.getElementById("settingsBreadcrumb");
    const settingsLockedGateEl = document.getElementById("settingsLockedGate");
    const settingsLockedGateUnlockBtn = document.getElementById("settingsLockedGateUnlockBtn");
    const settingsSubnavEl = document.getElementById("settingsSubnav");
    const settingsMainHeadEl = wrap.querySelector(".settings-main-head");
    let complianceBaseline = "";
    let devBaseline = "";
    const settingsPanelLoadState = Object.create(null);
    const settingsPanelLoaders = Object.create(null);
    const getCurrentSettingsPanelId = () => String(localStorage.getItem(SETTINGS_TAB_ROUTE_KEY) || localStorage.getItem("mc_settings_tab_v1") || "profile").trim();
    const createSettingsScope = (panelId = "") => createUiScopeSnapshot({
      view: "settings",
      to: getActiveTo(),
      panel: String(panelId || getCurrentSettingsPanelId()).trim(),
      renderId: _renderId
    });

    async function ensureSettingsPanelHydrated(panelId) {
      const id = String(panelId || "").trim();
      const loader = settingsPanelLoaders[id];
      if (typeof loader !== "function") return;
      if (settingsPanelLoadState[id] === "loaded") return;
      if (settingsPanelLoadState[id] === "loading") return;
      settingsPanelLoadState[id] = "loading";
      const panelScope = createSettingsScope(id);
      try {
        await loader();
        if (!isUiScopeCurrent(panelScope, { element: wrap })) {
          settingsPanelLoadState[id] = "";
          return;
        }
        settingsPanelLoadState[id] = "loaded";
        normalizeSettingsSurface();
      } catch (err) {
        if (!isUiScopeCurrent(panelScope, { element: wrap })) {
          settingsPanelLoadState[id] = "";
          return;
        }
        settingsPanelLoadState[id] = "error";
        showSettingsToast(err?.message || `Failed to load ${id} settings`, true);
      }
    }
    const sectionMeta = {
      profile: { title: "Profile", subtitle: "Personal account settings and preferences." },
      workspace: { title: "Pricing", subtitle: "" },
      automations: { title: "Automations", subtitle: "Automation defaults and behavior controls." },
      schedule: { title: "Schedule", subtitle: "Calendar setup, import options, and scheduling actions." },
      integrations: { title: "Integrations", subtitle: "Third-party platform connections." },
      "customer-billing": { title: "Customer Billing", subtitle: "Customer billing and recovered revenue visibility." },
      email: { title: "Email", subtitle: "Passcode-gated campaign tools for booked customer email outreach." },
      "call-routing": { title: "Call Routing", subtitle: "Carrier forwarding setup from business line to Relay number." },
      admin: { title: "Admin", subtitle: "Passcode-gated controls for protected account actions." },
      billing: { title: "Subscription", subtitle: "Relay subscription, invoices, and payment methods." },
      developer: { title: "Developer", subtitle: "" }
    };

    function ensurePanelHeaders() {
      panelEls.forEach((panel) => {
        const id = panel.getAttribute("data-settings-panel");
        if (id === "workspace") return;
        const meta = sectionMeta[id];
        if (!meta) return;
        if (panel.querySelector(".settings-panel-head")) return;
        const header = document.createElement("div");
        header.className = "settings-panel-head";
        header.innerHTML = `
          <div class="h1 settings-panel-title">${escapeHtml(meta.title)}</div>
          ${meta.subtitle ? `<div class="p settings-panel-subtitle">${escapeHtml(meta.subtitle)}</div>` : ""}
        `;
        panel.insertBefore(header, panel.firstChild);
      });
    }
    ensurePanelHeaders();

    function inferSettingsStatusTone(text) {
      const value = String(text || "").trim().toLowerCase();
      if (!value) return "is-hidden";
      if (/fail|error|invalid|required|blocked|unable|expired|different user|must start|not granted|not supported/.test(value)) return "is-error";
      if (/demo|locked|warning|needs attention|no .*available|select a valid|confirm irreversible|not configured|no workspace selected/.test(value)) return "is-warning";
      if (/saving|searching|checking|creating|uploading|removing|simulating|refreshing|importing|purchasing|loading|syncing/.test(value)) return "is-pending";
      if (/saved|complete|connected|enabled|verified|ready|unlocked|updated|created|copied|passed|generated|purchased|signed out|disconnected|reset|imported/.test(value)) return "is-success";
      return "is-muted";
    }

    function applySettingsStatusTone(el) {
      if (!el) return;
      const tone = inferSettingsStatusTone(el.textContent);
      el.classList.add("settings-status-line");
      el.classList.remove("is-success", "is-error", "is-warning", "is-pending", "is-muted", "is-hidden");
      el.classList.add(tone);
    }

    function watchSettingsStatus(el, onSuccess) {
      if (!el) return;
      el.__settingsStatusSuccessHandlers = Array.isArray(el.__settingsStatusSuccessHandlers)
        ? el.__settingsStatusSuccessHandlers
        : [];
      if (typeof onSuccess === "function" && !el.__settingsStatusSuccessHandlers.includes(onSuccess)) {
        el.__settingsStatusSuccessHandlers.push(onSuccess);
      }
      if (el.__settingsStatusObserved === true) {
        applySettingsStatusTone(el);
        return;
      }
      el.__settingsStatusObserved = true;
      let lastText = "";
      const refresh = () => {
        const nextText = String(el.textContent || "").trim();
        applySettingsStatusTone(el);
        if (nextText && nextText !== lastText && inferSettingsStatusTone(nextText) === "is-success") {
          el.__settingsStatusSuccessHandlers.forEach((handler) => {
            try { handler(); } catch {}
          });
        }
        lastText = nextText;
      };
      refresh();
      const observer = new MutationObserver(refresh);
      observer.observe(el, { childList: true, characterData: true, subtree: true });
    }

    function decorateActionZone(zone, options = {}) {
      if (!zone) return null;
      zone.classList.add("settings-action-zone");
      if (options.sticky) zone.classList.add("is-sticky");
      if (options.destructive) zone.classList.add("settings-destructive-zone");
      return zone;
    }

    function decorateActionZoneByButtonId(buttonId, options = {}) {
      const btn = document.getElementById(buttonId);
      if (!btn) return { button: null, zone: null };
      const zone = decorateActionZone(btn.closest(".row, .pricing-card-footer, .billing-card-foot, .developer-actions"), options);
      return { button: btn, zone };
    }

    function mirrorDirtyButtonToZone(button, zone) {
      if (!button || !zone) return;
      if (button.__settingsDirtyObserved === true) return;
      button.__settingsDirtyObserved = true;
      const sync = () => {
        const dirty = button.classList.contains("is-dirty");
        zone.classList.toggle("is-dirty", dirty);
      };
      sync();
      const observer = new MutationObserver(sync);
      observer.observe(button, { attributes: true, attributeFilter: ["class", "disabled"] });
    }

    function bindVisualDirtyState({ controls, button, zone, statusEl, snapshot }) {
      if (!controls?.length || typeof snapshot !== "function") return;
      let baseline = snapshot();
      const update = () => {
        const dirty = snapshot() !== baseline;
        if (zone) zone.classList.toggle("is-dirty", dirty);
        if (button) button.classList.toggle("is-dirty", dirty);
      };
      controls.forEach((control) => {
        if (!control) return;
        control.addEventListener("input", update);
        control.addEventListener("change", update);
      });
      watchSettingsStatus(statusEl, () => {
        baseline = snapshot();
        update();
      });
      update();
      return {
        resetBaseline() {
          baseline = snapshot();
          update();
        },
        update
      };
    }

    function markReadonlyAndDemoControls(scope) {
      scope.querySelectorAll('input[readonly], textarea[readonly], select[disabled], input:disabled, textarea:disabled').forEach((el) => {
        el.classList.add("settings-field-readonly");
      });
      scope.querySelectorAll('button[disabled], .btn.disabled').forEach((el) => {
        el.classList.add("settings-control-disabled");
      });
      scope.querySelectorAll('button[title*="Demo mode"], .badge').forEach((el) => {
        if (/demo/i.test(String(el.textContent || "")) || /demo mode/i.test(String(el.getAttribute("title") || ""))) {
          el.classList.add("settings-demo-badge");
        }
      });
    }

    function normalizeSettingsSurface() {
      settingsLockedGateEl?.classList.add("settings-locked-card", "settings-surface-card");
      wrap.querySelectorAll(".settings-panel .card, .settings-panel .billing-card, .settings-panel-developer > .card").forEach((cardEl) => {
        cardEl.classList.add("settings-surface-card");
      });

      wrap.querySelectorAll(".settings-panel .card, .settings-panel .billing-card, .settings-panel .pricing-card, .settings-panel .developer-admin-card").forEach((cardEl) => {
        const titleEl = cardEl.querySelector(":scope > .h1, :scope > .pricing-card-head .h1, :scope > .row .h1");
        if (titleEl && !titleEl.closest(".settings-panel-head")) {
          titleEl.classList.add("settings-section-title");
          const introHost = titleEl.closest(".pricing-card-head, .row") || cardEl;
          const helpEl = introHost.querySelector(":scope > .p, :scope .col > .p, :scope > div > .p");
          if (helpEl) helpEl.classList.add("settings-help-text");
          if (introHost === cardEl && !titleEl.parentElement.classList.contains("settings-section-intro")) {
            const intro = document.createElement("div");
            intro.className = "settings-section-intro";
            cardEl.insertBefore(intro, titleEl);
            intro.appendChild(titleEl);
            if (helpEl && helpEl.parentElement === cardEl) intro.appendChild(helpEl);
          } else if (introHost !== cardEl) {
            introHost.classList.add("settings-section-meta");
            const copyBlock = titleEl.closest(".col");
            if (copyBlock) copyBlock.classList.add("settings-section-intro");
          }
        }
      });

      wrap.querySelectorAll(".settings-panel .p").forEach((el) => {
        if (el.id && /status/i.test(el.id)) return;
        if (el.closest(".settings-panel-head")) return;
        if (el.closest(".settings-toast")) return;
        if (el.closest(".settings-section-intro") || el.closest(".settings-section-meta")) {
          el.classList.add("settings-help-text");
          return;
        }
        const text = String(el.textContent || "").trim();
        if (!text) return;
        if (/tip:|saved on this device|optional|local only|per-tenant|quiet hours|configure|manage|used for|owner|admin/i.test(text)) {
          el.classList.add("settings-inline-note");
        }
      });

      [
        ["saveBusinessNameBtn", { sticky: true }],
        ["workspaceSaveHoursBtn", {}],
        ["workspaceResetUserBtn", {}],
        ["profileSaveBtn", {}],
        ["workspaceSaveTimezoneBtn", {}],
        ["saveNotifSettingsBtn", { sticky: true }],
        ["workspaceSaveDefaultsBtn", {}],
        ["workspaceSavePricingBtn", { sticky: true }],
        ["emailSendCampaignBtn", { sticky: true }],
        ["callRoutingSaveBtn", {}],
        ["billingDetailsSaveBtn", { sticky: true }],
        ["devSettingsSaveBtn", {}],
        ["simulateConversationBtn", {}],
        ["workspaceSaveNumbersBtn", {}],
        ["adminQuickCreateBtn", {}],
        ["adminResetPasswordBtn", {}],
        ["superadminPlatformStripeSaveBtn", {}],
        ["superadminPlatformStripeTestBtn", {}],
        ["superadminPlatformStripeDisconnectBtn", { destructive: true }],
        ["superadminNumberSearchBtn", {}],
        ["superadminTwilioSaveBtn", {}],
        ["superadminTwilioTestBtn", {}],
        ["superadminTwilioDisconnectBtn", { destructive: true }]
      ].forEach(([buttonId, options]) => decorateActionZoneByButtonId(buttonId, options));

      wrap.querySelectorAll(".billing-table-wrap, .team-table-wrap, .revenue-table-wrap").forEach((el) => {
        el.classList.add("ops-table-wrap");
      });
      wrap.querySelectorAll(".billing-table, .team-table, .revenue-table").forEach((el) => {
        el.classList.add("ops-table");
      });
      [document.getElementById("adminAccountsTableBody")?.closest("table")].filter(Boolean).forEach((el) => {
        el.classList.add("ops-table");
      });
      [document.getElementById("adminAccountsTableBody")?.closest("table")?.parentElement].filter(Boolean).forEach((el) => {
        el.classList.add("ops-table-wrap");
      });
      wrap.querySelectorAll(".integrations-log-list, #customerBillingSignals").forEach((el) => {
        el.classList.add("ops-log-list");
      });
      wrap.querySelectorAll(".billing-status-pill, .team-status, .risk-badge, .stage-badge").forEach((el) => {
        el.classList.add("ops-status-chip");
      });

      const destructiveRows = [
        document.getElementById("integrationDisconnectConfirmBtn")?.closest(".row"),
        document.getElementById("billingDueCtaBtn")?.closest(".row"),
        document.getElementById("billingCancelBtn")?.closest(".billing-card-foot"),
        document.getElementById("teamDeleteWorkspaceBtn")?.closest(".team-danger-card")
      ];
      destructiveRows.forEach((rowEl) => {
        if (rowEl) rowEl.classList.add("settings-destructive-zone");
      });

      [
        "adminPasscodeStatus",
        "saveBusinessNameStatus",
        "workspaceResetUserStatus",
        "profileSaveStatus",
        "workspacePricingStatus",
        "notifSettingsStatus",
        "billingDetailsStatus",
        "billingStripeConnectionStatus",
        "devSettingsStatus",
        "simulateConversationStatus",
        "workspaceNumbersStatus",
        "adminQuickCreateStatus",
        "adminResetPasswordStatus",
        "superadminPlatformStripeActionStatus",
        "superadminNumberSearchStatus",
        "superadminTwilioActionStatus",
        "icsTestStatus",
        "twilioTestStatus",
        "stripeTestStatus",
        "teamInviteError",
        "teamBulkInviteStatus",
        "teamIpError",
        "teamDomainError",
        "teamAddMemberStatus"
      ].forEach((id) => watchSettingsStatus(document.getElementById(id)));

      markReadonlyAndDemoControls(wrap);

      ["adminPasscodeModal", "integrationDisconnectModal", "billingPlanModal", "billingInfoModal", "billingConfirmModal", "teamActionModal", "teamAddMemberModal"].forEach((id) => {
        const overlay = document.getElementById(id);
        const modalEl = overlay?.querySelector(".billing-modal, .team-modal");
        if (!modalEl) return;
        modalEl.classList.add("settings-modal");
        const footerRow = modalEl.querySelector(".row:last-child");
        const firstTitle = modalEl.querySelector(".h1");
        if (firstTitle) {
          const headWrap = firstTitle.closest(".row") || firstTitle.parentElement;
          headWrap?.classList.add("settings-modal-head");
        }
        if (footerRow) footerRow.classList.add("settings-modal-foot");
      });
    }

    normalizeSettingsSurface();

    const validPanelIds = new Set(panelEls.map((el) => el.getAttribute("data-settings-panel")));
    function showSettingsPanel(panelId) {
      if (adminAccessState.unlocked !== true) {
        panelEls.forEach((el) => { el.style.display = "none"; });
        tabEls.forEach((el) => {
          el.classList.remove("active");
          el.setAttribute("aria-selected", "false");
        });
        return;
      }
      let requestedId = (!canAccessDeveloperTools() && panelId === "developer") ? "workspace" : panelId;
      if (requestedId === "integrations") requestedId = "billing";
      if (requestedId === "team") requestedId = "admin";
      if (requestedId === "notifications") requestedId = "profile";
      if (!canAccessWorkspaceAdmin() && (requestedId === "call-routing" || requestedId === "customer-billing" || requestedId === "billing")) requestedId = "workspace";
      const resolvedPanelId = validPanelIds.has(requestedId) ? requestedId : "workspace";
      panelEls.forEach((el) => {
        el.style.display = el.getAttribute("data-settings-panel") === resolvedPanelId ? "" : "none";
      });
      tabEls.forEach((el) => {
        const active = el.getAttribute("data-settings-tab") === resolvedPanelId;
        el.classList.toggle("active", active);
        el.setAttribute("aria-selected", active ? "true" : "false");
      });
      if (breadcrumbEl) {
        const title = sectionMeta[resolvedPanelId]?.title || "Settings";
        breadcrumbEl.textContent = `Settings / ${title}`;
      }
      localStorage.setItem(SETTINGS_TAB_KEY, resolvedPanelId);
      if (typeof state === "object") {
        state.view = "settings";
        syncUrlWithState({ replace: false });
      }
      void ensureSettingsPanelHydrated(resolvedPanelId);
    }

    const ADMIN_UNLOCK_KEY_PREFIX = "mc_admin_unlock_v1";
    const adminModal = document.getElementById("adminPasscodeModal");
    const adminSubtitleEl = document.getElementById("adminPasscodeSubtitle");
    const adminStatusEl = document.getElementById("adminPasscodeStatus");
    const adminInputLabelEl = document.getElementById("adminPasscodeInputLabel");
    const adminCurrentWrapEl = document.getElementById("adminCurrentPasscodeWrap");
    const adminCurrentInputEl = document.getElementById("adminCurrentPasscodeInput");
    const adminPasscodeInputEl = document.getElementById("adminPasscodeInput");
    const adminConfirmWrapEl = document.getElementById("adminConfirmPasscodeWrap");
    const adminConfirmInputEl = document.getElementById("adminConfirmPasscodeInput");
    const adminGateStatusEl = document.getElementById("adminGateStatus");
    const adminLockBtn = document.getElementById("adminLockBtn");
    const adminUnlockBtn = document.getElementById("adminUnlockBtn");
    const adminSubmitBtn = document.getElementById("adminPasscodeSubmitBtn");
    const adminCancelBtn = document.getElementById("adminPasscodeCancelBtn");
    const adminChangePasscodeBtn = document.getElementById("adminChangePasscodeBtn");
    const adminTabBtn = wrap.querySelector('[data-settings-tab="admin"]');
    const workspaceTabBtn = wrap.querySelector('[data-settings-tab="workspace"]');
    const emailTabBtn = wrap.querySelector('[data-settings-tab="email"]');
    const callRoutingTabBtn = wrap.querySelector('[data-settings-tab="call-routing"]');
    const customerBillingTabBtn = wrap.querySelector('[data-settings-tab="customer-billing"]');
    const billingTabBtn = wrap.querySelector('[data-settings-tab="billing"]');

    const adminAccessState = {
      checked: false,
      configured: false,
      unlocked: false,
      mode: "verify",
      pendingPanel: ""
    };

    function getActiveAccountIdForAdminGate() {
      const to = String(getActiveTo() || "");
      const list = Array.isArray(authState?.accounts) ? authState.accounts : [];
      const active = list.find((a) => String(a?.to || "") === to) || list.find((a) => String(a?.accountId || "").trim());
      return String(active?.accountId || "").trim();
    }

    function getAdminUnlockKey() {
      const accountId = getActiveAccountIdForAdminGate();
      return accountId ? `${ADMIN_UNLOCK_KEY_PREFIX}:${accountId}` : "";
    }

    function isAdminPasscodeFormat(value) {
      return /^\d{4,12}$/.test(String(value || "").trim());
    }

    function setAdminPasscodeStatus(msg, isError = false) {
      if (!adminStatusEl) return;
      adminStatusEl.textContent = String(msg || "");
      adminStatusEl.style.color = isError ? "var(--danger, #d97373)" : "";
    }

    function isAdminPasscodeFormValid() {
      const mode = String(adminAccessState.mode || "verify");
      const passcode = String(adminPasscodeInputEl?.value || "").trim();
      const confirm = String(adminConfirmInputEl?.value || "").trim();
      const current = String(adminCurrentInputEl?.value || "").trim();
      if (!isAdminPasscodeFormat(passcode)) return false;
      if (mode === "verify") return true;
      if (passcode !== confirm) return false;
      if (mode === "change" && !isAdminPasscodeFormat(current)) return false;
      return true;
    }

    function refreshAdminPasscodeSubmitState() {
      if (!adminSubmitBtn) return;
      adminSubmitBtn.disabled = !isAdminPasscodeFormValid();
    }

    function refreshAdminGateStatusUI() {
      if (!adminGateStatusEl) return;
      const configured = adminAccessState.configured === true;
      const unlocked = adminAccessState.unlocked === true;
      adminGateStatusEl.textContent = configured
        ? `Status: ${unlocked ? "Unlocked for this session" : "Locked"}`
        : "Status: Not configured";
      refreshBillingTabLockUI();
      applySettingsGlobalGate();
    }

    function applySettingsTabLockState(tabEl, label) {
      if (!tabEl) return;
      if (adminAccessState.unlocked === true) {
        tabEl.classList.remove("is-locked");
        tabEl.textContent = label;
        tabEl.setAttribute("aria-label", label);
        tabEl.title = "";
        return;
      }
      tabEl.classList.add("is-locked");
      tabEl.innerHTML = `${label} <span class="settings-tab-lock" aria-hidden="true">??</span>`;
      tabEl.setAttribute("aria-label", `${label} locked`);
      tabEl.title = "Locked: click to unlock admin access";
    }

    function refreshBillingTabLockUI() {
      applySettingsTabLockState(adminTabBtn, "Admin");
      applySettingsTabLockState(workspaceTabBtn, "Pricing");
      applySettingsTabLockState(emailTabBtn, "Email");
      applySettingsTabLockState(callRoutingTabBtn, "Call Routing");
      applySettingsTabLockState(customerBillingTabBtn, "Customer Billing");
      applySettingsTabLockState(billingTabBtn, "Subscription");
    }

    function applySettingsGlobalGate() {
      const unlocked = adminAccessState.unlocked === true;
      if (settingsLockedGateEl) settingsLockedGateEl.style.display = unlocked ? "none" : "";
      if (settingsSubnavEl) settingsSubnavEl.style.display = unlocked ? "" : "none";
      if (settingsMainHeadEl) settingsMainHeadEl.style.display = unlocked ? "" : "none";
      if (!unlocked) {
        panelEls.forEach((el) => { el.style.display = "none"; });
        tabEls.forEach((el) => {
          el.classList.remove("active");
          el.setAttribute("aria-selected", "false");
        });
      }
    }

    function closeAdminPasscodeModal() {
      if (!adminModal) return;
      adminModal.classList.add("hidden");
      adminModal.setAttribute("aria-hidden", "true");
      setAdminPasscodeStatus("");
      if (adminPasscodeInputEl) adminPasscodeInputEl.value = "";
      if (adminConfirmInputEl) adminConfirmInputEl.value = "";
      if (adminCurrentInputEl) adminCurrentInputEl.value = "";
      adminAccessState.pendingPanel = "";
    }

    function renderAdminPasscodeMode() {
      const verifyMode = adminAccessState.mode === "verify";
      const changeMode = adminAccessState.mode === "change";
      const setMode = adminAccessState.mode === "set";
      if (adminInputLabelEl) adminInputLabelEl.textContent = verifyMode ? "Passcode" : "New passcode";
      if (adminSubtitleEl) {
        adminSubtitleEl.textContent = verifyMode
          ? "Enter admin passcode to unlock protected actions for this account."
          : (setMode
              ? "No admin passcode is set yet. Create one now (4-12 digits)."
              : "Update this account's admin passcode.");
      }
      if (adminPasscodeInputEl) {
        adminPasscodeInputEl.placeholder = verifyMode ? "Enter 4-12 digit passcode" : "New 4-12 digit passcode";
      }
      if (adminCurrentWrapEl) adminCurrentWrapEl.style.display = changeMode ? "" : "none";
      if (adminConfirmWrapEl) adminConfirmWrapEl.style.display = verifyMode ? "none" : "";
      if (adminSubmitBtn) adminSubmitBtn.textContent = verifyMode ? "Unlock" : (setMode ? "Set passcode" : "Update passcode");
      refreshAdminPasscodeSubmitState();
    }

    function openAdminPasscodeModal(mode = "verify", pendingPanel = "") {
      if (!adminModal) return;
      adminAccessState.mode = mode;
      adminAccessState.pendingPanel = String(pendingPanel || "");
      renderAdminPasscodeMode();
      setAdminPasscodeStatus("");
      adminModal.classList.remove("hidden");
      adminModal.setAttribute("aria-hidden", "false");
      if (adminPasscodeInputEl) adminPasscodeInputEl.focus();
      setTimeout(refreshAdminPasscodeSubmitState, 0);
    }

    async function loadAdminAccessState() {
      try {
        const data = await apiGet("/api/account/admin-access");
        adminAccessState.checked = true;
        adminAccessState.configured = data?.configured === true;
        const unlockKey = getAdminUnlockKey();
        adminAccessState.unlocked = unlockKey ? sessionStorage.getItem(unlockKey) === "1" : false;
      } catch {
        adminAccessState.checked = true;
        adminAccessState.configured = false;
        adminAccessState.unlocked = false;
      }
      refreshAdminGateStatusUI();
    }

    async function ensureAdminUnlocked(context = "action", pendingPanel = "") {
      if (!adminAccessState.checked) await loadAdminAccessState();
      if (adminAccessState.unlocked) return true;
      const pending = String(pendingPanel || (context === "panel" ? "admin" : "")).trim();
      if (!adminAccessState.configured) {
        openAdminPasscodeModal("set", pending);
      } else {
        openAdminPasscodeModal("verify", pending);
      }
      return false;
    }

    tabEls.forEach((el) => {
      el.addEventListener("click", async () => {
        const panelId = el.getAttribute("data-settings-tab");
        if (!panelId) return;
        if (panelId === "admin" || panelId === "workspace" || panelId === "email" || panelId === "call-routing" || panelId === "customer-billing" || panelId === "billing") {
          const ok = await ensureAdminUnlocked("panel", panelId);
          if (!ok) return;
        }
        showSettingsPanel(panelId);
      });
    });

    adminCancelBtn?.addEventListener("click", () => {
      closeAdminPasscodeModal();
    });
    adminUnlockBtn?.addEventListener("click", async () => {
      await ensureAdminUnlocked("action");
      refreshAdminGateStatusUI();
    });
    adminLockBtn?.addEventListener("click", () => {
      adminAccessState.unlocked = false;
      const unlockKey = getAdminUnlockKey();
      if (unlockKey) sessionStorage.removeItem(unlockKey);
      refreshAdminGateStatusUI();
      showSettingsToast("Admin locked for this session.");
    });
    settingsLockedGateUnlockBtn?.addEventListener("click", async () => {
      await ensureAdminUnlocked("panel", String(localStorage.getItem(SETTINGS_TAB_KEY) || "profile"));
    });
    adminChangePasscodeBtn?.addEventListener("click", async () => {
      if (!adminAccessState.checked) await loadAdminAccessState();
      openAdminPasscodeModal(adminAccessState.configured ? "change" : "set", "");
    });
    adminModal?.addEventListener("click", (e) => {
      if (e.target === adminModal) closeAdminPasscodeModal();
    });
    [adminCurrentInputEl, adminPasscodeInputEl, adminConfirmInputEl].forEach((el) => {
      el?.addEventListener("input", refreshAdminPasscodeSubmitState);
      el?.addEventListener("change", refreshAdminPasscodeSubmitState);
    });
    adminSubmitBtn?.addEventListener("click", async () => {
      if (!isAdminPasscodeFormValid()) {
        refreshAdminPasscodeSubmitState();
        return;
      }
      const mode = adminAccessState.mode;
      const passcode = String(adminPasscodeInputEl?.value || "").trim();
      const confirm = String(adminConfirmInputEl?.value || "").trim();
      const current = String(adminCurrentInputEl?.value || "").trim();
      if (!isAdminPasscodeFormat(passcode)) {
        setAdminPasscodeStatus("Passcode must be 4-12 digits.", true);
        return;
      }
      if (mode !== "verify" && passcode !== confirm) {
        setAdminPasscodeStatus("New passcode and confirmation do not match.", true);
        return;
      }
      try {
        if (adminSubmitBtn) adminSubmitBtn.disabled = true;
        if (mode === "verify") {
          await apiPost("/api/account/admin-access/verify", { passcode });
        } else {
          await apiPut("/api/account/admin-access/passcode", {
            newPasscode: passcode,
            currentPasscode: mode === "change" ? current : undefined
          });
        }
        adminAccessState.checked = true;
        adminAccessState.configured = true;
        adminAccessState.unlocked = true;
        const unlockKey = getAdminUnlockKey();
        if (unlockKey) sessionStorage.setItem(unlockKey, "1");
        refreshAdminGateStatusUI();
        const nextPanel = adminAccessState.pendingPanel;
        closeAdminPasscodeModal();
        if (nextPanel) showSettingsPanel(nextPanel);
        showSettingsToast(mode === "verify" ? "Admin unlocked." : "Admin passcode saved.");
      } catch (err) {
        setAdminPasscodeStatus(err?.message || "Admin passcode check failed.", true);
      } finally {
        refreshAdminPasscodeSubmitState();
      }
    });

    const protectedSettingsButtonIds = new Set([
      "saveBusinessNameBtn",
      "workspaceSaveHoursBtn",
      "workspaceSaveTimezoneBtn",
      "workspaceSaveDefaultsBtn",
      "workspaceSavePricingBtn",
      "workspaceSendResetCodeBtn",
      "workspaceVerifyResetCodeBtn",
      "workspaceResetUserBtn",
      "billingDetailsSaveBtn",
      "billingPortalBtn",
      "billingUpdatePaymentBtn",
      "billingStripeConnectBtn",
      "billingStripeRefreshBtn",
      "billingCompareBtn",
      "billingConfirmActionBtn",
      "billingDueCtaBtn",
      "emailSendCampaignBtn",
      "callRoutingSaveBtn",
      "callRoutingMarkCompleteBtn",
      "callRoutingRunTestBtn"
    ]);
    wrap.addEventListener("click", async (event) => {
      const btn = event.target?.closest?.("button[id]");
      if (!btn) return;
      const id = String(btn.id || "");
      if (!protectedSettingsButtonIds.has(id)) return;
      if (adminAccessState.unlocked) return;
      event.preventDefault();
      event.stopImmediatePropagation();
      await ensureAdminUnlocked("action");
    }, true);

    const settingsOpenScheduleViewBtn = document.getElementById("settingsOpenScheduleViewBtn");
    settingsOpenScheduleViewBtn?.addEventListener("click", () => {
      state.view = "schedule";
      if (typeof render === "function") render();
    });

    const profileDisplayNameInput = document.getElementById("profileDisplayNameInput");
    const profileEmailInput = document.getElementById("profileEmailInput");
    const profileRoleInput = document.getElementById("profileRoleInput");
    const profileDefaultViewSelect = document.getElementById("profileDefaultViewSelect");
    const profileShortcutHintSelect = document.getElementById("profileShortcutHintSelect");
    const profileSaveBtn = document.getElementById("profileSaveBtn");
    const profileSaveStatus = document.getElementById("profileSaveStatus");
    const profileActionZone = profileSaveBtn?.closest(".row");

    function loadProfilePrefs() {
      const saved = loadLS(PROFILE_PREFS_KEY, {});
      return {
        displayName: String(saved?.displayName || "").trim(),
        defaultView: NAV_VIEWS.has(String(saved?.defaultView || "").toLowerCase()) ? String(saved.defaultView).toLowerCase() : "home",
        shortcutHint: ["auto", "mac", "windows"].includes(String(saved?.shortcutHint || "").toLowerCase()) ? String(saved.shortcutHint).toLowerCase() : "auto"
      };
    }

    function applyProfilePrefsToUI() {
      const sess = getSession();
      const prefs = loadProfilePrefs();
      const email = String(sess?.email || "").trim();
      const roleRaw = String(sess?.role || "user").trim();
      const role = roleRaw ? roleRaw.charAt(0).toUpperCase() + roleRaw.slice(1) : "User";
      const fallbackName = (() => {
        const explicit = String(sess?.name || "").trim();
        if (explicit) return explicit;
        if (!email) return "Account";
        const local = email.split("@")[0] || "Account";
        return local
          .replace(/[._-]+/g, " ")
          .split(" ")
          .filter(Boolean)
          .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
          .join(" ");
      })();
      const displayName = prefs.displayName || fallbackName;
      if (profileDisplayNameInput) profileDisplayNameInput.value = displayName;
      if (profileEmailInput) profileEmailInput.value = email;
      if (profileRoleInput) profileRoleInput.value = role;
      if (profileDefaultViewSelect) profileDefaultViewSelect.value = prefs.defaultView;
      if (profileShortcutHintSelect) profileShortcutHintSelect.value = prefs.shortcutHint;
      if (profileSaveStatus) profileSaveStatus.textContent = "";
      if (authState.user) authState.user.name = displayName;
    }

    const profileDirtyBinding = bindVisualDirtyState({
      controls: [profileDisplayNameInput, profileDefaultViewSelect, profileShortcutHintSelect].filter(Boolean),
      button: profileSaveBtn,
      zone: profileActionZone,
      statusEl: profileSaveStatus,
      snapshot: () => JSON.stringify({
        displayName: String(profileDisplayNameInput?.value || "").trim(),
        defaultView: String(profileDefaultViewSelect?.value || "").trim(),
        shortcutHint: String(profileShortcutHintSelect?.value || "").trim()
      })
    });

    function applyProfileShortcutHintPreference() {
      const hintEl = document.getElementById("topbarSearchHint");
      if (!hintEl) return;
      const prefs = loadProfilePrefs();
      const forced = String(prefs.shortcutHint || "auto");
      const isMacLike = /Mac|iPhone|iPad|iPod/i.test(navigator.platform || "");
      const showMac = forced === "mac" ? true : forced === "windows" ? false : isMacLike;
      hintEl.textContent = showMac ? "Cmd K" : "Ctrl K";
    }

    profileSaveBtn?.addEventListener("click", () => {
      const current = loadProfilePrefs();
      const next = {
        ...current,
        displayName: String(profileDisplayNameInput?.value || "").trim(),
        defaultView: NAV_VIEWS.has(String(profileDefaultViewSelect?.value || "").toLowerCase()) ? String(profileDefaultViewSelect.value).toLowerCase() : current.defaultView,
        shortcutHint: ["auto", "mac", "windows"].includes(String(profileShortcutHintSelect?.value || "").toLowerCase()) ? String(profileShortcutHintSelect.value).toLowerCase() : current.shortcutHint
      };
      saveLS(PROFILE_PREFS_KEY, next);
      if (authState.user) {
        authState.user.name = next.displayName || authState.user.name || "";
      }
      applyProfileShortcutHintPreference();
      window.dispatchEvent(new CustomEvent("relay:auth-role-changed"));
      if (profileSaveStatus) profileSaveStatus.textContent = "Saved";
      showSettingsToast("Profile saved");
    });

    const devTabBtn = wrap.querySelector('[data-settings-tab="developer"]');
    const devPanel = wrap.querySelector('[data-settings-panel="developer"]');
    const adminSettingsTabBtn = wrap.querySelector('[data-settings-tab="admin"]');
    const adminSettingsPanelEl = wrap.querySelector('[data-settings-panel="admin"]');
    const callRoutingSettingsTabBtn = wrap.querySelector('[data-settings-tab="call-routing"]');
    const callRoutingSettingsPanelEl = wrap.querySelector('[data-settings-panel="call-routing"]');
    const customerBillingSettingsTabBtn = wrap.querySelector('[data-settings-tab="customer-billing"]');
    const customerBillingSettingsPanelEl = wrap.querySelector('[data-settings-panel="customer-billing"]');
    const subscriptionSettingsTabBtn = wrap.querySelector('[data-settings-tab="billing"]');
    const subscriptionSettingsPanelEl = wrap.querySelector('[data-settings-panel="billing"]');
    if (!canAccessDeveloperTools()) {
      if (devTabBtn) devTabBtn.style.display = "none";
      if (devPanel) devPanel.style.display = "none";
      if (localStorage.getItem(SETTINGS_TAB_KEY) === "developer") {
        localStorage.setItem(SETTINGS_TAB_KEY, "workspace");
      }
    }
    if (!canAccessWorkspaceAdmin()) {
      if (adminSettingsTabBtn) adminSettingsTabBtn.style.display = "none";
      if (adminSettingsPanelEl) adminSettingsPanelEl.style.display = "none";
      if (callRoutingSettingsTabBtn) callRoutingSettingsTabBtn.style.display = "none";
      if (callRoutingSettingsPanelEl) callRoutingSettingsPanelEl.style.display = "none";
      if (customerBillingSettingsTabBtn) customerBillingSettingsTabBtn.style.display = "none";
      if (customerBillingSettingsPanelEl) customerBillingSettingsPanelEl.style.display = "none";
      if (subscriptionSettingsTabBtn) subscriptionSettingsTabBtn.style.display = "none";
      if (subscriptionSettingsPanelEl) subscriptionSettingsPanelEl.style.display = "none";
      if (localStorage.getItem(SETTINGS_TAB_KEY) === "admin" || localStorage.getItem(SETTINGS_TAB_KEY) === "team") {
        localStorage.setItem(SETTINGS_TAB_KEY, "workspace");
      }
      if (localStorage.getItem(SETTINGS_TAB_KEY) === "call-routing" || localStorage.getItem(SETTINGS_TAB_KEY) === "customer-billing" || localStorage.getItem(SETTINGS_TAB_KEY) === "billing") {
        localStorage.setItem(SETTINGS_TAB_KEY, "workspace");
      }
    }

    if (localStorage.getItem(SETTINGS_TAB_KEY) === "team") {
      localStorage.setItem(SETTINGS_TAB_KEY, "admin");
    }
    if (localStorage.getItem(SETTINGS_TAB_KEY) === "messaging") {
      localStorage.setItem(SETTINGS_TAB_KEY, "profile");
    }
    await loadAdminAccessState();
    applySettingsGlobalGate();
    const initialPanel = localStorage.getItem(SETTINGS_TAB_KEY) || "workspace";
    if ((initialPanel === "admin" || initialPanel === "workspace" || initialPanel === "email" || initialPanel === "call-routing" || initialPanel === "customer-billing" || initialPanel === "billing") && !adminAccessState.unlocked) {
      showSettingsPanel("profile");
    } else {
      showSettingsPanel(initialPanel);
    }
    if (!adminAccessState.unlocked) {
      const pending = String(initialPanel || "profile").trim();
      setTimeout(() => {
        ensureAdminUnlocked("panel", pending);
      }, 0);
    }

    const customerBillingRecovered = document.getElementById("customerBillingRecovered");
    const customerBillingMonth = document.getElementById("customerBillingMonth");
    const customerBillingOpenOpps = document.getElementById("customerBillingOpenOpps");
    const customerBillingAtRisk = document.getElementById("customerBillingAtRisk");
    const customerBillingSignals = document.getElementById("customerBillingSignals");
    const customerBillingInvoiceSummary = document.getElementById("customerBillingInvoiceSummary");
    const customerBillingInvoicesBody = document.getElementById("customerBillingInvoicesBody");
    const customerBillingRefreshBtn = document.getElementById("customerBillingRefreshBtn");
    const customerBillingOpenRevenueBtn = document.getElementById("customerBillingOpenRevenueBtn");

    function renderCustomerBillingOverview(overview) {
      if (customerBillingRecovered) customerBillingRecovered.textContent = formatUsdAmount((Number(overview?.recoveredRevenueCents || 0) || 0) / 100);
      if (customerBillingMonth) customerBillingMonth.textContent = formatUsdAmount((Number(overview?.recoveredThisMonth || 0) || 0) / 100);
      if (customerBillingOpenOpps) customerBillingOpenOpps.textContent = String(Number(overview?.openOpportunities || 0));
      if (customerBillingAtRisk) customerBillingAtRisk.textContent = formatUsdAmount((Number(overview?.atRiskValueCents || 0) || 0) / 100);
      if (!customerBillingSignals) return;
      const rows = Array.isArray(overview?.revenueEvents) ? overview.revenueEvents : [];
      const filtered = rows
        .filter((row) => {
          const type = String(row?.type || "").toLowerCase();
          return type === "appointment_booked" || type === "sale_closed" || type === "opportunity_recovered";
        })
        .slice(0, 8);
      if (!filtered.length) {
        customerBillingSignals.innerHTML = `<div class="ops-empty-block">${RelayUI.renderEmptyState({ text: "No customer billing events yet.", className: "is-compact" })}</div>`;
        return;
      }
      customerBillingSignals.innerHTML = filtered.map((row) => {
        const type = String(row?.type || "event").replace(/_/g, " ");
        const status = String(row?.status || "").trim() || "recorded";
        const value = formatUsdAmount((Number(row?.estimatedValueCents || 0) || 0) / 100);
        const when = new Date(Number(row?.createdAt || Date.now())).toLocaleString();
        return `
          <div class="ops-log-item">
            <div class="ops-log-item-main">
              <span class="p">${escapeHtml(type)} | ${escapeHtml(status)} | ${escapeHtml(value)}</span>
            </div>
            <span class="ops-log-item-meta">${escapeHtml(when)}</span>
          </div>
        `;
      }).join("");
    }

    async function loadCustomerBillingOverview() {
      const [overviewResult, invoicesResult] = await Promise.allSettled([
        apiGet("/api/analytics/revenue-overview"),
        apiGet("/api/analytics/customer-invoices?limit=75")
      ]);

      if (overviewResult.status === "fulfilled") {
        renderCustomerBillingOverview(overviewResult.value || {});
      } else if (customerBillingSignals) {
        customerBillingSignals.innerHTML = `<div class="ops-empty-block">${RelayUI.renderEmptyState({ text: overviewResult.reason?.message || "Failed to load customer billing data.", className: "is-compact" })}</div>`;
      }

      if (invoicesResult.status === "fulfilled") {
        renderCustomerBillingInvoices(invoicesResult.value || {});
      } else {
        const fallbackPayload = await buildCustomerBillingInvoiceFallback().catch(() => null);
        if (fallbackPayload && Array.isArray(fallbackPayload.invoices)) {
          renderCustomerBillingInvoices(fallbackPayload);
        } else if (customerBillingInvoicesBody) {
          customerBillingInvoicesBody.innerHTML = `<tr><td colspan="8" class="billing-empty-row">${escapeHtml(invoicesResult.reason?.message || "Failed to load invoices.")}</td></tr>`;
        }
      }
    }

    async function buildCustomerBillingInvoiceFallback() {
      const to = getActiveTo();
      const res = await apiGet(`/api/conversations?to=${encodeURIComponent(to)}`);
      const threads = Array.isArray(res?.conversations) ? res.conversations : [];
      const invoices = [];
      for (const thread of threads) {
        const convo = thread || {};
        const status = String(convo?.status || "").toLowerCase();
        const stage = String(convo?.stage || "").toLowerCase();
        const bookedMs = getLatestBookedConfirmationTime(convo);
        const booked = status === "booked"
          || status === "closed"
          || /booked|appointment_booked|scheduled/.test(stage)
          || (Number.isFinite(bookedMs) && bookedMs > 0)
          || (Number.isFinite(Number(convo?.bookingTime || 0)) && Number(convo.bookingTime) > 0);
        if (!booked) continue;
        const amount = resolveConversationAmount(convo);
        if (!Number.isFinite(amount) || amount <= 0) continue;
        const ld = convo?.leadData || {};
        const method = normalizePaymentMethod(ld?.payment_method || ld?.paymentMethod || convo?.paymentMethod || convo?.payment_method) || "unknown";
        const paid = isPaidState(ld?.payment_status || ld?.paymentStatus || convo?.paymentStatus || convo?.payment_status);
        const paymentStatus = paid || method === "cash" ? "paid" : "open";
        const lifecycleStatus = status === "closed" ? "close" : "booked";
        const ts = Number(convo?.bookingTime || ld?.booking_time || bookedMs || convo?.updatedAt || convo?.lastActivityAt || Date.now()) || Date.now();
        const amountCents = Math.round(Number(amount) * 100);
        invoices.push({
          id: String(convo?.id || thread?.id || `inv_${ts}`),
          invoiceNumber: `INV-${String(ts).slice(-8)}`,
          contactName: String(convo?.contactName || ld?.customer_name || "Unknown"),
          phone: String(convo?.from || ""),
          email: String(ld?.customer_email || ld?.email || "").toLowerCase(),
          serviceItems: (() => {
            const items = [];
            items.push(...getServiceLabelsFromLeadData(ld));
            if (!items.length) items.push(...inferServiceLabelsFromMessages(Array.isArray(convo?.messages) ? convo.messages : []));
            if (!items.length) {
              const fallback = String(ld?.service || ld?.request || convo?.service || "").trim();
              if (fallback) items.push(humanizeServiceLabel(fallback));
            }
            return Array.from(new Set(items.map((x) => String(x || "").trim()).filter(Boolean))).slice(0, 8);
          })(),
          service: String(ld?.service || ld?.request || convo?.service || "").trim(),
          amountCents,
          status: lifecycleStatus,
          paymentStatus,
          paymentMethod: method,
          bookedAt: ts
        });
      }
      invoices.sort((a, b) => Number(b?.bookedAt || 0) - Number(a?.bookedAt || 0));
      const summary = {
        total: invoices.length,
        paidCount: invoices.filter((r) => r.status === "paid").length,
        openCount: invoices.filter((r) => r.status === "open").length,
        refundedCount: 0
      };
      return {
        invoices: invoices.slice(0, 75),
        summary
      };
    }

    function renderCustomerBillingInvoices(payload) {
      if (!customerBillingInvoicesBody) return;
      const rows = Array.isArray(payload?.invoices) ? payload.invoices : [];
      const summary = payload?.summary || {};
      const now = new Date();
      const currentYear = now.getFullYear();
      const currentMonth = now.getMonth();
      const normalizedRows = rows.map((row) => {
        const statusRaw = String(row?.status || "").trim().toLowerCase();
        const lifecycleStatus = statusRaw === "closed" ? "close" : statusRaw;
        return { ...row, _status: lifecycleStatus };
      });
      const recoveredRows = normalizedRows.filter((row) => row._status === "booked" || row._status === "close");
      const recoveredCents = recoveredRows.reduce((acc, row) => acc + Number(row?.amountCents || 0), 0);
      const bookedMonthCents = recoveredRows.reduce((acc, row) => {
        const ts = Number(row?.bookedAt || 0);
        if (!Number.isFinite(ts) || ts <= 0) return acc;
        const d = new Date(ts);
        if (d.getFullYear() === currentYear && d.getMonth() === currentMonth) {
          return acc + Number(row?.amountCents || 0);
        }
        return acc;
      }, 0);
      const openCountFromInvoices = normalizedRows.filter((row) => row._status === "open").length;
      if (customerBillingRecovered) customerBillingRecovered.textContent = formatUsdAmount(recoveredCents / 100);
      if (customerBillingMonth) customerBillingMonth.textContent = formatUsdAmount(bookedMonthCents / 100);
      if (customerBillingOpenOpps) customerBillingOpenOpps.textContent = String(openCountFromInvoices);
      if (customerBillingInvoiceSummary) {
        const total = Number(summary?.total || normalizedRows.length || 0);
        const paid = normalizedRows.filter((row) => row._status === "booked" || row._status === "close").length;
        const open = openCountFromInvoices;
        const refunded = Number(summary?.refundedCount || 0);
        customerBillingInvoiceSummary.textContent = `${total} invoices | ${paid} paid | ${open} open | ${refunded} refunded`;
      }
      if (!normalizedRows.length) {
        customerBillingInvoicesBody.innerHTML = `<tr><td colspan="8" class="billing-empty-row">No customer invoices yet.</td></tr>`;
        return;
      }
      customerBillingInvoicesBody.innerHTML = normalizedRows.map((row) => {
        const bookedAt = Number(row?.bookedAt || 0);
        const when = bookedAt ? new Date(bookedAt).toLocaleDateString() : "--";
        const invoiceNumber = String(row?.invoiceNumber || row?.id || "--");
        const customer = `${String(row?.contactName || "Unknown")} ${row?.phone ? `(${String(row.phone)})` : ""}`.trim();
        const serviceItems = Array.isArray(row?.serviceItems)
          ? row.serviceItems.map((x) => String(x || "").trim()).filter(Boolean)
          : [];
        const fallbackService = String(row?.service || "").trim();
        const renderedServiceList = serviceItems.length
          ? serviceItems
          : (fallbackService ? [fallbackService] : ["No services captured"]);
        const serviceHtml = renderedServiceList
          .slice(0, 8)
          .map((item) => `<div class="p">- ${escapeHtml(item)}</div>`)
          .join("");
        const method = String(row?.paymentMethod || "unknown");
        const status = String(row?._status || "open");
        const amount = formatUsdAmount((Number(row?.amountCents || 0) || 0) / 100);
        const invoiceId = String(row?.id || "").trim();
        const pdfHref = `${API_BASE}/api/analytics/customer-invoices/${encodeURIComponent(invoiceId)}/pdf?to=${encodeURIComponent(getActiveTo())}`;
        const pdfBtn = invoiceId
          ? `<a class="btn" href="${escapeAttr(pdfHref)}" target="_blank" rel="noopener">Download</a>`
          : `<span class="p">--</span>`;
        return `
          <tr>
            <td>${escapeHtml(when)}</td>
            <td>${escapeHtml(invoiceNumber)}</td>
            <td>${escapeHtml(customer)}</td>
            <td>${serviceHtml}</td>
            <td>${escapeHtml(method)}</td>
            <td>${escapeHtml(status)}</td>
            <td>${escapeHtml(amount)}</td>
            <td>${pdfBtn}</td>
          </tr>
        `;
      }).join("");
    }

    customerBillingRefreshBtn?.addEventListener("click", loadCustomerBillingOverview);
    customerBillingOpenRevenueBtn?.addEventListener("click", () => {
      state.view = "analytics";
      if (typeof render === "function") render();
    });
    settingsPanelLoaders["customer-billing"] = async () => {
      await loadCustomerBillingOverview();
    };

    const emailRecipientsSummary = document.getElementById("emailRecipientsSummary");
    const emailRecipientList = document.getElementById("emailRecipientList");
    const emailCampaignHistory = document.getElementById("emailCampaignHistory");
    const emailCampaignSubject = document.getElementById("emailCampaignSubject");
    const emailCampaignBody = document.getElementById("emailCampaignBody");
    const emailCampaignStatus = document.getElementById("emailCampaignStatus");
    const emailSelectedSummary = document.getElementById("emailSelectedSummary");
    const emailSelectAllRecipientsBtn = document.getElementById("emailSelectAllRecipientsBtn");
    const emailClearRecipientsBtn = document.getElementById("emailClearRecipientsBtn");
    const emailRefreshRecipientsBtn = document.getElementById("emailRefreshRecipientsBtn");
    const emailRefreshCampaignsBtn = document.getElementById("emailRefreshCampaignsBtn");
    const emailSendCampaignBtn = document.getElementById("emailSendCampaignBtn");
    const emailTemplateBtns = Array.from(wrap.querySelectorAll("[data-email-template]"));
    let emailRecipients = [];
    let emailCampaigns = [];
    let selectedEmailTemplateKey = "";
    const selectedEmailRecipients = new Set();

    function getEmailTemplateMap() {
      const bizName = String((state?.workspace?.name || state?.businessName || "our shop")).trim() || "our shop";
      return {
        holiday_offer: {
          subject: `${bizName}: Holiday detail special this week`,
          body: `Hi there,\n\nWe're running a limited holiday detail offer this week. If you want to lock in a spot, reply to this email and we'll send you priority booking options.\n\n- ${bizName}`
        },
        flash_sale: {
          subject: `${bizName}: 48-hour flash sale`,
          body: `Hi there,\n\nFor the next 48 hours, we're offering a flash deal on selected detailing packages. Reply and we'll match you with the best package for your vehicle.\n\n- ${bizName}`
        },
        maintenance_followup: {
          subject: `${bizName}: Time for your next maintenance detail`,
          body: `Hi there,\n\nIt's a good time to schedule your next maintenance detail. We can keep your vehicle protected and dialed in with a faster maintenance package.\n\nReply here and we'll get you booked.\n\n- ${bizName}`
        }
      };
    }

    function renderEmailRecipients() {
      if (!emailRecipientsSummary || !emailRecipientList) return;
      const count = Array.isArray(emailRecipients) ? emailRecipients.length : 0;
      const recipientEmails = new Set(emailRecipients.map((r) => String(r?.email || "").trim().toLowerCase()).filter(Boolean));
      Array.from(selectedEmailRecipients).forEach((email) => {
        if (!recipientEmails.has(email)) selectedEmailRecipients.delete(email);
      });
      emailRecipientsSummary.textContent = `${count} recipient${count === 1 ? "" : "s"} available from bookings and contacts`;
      if (!count) {
        emailRecipientList.innerHTML = `<div class="p">No booking emails captured yet.</div>`;
        if (emailSendCampaignBtn) emailSendCampaignBtn.disabled = true;
        if (emailSelectedSummary) emailSelectedSummary.textContent = "0 selected";
        return;
      }
      emailRecipientList.innerHTML = emailRecipients.map((row) => {
        const name = String(row?.name || "Unknown").trim() || "Unknown";
        const phone = String(row?.phone || "Unknown").trim() || "Unknown";
        const email = String(row?.email || "").trim().toLowerCase();
        const left = `${name} (${phone}) : ${email}`;
        const checked = selectedEmailRecipients.has(email) ? "checked" : "";
        return `
          <div class="row" style="justify-content:space-between; gap:10px; flex-wrap:wrap; border:1px solid var(--border); border-radius:10px; padding:8px 10px;">
            <label class="row" style="gap:10px; align-items:center; flex-wrap:wrap; cursor:pointer;">
              <input type="checkbox" data-email-recipient="${escapeAttr(email)}" ${checked} />
              <span class="p">${escapeHtml(left)}</span>
            </label>
          </div>
        `;
      }).join("");
      emailRecipientList.querySelectorAll("[data-email-recipient]").forEach((el) => {
        el.addEventListener("change", () => {
          const email = String(el.getAttribute("data-email-recipient") || "").trim().toLowerCase();
          if (!email) return;
          if (el.checked) selectedEmailRecipients.add(email);
          else selectedEmailRecipients.delete(email);
          syncEmailRecipientSelectionUI();
        });
      });
      syncEmailRecipientSelectionUI();
    }

    function syncEmailRecipientSelectionUI() {
      const selectedCount = selectedEmailRecipients.size;
      const totalCount = Array.isArray(emailRecipients) ? emailRecipients.length : 0;
      if (emailSelectedSummary) {
        emailSelectedSummary.textContent = `${selectedCount} selected`;
      }
      if (emailSendCampaignBtn) {
        if (!totalCount) {
          emailSendCampaignBtn.textContent = "Send campaign";
          emailSendCampaignBtn.disabled = true;
        } else if (selectedCount > 0) {
          emailSendCampaignBtn.textContent = `Send to selected (${selectedCount})`;
          emailSendCampaignBtn.disabled = false;
        } else {
          emailSendCampaignBtn.textContent = `Send to all (${totalCount})`;
          emailSendCampaignBtn.disabled = false;
        }
      }
    }

    function renderEmailCampaignHistory() {
      if (!emailCampaignHistory) return;
      const rows = Array.isArray(emailCampaigns) ? emailCampaigns : [];
      if (!rows.length) {
        emailCampaignHistory.innerHTML = `<div class="p">No campaigns sent yet.</div>`;
        return;
      }
      emailCampaignHistory.innerHTML = rows.map((item) => {
        const ts = Number(item?.ts || Date.now());
        const subject = String(item?.subject || "(No subject)").trim();
        const count = Number(item?.recipientCount || 0);
        const deliveredCount = Number(item?.deliveredCount || 0);
        const failedCount = Number(item?.failedCount || 0);
        const provider = String(item?.provider || "").trim();
        const status = String(item?.status || "sent").trim();
        return `
          <div class="row" style="justify-content:space-between; gap:10px; flex-wrap:wrap; border:1px solid var(--border); border-radius:10px; padding:8px 10px;">
            <span class="p">${escapeHtml(subject)} | ${count} recipients | ${deliveredCount} delivered${failedCount ? `, ${failedCount} failed` : ""}</span>
            <span class="p">${escapeHtml(provider ? `${provider} | ` : "")}${escapeHtml(status)} | ${new Date(ts).toLocaleString()}</span>
          </div>
        `;
      }).join("");
    }

    function setEmailCampaignStatus(msg, isError = false) {
      if (!emailCampaignStatus) return;
      emailCampaignStatus.textContent = String(msg || "");
      emailCampaignStatus.style.color = isError ? "var(--danger, #d97373)" : "";
    }

    async function loadEmailRecipients() {
      try {
        const res = await apiGet("/api/email/contacts");
        emailRecipients = Array.isArray(res?.recipients) ? res.recipients : [];
      } catch (err) {
        emailRecipients = [];
        setEmailCampaignStatus(err?.message || "Could not load email recipients.", true);
      }
      renderEmailRecipients();
    }

    async function loadEmailCampaignHistory() {
      try {
        const res = await apiGet("/api/email/campaigns?limit=20");
        emailCampaigns = Array.isArray(res?.campaigns) ? res.campaigns : [];
      } catch {
        emailCampaigns = [];
      }
      renderEmailCampaignHistory();
    }

    function applyEmailTemplate(templateKey) {
      const templates = getEmailTemplateMap();
      const tpl = templates[templateKey];
      if (!tpl) return;
      selectedEmailTemplateKey = String(templateKey || "");
      if (emailCampaignSubject) emailCampaignSubject.value = String(tpl.subject || "");
      if (emailCampaignBody) emailCampaignBody.value = String(tpl.body || "");
      setEmailCampaignStatus("Template applied.");
    }

    async function sendEmailCampaign() {
      const subject = String(emailCampaignSubject?.value || "").trim();
      const body = String(emailCampaignBody?.value || "").trim();
      if (!subject || !body) {
        setEmailCampaignStatus("Subject and message are required.", true);
        return;
      }
      const btn = emailSendCampaignBtn;
      const selectedEmails = Array.from(selectedEmailRecipients.values()).filter(Boolean);
      const originalLabel = btn?.textContent || "Send campaign";
      try {
        if (btn) {
          btn.disabled = true;
          btn.textContent = "Sending...";
        }
        const payload = {
          subject,
          body,
          templateKey: selectedEmailTemplateKey || undefined,
          recipientEmails: selectedEmails.length ? selectedEmails : undefined
        };
        const res = await apiPost("/api/email/campaigns/send", payload);
        const count = Number(res?.campaign?.recipientCount || 0);
        const delivered = Number(res?.campaign?.deliveredCount || 0);
        const failed = Number(res?.campaign?.failedCount || 0);
        if (failed > 0) setEmailCampaignStatus(`Campaign sent: ${delivered} delivered, ${failed} failed.`, true);
        else setEmailCampaignStatus(`Campaign sent to ${count} recipients.`);
        await loadEmailCampaignHistory();
      } catch (err) {
        setEmailCampaignStatus(err?.message || "Failed to send email campaign.", true);
      } finally {
        if (btn) {
          btn.disabled = false;
          btn.textContent = originalLabel;
        }
        syncEmailRecipientSelectionUI();
      }
    }

    emailTemplateBtns.forEach((btn) => {
      btn.addEventListener("click", () => {
        const key = String(btn.getAttribute("data-email-template") || "").trim();
        if (!key) return;
        applyEmailTemplate(key);
      });
    });
    emailSelectAllRecipientsBtn?.addEventListener("click", () => {
      selectedEmailRecipients.clear();
      emailRecipients.forEach((row) => {
        const email = String(row?.email || "").trim().toLowerCase();
        if (email) selectedEmailRecipients.add(email);
      });
      renderEmailRecipients();
    });
    emailClearRecipientsBtn?.addEventListener("click", () => {
      selectedEmailRecipients.clear();
      renderEmailRecipients();
    });
    emailRefreshRecipientsBtn?.addEventListener("click", loadEmailRecipients);
    emailRefreshCampaignsBtn?.addEventListener("click", loadEmailCampaignHistory);
    emailSendCampaignBtn?.addEventListener("click", sendEmailCampaign);
    settingsPanelLoaders.email = async () => {
      await loadEmailRecipients();
      await loadEmailCampaignHistory();
    };

    const callRoutingBusinessNumberInput = document.getElementById("callRoutingBusinessNumber");
    const callRoutingRelayNumberInput = document.getElementById("callRoutingRelayNumber");
    const callRoutingCarrierSelect = document.getElementById("callRoutingCarrier");
    const callRoutingEnableCodeInput = document.getElementById("callRoutingEnableCode");
    const callRoutingDisableCodeInput = document.getElementById("callRoutingDisableCode");
    const callRoutingStatusLine = document.getElementById("callRoutingStatusLine");
    const callRoutingStatusBadge = document.getElementById("callRoutingStatusBadge");
    const callRoutingStatus = document.getElementById("callRoutingStatus");
    const callRoutingCopyEnableBtn = document.getElementById("callRoutingCopyEnableBtn");
    const callRoutingCopyDisableBtn = document.getElementById("callRoutingCopyDisableBtn");
    const callRoutingMarkCompleteBtn = document.getElementById("callRoutingMarkCompleteBtn");
    const callRoutingRunTestBtn = document.getElementById("callRoutingRunTestBtn");
    const callRoutingSaveBtn = document.getElementById("callRoutingSaveBtn");

    function getCallRoutingScopeKey() {
      const active = resolveActiveWorkspace();
      const accountId = String(active?.accountId || "").trim();
      const to = String(active?.to || "").trim();
      return accountId || to || "default";
    }

    function getCallRoutingStorageKey() {
      return `mc_call_routing_v1:${getCallRoutingScopeKey()}`;
    }

    function getCarrierEnableCode(carrier) {
      void carrier;
      return "*72";
    }

    function getCarrierDisableCode(carrier) {
      void carrier;
      return "*73";
    }

    function setCallRoutingStatusText(text) {
      const value = String(text || "Not setup");
      if (callRoutingStatusLine) callRoutingStatusLine.textContent = value;
      if (callRoutingStatusBadge) callRoutingStatusBadge.textContent = value;
    }

    function setCallRoutingInlineStatus(msg, isError = false) {
      if (!callRoutingStatus) return;
      callRoutingStatus.textContent = String(msg || "");
      callRoutingStatus.style.color = isError ? "var(--danger, #d97373)" : "";
    }

    function readCallRoutingFromUi() {
      return {
        businessNumber: String(callRoutingBusinessNumberInput?.value || "").trim(),
        relayNumber: String(callRoutingRelayNumberInput?.value || "").trim(),
        carrier: String(callRoutingCarrierSelect?.value || "generic").trim(),
        enableCode: String(callRoutingEnableCodeInput?.value || getCarrierEnableCode("generic")).trim() || "*72",
        disableCode: String(callRoutingDisableCodeInput?.value || getCarrierDisableCode("generic")).trim() || "*73",
        status: String(callRoutingStatusLine?.textContent || "Not setup").trim(),
        updatedAt: Date.now()
      };
    }

    function applyCallRoutingToUi(data) {
      const relayNumber = String(data?.relayNumber || getActiveTo() || "").trim();
      const carrier = String(data?.carrier || "generic").trim() || "generic";
      if (callRoutingBusinessNumberInput) callRoutingBusinessNumberInput.value = String(data?.businessNumber || "");
      if (callRoutingRelayNumberInput) callRoutingRelayNumberInput.value = relayNumber;
      if (callRoutingCarrierSelect) callRoutingCarrierSelect.value = carrier;
      if (callRoutingEnableCodeInput) {
        callRoutingEnableCodeInput.value = String(data?.enableCode || getCarrierEnableCode(carrier) || "*72");
      }
      if (callRoutingDisableCodeInput) {
        callRoutingDisableCodeInput.value = String(data?.disableCode || getCarrierDisableCode(carrier) || "*73");
      }
      setCallRoutingStatusText(String(data?.status || "Not setup"));
    }

    function loadCallRoutingState() {
      const saved = loadLS(getCallRoutingStorageKey(), {});
      applyCallRoutingToUi(saved);
    }

    function saveCallRoutingState(statusOverride = "") {
      const payload = readCallRoutingFromUi();
      if (statusOverride) payload.status = statusOverride;
      saveLS(getCallRoutingStorageKey(), payload);
      applyCallRoutingToUi(payload);
      return payload;
    }

    async function copyCallRoutingValue(value, label) {
      const text = String(value || "").trim();
      if (!text) {
        showSettingsToast(`${label} is empty`, true);
        return;
      }
      try {
        await navigator.clipboard.writeText(text);
        showSettingsToast(`${label} copied`);
      } catch {
        showSettingsToast(`Failed to copy ${label.toLowerCase()}`, true);
      }
    }

    callRoutingCarrierSelect?.addEventListener("change", () => {
      const carrier = String(callRoutingCarrierSelect?.value || "generic");
      if (callRoutingEnableCodeInput && !String(callRoutingEnableCodeInput.value || "").trim()) {
        callRoutingEnableCodeInput.value = getCarrierEnableCode(carrier);
      }
      if (callRoutingDisableCodeInput && !String(callRoutingDisableCodeInput.value || "").trim()) {
        callRoutingDisableCodeInput.value = getCarrierDisableCode(carrier);
      }
      setCallRoutingInlineStatus("");
    });
    callRoutingCopyEnableBtn?.addEventListener("click", async () => {
      await copyCallRoutingValue(callRoutingEnableCodeInput?.value, "Enable code");
    });
    callRoutingCopyDisableBtn?.addEventListener("click", async () => {
      await copyCallRoutingValue(callRoutingDisableCodeInput?.value, "Disable code");
    });
    callRoutingSaveBtn?.addEventListener("click", () => {
      saveCallRoutingState("Pending test");
      setCallRoutingInlineStatus("Saved. Run a test call to confirm forwarding.");
      showSettingsToast("Call routing saved");
    });
    callRoutingMarkCompleteBtn?.addEventListener("click", () => {
      saveCallRoutingState("Active");
      setCallRoutingInlineStatus("Marked active.");
      showSettingsToast("Call routing marked active");
    });
    callRoutingRunTestBtn?.addEventListener("click", () => {
      saveCallRoutingState("Pending test");
      setCallRoutingInlineStatus("Test initiated. Place a live call to verify forwarding.");
      showSettingsToast("Run a live test call to the business number");
    });
    loadCallRoutingState();

    const notifDefaults = {
      channels: { email: true, sms: false, desktop: false },
      triggers: { vipMessage: true, missedCall: true, newBooking: true },
      quietHours: { enabled: false, start: "21:00", end: "08:00", timezone: "America/New_York" },
      dedupeMinutes: 10,
      escalation: { enabled: false, afterMinutes: 5, channel: "sms" }
    };
    const notifStatusEl = document.getElementById("notifSettingsStatus");
    const notifSaveBtn = document.getElementById("saveNotifSettingsBtn");
    const notifActivityList = document.getElementById("notifActivityList");
    const notifTestAlertBtn = document.getElementById("notifTestAlertBtn");
    const notifTestModal = document.getElementById("notifTestModal");
    const notifTestCancelBtn = document.getElementById("notifTestCancelBtn");
    const notifTestSendBtn = document.getElementById("notifTestSendBtn");
    const notifTestEventType = document.getElementById("notifTestEventType");
    const notifTestChannel = document.getElementById("notifTestChannel");
    const notifQuietHoursTz = document.getElementById("notifQuietHoursTz");
    const notifDesktopCheckbox = document.getElementById("notifChannelDesktop");
    let notifDesktopReady = false;
    const seenDesktopEvents = window.__relayDesktopSeenEvents instanceof Set
      ? window.__relayDesktopSeenEvents
      : new Set();
    window.__relayDesktopSeenEvents = seenDesktopEvents;

    function normalizeNotifSettings(saved) {
      return {
        channels: {
          email: saved?.channels?.email ?? notifDefaults.channels.email,
          sms: saved?.channels?.sms ?? notifDefaults.channels.sms,
          desktop: saved?.channels?.desktop ?? notifDefaults.channels.desktop
        },
        triggers: {
          vipMessage: saved?.triggers?.vipMessage ?? notifDefaults.triggers.vipMessage,
          missedCall: saved?.triggers?.missedCall ?? notifDefaults.triggers.missedCall,
          newBooking: saved?.triggers?.newBooking ?? notifDefaults.triggers.newBooking
        },
        quietHours: {
          enabled: saved?.quietHours?.enabled === true,
          start: String(saved?.quietHours?.start || notifDefaults.quietHours.start),
          end: String(saved?.quietHours?.end || notifDefaults.quietHours.end),
          timezone: String(saved?.quietHours?.timezone || notifDefaults.quietHours.timezone)
        },
        dedupeMinutes: Number(saved?.dedupeMinutes || notifDefaults.dedupeMinutes),
        escalation: {
          enabled: saved?.escalation?.enabled === true,
          afterMinutes: Number(saved?.escalation?.afterMinutes || notifDefaults.escalation.afterMinutes),
          channel: String(saved?.escalation?.channel || notifDefaults.escalation.channel)
        }
      };
    }

    async function loadNotifSettings() {
      const data = await apiGet("/api/notifications/settings");
      return normalizeNotifSettings(data?.settings || {});
    }

    function readNotifSettingsFromUI() {
      return normalizeNotifSettings({
        channels: {
          email: document.getElementById("notifChannelEmail")?.checked === true,
          sms: document.getElementById("notifChannelSms")?.checked === true,
          desktop: document.getElementById("notifChannelDesktop")?.checked === true
        },
        triggers: {
          vipMessage: document.getElementById("notifTriggerVipMessage")?.checked === true,
          missedCall: document.getElementById("notifTriggerMissedCall")?.checked === true,
          newBooking: document.getElementById("notifTriggerNewBooking")?.checked === true
        },
        quietHours: {
          enabled: document.getElementById("notifQuietHoursEnabled")?.checked === true,
          start: String(document.getElementById("notifQuietHoursStart")?.value || "21:00"),
          end: String(document.getElementById("notifQuietHoursEnd")?.value || "08:00"),
          timezone: String(notifQuietHoursTz?.textContent || notifDefaults.quietHours.timezone)
        },
        dedupeMinutes: Number(document.getElementById("notifDedupeMinutes")?.value || 10)
      });
    }

    function applyNotifSettingsToUI(settings) {
      const email = document.getElementById("notifChannelEmail");
      const sms = document.getElementById("notifChannelSms");
      const desktop = document.getElementById("notifChannelDesktop");
      const vipMessage = document.getElementById("notifTriggerVipMessage");
      const missedCall = document.getElementById("notifTriggerMissedCall");
      const newBooking = document.getElementById("notifTriggerNewBooking");
      const quietEnabled = document.getElementById("notifQuietHoursEnabled");
      const quietStart = document.getElementById("notifQuietHoursStart");
      const quietEnd = document.getElementById("notifQuietHoursEnd");
      const dedupeMinutes = document.getElementById("notifDedupeMinutes");

      if (email) email.checked = settings.channels.email;
      if (sms) sms.checked = settings.channels.sms;
      if (desktop) desktop.checked = settings.channels.desktop;
      if (vipMessage) vipMessage.checked = settings.triggers.vipMessage;
      if (missedCall) missedCall.checked = settings.triggers.missedCall;
      if (newBooking) newBooking.checked = settings.triggers.newBooking;
      if (quietEnabled) quietEnabled.checked = settings.quietHours.enabled === true;
      if (quietStart) quietStart.value = settings.quietHours.start || "21:00";
      if (quietEnd) quietEnd.value = settings.quietHours.end || "08:00";
      if (dedupeMinutes) dedupeMinutes.value = String(settings.dedupeMinutes || 10);
      if (notifQuietHoursTz && settings.quietHours.timezone) notifQuietHoursTz.textContent = settings.quietHours.timezone;
    }

    async function refreshNotifActivity() {
      if (!notifActivityList) return;
      try {
        const data = await apiGet("/api/notifications/log?limit=20");
        const items = data?.items || [];
        if (!items.length) {
          notifActivityList.innerHTML = `<div class="p">No alerts yet</div>`;
          return;
        }
        notifActivityList.innerHTML = items.map((item) => `
          <div class="row" style="justify-content:space-between; gap:10px; flex-wrap:wrap; border:1px solid var(--border); border-radius:10px; padding:8px 10px;">
            <span class="p">${escapeHtml(String(item.eventType || "event"))} | ${escapeHtml(String(item.channel || "system"))}</span>
            <span class="p">${escapeHtml(String(item.status || "sent"))} | ${new Date(Number(item.ts || Date.now())).toLocaleString()}</span>
          </div>
        `).join("");

        const desktopEvents = items
          .filter((item) => String(item?.channel || "") === "desktop" && String(item?.status || "") === "sent")
          .map((item) => ({
            id: String(item?.eventId || `${item?.ts || ""}_${item?.eventType || ""}_${item?.channel || ""}`),
            eventType: String(item?.eventType || "event"),
            ts: Number(item?.ts || Date.now())
          }));
        if (!notifDesktopReady) {
          desktopEvents.forEach((ev) => seenDesktopEvents.add(ev.id));
          notifDesktopReady = true;
          return;
        }
        if (typeof window !== "undefined" && "Notification" in window && Notification.permission === "granted") {
          desktopEvents
            .sort((a, b) => a.ts - b.ts)
            .forEach((ev) => {
              if (seenDesktopEvents.has(ev.id)) return;
              seenDesktopEvents.add(ev.id);
              try {
                new Notification("Relay Notification", {
                  body: `${ev.eventType.replace(/_/g, " ")} alert`,
                  tag: `relay_${ev.id}`
                });
              } catch {}
            });
        }
      } catch {
        notifActivityList.innerHTML = `<div class="p">Activity unavailable in offline mode.</div>`;
      }
    }

    async function ensureDesktopPermissionPrompt() {
      if (typeof window === "undefined" || !("Notification" in window)) {
        showSettingsToast("Desktop notifications are not supported in this browser", true);
        return false;
      }
      if (Notification.permission === "granted") return true;
      if (Notification.permission === "denied") {
        showSettingsToast("Desktop notifications are blocked in browser settings", true);
        return false;
      }
      try {
        const result = await Notification.requestPermission();
        if (result !== "granted") {
          showSettingsToast("Desktop notification permission not granted", true);
          return false;
        }
        showSettingsToast("Desktop notifications enabled");
        return true;
      } catch {
        showSettingsToast("Failed to request desktop notification permission", true);
        return false;
      }
    }

    function openNotifTestModal() {
      if (!notifTestModal) return;
      notifTestModal.classList.remove("hidden");
      notifTestModal.setAttribute("aria-hidden", "false");
    }

    function closeNotifTestModal() {
      if (!notifTestModal) return;
      notifTestModal.classList.add("hidden");
      notifTestModal.setAttribute("aria-hidden", "true");
    }

    let notifBaseline = normalizeNotifSettings({});
    try {
      notifBaseline = await loadNotifSettings();
    } catch (err) {
      if (notifStatusEl) notifStatusEl.textContent = "Unavailable";
      showSettingsToast(err?.message || "Failed to load notification settings", true);
    }
    applyNotifSettingsToUI(notifBaseline);
    if (notifBaseline?.channels?.desktop === true) {
      await ensureDesktopPermissionPrompt();
    }
    await refreshNotifActivity();

    const notifControlIds = [
      "notifChannelEmail",
      "notifChannelSms",
      "notifChannelDesktop",
      "notifTriggerVipMessage",
      "notifTriggerMissedCall",
      "notifTriggerNewBooking",
      "notifQuietHoursEnabled",
      "notifQuietHoursStart",
      "notifQuietHoursEnd",
      "notifDedupeMinutes"
    ];

    function syncNotifDirtyState() {
      if (!notifSaveBtn) return;
      const dirty = JSON.stringify(readNotifSettingsFromUI()) !== JSON.stringify(notifBaseline);
      notifSaveBtn.disabled = !dirty;
      notifSaveBtn.classList.toggle("is-dirty", dirty);
    }

    notifControlIds.forEach((id) => {
      document.getElementById(id)?.addEventListener("change", syncNotifDirtyState);
    });
    notifDesktopCheckbox?.addEventListener("change", async () => {
      if (notifDesktopCheckbox.checked) {
        const ok = await ensureDesktopPermissionPrompt();
        if (!ok) {
          notifDesktopCheckbox.checked = false;
        }
      }
      syncNotifDirtyState();
    });
    syncNotifDirtyState();

    notifSaveBtn?.addEventListener("click", async () => {
      const btn = notifSaveBtn;
      const next = readNotifSettingsFromUI();
      const original = btn.innerHTML;
      btn.disabled = true;
      if (next?.channels?.desktop === true) {
        const ok = await ensureDesktopPermissionPrompt();
        if (!ok) {
          next.channels.desktop = false;
          if (notifDesktopCheckbox) notifDesktopCheckbox.checked = false;
        }
      }
      try {
        const res = await apiPut("/api/notifications/settings", next);
        notifBaseline = normalizeNotifSettings(res?.settings || next);
        if (notifStatusEl) notifStatusEl.textContent = "Saved";
        showSettingsToast("Notification settings saved");
      } catch (err) {
        if (notifStatusEl) notifStatusEl.textContent = "Save failed";
        showSettingsToast(err?.message || "Failed to save notification settings", true);
        console.error("Notification settings save failed:", err);
      } finally {
        await refreshNotifActivity();
        btn.classList.add("saved");
        btn.innerHTML = "Saved";
        setTimeout(() => {
          syncNotifDirtyState();
          btn.classList.remove("saved");
          btn.innerHTML = original;
        }, 1200);
      }
    });

    notifTestAlertBtn?.addEventListener("click", () => openNotifTestModal());
    notifTestCancelBtn?.addEventListener("click", () => closeNotifTestModal());
    notifTestModal?.addEventListener("click", (e) => {
      if (e.target === notifTestModal) closeNotifTestModal();
    });
    notifTestSendBtn?.addEventListener("click", async () => {
      const type = String(notifTestEventType?.value || "missed_call");
      const channel = String(notifTestChannel?.value || "");
      try {
        notifTestSendBtn.disabled = true;
        await apiPost("/api/notifications/emitTest", { type, channel });
        showSettingsToast("Test notification event emitted");
        closeNotifTestModal();
      } catch (err) {
        showSettingsToast(err?.message || "Failed to emit test notification", true);
      } finally {
        notifTestSendBtn.disabled = false;
        await refreshNotifActivity();
      }
    });

    if (window.__relayNotifPoller) {
      clearInterval(window.__relayNotifPoller);
    }
    window.__relayNotifPoller = setInterval(() => {
      refreshNotifActivity();
    }, 15000);

    // ---- team settings (frontend-only local state) ----
    const TEAM_SETTINGS_KEY = "mc_team_settings_v1";
    const teamRoot = document.getElementById("teamSectionRoot");

    function makePermissionsFor(overrides = {}) {
      const base = {
        Contacts: { view: true, edit: false },
        Messages: { view: true, edit: false },
        Automations: { view: true, edit: false },
        AI: { view: true, edit: false },
        Billing: { view: false, edit: false },
        Integrations: { view: false, edit: false },
        Developer: { view: false, edit: false }
      };
      for (const [area, perms] of Object.entries(overrides)) {
        base[area] = { ...base[area], ...perms };
      }
      return base;
    }

    function makeFullPermissions() {
      return makePermissionsFor({
        Contacts: { view: true, edit: true },
        Messages: { view: true, edit: true },
        Automations: { view: true, edit: true },
        AI: { view: true, edit: true },
        Billing: { view: true, edit: true },
        Integrations: { view: true, edit: true },
        Developer: { view: true, edit: true }
      });
    }

    function makeAdminPermissions() {
      return makePermissionsFor({
        Contacts: { view: true, edit: true },
        Messages: { view: true, edit: true },
        Automations: { view: true, edit: true },
        AI: { view: true, edit: true },
        Billing: { view: true, edit: false },
        Integrations: { view: true, edit: true },
        Developer: { view: false, edit: false }
      });
    }

    function makeAgentPermissions() {
      return makePermissionsFor({
        Contacts: { view: true, edit: true },
        Messages: { view: true, edit: true },
        Automations: { view: true, edit: false },
        AI: { view: true, edit: false }
      });
    }

    function makeReadOnlyPermissions() {
      return makePermissionsFor({
        Contacts: { view: true, edit: false },
        Messages: { view: true, edit: false },
        Automations: { view: true, edit: false },
        AI: { view: true, edit: false }
      });
    }

    function createDefaultTeamState() {
      return {
        members: [],
        selectedRolePreset: "Admin",
        rolePermissions: {
          Owner: makeFullPermissions(),
          Admin: makeAdminPermissions(),
          Agent: makeAgentPermissions(),
          "Read-only": makeReadOnlyPermissions()
        },
        security: {
          enforceMfa: false,
          sessionTimeout: "8h",
          ipAllowlist: ["203.0.113.4", "198.51.100.0/24"],
          allowedDomains: ["relay.com"],
          allowPersonalApiKeys: false
        },
        invitations: {
          linkExpiry: "24h",
          generatedLink: "",
          bulkQueued: 0
        },
        audit: []
      };
    }

    function parseCsvList(v){
      return String(v || "")
        .split(",")
        .map(s => s.trim())
        .filter(Boolean);
    }

    function normalizeTeamState(raw) {
      const fallback = createDefaultTeamState();
      const state = raw && typeof raw === "object" ? raw : {};
      return {
        members: Array.isArray(state.members) ? state.members : fallback.members,
        selectedRolePreset: state.selectedRolePreset || fallback.selectedRolePreset,
        rolePermissions: state.rolePermissions && typeof state.rolePermissions === "object" ? state.rolePermissions : fallback.rolePermissions,
        security: { ...fallback.security, ...(state.security || {}) },
        invitations: { ...fallback.invitations, ...(state.invitations || {}) },
        audit: Array.isArray(state.audit) ? state.audit : fallback.audit
      };
    }

    function isValidEmail(value) {
      return /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(String(value || "").trim());
    }

    function isValidDomain(value) {
      return /^(?:[a-zA-Z0-9-]+\.)+[a-zA-Z]{2,}$/.test(String(value || "").trim());
    }

    function isValidIpOrCidr(value) {
      const v = String(value || "").trim();
      if (!v) return false;
      const [ip, cidr] = v.split("/");
      const parts = String(ip || "").split(".");
      if (parts.length !== 4) return false;
      for (const p of parts) {
        const n = Number(p);
        if (!Number.isInteger(n) || n < 0 || n > 255) return false;
      }
      if (cidr === undefined) return true;
      const c = Number(cidr);
      return Number.isInteger(c) && c >= 0 && c <= 32;
    }

    function splitListInput(value) {
      return String(value || "")
        .split(/[\n,]+/)
        .map((x) => x.trim())
        .filter(Boolean);
    }

    function formatRelativeTs(ts) {
      if (!ts) return "Never";
      const diffMs = Math.max(0, Date.now() - Number(ts));
      const min = Math.floor(diffMs / 60000);
      if (min < 1) return "Just now";
      if (min < 60) return `${min}m ago`;
      const hrs = Math.floor(min / 60);
      if (hrs < 24) return `${hrs}h ago`;
      return `${Math.floor(hrs / 24)}d ago`;
    }

    if (teamRoot) {
      let teamState = normalizeTeamState(loadLS(TEAM_SETTINGS_KEY, null));
      let teamUi = {
        search: "",
        roleFilter: "",
        statusFilter: "",
        sortOrder: "desc",
        openMenuMemberId: null
      };
      let teamModalConfirmHandler = null;

      const teamEls = {
        tbody: document.getElementById("teamMembersTbody"),
        membersNote: document.getElementById("teamMembersNote"),
        search: document.getElementById("teamMemberSearch"),
        roleFilter: document.getElementById("teamRoleFilter"),
        statusFilter: document.getElementById("teamStatusFilter"),
        sort: document.getElementById("teamSortLastActive"),
        rolePills: document.getElementById("teamRolePills"),
        permTbody: document.getElementById("teamPermTbody"),
        enforceMfa: document.getElementById("teamEnforceMfa"),
        sessionTimeout: document.getElementById("teamSessionTimeout"),
        signOutAllBtn: document.getElementById("teamSignOutAllBtn"),
        ipAllowlist: document.getElementById("teamIpAllowlist"),
        ipError: document.getElementById("teamIpError"),
        allowedDomains: document.getElementById("teamAllowedDomains"),
        domainError: document.getElementById("teamDomainError"),
        allowPersonalApiKeys: document.getElementById("teamAllowPersonalApiKeys"),
        auditTbody: document.getElementById("teamAuditTbody"),
        inviteEmail: document.getElementById("teamInviteEmail"),
        inviteRole: document.getElementById("teamInviteRole"),
        inviteSendBtn: document.getElementById("teamInviteSendBtn"),
        inviteError: document.getElementById("teamInviteError"),
        bulkInput: document.getElementById("teamBulkInviteInput"),
        bulkBtn: document.getElementById("teamBulkInviteBtn"),
        bulkStatus: document.getElementById("teamBulkInviteStatus"),
        linkExpiry: document.getElementById("teamInviteLinkExpiry"),
        linkOutput: document.getElementById("teamInviteLinkOutput"),
        generateLinkBtn: document.getElementById("teamGenerateInviteLinkBtn"),
        copyLinkBtn: document.getElementById("teamCopyInviteLinkBtn"),
        transferOwnerSelect: document.getElementById("teamTransferOwnerSelect"),
        transferOwnerBtn: document.getElementById("teamTransferOwnerBtn"),
        deleteConfirm: document.getElementById("teamDeleteConfirmToggle"),
        deleteWorkspaceBtn: document.getElementById("teamDeleteWorkspaceBtn"),
        actionMenu: document.getElementById("teamActionMenu"),
        modal: document.getElementById("teamActionModal"),
        modalTitle: document.getElementById("teamModalTitle"),
        modalBody: document.getElementById("teamModalBody"),
        modalExtra: document.getElementById("teamModalExtra"),
        modalClose: document.getElementById("teamModalClose"),
        modalCancel: document.getElementById("teamModalCancel"),
        modalConfirm: document.getElementById("teamModalConfirm"),
        addMemberBtn: document.getElementById("teamAddMemberBtn"),
        addMemberModal: document.getElementById("teamAddMemberModal"),
        addMemberCloseBtn: document.getElementById("teamAddMemberCloseBtn"),
        addMemberCancelBtn: document.getElementById("teamAddMemberCancelBtn"),
        addMemberSaveBtn: document.getElementById("teamAddMemberSaveBtn"),
        addMemberName: document.getElementById("teamAddMemberName"),
        addMemberEmail: document.getElementById("teamAddMemberEmail"),
        addMemberRole: document.getElementById("teamAddMemberRole"),
        addMemberStatus: document.getElementById("teamAddMemberStatus"),
        addMemberTempPassword: document.getElementById("teamAddMemberTempPassword")
      };

      function persistTeamState() {
        saveLS(TEAM_SETTINGS_KEY, teamState);
      }

      function addTeamAudit(event, details, actor = "Current user") {
        teamState.audit.unshift({ ts: Date.now(), actor, event, details });
        teamState.audit = teamState.audit.slice(0, 25);
      }

      function countOwners() {
        return teamState.members.filter((m) => m.role === "Owner" && m.status !== "Suspended").length;
      }

      function roleFromApi(role) {
        const v = String(role || "").trim().toLowerCase();
        if (v === "owner") return "Owner";
        if (v === "admin") return "Admin";
        if (v === "agent") return "Agent";
        if (v === "readonly" || v === "read-only") return "Read-only";
        return "Read-only";
      }

      function roleToApi(role) {
        const v = String(role || "").trim().toLowerCase();
        if (v === "owner") return "owner";
        if (v === "admin") return "admin";
        if (v === "agent") return "agent";
        if (v === "read-only" || v === "readonly") return "readonly";
        return "admin";
      }

      async function loadWorkspaceMembersFromApi() {
        try {
          const res = await apiGet("/api/account/users");
          const users = Array.isArray(res?.users) ? res.users : [];
          teamState.members = users.map((u) => {
            const email = String(u?.email || "").trim();
            const fallbackName = email.includes("@")
              ? email.split("@")[0].replace(/[._-]/g, " ").trim()
              : "Member";
            return {
              id: String(u?.id || ""),
              name: String(u?.name || "").trim() || fallbackName,
              email,
              role: roleFromApi(u?.role),
              status: u?.disabled === true ? "Suspended" : "Active",
              lastActiveAt: Number(u?.lastLoginAt || 0) || null,
              mfa: false,
              inviteToken: ""
            };
          });
          renderMembersTable();
          renderTransferOwnerOptions();
        } catch (err) {
          if (teamEls.membersNote) teamEls.membersNote.textContent = err?.message || "Failed to load team members.";
        }
      }

      function getFilteredMembers() {
        const out = Array.isArray(teamState.members) ? [...teamState.members] : [];
        out.sort((a, b) => {
          const av = Number(a.lastActiveAt || 0);
          const bv = Number(b.lastActiveAt || 0);
          return teamUi.sortOrder === "asc" ? av - bv : bv - av;
        });
        return out;
      }

      function resetAddMemberForm() {
        if (teamEls.addMemberName) teamEls.addMemberName.value = "";
        if (teamEls.addMemberEmail) teamEls.addMemberEmail.value = "";
        if (teamEls.addMemberRole) teamEls.addMemberRole.value = "Agent";
        if (teamEls.addMemberStatus) teamEls.addMemberStatus.textContent = "";
        if (teamEls.addMemberTempPassword) {
          teamEls.addMemberTempPassword.classList.add("hidden");
          teamEls.addMemberTempPassword.textContent = "";
        }
      }

      function openAddMemberModal() {
        if (!teamEls.addMemberModal) return;
        resetAddMemberForm();
        teamEls.addMemberModal.classList.remove("hidden");
      }

      function closeAddMemberModal() {
        if (!teamEls.addMemberModal) return;
        teamEls.addMemberModal.classList.add("hidden");
      }

      function statusClass(status) {
        if (status === "Active") return "is-active";
        if (status === "Invited") return "is-invited";
        if (status === "Suspended") return "is-suspended";
        return "";
      }

      function closeTeamActionMenu() {
        teamUi.openMenuMemberId = null;
        teamUi.anchorButtonEl = null;
        if (!teamEls.actionMenu) return;
        window.removeEventListener("resize", positionTeamActionMenu, true);
        window.removeEventListener("scroll", positionTeamActionMenu, true);
        teamEls.actionMenu.classList.add("hidden");
        teamEls.actionMenu.innerHTML = "";
      }

      function openTeamModal({ title, body, confirmLabel = "Confirm", danger = false, extraHtml = "", onConfirm }) {
        if (!teamEls.modal) return;
        teamModalConfirmHandler = onConfirm || null;
        if (teamEls.modalTitle) teamEls.modalTitle.textContent = title || "Confirm action";
        if (teamEls.modalBody) teamEls.modalBody.textContent = body || "";
        if (teamEls.modalExtra) teamEls.modalExtra.innerHTML = extraHtml || "";
        if (teamEls.modalConfirm) {
          teamEls.modalConfirm.textContent = confirmLabel;
          teamEls.modalConfirm.classList.toggle("danger", danger);
        }
        teamEls.modal.classList.remove("hidden");
      }

      function closeTeamModal() {
        if (teamEls.modal) teamEls.modal.classList.add("hidden");
        if (teamEls.modalExtra) teamEls.modalExtra.innerHTML = "";
        teamModalConfirmHandler = null;
      }

      function renderMembersTable() {
        if (!teamEls.tbody) return;
        const rows = getFilteredMembers();
        if (!rows.length) {
          teamEls.tbody.innerHTML = `<tr><td colspan="7" class="team-empty-row">No team members.</td></tr>`;
          if (teamEls.membersNote) teamEls.membersNote.textContent = "No team members.";
          return;
        }
        teamEls.tbody.innerHTML = rows.map((m) => `
          <tr>
            <td>${escapeHtml(m.name)}</td>
            <td>${escapeHtml(m.email)}</td>
            <td>${escapeHtml(m.role)}</td>
            <td><span class="team-status ${statusClass(m.status)}">${escapeHtml(m.status)}</span></td>
            <td>${escapeHtml(formatRelativeTs(m.lastActiveAt))}</td>
            <td>${m.mfa ? "Enabled" : "Not set"}</td>
            <td class="team-actions-cell"><button class="btn team-row-action-btn" type="button" data-team-action-btn="${escapeAttr(m.id)}">Actions</button></td>
          </tr>
        `).join("");
        if (teamEls.membersNote) teamEls.membersNote.textContent = `Showing ${rows.length} member${rows.length === 1 ? "" : "s"}.`;
      }

      function renderRolePresets() {
        if (!teamEls.rolePills) return;
        const presets = ["Owner", "Admin", "Agent", "Read-only"];
        teamEls.rolePills.innerHTML = presets.map((role) => `
          <button class="btn team-role-pill ${teamState.selectedRolePreset === role ? "active" : ""}" type="button" data-team-role-pill="${escapeAttr(role)}">${escapeHtml(role)}</button>
        `).join("");
      }

      function renderPermissionMatrix() {
        if (!teamEls.permTbody) return;
        const role = teamState.selectedRolePreset || "Admin";
        const areas = ["Contacts", "Messages", "Automations", "AI", "Billing", "Integrations", "Developer"];
        const perms = teamState.rolePermissions[role] || makeAdminPermissions();
        teamEls.permTbody.innerHTML = areas.map((area) => `
          <tr>
            <td>${escapeHtml(area)}</td>
            <td><input type="checkbox" disabled ${perms[area]?.view ? "checked" : ""}></td>
            <td><input type="checkbox" disabled ${perms[area]?.edit ? "checked" : ""}></td>
          </tr>
        `).join("");
      }

      function renderAuditRows() {
        if (!teamEls.auditTbody) return;
        if (!teamState.audit.length) {
          teamEls.auditTbody.innerHTML = `<tr><td colspan="4" class="team-empty-row">No audit entries yet.</td></tr>`;
          return;
        }
        teamEls.auditTbody.innerHTML = teamState.audit.slice(0, 8).map((a) => `
          <tr>
            <td>${escapeHtml(formatRelativeTs(a.ts))}</td>
            <td>${escapeHtml(a.actor || "System")}</td>
            <td>${escapeHtml(a.event || "Event")}</td>
            <td>${escapeHtml(a.details || "")}</td>
          </tr>
        `).join("");
      }

      function renderTransferOwnerOptions() {
        if (!teamEls.transferOwnerSelect) return;
        const candidates = teamState.members.filter((m) => m.status === "Active" && m.role !== "Owner");
        if (!candidates.length) {
          teamEls.transferOwnerSelect.innerHTML = `<option value="">No eligible active non-owner</option>`;
          if (teamEls.transferOwnerBtn) teamEls.transferOwnerBtn.disabled = true;
          return;
        }
        teamEls.transferOwnerSelect.innerHTML = candidates.map((m) => `<option value="${escapeAttr(m.id)}">${escapeHtml(m.name)} (${escapeHtml(m.email)})</option>`).join("");
        if (teamEls.transferOwnerBtn) teamEls.transferOwnerBtn.disabled = false;
      }

      function renderTeamSecurityUI() {
        if (teamEls.enforceMfa) teamEls.enforceMfa.checked = teamState.security.enforceMfa === true;
        if (teamEls.sessionTimeout) teamEls.sessionTimeout.value = teamState.security.sessionTimeout || "8h";
        if (teamEls.ipAllowlist) teamEls.ipAllowlist.value = (teamState.security.ipAllowlist || []).join("\n");
        if (teamEls.allowedDomains) teamEls.allowedDomains.value = (teamState.security.allowedDomains || []).join(", ");
        if (teamEls.allowPersonalApiKeys) teamEls.allowPersonalApiKeys.checked = teamState.security.allowPersonalApiKeys === true;
        if (teamEls.linkExpiry) teamEls.linkExpiry.value = teamState.invitations.linkExpiry || "24h";
        if (teamEls.linkOutput) teamEls.linkOutput.value = teamState.invitations.generatedLink || "";
        if (teamEls.bulkStatus) {
          const c = Number(teamState.invitations.bulkQueued || 0);
          teamEls.bulkStatus.textContent = c > 0 ? `${c} invite${c === 1 ? "" : "s"} queued locally.` : "No bulk invites queued.";
        }
      }

      function renderTeamTab() {
        renderMembersTable();
        renderRolePresets();
        renderPermissionMatrix();
        renderAuditRows();
        renderTransferOwnerOptions();
        renderTeamSecurityUI();
      }

      async function copyText(value) {
        const text = String(value || "").trim();
        if (!text) return false;
        try {
          await navigator.clipboard.writeText(text);
          return true;
        } catch {
          return false;
        }
      }

      function positionTeamActionMenu() {
        if (!teamEls.actionMenu || teamEls.actionMenu.classList.contains("hidden")) return;
        if (!teamUi.anchorButtonEl) return;
        const viewportPad = 8;
        const gap = 6;
        const btnRect = teamUi.anchorButtonEl.getBoundingClientRect();
        const menuRect = teamEls.actionMenu.getBoundingClientRect();
        const vw = window.innerWidth;
        const vh = window.innerHeight;

        // Default: below button, right edge aligned to button right edge.
        let left = btnRect.right - menuRect.width;
        let top = btnRect.bottom + gap;

        // Horizontal fit / inward flip.
        if (left + menuRect.width > vw - viewportPad) {
          left = btnRect.left - menuRect.width;
        }
        if (left < viewportPad) {
          left = viewportPad;
        }
        if (left + menuRect.width > vw - viewportPad) {
          left = Math.max(viewportPad, vw - viewportPad - menuRect.width);
        }

        // Vertical fit; prefer below, flip up if needed.
        if (top + menuRect.height > vh - viewportPad) {
          top = btnRect.top - menuRect.height - gap;
        }
        if (top < viewportPad) {
          top = viewportPad;
        }
        if (top + menuRect.height > vh - viewportPad) {
          top = Math.max(viewportPad, vh - viewportPad - menuRect.height);
        }

        teamEls.actionMenu.style.left = `${Math.round(left)}px`;
        teamEls.actionMenu.style.top = `${Math.round(top)}px`;
      }

      function openTeamActionMenu(memberId, buttonEl) {
        if (!teamEls.actionMenu || !buttonEl) return;
        const member = teamState.members.find((m) => m.id === memberId);
        if (!member) return;
        teamUi.anchorButtonEl = buttonEl;
        teamUi.openMenuMemberId = memberId;
        const actions = [
          { key: "change-role", label: "Change role" },
          { key: "toggle-disabled", label: member.status === "Suspended" ? "Activate" : "Suspend" },
          { key: "remove", label: "Remove", danger: true }
        ];
        teamEls.actionMenu.innerHTML = actions.map((a) => `
          <button class="team-action-item ${a.danger ? "danger" : ""}" type="button" data-team-menu-action="${a.key}" data-team-member-id="${escapeAttr(memberId)}" ${a.disabled ? "disabled" : ""}>${escapeHtml(a.label)}</button>
        `).join("");
        if (teamEls.actionMenu.parentElement !== document.body) {
          document.body.appendChild(teamEls.actionMenu);
        }
        teamEls.actionMenu.style.left = `-9999px`;
        teamEls.actionMenu.style.top = `-9999px`;
        teamEls.actionMenu.classList.remove("hidden");
        positionTeamActionMenu();
        window.addEventListener("resize", positionTeamActionMenu, true);
        window.addEventListener("scroll", positionTeamActionMenu, true);
      }

      function validateIpAllowlistUI() {
        const entries = splitListInput(teamEls.ipAllowlist?.value || "");
        const invalid = entries.some((x) => !isValidIpOrCidr(x));
        if (teamEls.ipError) teamEls.ipError.classList.toggle("hidden", !invalid);
        return !invalid;
      }

      function validateDomainAllowlistUI() {
        const entries = splitListInput(teamEls.allowedDomains?.value || "");
        const invalid = entries.some((x) => !isValidDomain(x));
        if (teamEls.domainError) teamEls.domainError.classList.toggle("hidden", !invalid);
        return !invalid;
      }

      async function loadTeamSecurityFromApi() {
        try {
          const res = await apiGet("/api/account/team-security");
          const cfg = res?.settings || {};
          teamState.security.enforceMfa = cfg.enforceMfa === true;
          teamState.security.sessionTimeout = String(cfg.sessionTimeout || "8h");
          teamState.security.ipAllowlist = Array.isArray(cfg.ipAllowlist) ? cfg.ipAllowlist.map((x) => String(x)) : [];
          teamState.security.allowedDomains = Array.isArray(cfg.allowedDomains) ? cfg.allowedDomains.map((x) => String(x)) : [];
          teamState.security.allowPersonalApiKeys = cfg.allowPersonalApiKeys === true;
          renderTeamSecurityUI();
        } catch (err) {
          showSettingsToast(err?.message || "Failed to load team security settings", true);
        }
      }

      function inferNameFromEmail(email) {
        const local = String(email || "").split("@")[0] || "member";
        return local
          .replace(/[._-]+/g, " ")
          .trim()
          .replace(/\b\w/g, (m) => m.toUpperCase()) || "Member";
      }

      async function saveTeamSecurityToApi(patch) {
        const res = await apiPatch("/api/account/team-security", patch);
        const cfg = res?.settings || {};
        teamState.security.enforceMfa = cfg.enforceMfa === true;
        teamState.security.sessionTimeout = String(cfg.sessionTimeout || "8h");
        teamState.security.ipAllowlist = Array.isArray(cfg.ipAllowlist) ? cfg.ipAllowlist.map((x) => String(x)) : [];
        teamState.security.allowedDomains = Array.isArray(cfg.allowedDomains) ? cfg.allowedDomains.map((x) => String(x)) : [];
        teamState.security.allowPersonalApiKeys = cfg.allowPersonalApiKeys === true;
        renderTeamSecurityUI();
      }

      function handleTeamMenuAction(action, memberId) {
        const member = teamState.members.find((m) => m.id === memberId);
        if (!member) return;
        closeTeamActionMenu();

        if (action === "change-role") {
          const extra = `
            <label class="p">New role</label>
            <select class="select" id="teamModalRoleSelect">
              <option ${member.role === "Owner" ? "selected" : ""}>Owner</option>
              <option ${member.role === "Admin" ? "selected" : ""}>Admin</option>
              <option ${member.role === "Agent" ? "selected" : ""}>Agent</option>
              <option ${member.role === "Read-only" ? "selected" : ""}>Read-only</option>
            </select>
          `;
          openTeamModal({
            title: "Change role",
            body: `Update role for ${member.name}.`,
            confirmLabel: "Update role",
            extraHtml: extra,
            onConfirm: async () => {
              const nextRole = String(document.getElementById("teamModalRoleSelect")?.value || member.role);
              if (member.role === "Owner" && nextRole !== "Owner" && countOwners() <= 2) {
                showSettingsToast("Keep at least 2 owners before demoting an owner.", true);
                return;
              }
              try {
                await apiPatch(`/api/account/users/${encodeURIComponent(member.id)}`, {
                  role: roleToApi(nextRole)
                });
                await loadWorkspaceMembersFromApi();
                closeTeamModal();
                showSettingsToast("Role updated.");
              } catch (err) {
                showSettingsToast(err?.message || "Failed to update role.", true);
              }
            }
          });
          return;
        }

        const isRemoval = action === "remove";
        const isToggleDisabled = action === "toggle-disabled";
        const willDisable = member.status !== "Suspended";
        const title = isRemoval ? "Remove member" : (willDisable ? "Suspend member" : "Activate member");
        const label = isRemoval ? "Remove" : (willDisable ? "Suspend" : "Activate");
        openTeamModal({
          title,
          body: isRemoval
            ? `Remove ${member.name} from this workspace?`
            : (willDisable
              ? `Suspend ${member.name}? They will lose workspace access.`
              : `Activate ${member.name}? They will regain workspace access.`),
          confirmLabel: label,
          danger: isRemoval || willDisable,
          onConfirm: async () => {
            if (member.role === "Owner" && countOwners() <= 2) {
              showSettingsToast("Keep at least 2 owners before removing/suspending an owner.", true);
              return;
            }
            try {
              if (isRemoval) {
                await apiDelete(`/api/account/users/${encodeURIComponent(member.id)}`);
              } else if (isToggleDisabled) {
                await apiPatch(`/api/account/users/${encodeURIComponent(member.id)}`, {
                  disabled: willDisable
                });
              }
              await loadWorkspaceMembersFromApi();
              closeTeamModal();
              showSettingsToast(isRemoval ? "Member removed." : (willDisable ? "Member suspended." : "Member activated."));
            } catch (err) {
              showSettingsToast(err?.message || "Failed to update member.", true);
            }
          }
        });
      }

      function bindTeamEvents() {
        teamEls.search?.addEventListener("input", (e) => { teamUi.search = e.target.value || ""; renderMembersTable(); });
        teamEls.roleFilter?.addEventListener("change", (e) => { teamUi.roleFilter = e.target.value || ""; renderMembersTable(); });
        teamEls.statusFilter?.addEventListener("change", (e) => { teamUi.statusFilter = e.target.value || ""; renderMembersTable(); });
        teamEls.sort?.addEventListener("change", (e) => { teamUi.sortOrder = e.target.value || "desc"; renderMembersTable(); });

        teamEls.addMemberBtn?.addEventListener("click", () => {
          openAddMemberModal();
        });
        teamEls.addMemberCloseBtn?.addEventListener("click", closeAddMemberModal);
        teamEls.addMemberCancelBtn?.addEventListener("click", closeAddMemberModal);
        teamEls.addMemberModal?.addEventListener("click", (e) => {
          if (e.target === teamEls.addMemberModal) closeAddMemberModal();
        });
        teamEls.addMemberSaveBtn?.addEventListener("click", async () => {
          const name = String(teamEls.addMemberName?.value || "").trim();
          const email = String(teamEls.addMemberEmail?.value || "").trim().toLowerCase();
          const roleLabel = String(teamEls.addMemberRole?.value || "Agent").trim();
          if (!name || !isValidEmail(email)) {
            if (teamEls.addMemberStatus) teamEls.addMemberStatus.textContent = "Enter a valid name and email.";
            return;
          }
          if (teamEls.addMemberSaveBtn) teamEls.addMemberSaveBtn.disabled = true;
          if (teamEls.addMemberStatus) teamEls.addMemberStatus.textContent = "Creating member...";
          try {
            const res = await apiPost("/api/account/users", {
              name,
              email,
              role: roleToApi(roleLabel)
            });
            const tempPassword = String(res?.temporaryPassword || "").trim();
            if (teamEls.addMemberStatus) teamEls.addMemberStatus.textContent = "Member created.";
            if (teamEls.addMemberTempPassword) {
              teamEls.addMemberTempPassword.classList.remove("hidden");
              teamEls.addMemberTempPassword.textContent = tempPassword
                ? `Temporary password: ${tempPassword}`
                : "Temporary password not returned.";
            }
            await loadWorkspaceMembersFromApi();
            showSettingsToast("Member created");
          } catch (err) {
            if (teamEls.addMemberStatus) teamEls.addMemberStatus.textContent = err?.message || "Failed to create member.";
          } finally {
            if (teamEls.addMemberSaveBtn) teamEls.addMemberSaveBtn.disabled = false;
          }
        });

        teamEls.rolePills?.addEventListener("click", (e) => {
          const btn = e.target.closest("[data-team-role-pill]");
          if (!btn) return;
          teamState.selectedRolePreset = btn.getAttribute("data-team-role-pill");
          persistTeamState();
          renderRolePresets();
          renderPermissionMatrix();
        });

        if (window.__relayTeamDocClickHandler) {
          document.removeEventListener("click", window.__relayTeamDocClickHandler);
        }
        window.__relayTeamDocClickHandler = (e) => {
          const actionBtn = e.target.closest("[data-team-action-btn]");
          if (actionBtn) {
            if (teamUi.openMenuMemberId === actionBtn.getAttribute("data-team-action-btn")) {
              closeTeamActionMenu();
              return;
            }
            openTeamActionMenu(actionBtn.getAttribute("data-team-action-btn"), actionBtn);
            return;
          }
          const menuAction = e.target.closest("[data-team-menu-action]");
          if (menuAction) {
            handleTeamMenuAction(menuAction.getAttribute("data-team-menu-action"), menuAction.getAttribute("data-team-member-id"));
            return;
          }
          if (teamEls.actionMenu && !e.target.closest("#teamActionMenu")) closeTeamActionMenu();
        };
        document.addEventListener("click", window.__relayTeamDocClickHandler);

        if (window.__relayTeamEscHandler) {
          document.removeEventListener("keydown", window.__relayTeamEscHandler);
        }
        window.__relayTeamEscHandler = (e) => {
          if (e.key === "Escape") {
            closeTeamActionMenu();
            closeTeamModal();
          }
        };
        document.addEventListener("keydown", window.__relayTeamEscHandler);

        teamEls.enforceMfa?.addEventListener("change", async () => {
          const next = teamEls.enforceMfa.checked === true;
          try {
            await saveTeamSecurityToApi({ enforceMfa: next });
            addTeamAudit("MFA enabled", next ? "Workspace MFA enforcement turned on" : "Workspace MFA enforcement turned off");
            renderAuditRows();
            showSettingsToast("MFA policy saved");
          } catch (err) {
            showSettingsToast(err?.message || "Failed to save MFA policy", true);
            renderTeamSecurityUI();
          }
        });
        teamEls.sessionTimeout?.addEventListener("change", async () => {
          const next = String(teamEls.sessionTimeout.value || "8h");
          try {
            await saveTeamSecurityToApi({ sessionTimeout: next });
            showSettingsToast("Session timeout saved");
          } catch (err) {
            showSettingsToast(err?.message || "Failed to save session timeout", true);
            renderTeamSecurityUI();
          }
        });
        teamEls.ipAllowlist?.addEventListener("blur", validateIpAllowlistUI);
        teamEls.ipAllowlist?.addEventListener("input", () => teamEls.ipError?.classList.add("hidden"));
        teamEls.allowedDomains?.addEventListener("blur", validateDomainAllowlistUI);
        teamEls.allowedDomains?.addEventListener("input", () => teamEls.domainError?.classList.add("hidden"));
        teamEls.ipAllowlist?.addEventListener("blur", async () => {
          if (!validateIpAllowlistUI()) return;
          try {
            await saveTeamSecurityToApi({ ipAllowlist: splitListInput(teamEls.ipAllowlist?.value || "") });
            showSettingsToast("IP allowlist saved");
          } catch (err) {
            showSettingsToast(err?.message || "Failed to save IP allowlist", true);
          }
        });
        teamEls.allowedDomains?.addEventListener("blur", async () => {
          if (!validateDomainAllowlistUI()) return;
          try {
            await saveTeamSecurityToApi({ allowedDomains: splitListInput(teamEls.allowedDomains?.value || "") });
            showSettingsToast("Allowed domains saved");
          } catch (err) {
            showSettingsToast(err?.message || "Failed to save allowed domains", true);
          }
        });
        teamEls.allowPersonalApiKeys?.addEventListener("change", async () => {
          const next = teamEls.allowPersonalApiKeys.checked === true;
          try {
            await saveTeamSecurityToApi({ allowPersonalApiKeys: next });
            showSettingsToast("API key policy saved");
          } catch (err) {
            showSettingsToast(err?.message || "Failed to save API key policy", true);
            renderTeamSecurityUI();
          }
        });

        teamEls.signOutAllBtn?.addEventListener("click", () => openTeamModal({
          title: "Sign out all users",
          body: "This will terminate active sessions across the workspace.",
          confirmLabel: "Sign out all",
          danger: true,
          onConfirm: async () => {
            try {
              const result = await apiPost("/api/account/team-security/signout-all", {});
              addTeamAudit("Settings changed", "Sign out all users initiated");
              renderAuditRows();
              closeTeamModal();
              showSettingsToast(`Signed out ${Number(result?.signedOutUsers || 0)} user(s), revoked ${Number(result?.revokedSessions || 0)} session(s).`);
            } catch (err) {
              showSettingsToast(err?.message || "Failed to sign out users", true);
            }
          }
        }));

        teamEls.inviteSendBtn?.addEventListener("click", async () => {
          const email = String(teamEls.inviteEmail?.value || "").trim().toLowerCase();
          const role = String(teamEls.inviteRole?.value || "Agent");
          const expiresIn = String(teamEls.linkExpiry?.value || "24h");
          const valid = isValidEmail(email);
          if (teamEls.inviteError) teamEls.inviteError.classList.toggle("hidden", valid);
          if (!valid) return;
          try {
            const created = await apiPost("/api/account/invitations", {
              email,
              name: inferNameFromEmail(email),
              role: roleToApi(role),
              expiresIn
            });
            const acceptPath = String(created?.invitation?.acceptPath || "").trim();
            const inviteLink = acceptPath ? `${window.location.origin}${acceptPath}` : "";
            addTeamAudit("User invited", `Invited ${email} as ${role}`);
            renderAuditRows();
            if (teamEls.inviteEmail) teamEls.inviteEmail.value = "";
            const subject = encodeURIComponent("Relay workspace invite");
            const body = encodeURIComponent(
              `You have been invited to Relay.\n\nAccept invite: ${inviteLink || `${window.location.origin}/`}\nEmail: ${email}\n\nSet your password on the invite page.`
            );
            window.location.href = `mailto:${encodeURIComponent(email)}?subject=${subject}&body=${body}`;
            showSettingsToast("Invite link created and ready to send.");
          } catch (err) {
            showSettingsToast(err?.message || "Failed to send invite.", true);
          }
        });

        teamEls.bulkBtn?.addEventListener("click", async () => {
          const emails = splitListInput(teamEls.bulkInput?.value || "");
          const invalid = emails.filter((e) => !isValidEmail(e));
          if (invalid.length) {
            showSettingsToast(`Invalid emails: ${invalid.slice(0, 3).join(", ")}`, true);
            return;
          }
          const role = String(teamEls.inviteRole?.value || "Agent");
          const expiresIn = String(teamEls.linkExpiry?.value || "24h");
          let createdCount = 0;
          let failedCount = 0;
          try {
            const payload = await apiPost("/api/account/invitations/bulk", {
              emails,
              role: roleToApi(role),
              expiresIn
            });
            createdCount = Array.isArray(payload?.created) ? payload.created.length : 0;
            failedCount = Array.isArray(payload?.failed) ? payload.failed.length : 0;
          } catch (err) {
            showSettingsToast(err?.message || "Bulk invite failed.", true);
            return;
          }
          teamState.invitations.bulkQueued = createdCount;
          if (teamEls.bulkStatus) {
            teamEls.bulkStatus.textContent = `Bulk invite complete: ${createdCount} invite link(s) created, ${failedCount} failed.`;
          }
          addTeamAudit("User invited", `Bulk invite processed: ${createdCount} invite links created, ${failedCount} failed`);
          renderAuditRows();
          persistTeamState();
          showSettingsToast(`Bulk invite complete: ${createdCount} invite links created, ${failedCount} failed.`);
        });

        teamEls.linkExpiry?.addEventListener("change", () => { teamState.invitations.linkExpiry = teamEls.linkExpiry.value || "24h"; persistTeamState(); });
        teamEls.generateLinkBtn?.addEventListener("click", async () => {
          const expiry = teamEls.linkExpiry?.value || "24h";
          const role = String(teamEls.inviteRole?.value || "Agent");
          try {
            const payload = await apiPost("/api/account/invitations/link", {
              role: roleToApi(role),
              expiresIn: expiry
            });
            const acceptPath = String(payload?.invitation?.acceptPath || "").trim();
            teamState.invitations.generatedLink = acceptPath ? `${window.location.origin}${acceptPath}` : "";
            teamState.invitations.linkExpiry = expiry;
            addTeamAudit("Settings changed", `Login invite link generated (${expiry})`);
            persistTeamState();
            renderTeamTab();
            showSettingsToast("Invite login link generated.");
          } catch (err) {
            showSettingsToast(err?.message || "Failed to generate invite link.", true);
          }
        });
        teamEls.copyLinkBtn?.addEventListener("click", () => {
          copyText(teamEls.linkOutput?.value || "").then((ok) => showSettingsToast(ok ? "Invite link copied." : "Generate a link first.", !ok));
        });

        teamEls.transferOwnerBtn?.addEventListener("click", () => {
          const member = teamState.members.find((m) => m.id === String(teamEls.transferOwnerSelect?.value || ""));
          if (!member) return;
          openTeamModal({
            title: "Transfer ownership",
            body: `Transfer ownership to ${member.name}?`,
            confirmLabel: "Transfer ownership",
            danger: true,
            onConfirm: () => {
              const owner = teamState.members.find((m) => m.role === "Owner" && m.status !== "Suspended");
              if (owner) owner.role = "Admin";
              member.role = "Owner";
              addTeamAudit("Role changed", `${member.email} promoted to Owner`);
              persistTeamState();
              renderTeamTab();
              closeTeamModal();
              showSettingsToast("Ownership transferred (UI preview).");
            }
          });
        });

        if (teamEls.deleteWorkspaceBtn && teamEls.deleteConfirm) {
          teamEls.deleteWorkspaceBtn.disabled = teamEls.deleteConfirm.checked !== true;
          teamEls.deleteConfirm.addEventListener("change", () => {
            teamEls.deleteWorkspaceBtn.disabled = teamEls.deleteConfirm.checked !== true;
          });
        }

        teamEls.deleteWorkspaceBtn?.addEventListener("click", () => {
          if (teamEls.deleteConfirm?.checked !== true) {
            showSettingsToast("Confirm irreversible action before deleting workspace.", true);
            return;
          }
          openTeamModal({
            title: "Delete workspace",
            body: "Placeholder only. Backend deletion safeguards are not connected.",
            confirmLabel: "I understand",
            danger: true,
            onConfirm: () => {
              addTeamAudit("Settings changed", "Delete workspace requested (placeholder)");
              renderAuditRows();
              closeTeamModal();
              showSettingsToast("Workspace deletion placeholder executed.");
            }
          });
        });

        teamEls.modalClose?.addEventListener("click", closeTeamModal);
        teamEls.modalCancel?.addEventListener("click", closeTeamModal);
        teamEls.modal?.addEventListener("click", (e) => { if (e.target === teamEls.modal) closeTeamModal(); });
        teamEls.modalConfirm?.addEventListener("click", async () => {
          if (typeof teamModalConfirmHandler !== "function") return;
          try {
            await teamModalConfirmHandler();
          } catch (err) {
            showSettingsToast(err?.message || "Action failed.", true);
          }
        });
      }

      await loadWorkspaceMembersFromApi();
      await loadTeamSecurityFromApi();
      renderTeamTab();
      bindTeamEvents();
    }

    // ---- billing settings (tenant-scoped API; demo-friendly UI) ----
    const billingRoot = wrap.querySelector('[data-settings-panel="billing"]');
    if (billingRoot) {
      const billingEls = {
        masterCard: document.getElementById("billingMasterCard"),
        masterAsOf: document.getElementById("billingMasterAsOf"),
        masterPaidRevenue: document.getElementById("billingMasterPaidRevenue"),
        masterOutstanding: document.getElementById("billingMasterOutstanding"),
        masterRunRate: document.getElementById("billingMasterRunRate"),
        masterConnected: document.getElementById("billingMasterConnected"),
        masterStatsLine: document.getElementById("billingMasterStatsLine"),
        masterTopBody: document.getElementById("billingMasterTopBody"),
        healthBanner: document.getElementById("billingHealthBanner"),
        dueCtaBtn: document.getElementById("billingDueCtaBtn"),
        demoBadge: document.getElementById("billingDemoBadge"),
        statusPill: document.getElementById("billingStatusPill"),
        planLine: document.getElementById("billingPlanLine"),
        priceLine: document.getElementById("billingPriceLine"),
        nextBillLine: document.getElementById("billingNextBillLine"),
        seatsLine: document.getElementById("billingSeatsLine"),
        lockLine: document.getElementById("billingLockLine"),
        dunningLine: document.getElementById("billingDunningLine"),
        paymentMethodLine: document.getElementById("billingPaymentMethodLine"),
        usageResetLine: document.getElementById("billingUsageResetLine"),
        usageRevenue: document.getElementById("billingUsageRevenue"),
        usageAutomations: document.getElementById("billingUsageAutomations"),
        usageConversations: document.getElementById("billingUsageConversations"),
        usageRevenueBar: document.getElementById("billingUsageRevenueBar"),
        usageAutomationsBar: document.getElementById("billingUsageAutomationsBar"),
        usageConversationsBar: document.getElementById("billingUsageConversationsBar"),
        invoicesTbody: document.getElementById("billingInvoicesTbody"),
        invoicesFooter: document.getElementById("billingInvoicesFooter"),
        activityTbody: document.getElementById("billingActivityTbody"),
        updatedAt: document.getElementById("billingUpdatedAt"),
        detailsStatus: document.getElementById("billingDetailsStatus"),
        detailsSaveBtn: document.getElementById("billingDetailsSaveBtn"),
        portalBtn: document.getElementById("billingPortalBtn"),
        compareBtn: document.getElementById("billingCompareBtn"),
        startTrialBtn: document.getElementById("billingStartTrialBtn"),
        cancelBtn: document.getElementById("billingCancelBtn"),
        updatePaymentBtn: document.getElementById("billingUpdatePaymentBtn"),
        stripeConnectionStatus: document.getElementById("billingStripeConnectionStatus"),
        stripeConnectBtn: document.getElementById("billingStripeConnectBtn"),
        stripeRefreshBtn: document.getElementById("billingStripeRefreshBtn"),
        companyName: document.getElementById("billingCompanyName"),
        email: document.getElementById("billingEmail"),
        address1: document.getElementById("billingAddress1"),
        address2: document.getElementById("billingAddress2"),
        city: document.getElementById("billingCity"),
        state: document.getElementById("billingState"),
        postalCode: document.getElementById("billingPostalCode"),
        country: document.getElementById("billingCountry"),
        taxId: document.getElementById("billingTaxId"),
        planModal: document.getElementById("billingPlanModal"),
        planModalCloseBtn: document.getElementById("billingPlanModalCloseBtn"),
        cadenceMonthlyBtn: document.getElementById("billingCadenceMonthlyBtn"),
        cadenceAnnualBtn: document.getElementById("billingCadenceAnnualBtn"),
        planGrid: document.getElementById("billingPlanGrid"),
        infoModal: document.getElementById("billingInfoModal"),
        infoModalTitle: document.getElementById("billingInfoModalTitle"),
        infoModalBody: document.getElementById("billingInfoModalBody"),
        infoModalCloseBtn: document.getElementById("billingInfoModalCloseBtn"),
        confirmModal: document.getElementById("billingConfirmModal"),
        confirmModalTitle: document.getElementById("billingConfirmModalTitle"),
        confirmModalBody: document.getElementById("billingConfirmModalBody"),
        confirmCancelBtn: document.getElementById("billingConfirmCancelBtn"),
        confirmActionBtn: document.getElementById("billingConfirmActionBtn"),
        viewAllInvoicesBtn: document.getElementById("billingViewAllInvoicesBtn")
      };

      const PLAN_FEATURES = {
        discountAnnual: 0.15,
        starter: {
          key: "starter",
          name: "Starter",
          monthlyPrice: 79,
          blurb: "Core AI intake and follow-up for smaller teams that want consistent lead capture.",
          featureValues: {
            monitoredOpportunities: "100",
            prmFrequency: "15 min",
            outcomePacks: "5",
            aiReceptionist: "Intake + booking nudges",
            reportingDepth: "Revenue overview",
            revenueAnalytics: "Core metrics"
          },
          addedBenefits: [
            "Single workspace",
            "Smart intake prompts",
            "Lead recovery baseline"
          ]
        },
        pro: {
          key: "pro",
          name: "Pro",
          monthlyPrice: 129,
          blurb: "Faster automations and deeper analytics for teams actively scaling booked revenue.",
          recommendation: "Recommended",
          popularLabel: "Best for growing teams",
          featureValues: {
            monitoredOpportunities: "500",
            prmFrequency: "5 min",
            outcomePacks: "Unlimited",
            aiReceptionist: "Full intake + escalation",
            reportingDepth: "Executive dashboard",
            revenueAnalytics: "ROI + funnel metrics"
          },
          addedBenefits: [
            "Priority support",
            "Policy controls",
            "Advanced playbooks"
          ]
        },
        growth: {
          key: "growth",
          name: "Growth",
          monthlyPrice: 249,
          blurb: "Multi-team controls, higher limits, and tighter operational guardrails for larger accounts.",
          featureValues: {
            monitoredOpportunities: "2000",
            prmFrequency: "1 min",
            outcomePacks: "Unlimited",
            aiReceptionist: "Full + high-priority routing",
            reportingDepth: "Exec + operator views",
            revenueAnalytics: "Cohorts + advanced forecasts"
          },
          addedBenefits: [
            "Higher seat limits",
            "Priority incident response",
            "Advanced governance"
          ]
        }
      };
      const PLAN_FEATURE_ROWS = [
        { key: "monitoredOpportunities", label: "Monitored opportunities" },
        { key: "prmFrequency", label: "PRM risk cadence" },
        { key: "outcomePacks", label: "Enabled outcome packs" },
        { key: "aiReceptionist", label: "AI receptionist capability" },
        { key: "reportingDepth", label: "Reporting depth" },
        { key: "revenueAnalytics", label: "Revenue analytics" }
      ];
      const BILLING_PLAN_OVERRIDE_KEY = "mc_billing_plan_override_v1";
      const BILLING_CADENCE_KEY = "mc_billing_cadence_v1";

      let billingState = {
        summary: null,
        invoices: [],
        demoMode: true,
        accountId: "",
        cadence: "monthly",
        detailsBaseline: "",
        portalUrl: null,
        portalMessage: "Billing Portal available after connecting Stripe",
        confirmAction: null
      };
      let billingLastFocused = null;
      const isSuperadminBilling = false;
      const isSuperadminUser = String(authState?.user?.role || "").toLowerCase() === "superadmin";

      function money(cents) {
        const n = Number(cents || 0) / 100;
        return n.toLocaleString("en-US", { style: "currency", currency: "USD" });
      }
      function moneyWhole(cents) {
        const n = Number(cents || 0) / 100;
        return n.toLocaleString("en-US", { style: "currency", currency: "USD", maximumFractionDigits: 0 });
      }
      function dt(ts) {
        if (!ts) return "?";
        return new Date(Number(ts)).toLocaleDateString();
      }
      function ago(ts) {
        if (!ts) return "?";
        return formatRelativeTs(Number(ts));
      }
      function clampPct(used, limit) {
        if (!limit || limit <= 0) return 0;
        return Math.max(0, Math.min(100, Math.round((Number(used || 0) / Number(limit)) * 100)));
      }
      function openStripeConnectionModalFromBilling() {
        const modal = document.getElementById("integrationStripeModal");
        if (!modal) {
          showSettingsToast("Stripe setup modal is unavailable right now.", true);
          return;
        }
        modal.classList.remove("hidden");
        modal.setAttribute("aria-hidden", "false");
      }
      async function refreshBillingStripeConnection() {
        try {
          const integrations = await apiGet("/api/integrations");
          const stripe = integrations?.stripe || {};
          const enabled = stripe?.enabled === true;
          const accountId = String(stripe?.accountId || "").trim();
          const status = String(stripe?.lastStatus || "").toLowerCase();
          if (billingEls.stripeConnectionStatus) {
            if (enabled) {
              billingEls.stripeConnectionStatus.textContent = accountId
                ? `Connected (${accountId})`
                : "Connected";
            } else if (status === "error") {
              billingEls.stripeConnectionStatus.textContent = `Connection error: ${String(stripe?.lastError || "Needs attention")}`;
            } else {
              billingEls.stripeConnectionStatus.textContent = "Not connected";
            }
          }
          if (billingEls.stripeConnectBtn) {
            billingEls.stripeConnectBtn.textContent = enabled ? "Manage" : "Connect";
          }
        } catch (err) {
          if (billingEls.stripeConnectionStatus) billingEls.stripeConnectionStatus.textContent = "Unable to load connection status";
          if (billingEls.stripeConnectBtn) billingEls.stripeConnectBtn.textContent = "Connect";
        }
      }
      function normalizePlanKey(planKey) {
        const key = String(planKey || "").toLowerCase();
        if (key === "basic" || key === "starter") return "starter";
        if (key === "pro") return "pro";
        if (key === "growth") return "growth";
        return "pro";
      }
      function getTenantBillingScopeId() {
        return String(billingState.accountId || getActiveTo() || "default");
      }
      function getScopedPrefKey(baseKey) {
        return `${baseKey}__${getTenantBillingScopeId()}`;
      }
      function loadTenantCadencePref() {
        const saved = String(localStorage.getItem(getScopedPrefKey(BILLING_CADENCE_KEY)) || "").toLowerCase();
        return saved === "annual" ? "annual" : "monthly";
      }
      function saveTenantCadencePref(cadence) {
        const next = cadence === "annual" ? "annual" : "monthly";
        billingState.cadence = next;
        localStorage.setItem(getScopedPrefKey(BILLING_CADENCE_KEY), next);
      }
      function loadTenantPlanOverride() {
        const saved = String(localStorage.getItem(getScopedPrefKey(BILLING_PLAN_OVERRIDE_KEY)) || "").toLowerCase();
        if (saved === "basic") return "starter";
        return saved === "starter" || saved === "pro" || saved === "growth" ? saved : null;
      }
      function saveTenantPlanOverride(planKey) {
        const next = normalizePlanKey(planKey);
        localStorage.setItem(getScopedPrefKey(BILLING_PLAN_OVERRIDE_KEY), next);
      }
      function annualPricingFor(monthlyPrice) {
        const monthly = Number(monthlyPrice || 0);
        const fullYear = monthly * 12;
        const discounted = fullYear * (1 - PLAN_FEATURES.discountAnnual);
        return { fullYear, discounted };
      }
      function fmtUsd(amount, { decimals = 0 } = {}) {
        return Number(amount || 0).toLocaleString("en-US", {
          style: "currency",
          currency: "USD",
          minimumFractionDigits: decimals,
          maximumFractionDigits: decimals
        });
      }
      function syncCadenceButtons() {
        if (billingEls.cadenceMonthlyBtn) billingEls.cadenceMonthlyBtn.classList.toggle("is-active", billingState.cadence !== "annual");
        if (billingEls.cadenceAnnualBtn) billingEls.cadenceAnnualBtn.classList.toggle("is-active", billingState.cadence === "annual");
      }
      function openBillingPortal({ plan, cadence }) {
        if (billingState.portalUrl) {
          window.open(billingState.portalUrl, "_blank", "noopener,noreferrer");
          return;
        }
        showBillingInfoModal(
          "Manage subscription",
          `Live billing portal is not connected yet. Requested plan=${plan}, cadence=${cadence}.`
        );
      }
      async function openBillingCheckout({ planKey = "", cadence = "" } = {}) {
        try {
          const returnUrl = getBillingReturnUrl();
          const selectedPlan = normalizePlanKey(planKey || billingState.summary?.plan?.key || "pro");
          const selectedCadence = cadence === "annual" ? "annual" : "monthly";
          const result = await apiPost("/api/billing/checkout", {
            returnUrl,
            planKey: selectedPlan,
            cadence: selectedCadence
          });
          const url = String(result?.url || "").trim();
          if (url) {
            window.open(url, "_blank", "noopener,noreferrer");
            return;
          }
          showBillingInfoModal("Billing renewal", "Checkout link is unavailable right now.");
        } catch (err) {
          showBillingInfoModal("Billing renewal", err?.message || "Failed to start billing renewal checkout.");
        }
      }
      function openBillingConfirmModal({ title, body, confirmLabel = "Confirm", onConfirm }) {
        if (billingEls.confirmModalTitle) billingEls.confirmModalTitle.textContent = title;
        if (billingEls.confirmModalBody) billingEls.confirmModalBody.textContent = body;
        if (billingEls.confirmActionBtn) billingEls.confirmActionBtn.textContent = confirmLabel;
        billingState.confirmAction = typeof onConfirm === "function" ? onConfirm : null;
        openBillingModal(billingEls.confirmModal);
      }
      function readBillingDetailsFromUI() {
        return {
          companyName: String(billingEls.companyName?.value || "").trim(),
          billingEmail: String(billingEls.email?.value || "").trim(),
          addressLine1: String(billingEls.address1?.value || "").trim(),
          addressLine2: String(billingEls.address2?.value || "").trim(),
          city: String(billingEls.city?.value || "").trim(),
          state: String(billingEls.state?.value || "").trim(),
          postalCode: String(billingEls.postalCode?.value || "").trim(),
          country: String(billingEls.country?.value || "").trim(),
          taxId: String(billingEls.taxId?.value || "").trim()
        };
      }
      function setBillingDirtyState() {
        if (!billingEls.detailsSaveBtn) return;
        const now = JSON.stringify(readBillingDetailsFromUI());
        const validEmail = !billingEls.email?.value || /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(String(billingEls.email?.value || "").trim());
        const dirty = now !== billingState.detailsBaseline && validEmail;
        billingEls.detailsSaveBtn.disabled = !dirty;
        billingEls.detailsSaveBtn.classList.toggle("is-dirty", dirty);
        if (!validEmail) {
          if (billingEls.detailsStatus) billingEls.detailsStatus.textContent = "Billing email is invalid.";
        } else if (billingEls.detailsStatus?.textContent === "Billing email is invalid.") {
          billingEls.detailsStatus.textContent = "";
        }
      }

      function openBillingModal(el) {
        if (!el) return;
        billingLastFocused = document.activeElement;
        el.classList.remove("hidden");
        el.setAttribute("aria-hidden", "false");
        const first = el.querySelector("button, [href], input, select, textarea, [tabindex]:not([tabindex='-1'])");
        first?.focus();
      }
      function closeBillingModal(el) {
        if (!el) return;
        el.classList.add("hidden");
        el.setAttribute("aria-hidden", "true");
        if (billingLastFocused && typeof billingLastFocused.focus === "function") billingLastFocused.focus();
      }
      function showBillingInfoModal(title, body) {
        if (billingEls.infoModalTitle) billingEls.infoModalTitle.textContent = title;
        if (billingEls.infoModalBody) billingEls.infoModalBody.textContent = body;
        openBillingModal(billingEls.infoModal);
      }

      function formatLockReason(reason) {
        const key = String(reason || "").trim().toLowerCase();
        if (!key) return "Unknown";
        if (key === "payment_unpaid") return "Unpaid after retries";
        if (key === "plan_canceled") return "Plan canceled";
        if (key === "trial_expired") return "Trial expired";
        return key.replace(/_/g, " ");
      }

      function renderBillingHealthBanner(summary) {
        if (!billingEls.healthBanner) return;
        const plan = summary?.plan || {};
        const lock = summary?.lock || {};
        const dunning = summary?.dunning || {};
        const status = String(plan?.status || "").toLowerCase();
        const billDue = isBillDueStatus(status);
        if (lock.locked === true) {
          billingEls.healthBanner.className = "billing-health-banner is-past-due";
          billingEls.healthBanner.textContent = `Access locked: ${formatLockReason(lock.reason)}.`;
          billingEls.healthBanner.classList.remove("hidden");
          if (billingEls.dueCtaBtn) billingEls.dueCtaBtn.classList.remove("hidden");
          return;
        }
        if (billDue) {
          billingEls.healthBanner.className = "billing-health-banner is-past-due";
          billingEls.healthBanner.textContent = dunning?.nextRetryAt
            ? `Payment failed. Next retry ${dt(dunning.nextRetryAt)}.`
            : "Payment failed. Update payment method.";
          billingEls.healthBanner.classList.remove("hidden");
          if (billingEls.dueCtaBtn) billingEls.dueCtaBtn.classList.remove("hidden");
          return;
        }
        if (status === "trialing") {
          const days = Number(plan?.trialDaysLeft || 0);
          billingEls.healthBanner.className = "billing-health-banner is-trialing";
          billingEls.healthBanner.textContent = `Trial ends in ${days} day${days === 1 ? "" : "s"}.`;
          billingEls.healthBanner.classList.remove("hidden");
          if (billingEls.dueCtaBtn) billingEls.dueCtaBtn.classList.add("hidden");
          return;
        }
        if (status === "active") {
          billingEls.healthBanner.className = "billing-health-banner is-ok";
          billingEls.healthBanner.textContent = "All set. Your subscription is active.";
          billingEls.healthBanner.classList.remove("hidden");
          if (billingEls.dueCtaBtn) billingEls.dueCtaBtn.classList.add("hidden");
          return;
        }
        billingEls.healthBanner.classList.add("hidden");
        if (billingEls.dueCtaBtn) billingEls.dueCtaBtn.classList.add("hidden");
      }

      function renderBillingInvoices() {
        if (!billingEls.invoicesTbody) return;
        const invoices = Array.isArray(billingState.invoices) ? billingState.invoices : [];
        if (!invoices.length) {
          billingEls.invoicesTbody.innerHTML = `<tr><td colspan="5" class="billing-empty-row">No invoices yet.</td></tr>`;
          if (billingEls.invoicesFooter) billingEls.invoicesFooter.textContent = "Showing 0 of 0";
          return;
        }
        billingEls.invoicesTbody.innerHTML = invoices.slice(0, 5).map((inv) => `
          <tr>
            <td>${escapeHtml(dt(inv.date))}</td>
            <td>${escapeHtml(inv.number || inv.id || "?")}</td>
            <td>${escapeHtml(money(inv.amount))}</td>
            <td><span class="billing-status-pill is-${escapeAttr(String(inv.status || "").toLowerCase())}">${escapeHtml(String(inv.status || "").replace("_", " "))}</span></td>
            <td>
              <button class="btn billing-download-btn" data-billing-invoice-id="${escapeAttr(inv.id)}" ${!inv.pdfUrl ? "disabled title=\"Connect billing provider to download PDFs.\"" : ""}>Download PDF</button>
            </td>
          </tr>
        `).join("");
        if (billingEls.invoicesFooter) billingEls.invoicesFooter.textContent = `Showing ${Math.min(5, invoices.length)} of ${invoices.length}`;
      }

      function renderBillingActivity(summary) {
        if (!billingEls.activityTbody) return;
        const items = Array.isArray(summary?.activity) ? summary.activity.slice(0, 5) : [];
        if (!items.length) {
          billingEls.activityTbody.innerHTML = `<tr><td colspan="3" class="billing-empty-row">No billing activity yet.</td></tr>`;
          return;
        }
        billingEls.activityTbody.innerHTML = items.map((x) => `
          <tr>
            <td>${escapeHtml(ago(x.ts))}</td>
            <td>${escapeHtml(x.type || "event")}</td>
            <td>${escapeHtml(x.message || "")}</td>
          </tr>
        `).join("");
      }

      function renderBillingPlanModal(summary) {
        if (!billingEls.planGrid) return;
        const current = normalizePlanKey(summary?.plan?.key);
        const cadence = billingState.cadence === "annual" ? "annual" : "monthly";
        syncCadenceButtons();

        function renderPrice(planCfg) {
          if (cadence === "monthly") {
            return `
              <div class="billing-plan-price">${escapeHtml(fmtUsd(planCfg.monthlyPrice))} / month</div>
            `;
          }
          const annual = annualPricingFor(planCfg.monthlyPrice);
          return `
            <div class="billing-plan-price-row">
              <span class="billing-price-strike">${escapeHtml(fmtUsd(annual.fullYear))}/yr</span>
              <span class="badge">Save 15%</span>
            </div>
            <div class="billing-plan-price">${escapeHtml(fmtUsd(Math.round(annual.discounted)))} / yr</div>
          `;
        }

        function renderFeatureList(planCfg) {
          return PLAN_FEATURE_ROWS.map((row) => {
            const value = String(planCfg.featureValues?.[row.key] || "?");
            const isCheck = value === "YES";
            const isDash = value === "?";
            return `
              <li class="billing-compare-row">
                <span class="billing-compare-label">${escapeHtml(row.label)}</span>
                <span class="billing-compare-value ${isCheck ? "is-check" : ""} ${isDash ? "is-dash" : ""}">${escapeHtml(value)}</span>
              </li>
            `;
          }).join("");
        }

        function renderValueMeter(summary) {
          const planKey = normalizePlanKey(summary?.plan?.key);
          const planCfg = PLAN_FEATURES[planKey] || PLAN_FEATURES.pro;
          const revenueUsage = Number(summary?.usage?.revenue?.used || 0);
          const planCents = Math.round((planCfg.monthlyPrice || 0) * 100);
          const percentage = planCents ? Math.min(100, Math.round((revenueUsage / planCents) * 100)) : 0;
          const roi = planCents ? Number(revenueUsage / planCents).toFixed(1) : "0.0";
          return `
            <div class="value-meter-card">
              <div class="value-meter-header">
                <div>
                  <span class="muted">Recovered revenue this month</span>
                  <div class="h2" style="margin:4px 0 0;">${moneyFromCents(revenueUsage)}</div>
                </div>
                <div>
                  <span class="muted">ROI vs subscription</span>
                  <strong>${roi}x</strong>
                </div>
              </div>
              <div class="value-meter-bar">
                <span style="width:${percentage}%"></span>
              </div>
              <div class="value-meter-foot">
                <span>${percentage}% of monthly cost recovered</span>
                <span>Plan cost: ${fmtUsd(planCfg.monthlyPrice)}</span>
              </div>
            </div>
          `;
        }

        const planOrder = ["starter", "pro", "growth"];
        billingEls.planGrid.innerHTML = planOrder.map((planKey) => {
          const cfg = PLAN_FEATURES[planKey];
          if (!cfg) return "";
          const isCurrent = current === planKey;
          const cardCls = planKey === "pro" ? "billing-plan-card-pro" : "billing-plan-card-basic";
          const btnCls = planKey === "pro" ? "btn primary" : "btn";
          const label = isCurrent ? "Current plan" : (planKey === "starter" ? "Switch to Starter" : (planKey === "growth" ? "Upgrade to Growth" : "Switch to Pro"));
          return `
            <div class="billing-plan-card ${cardCls} ${isCurrent ? "is-current" : ""}" data-billing-plan-card="${escapeAttr(planKey)}">
              <div class="row" style="justify-content:space-between; align-items:center; gap:8px;">
                <div class="h1" style="margin:0;">${escapeHtml(cfg.name)}</div>
                ${cfg.recommendation ? `<span class="badge">${escapeHtml(cfg.recommendation)}</span>` : ""}
              </div>
              ${renderPrice(cfg)}
              ${cfg.popularLabel ? `<div class="p billing-pro-popular">${escapeHtml(cfg.popularLabel)}</div>` : ""}
              <div class="p">${escapeHtml(cfg.blurb)}</div>
              <ul class="billing-plan-features">${renderFeatureList(cfg)}</ul>
              <button class="${btnCls}" data-billing-select-plan="${escapeAttr(planKey)}" type="button" ${isCurrent ? "disabled" : ""}>
                ${escapeHtml(label)}
              </button>
            </div>
          `;
        }).join("");
      }

      function renderBillingSummary() {
        const summary = billingState.summary || {};
        const plan = summary.plan || {};
        const lock = summary.lock || {};
        const dunning = summary.dunning || {};
        const currentPlanKey = normalizePlanKey(plan.key);
        const payment = summary.paymentMethod || null;
        const usage = summary.usage || {};

        renderBillingHealthBanner(summary);

        if (billingEls.demoBadge) billingEls.demoBadge.classList.toggle("hidden", billingState.demoMode !== true);
        if (billingEls.statusPill) {
          billingEls.statusPill.textContent = String(plan.status || "active").replace("_", " ");
          billingEls.statusPill.className = `billing-status-pill is-${String(plan.status || "active").toLowerCase()}`;
        }
        if (billingEls.planLine) billingEls.planLine.textContent = `${String(plan.name || currentPlanKey || "Plan")} plan`;
        if (billingEls.priceLine) billingEls.priceLine.textContent = `$${Number(plan.priceMonthly || 0)} / ${plan.interval || "month"}`;
        if (billingEls.nextBillLine) {
          billingEls.nextBillLine.textContent = plan.status === "canceled"
            ? `Ends on ${dt(plan.endsAt)}`
            : `Next billing date: ${dt(plan.nextBillingAt)}`;
        }
        if (billingEls.seatsLine) billingEls.seatsLine.textContent = `Seats: ${Number(plan?.seats?.used || 0)} / ${Number(plan?.seats?.total || 1)} used`;
        if (billingEls.lockLine) {
          billingEls.lockLine.textContent = lock.locked === true
            ? `Access: Locked (${formatLockReason(lock.reason)})`
            : "Access: Active";
        }
        if (billingEls.dunningLine) {
          const attempts = `${Number(dunning.attempts || 0)} / ${Number(dunning.maxAttempts || 4)}`;
          const nextRetry = dunning.nextRetryAt ? ` | Next retry: ${dt(dunning.nextRetryAt)}` : "";
          const grace = dunning.graceEndsAt ? ` | Grace ends: ${dt(dunning.graceEndsAt)}` : "";
          billingEls.dunningLine.textContent = `Dunning attempts: ${attempts}${nextRetry}${grace}`;
        }
        if (billingEls.portalBtn) billingEls.portalBtn.textContent = "Manage subscription";
        if (billingEls.compareBtn) {
          const isStarter = currentPlanKey === "starter";
          billingEls.compareBtn.textContent = isStarter ? "Upgrade plan" : "Change plan";
          billingEls.compareBtn.classList.toggle("primary", isStarter);
        }
        if (billingEls.startTrialBtn) {
          const isTrialing = String(plan.status || "").toLowerCase() === "trialing";
          billingEls.startTrialBtn.disabled = isTrialing;
          billingEls.startTrialBtn.textContent = isTrialing ? "Trial active" : "Start trial";
        }
        if (billingEls.paymentMethodLine) {
          billingEls.paymentMethodLine.textContent = payment
            ? `${payment.brand || "Card"} **** ${payment.last4 || "----"} | Expires ${String(payment.expMonth || "").padStart(2, "0")}/${payment.expYear || ""}`
            : "No payment method on file";
        }
        if (billingEls.usageResetLine) billingEls.usageResetLine.textContent = `Resets on ${dt(usage?.cycleResetsAt)}`;

        const revenue = usage?.revenueRecovered || usage?.messagesSent || {};
        const a = usage?.automationsRun || {};
        const c = usage?.activeConversations || {};
        if (billingEls.usageRevenue) billingEls.usageRevenue.textContent = `${Number(revenue.used || 0)} / ${Number(revenue.limit || 0)}`;
        if (billingEls.usageAutomations) billingEls.usageAutomations.textContent = `${Number(a.used || 0)} / ${Number(a.limit || 0)}`;
        if (billingEls.usageConversations) billingEls.usageConversations.textContent = `${Number(c.used || 0)} / ${Number(c.limit || 0)}`;
        if (billingEls.usageRevenueBar) billingEls.usageRevenueBar.style.width = `${clampPct(revenue.used, revenue.limit)}%`;
        if (billingEls.usageAutomationsBar) billingEls.usageAutomationsBar.style.width = `${clampPct(a.used, a.limit)}%`;
        if (billingEls.usageConversationsBar) billingEls.usageConversationsBar.style.width = `${clampPct(c.used, c.limit)}%`;
        if (billingEls.updatedAt) billingEls.updatedAt.textContent = `Last billing update: ${new Date(Number(summary.updatedAt || Date.now())).toLocaleString()}`;

        const d = summary.details || {};
        if (billingEls.companyName) billingEls.companyName.value = d.companyName || "";
        if (billingEls.email) billingEls.email.value = d.billingEmail || "";
        if (billingEls.address1) billingEls.address1.value = d.addressLine1 || "";
        if (billingEls.address2) billingEls.address2.value = d.addressLine2 || "";
        if (billingEls.city) billingEls.city.value = d.city || "";
        if (billingEls.state) billingEls.state.value = d.state || "";
        if (billingEls.postalCode) billingEls.postalCode.value = d.postalCode || "";
        if (billingEls.country) billingEls.country.value = d.country || "";
        if (billingEls.taxId) billingEls.taxId.value = d.taxId || "";
        billingState.detailsBaseline = JSON.stringify(readBillingDetailsFromUI());
        setBillingDirtyState();

        renderBillingActivity(summary);
        renderBillingPlanModal(summary);
      }

      function renderMasterBillingOverview(overview) {
        if (!billingEls.masterCard) return;
        if (!isSuperadminBilling) {
          billingEls.masterCard.style.display = "none";
          return;
        }
        billingEls.masterCard.style.display = "";
        const sum = overview?.summary || {};
        if (billingEls.masterAsOf) billingEls.masterAsOf.textContent = `As of ${new Date(Number(overview?.asOf || Date.now())).toLocaleString()}`;
        if (billingEls.masterPaidRevenue) billingEls.masterPaidRevenue.textContent = moneyWhole(sum.paidRevenueCents || 0);
        if (billingEls.masterOutstanding) billingEls.masterOutstanding.textContent = moneyWhole(sum.outstandingCents || 0);
        if (billingEls.masterRunRate) billingEls.masterRunRate.textContent = moneyWhole(sum.monthlyRunRateCents || 0);
        if (billingEls.masterConnected) billingEls.masterConnected.textContent = `${Number(sum.stripeConnectedCount || 0)} / ${Number(sum.workspaceCount || 0)}`;
        if (billingEls.masterStatsLine) {
          billingEls.masterStatsLine.textContent = `Invoices tracked: ${Number(sum.invoiceCount || 0)} | Last 30d payments: ${Number(sum.recentPaymentsCount || 0)} (${moneyWhole(sum.recentPaymentsCents || 0)})`;
        }
        if (billingEls.masterTopBody) {
          const rows = Array.isArray(overview?.topWorkspaces) ? overview.topWorkspaces : [];
          if (!rows.length) {
            billingEls.masterTopBody.innerHTML = `<tr><td colspan="3" class="billing-empty-row">No workspace revenue yet.</td></tr>`;
          } else {
            billingEls.masterTopBody.innerHTML = rows.map((row) => `
              <tr>
                <td>${escapeHtml(row.businessName || "Workspace")}</td>
                <td>${escapeHtml(row.to || "--")}</td>
                <td>${escapeHtml(money(row.paidRevenueCents || 0))}</td>
              </tr>
            `).join("");
          }
        }
      }

      async function loadMasterBillingOverview() {
        if (!isSuperadminBilling) {
          if (billingEls.masterCard) billingEls.masterCard.style.display = "none";
          return;
        }
        try {
          const overview = await apiGet("/api/admin/billing/overview");
          renderMasterBillingOverview(overview || {});
        } catch (err) {
          if (billingEls.masterCard) billingEls.masterCard.style.display = "none";
          console.error("Master billing overview load failed:", err);
        }
      }

      function applyDemoPlanToSummary(summary, planKey) {
        if (!summary || typeof summary !== "object") return summary;
        const normalized = normalizePlanKey(planKey);
        const cfg = PLAN_FEATURES[normalized] || PLAN_FEATURES.pro;
        const next = { ...summary };
        next.plan = {
          ...(summary.plan || {}),
          key: cfg.key,
          name: cfg.name,
          priceMonthly: Number(cfg.monthlyPrice || 0),
          seats: {
            ...(summary?.plan?.seats || {}),
            total: Number(summary?.plan?.seats?.total || 0) || (cfg.key === "starter" ? 3 : (cfg.key === "pro" ? 10 : 25))
          }
        };
        return next;
      }

      function applyTenantBillingPreferences(summary) {
        const scopedCadence = loadTenantCadencePref();
        billingState.cadence = scopedCadence;
        syncCadenceButtons();
        if (billingState.demoMode !== true) return summary;
        const overrideKey = loadTenantPlanOverride();
        if (!overrideKey) return summary;
        return applyDemoPlanToSummary(summary, overrideKey);
      }

      async function loadBillingData() {
        try {
          const [summaryRes, invoiceRes, portalRes] = await Promise.all([
            apiGet("/api/billing/summary"),
            apiGet("/api/billing/invoices"),
            apiGet("/api/billing/portal")
          ]);
          billingState.accountId = String(summaryRes?.accountId || billingState.accountId || getActiveTo());
          let nextSummary = summaryRes?.billing || null;
          billingState.demoMode = summaryRes?.demoMode !== false;
          nextSummary = applyTenantBillingPreferences(nextSummary);
          billingState.summary = nextSummary;
          navBillDueState.planKey = normalizeNavPlanKey(nextSummary?.plan?.key);
          navBillDueState.status = String(nextSummary?.plan?.status || "").toLowerCase();
          syncNavBillDueButton();
          billingState.invoices = invoiceRes?.invoices || [];
          billingState.portalUrl = portalRes?.url || null;
          billingState.portalMessage = portalRes?.message || "Billing Portal available after connecting Stripe";
          renderBillingSummary();
          renderBillingInvoices();
          await refreshBillingStripeConnection();
          await loadMasterBillingOverview();
          if (billingEls.viewAllInvoicesBtn) billingEls.viewAllInvoicesBtn.disabled = billingState.demoMode;
        } catch (err) {
          console.error("Billing settings load failed:", err);
          const now = Date.now();
          billingState.accountId = String(billingState.accountId || getActiveTo());
          let fallbackSummary = {
            provider: "demo",
            isLive: false,
            plan: {
              key: "starter",
              name: "Starter",
              priceMonthly: 79,
              interval: "month",
              status: "active",
              trialEndsAt: null,
              trialDaysLeft: null,
              nextBillingAt: now + (1000 * 60 * 60 * 24 * 18),
              endsAt: null,
              seats: { used: 0, total: 3 }
            },
            lock: { locked: false, reason: null },
            dunning: { attempts: 0, maxAttempts: 4, nextRetryAt: null, graceEndsAt: null, lockedAt: null },
            usage: {
              cycleResetsAt: now + (1000 * 60 * 60 * 24 * 18),
              messagesSent: { used: 0, limit: 0 },
              automationsRun: { used: 0, limit: 0 },
              activeConversations: { used: 0, limit: 0 }
            },
            paymentMethod: null,
            details: {
              companyName: "",
              billingEmail: "",
              addressLine1: "",
              addressLine2: "",
              city: "",
              state: "",
              postalCode: "",
              country: "US",
              taxId: ""
            },
            activity: [],
            updatedAt: now
          };
          billingState.demoMode = true;
          fallbackSummary = applyTenantBillingPreferences(fallbackSummary);
          billingState.summary = fallbackSummary;
          navBillDueState.planKey = normalizeNavPlanKey(fallbackSummary?.plan?.key);
          navBillDueState.status = String(fallbackSummary?.plan?.status || "").toLowerCase();
          syncNavBillDueButton();
          billingState.invoices = [];
          billingState.portalUrl = null;
          billingState.portalMessage = "Billing APIs are in demo mode.";
          renderBillingSummary();
          renderBillingInvoices();
          await refreshBillingStripeConnection();
          await loadMasterBillingOverview();
          if (billingEls.viewAllInvoicesBtn) billingEls.viewAllInvoicesBtn.disabled = true;
          showSettingsToast("Billing is running in demo mode right now.", true);
        }
      }

      function trapFocusInModal(modalEl, e) {
        if (e.key !== "Tab") return;
        const focusables = Array.from(modalEl.querySelectorAll("button,[href],input,select,textarea,[tabindex]:not([tabindex='-1'])"))
          .filter((el) => !el.disabled);
        if (!focusables.length) return;
        const first = focusables[0];
        const last = focusables[focusables.length - 1];
        if (e.shiftKey && document.activeElement === first) {
          e.preventDefault();
          last.focus();
        } else if (!e.shiftKey && document.activeElement === last) {
          e.preventDefault();
          first.focus();
        }
      }

      function bindBillingEvents() {
        [
          billingEls.companyName, billingEls.email, billingEls.address1, billingEls.address2,
          billingEls.city, billingEls.state, billingEls.postalCode, billingEls.country, billingEls.taxId
        ].forEach((el) => el?.addEventListener("input", setBillingDirtyState));

        billingEls.detailsSaveBtn?.addEventListener("click", async () => {
          try {
            const details = readBillingDetailsFromUI();
            await apiPatch("/api/billing/details", details);
            billingState.detailsBaseline = JSON.stringify(details);
            setBillingDirtyState();
            if (billingEls.detailsStatus) billingEls.detailsStatus.textContent = "Saved";
            showSettingsToast("Billing details saved");
            await loadBillingData();
          } catch (err) {
            if (billingEls.detailsStatus) billingEls.detailsStatus.textContent = "Failed to save.";
            showSettingsToast(err?.message || "Failed to save billing details", true);
          }
        });

        billingEls.portalBtn?.addEventListener("click", () => {
          openBillingPortal({ plan: normalizePlanKey(billingState.summary?.plan?.key), cadence: billingState.cadence });
        });
        billingEls.dueCtaBtn?.addEventListener("click", () => {
          openBillingCheckout({ planKey: normalizePlanKey(billingState.summary?.plan?.key), cadence: billingState.cadence });
        });

        billingEls.updatePaymentBtn?.addEventListener("click", () => {
          showBillingInfoModal("Update payment method", billingState.demoMode
            ? "Payment method updates are available after connecting Stripe."
            : "Redirecting to payment method update flow.");
        });
        billingEls.stripeConnectBtn?.addEventListener("click", () => {
          openStripeConnectionModalFromBilling();
        });
        billingEls.stripeRefreshBtn?.addEventListener("click", async () => {
          await refreshBillingStripeConnection();
        });

        billingEls.compareBtn?.addEventListener("click", () => {
          billingState.cadence = loadTenantCadencePref();
          renderBillingPlanModal(billingState.summary || {});
          openBillingModal(billingEls.planModal);
        });
        billingEls.startTrialBtn?.addEventListener("click", async () => {
          try {
            await apiPost("/api/billing/trial/start", { days: 14 });
            showSettingsToast("14-day trial started.");
            await loadBillingData();
          } catch (err) {
            showSettingsToast(err?.message || "Failed to start trial", true);
          }
        });
        billingEls.planModalCloseBtn?.addEventListener("click", () => closeBillingModal(billingEls.planModal));
        billingEls.infoModalCloseBtn?.addEventListener("click", () => closeBillingModal(billingEls.infoModal));
        billingEls.confirmCancelBtn?.addEventListener("click", () => closeBillingModal(billingEls.confirmModal));
        billingEls.confirmActionBtn?.addEventListener("click", async () => {
          const fn = billingState.confirmAction;
          closeBillingModal(billingEls.confirmModal);
          billingState.confirmAction = null;
          if (typeof fn === "function") {
            try {
              await fn();
            } catch (err) {
              showSettingsToast(err?.message || "Plan update failed", true);
            }
          }
        });

        billingEls.cadenceMonthlyBtn?.addEventListener("click", () => {
          saveTenantCadencePref("monthly");
          renderBillingPlanModal(billingState.summary || {});
        });
        billingEls.cadenceAnnualBtn?.addEventListener("click", () => {
          saveTenantCadencePref("annual");
          renderBillingPlanModal(billingState.summary || {});
        });

        billingEls.planGrid?.addEventListener("click", (e) => {
          const btn = e.target.closest("[data-billing-select-plan]");
          if (!btn) return;
          const planKey = normalizePlanKey(btn.getAttribute("data-billing-select-plan"));
          const cadence = billingState.cadence === "annual" ? "annual" : "monthly";
          const currentPlan = normalizePlanKey(billingState.summary?.plan?.key);
          if (planKey === currentPlan) return;
          openBillingConfirmModal({
            title: "Confirm plan change",
            body: `Switch from ${currentPlan} to ${planKey} (${cadence})?`,
            confirmLabel: `Switch to ${planKey}`,
            onConfirm: async () => {
              if (billingState.demoMode === true) {
                await apiPatch("/api/billing/plan", { planKey });
                saveTenantPlanOverride(planKey);
              } else {
                await openBillingCheckout({ planKey, cadence });
              }
              closeBillingModal(billingEls.planModal);
              await loadBillingData();
              showSettingsToast(billingState.demoMode === true ? `Plan updated to ${planKey}.` : "Stripe checkout opened.");
            }
          });
        });

        billingEls.planGrid?.addEventListener("mouseover", (e) => {
          const card = e.target.closest("[data-billing-plan-card='pro']");
          if (card) card.classList.add("is-interacting");
        });
        billingEls.planGrid?.addEventListener("mouseout", (e) => {
          const card = e.target.closest("[data-billing-plan-card='pro']");
          if (card) card.classList.remove("is-interacting");
        });
        billingEls.planGrid?.addEventListener("focusin", (e) => {
          const card = e.target.closest("[data-billing-plan-card='pro']");
          if (card) card.classList.add("is-interacting");
        });
        billingEls.planGrid?.addEventListener("focusout", (e) => {
          const card = e.target.closest("[data-billing-plan-card='pro']");
          if (card) card.classList.remove("is-interacting");
        });

        billingEls.invoicesTbody?.addEventListener("click", (e) => {
          const btn = e.target.closest(".billing-download-btn");
          if (!btn) return;
          const id = btn.getAttribute("data-billing-invoice-id");
          const inv = billingState.invoices.find((x) => String(x.id) === String(id));
          if (!inv || !inv.pdfUrl) {
            showSettingsToast("Connect billing provider to download PDFs.", true);
            return;
          }
          window.open(inv.pdfUrl, "_blank", "noopener,noreferrer");
        });

        [billingEls.planModal, billingEls.infoModal, billingEls.confirmModal].forEach((modalEl) => {
          if (!modalEl) return;
          modalEl.addEventListener("click", (e) => {
            if (e.target === modalEl) closeBillingModal(modalEl);
          });
          modalEl.addEventListener("keydown", (e) => {
            if (e.key === "Escape") closeBillingModal(modalEl);
            trapFocusInModal(modalEl, e);
          });
        });
      }

      bindBillingEvents();
      settingsPanelLoaders.billing = async () => {
        await loadBillingData();
      };
    }

    function readCompliancePatchFromUI() {
      const getChecked = (id) => document.getElementById(id)?.checked === true;
      const getValue = (id) => (document.getElementById(id)?.value || "").trim();
      const sourceOpts = parseCsvList(getValue("cmpConsentSourceOptions"));
      const stopKeywords = parseCsvList(getValue("cmpStopKeywords"));
      const helpKeywords = parseCsvList(getValue("cmpHelpKeywords"));
      const resubKeywords = parseCsvList(getValue("cmpResubKeywords"));

      return {
        stopKeywords: stopKeywords.length ? stopKeywords : ["STOP", "UNSUBSCRIBE", "CANCEL", "END", "QUIT"],
        helpKeywords: helpKeywords.length ? helpKeywords : ["HELP", "INFO"],
        stopBehavior: {
          enabled: getChecked("cmpStopEnabled"),
          autoReply: getChecked("cmpStopAutoReply"),
          autoReplyText: getValue("cmpStopAutoReplyText")
        },
        optOut: {
          enforce: getChecked("cmpOptOutEnforce"),
          allowTransactional: getChecked("cmpAllowTransactional"),
          storeAsTag: getValue("cmpStoreAsTag") || "DNR",
          resubscribeKeywords: resubKeywords.length ? resubKeywords : ["START", "UNSTOP", "YES"]
        },
        consent: {
          requireForOutbound: getChecked("cmpConsentRequired"),
          consentCheckboxText: getValue("cmpConsentCheckboxText"),
          consentSourceOptions: sourceOpts.length ? sourceOpts : ["verbal", "form", "existing_customer", "other"]
        },
        retention: {
          enabled: getChecked("cmpRetentionEnabled"),
          purgeOnSchedule: getChecked("cmpRetentionSchedule"),
          messageLogDays: Number(getValue("cmpRetentionDays") || 90)
        }
      };
    }

    function renderComplianceStatusCard(comp){
      const statusCard = document.getElementById("cmpStatusCard");
      if (!statusCard) return;
      const enforce = comp?.optOut?.enforce ? "Enabled" : "Disabled";
      const days = comp?.retention?.messageLogDays ?? "";
      const lastPurge = comp?.retention?.lastPurgeAt
        ? new Date(comp.retention.lastPurgeAt).toLocaleString()
        : "Never";
      statusCard.innerHTML = `
        <div class="p">Outbound enforcement: <b>${escapeHtml(enforce)}</b></div>
        <div class="p">Retention days: <b>${escapeHtml(String(days))}</b></div>
        <div class="p">Last purge: <b>${escapeHtml(lastPurge)}</b></div>
      `;
    }

    async function loadComplianceUI() {
      const to = getActiveTo();
      const scopeSnapshot = createSettingsScope("profile");
      const acct = await loadAccountSettings(to);
      if (!isUiScopeCurrent(scopeSnapshot, { element: wrap })) return;
      const comp = acct?.compliance || {};
      state.activeCompliance = comp;

      const setChecked = (id, val) => {
        const el = document.getElementById(id);
        if (el) el.checked = !!val;
      };
      const setValue = (id, val) => {
        const el = document.getElementById(id);
        if (el) el.value = val ?? "";
      };

      setChecked("cmpStopEnabled", comp?.stopBehavior?.enabled);
      setChecked("cmpStopAutoReply", comp?.stopBehavior?.autoReply);
      setValue("cmpStopKeywords", (comp?.stopKeywords || []).join(","));
      setValue("cmpHelpKeywords", (comp?.helpKeywords || []).join(","));
      setValue("cmpStopAutoReplyText", comp?.stopBehavior?.autoReplyText || "");
      setChecked("cmpOptOutEnforce", comp?.optOut?.enforce);
      setChecked("cmpAllowTransactional", comp?.optOut?.allowTransactional);
      setValue("cmpStoreAsTag", comp?.optOut?.storeAsTag || "DNR");
      setValue("cmpResubKeywords", (comp?.optOut?.resubscribeKeywords || []).join(","));
      setChecked("cmpConsentRequired", comp?.consent?.requireForOutbound);
      setValue("cmpConsentCheckboxText", comp?.consent?.consentCheckboxText || "");
      setValue("cmpConsentSourceOptions", (comp?.consent?.consentSourceOptions || []).join(","));
      setChecked("cmpRetentionEnabled", comp?.retention?.enabled);
      setChecked("cmpRetentionSchedule", comp?.retention?.purgeOnSchedule);
      setValue("cmpRetentionDays", comp?.retention?.messageLogDays || 90);
      if (!isUiScopeCurrent(scopeSnapshot, { element: wrap })) return;
      renderComplianceStatusCard(comp);
      complianceBaseline = JSON.stringify(readCompliancePatchFromUI());
      syncComplianceDirtyState();
    }

    const cmpSaveBtn = document.getElementById("cmpSaveBtn");
    const complianceFieldIds = [
      "cmpStopEnabled", "cmpStopAutoReply", "cmpStopKeywords", "cmpHelpKeywords", "cmpStopAutoReplyText",
      "cmpOptOutEnforce", "cmpAllowTransactional", "cmpStoreAsTag", "cmpResubKeywords",
      "cmpConsentRequired", "cmpConsentCheckboxText", "cmpConsentSourceOptions",
      "cmpRetentionEnabled", "cmpRetentionSchedule", "cmpRetentionDays"
    ];

    function syncComplianceDirtyState() {
      if (!cmpSaveBtn) return;
      const dirty = JSON.stringify(readCompliancePatchFromUI()) !== complianceBaseline;
      setDirtyButton(cmpSaveBtn, dirty);
    }

    complianceFieldIds.forEach((id) => {
      const el = document.getElementById(id);
      if (!el) return;
      el.addEventListener("input", syncComplianceDirtyState);
      el.addEventListener("change", syncComplianceDirtyState);
    });

    cmpSaveBtn?.addEventListener("click", async () => {
      const to = getActiveTo();
      const status = document.getElementById("cmpSaveStatus");
      if (status) status.textContent = "";
      const patch = readCompliancePatchFromUI();

      try {
        await runGuardedButtonAction(cmpSaveBtn, async () => {
          const res = await apiPatch(`/api/account/compliance?to=${encodeURIComponent(to)}`, patch);
          if (!isUiScopeCurrent(createSettingsScope("profile"), { element: wrap })) return;
          state.activeCompliance = res?.compliance || patch;
          if (status) status.textContent = "Saved";
          renderComplianceStatusCard(state.activeCompliance);
          complianceBaseline = JSON.stringify(readCompliancePatchFromUI());
          syncComplianceDirtyState();
          showSettingsToast("Compliance settings saved");
        }, { pendingText: "Saving..." });
      } catch (err) {
        if (status) status.textContent = "Failed to save.";
        console.error(err);
      }
    });

    document.getElementById("cmpRunPurgeNowBtn")?.addEventListener("click", async () => {
      const to = getActiveTo();
      const status = document.getElementById("cmpSaveStatus");
      if (status) status.textContent = "";
      try {
        await apiPost(`/api/account/compliance/purge-now?to=${encodeURIComponent(to)}`, {});
        await loadComplianceUI();
        if (status) status.textContent = "Purge complete ";
      } catch (err) {
        if (status) status.textContent = "Purge failed.";
        console.error(err);
      }
    });

    try {
      await loadComplianceUI();
    } catch (err) {
      console.error("Compliance settings load failed:", err);
    }

    // signed-in email + profile panel
    syncSignedInEmailUI();
    applyProfilePrefsToUI();
    applyProfileShortcutHintPreference();
    profileDirtyBinding?.resetBaseline?.();

    // logout
    document.getElementById("logoutSettingsBtn")?.addEventListener("click", async () => {
      await logoutAndShowLogin();
    });

    // theme toggle
    const toggle = document.getElementById("themeToggle");
    if (toggle) {
      const current = localStorage.getItem(THEME_KEY) || "dark";
      toggle.checked = current === "dark";
      toggle.addEventListener("change", () => {
        applyTheme(toggle.checked ? "dark" : "light");
      });
    }

    const bnInput = document.getElementById("businessNameInput");
    const industryInput = document.getElementById("workspaceIndustryInput");
    const logoInput = document.getElementById("workspaceLogoInput");
    const workspaceLogoFileInput = document.getElementById("workspaceLogoFileInput");
    const workspaceLogoUploadBtn = document.getElementById("workspaceLogoUploadBtn");
    const workspaceLogoRemoveBtn = document.getElementById("workspaceLogoRemoveBtn");
    const workspaceLogoUploadStatus = document.getElementById("workspaceLogoUploadStatus");
    const workspaceLogoPreview = document.getElementById("workspaceLogoPreview");
    const bnStatus = document.getElementById("saveBusinessNameStatus");
    const bnSaveBtn = document.getElementById("saveBusinessNameBtn");
    const numbersList = document.getElementById("workspaceNumbersList");
    const addNumberInput = document.getElementById("workspaceNewNumberInput");
    const addNumberLabelInput = document.getElementById("workspaceNewNumberLabelInput");
    const addNumberBtn = document.getElementById("workspaceAddNumberBtn");
    const saveNumbersBtn = document.getElementById("workspaceSaveNumbersBtn");
    const numbersStatus = document.getElementById("workspaceNumbersStatus");
    const tzSelect = document.getElementById("workspaceTimezoneSelect");
    const tzPreview = document.getElementById("workspaceLocalTimePreview");
    const saveTimezoneBtn = document.getElementById("workspaceSaveTimezoneBtn");
    const hoursGrid = document.getElementById("workspaceHoursGrid");
    const copyHoursBtn = document.getElementById("workspaceCopyHoursBtn");
    const saveHoursBtn = document.getElementById("workspaceSaveHoursBtn");
    const defaultFlowSelect = document.getElementById("workspaceDefaultFlowSelect");
    const saveDefaultsBtn = document.getElementById("workspaceSaveDefaultsBtn");
    const pricingFlowSelect = document.getElementById("workspacePricingFlowSelect");
    const pricingRowsMount = document.getElementById("workspacePricingRows");
    const paintScopeRowsMount = document.getElementById("workspacePaintScopeRows");
    const serviceScopeRowsMount = document.getElementById("workspaceServiceScopeRows");
    const pricingStatus = document.getElementById("workspacePricingStatus");
    const savePricingBtn = document.getElementById("workspaceSavePricingBtn");
    const workspacePasswordResetCard = document.getElementById("workspacePasswordResetCard");
    const workspaceResetUserSelect = document.getElementById("workspaceResetUserSelect");
    const workspaceResetCodeInput = document.getElementById("workspaceResetCodeInput");
    const workspaceResetUserPasswordInput = document.getElementById("workspaceResetUserPasswordInput");
    const workspaceResetNewPasswordRow = document.getElementById("workspaceResetNewPasswordRow");
    const workspaceResetUserStatus = document.getElementById("workspaceResetUserStatus");
    const workspaceSendResetCodeBtn = document.getElementById("workspaceSendResetCodeBtn");
    const workspaceVerifyResetCodeBtn = document.getElementById("workspaceVerifyResetCodeBtn");
    const workspaceResetUserBtn = document.getElementById("workspaceResetUserBtn");
    const automationsEnabledToggle = document.getElementById("workspaceAutomationsEnabled");
    const automationsQuietHoursToggle = document.getElementById("workspaceAutomationsQuietHours");
    const automationsSafeOptOutToggle = document.getElementById("workspaceAutomationsSafeOptOut");
    const automationsProfileSelect = document.getElementById("workspaceAutomationsProfile");
    const automationsQuietHoursTz = document.getElementById("workspaceAutomationsQuietHoursTz");
    const automationsStatus = document.getElementById("workspaceAutomationsStatus");
    const automationsSaveBtn = document.getElementById("workspaceAutomationsSaveBtn");
    const automationsLegacyMount = document.getElementById("settingsAutomationsLegacyMount");
    const workspaceRole = String(authState?.user?.role || "").toLowerCase();
    const canResetWorkspacePasswords = workspaceRole === "owner" || workspaceRole === "admin";
    if (workspacePasswordResetCard) workspacePasswordResetCard.style.display = canResetWorkspacePasswords ? "" : "none";
    const workspaceResetCodeState = {
      userId: "",
      code: "",
      verified: false,
      sentAt: 0
    };

    function resetWorkspaceResetFlow() {
      workspaceResetCodeState.userId = "";
      workspaceResetCodeState.code = "";
      workspaceResetCodeState.verified = false;
      workspaceResetCodeState.sentAt = 0;
      if (workspaceResetCodeInput) workspaceResetCodeInput.value = "";
      if (workspaceResetUserPasswordInput) workspaceResetUserPasswordInput.value = "";
      if (workspaceResetNewPasswordRow) workspaceResetNewPasswordRow.style.display = "none";
      if (workspaceSendResetCodeBtn) workspaceSendResetCodeBtn.style.display = "";
      if (workspaceVerifyResetCodeBtn) workspaceVerifyResetCodeBtn.style.display = "";
      if (workspaceResetUserBtn) workspaceResetUserBtn.style.display = "none";
    }

    function validateStrongPassword(password) {
      const value = String(password || "");
      if (value.length < 10) return "Password must be at least 10 characters";
      if (!/[A-Z]/.test(value)) return "Password must include at least one uppercase letter";
      if (!/[a-z]/.test(value)) return "Password must include at least one lowercase letter";
      if (!/[0-9]/.test(value)) return "Password must include at least one number";
      if (!/[^A-Za-z0-9]/.test(value)) return "Password must include at least one symbol";
      return "";
    }

    if (automationsLegacyMount && !automationsLegacyMount.dataset.mounted) {
      automationsLegacyMount.appendChild(viewAutomations());
      automationsLegacyMount.dataset.mounted = "1";
    }

    const workspaceDays = ["mon", "tue", "wed", "thu", "fri", "sat", "sun"];
    const workspaceDayLabel = {
      mon: "Monday",
      tue: "Tuesday",
      wed: "Wednesday",
      thu: "Thursday",
      fri: "Friday",
      sat: "Saturday",
      sun: "Sunday"
    };
    const PRICING_SERVICES = [
      { key: "full", label: "Full Detail" },
      { key: "interior", label: "Interior Detail" },
      { key: "exterior", label: "Exterior Wash & Wax" },
      { key: "ceramic", label: "Ceramic Coating" },
      { key: "tint", label: "Window Tint" },
      { key: "headlight", label: "Headlight Restoration" },
      { key: "paint_correction", label: "Paint Correction" },
      { key: "ppf", label: "PPF" }
    ];
    const PRICING_SCOPES = [
      { key: "spot", label: "Spot / Small area" },
      { key: "standard", label: "Single panel typical" },
      { key: "large", label: "Multi-panel / severe" }
    ];
    const DEFAULT_WORKSPACE_PRICING = {
      services: {
        full: { price: "$200-300", hoursMin: 3, hoursMax: 4 },
        interior: { price: "$100-150", hoursMin: 2, hoursMax: 2 },
        exterior: { price: "$80-120", hoursMin: 1, hoursMax: 2 },
        ceramic: { price: "$500-800", hoursMin: 8, hoursMax: 16 },
        tint: { price: "$200-400", hoursMin: 2, hoursMax: 4 },
        headlight: { price: "$80-160", hoursMin: 1, hoursMax: 2 },
        paint_correction: { price: "$300-600", hoursMin: 4, hoursMax: 8 },
        ppf: { price: "$1200-2000", hoursMin: 8, hoursMax: 16 }
      },
      paintScopes: {
        spot: { price: "$120-260", hoursMin: 1, hoursMax: 3 },
        standard: { price: "$220-450", hoursMin: 2, hoursMax: 5 },
        large: { price: "$450-900", hoursMin: 6, hoursMax: 12 }
      }
    };
    const DEFAULT_SERVICE_SCOPE_TEMPLATES = {
      full: [{ key: "basic", label: "Basic" }, { key: "standard", label: "Standard" }, { key: "premium", label: "Premium" }],
      interior: [
        { key: "light", label: "Light (quick refresh)" },
        { key: "pet_hair", label: "Pet hair removal" },
        { key: "stains_odor", label: "Stains / odor treatment" },
        { key: "heavy", label: "Heavy soil + deep clean" }
      ],
      exterior: [{ key: "basic", label: "Basic" }, { key: "standard", label: "Standard" }, { key: "premium", label: "Premium" }],
      ceramic: [{ key: "one_year", label: "1 Year" }, { key: "two_year", label: "2 Year" }, { key: "five_year", label: "5 Year" }],
      tint: [
        { key: "front_two", label: "Front 2 windows" },
        { key: "rear_two", label: "Rear 2 windows" },
        { key: "back_window", label: "Back windshield (rear glass)" },
        { key: "side_set_four", label: "4 side windows" },
        { key: "full_sides_plus_back", label: "All sides + rear glass" },
        { key: "windshield_full", label: "Full front windshield" },
        { key: "windshield_strip", label: "Windshield brow/strip" },
        { key: "sunroof", label: "Sunroof tint" },
        { key: "remove_old_tint", label: "Old tint removal" },
        { key: "adhesive_cleanup", label: "Adhesive cleanup / glue removal" }
      ],
      headlight: [{ key: "light", label: "Light" }, { key: "moderate", label: "Moderate" }, { key: "heavy", label: "Heavy" }],
      paint_correction: [{ key: "spot", label: "Spot" }, { key: "standard", label: "Standard" }, { key: "large", label: "Large" }],
      ppf: [{ key: "partial", label: "Partial" }, { key: "full", label: "Full" }, { key: "full_vehicle", label: "Full Vehicle" }]
    };
    function buildTimezoneOptions() {
      const fallback = [
        "America/New_York",
        "America/Chicago",
        "America/Denver",
        "America/Los_Angeles",
        "America/Phoenix",
        "America/Anchorage",
        "Pacific/Honolulu",
        "Europe/London",
        "UTC"
      ];
      try {
        if (typeof Intl !== "undefined" && typeof Intl.supportedValuesOf === "function") {
          const all = Intl.supportedValuesOf("timeZone");
          if (Array.isArray(all) && all.length) return all;
        }
      } catch {}
      return fallback;
    }
    const timezoneOptions = buildTimezoneOptions();

    let workspaceModel = null;
    let workspaceFlows = [];
    let selectedPricingFlowId = "";
    let workspaceBaseline = {
      identity: "",
      phoneNumbers: "",
      timezone: "",
      businessHours: "",
      defaultFlowId: "",
      pricingByFlow: "",
      pricingFlowId: "",
      automationDefaults: "",
      bookingUrl: "",
      schedulingJson: ""
    };

    function setDirtyButton(btn, dirty) {
      if (!btn) return;
      btn.disabled = !dirty;
      btn.classList.toggle("is-dirty", dirty);
    }

    [
      "saveBusinessNameBtn",
      "workspaceSaveNumbersBtn",
      "workspaceSaveTimezoneBtn",
      "workspaceSaveHoursBtn",
      "workspaceSaveDefaultsBtn",
      "workspaceSavePricingBtn",
      "saveNotifSettingsBtn",
      "billingDetailsSaveBtn",
      "devSettingsSaveBtn"
    ].forEach((id) => {
      const btn = document.getElementById(id);
      mirrorDirtyButtonToZone(btn, btn?.closest(".settings-action-zone, .row, .pricing-card-footer, .developer-actions"));
    });

    function normalizePhoneNumbers(list) {
      return (list || []).map((n) => ({
        number: String(n?.number || "").trim(),
        label: String(n?.label || "").trim(),
        isPrimary: n?.isPrimary === true
      }));
    }

    function readIdentitySnapshot() {
      return JSON.stringify({
        businessName: String(bnInput?.value || "").trim(),
        industry: String(industryInput?.value || "").trim(),
        logoUrl: String(logoInput?.value || "").trim()
      });
    }

    function syncIdentityDirtyState() {
      const businessName = String(bnInput?.value || "").trim();
      const invalid = businessName.length === 1;
      if (bnStatus && invalid) bnStatus.textContent = "Business name must be at least 2 characters.";
      if (bnStatus && !invalid && bnStatus.textContent.includes("2 characters")) bnStatus.textContent = "";
      const dirty = !invalid && readIdentitySnapshot() !== workspaceBaseline.identity;
      setDirtyButton(bnSaveBtn, dirty);
    }

    function syncWorkspaceLogoUi() {
      const logoUrl = String(logoInput?.value || "").trim();
      if (workspaceLogoRemoveBtn) workspaceLogoRemoveBtn.disabled = !logoUrl;
      if (!workspaceLogoPreview) return;
      if (!logoUrl) {
        workspaceLogoPreview.style.display = "none";
        workspaceLogoPreview.removeAttribute("src");
        return;
      }
      workspaceLogoPreview.src = logoUrl;
      workspaceLogoPreview.style.display = "";
    }

    function syncNumbersDirtyState() {
      const dirty = JSON.stringify(normalizePhoneNumbers(workspaceModel?.phoneNumbers || [])) !== workspaceBaseline.phoneNumbers;
      setDirtyButton(saveNumbersBtn, dirty);
    }

    function syncTimezoneDirtyState() {
      const dirty = String(tzSelect?.value || "") !== workspaceBaseline.timezone;
      setDirtyButton(saveTimezoneBtn, dirty);
    }

    function syncHoursDirtyState() {
      const dirty = JSON.stringify(workspaceModel?.businessHours || {}) !== workspaceBaseline.businessHours;
      setDirtyButton(saveHoursBtn, dirty);
    }

    function syncDefaultsDirtyState() {
      const dirty = String(defaultFlowSelect?.value || "") !== workspaceBaseline.defaultFlowId;
      setDirtyButton(saveDefaultsBtn, dirty);
    }

    function normalizePricingConfig(value, serviceKeys = []) {
      const src = value && typeof value === "object" ? value : {};
      const out = { services: {}, paintScopes: {}, serviceScopes: {} };
      const uniqueServiceKeys = Array.from(new Set([
        ...PRICING_SERVICES.map((x) => x.key),
        ...serviceKeys,
        ...Object.keys(src?.services || {})
      ]));
      for (const key of uniqueServiceKeys) {
        const item = PRICING_SERVICES.find((x) => x.key === key) || { key, label: key };
        const base = DEFAULT_WORKSPACE_PRICING.services[item.key] || { price: "$0-0", hoursMin: 1, hoursMax: 1 };
        const cur = src?.services?.[item.key] || {};
        const hoursMin = Math.max(0, Number(cur.hoursMin ?? base.hoursMin) || 0);
        const hoursMax = Math.max(hoursMin, Number(cur.hoursMax ?? base.hoursMax) || hoursMin);
        out.services[item.key] = {
          price: String(cur.price || base.price || "$0-0").trim(),
          hoursMin,
          hoursMax
        };
      }
      for (const item of PRICING_SCOPES) {
        const base = DEFAULT_WORKSPACE_PRICING.paintScopes[item.key] || { price: "$0-0", hoursMin: 1, hoursMax: 1 };
        const cur = src?.paintScopes?.[item.key] || {};
        const hoursMin = Math.max(0, Number(cur.hoursMin ?? base.hoursMin) || 0);
        const hoursMax = Math.max(hoursMin, Number(cur.hoursMax ?? base.hoursMax) || hoursMin);
        out.paintScopes[item.key] = {
          price: String(cur.price || base.price || "$0-0").trim(),
          hoursMin,
          hoursMax
        };
      }
      const scopeServices = Array.from(new Set([
        ...uniqueServiceKeys,
        ...Object.keys(src?.serviceScopes || {})
      ]));
      for (const serviceKey of scopeServices) {
        const templateScopes = DEFAULT_SERVICE_SCOPE_TEMPLATES[serviceKey] || [];
        const customScopes = Object.keys(src?.serviceScopes?.[serviceKey] || {}).map((k) => ({ key: k, label: humanizeKey(k) }));
        const scopeItems = Array.from(new Map([...templateScopes, ...customScopes].map((x) => [x.key, x])).values());
        if (!scopeItems.length) continue;
        out.serviceScopes[serviceKey] = {};
        for (const scopeItem of scopeItems) {
          const fallback = src?.services?.[serviceKey] || out.services?.[serviceKey] || { price: "$0-0", hoursMin: 1, hoursMax: 1 };
          const cur = src?.serviceScopes?.[serviceKey]?.[scopeItem.key] || {};
          const hoursMin = Math.max(0, Number(cur.hoursMin ?? fallback.hoursMin) || 0);
          const hoursMax = Math.max(hoursMin, Number(cur.hoursMax ?? fallback.hoursMax) || hoursMin);
          out.serviceScopes[serviceKey][scopeItem.key] = {
            name: String(cur.name || `${humanizeKey(serviceKey)} (${scopeItem.label})`).trim(),
            price: String(cur.price || fallback.price || "$0-0").trim(),
            hoursMin,
            hoursMax
          };
        }
      }
      return out;
    }

    function humanizeKey(key) {
      const k = String(key || "").trim();
      if (!k) return "";
      if (k.toLowerCase() === "ppf") return "PPF";
      return k.replace(/[_-]+/g, " ").replace(/\b\w/g, (m) => m.toUpperCase());
    }

    function extractFlowServiceKeys(flow) {
      const intents = flow?.steps?.detect_intent_ai?.intents || {};
      return Object.keys(intents)
        .map((k) => String(k || "").trim())
        .filter((k) => k && k !== "other" && k !== "escalate");
    }

    function getPricingFlowId() {
      return String(selectedPricingFlowId || workspaceModel?.defaults?.defaultFlowId || "").trim();
    }

    function getPricingProfileForFlow(flowId) {
      const byFlow = workspaceModel?.pricingByFlow && typeof workspaceModel.pricingByFlow === "object"
        ? workspaceModel.pricingByFlow
        : {};
      return byFlow[flowId] || {};
    }

    function getPricingServiceList(flowId) {
      const flow = workspaceFlows.find((f) => String(f?.id || "") === String(flowId || ""));
      const flowKeys = extractFlowServiceKeys(flow);
      const pricingKeys = Object.keys(getPricingProfileForFlow(flowId)?.services || {});
      const ids = Array.from(new Set([...flowKeys, ...pricingKeys]));
      if (!ids.length) return PRICING_SERVICES;
      return ids.map((key) => {
        const known = PRICING_SERVICES.find((x) => x.key === key);
        return known || { key, label: humanizeKey(key) };
      });
    }

    function getScopeItemsForService(serviceKey, pricing) {
      const templ = DEFAULT_SERVICE_SCOPE_TEMPLATES[serviceKey] || [];
      const existing = Object.keys(pricing?.serviceScopes?.[serviceKey] || {}).map((k) => ({ key: k, label: humanizeKey(k) }));
      return Array.from(new Map([...templ, ...existing].map((x) => [x.key, x])).values());
    }

    function ensureWorkspacePricingByFlow() {
      if (!workspaceModel) return;
      if (!workspaceModel.pricingByFlow || typeof workspaceModel.pricingByFlow !== "object") {
        workspaceModel.pricingByFlow = {};
      }
    }

    function getNormalizedPricingForFlow(flowId) {
      if (!flowId || !workspaceModel) return null;
      ensureWorkspacePricingByFlow();
      const pricingByFlow = workspaceModel.pricingByFlow || {};
      const current = pricingByFlow[flowId] || {};
      const serviceKeys = getPricingServiceList(flowId).map((x) => x.key);
      const normalized = normalizePricingConfig(current, serviceKeys);
      pricingByFlow[flowId] = normalized;
      workspaceModel.pricingByFlow = pricingByFlow;
      return normalized;
    }

    function addWorkspaceServiceToFlow(flowId) {
      if (!flowId || !workspaceModel) return;
      const pricing = getNormalizedPricingForFlow(flowId);
      if (!pricing) return;
      let baseId = sanitizeOnboardingServiceId(`service_${Object.keys(pricing.services).length + 1}`, "service");
      let counter = 2;
      while (pricing.services?.[baseId]) {
        baseId = sanitizeOnboardingServiceId(`service_${Object.keys(pricing.services).length + counter}`, "service");
        counter += 1;
      }
      pricing.services[baseId] = {
        name: "New service",
        price: "$0-0",
        hoursMin: 1,
        hoursMax: 1
      };
      pricing.serviceScopes[baseId] = {};
      workspaceModel.pricingByFlow[flowId] = pricing;
      renderPricingEditor();
      syncPricingDirtyState();
    }

    function removeWorkspaceServiceFromFlow(flowId, serviceKey) {
      if (!flowId || !serviceKey || !workspaceModel) return;
      const available = getPricingServiceList(flowId).map((x) => x.key);
      if (available.filter((key) => key !== serviceKey).length === 0) return;
      const pricing = getNormalizedPricingForFlow(flowId);
      if (!pricing) return;
      if (pricing.services?.[serviceKey]) delete pricing.services[serviceKey];
      if (pricing.serviceScopes?.[serviceKey]) delete pricing.serviceScopes[serviceKey];
      workspaceModel.pricingByFlow[flowId] = pricing;
      renderPricingEditor();
      syncPricingDirtyState();
    }

    function addWorkspaceScopeToFlow(flowId, serviceKey) {
      if (!flowId || !serviceKey || !workspaceModel) return;
      const pricing = getNormalizedPricingForFlow(flowId);
      if (!pricing) return;
      pricing.serviceScopes = pricing.serviceScopes || {};
      pricing.serviceScopes[serviceKey] = pricing.serviceScopes[serviceKey] || {};
      const existingKeys = Object.keys(pricing.serviceScopes[serviceKey] || {});
      let idx = existingKeys.length + 1;
      let scopeId = sanitizeOnboardingScopeId(`scope_${idx}`, "scope");
      while (pricing.serviceScopes[serviceKey][scopeId]) {
        idx += 1;
        scopeId = sanitizeOnboardingScopeId(`scope_${idx}`, "scope");
      }
      pricing.serviceScopes[serviceKey][scopeId] = {
        name: "New scope",
        price: "$0-0",
        hoursMin: 1,
        hoursMax: 1
      };
      workspaceModel.pricingByFlow[flowId] = pricing;
      renderPricingEditor();
      syncPricingDirtyState();
    }

    function removeWorkspaceScopeFromFlow(flowId, serviceKey, scopeKey) {
      if (!flowId || !serviceKey || !scopeKey || !workspaceModel) return;
      const pricing = getNormalizedPricingForFlow(flowId);
      if (!pricing?.serviceScopes?.[serviceKey]) return;
      if (pricing.serviceScopes[serviceKey][scopeKey]) {
        delete pricing.serviceScopes[serviceKey][scopeKey];
      }
      if (!Object.keys(pricing.serviceScopes[serviceKey] || {}).length) {
        delete pricing.serviceScopes[serviceKey];
      }
      workspaceModel.pricingByFlow[flowId] = pricing;
      renderPricingEditor();
      syncPricingDirtyState();
    }

    function renderPricingRows(mount, list, kind, values) {
      if (!mount) return;
      mount.innerHTML = list.map((item) => {
        const row = values?.[item.key] || { price: "$0-0", hoursMin: 1, hoursMax: 1, name: "" };
        const displayName = String(row.name || onboardingPricingLabelForService(item.key) || item.label || humanizeKey(item.key)).trim();
      return `
        <div class="pricing-row">
          <div class="grid2" style="gap:12px; align-items:flex-end;">
            <div class="col">
              <label class="p">Service name</label>
              <input class="input" data-pricing-kind="${escapeAttr(kind)}" data-pricing-key="${escapeAttr(item.key)}" data-pricing-field="name" value="${escapeAttr(displayName)}" placeholder="Service name" />
            </div>
            <div class="col">
              <label class="p">${escapeHtml(item.label)} price range</label>
              <input class="input" data-pricing-kind="${escapeAttr(kind)}" data-pricing-key="${escapeAttr(item.key)}" data-pricing-field="price" value="${escapeAttr(String(row.price || ""))}" placeholder="$120-260" />
            </div>
            <div class="row pricing-row-hours" style="gap:8px; align-items:flex-end;">
              <div class="col" style="min-width:120px;">
                <label class="p">Hours min</label>
                <input class="input" type="number" min="0" step="0.25" data-pricing-kind="${escapeAttr(kind)}" data-pricing-key="${escapeAttr(item.key)}" data-pricing-field="hoursMin" value="${escapeAttr(String(row.hoursMin ?? 0))}" />
              </div>
              <div class="col" style="min-width:120px;">
                <label class="p">Hours max</label>
                <input class="input" type="number" min="0" step="0.25" data-pricing-kind="${escapeAttr(kind)}" data-pricing-key="${escapeAttr(item.key)}" data-pricing-field="hoursMax" value="${escapeAttr(String(row.hoursMax ?? 0))}" />
              </div>
            </div>
            <div class="col" style="flex:0 0 auto;">
              <button class="btn pricing-row-remove" type="button" data-workspace-remove-service="${escapeAttr(item.key)}">Remove</button>
            </div>
          </div>
        </div>
      `;
      }).join("");
    }

    function renderServiceScopeRows(mount, services, pricing) {
      if (!mount) return;
      const html = services.map((svc) => {
        const scopeItems = getScopeItemsForService(svc.key, pricing);
        if (!scopeItems.length) return "";
        return `
          <div class="card" style="background:var(--panel);">
            <div class="p"><b>${escapeHtml(svc.label)}</b></div>
            <div style="height:6px;"></div>
            <div class="col" style="gap:8px;">
              ${scopeItems.map((scope) => {
                const row = pricing?.serviceScopes?.[svc.key]?.[scope.key] || {};
                const scopeName = String(row.name || scope.label || onboardingHumanizeKey(scope.key)).trim();
                return `
                  <div class="pricing-row">
                    <div class="grid2" style="gap:12px; align-items:flex-end;">
                      <div class="col">
                        <label class="p">Scope name</label>
                        <input class="input" data-pricing-kind="serviceScopes" data-pricing-key="${escapeAttr(svc.key)}" data-pricing-scope="${escapeAttr(scope.key)}" data-pricing-field="name" value="${escapeAttr(scopeName)}" placeholder="Scope name" />
                      </div>
                      <div class="col">
                        <label class="p">${escapeHtml(scope.label)} price range</label>
                        <input class="input" data-pricing-kind="serviceScopes" data-pricing-key="${escapeAttr(svc.key)}" data-pricing-scope="${escapeAttr(scope.key)}" data-pricing-field="price" value="${escapeAttr(String(row.price || ""))}" placeholder="$120-260" />
                      </div>
                      <div class="row pricing-row-hours" style="gap:8px; align-items:flex-end;">
                        <div class="col" style="min-width:120px;">
                          <label class="p">Hours min</label>
                          <input class="input" type="number" min="0" step="0.25" data-pricing-kind="serviceScopes" data-pricing-key="${escapeAttr(svc.key)}" data-pricing-scope="${escapeAttr(scope.key)}" data-pricing-field="hoursMin" value="${escapeAttr(String(row.hoursMin ?? 0))}" />
                        </div>
                        <div class="col" style="min-width:120px;">
                          <label class="p">Hours max</label>
                          <input class="input" type="number" min="0" step="0.25" data-pricing-kind="serviceScopes" data-pricing-key="${escapeAttr(svc.key)}" data-pricing-scope="${escapeAttr(scope.key)}" data-pricing-field="hoursMax" value="${escapeAttr(String(row.hoursMax ?? 0))}" />
                        </div>
                      </div>
                      <div class="col" style="flex:0 0 auto;">
                        <button class="btn pricing-row-remove" type="button" data-workspace-remove-scope="${escapeAttr(`${svc.key}::${scope.key}`)}">Remove</button>
                      </div>
                    </div>
                  </div>
                `;
              }).join("")}
              <div class="row" style="justify-content:flex-end;">
                <button class="btn" type="button" data-workspace-add-scope="${escapeAttr(svc.key)}">Add scope</button>
              </div>
            </div>
          </div>
        `;
      }).filter(Boolean).join("");
      mount.innerHTML = html || `<div class="p">No scope profiles for this flow yet.</div>`;
    }

    function renderPricingEditor() {
      const flowId = getPricingFlowId();
      if (!flowId) {
        if (pricingRowsMount) pricingRowsMount.innerHTML = `<div class="p">Select a flow to edit pricing.</div>`;
        if (paintScopeRowsMount) paintScopeRowsMount.innerHTML = "";
        if (serviceScopeRowsMount) serviceScopeRowsMount.innerHTML = "";
        if (savePricingBtn) savePricingBtn.disabled = true;
        return;
      }
      const serviceList = getPricingServiceList(flowId);
      const serviceKeys = serviceList.map((x) => x.key);
      const pricing = normalizePricingConfig(getPricingProfileForFlow(flowId), serviceKeys);
      if (!workspaceModel.pricingByFlow || typeof workspaceModel.pricingByFlow !== "object") workspaceModel.pricingByFlow = {};
      workspaceModel.pricingByFlow[flowId] = pricing;
      renderPricingRows(pricingRowsMount, serviceList, "services", pricing.services);
      renderPricingRows(paintScopeRowsMount, PRICING_SCOPES, "paintScopes", pricing.paintScopes);
      renderServiceScopeRows(serviceScopeRowsMount, serviceList, pricing);
      if (paintScopeRowsMount) {
        const hasPaint = serviceKeys.includes("paint_correction");
        const sectionLabel = paintScopeRowsMount.previousElementSibling;
        paintScopeRowsMount.style.display = hasPaint ? "" : "none";
        if (sectionLabel) sectionLabel.style.display = hasPaint ? "" : "none";
      }
    }

    function readPricingFromUI() {
      const flowId = getPricingFlowId();
      if (!flowId) return normalizePricingConfig({});
      const serviceKeys = getPricingServiceList(flowId).map((x) => x.key);
      const next = normalizePricingConfig(getPricingProfileForFlow(flowId), serviceKeys);
      const inputs = Array.from(wrap.querySelectorAll("[data-pricing-kind][data-pricing-key][data-pricing-field]"));
      for (const el of inputs) {
        const kind = String(el.getAttribute("data-pricing-kind") || "");
        const key = String(el.getAttribute("data-pricing-key") || "");
        const field = String(el.getAttribute("data-pricing-field") || "");
        if (!field) continue;
        if (kind === "serviceScopes") {
          const scopeKey = String(el.getAttribute("data-pricing-scope") || "");
          if (!scopeKey) continue;
          next.serviceScopes[key] = next.serviceScopes[key] || {};
          next.serviceScopes[key][scopeKey] = next.serviceScopes[key][scopeKey] || {
            name: `${humanizeKey(key)} (${humanizeKey(scopeKey)})`,
            price: "$0-0",
            hoursMin: 1,
            hoursMax: 1
          };
          if (field === "name") next.serviceScopes[key][scopeKey].name = String(el.value || "").trim();
          if (field === "price") next.serviceScopes[key][scopeKey].price = String(el.value || "").trim();
          if (field === "hoursMin" || field === "hoursMax") next.serviceScopes[key][scopeKey][field] = Math.max(0, Number(el.value || 0) || 0);
          continue;
        }
        if (!next?.[kind]?.[key]) continue;
        if (field === "name") next[kind][key].name = String(el.value || "").trim();
        if (field === "price") next[kind][key].price = String(el.value || "").trim();
        if (field === "hoursMin" || field === "hoursMax") next[kind][key][field] = Math.max(0, Number(el.value || 0) || 0);
      }
      for (const item of getPricingServiceList(flowId)) {
        const row = next.services[item.key];
        if (!row) continue;
        row.hoursMax = Math.max(row.hoursMin, row.hoursMax);
      }
      for (const item of PRICING_SCOPES) {
        const row = next.paintScopes[item.key];
        row.hoursMax = Math.max(row.hoursMin, row.hoursMax);
      }
      for (const [svcKey, scopeMap] of Object.entries(next.serviceScopes || {})) {
        for (const [scopeKey, row] of Object.entries(scopeMap || {})) {
          if (!row) continue;
          row.name = String(row.name || `${humanizeKey(svcKey)} (${humanizeKey(scopeKey)})`).trim();
          row.hoursMax = Math.max(Number(row.hoursMin || 0), Number(row.hoursMax || 0));
        }
      }
      return next;
    }

    function syncPricingDirtyState() {
      if (!savePricingBtn) return;
      const flowId = getPricingFlowId();
      if (!flowId) {
        setDirtyButton(savePricingBtn, false);
        return;
      }
      const currentByFlow = {
        ...(workspaceModel?.pricingByFlow || {}),
        [flowId]: normalizePricingConfig(readPricingFromUI(), getPricingServiceList(flowId).map((x) => x.key))
      };
      const current = JSON.stringify(currentByFlow);
      const baselineChanged = current !== workspaceBaseline.pricingByFlow;
      setDirtyButton(savePricingBtn, baselineChanged);
    }

    function normalizeAutomationDefaults(value) {
      const profileRaw = String(value?.profile || "").trim().toLowerCase();
      const profile = ["balanced", "aggressive", "conservative", "custom"].includes(profileRaw) ? profileRaw : "balanced";
      return {
        enabled: value?.enabled !== false,
        quietHours: value?.quietHours === true,
        profile,
        safeOptOut: value?.safeOptOut !== false
      };
    }

    function applyAutomationDefaultsToUI(value) {
      if (automationsEnabledToggle) automationsEnabledToggle.checked = value.enabled === true;
      if (automationsQuietHoursToggle) automationsQuietHoursToggle.checked = value.quietHours === true;
      if (automationsSafeOptOutToggle) automationsSafeOptOutToggle.checked = value.safeOptOut !== false;
      if (automationsProfileSelect) automationsProfileSelect.value = value.profile || "balanced";
    }

    function readAutomationDefaultsFromUI() {
      return normalizeAutomationDefaults({
        enabled: automationsEnabledToggle?.checked === true,
        quietHours: automationsQuietHoursToggle?.checked === true,
        safeOptOut: automationsSafeOptOutToggle?.checked !== false,
        profile: String(automationsProfileSelect?.value || "balanced")
      });
    }

    function syncAutomationDefaultsDirtyState() {
      if (!automationsSaveBtn) return;
      const current = JSON.stringify(readAutomationDefaultsFromUI());
      setDirtyButton(automationsSaveBtn, current !== workspaceBaseline.automationDefaults);
    }

    function showSettingsToast(message, isError = false) {
      const headerEl = document.getElementById("settingsBreadcrumb")?.parentElement || wrap.querySelector(".settings-main-head");
      let toastEl = document.getElementById("settingsToast");
      if (!toastEl && headerEl) {
        toastEl = document.createElement("div");
        toastEl.id = "settingsToast";
        toastEl.className = "hidden settings-toast";
        headerEl.appendChild(toastEl);
      }
      if (!toastEl) return;
      toastEl.textContent = message;
      toastEl.classList.remove("hidden");
      toastEl.classList.toggle("is-error", isError);
      toastEl.classList.toggle("is-success", !isError && Boolean(String(message || "").trim()));
      if (showSettingsToast._timer) clearTimeout(showSettingsToast._timer);
      showSettingsToast._timer = setTimeout(() => {
        toastEl.classList.add("hidden");
      }, 1800);
    }

    // ---- Integrations hub (tenant-scoped) ----
    const integrationsCardsEl = document.getElementById("integrationsCards");
    const integrationsSearchInput = document.getElementById("integrationsSearchInput");
    const integrationsActivityLogEl = document.getElementById("integrationsActivityLog");
    const integrationsImportedEventsEl = document.getElementById("integrationsImportedEvents");
    const intHealthConnected = document.getElementById("intHealthConnected");
    const intHealthLastSync = document.getElementById("intHealthLastSync");
    const intHealthErrors = document.getElementById("intHealthErrors");
    const integrationLearnModal = document.getElementById("integrationLearnModal");
    const integrationComingSoonModal = document.getElementById("integrationComingSoonModal");
    const integrationIcsModal = document.getElementById("integrationIcsModal");
    const integrationTwilioModal = document.getElementById("integrationTwilioModal");
    const integrationStripeModal = document.getElementById("integrationStripeModal");
    const integrationDisconnectModal = document.getElementById("integrationDisconnectModal");
    const integrationIcsCloseBtn = document.getElementById("integrationIcsCloseBtn");
    const integrationTwilioCloseBtn = document.getElementById("integrationTwilioCloseBtn");
    const integrationStripeCloseBtn = document.getElementById("integrationStripeCloseBtn");
    const integrationLearnModalCloseBtn = document.getElementById("integrationLearnModalCloseBtn");
    const integrationComingSoonCloseBtn = document.getElementById("integrationComingSoonCloseBtn");
    const integrationDisconnectCancelBtn = document.getElementById("integrationDisconnectCancelBtn");
    const integrationDisconnectConfirmBtn = document.getElementById("integrationDisconnectConfirmBtn");
    const icsUrlInput = document.getElementById("icsUrlInput");
    const icsPrivacyModeInput = document.getElementById("icsPrivacyModeInput");
    const icsIncludeDetailsInput = document.getElementById("icsIncludeDetailsInput");
    const icsTestBtn = document.getElementById("icsTestBtn");
    const icsTestStatus = document.getElementById("icsTestStatus");
    const icsSyncMinutesSelect = document.getElementById("icsSyncMinutesSelect");
    const icsTimezoneBadge = document.getElementById("icsTimezoneBadge");
    const icsManageDetails = document.getElementById("icsManageDetails");
    const icsSyncNowBtn = document.getElementById("icsSyncNowBtn");
    const icsDisconnectBtn = document.getElementById("icsDisconnectBtn");
    const icsSaveBtn = document.getElementById("icsSaveBtn");
    const twilioAccountSidInput = document.getElementById("twilioAccountSidInput");
    const twilioApiKeySidInput = document.getElementById("twilioApiKeySidInput");
    const twilioApiKeySecretInput = document.getElementById("twilioApiKeySecretInput");
    const twilioWebhookTokenInput = document.getElementById("twilioWebhookTokenInput");
    const twilioMessagingServiceSidInput = document.getElementById("twilioMessagingServiceSidInput");
    const twilioPhoneNumberInput = document.getElementById("twilioPhoneNumberInput");
    const twilioVoiceForwardToInput = document.getElementById("twilioVoiceForwardToInput");
    const twilioVoiceDialTimeoutInput = document.getElementById("twilioVoiceDialTimeoutInput");
    const twilioManageDetails = document.getElementById("twilioManageDetails");
    const twilioTestStatus = document.getElementById("twilioTestStatus");
    const twilioTestBtn = document.getElementById("twilioTestBtn");
    const twilioDisconnectBtn = document.getElementById("twilioDisconnectBtn");
    const twilioSaveBtn = document.getElementById("twilioSaveBtn");
    const stripeSecretKeyInput = document.getElementById("stripeSecretKeyInput");
    const stripePublishableKeyInput = document.getElementById("stripePublishableKeyInput");
    const stripeWebhookSecretInput = document.getElementById("stripeWebhookSecretInput");
    const stripeCustomerIdInput = document.getElementById("stripeCustomerIdInput");
    const stripeWebhookUrlLine = document.getElementById("stripeWebhookUrlLine");
    const stripeManageDetails = document.getElementById("stripeManageDetails");
    const stripeTestStatus = document.getElementById("stripeTestStatus");
    const stripeTestBtn = document.getElementById("stripeTestBtn");
    const stripeSyncBtn = document.getElementById("stripeSyncBtn");
    const stripeDisconnectBtn = document.getElementById("stripeDisconnectBtn");
    const stripeSaveBtn = document.getElementById("stripeSaveBtn");
    const catBtnEls = Array.from(wrap.querySelectorAll(".integrations-cat-btn"));
    const learnTitleEl = document.getElementById("integrationLearnModalTitle");
    const learnBodyEl = document.getElementById("integrationLearnModalBody");
    const comingSoonBodyEl = document.getElementById("integrationComingSoonBody");

    let integrationsCategory = "payments";
    let integrationsSearch = "";
    let integrationsSnapshot = null;

    function openModal(el) {
      if (!el) return;
      el.classList.remove("hidden");
      el.setAttribute("aria-hidden", "false");
    }

    function closeModal(el) {
      if (!el) return;
      el.classList.add("hidden");
      el.setAttribute("aria-hidden", "true");
    }

    function bindOverlayClose(el) {
      if (!el) return;
      el.addEventListener("click", (e) => {
        if (e.target === el) closeModal(el);
      });
    }

    bindOverlayClose(integrationLearnModal);
    bindOverlayClose(integrationComingSoonModal);
    bindOverlayClose(integrationIcsModal);
    bindOverlayClose(integrationTwilioModal);
    bindOverlayClose(integrationStripeModal);
    bindOverlayClose(integrationDisconnectModal);

    integrationIcsCloseBtn?.addEventListener("click", () => closeModal(integrationIcsModal));
    integrationTwilioCloseBtn?.addEventListener("click", () => closeModal(integrationTwilioModal));
    integrationStripeCloseBtn?.addEventListener("click", () => closeModal(integrationStripeModal));
    integrationLearnModalCloseBtn?.addEventListener("click", () => closeModal(integrationLearnModal));
    integrationComingSoonCloseBtn?.addEventListener("click", () => closeModal(integrationComingSoonModal));
    integrationDisconnectCancelBtn?.addEventListener("click", () => closeModal(integrationDisconnectModal));

    document.addEventListener("keydown", (e) => {
      if (e.key !== "Escape") return;
      closeModal(integrationLearnModal);
      closeModal(integrationComingSoonModal);
      closeModal(integrationIcsModal);
      closeModal(integrationTwilioModal);
      closeModal(integrationStripeModal);
      closeModal(integrationDisconnectModal);
    });

    function formatSyncTs(ts) {
      const n = Number(ts || 0);
      if (!n) return "--";
      try {
        return new Date(n).toLocaleString();
      } catch {
        return "--";
      }
    }

    function calendarStatusFromSnapshot() {
      const cfg = integrationsSnapshot?.calendarIcs || {};
      if (cfg.enabled && cfg.lastSyncStatus === "error") return "error";
      if (cfg.enabled) return "connected";
      return "not_connected";
    }

    function providerStatusFromSnapshot(provider) {
      const cfg = integrationsSnapshot?.calendarProviders?.[provider] || {};
      if (cfg.enabled && cfg.lastSyncStatus === "error") return "error";
      if (cfg.enabled) return "connected";
      return "not_connected";
    }

    function twilioStatusFromSnapshot() {
      const cfg = integrationsSnapshot?.twilio || {};
      if (cfg.enabled && cfg.lastStatus === "error") return "error";
      if (cfg.enabled) return "connected";
      return "not_connected";
    }

    function stripeStatusFromSnapshot() {
      const cfg = integrationsSnapshot?.stripe || {};
      if (cfg.enabled && cfg.lastStatus === "error") return "error";
      if (cfg.enabled) return "connected";
      return "not_connected";
    }

    function integrationCardsModel() {
      const stripeCfg = integrationsSnapshot?.stripe || {};
      const stripeStatus = stripeStatusFromSnapshot();
      return [
        {
          key: "stripe_payments",
          category: "payments",
          name: "Payments (Stripe)",
          description: "Track invoices and payment outcomes directly in Relay.",
          status: stripeStatus,
          primary: stripeStatus === "connected" ? "Manage" : (stripeStatus === "error" ? "Fix" : "Connect"),
          health: stripeStatus === "connected"
            ? `Account: ${stripeCfg.accountId || "--"} | Last tested: ${formatSyncTs(stripeCfg.lastTestedAt)}`
            : (stripeStatus === "error" ? (stripeCfg.lastError || "Needs attention") : "Setup time: ~60 seconds"),
          syncNow: stripeStatus === "connected",
          syncLabel: "Test"
        }
      ];
    }

    function statusMeta(status) {
      if (status === "connected") return { label: "Connected", cls: "is-active" };
      if (status === "error") return { label: "Error", cls: "is-past_due" };
      if (status === "coming_soon") return { label: "Coming soon", cls: "is-trialing" };
      return { label: "Not connected", cls: "is-open" };
    }

    function renderIntegrationHealth() {
      const cards = integrationCardsModel();
      const connected = cards.filter((c) => c.status === "connected").length;
      const hasError = cards.some((c) => c.status === "error");
      const calCfg = integrationsSnapshot?.calendarIcs || {};
      if (intHealthConnected) intHealthConnected.textContent = `Total integrations connected: ${connected}`;
      const allSyncTs = [
        Number(calCfg?.lastSyncedAt || 0),
        Number(integrationsSnapshot?.calendarProviders?.google?.lastSyncedAt || 0),
        Number(integrationsSnapshot?.calendarProviders?.outlook?.lastSyncedAt || 0),
        Number(integrationsSnapshot?.twilio?.lastTestedAt || 0),
        Number(integrationsSnapshot?.stripe?.lastTestedAt || 0)
      ].filter((n) => Number.isFinite(n) && n > 0);
      const latest = allSyncTs.length ? Math.max(...allSyncTs) : 0;
      if (intHealthLastSync) intHealthLastSync.textContent = `Last sync time: ${formatSyncTs(latest)}`;
      if (intHealthErrors) intHealthErrors.textContent = hasError ? "Errors: 1+ need attention" : "Errors: none";
    }

    function renderIntegrationLogs() {
      const logs = integrationsSnapshot?.integrationLogs || [];
      if (!integrationsActivityLogEl) return;
      if (!logs.length) {
        integrationsActivityLogEl.innerHTML = `<div class="ops-empty-block">${RelayUI.renderEmptyState({ text: "No integration activity yet.", className: "is-compact" })}</div>`;
        return;
      }
      integrationsActivityLogEl.innerHTML = logs.slice(0, 10).map((log) => `
        <div class="integrations-log-item ops-log-item">
          <div class="ops-log-item-main">
            <div class="p">${escapeHtml(log.message || log.type || "Event")}</div>
          </div>
          <div class="ops-log-item-meta">${escapeHtml(formatSyncTs(log.ts))}</div>
        </div>
      `).join("");
    }

    function renderImportedEvents() {
      const events = integrationsSnapshot?.importedEvents || [];
      if (!integrationsImportedEventsEl) return;
      if (!events.length) {
        integrationsImportedEventsEl.innerHTML = `<div class="ops-empty-block">${RelayUI.renderEmptyState({ text: "No imported calendar events yet.", className: "is-compact" })}</div>`;
        return;
      }
      integrationsImportedEventsEl.innerHTML = events.slice(0, 10).map((ev) => `
        <div class="integrations-log-item ops-log-item">
          <div class="ops-log-item-main">
            <div class="p">${escapeHtml(`[${String(ev.source || "calendar").toUpperCase()}] ${ev.summary || "Busy"}`)}</div>
          </div>
          <div class="ops-log-item-meta">${escapeHtml(formatSyncTs(ev.start))}</div>
        </div>
      `).join("");
    }

    function renderIntegrationCards() {
      if (!integrationsCardsEl) return;
      const query = integrationsSearch.trim().toLowerCase();
      const cards = integrationCardsModel().filter((card) => {
        const inCat = integrationsCategory === "all" || card.category === integrationsCategory;
        if (!inCat) return false;
        if (!query) return true;
        const hay = `${card.name} ${card.description}`.toLowerCase();
        return hay.includes(query);
      });

      if (!cards.length) {
        integrationsCardsEl.innerHTML = `<div class="card"><div class="p">No integrations found.</div></div>`;
        return;
      }

      integrationsCardsEl.innerHTML = cards.map((card) => {
        const s = statusMeta(card.status);
        return `
          <div class="card integrations-card" data-integration-key="${escapeAttr(card.key)}">
            <div class="row" style="justify-content:space-between; align-items:flex-start; gap:8px;">
              <div class="col" style="gap:4px;">
                <div class="h1" style="margin:0;">${escapeHtml(card.name)}</div>
                <div class="p">${escapeHtml(card.description)}</div>
              </div>
              <span class="billing-status-pill ${s.cls}">${escapeHtml(s.label)}</span>
            </div>
            <div class="p integrations-health-line">${escapeHtml(card.health)}</div>
            <div class="row" style="justify-content:flex-end; gap:8px; flex-wrap:wrap;">
              ${card.syncNow ? `<button class="btn" type="button" data-action="sync-now">${escapeHtml(card.syncLabel || "Sync now")}</button>` : ""}
              <button class="btn primary" type="button" data-action="primary">${escapeHtml(card.primary)}</button>
            </div>
          </div>
        `;
      }).join("");
    }

    function setCategory(cat) {
      integrationsCategory = cat;
      catBtnEls.forEach((btn) => btn.classList.toggle("active", btn.getAttribute("data-int-cat") === cat));
      renderIntegrationCards();
    }

    function syncIcsManageDetails() {
      const cfg = integrationsSnapshot?.calendarIcs || {};
      if (!icsManageDetails) return;
      if (!cfg.enabled) {
        icsManageDetails.textContent = "Not connected";
        return;
      }
      icsManageDetails.textContent = `URL: ${cfg.urlMasked || "--"} | Last synced: ${formatSyncTs(cfg.lastSyncedAt)} | Imported: ${Number(cfg.importedCountLast || 0)}`;
    }
    function syncTwilioManageDetails() {
      const cfg = integrationsSnapshot?.twilio || {};
      if (!twilioManageDetails) return;
      if (!cfg.enabled) {
        twilioManageDetails.textContent = "Not connected";
        return;
      }
      twilioManageDetails.textContent = `SID: ${cfg.accountSidMasked || "--"} | Last tested: ${formatSyncTs(cfg.lastTestedAt)} | Forward: ${cfg.voiceForwardTo || "not set"} | Webhook token: ${cfg.hasWebhookAuthToken ? "set" : "missing"}`;
    }
    function syncStripeManageDetails() {
      const cfg = integrationsSnapshot?.stripe || {};
      if (!stripeManageDetails) return;
      if (!cfg.enabled) {
        stripeManageDetails.textContent = "Not connected";
        return;
      }
      stripeManageDetails.textContent = `Stripe account: ${cfg.accountId || "--"} | Last tested: ${formatSyncTs(cfg.lastTestedAt)} | Customer: ${cfg.customerId || "not set"}`;
    }
    async function refreshIntegrationsSnapshot() {
      try {
        const data = await apiGet("/api/integrations");
        integrationsSnapshot = data || {};
      } catch (err) {
        integrationsSnapshot = {
          twilio: { enabled: false },
          stripe: { enabled: false },
          calendarIcs: { enabled: false },
          calendarProviders: {
            google: { enabled: false },
            outlook: { enabled: false }
          }
        };
        console.error("Failed to load integrations:", err);
      }
      renderIntegrationHealth();
      renderIntegrationLogs();
      renderImportedEvents();
      renderIntegrationCards();
      syncIcsManageDetails();
      syncTwilioManageDetails();
      syncStripeManageDetails();
    }

    function openLearnModal(card) {
      if (learnTitleEl) learnTitleEl.textContent = card.name;
      if (learnBodyEl) learnBodyEl.textContent = card.description;
      openModal(integrationLearnModal);
    }

    function openComingSoonModal(card) {
      if (comingSoonBodyEl) comingSoonBodyEl.textContent = `${card.name} is coming soon. Join the waitlist from this preview.`;
      openModal(integrationComingSoonModal);
    }

    function openIcsModal() {
      const cfg = integrationsSnapshot?.calendarIcs || {};
      if (icsUrlInput) icsUrlInput.value = "";
      if (icsPrivacyModeInput) icsPrivacyModeInput.checked = cfg.privacyMode !== false;
      if (icsIncludeDetailsInput) icsIncludeDetailsInput.checked = false;
      if (icsIncludeDetailsInput) icsIncludeDetailsInput.disabled = true;
      if (icsSyncMinutesSelect) icsSyncMinutesSelect.value = String(Number(cfg.syncMinutes ?? 60));
      if (icsTestStatus) icsTestStatus.textContent = "";
      syncIcsManageDetails();
      openModal(integrationIcsModal);
    }
    function openTwilioModal() {
      const cfg = integrationsSnapshot?.twilio || {};
      if (twilioAccountSidInput) twilioAccountSidInput.value = String(cfg.accountSid || "");
      if (twilioApiKeySidInput) twilioApiKeySidInput.value = "";
      if (twilioApiKeySecretInput) twilioApiKeySecretInput.value = "";
      if (twilioWebhookTokenInput) twilioWebhookTokenInput.value = "";
      if (twilioMessagingServiceSidInput) twilioMessagingServiceSidInput.value = String(cfg.messagingServiceSid || "");
      if (twilioPhoneNumberInput) twilioPhoneNumberInput.value = String(cfg.phoneNumber || "");
      if (twilioVoiceForwardToInput) twilioVoiceForwardToInput.value = String(cfg.voiceForwardTo || "");
      if (twilioVoiceDialTimeoutInput) twilioVoiceDialTimeoutInput.value = String(Number(cfg.voiceDialTimeoutSec || 20) || 20);
      if (twilioTestStatus) twilioTestStatus.textContent = "";
      syncTwilioManageDetails();
      openModal(integrationTwilioModal);
    }

    function openStripeModal() {
      const cfg = integrationsSnapshot?.stripe || {};
      if (stripeSecretKeyInput) stripeSecretKeyInput.value = "";
      if (stripePublishableKeyInput) stripePublishableKeyInput.value = "";
      if (stripeWebhookSecretInput) stripeWebhookSecretInput.value = "";
      if (stripeCustomerIdInput) stripeCustomerIdInput.value = String(cfg.customerId || "");
      if (stripeWebhookUrlLine) {
        const to = encodeURIComponent(String(getActiveTo() || ""));
        stripeWebhookUrlLine.textContent = `Webhook URL: ${API_BASE}/webhooks/stripe?to=${to}`;
      }
      if (stripeTestStatus) stripeTestStatus.textContent = "";
      syncStripeManageDetails();
      openModal(integrationStripeModal);
    }
    async function startTwoWayOAuth(provider) {
      const providerLabel = provider === "google" ? "Google" : "Outlook";
      const started = await apiPost(`/api/integrations/calendar/${provider}/oauth/start`, {});
      const authUrl = String(started?.authUrl || "");
      if (!authUrl) {
        throw new Error(`${providerLabel} OAuth URL was not returned`);
      }
      const popup = window.open(authUrl, `relay_calendar_oauth_${provider}`, "width=560,height=720,noopener,noreferrer");
      if (!popup) {
        window.location.assign(authUrl);
        return;
      }

      return await new Promise((resolve, reject) => {
        let settled = false;
        const timeoutId = setTimeout(() => {
          cleanup();
          reject(new Error(`${providerLabel} OAuth timed out`));
        }, 120000);
        const closePoll = setInterval(() => {
          if (popup.closed) {
            cleanup();
            reject(new Error(`${providerLabel} OAuth window was closed`));
          }
        }, 400);

        function cleanup() {
          if (settled) return;
          settled = true;
          clearTimeout(timeoutId);
          clearInterval(closePoll);
          window.removeEventListener("message", onMessage);
        }

        function onMessage(evt) {
          const data = evt?.data || {};
          if (data.type !== "relay:calendar-oauth") return;
          if (String(data.provider || "") !== provider) return;
          cleanup();
          if (data.ok === true) {
            resolve(data);
            return;
          }
          reject(new Error(String(data.message || `${providerLabel} OAuth failed`)));
        }

        window.addEventListener("message", onMessage);
      });
    }

    integrationsSearchInput?.addEventListener("input", () => {
      integrationsSearch = String(integrationsSearchInput.value || "");
      renderIntegrationCards();
    });
    catBtnEls.forEach((btn) => {
      btn.addEventListener("click", () => setCategory(btn.getAttribute("data-int-cat") || "all"));
    });

    integrationsCardsEl?.addEventListener("click", async (e) => {
      const target = e.target.closest("button[data-action]");
      if (!target) return;
      const cardEl = e.target.closest("[data-integration-key]");
      const key = cardEl?.getAttribute("data-integration-key");
      const card = integrationCardsModel().find((c) => c.key === key);
      if (!card) return;
      const action = target.getAttribute("data-action");

      const providerMap = {
        google_two_way: "google"
      };

      if (key === "stripe_payments") {
        if (action === "sync-now") {
          try {
            target.disabled = true;
            await apiPost("/api/integrations/stripe/test", {});
            await refreshIntegrationsSnapshot();
            showSettingsToast("Stripe test successful");
          } catch (err) {
            showSettingsToast(err?.message || "Stripe test failed", true);
          } finally {
            target.disabled = false;
          }
          return;
        }
        if (action === "primary") {
          openStripeModal();
          return;
        }
      }

      if (providerMap[key]) {
        const provider = providerMap[key];
        const providerLabel = provider === "google" ? "Google" : "Outlook";
        if (action === "sync-now") {
          try {
            target.disabled = true;
            await apiPost(`/api/integrations/calendar/${provider}/two-way/sync`, {});
            await refreshIntegrationsSnapshot();
            showSettingsToast(`${providerLabel} sync complete`);
          } catch (err) {
            showSettingsToast(err?.message || `${provider} sync failed`, true);
          } finally {
            target.disabled = false;
          }
          return;
        }
        if (action === "primary") {
          const providerCfg = integrationsSnapshot?.calendarProviders?.[provider] || {};
          if (providerCfg.enabled) {
            try {
              target.disabled = true;
              await apiDelete(`/api/integrations/calendar/${provider}/two-way`);
              await refreshIntegrationsSnapshot();
              showSettingsToast(`${providerLabel} disconnected`);
            } catch (err) {
              showSettingsToast(err?.message || `Failed to disconnect ${provider}`, true);
            } finally {
              target.disabled = false;
            }
          } else {
            try {
              target.disabled = true;
              await startTwoWayOAuth(provider);
              await refreshIntegrationsSnapshot();
              showSettingsToast(`${providerLabel} connected`);
            } catch (err) {
              showSettingsToast(err?.message || `Failed to connect ${provider}`, true);
            } finally {
              target.disabled = false;
            }
          }
          return;
        }
      }

      if (key !== "calendar_ics") {
        openComingSoonModal(card);
        return;
      }
      if (action === "sync-now") {
        try {
          target.disabled = true;
          await apiPost("/api/integrations/calendar/ics/sync", {});
          await refreshIntegrationsSnapshot();
          showSettingsToast("Calendar sync complete");
        } catch (err) {
          showSettingsToast(err?.message || "Calendar sync failed", true);
        } finally {
          target.disabled = false;
        }
        return;
      }
      if (action === "primary") {
        openIcsModal();
      }
    });

    twilioSaveBtn?.addEventListener("click", async () => {
      const accountSid = String(twilioAccountSidInput?.value || "").trim();
      const apiKeySid = String(twilioApiKeySidInput?.value || "").trim();
      const apiKeySecret = String(twilioApiKeySecretInput?.value || "").trim();
      const messagingServiceSid = String(twilioMessagingServiceSidInput?.value || "").trim();
      const phoneNumber = String(twilioPhoneNumberInput?.value || "").trim();
      const voiceForwardTo = String(twilioVoiceForwardToInput?.value || "").trim();
      const voiceDialTimeoutSec = Number(twilioVoiceDialTimeoutInput?.value || 20);
      const webhookAuthToken = String(twilioWebhookTokenInput?.value || "").trim();

      try {
        twilioSaveBtn.disabled = true;
        if (twilioTestStatus) twilioTestStatus.textContent = "";
        await apiPut("/api/integrations/twilio", {
          accountSid,
          apiKeySid,
          apiKeySecret,
          messagingServiceSid,
          phoneNumber,
          voiceForwardTo,
          voiceDialTimeoutSec,
          webhookAuthToken
        });
        await refreshIntegrationsSnapshot();
        showSettingsToast("Twilio connected");
        closeModal(integrationTwilioModal);
      } catch (err) {
        if (twilioTestStatus) twilioTestStatus.textContent = err?.message || "Failed to connect Twilio";
      } finally {
        twilioSaveBtn.disabled = false;
      }
    });

    twilioTestBtn?.addEventListener("click", async () => {
      try {
        twilioTestBtn.disabled = true;
        if (twilioTestStatus) twilioTestStatus.textContent = "";
        const result = await apiPost("/api/integrations/twilio/test", {});
        await refreshIntegrationsSnapshot();
        const label = String(result?.friendlyName || result?.accountSid || "").trim();
        if (twilioTestStatus) twilioTestStatus.textContent = label ? `Connected: ${label}` : "Twilio connection is valid.";
        showSettingsToast("Twilio test successful");
      } catch (err) {
        if (twilioTestStatus) twilioTestStatus.textContent = err?.message || "Twilio test failed";
        showSettingsToast(err?.message || "Twilio test failed", true);
      } finally {
        twilioTestBtn.disabled = false;
      }
    });

    twilioDisconnectBtn?.addEventListener("click", async () => {
      try {
        twilioDisconnectBtn.disabled = true;
        await apiDelete("/api/integrations/twilio");
        await refreshIntegrationsSnapshot();
        showSettingsToast("Twilio disconnected");
        closeModal(integrationTwilioModal);
      } catch (err) {
        showSettingsToast(err?.message || "Failed to disconnect Twilio", true);
      } finally {
        twilioDisconnectBtn.disabled = false;
      }
    });

    stripeSaveBtn?.addEventListener("click", async () => {
      const secretKey = String(stripeSecretKeyInput?.value || "").trim();
      const publishableKey = String(stripePublishableKeyInput?.value || "").trim();
      const webhookSecret = String(stripeWebhookSecretInput?.value || "").trim();
      const customerId = String(stripeCustomerIdInput?.value || "").trim();
      try {
        stripeSaveBtn.disabled = true;
        if (stripeTestStatus) stripeTestStatus.textContent = "";
        await apiPut("/api/integrations/stripe", {
          secretKey,
          publishableKey,
          webhookSecret,
          customerId
        });
        await refreshIntegrationsSnapshot();
        showSettingsToast("Stripe connected");
        closeModal(integrationStripeModal);
      } catch (err) {
        if (stripeTestStatus) stripeTestStatus.textContent = err?.message || "Failed to connect Stripe";
      } finally {
        stripeSaveBtn.disabled = false;
      }
    });

    stripeTestBtn?.addEventListener("click", async () => {
      try {
        stripeTestBtn.disabled = true;
        if (stripeTestStatus) stripeTestStatus.textContent = "";
        const result = await apiPost("/api/integrations/stripe/test", {});
        await refreshIntegrationsSnapshot();
        if (stripeTestStatus) stripeTestStatus.textContent = `Connected: ${String(result?.accountId || "--")}`;
        showSettingsToast("Stripe test successful");
      } catch (err) {
        if (stripeTestStatus) stripeTestStatus.textContent = err?.message || "Stripe test failed";
        showSettingsToast(err?.message || "Stripe test failed", true);
      } finally {
        stripeTestBtn.disabled = false;
      }
    });

    stripeSyncBtn?.addEventListener("click", async () => {
      try {
        stripeSyncBtn.disabled = true;
        if (stripeTestStatus) stripeTestStatus.textContent = "";
        const result = await apiPost("/api/integrations/stripe/sync", {});
        await refreshIntegrationsSnapshot();
        if (stripeTestStatus) stripeTestStatus.textContent = `Invoices synced: ${Number(result?.invoicesImported || 0)}`;
        showSettingsToast("Stripe invoices synced");
      } catch (err) {
        if (stripeTestStatus) stripeTestStatus.textContent = err?.message || "Stripe sync failed";
        showSettingsToast(err?.message || "Stripe sync failed", true);
      } finally {
        stripeSyncBtn.disabled = false;
      }
    });

    stripeDisconnectBtn?.addEventListener("click", async () => {
      try {
        stripeDisconnectBtn.disabled = true;
        await apiDelete("/api/integrations/stripe");
        await refreshIntegrationsSnapshot();
        showSettingsToast("Stripe disconnected");
        closeModal(integrationStripeModal);
      } catch (err) {
        showSettingsToast(err?.message || "Failed to disconnect Stripe", true);
      } finally {
        stripeDisconnectBtn.disabled = false;
      }
    });

    icsPrivacyModeInput?.addEventListener("change", () => {
      if (icsIncludeDetailsInput) {
        icsIncludeDetailsInput.disabled = icsPrivacyModeInput.checked === true;
        if (icsIncludeDetailsInput.disabled) icsIncludeDetailsInput.checked = false;
      }
    });

    icsTestBtn?.addEventListener("click", async () => {
      const url = String(icsUrlInput?.value || "").trim();
      if (icsTestStatus) icsTestStatus.textContent = "";
      if (!url) {
        if (icsTestStatus) icsTestStatus.textContent = "Please enter a calendar feed URL.";
        return;
      }
      try {
        icsTestBtn.disabled = true;
        const result = await apiPost("/api/integrations/calendar/ics/test", {
          url,
          privacyMode: icsPrivacyModeInput?.checked === true
        });
        const sample = result?.sample?.summary ? `Sample: ${result.sample.summary}` : "Connection successful";
        const warnings = Array.isArray(result?.warnings) && result.warnings.length ? ` (${result.warnings.join("; ")})` : "";
        if (icsTestStatus) icsTestStatus.textContent = `${sample}${warnings}`;
      } catch (err) {
        if (icsTestStatus) icsTestStatus.textContent = err?.message || "Connection failed";
      } finally {
        icsTestBtn.disabled = false;
      }
    });

    icsSaveBtn?.addEventListener("click", async () => {
      const url = String(icsUrlInput?.value || "").trim();
      if (!url) {
        if (icsTestStatus) icsTestStatus.textContent = "Please enter a calendar feed URL.";
        return;
      }
      const provider = wrap.querySelector('input[name="icsProvider"]:checked')?.value || "other";
      const privacyMode = icsPrivacyModeInput?.checked === true;
      const syncMinutes = Number(icsSyncMinutesSelect?.value || 60);

      try {
        icsSaveBtn.disabled = true;
        await apiPut("/api/integrations/calendar/ics", {
          url,
          provider,
          privacyMode,
          syncMinutes
        });
        await refreshIntegrationsSnapshot();
        showSettingsToast("Calendar connected");
        closeModal(integrationIcsModal);
      } catch (err) {
        if (icsTestStatus) icsTestStatus.textContent = err?.message || "Failed to connect calendar";
      } finally {
        icsSaveBtn.disabled = false;
      }
    });

    icsSyncNowBtn?.addEventListener("click", async () => {
      try {
        icsSyncNowBtn.disabled = true;
        await apiPost("/api/integrations/calendar/ics/sync", {});
        await refreshIntegrationsSnapshot();
        showSettingsToast("Calendar sync complete");
      } catch (err) {
        showSettingsToast(err?.message || "Calendar sync failed", true);
      } finally {
        icsSyncNowBtn.disabled = false;
      }
    });

    icsDisconnectBtn?.addEventListener("click", () => openModal(integrationDisconnectModal));
    integrationDisconnectConfirmBtn?.addEventListener("click", async () => {
      try {
        integrationDisconnectConfirmBtn.disabled = true;
        await apiDelete("/api/integrations/calendar/ics");
        await refreshIntegrationsSnapshot();
        showSettingsToast("Calendar disconnected");
        closeModal(integrationDisconnectModal);
        closeModal(integrationIcsModal);
      } catch (err) {
        showSettingsToast(err?.message || "Failed to disconnect calendar", true);
      } finally {
        integrationDisconnectConfirmBtn.disabled = false;
      }
    });

    settingsPanelLoaders.integrations = async () => {
      try {
        const acct = await loadAccountSettings(getActiveTo());
        const tz = String(acct?.workspace?.timezone || "America/New_York");
        if (icsTimezoneBadge) icsTimezoneBadge.textContent = tz;
      } catch {}
      setCategory("payments");
      await refreshIntegrationsSnapshot();
    };

    function getWorkspaceFromAccount(acct) {
      const to = getActiveTo();
      const identityName = acct?.workspace?.identity?.businessName || acct?.businessName || "";
      const numbers = Array.isArray(acct?.workspace?.phoneNumbers) && acct.workspace.phoneNumbers.length
        ? acct.workspace.phoneNumbers
        : [{ number: to, label: "Primary", isPrimary: true }];
      return {
        identity: {
          businessName: String(identityName || ""),
          industry: String(acct?.workspace?.identity?.industry || ""),
          logoUrl: String(acct?.workspace?.identity?.logoUrl || "")
        },
        timezone: String(acct?.workspace?.timezone || "America/New_York"),
        phoneNumbers: numbers.map((n, i) => ({
          number: String(n?.number || ""),
          label: String(n?.label || "") || (i === 0 ? "Primary" : "Line"),
          isPrimary: n?.isPrimary === true
        })),
        businessHours: acct?.workspace?.businessHours || {
          mon: [{ start: "09:00", end: "17:00" }],
          tue: [{ start: "09:00", end: "17:00" }],
          wed: [{ start: "09:00", end: "17:00" }],
          thu: [{ start: "09:00", end: "17:00" }],
          fri: [{ start: "09:00", end: "17:00" }],
          sat: [],
          sun: []
        },
        pricingByFlow: { ...(acct?.workspace?.pricingByFlow || {}) },
        pricing: normalizePricingConfig(acct?.workspace?.pricing || {}),
        defaults: {
          defaultFlowId: String(acct?.defaults?.defaultFlowId || "")
        }
      };
    }

    function renderNumbers() {
      if (!numbersList || !workspaceModel) return;
      numbersList.innerHTML = "";
      workspaceModel.phoneNumbers.forEach((item, idx) => {
        const row = document.createElement("div");
        row.className = "row";
        row.style.gap = "8px";
        row.style.flexWrap = "wrap";
        row.innerHTML = `
          <input class="input" data-num-index="${idx}" data-field="number" style="max-width:220px;" value="${escapeAttr(item.number)}" />
          <input class="input" data-num-index="${idx}" data-field="label" style="max-width:180px;" value="${escapeAttr(item.label || "")}" />
          <label class="toggle"><input type="radio" name="workspacePrimaryNumber" data-primary-index="${idx}" ${item.isPrimary ? "checked" : ""} /> Primary</label>
          <button class="btn" type="button" data-remove-index="${idx}">Remove</button>
        `;
        numbersList.appendChild(row);
      });

      numbersList.querySelectorAll("[data-num-index]").forEach((el) => {
        el.addEventListener("input", (e) => {
          const i = Number(e.target.getAttribute("data-num-index"));
          const field = e.target.getAttribute("data-field");
          if (!workspaceModel.phoneNumbers[i]) return;
          workspaceModel.phoneNumbers[i][field] = e.target.value;
          syncNumbersDirtyState();
        });
      });

      numbersList.querySelectorAll("[data-primary-index]").forEach((el) => {
        el.addEventListener("change", (e) => {
          const i = Number(e.target.getAttribute("data-primary-index"));
          workspaceModel.phoneNumbers.forEach((n, idx) => {
            n.isPrimary = idx === i;
          });
          syncNumbersDirtyState();
        });
      });

      numbersList.querySelectorAll("[data-remove-index]").forEach((el) => {
        el.addEventListener("click", (e) => {
          const i = Number(e.target.getAttribute("data-remove-index"));
          workspaceModel.phoneNumbers.splice(i, 1);
          if (!workspaceModel.phoneNumbers.some((n) => n.isPrimary) && workspaceModel.phoneNumbers[0]) {
            workspaceModel.phoneNumbers[0].isPrimary = true;
          }
          renderNumbers();
          syncNumbersDirtyState();
        });
      });

      syncNumbersDirtyState();
    }

    function ensureTzOptions() {
      if (!tzSelect) return;
      const all = new Set(timezoneOptions);
      if (workspaceModel?.timezone) all.add(workspaceModel.timezone);
      tzSelect.innerHTML = Array.from(all).map((tz) => `<option value="${escapeAttr(tz)}">${escapeHtml(tz)}</option>`).join("");
    }

    function updateTzPreview() {
      if (!tzPreview || !tzSelect) return;
      try {
        const nowStr = new Intl.DateTimeFormat("en-US", {
          timeZone: tzSelect.value,
          weekday: "short",
          hour: "2-digit",
          minute: "2-digit"
        }).format(new Date());
        tzPreview.textContent = nowStr;
      } catch {
        tzPreview.textContent = "Invalid timezone";
      }
    }

    function renderHours() {
      if (!hoursGrid || !workspaceModel) return;
      hoursGrid.innerHTML = workspaceDays.map((day) => {
        const slot = workspaceModel.businessHours?.[day]?.[0] || { start: "09:00", end: "17:00" };
        const open = Array.isArray(workspaceModel.businessHours?.[day]) && workspaceModel.businessHours[day].length > 0;
        return `
          <div class="row" style="gap:10px; align-items:center; flex-wrap:wrap;">
            <div style="min-width:96px; font-weight:600;">${workspaceDayLabel[day]}</div>
            <label class="toggle"><input type="checkbox" data-hours-open="${day}" ${open ? "checked" : ""} /> Open</label>
            <input class="input" type="time" data-hours-start="${day}" value="${escapeAttr(slot.start || "09:00")}" style="max-width:150px;" ${open ? "" : "disabled"} />
            <span class="p">to</span>
            <input class="input" type="time" data-hours-end="${day}" value="${escapeAttr(slot.end || "17:00")}" style="max-width:150px;" ${open ? "" : "disabled"} />
          </div>
        `;
      }).join("");

      workspaceDays.forEach((day) => {
        const openEl = hoursGrid.querySelector(`[data-hours-open="${day}"]`);
        const startEl = hoursGrid.querySelector(`[data-hours-start="${day}"]`);
        const endEl = hoursGrid.querySelector(`[data-hours-end="${day}"]`);

        openEl?.addEventListener("change", () => {
          if (openEl.checked) {
            workspaceModel.businessHours[day] = [{ start: startEl?.value || "09:00", end: endEl?.value || "17:00" }];
            if (startEl) startEl.disabled = false;
            if (endEl) endEl.disabled = false;
          } else {
            workspaceModel.businessHours[day] = [];
            if (startEl) startEl.disabled = true;
            if (endEl) endEl.disabled = true;
          }
          syncHoursDirtyState();
        });

        startEl?.addEventListener("change", () => {
          if (!workspaceModel.businessHours[day]?.[0]) workspaceModel.businessHours[day] = [{ start: "09:00", end: "17:00" }];
          workspaceModel.businessHours[day][0].start = startEl.value;
          syncHoursDirtyState();
        });
        endEl?.addEventListener("change", () => {
          if (!workspaceModel.businessHours[day]?.[0]) workspaceModel.businessHours[day] = [{ start: "09:00", end: "17:00" }];
          workspaceModel.businessHours[day][0].end = endEl.value;
          syncHoursDirtyState();
        });
      });

      syncHoursDirtyState();
    }

    function renderWorkspaceResetUserOptions(users) {
      if (!workspaceResetUserSelect) return;
      resetWorkspaceResetFlow();
      workspaceResetUserSelect.innerHTML = "";
      const list = Array.isArray(users) ? users : [];
      if (!list.length) {
        const opt = document.createElement("option");
        opt.value = "";
        opt.textContent = "No users in this workspace";
        opt.disabled = true;
        opt.selected = true;
        workspaceResetUserSelect.appendChild(opt);
        workspaceResetUserSelect.disabled = true;
        if (workspaceResetUserBtn) workspaceResetUserBtn.disabled = true;
        return;
      }
      for (const u of list) {
        const opt = document.createElement("option");
        opt.value = String(u.id || "");
        opt.textContent = `${u.email} (${u.role})${u.disabled ? " [disabled]" : ""}`;
        workspaceResetUserSelect.appendChild(opt);
      }
      workspaceResetUserSelect.disabled = false;
      if (workspaceResetUserBtn) workspaceResetUserBtn.disabled = false;
    }

    async function refreshWorkspaceResetUsers() {
      if (!canResetWorkspacePasswords) return;
      const scopeSnapshot = createSettingsScope("workspace");
      try {
        const res = await apiGet("/api/account/users");
        if (!isUiScopeCurrent(scopeSnapshot, { element: wrap })) return;
        renderWorkspaceResetUserOptions(Array.isArray(res?.users) ? res.users : []);
      } catch (err) {
        if (!isUiScopeCurrent(scopeSnapshot, { element: wrap })) return;
        renderWorkspaceResetUserOptions([]);
        if (workspaceResetUserStatus) workspaceResetUserStatus.textContent = err?.message || "Failed to load users";
      }
    }

    async function refreshWorkspaceUI() {
      const activeTo = getActiveTo();
      const scopeSnapshot = createSettingsScope("workspace");
      if (bnStatus) bnStatus.textContent = "";
      if (numbersStatus) numbersStatus.textContent = "";
      const acct = await loadAccountSettings(activeTo);
      if (!isUiScopeCurrent(scopeSnapshot, { element: wrap })) return;
      workspaceModel = getWorkspaceFromAccount(acct);
      selectedPricingFlowId = String(workspaceModel?.defaults?.defaultFlowId || selectedPricingFlowId || "");

      if (bnInput) bnInput.value = workspaceModel.identity.businessName;
      if (industryInput) industryInput.value = workspaceModel.identity.industry;
      if (logoInput) logoInput.value = workspaceModel.identity.logoUrl;
      if (workspaceLogoUploadStatus) workspaceLogoUploadStatus.textContent = "";
      syncWorkspaceLogoUi();
      ensureTzOptions();
      if (tzSelect) tzSelect.value = workspaceModel.timezone;
      updateTzPreview();
      renderNumbers();
      renderHours();
      workspaceBaseline.identity = readIdentitySnapshot();
      workspaceBaseline.phoneNumbers = JSON.stringify(normalizePhoneNumbers(workspaceModel.phoneNumbers));
      workspaceBaseline.timezone = String(workspaceModel.timezone || "");
      workspaceBaseline.businessHours = JSON.stringify(workspaceModel.businessHours || {});
      renderPricingEditor();
      workspaceBaseline.pricingByFlow = JSON.stringify(workspaceModel.pricingByFlow || {});
      workspaceBaseline.pricingFlowId = String(selectedPricingFlowId || workspaceModel?.defaults?.defaultFlowId || "");
      const automationDefaults = normalizeAutomationDefaults(acct?.defaults?.automation || {});
      if (!isUiScopeCurrent(scopeSnapshot, { element: wrap })) return;
      applyAutomationDefaultsToUI(automationDefaults);
      workspaceBaseline.automationDefaults = JSON.stringify(automationDefaults);
      if (automationsQuietHoursTz) automationsQuietHoursTz.textContent = workspaceModel.timezone || "America/New_York";
      if (notifQuietHoursTz) notifQuietHoursTz.textContent = workspaceModel.timezone || "America/New_York";
      syncIdentityDirtyState();
      syncWorkspaceLogoUi();
      syncNumbersDirtyState();
      syncTimezoneDirtyState();
      syncHoursDirtyState();
      syncPricingDirtyState();
      syncAutomationDefaultsDirtyState();
      await refreshWorkspaceResetUsers();
    }

    async function refreshDefaultFlowOptions() {
      if (!defaultFlowSelect) return;
      const activeTo = getActiveTo();
      const scopeSnapshot = createSettingsScope("workspace");
      const data = await apiGet(`/api/flows?to=${encodeURIComponent(activeTo)}`);
      if (!isUiScopeCurrent(scopeSnapshot, { element: wrap })) return;
      const flows = Array.isArray(data?.flows) ? data.flows : [];
      workspaceFlows = flows.slice();
      const options = ['<option value="">No default flow</option>']
        .concat(flows.map((f) => `<option value="${escapeAttr(f.id)}">${escapeHtml(f.name || f.id)}</option>`));
      defaultFlowSelect.innerHTML = options.join("");
      defaultFlowSelect.value = workspaceModel?.defaults?.defaultFlowId || "";
      workspaceBaseline.defaultFlowId = defaultFlowSelect.value || "";
      if (pricingFlowSelect) {
        const flowOpts = ['<option value="">Choose flow</option>']
          .concat(flows.map((f) => `<option value="${escapeAttr(f.id)}">${escapeHtml(f.name || f.id)}</option>`));
        pricingFlowSelect.innerHTML = flowOpts.join("");
        const desired = String(selectedPricingFlowId || workspaceModel?.defaults?.defaultFlowId || flows?.[0]?.id || "").trim();
        selectedPricingFlowId = desired;
        pricingFlowSelect.value = desired;
      }
      renderPricingEditor();
      syncDefaultsDirtyState();
      syncPricingDirtyState();
    }

    try {
      await refreshWorkspaceUI();
      await refreshDefaultFlowOptions();
    } catch (err) {
      console.error(err);
    }

    bnInput?.addEventListener("input", syncIdentityDirtyState);
    industryInput?.addEventListener("input", syncIdentityDirtyState);
    logoInput?.addEventListener("input", () => {
      syncIdentityDirtyState();
      syncWorkspaceLogoUi();
    });

    workspaceLogoUploadBtn?.addEventListener("click", () => {
      workspaceLogoFileInput?.click();
    });

    workspaceLogoFileInput?.addEventListener("change", async () => {
      const file = workspaceLogoFileInput?.files?.[0];
      if (!file) return;
      if (workspaceLogoUploadStatus) workspaceLogoUploadStatus.textContent = "Uploading...";
      if (workspaceLogoUploadBtn) workspaceLogoUploadBtn.disabled = true;
      try {
        const activeTo = getActiveTo();
        const uploaded = await uploadOnboardingLogoFile(file, activeTo);
        const nextLogoUrl = String(uploaded?.logoUrl || "").trim();
        if (!nextLogoUrl) throw new Error("Upload succeeded but no logo URL was returned.");
        if (logoInput) logoInput.value = nextLogoUrl;
        if (workspaceModel?.identity) workspaceModel.identity.logoUrl = nextLogoUrl;
        syncWorkspaceLogoUi();
        syncIdentityDirtyState();
        if (workspaceLogoUploadStatus) workspaceLogoUploadStatus.textContent = "Uploaded";
        window.dispatchEvent(new CustomEvent("relay:workspace-branding-changed"));
      } catch (err) {
        if (workspaceLogoUploadStatus) workspaceLogoUploadStatus.textContent = err?.message || "Upload failed";
      } finally {
        if (workspaceLogoUploadBtn) workspaceLogoUploadBtn.disabled = false;
        if (workspaceLogoFileInput) workspaceLogoFileInput.value = "";
      }
    });

    workspaceLogoRemoveBtn?.addEventListener("click", async () => {
      const activeTo = getActiveTo();
      if (workspaceLogoUploadStatus) workspaceLogoUploadStatus.textContent = "Removing...";
      if (workspaceLogoRemoveBtn) workspaceLogoRemoveBtn.disabled = true;
      try {
        await apiDelete(`/api/account/logo?to=${encodeURIComponent(activeTo)}`, {});
        if (logoInput) logoInput.value = "";
        if (workspaceModel?.identity) workspaceModel.identity.logoUrl = "";
        syncWorkspaceLogoUi();
        syncIdentityDirtyState();
        if (workspaceLogoUploadStatus) workspaceLogoUploadStatus.textContent = "Removed";
        window.dispatchEvent(new CustomEvent("relay:workspace-branding-changed"));
      } catch (err) {
        if (workspaceLogoUploadStatus) workspaceLogoUploadStatus.textContent = err?.message || "Remove failed";
      } finally {
        syncWorkspaceLogoUi();
      }
    });

    tzSelect?.addEventListener("change", () => {
      workspaceModel.timezone = tzSelect.value;
      if (notifQuietHoursTz) notifQuietHoursTz.textContent = workspaceModel.timezone || "America/New_York";
      if (automationsQuietHoursTz) automationsQuietHoursTz.textContent = workspaceModel.timezone || "America/New_York";
      updateTzPreview();
      syncTimezoneDirtyState();
    });

    addNumberBtn?.addEventListener("click", () => {
      const number = (addNumberInput?.value || "").trim();
      const label = (addNumberLabelInput?.value || "").trim();
      if (!number) {
        if (numbersStatus) numbersStatus.textContent = "Phone number is required.";
        return;
      }
      if (!/^\+[1-9]\d{1,14}$/.test(number)) {
        if (numbersStatus) numbersStatus.textContent = "Enter a valid E.164 number (example: +18145550003).";
        return;
      }
      if (numbersStatus) numbersStatus.textContent = "";
      workspaceModel.phoneNumbers.push({
        number,
        label: label || "Line",
        isPrimary: workspaceModel.phoneNumbers.length === 0
      });
      if (addNumberInput) addNumberInput.value = "";
      if (addNumberLabelInput) addNumberLabelInput.value = "";
      renderNumbers();
      syncNumbersDirtyState();
    });

    bnSaveBtn?.addEventListener("click", async (e) => {
      e.preventDefault();
      e.stopPropagation();
      const activeTo = getActiveTo();
      const name = (bnInput?.value || "").trim();
      const industry = (industryInput?.value || "").trim();
      const logoUrl = (logoInput?.value || "").trim();
      if (bnStatus) bnStatus.textContent = "";

      try {
        await runGuardedButtonAction(bnSaveBtn, async () => {
          await apiPatch(`/api/account/workspace?to=${encodeURIComponent(activeTo)}`, {
            workspace: { identity: { businessName: name, industry, logoUrl } }
          });
          if (!isUiScopeCurrent(createSettingsScope("workspace"), { element: wrap })) return;
          workspaceModel.identity.businessName = name;
          workspaceModel.identity.industry = industry;
          workspaceModel.identity.logoUrl = logoUrl;
          workspaceBaseline.identity = readIdentitySnapshot();
          syncIdentityDirtyState();
          if (bnStatus) bnStatus.textContent = "Saved ?";
          showSettingsToast("Identity saved");
          window.dispatchEvent(new CustomEvent("relay:workspace-branding-changed"));
        }, { pendingText: "Saving..." });
      } catch (err) {
        if (bnStatus) bnStatus.textContent = "Failed to save.";
        showSettingsToast(err?.message || "Failed to save identity", true);
        console.error(err);
      }
    });

    saveNumbersBtn?.addEventListener("click", async () => {
      const activeTo = getActiveTo();
      try {
        await apiPatch(`/api/account/workspace?to=${encodeURIComponent(activeTo)}`, {
          workspace: { phoneNumbers: workspaceModel.phoneNumbers }
        });
        workspaceBaseline.phoneNumbers = JSON.stringify(normalizePhoneNumbers(workspaceModel.phoneNumbers));
        syncNumbersDirtyState();
        if (numbersStatus) numbersStatus.textContent = "Saved";
        showSettingsToast("Phone numbers saved");
      } catch (err) {
        if (numbersStatus) numbersStatus.textContent = "Failed to save.";
        showSettingsToast(err?.message || "Failed to save numbers", true);
        console.error(err);
      }
    });

    saveTimezoneBtn?.addEventListener("click", async () => {
      const activeTo = getActiveTo();
      try {
        await apiPatch(`/api/account/workspace?to=${encodeURIComponent(activeTo)}`, {
          workspace: { timezone: workspaceModel.timezone }
        });
        workspaceBaseline.timezone = String(workspaceModel.timezone || "");
        syncTimezoneDirtyState();
        showSettingsToast("Timezone saved");
      } catch (err) {
        showSettingsToast(err?.message || "Failed to save timezone", true);
        console.error(err);
      }
    });

    copyHoursBtn?.addEventListener("click", () => {
      const monday = workspaceModel.businessHours?.mon?.[0] || { start: "09:00", end: "17:00" };
      ["tue", "wed", "thu", "fri"].forEach((day) => {
        workspaceModel.businessHours[day] = [{ start: monday.start, end: monday.end }];
      });
      renderHours();
      syncHoursDirtyState();
    });

    saveHoursBtn?.addEventListener("click", async () => {
      const activeTo = getActiveTo();
      try {
        await apiPatch(`/api/account/workspace?to=${encodeURIComponent(activeTo)}`, {
          workspace: { businessHours: workspaceModel.businessHours }
        });
        workspaceBaseline.businessHours = JSON.stringify(workspaceModel.businessHours || {});
        syncHoursDirtyState();
        showSettingsToast("Business hours saved");
      } catch (err) {
        showSettingsToast(err?.message || "Failed to save business hours", true);
        console.error(err);
      }
    });

    saveDefaultsBtn?.addEventListener("click", async () => {
      const activeTo = getActiveTo();
      const defaultFlowId = (defaultFlowSelect?.value || "").trim();
      try {
        await apiPatch(`/api/account/workspace?to=${encodeURIComponent(activeTo)}`, {
          defaults: { defaultFlowId }
        });
        workspaceModel.defaults.defaultFlowId = defaultFlowId;
        workspaceBaseline.defaultFlowId = defaultFlowId;
        syncDefaultsDirtyState();
        showSettingsToast("Defaults saved");
      } catch (err) {
        showSettingsToast(err?.message || "Failed to save defaults", true);
        console.error(err);
      }
    });

    savePricingBtn?.addEventListener("click", async () => {
      const activeTo = getActiveTo();
      const flowId = getPricingFlowId();
      if (!flowId) {
        if (pricingStatus) pricingStatus.textContent = "Choose a flow first.";
        return;
      }
      const pricing = normalizePricingConfig(readPricingFromUI(), getPricingServiceList(flowId).map((x) => x.key));
      const pricingByFlow = {
        ...(workspaceModel?.pricingByFlow || {}),
        [flowId]: pricing
      };
      try {
        await apiPatch(`/api/account/workspace?to=${encodeURIComponent(activeTo)}`, {
          workspace: { pricingByFlow }
        });
        workspaceModel.pricingByFlow = pricingByFlow;
        workspaceBaseline.pricingByFlow = JSON.stringify(pricingByFlow);
        workspaceBaseline.pricingFlowId = flowId;
        syncPricingDirtyState();
        if (pricingStatus) pricingStatus.textContent = "Saved";
        showSettingsToast("Pricing saved");
      } catch (err) {
        if (pricingStatus) pricingStatus.textContent = "Failed to save.";
        showSettingsToast(err?.message || "Failed to save pricing", true);
        console.error(err);
      }
    });

    workspaceResetUserBtn?.addEventListener("click", async () => {
      if (!canResetWorkspacePasswords) {
        if (workspaceResetUserStatus) workspaceResetUserStatus.textContent = "Only owners or admins can reset passwords";
        return;
      }
      const userId = String(workspaceResetUserSelect?.value || "").trim();
      const password = String(workspaceResetUserPasswordInput?.value || "");
      if (!workspaceResetCodeState.verified || workspaceResetCodeState.userId !== userId) {
        if (workspaceResetUserStatus) workspaceResetUserStatus.textContent = "Verify code first";
        return;
      }
      if (!userId || !password) {
        if (workspaceResetUserStatus) workspaceResetUserStatus.textContent = "User and new password are required";
        return;
      }
      const strongErr = validateStrongPassword(password);
      if (strongErr) {
        if (workspaceResetUserStatus) workspaceResetUserStatus.textContent = strongErr;
        return;
      }
      try {
        await apiPost(`/api/account/users/${encodeURIComponent(userId)}/reset-password`, { password });
        if (workspaceResetUserStatus) workspaceResetUserStatus.textContent = "Password reset";
        resetWorkspaceResetFlow();
        showSettingsToast("Password reset");
      } catch (err) {
        if (workspaceResetUserStatus) workspaceResetUserStatus.textContent = err?.message || "Reset failed";
        showSettingsToast(err?.message || "Failed to reset password", true);
      }
    });

    workspaceSendResetCodeBtn?.addEventListener("click", () => {
      if (!canResetWorkspacePasswords) {
        if (workspaceResetUserStatus) workspaceResetUserStatus.textContent = "Only owners or admins can send reset codes";
        return;
      }
      const userId = String(workspaceResetUserSelect?.value || "").trim();
      const selected = workspaceResetUserSelect?.selectedOptions?.[0];
      const label = String(selected?.textContent || "").trim();
      const email = label.includes(" (") ? label.split(" (")[0].trim() : "";
      if (!userId || !email || !email.includes("@")) {
        if (workspaceResetUserStatus) workspaceResetUserStatus.textContent = "Select a valid user email first";
        return;
      }
      const code = String(Math.floor(10000000 + Math.random() * 90000000));
      workspaceResetCodeState.userId = userId;
      workspaceResetCodeState.code = code;
      workspaceResetCodeState.verified = false;
      workspaceResetCodeState.sentAt = Date.now();
      if (workspaceResetCodeInput) workspaceResetCodeInput.value = "";
      if (workspaceResetUserPasswordInput) workspaceResetUserPasswordInput.value = "";
      if (workspaceResetNewPasswordRow) workspaceResetNewPasswordRow.style.display = "none";
      if (workspaceSendResetCodeBtn) workspaceSendResetCodeBtn.style.display = "";
      if (workspaceVerifyResetCodeBtn) workspaceVerifyResetCodeBtn.style.display = "";
      if (workspaceResetUserBtn) workspaceResetUserBtn.style.display = "none";
      const subject = encodeURIComponent("Relay password reset code");
      const body = encodeURIComponent(`Your temporary Relay password reset code is: ${code}\n\nUse this code to log in, then change your password.`);
      window.location.href = `mailto:${encodeURIComponent(email)}?subject=${subject}&body=${body}`;
      if (workspaceResetUserStatus) {
        workspaceResetUserStatus.textContent = `Code sent to ${email}. Enter code and verify.`;
      }
    });

    workspaceVerifyResetCodeBtn?.addEventListener("click", () => {
      if (!canResetWorkspacePasswords) {
        if (workspaceResetUserStatus) workspaceResetUserStatus.textContent = "Only owners or admins can verify reset codes";
        return;
      }
      const userId = String(workspaceResetUserSelect?.value || "").trim();
      const entered = String(workspaceResetCodeInput?.value || "").trim();
      if (!workspaceResetCodeState.code || !workspaceResetCodeState.userId) {
        if (workspaceResetUserStatus) workspaceResetUserStatus.textContent = "Send a code first";
        return;
      }
      if (workspaceResetCodeState.userId !== userId) {
        if (workspaceResetUserStatus) workspaceResetUserStatus.textContent = "Code is for a different user. Send a new code.";
        return;
      }
      if (Date.now() - Number(workspaceResetCodeState.sentAt || 0) > (15 * 60 * 1000)) {
        if (workspaceResetUserStatus) workspaceResetUserStatus.textContent = "Code expired. Send a new code.";
        return;
      }
      if (!entered || entered !== workspaceResetCodeState.code) {
        if (workspaceResetUserStatus) workspaceResetUserStatus.textContent = "Invalid code";
        return;
      }
      workspaceResetCodeState.verified = true;
      if (workspaceResetNewPasswordRow) workspaceResetNewPasswordRow.style.display = "";
      if (workspaceSendResetCodeBtn) workspaceSendResetCodeBtn.style.display = "none";
      if (workspaceVerifyResetCodeBtn) workspaceVerifyResetCodeBtn.style.display = "none";
      if (workspaceResetUserBtn) workspaceResetUserBtn.style.display = "";
      if (workspaceResetUserStatus) workspaceResetUserStatus.textContent = "Code verified. Enter a strong new password.";
    });

    workspaceResetUserSelect?.addEventListener("change", () => {
      resetWorkspaceResetFlow();
      if (workspaceResetUserStatus) workspaceResetUserStatus.textContent = "";
    });

    defaultFlowSelect?.addEventListener("change", syncDefaultsDirtyState);
    pricingFlowSelect?.addEventListener("change", () => {
      selectedPricingFlowId = String(pricingFlowSelect.value || "").trim();
      if (pricingStatus) pricingStatus.textContent = "";
      renderPricingEditor();
      syncPricingDirtyState();
    });
    wrap.querySelectorAll("[data-pricing-kind][data-pricing-key][data-pricing-field]").forEach((el) => {
      el.addEventListener("input", syncPricingDirtyState);
      el.addEventListener("change", syncPricingDirtyState);
    });
    wrap.addEventListener("input", (e) => {
      const t = e.target;
      if (!(t instanceof Element)) return;
      if (t.matches("[data-pricing-kind][data-pricing-key][data-pricing-field]")) syncPricingDirtyState();
    });
    wrap.addEventListener("change", (e) => {
      const t = e.target;
      if (!(t instanceof Element)) return;
      if (t.matches("[data-pricing-kind][data-pricing-key][data-pricing-field]")) syncPricingDirtyState();
    });
    wrap.addEventListener("click", (e) => {
      const t = e.target;
      if (!(t instanceof Element)) return;
      const flowId = getPricingFlowId();
      if (t.matches("[data-workspace-add-service]")) {
        e.preventDefault();
        addWorkspaceServiceToFlow(flowId);
        return;
      }
      if (t.matches("[data-workspace-remove-service]")) {
        e.preventDefault();
        removeWorkspaceServiceFromFlow(flowId, String(t.getAttribute("data-workspace-remove-service") || ""));
        return;
      }
      if (t.matches("[data-workspace-add-scope]")) {
        e.preventDefault();
        addWorkspaceScopeToFlow(flowId, String(t.getAttribute("data-workspace-add-scope") || ""));
        return;
      }
      if (t.matches("[data-workspace-remove-scope]")) {
        e.preventDefault();
        const token = String(t.getAttribute("data-workspace-remove-scope") || "");
        const [serviceKey, scopeKey] = token.split("::");
        removeWorkspaceScopeFromFlow(flowId, serviceKey, scopeKey);
        return;
      }
    });
    [automationsEnabledToggle, automationsQuietHoursToggle, automationsSafeOptOutToggle, automationsProfileSelect]
      .forEach((el) => el?.addEventListener("change", syncAutomationDefaultsDirtyState));

    automationsSaveBtn?.addEventListener("click", async () => {
      const activeTo = getActiveTo();
      const automation = readAutomationDefaultsFromUI();
      try {
        await apiPatch(`/api/account/workspace?to=${encodeURIComponent(activeTo)}`, {
          defaults: { automation }
        });
        workspaceBaseline.automationDefaults = JSON.stringify(automation);
        syncAutomationDefaultsDirtyState();
        if (automationsStatus) automationsStatus.textContent = "Saved";
        showSettingsToast("Automation settings saved");
      } catch (err) {
        if (automationsStatus) automationsStatus.textContent = "Failed to save.";
        showSettingsToast(err?.message || "Failed to save automation settings", true);
        console.error(err);
      }
    });

    // ---- Calendly load/save (based on ACTIVE_TO) ----
    const input = document.getElementById("calendlyUrl");
    const slotIntervalInput = document.getElementById("slotIntervalMin");
    const leadTimeInput = document.getElementById("leadTimeMin");
    const bufferInput = document.getElementById("bufferMin");
    const maxPerDayInput = document.getElementById("maxBookingsPerDay");
    const status = document.getElementById("saveCalendlyStatus");
    const saveBtn = document.getElementById("saveCalendlyBtn");
    const generatedBookingUrlEl = document.getElementById("generatedBookingUrl");
    const bookingPreviewFrame = document.getElementById("bookingUrlPreviewFrame");
    const bookingPreviewOpen = document.getElementById("bookingUrlPreviewOpen");

    function setBookingPreview(url) {
      const safeUrl = String(url || "").trim();
      if (generatedBookingUrlEl) {
        generatedBookingUrlEl.href = safeUrl || "#";
        generatedBookingUrlEl.textContent = safeUrl || "Not generated yet";
      }
      if (bookingPreviewFrame) {
        bookingPreviewFrame.src = safeUrl || "about:blank";
      }
      if (bookingPreviewOpen) {
        if (safeUrl) {
          bookingPreviewOpen.href = safeUrl;
          bookingPreviewOpen.style.display = "inline";
        } else {
          bookingPreviewOpen.href = "#";
          bookingPreviewOpen.style.display = "none";
        }
      }
    }

    function syncBookingDirtyState() {
      const url = String(input?.value || "").trim();
      const generatedUrlRaw = String(generatedBookingUrlEl?.getAttribute("href") || "").trim();
      const schedulingSnapshot = JSON.stringify({
        slotIntervalMin: Math.max(10, Number(slotIntervalInput?.value || 30) || 30),
        leadTimeMin: Math.max(0, Number(leadTimeInput?.value || 60) || 60),
        bufferMin: Math.max(0, Number(bufferInput?.value || 0) || 0),
        maxBookingsPerDay: Math.max(0, Number(maxPerDayInput?.value || 0) || 0)
      });
      const valid = !url || url.startsWith("http://") || url.startsWith("https://");
      if (status && !valid) {
        status.textContent = "Booking link must start with http:// or https://";
      }
      if (status && valid && status.textContent.includes("must start")) {
        status.textContent = "";
      }
      if (url && valid) {
        setBookingPreview(url);
      } else if (!url && generatedUrlRaw && generatedUrlRaw !== "#") {
        setBookingPreview(generatedUrlRaw);
      }
      const dirty = valid && (url !== workspaceBaseline.bookingUrl || schedulingSnapshot !== workspaceBaseline.schedulingJson);
      if (saveBtn) {
        saveBtn.disabled = !valid;
        saveBtn.classList.toggle("is-dirty", dirty);
      }
    }

    async function refreshCalendlyUI(){
      const activeTo = getActiveTo();
      if (status) status.textContent = "";

      // 1) Try backend
      try {
        const acct = await loadAccountSettings(activeTo);
        const schedMode = String(acct?.scheduling?.mode || "").toLowerCase();
        const publicToken = String(acct?.scheduling?.publicToken || "").trim();
        const derivedPublicUrl = publicToken ? `${API_BASE}/book/${encodeURIComponent(publicToken)}` : "";
        const publicUrl = String(acct?.scheduling?.publicUrl || derivedPublicUrl || "").trim();
        const schedUrl = (acct?.scheduling?.url || acct?.bookingUrl || "").trim();
        if (slotIntervalInput) slotIntervalInput.value = String(Number(acct?.scheduling?.slotIntervalMin || 30) || 30);
        if (leadTimeInput) leadTimeInput.value = String(Number(acct?.scheduling?.leadTimeMin || 60) || 60);
        if (bufferInput) bufferInput.value = String(Number(acct?.scheduling?.bufferMin || 0) || 0);
        if (maxPerDayInput) maxPerDayInput.value = String(Number(acct?.scheduling?.maxBookingsPerDay || 0) || 0);
        workspaceBaseline.schedulingJson = JSON.stringify({
          slotIntervalMin: Number(acct?.scheduling?.slotIntervalMin || 30) || 30,
          leadTimeMin: Number(acct?.scheduling?.leadTimeMin || 60) || 60,
          bufferMin: Number(acct?.scheduling?.bufferMin || 0) || 0,
          maxBookingsPerDay: Number(acct?.scheduling?.maxBookingsPerDay || 0) || 0
        });

        if (schedMode === "internal") {
          if (input) input.value = "";
          if (publicUrl) cacheBookingUrl(activeTo, publicUrl);
          setBookingPreview(publicUrl);
          workspaceBaseline.bookingUrl = "";
          syncBookingDirtyState();
          if (status && publicUrl) status.textContent = "Using generated Relay booking link.";
          return;
        }

        if (schedUrl) {
          if (input) input.value = schedUrl;
          cacheBookingUrl(activeTo, schedUrl);
          setBookingPreview(schedUrl);
          workspaceBaseline.bookingUrl = schedUrl;
          syncBookingDirtyState();
          if (status) status.textContent = "Using custom external booking link.";
          return;
        }

        setBookingPreview(publicUrl);
        if (status && publicUrl) status.textContent = "Generated Relay booking link is ready.";
      } catch (e) {
        console.error(e);
      }

      // 2) Fallback to local cache (dev-friendly)
      const cached = getCachedBookingUrl(activeTo);
      if (input) input.value = cached;
      setBookingPreview(cached);
      workspaceBaseline.bookingUrl = cached;
      workspaceBaseline.schedulingJson = JSON.stringify({
        slotIntervalMin: Math.max(10, Number(slotIntervalInput?.value || 30) || 30),
        leadTimeMin: Math.max(0, Number(leadTimeInput?.value || 60) || 60),
        bufferMin: Math.max(0, Number(bufferInput?.value || 0) || 0),
        maxBookingsPerDay: Math.max(0, Number(maxPerDayInput?.value || 0) || 0)
      });
      syncBookingDirtyState();

      if (!cached && status && !status.textContent) {
        status.textContent = "No booking URL available yet.";
      }
    }

    await refreshCalendlyUI();
    input?.addEventListener("input", syncBookingDirtyState);
    slotIntervalInput?.addEventListener("input", syncBookingDirtyState);
    leadTimeInput?.addEventListener("input", syncBookingDirtyState);
    bufferInput?.addEventListener("input", syncBookingDirtyState);
    maxPerDayInput?.addEventListener("input", syncBookingDirtyState);

    saveBtn?.addEventListener("click", async (e) => {
      e.preventDefault();
      e.stopPropagation();
      const activeTo = getActiveTo();
      const url = (input?.value || "").trim();
      const slotIntervalMin = Math.max(10, Number(slotIntervalInput?.value || 30) || 30);
      const leadTimeMin = Math.max(0, Number(leadTimeInput?.value || 60) || 60);
      const bufferMin = Math.max(0, Number(bufferInput?.value || 0) || 0);
      const maxBookingsPerDay = Math.max(0, Number(maxPerDayInput?.value || 0) || 0);
      if (status) status.textContent = "";

      // light validation
      if (url && !(url.startsWith("http://") || url.startsWith("https://"))) {
        if (status) status.textContent = "Booking link must start with http:// or https://";
        return;
      }

      // optimistic cache so it survives refresh even if backend restarts
      cacheBookingUrl(activeTo, url);

      // button feedback
      const originalHTML = saveBtn.innerHTML;
      saveBtn.classList.remove("saved");
      saveBtn.disabled = true;

      try {
        const mode = url ? "link" : "internal";
        await apiPost("/api/account/scheduling", {
          to: activeTo,
          scheduling: {
            mode,
            url,
            label: "Book a time",
            instructions: "",
            slotIntervalMin,
            leadTimeMin,
            bufferMin,
            maxBookingsPerDay
          }
        });
      } catch (e) {
        //fallback for safety
        await apiPost("/api/account/booking", { to: activeTo, bookingUrl: url });
      }

      saveBtn.classList.add("saved");
      saveBtn.innerHTML = `
        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
          <path d="M20 6 9 17l-5-5"></path>
        </svg>
        Saved
      `;
      if (status) status.textContent = "Saved";
      workspaceBaseline.bookingUrl = url;
      workspaceBaseline.schedulingJson = JSON.stringify({
        slotIntervalMin,
        leadTimeMin,
        bufferMin,
        maxBookingsPerDay
      });
      syncBookingDirtyState();

      await refreshCalendlyUI();

      setTimeout(() => {
        saveBtn.classList.remove("saved");
        saveBtn.innerHTML = originalHTML;
        saveBtn.disabled = false;
      }, 1600);
    });

    if (!window.__relayGlobalSubmitGuardBound) {
      window.__relayGlobalSubmitGuardBound = true;
      document.addEventListener("submit", (e) => {
        if (e.target && e.target.id === "loginForm") return; // allow login overlay form
        e.preventDefault();
        e.stopPropagation();
      }, true);
    }

    // ---- simulator ----
    const devModeEnabled = document.getElementById("devModeEnabled");
    const devAutoCreateTenants = document.getElementById("devAutoCreateTenants");
    const devVerboseTenantLogs = document.getElementById("devVerboseTenantLogs");
    const devSimulateOutbound = document.getElementById("devSimulateOutbound");
    const devSettingsSaveBtn = document.getElementById("devSettingsSaveBtn");
    const devSettingsStatus = document.getElementById("devSettingsStatus");
    const simulateConversationBtn = document.getElementById("simulateConversationBtn");
    const simulateConversationStatus = document.getElementById("simulateConversationStatus");

    function applyDevSettingsUI(settings) {
      if (devModeEnabled) devModeEnabled.checked = settings.enabled === true;
      if (devAutoCreateTenants) devAutoCreateTenants.checked = settings.autoCreateTenants === true;
      if (devVerboseTenantLogs) devVerboseTenantLogs.checked = settings.verboseTenantLogs === true;
      if (devSimulateOutbound) devSimulateOutbound.checked = settings.simulateOutbound === true;
      if (devAutoCreateTenants) devAutoCreateTenants.disabled = devModeEnabled?.checked !== true;
    }

    function readDevSettingsFromUI() {
      return JSON.stringify({
        enabled: devModeEnabled?.checked === true,
        autoCreateTenants: devAutoCreateTenants?.checked === true,
        verboseTenantLogs: devVerboseTenantLogs?.checked === true,
        simulateOutbound: devSimulateOutbound?.checked === true
      });
    }

    function syncDevDirtyState() {
      const dirty = readDevSettingsFromUI() !== devBaseline;
      setDirtyButton(devSettingsSaveBtn, dirty);
    }

    async function loadDevSettingsUI() {
      if (!canAccessDeveloperRoutes()) {
        applyDevSettingsUI({});
        devBaseline = readDevSettingsFromUI();
        syncDevDirtyState();
        if (devSettingsStatus) devSettingsStatus.textContent = "Developer tools require superadmin access.";
        return;
      }
      try {
        const data = await apiGet("/api/dev/settings");
        applyDevSettingsUI(data?.settings || {});
        devBaseline = readDevSettingsFromUI();
        syncDevDirtyState();
      } catch (err) {
        if (isApi404Error(err)) {
          applyDevSettingsUI({});
          devBaseline = readDevSettingsFromUI();
          syncDevDirtyState();
          if (devSettingsStatus) devSettingsStatus.textContent = "Developer tools are unavailable in production.";
          return;
        }
        console.error("Failed to load dev settings:", err);
        if (devSettingsStatus) devSettingsStatus.textContent = "Failed to load.";
      }
    }

    devModeEnabled?.addEventListener("change", () => {
      if (devAutoCreateTenants) {
        devAutoCreateTenants.disabled = devModeEnabled.checked !== true;
      }
      syncDevDirtyState();
    });
    devAutoCreateTenants?.addEventListener("change", syncDevDirtyState);
    devVerboseTenantLogs?.addEventListener("change", syncDevDirtyState);
    devSimulateOutbound?.addEventListener("change", syncDevDirtyState);

    devSettingsSaveBtn?.addEventListener("click", async () => {
      if (!canAccessDeveloperRoutes()) {
        if (devSettingsStatus) devSettingsStatus.textContent = "Developer tools require superadmin access.";
        return;
      }
      try {
        const patch = {
          enabled: devModeEnabled?.checked === true,
          autoCreateTenants: devAutoCreateTenants?.checked === true,
          verboseTenantLogs: devVerboseTenantLogs?.checked === true,
          simulateOutbound: devSimulateOutbound?.checked === true
        };
        const res = await apiPatch("/api/dev/settings", patch);
        applyDevSettingsUI({ ...patch, ...(res?.settings || {}) });
        devBaseline = readDevSettingsFromUI();
        syncDevDirtyState();
        if (devSettingsStatus) devSettingsStatus.textContent = "Saved ?";
        showSettingsToast("Developer settings saved");
      } catch (err) {
        console.error("Failed to save dev settings:", err);
        if (devSettingsStatus) devSettingsStatus.textContent = "Failed to save.";
        showSettingsToast(err?.message || "Failed to save developer settings", true);
      }
    });

    simulateConversationBtn?.addEventListener("click", async () => {
      if (!canAccessDeveloperRoutes()) {
        if (simulateConversationStatus) simulateConversationStatus.textContent = "Conversation simulation is only available in developer mode.";
        return;
      }
      if (simulateConversationBtn) simulateConversationBtn.disabled = true;
      if (simulateConversationStatus) simulateConversationStatus.textContent = "Simulating conversation...";
      try {
        const out = await runDeveloperConversationSimulation();
        if (simulateConversationStatus) {
          simulateConversationStatus.textContent = out?.message || "Simulation complete.";
        }
      } catch (err) {
        if (simulateConversationStatus) simulateConversationStatus.textContent = err?.message || "Simulation failed.";
      } finally {
        if (simulateConversationBtn) simulateConversationBtn.disabled = false;
      }
    });

    await loadDevSettingsUI();

    // ---- superadmin account/user management ----
    const adminPanel = document.getElementById("adminSuperPanel");
    const adminAccountsSummary = document.getElementById("adminAccountsSummary");
    const adminAccountsTableBody = document.getElementById("adminAccountsTableBody");
    const adminQuickCreateBtn = document.getElementById("adminQuickCreateBtn");
    const adminQuickCreateTemplate = document.getElementById("adminQuickCreateTemplate");
    const adminQuickCreateStatus = document.getElementById("adminQuickCreateStatus");
    const adminResetUserId = document.getElementById("adminResetUserId");
    const adminResetPasswordInput = document.getElementById("adminResetPasswordInput");
    const adminResetPasswordBtn = document.getElementById("adminResetPasswordBtn");
    const adminResetPasswordStatus = document.getElementById("adminResetPasswordStatus");
    const adminRefreshBtn = document.getElementById("adminRefreshBtn");
    const adminUsersOut = document.getElementById("adminUsersOut");
    let adminUsersCache = [];
    function wait(ms) { return new Promise((resolve) => setTimeout(resolve, ms)); }
    
    function formatAdminCreatedAt(value) {
      const ts = Number(value || 0);
      if (!Number.isFinite(ts) || ts <= 0) return "-";
      try {
        return new Date(ts).toLocaleString();
      } catch {
        return "-";
      }
    }

    function renderAdminAccountsTable(accounts) {
      const rows = Array.isArray(accounts) ? accounts : [];
      if (adminAccountsSummary) {
        adminAccountsSummary.textContent = `${rows.length} account${rows.length === 1 ? "" : "s"} created`;
      }
      if (!adminAccountsTableBody) return;
      adminAccountsTableBody.innerHTML = "";
      if (!rows.length) {
        const tr = document.createElement("tr");
        tr.innerHTML = `<td colspan="4" class="billing-empty-row">No accounts yet.</td>`;
        adminAccountsTableBody.appendChild(tr);
        return;
      }
      for (const account of rows) {
        const tr = document.createElement("tr");
        tr.innerHTML = `
          <td>${escapeHtml(account?.businessName || "-")}</td>
          <td>${escapeHtml(account?.accountId || "-")}</td>
          <td>${escapeHtml(account?.to || "-")}</td>
          <td>${escapeHtml(formatAdminCreatedAt(account?.createdAt))}</td>
        `;
        adminAccountsTableBody.appendChild(tr);
      }
    }

    function applyAdminUserSelect(users) {
      if (!adminResetUserId) return;
      adminResetUserId.innerHTML = "";
      const list = Array.isArray(users) ? users : [];
      for (const u of list) {
        const opt = document.createElement("option");
        opt.value = String(u.id || "");
        opt.textContent = `${u.email} (${u.role})${u.disabled ? " [disabled]" : ""}`;
        adminResetUserId.appendChild(opt);
      }
    }

    async function refreshAdminPanel() {
      if (String(authState?.user?.role || "") !== "superadmin") {
        if (adminPanel) adminPanel.style.display = "none";
        return;
      }
      if (adminPanel) adminPanel.style.display = "";
      try {
        const [accountsRes, usersRes, meRes] = await Promise.all([
          apiGet("/api/admin/accounts"),
          apiGet("/api/admin/users"),
          apiGet("/api/auth/me")
        ]);
        renderAdminAccountsTable(accountsRes?.accounts || []);
        if (adminUsersOut) adminUsersOut.textContent = JSON.stringify(usersRes?.users || [], null, 2);
        adminUsersCache = Array.isArray(usersRes?.users) ? usersRes.users : [];
        applyAdminUserSelect(adminUsersCache);
        setSession(meRes || {});
      } catch (err) {
        if (adminAccountsSummary) adminAccountsSummary.textContent = err?.message || "Failed to load accounts";
        renderAdminAccountsTable([]);
      }
    }

    adminRefreshBtn?.addEventListener("click", refreshAdminPanel);

    adminQuickCreateBtn?.addEventListener("click", async () => {
      const stamp = String(Date.now());
      const suffix = stamp.slice(-6);
      const to = `+1555${stamp.slice(-7)}`;
      const template = String(adminQuickCreateTemplate?.value || "detailer").trim();
      const templateLabelMap = {
        detailer: "Detailer"
      };
      const templateLabel = templateLabelMap[template] || "Detailer";
      const businessName = `Relay ${templateLabel} ${suffix}`;
      const email = `owner${suffix}@relay.local`;
      const password = "TempPass123!";
      try {
        if (adminQuickCreateBtn) adminQuickCreateBtn.disabled = true;
        if (adminQuickCreateStatus) adminQuickCreateStatus.textContent = "Creating test account...";
        const accountRes = await apiPost("/api/admin/accounts", {
          to,
          businessName,
          nicheTemplate: template,
          activateSubscription: true
        });
        const accountId = String(accountRes?.account?.accountId || "").trim();
        if (!accountId) throw new Error("No accountId returned");
        await apiPost("/api/admin/users", {
          email,
          password,
          role: "owner",
          accountIds: [accountId]
        });
        if (adminQuickCreateStatus) {
          adminQuickCreateStatus.textContent = `Created ${businessName} | ${email} / ${password}`;
        }
        await wait(250);
        await refreshAdminPanel();
      } catch (err) {
        if (adminQuickCreateStatus) adminQuickCreateStatus.textContent = err?.message || "Failed to create test account";
      } finally {
        if (adminQuickCreateBtn) adminQuickCreateBtn.disabled = false;
      }
    });

    adminResetPasswordBtn?.addEventListener("click", async () => {
      const userId = String(adminResetUserId?.value || "").trim();
      const nextPassword = String(adminResetPasswordInput?.value || "");
      if (!userId || !nextPassword) {
        if (adminResetPasswordStatus) adminResetPasswordStatus.textContent = "User and password are required";
        return;
      }
      try {
        await apiPut(`/api/admin/users/${encodeURIComponent(userId)}`, { password: nextPassword });
        const user = adminUsersCache.find((u) => String(u.id || "") === userId);
        if (adminResetPasswordStatus) adminResetPasswordStatus.textContent = `Password reset for ${user?.email || "user"}`;
        if (adminResetPasswordInput) adminResetPasswordInput.value = "";
        await wait(250);
        await refreshAdminPanel();
        showSettingsToast("Password reset");
      } catch (err) {
        if (adminResetPasswordStatus) adminResetPasswordStatus.textContent = err?.message || "Reset failed";
        showSettingsToast("Failed to reset password", true);
      }
    });

    // ---- superadmin Twilio + assignments + billing operations ----
    const superadminOpsPanel = document.getElementById("superadminOpsPanel");
    const superadminOpsAsOf = document.getElementById("superadminOpsAsOf");
    const superadminOpsRefreshBtn = document.getElementById("superadminOpsRefreshBtn");
    const superadminOpsWorkspaceCount = document.getElementById("superadminOpsWorkspaceCount");
    const superadminOpsTwilioConnected = document.getElementById("superadminOpsTwilioConnected");
    const superadminOpsMainSid = document.getElementById("superadminOpsMainSid");
    const superadminOpsMainSidCount = document.getElementById("superadminOpsMainSidCount");
    const superadminOpsTableBody = document.getElementById("superadminOpsTableBody");
    const superadminPlatformStripeSecretKeyInput = document.getElementById("superadminPlatformStripeSecretKeyInput");
    const superadminPlatformStripePublishableKeyInput = document.getElementById("superadminPlatformStripePublishableKeyInput");
    const superadminPlatformStripeWebhookSecretInput = document.getElementById("superadminPlatformStripeWebhookSecretInput");
    const superadminPlatformStripeStatusBadge = document.getElementById("superadminPlatformStripeStatusBadge");
    const superadminPlatformStripeDetails = document.getElementById("superadminPlatformStripeDetails");
    const superadminPlatformStripeActionStatus = document.getElementById("superadminPlatformStripeActionStatus");
    const superadminPlatformStripeSaveBtn = document.getElementById("superadminPlatformStripeSaveBtn");
    const superadminPlatformStripeTestBtn = document.getElementById("superadminPlatformStripeTestBtn");
    const superadminPlatformStripeDisconnectBtn = document.getElementById("superadminPlatformStripeDisconnectBtn");
    const superadminTwilioInventorySummary = document.getElementById("superadminTwilioInventorySummary");
    const superadminTwilioInventoryBody = document.getElementById("superadminTwilioInventoryBody");
    const superadminTwilioInventoryError = document.getElementById("superadminTwilioInventoryError");
    const superadminNumberSearchCountry = document.getElementById("superadminNumberSearchCountry");
    const superadminNumberSearchAreaCode = document.getElementById("superadminNumberSearchAreaCode");
    const superadminNumberSearchContains = document.getElementById("superadminNumberSearchContains");
    const superadminNumberSearchLimit = document.getElementById("superadminNumberSearchLimit");
    const superadminNumberPurchaseLabel = document.getElementById("superadminNumberPurchaseLabel");
    const superadminNumberWebhookBaseUrl = document.getElementById("superadminNumberWebhookBaseUrl");
    const superadminNumberSearchStatus = document.getElementById("superadminNumberSearchStatus");
    const superadminNumberSearchBtn = document.getElementById("superadminNumberSearchBtn");
    const superadminNumberSearchResultsBody = document.getElementById("superadminNumberSearchResultsBody");
    const superadminTwilioAccountSelect = document.getElementById("superadminTwilioAccountSelect");
    const superadminTwilioStatusBadge = document.getElementById("superadminTwilioStatusBadge");
    const superadminTwilioAccountSidInput = document.getElementById("superadminTwilioAccountSidInput");
    const superadminTwilioApiKeySidInput = document.getElementById("superadminTwilioApiKeySidInput");
    const superadminTwilioApiKeySecretInput = document.getElementById("superadminTwilioApiKeySecretInput");
    const superadminTwilioWebhookTokenInput = document.getElementById("superadminTwilioWebhookTokenInput");
    const superadminTwilioMessagingServiceSidInput = document.getElementById("superadminTwilioMessagingServiceSidInput");
    const superadminTwilioPhoneNumberInput = document.getElementById("superadminTwilioPhoneNumberInput");
    const superadminTwilioVoiceForwardToInput = document.getElementById("superadminTwilioVoiceForwardToInput");
    const superadminTwilioVoiceDialTimeoutInput = document.getElementById("superadminTwilioVoiceDialTimeoutInput");
    const superadminTwilioActionStatus = document.getElementById("superadminTwilioActionStatus");
    const superadminTwilioSaveBtn = document.getElementById("superadminTwilioSaveBtn");
    const superadminTwilioTestBtn = document.getElementById("superadminTwilioTestBtn");
    const superadminTwilioDisconnectBtn = document.getElementById("superadminTwilioDisconnectBtn");
    let superadminOpsState = { workspaces: [] };
    let superadminAvailableNumbers = [];
    let superadminPlatformStripeState = null;

    function formatMoneyFromMonthly(monthlyPrice) {
      return Number(monthlyPrice || 0).toLocaleString("en-US", { style: "currency", currency: "USD", maximumFractionDigits: 0 });
    }

    function renderSuperadminPlatformStripe(cfg) {
      const stripe = cfg && typeof cfg === "object" ? cfg : {};
      superadminPlatformStripeState = stripe;
      if (superadminPlatformStripeStatusBadge) {
        const status = stripe.enabled ? `Connected (${stripe.lastStatus || "ok"})` : "Not connected";
        superadminPlatformStripeStatusBadge.textContent = status;
      }
      if (superadminPlatformStripeDetails) {
        const details = stripe.enabled
          ? `Account: ${stripe.accountId || "--"} | Email: ${stripe.accountEmail || "--"} | Last tested: ${formatSyncTs(stripe.lastTestedAt)}`
          : "Not connected";
        superadminPlatformStripeDetails.textContent = details;
      }
      if (superadminPlatformStripePublishableKeyInput) {
        superadminPlatformStripePublishableKeyInput.value = String(stripe.publishableKey || "");
      }
      if (superadminPlatformStripeSecretKeyInput) {
        superadminPlatformStripeSecretKeyInput.value = "";
        superadminPlatformStripeSecretKeyInput.placeholder = stripe.secretKeyMasked
          ? `Saved (${stripe.secretKeyMasked})`
          : "sk_live_...";
      }
      if (superadminPlatformStripeWebhookSecretInput) {
        superadminPlatformStripeWebhookSecretInput.value = "";
        superadminPlatformStripeWebhookSecretInput.placeholder = stripe.webhookSecretMasked
          ? `Saved (${stripe.webhookSecretMasked})`
          : "whsec_...";
      }
    }

    async function refreshSuperadminPlatformStripe() {
      if (String(authState?.user?.role || "").toLowerCase() !== "superadmin") return;
      try {
        const res = await apiGet("/api/admin/developer/platform-billing/stripe");
        renderSuperadminPlatformStripe(res?.stripe || {});
      } catch (err) {
        if (superadminPlatformStripeActionStatus) {
          superadminPlatformStripeActionStatus.textContent = err?.message || "Failed to load platform Stripe";
        }
      }
    }

    function renderSuperadminTwilioEditor() {
      const accountId = String(superadminTwilioAccountSelect?.value || "");
      const row = (superadminOpsState.workspaces || []).find((x) => String(x.accountId || "") === accountId) || null;
      if (!row) {
        if (superadminTwilioStatusBadge) superadminTwilioStatusBadge.textContent = "No workspace selected";
        return;
      }
      const tw = row.twilio || {};
      if (superadminTwilioStatusBadge) {
        const status = tw.enabled ? `Connected (${tw.lastStatus || "ok"})` : "Not connected";
        superadminTwilioStatusBadge.textContent = status;
      }
      if (superadminTwilioAccountSidInput) superadminTwilioAccountSidInput.value = String(tw.accountSid || "");
      if (superadminTwilioApiKeySidInput) superadminTwilioApiKeySidInput.value = String(tw.apiKeySid || "");
      if (superadminTwilioApiKeySecretInput) {
        superadminTwilioApiKeySecretInput.value = "";
        superadminTwilioApiKeySecretInput.placeholder = tw.hasApiKeySecret ? "Saved (hidden). Enter new value to change." : "Leave blank to keep current";
      }
      if (superadminTwilioWebhookTokenInput) {
        superadminTwilioWebhookTokenInput.value = "";
        superadminTwilioWebhookTokenInput.placeholder = tw.hasWebhookAuthToken ? "Saved (hidden). Enter new value to change." : "Optional (recommended)";
      }
      if (superadminTwilioMessagingServiceSidInput) superadminTwilioMessagingServiceSidInput.value = String(tw.messagingServiceSid || "");
      if (superadminTwilioPhoneNumberInput) superadminTwilioPhoneNumberInput.value = String(tw.phoneNumber || "");
      if (superadminTwilioVoiceForwardToInput) superadminTwilioVoiceForwardToInput.value = String(tw.voiceForwardTo || "");
      if (superadminTwilioVoiceDialTimeoutInput) superadminTwilioVoiceDialTimeoutInput.value = String(Number(tw.voiceDialTimeoutSec || 20) || 20);
    }

    function renderSuperadminOpsView(payload) {
      const overview = payload && typeof payload === "object" ? payload : {};
      const summary = overview.summary || {};
      const workspaces = Array.isArray(overview.workspaces) ? overview.workspaces : [];
      superadminOpsState = { workspaces };

      if (superadminOpsAsOf) superadminOpsAsOf.textContent = `As of ${new Date(Number(overview.asOf || Date.now())).toLocaleString()}`;
      if (superadminOpsWorkspaceCount) superadminOpsWorkspaceCount.textContent = String(Number(summary.workspaceCount || 0));
      if (superadminOpsTwilioConnected) superadminOpsTwilioConnected.textContent = String(Number(summary.twilioConnectedCount || 0));
      if (superadminOpsMainSid) superadminOpsMainSid.textContent = String(summary.mainTwilioAccountSid || "--");
      if (superadminOpsMainSidCount) superadminOpsMainSidCount.textContent = String(Number(summary.mainTwilioWorkspaceCount || 0));

      if (superadminOpsTableBody) {
        if (!workspaces.length) {
          superadminOpsTableBody.innerHTML = `<tr><td colspan="5" class="billing-empty-row">No workspaces found.</td></tr>`;
        } else {
          superadminOpsTableBody.innerHTML = workspaces.map((row) => {
            const numbers = Array.isArray(row.numbers) ? row.numbers : [];
            const assigned = Array.isArray(row.assignedUsers) ? row.assignedUsers : [];
            const tw = row.twilio || {};
            const bl = row.billing || {};
            const assignedText = assigned.length
              ? assigned.map((u) => `${u.email} (${u.role})`).join(", ")
              : "Unassigned";
            const twilioText = tw.enabled
              ? `${tw.accountSidMasked || "--"} | ${tw.phoneNumber || tw.messagingServiceSid || "sender not set"}`
              : "Not connected";
            const billingText = `${bl.planName || "Plan"} (${bl.planStatus || "active"}) | ${formatMoneyFromMonthly(bl.priceMonthly)}/mo`;
            return `
              <tr>
                <td>${escapeHtml(row.businessName || "Workspace")}<div class="p">${escapeHtml(row.to || "--")}</div></td>
                <td>${escapeHtml(numbers.join(", ") || "--")}</td>
                <td>${escapeHtml(assignedText)}</td>
                <td>${escapeHtml(twilioText)}</td>
                <td>${escapeHtml(billingText)}</td>
              </tr>
            `;
          }).join("");
        }
      }

      if (superadminTwilioAccountSelect) {
        const selectedBefore = String(superadminTwilioAccountSelect.value || "");
        superadminTwilioAccountSelect.innerHTML = "";
        for (const row of workspaces) {
          const opt = document.createElement("option");
          opt.value = String(row.accountId || "");
          opt.textContent = `${row.businessName || "Workspace"} (${row.to || "--"})`;
          superadminTwilioAccountSelect.appendChild(opt);
        }
        if (selectedBefore && workspaces.some((w) => String(w.accountId || "") === selectedBefore)) {
          superadminTwilioAccountSelect.value = selectedBefore;
        }
      }
      renderSuperadminTwilioEditor();
    }

    function renderSuperadminTwilioInventory(payload) {
      const data = payload && typeof payload === "object" ? payload : {};
      const summary = data.summary || {};
      const rows = Array.isArray(data.numbers) ? data.numbers : [];
      const errors = Array.isArray(data.errors) ? data.errors : [];

      if (superadminTwilioInventorySummary) {
        superadminTwilioInventorySummary.textContent = `${Number(summary.twilioNumberCount || 0)} numbers | ${Number(summary.assignedCount || 0)} assigned | ${Number(summary.unassignedCount || 0)} unassigned`;
      }

      if (superadminTwilioInventoryBody) {
        if (!rows.length) {
          superadminTwilioInventoryBody.innerHTML = `<tr><td colspan="4" class="billing-empty-row">No Twilio numbers discovered yet.</td></tr>`;
        } else {
          superadminTwilioInventoryBody.innerHTML = rows.map((row) => {
            const assigned = Array.isArray(row.assignedWorkspaces) ? row.assignedWorkspaces : [];
            const discovered = Array.isArray(row.discoveredViaWorkspaces) ? row.discoveredViaWorkspaces : [];
            const assignedText = assigned.length
              ? assigned.map((x) => `${x.businessName} (${x.to || "--"})`).join(", ")
              : "Unassigned";
            const discoveredText = discovered.length
              ? discovered.map((x) => `${x.businessName} (${x.to || "--"})`).join(", ")
              : "--";
            return `
              <tr>
                <td>${escapeHtml(String(row.phoneNumber || "--"))}</td>
                <td>${escapeHtml(String(row.friendlyName || "--"))}</td>
                <td>${escapeHtml(assignedText)}</td>
                <td>${escapeHtml(discoveredText)}</td>
              </tr>
            `;
          }).join("");
        }
      }

      if (superadminTwilioInventoryError) {
        if (!errors.length) superadminTwilioInventoryError.textContent = "";
        else {
          superadminTwilioInventoryError.textContent = `Some Twilio credential sets failed inventory load: ${errors.map((e) => `${e.twilioAccountSidMasked || "--"} (${e.error || "error"})`).join(" | ")}`;
        }
      }
    }

    function renderSuperadminAvailableNumbers() {
      if (!superadminNumberSearchResultsBody) return;
      const rows = Array.isArray(superadminAvailableNumbers) ? superadminAvailableNumbers : [];
      if (!rows.length) {
        superadminNumberSearchResultsBody.innerHTML = `<tr><td colspan="4" class="billing-empty-row">No results yet.</td></tr>`;
        return;
      }
      superadminNumberSearchResultsBody.innerHTML = rows.map((row, idx) => {
        const caps = row?.capabilities && typeof row.capabilities === "object" ? row.capabilities : {};
        const capText = [
          caps.voice ? "Voice" : null,
          caps.sms ? "SMS" : null,
          caps.mms ? "MMS" : null
        ].filter(Boolean).join(", ") || "--";
        const location = [row?.locality, row?.region].filter(Boolean).join(", ") || row?.postalCode || "--";
        return `
          <tr>
            <td>${escapeHtml(String(row?.phoneNumber || "--"))}</td>
            <td>${escapeHtml(String(location || "--"))}</td>
            <td>${escapeHtml(String(capText || "--"))}</td>
            <td><button class="btn primary" type="button" data-superadmin-buy-number="${idx}">Buy + Assign</button></td>
          </tr>
        `;
      }).join("");
    }

    async function searchSuperadminAvailableNumbers() {
      const accountId = String(superadminTwilioAccountSelect?.value || "").trim();
      if (!accountId) {
        if (superadminNumberSearchStatus) superadminNumberSearchStatus.textContent = "Select a workspace first.";
        return;
      }
      const country = String(superadminNumberSearchCountry?.value || "US").trim() || "US";
      const areaCode = String(superadminNumberSearchAreaCode?.value || "").trim();
      const contains = String(superadminNumberSearchContains?.value || "").trim();
      const limit = Number(superadminNumberSearchLimit?.value || 20);
      const qs = new URLSearchParams();
      qs.set("country", country);
      if (areaCode) qs.set("areaCode", areaCode);
      if (contains) qs.set("contains", contains);
      qs.set("limit", String(limit));
      try {
        if (superadminNumberSearchBtn) superadminNumberSearchBtn.disabled = true;
        if (superadminNumberSearchStatus) superadminNumberSearchStatus.textContent = "Searching...";
        const res = await apiGet(`/api/admin/developer/twilio/${encodeURIComponent(accountId)}/available-numbers?${qs.toString()}`);
        superadminAvailableNumbers = Array.isArray(res?.numbers) ? res.numbers : [];
        renderSuperadminAvailableNumbers();
        if (superadminNumberSearchStatus) {
          superadminNumberSearchStatus.textContent = `Found ${superadminAvailableNumbers.length} number${superadminAvailableNumbers.length === 1 ? "" : "s"}.`;
        }
      } catch (err) {
        superadminAvailableNumbers = [];
        renderSuperadminAvailableNumbers();
        if (superadminNumberSearchStatus) superadminNumberSearchStatus.textContent = err?.message || "Search failed";
      } finally {
        if (superadminNumberSearchBtn) superadminNumberSearchBtn.disabled = false;
      }
    }

    async function buyAndAssignSuperadminNumber(index) {
      const accountId = String(superadminTwilioAccountSelect?.value || "").trim();
      const row = Array.isArray(superadminAvailableNumbers) ? superadminAvailableNumbers[index] : null;
      if (!accountId || !row?.phoneNumber) return;
      const label = String(superadminNumberPurchaseLabel?.value || "").trim();
      const webhookBaseUrl = String(superadminNumberWebhookBaseUrl?.value || "").trim();
      try {
        if (superadminNumberSearchStatus) superadminNumberSearchStatus.textContent = `Purchasing ${row.phoneNumber}...`;
        await apiPost(`/api/admin/developer/twilio/${encodeURIComponent(accountId)}/purchase-number`, {
          phoneNumber: String(row.phoneNumber),
          label,
          setPrimary: true,
          webhookBaseUrl
        });
        if (superadminNumberSearchStatus) superadminNumberSearchStatus.textContent = `Purchased and assigned ${row.phoneNumber}.`;
        showSettingsToast(`Purchased ${row.phoneNumber} and assigned to workspace`);
        await refreshSuperadminOps();
      } catch (err) {
        if (superadminNumberSearchStatus) superadminNumberSearchStatus.textContent = err?.message || "Purchase failed";
        showSettingsToast("Failed to purchase number", true);
      }
    }

    async function refreshSuperadminTwilioInventory() {
      if (String(authState?.user?.role || "").toLowerCase() !== "superadmin") return;
      try {
        const inventory = await apiGet("/api/admin/developer/twilio-number-inventory");
        renderSuperadminTwilioInventory(inventory || {});
      } catch (err) {
        if (superadminTwilioInventoryError) {
          superadminTwilioInventoryError.textContent = err?.message || "Failed to load Twilio number inventory";
        }
      }
    }

    async function refreshSuperadminOps() {
      if (String(authState?.user?.role || "").toLowerCase() !== "superadmin") {
        if (superadminOpsPanel) superadminOpsPanel.style.display = "none";
        return;
      }
      if (superadminOpsPanel) superadminOpsPanel.style.display = "";
      try {
        let overview = await apiGet("/api/admin/developer/ops-overview");
        const rows = Array.isArray(overview?.workspaces) ? overview.workspaces : [];
        if (!rows.length) {
          const accountsRes = await apiGet("/api/admin/accounts");
          const accounts = Array.isArray(accountsRes?.accounts) ? accountsRes.accounts : [];
          overview = {
            ok: true,
            asOf: Date.now(),
            summary: {
              workspaceCount: accounts.length,
              twilioConnectedCount: 0,
              mainTwilioAccountSid: "",
              mainTwilioWorkspaceCount: 0
            },
            workspaces: accounts.map((a) => ({
              to: String(a?.to || ""),
              accountId: String(a?.accountId || ""),
              businessName: String(a?.businessName || "").trim() || "Workspace",
              numbers: [String(a?.to || "")].filter(Boolean),
              assignedUsers: [],
              twilio: {
                enabled: false,
                accountSidMasked: "",
                messagingServiceSid: "",
                phoneNumber: "",
                voiceForwardTo: "",
                voiceDialTimeoutSec: 20,
                lastStatus: null,
                lastTestedAt: null
              },
              billing: {
                provider: "demo",
                isLive: false,
                planName: "Pro",
                planStatus: "active",
                priceMonthly: 0,
                nextBillingAt: null,
                billingEmail: ""
              }
            }))
          };
        }
        renderSuperadminOpsView(overview || {});
        await refreshSuperadminPlatformStripe();
        await refreshSuperadminTwilioInventory();
      } catch (err) {
        if (superadminTwilioActionStatus) superadminTwilioActionStatus.textContent = err?.message || "Failed to load overview";
        if (superadminTwilioInventoryError) superadminTwilioInventoryError.textContent = "";
      }
    }

    superadminTwilioAccountSelect?.addEventListener("change", renderSuperadminTwilioEditor);
    superadminTwilioAccountSelect?.addEventListener("change", () => {
      superadminAvailableNumbers = [];
      renderSuperadminAvailableNumbers();
      if (superadminNumberSearchStatus) superadminNumberSearchStatus.textContent = "";
    });
    superadminOpsRefreshBtn?.addEventListener("click", refreshSuperadminOps);
    superadminNumberSearchBtn?.addEventListener("click", searchSuperadminAvailableNumbers);
    superadminNumberSearchResultsBody?.addEventListener("click", (e) => {
      const btn = e.target.closest("[data-superadmin-buy-number]");
      if (!btn) return;
      const idx = Number(btn.getAttribute("data-superadmin-buy-number"));
      if (!Number.isFinite(idx)) return;
      buyAndAssignSuperadminNumber(idx);
    });

    superadminPlatformStripeSaveBtn?.addEventListener("click", async () => {
      const payload = {
        secretKey: String(superadminPlatformStripeSecretKeyInput?.value || "").trim(),
        publishableKey: String(superadminPlatformStripePublishableKeyInput?.value || "").trim(),
        webhookSecret: String(superadminPlatformStripeWebhookSecretInput?.value || "").trim()
      };
      if (!payload.secretKey) {
        if (superadminPlatformStripeActionStatus) superadminPlatformStripeActionStatus.textContent = "Secret key is required.";
        return;
      }
      try {
        await apiPut("/api/admin/developer/platform-billing/stripe", payload);
        if (superadminPlatformStripeActionStatus) superadminPlatformStripeActionStatus.textContent = "Platform Stripe saved.";
        showSettingsToast("Platform Stripe saved");
        await refreshSuperadminPlatformStripe();
      } catch (err) {
        if (superadminPlatformStripeActionStatus) superadminPlatformStripeActionStatus.textContent = err?.message || "Save failed";
        showSettingsToast("Failed to save Platform Stripe", true);
      }
    });

    superadminPlatformStripeTestBtn?.addEventListener("click", async () => {
      try {
        const result = await apiPost("/api/admin/developer/platform-billing/stripe/test", {});
        if (superadminPlatformStripeActionStatus) {
          superadminPlatformStripeActionStatus.textContent = `Platform Stripe test passed (${result?.accountId || "ok"})`;
        }
        showSettingsToast("Platform Stripe test passed");
        await refreshSuperadminPlatformStripe();
      } catch (err) {
        if (superadminPlatformStripeActionStatus) superadminPlatformStripeActionStatus.textContent = err?.message || "Test failed";
        showSettingsToast("Platform Stripe test failed", true);
      }
    });

    superadminPlatformStripeDisconnectBtn?.addEventListener("click", async () => {
      try {
        await apiDelete("/api/admin/developer/platform-billing/stripe");
        if (superadminPlatformStripeActionStatus) superadminPlatformStripeActionStatus.textContent = "Platform Stripe disconnected.";
        showSettingsToast("Platform Stripe disconnected");
        await refreshSuperadminPlatformStripe();
      } catch (err) {
        if (superadminPlatformStripeActionStatus) superadminPlatformStripeActionStatus.textContent = err?.message || "Disconnect failed";
        showSettingsToast("Failed to disconnect Platform Stripe", true);
      }
    });

    superadminTwilioSaveBtn?.addEventListener("click", async () => {
      const accountId = String(superadminTwilioAccountSelect?.value || "").trim();
      if (!accountId) {
        if (superadminTwilioActionStatus) superadminTwilioActionStatus.textContent = "Select a workspace first.";
        return;
      }
      const payload = {
        accountSid: String(superadminTwilioAccountSidInput?.value || "").trim(),
        apiKeySid: String(superadminTwilioApiKeySidInput?.value || "").trim(),
        apiKeySecret: String(superadminTwilioApiKeySecretInput?.value || "").trim(),
        webhookAuthToken: String(superadminTwilioWebhookTokenInput?.value || "").trim(),
        messagingServiceSid: String(superadminTwilioMessagingServiceSidInput?.value || "").trim(),
        phoneNumber: String(superadminTwilioPhoneNumberInput?.value || "").trim(),
        voiceForwardTo: String(superadminTwilioVoiceForwardToInput?.value || "").trim(),
        voiceDialTimeoutSec: Number(superadminTwilioVoiceDialTimeoutInput?.value || 20)
      };
      try {
        await apiPut(`/api/admin/developer/twilio/${encodeURIComponent(accountId)}`, payload);
        if (superadminTwilioActionStatus) superadminTwilioActionStatus.textContent = "Twilio settings saved.";
        showSettingsToast("Twilio settings saved");
        await refreshSuperadminOps();
      } catch (err) {
        if (superadminTwilioActionStatus) superadminTwilioActionStatus.textContent = err?.message || "Save failed";
        showSettingsToast("Failed to save Twilio settings", true);
      }
    });

    superadminTwilioTestBtn?.addEventListener("click", async () => {
      const accountId = String(superadminTwilioAccountSelect?.value || "").trim();
      if (!accountId) return;
      try {
        const result = await apiPost(`/api/admin/developer/twilio/${encodeURIComponent(accountId)}/test`, {});
        if (superadminTwilioActionStatus) superadminTwilioActionStatus.textContent = `Twilio test passed (${result?.status || "ok"})`;
        showSettingsToast("Twilio test passed");
        await refreshSuperadminOps();
      } catch (err) {
        if (superadminTwilioActionStatus) superadminTwilioActionStatus.textContent = err?.message || "Test failed";
        showSettingsToast("Twilio test failed", true);
      }
    });

    superadminTwilioDisconnectBtn?.addEventListener("click", async () => {
      const accountId = String(superadminTwilioAccountSelect?.value || "").trim();
      if (!accountId) return;
      try {
        await apiDelete(`/api/admin/developer/twilio/${encodeURIComponent(accountId)}`);
        if (superadminTwilioActionStatus) superadminTwilioActionStatus.textContent = "Twilio disconnected.";
        showSettingsToast("Twilio disconnected");
        await refreshSuperadminOps();
      } catch (err) {
        if (superadminTwilioActionStatus) superadminTwilioActionStatus.textContent = err?.message || "Disconnect failed";
        showSettingsToast("Failed to disconnect Twilio", true);
      }
    });

    settingsPanelLoaders.developer = async () => {
      await refreshAdminPanel();
      await refreshSuperadminOps();
    };

    await ensureSettingsPanelHydrated(localStorage.getItem(SETTINGS_TAB_KEY) || "workspace");

    const simType = document.getElementById("simType");
    const simTo = document.getElementById("simTo");
    const simToCustomWrap = document.getElementById("simToCustomWrap");
    const simToCustom = document.getElementById("simToCustom");
    const simToCustomError = document.getElementById("simToCustomError");
    const bodyWrap = document.getElementById("bodyWrap");
    const simOut = document.getElementById("simOut");
    const simSendBtn = document.getElementById("simSend");
    populateAllowedAccountOptions();

    function isValidSimulatorCustomTo(value) {
      return /^\+1\d{10}$/.test(String(value || "").trim());
    }

    function getSimulatorToValue() {
      if (!simTo) return "";
      if (simTo.value === "__custom__") return String(simToCustom?.value || "").trim();
      return simTo.value.trim();
    }

    function updateSimulatorToUI() {
      const usingCustom = simTo?.value === "__custom__";
      if (simToCustomWrap) simToCustomWrap.classList.toggle("hidden", !usingCustom);
      if (!usingCustom) {
        if (simToCustomError) simToCustomError.classList.add("hidden");
        if (simSendBtn) simSendBtn.disabled = false;
        return;
      }
      const valid = isValidSimulatorCustomTo(simToCustom?.value || "");
      if (simToCustomError) simToCustomError.classList.toggle("hidden", valid);
      if (simSendBtn) simSendBtn.disabled = !valid;
    }

    function updateBodyVisibility(){
      const isSms = simType.value === "sms";
      bodyWrap.style.display = isSms ? "" : "none";
    }
    updateBodyVisibility();
    simType.addEventListener("change", updateBodyVisibility);

    // keep dropdown synced with ACTIVE_TO (fallback to custom if not in list)
    if (simTo) {
      const activeTo = getActiveTo();
      const hasMatch = Array.from(simTo.options).some((opt) => opt.value === activeTo);
      if (hasMatch) {
        simTo.value = activeTo;
      } else {
        const hasCustom = Array.from(simTo.options).some((opt) => opt.value === "__custom__");
        if (hasCustom) {
          simTo.value = "__custom__";
          if (simToCustom) simToCustom.value = activeTo;
        } else if (simTo.options.length) {
          simTo.value = simTo.options[0].value;
          setActiveTo(simTo.value.trim());
        }
      }
    }
    updateSimulatorToUI();

    simTo?.addEventListener("change", async () => {
      updateSimulatorToUI();
      if (simTo.value === "__custom__") return;
      setActiveTo(simTo.value.trim());
      state.activeTo = getActiveTo();
      state.analyticsSummaryCache = {};
      state.analyticsError = null;
      state.rules = loadLS(rulesKey(getActiveTo()), []);
      syncRulesToBackend();
      syncVipToBackend();
      try {
        await refreshWorkspaceUI();
        await refreshDefaultFlowOptions();
        await loadComplianceUI();
      } catch (err) {
        console.error(err);
      }
      await refreshCalendlyUI();
    });
    simToCustom?.addEventListener("input", updateSimulatorToUI);

    document.getElementById("simClear")?.addEventListener("click", (e) => {
      e.preventDefault();
      e.stopPropagation();
      simOut.textContent = "";
    });

  document.getElementById("simSend")?.addEventListener("click", async (e) => {
    const btn = e.target;
    if (btn.disabled) return; // Already sending
  
    btn.disabled = true;
    btn.textContent = "Sending...";
    simOut.textContent = "Sending...";

    const To = getSimulatorToValue();
    const From = document.getElementById("simFrom").value.trim();
    const Body = document.getElementById("simBody").value.trim();
    if (simTo?.value === "__custom__" && !isValidSimulatorCustomTo(To)) {
      updateSimulatorToUI();
      throw new Error("Invalid custom To number. Expected +1XXXXXXXXXX.");
    }

    try {
      const endpoint = simType.value === "missed-call"
        ? "/webhooks/missed-call"
        : "/webhooks/sms";

      const payload = simType.value === "missed-call"
        ? { To, From }
        : { To, From, Body };

      const res = await fetch(API_BASE + endpoint, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      });

      if (!res.ok) {
        throw new Error(`HTTP ${res.status}: ${await res.text()}`);
      }

      const data = await res.json();
      simOut.textContent = JSON.stringify(data, null, 2);

      // FALLBACK: If backend didn't add id, construct it ourselves
      if (data.conversation && !data.conversation.id) {
        data.conversation.id = `${To}__${From}`;
      }

      // Set state
      if (data.conversation) {
        state.activeConversation = data.conversation;
        state.activeThreadId = data.conversation.id;
        state.activeTo = To;
        setActiveTo(To);
      }

      // Wait for backend save (debounce is 150ms)
      await new Promise(resolve => setTimeout(resolve, 200));

      // Clear and reload
      state.threads = [];
      state.view = "messages";
      await render();

    } catch (err) {
      console.error("Webhook error:", err);
      simOut.textContent = "Error: " + (err?.message || String(err));
    } finally {
      // Re-enable button
      btn.disabled = false;
      btn.textContent = "Send";
    }
  });

  }, 0);

  return wrap;
}



function demoApiGet(path) {
  const p = String(path || "");
  const convoList = getDemoConversations();
  const safeConvos = convoList.map((c) => ({ ...c, leadData: { ...(c.leadData || {}) }, messages: Array.isArray(c.messages) ? c.messages.map((m) => ({ ...m })) : [] }));
  if (p.startsWith("/api/conversations?")) return { conversations: safeConvos };
  if (p.startsWith("/api/conversations/")) {
    const convoId = decodeURIComponent(p.slice("/api/conversations/".length).split("?")[0] || "");
    const convo = safeConvos.find((c) => String(c.id || "") === convoId) || null;
    if (!convo) throw new Error("API 404: conversation not found");
    return { conversation: convo };
  }
  if (p.startsWith("/api/analytics/revenue-overview")) return getDemoOverview();
  if (p.startsWith("/api/analytics/funnel")) return getDemoFunnel();
  if (p.startsWith("/api/analytics/todays-wins")) return getDemoWins();
  if (p.startsWith("/api/analytics/summary")) {
    const raw = p.split("range=")[1] || "7";
    const range = Number(String(raw).split("&")[0] || 7);
    return getDemoSummary(range);
  }
  if (p.startsWith("/api/analytics/customer-recovered")) return { rows: [] };
  if (p.startsWith("/api/account")) return { account: getDemoAccount() };
  if (p.startsWith("/api/contacts")) {
    const contacts = safeConvos.map((c, idx) => ({
      id: `demo_contact_${idx + 1}`,
      phone: String(c.from || ""),
      name: idx === 0 ? "Morgan Client" : (idx === 1 ? "Avery Prospect" : "Jordan Lead"),
      flags: { vip: idx === 0, doNotAutoReply: false, blocked: false }
    }));
    return { contacts };
  }
  if (p.startsWith("/api/auth/me")) {
    return {
      user: { id: "demo_user", email: "demo@relay.local", role: "owner", name: "Demo Operator" },
      accounts: [{ accountId: "acct_demo", to: DEMO_TO, businessName: "Arc Relay Demo Workspace" }],
      csrfToken: "demo_csrf"
    };
  }
  if (p.startsWith("/api/rules")) return { rules: Array.isArray(state.rules) ? state.rules : [] };
  if (p.startsWith("/api/vip")) return { vipList: Array.isArray(state.vip) ? state.vip : [] };
  return {};
}
async function apiGet(path){
  if (IS_DEMO_MODE) {
    return demoApiGet(path);
  }
  const res = await fetch(API_BASE + withTenantQuery(path));
  if (res.status === 401) {
    handleUnauthorizedSession();
    throw new Error("API 401: Unauthorized");
  }
  if(!res.ok){
    const txt = await res.text();
    throw new Error(`API ${res.status}: ${txt}`);
  }
  return res.json();
}

function isApi404Error(err) {
  const msg = String(err?.message || "");
  return /^API 404:/i.test(msg);
}

async function apiGetOptional(path, fallbackValue = {}) {
  try {
    return await apiGet(path);
  } catch (err) {
    if (isApi404Error(err)) return fallbackValue;
    throw err;
  }
}

async function apiGetContacts(to){
  const data = await apiGet(`/api/contacts?to=${encodeURIComponent(to)}`);
  return data?.contacts || [];
}


async function apiUpsertContact(to, contact){
  const data = await apiPost(`/api/contacts`, { to, contact });
  return data.contact;
}


async function loadThreads(options = {}){
  const skipTopbarRefresh = options && options.skipTopbarRefresh === true;
  const force = options && options.force === true;
  const requestedTo = String(getActiveTo() || "").trim();
  const scopeSnapshot = createUiScopeSnapshot({ to: requestedTo });
  if (IS_DEMO_MODE) {
    const demoThreads = getDemoConversations();
    if (isUiScopeCurrent(scopeSnapshot)) {
      state.threads = demoThreads;
      state.threadsLastLoadedAt = Date.now();
      state.recentLeadsCache = {};
      state.recentActivityCache = {};
      if(!state.activeThreadId && state.threads.length) state.activeThreadId = state.threads[0].id;
      else if (state.activeThreadId && !state.threads.some(t => t.id === state.activeThreadId)) state.activeThreadId = state.threads[0]?.id || null;
    }
    if (!skipTopbarRefresh && typeof window.refreshTopbarRecoveryStrip === "function") { window.refreshTopbarRecoveryStrip({ force: true }).catch(() => {}); }
    return demoThreads;
  }
  const skipThreadHydration = options && options.skipThreadHydration === true;
  const to = requestedTo;
  const cacheEntry = state.threadListCacheByTo?.[to] || null;
  if (!force && cacheEntry && (Date.now() - Number(cacheEntry.loadedAt || 0)) < THREADS_CACHE_TTL_MS) {
    if (isUiScopeCurrent(scopeSnapshot)) {
      state.threads = mergeSimulatedConversations(cloneJsonSafe(cacheEntry.data || []), to);
      state.threadsLastLoadedAt = Number(cacheEntry.loadedAt || Date.now());
    }
  } else if (!force && state.threadListPromiseByTo?.[to]) {
    await state.threadListPromiseByTo[to];
  } else {
    const loadPromise = apiGet(`/api/conversations?to=${encodeURIComponent(to)}`)
      .then((data) => {
        const conversations = Array.isArray(data?.conversations) ? data.conversations : [];
        state.threadListCacheByTo[to] = { loadedAt: Date.now(), data: cloneJsonSafe(conversations) };
        if (isUiScopeCurrent(scopeSnapshot)) {
          state.threads = mergeSimulatedConversations(cloneJsonSafe(conversations), to);
          state.threadsLastLoadedAt = Date.now();
          state.recentLeadsCache = {};
          state.recentActivityCache = {};
        }
      })
      .finally(() => {
        delete state.threadListPromiseByTo[to];
      });
    state.threadListPromiseByTo[to] = loadPromise;
    await loadPromise;
  }
  if (isUiScopeCurrent(scopeSnapshot)) {
    state.threads = mergeSimulatedConversations(state.threads, to);
  }

  if (isUiScopeCurrent(scopeSnapshot) && !state.activeThreadId && state.threads.length){
    state.activeThreadId = state.threads[0].id;
  } else if (isUiScopeCurrent(scopeSnapshot) && state.activeThreadId && !state.threads.some(t => t.id === state.activeThreadId)) {
    state.activeThreadId = state.threads[0]?.id || null;
  }

  const scopeKey = getAnalyticsScopeKey();
  const lastHydratedAt = Number(state.analyticsHydratedAtByScope?.[scopeKey] || 0);
  const shouldHydrateThreads = !skipThreadHydration && (force || !lastHydratedAt || (Date.now() - lastHydratedAt) > ANALYTICS_THREAD_HYDRATION_TTL_MS);
  if (shouldHydrateThreads) {
    try {
      await hydrateAnalyticsBookedThreads(String(to || ""), { force });
      if (isUiScopeCurrent(scopeSnapshot)) {
        state.analyticsHydratedAtByScope[scopeKey] = Date.now();
      }
    } catch {}
  }
  if (!isUiScopeCurrent(scopeSnapshot)) {
    return cloneJsonSafe(state.threadListCacheByTo?.[to]?.data || []);
  }
  rebuildRevenueLedgerFromThreads(state.threads);
  if (typeof window.refreshTopbarNotificationsUI === "function") window.refreshTopbarNotificationsUI();
  if (!skipTopbarRefresh && typeof window.refreshTopbarRecoveryStrip === "function") {
    window.refreshTopbarRecoveryStrip({ force }).catch(() => {});
  }
  return state.threads;
}

async function loadConversation(convoId, options = {}){
  if(!convoId) return null;
  const force = options && options.force === true;
  const requestedId = String(convoId || "").trim();
  const requestedTo = String(getActiveTo() || "").trim();
  const cacheKey = getConversationCacheKey(requestedId, requestedTo);
  const scopeSnapshot = createUiScopeSnapshot({ view: "messages", to: requestedTo });
  if (IS_DEMO_MODE) {
    const convo = getDemoConversations().find((c) => String(c.id || "") === String(convoId || ""));
    const nextConversation = convo ? { ...convo, leadData: { ...(convo.leadData || {}) }, messages: Array.isArray(convo.messages) ? convo.messages.map((m) => ({ ...m })) : [] } : null;
    if (isUiScopeCurrent(scopeSnapshot, { activeThreadId: requestedId })) {
      state.activeConversation = nextConversation;
      rebuildRevenueLedgerFromThreads(state.threads);
    }
    return nextConversation;
  }
  const simulatedConversation = findSimulatedConversation(requestedId, requestedTo);
  if (simulatedConversation) {
    state.conversationCacheById[cacheKey] = { loadedAt: Date.now(), data: cloneJsonSafe(simulatedConversation) };
    if (isUiScopeCurrent(scopeSnapshot, { activeThreadId: requestedId })) {
      state.activeConversation = cloneJsonSafe(simulatedConversation);
      rebuildRevenueLedgerFromThreads(state.threads);
    }
    return cloneJsonSafe(simulatedConversation);
  }
  const cacheEntry = state.conversationCacheById?.[cacheKey] || null;
  if (!force && cacheEntry && (Date.now() - Number(cacheEntry.loadedAt || 0)) < CONVERSATION_CACHE_TTL_MS) {
    const cachedConversation = cloneJsonSafe(cacheEntry.data);
    if (isUiScopeCurrent(scopeSnapshot, { activeThreadId: requestedId })) {
      state.activeConversation = cachedConversation;
    }
    if (isUiScopeCurrent(scopeSnapshot, { activeThreadId: requestedId })) {
      rebuildRevenueLedgerFromThreads(state.threads);
    }
    return cachedConversation;
  } else if (!force && state.conversationPromiseById?.[cacheKey]) {
    await state.conversationPromiseById[cacheKey];
  } else {
    const loadPromise = apiGet(`/api/conversations/${encodeURIComponent(convoId)}`)
      .then((data) => {
        const nextConversation = cloneJsonSafe(data.conversation);
        state.conversationCacheById[cacheKey] = { loadedAt: Date.now(), data: nextConversation };
        if (isUiScopeCurrent(scopeSnapshot, { activeThreadId: requestedId })) {
          state.activeConversation = cloneJsonSafe(nextConversation);
        }
      })
      .finally(() => {
        delete state.conversationPromiseById[cacheKey];
      });
    state.conversationPromiseById[cacheKey] = loadPromise;
    await loadPromise;
  }
  const resolvedConversation = cloneJsonSafe(state.conversationCacheById?.[cacheKey]?.data || null);
  if (isUiScopeCurrent(scopeSnapshot, { activeThreadId: requestedId })) {
    rebuildRevenueLedgerFromThreads(state.threads);
    return state.activeConversation;
  }
  return resolvedConversation;
}



/* ==========
  Navigation
========== */
// Navigation click handling is delegated in bindNavDelegated().

/* ==========
  Utils
========== */
function normalizePhone(phone){
  return (phone || "").replace(/[^\d+]/g, "");
}
function escapeHtml(str){
  return String(str ?? "")
    .replaceAll("&","&amp;")
    .replaceAll("<","&lt;")
    .replaceAll(">","&gt;")
    .replaceAll('"',"&quot;")
    .replaceAll("'","&#039;");
}
function escapeAttr(str){
  return escapeHtml(str).replaceAll("\n"," ");
}

let threadContextTarget = null;
let messageContextTarget = null;

function isSuperadminUser() {
  return String(authState?.user?.role || "").trim().toLowerCase() === "superadmin";
}

document.addEventListener(
  "contextmenu",
  (e) => {
    const bubbleEl = e.target.closest("#bubbles .bubble-wrapper[data-msg-index]");
    if (bubbleEl) {
      e.preventDefault();
      e.stopPropagation();
      const convoId = String(state.activeConversation?.id || "").trim();
      const idx = Number(bubbleEl.getAttribute("data-msg-index"));
      if (!convoId || !Number.isFinite(idx)) return;
      const textEl = bubbleEl.querySelector(".bubble-text");
      const text = String(textEl?.textContent || "").trim();
      messageContextTarget = { convoId, idx, text };
      const menu = document.getElementById("msgContextMenu");
      if (!menu) return;
      menu.style.left = `${e.clientX}px`;
      menu.style.top = `${e.clientY}px`;
      menu.classList.remove("hidden");
      document.getElementById("threadContextMenu")?.classList.add("hidden");
      return;
    }

    const threadEl = e.target.closest(".thread");
    if (!threadEl) return;

    // prevent default browser menu
    e.preventDefault();
    e.stopPropagation();

    threadContextTarget = threadEl;

    const menu = document.getElementById("threadContextMenu");
	if (!menu) return; // prevents crash if HTML missing
    const copyThreadDebugBtn = document.getElementById("copyThreadDebugBtn");
    if (copyThreadDebugBtn) {
      copyThreadDebugBtn.classList.toggle("hidden", !isSuperadminUser());
    }
    menu.style.left = e.clientX + "px";
    menu.style.top = e.clientY + "px";
    menu.classList.remove("hidden");

    // Also hide the message menu if open
    document.getElementById("msgContextMenu")?.classList.add("hidden");
  },
  true
);

// click anywhere closes thread menu
document.addEventListener("click", () => {
  document.getElementById("threadContextMenu")?.classList.add("hidden");
  document.getElementById("msgContextMenu")?.classList.add("hidden");
});


document.getElementById("deleteThreadBtn")?.addEventListener("click", async () => {
  if (!threadContextTarget) return;

  const convoId = threadContextTarget.dataset.thread;
  if (!convoId) return;

  //  1) DELETE FROM BACKEND FIRST
  try {
    await apiDelete(`/api/conversations/${encodeURIComponent(convoId)}`);
  } catch (err) {
    console.error("Failed to delete on backend:", err);
    return;
  }

  //  2) THEN DELETE FROM FRONTEND STATE
  state.threads = state.threads.filter(t => t.id !== convoId);

  //  3) CLEAR CHAT IF IT WAS ACTIVE
  if (state.activeThreadId === convoId) {
    state.activeThreadId = null;
    state.activeConversation = null;

    const bubbles = document.getElementById("bubbles");
    const chatHead = document.getElementById("chatHead");
    if (bubbles) bubbles.innerHTML = "";
    if (chatHead) chatHead.innerHTML = "<div class='p'>No conversation selected</div>";
	renderLeadDetails();
  }

  //  4) CLOSE MENU + RE-RENDER
  threadContextTarget = null;
  document.getElementById("threadContextMenu")?.classList.add("hidden");
  await loadThreads();
  renderThreadListFromAPI($("#threadSearch")?.value?.trim() || "");
});

document.getElementById("copyThreadDebugBtn")?.addEventListener("click", async () => {
  if (!isSuperadminUser()) return;
  const target = threadContextTarget;
  if (!target) return;
  const convoId = String(target.dataset?.thread || "").trim();
  if (!convoId) return;
  const summary = (state.threads || []).find((t) => String(t?.id || "").trim() === convoId) || null;
  const active = (String(state.activeConversation?.id || "").trim() === convoId) ? state.activeConversation : null;
  const attrNames = typeof target.getAttributeNames === "function" ? target.getAttributeNames() : [];
  const attrs = {};
  attrNames.forEach((name) => {
    attrs[name] = target.getAttribute(name);
  });
  const payload = {
    copiedAt: new Date().toISOString(),
    role: String(authState?.user?.role || ""),
    conversationId: convoId,
    uiState: {
      activeThreadId: String(state.activeThreadId || ""),
      currentView: String(state.view || "")
    },
    threadElement: {
      id: target.id || "",
      className: String(target.className || ""),
      classList: Array.from(target.classList || []),
      dataset: { ...(target.dataset || {}) },
      attributes: attrs,
      textPreview: String(target.textContent || "").trim().slice(0, 500)
    },
    threadSummary: summary,
    activeConversation: active
  };
  try {
    await navigator.clipboard.writeText(JSON.stringify(payload, null, 2));
  } catch (err) {
    console.error("Copy thread debug failed:", err);
  } finally {
    document.getElementById("threadContextMenu")?.classList.add("hidden");
  }
});

document.getElementById("copyMessageBtn")?.addEventListener("click", async () => {
  const target = messageContextTarget;
  if (!target) return;
  try {
    await navigator.clipboard.writeText(String(target.text || ""));
  } catch (err) {
    console.error("Copy message failed:", err);
  } finally {
    document.getElementById("msgContextMenu")?.classList.add("hidden");
  }
});

document.getElementById("deleteMessageBtn")?.addEventListener("click", async () => {
  const target = messageContextTarget;
  if (!target) return;
  const convoId = String(target.convoId || "").trim();
  const idx = Number(target.idx);
  if (!convoId || !Number.isFinite(idx) || idx < 0) return;
  try {
    const res = await apiDelete(`/api/conversations/${encodeURIComponent(convoId)}/messages/${encodeURIComponent(String(idx))}`);
    if (res?.conversation) {
      state.activeConversation = res.conversation;
      state.activeThreadId = res.conversation.id || convoId;
      await loadThreads();
      await renderThreadListFromAPI($("#threadSearch")?.value?.trim() || "");
      renderChatFromAPI();
    }
  } catch (err) {
    console.error("Delete message failed:", err);
  } finally {
    document.getElementById("msgContextMenu")?.classList.add("hidden");
  }
});


async function apiPost(path, body){
  if (IS_DEMO_MODE) throw new Error("Demo mode is read-only. Start with Arc Relay to make changes.");
  const res = await fetch(API_BASE + withTenantQuery(path), {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  if (res.status === 401) {
    handleUnauthorizedSession();
    throw new Error("API 401: Unauthorized");
  }
  if(!res.ok){
    const txt = await res.text();
    throw new Error(`API ${res.status}: ${txt}`);
  }
  const data = await res.json();
  invalidateWorkspaceCacheForPath(path);
  return data;
}

async function apiPut(path, body){
  if (IS_DEMO_MODE) throw new Error("Demo mode is read-only. Start with Arc Relay to make changes.");
  const res = await fetch(API_BASE + withTenantQuery(path), {
    method: "PUT",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  if (res.status === 401) {
    handleUnauthorizedSession();
    throw new Error("API 401: Unauthorized");
  }
  if(!res.ok){
    const txt = await res.text();
    throw new Error(`API ${res.status}: ${txt}`);
  }
  const data = await res.json();
  invalidateWorkspaceCacheForPath(path);
  return data;
}

async function apiPatch(path, body){
  if (IS_DEMO_MODE) throw new Error("Demo mode is read-only. Start with Arc Relay to make changes.");
  const res = await fetch(API_BASE + withTenantQuery(path), {
    method: "PATCH",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  if (res.status === 401) {
    handleUnauthorizedSession();
    throw new Error("API 401: Unauthorized");
  }
  if(!res.ok){
    const txt = await res.text();
    throw new Error(`API ${res.status}: ${txt}`);
  }
  const data = await res.json();
  invalidateWorkspaceCacheForPath(path);
  return data;
}

async function apiDelete(path){
  if (IS_DEMO_MODE) throw new Error("Demo mode is read-only. Start with Arc Relay to make changes.");
  const res = await fetch(API_BASE + withTenantQuery(path), {
    method: "DELETE",
  });
  if (res.status === 401) {
    handleUnauthorizedSession();
    throw new Error("API 401: Unauthorized");
  }
  if(!res.ok){
    const txt = await res.text();
    throw new Error(`API ${res.status}: ${txt}`);
  }
  const data = await res.json();
  invalidateWorkspaceCacheForPath(path);
  return data;
}

async function loadAccountSettings(to, options = {}){
  if (!IS_DEMO_MODE && !authState.user) return null;
  const force = options && options.force === true;
  const cacheEntry = state.accountSettingsCacheByTo?.[to] || null;
  if (!force && cacheEntry && (Date.now() - Number(cacheEntry.loadedAt || 0)) < ACCOUNT_SETTINGS_CACHE_TTL_MS) {
    return cloneJsonSafe(cacheEntry.data);
  }
  if (!force && state.accountSettingsPromiseByTo?.[to]) {
    return state.accountSettingsPromiseByTo[to];
  }
  const loadPromise = apiGet(`/api/account?to=${encodeURIComponent(to)}`)
    .then((data) => {
      const account = data.account || null;
      state.accountSettingsCacheByTo[to] = { loadedAt: Date.now(), data: cloneJsonSafe(account) };
      return cloneJsonSafe(account);
    })
    .finally(() => {
      delete state.accountSettingsPromiseByTo[to];
    });
  state.accountSettingsPromiseByTo[to] = loadPromise;
  return loadPromise;
}

function invalidateWorkspaceCacheForPath(path) {
  const rawPath = String(path || "");
  const activeTo = String(getActiveTo() || "").trim();
  if (/^\/api\/contacts\b/i.test(rawPath) && activeTo) {
    delete state.contactsCacheByTo[activeTo];
    delete state.contactsPromiseByTo[activeTo];
  }
  if (/^\/api\/account\b/i.test(rawPath) && activeTo) {
    delete state.accountSettingsCacheByTo[activeTo];
    delete state.accountSettingsPromiseByTo[activeTo];
  }
  if (/^\/api\/conversations\b/i.test(rawPath) && activeTo) {
    delete state.threadListCacheByTo[activeTo];
    delete state.threadListPromiseByTo[activeTo];
    Object.keys(state.conversationCacheById || {}).forEach((key) => {
      if (String(key || "").startsWith(`${activeTo}::`)) delete state.conversationCacheById[key];
    });
    Object.keys(state.conversationPromiseById || {}).forEach((key) => {
      if (String(key || "").startsWith(`${activeTo}::`)) delete state.conversationPromiseById[key];
    });
    state.recentLeadsCache = {};
    state.recentActivityCache = {};
  }
}
function withTenantQuery(path) {
  const p = String(path || "");
  if (!p.startsWith("/api/")) return p;
  if (/[?&](to|accountId)=/.test(p)) return p;
  const sep = p.includes("?") ? "&" : "?";
  const active = resolveActiveWorkspace();
  const activeTo = String(active?.to || "").trim();
  const accountId = String(active?.accountId || "").trim();
  if (activeTo) return `${p}${sep}to=${encodeURIComponent(activeTo)}`;
  if (accountId) return `${p}${sep}accountId=${encodeURIComponent(accountId)}`;
  return p;
}


window.addEventListener("DOMContentLoaded", () => {
  bindNavDelegated();
  positionNavGhostHighlight({ instant: true });
  window.addEventListener("resize", () => positionNavGhostHighlight({ instant: true }));
  const topbarHomeBtn = document.getElementById("topbarHomeBtn");
  const topbarLogoImg = document.querySelector(".topbar-logo");
  const defaultTopbarLogoSrc = String(topbarLogoImg?.getAttribute("src") || "/logos/main-topbar.png").trim() || "/logos/main-topbar.png";
  const topbarTenantPill = document.getElementById("topbarTenantPill");
  const topbarRecoveryStrip = document.getElementById("topbarRecoveryStrip");
  const topbarRecoveredMonth = document.getElementById("topbarRecoveredMonth");
  const topbarMissedConverted = document.getElementById("topbarMissedConverted");
  const topbarBookedAppointments = document.getElementById("topbarBookedAppointments");
  const topbarStatusPill = document.getElementById("topbarStatusPill");
  const topbarBellBtn = document.getElementById("topbarBellBtn");
  const topbarBellBadge = document.getElementById("topbarBellBadge");
  const topbarNotifMenu = document.getElementById("topbarNotifMenu");
  const topbarNotifList = document.getElementById("topbarNotifList");
  const topbarNotifMarkReadBtn = document.getElementById("topbarNotifMarkReadBtn");
  const topbarBillDueBtn = document.getElementById("topbarBillDueBtn");
  const topbarUserMenu = document.getElementById("topbarUserMenu");
  const topbarUserAvatar = document.getElementById("topbarUserAvatar");
  const topbarUserLabel = document.getElementById("topbarUserLabel");
  const topbarUserCardName = document.getElementById("topbarUserCardName");
  const topbarUserCardEmail = document.getElementById("topbarUserCardEmail");
  const topbarUserCardRole = document.getElementById("topbarUserCardRole");
  const topbarProfileBtn = document.getElementById("topbarProfileBtn");
  const topbarQuickActionsBtn = document.getElementById("topbarQuickActionsBtn");
  const topbarThemeToggleBtn = document.getElementById("topbarThemeToggleBtn");
  const topbarLogoutBtn = document.getElementById("topbarLogoutBtn");
  const navBillDueBtn = document.getElementById("navBillDueBtn");
  const navUpgradePlanBtn = document.getElementById("navUpgradePlanBtn");
  const arcDock = document.getElementById("arcDock");
  const arcDockHandle = document.getElementById("arcDockHandle");
  const arcDockUpgradeBtn = document.getElementById("arcDockUpgradeBtn");
  const SETTINGS_TAB_KEY = "mc_settings_tab_v1";
  const topbarLogoCacheByScope = {};
  const topbarAdminAccessCacheByScope = {};


  function refreshTopbarTenantPill() {
    if (!topbarTenantPill) return;
    const to = getActiveTo();
    topbarTenantPill.textContent = `Workspace ${to}`;
  }

  function setTopbarRecoveryLoading(isLoading) {
    if (!topbarRecoveryStrip) return;
    topbarRecoveryStrip.classList.toggle("is-loading", isLoading === true);
    topbarRecoveryStrip.setAttribute("aria-busy", isLoading === true ? "true" : "false");
  }

  function setTopbarRecoveryLine(strongEl, valueText, labelText) {
    if (!strongEl || !strongEl.parentElement) return;
    strongEl.parentElement.innerHTML = `<strong id="${strongEl.id}">${escapeHtml(String(valueText || "0"))}</strong> ${escapeHtml(String(labelText || ""))}`;
  }

  function isZeroLikeTopbarMetrics(metrics) {
    if (!metrics || typeof metrics !== "object") return true;
    const line1 = String(metrics.line1Value || "").trim();
    const line2 = Number(String(metrics.line2Value || "0").replace(/[^\d.-]/g, "")) || 0;
    const line3 = Number(String(metrics.line3Value || "0").replace(/[^\d.-]/g, "")) || 0;
    const line1Cents = parseMoneyLabelToCents(line1);
    return line1Cents <= 0 && line2 <= 0 && line3 <= 0;
  }

  async function refreshTopbarRecoveryStrip({ force = false } = {}) {
    if (!topbarRecoveredMonth || !topbarMissedConverted || !topbarBookedAppointments) return;
    const scopeKey = getAnalyticsScopeKey();
    const cached = state.homeOverviewCache?.[scopeKey] || null;
    const cachedMetrics = state.topbarMetricsCacheByScope?.[scopeKey] || null;
    const cachedContactPhones = safeArray(state.topbarContactsCacheByScope?.[scopeKey]);
    const cachedContactSet = new Set(cachedContactPhones.map((p) => normalizePhone(String(p || ""))).filter(Boolean));
    const sess = getSession();
    if (!sess) {
      if (cachedMetrics) {
        setTopbarRecoveryLine(topbarRecoveredMonth, cachedMetrics.line1Value, cachedMetrics.line1Label);
        setTopbarRecoveryLine(topbarMissedConverted, cachedMetrics.line2Value, cachedMetrics.line2Label);
        setTopbarRecoveryLine(topbarBookedAppointments, cachedMetrics.line3Value, cachedMetrics.line3Label);
      }
      return;
    }
    if (cachedMetrics && !force) {
      setTopbarRecoveryLine(topbarRecoveredMonth, cachedMetrics.line1Value, cachedMetrics.line1Label);
      setTopbarRecoveryLine(topbarMissedConverted, cachedMetrics.line2Value, cachedMetrics.line2Label);
      setTopbarRecoveryLine(topbarBookedAppointments, cachedMetrics.line3Value, cachedMetrics.line3Label);
    }
    if (shouldSkipDashboardBootForView()) {
      if (cachedMetrics) {
        setTopbarRecoveryLine(topbarRecoveredMonth, cachedMetrics.line1Value, cachedMetrics.line1Label);
        setTopbarRecoveryLine(topbarMissedConverted, cachedMetrics.line2Value, cachedMetrics.line2Label);
        setTopbarRecoveryLine(topbarBookedAppointments, cachedMetrics.line3Value, cachedMetrics.line3Label);
      }
      return;
    }
    if (cached && cachedContactSet.size > 0 && !force && Array.isArray(state.threads) && state.threads.length) {
      const m = deriveTopbarMetrics(cached, { savedContactSet: cachedContactSet });
      state.topbarMetricsCacheByScope[scopeKey] = m;
      setTopbarRecoveryLine(topbarRecoveredMonth, m.line1Value, m.line1Label);
      setTopbarRecoveryLine(topbarMissedConverted, m.line2Value, m.line2Label);
      setTopbarRecoveryLine(topbarBookedAppointments, m.line3Value, m.line3Label);
      return;
    }
    setTopbarRecoveryLoading(true);
    try {
      if (force || !Array.isArray(state.threads) || !state.threads.length) {
        try { await loadThreads({ skipTopbarRefresh: true, skipThreadHydration: true }); } catch {}
      }
      try { await hydrateAnalyticsBookedThreads(String(getActiveTo() || "")); } catch {}
      const overview = cached || await apiGet("/api/analytics/revenue-overview");
      let contactPhones = cachedContactPhones;
      if (force || !contactPhones.length) {
        try {
          const contacts = await apiGetContacts(getActiveTo());
          contactPhones = safeArray(contacts).map((c) => normalizePhone(String(c?.phone || ""))).filter(Boolean);
          state.topbarContactsCacheByScope[scopeKey] = contactPhones;
        } catch {}
      }
      const savedContactSet = new Set(safeArray(contactPhones).map((p) => normalizePhone(String(p || ""))).filter(Boolean));
      state.homeOverviewCache[scopeKey] = overview || {};
      const m = deriveTopbarMetrics(overview || {}, { savedContactSet });
      const keepCached = !!cachedMetrics && isZeroLikeTopbarMetrics(m) && !isZeroLikeTopbarMetrics(cachedMetrics);
      const finalMetrics = keepCached ? cachedMetrics : m;
      state.topbarMetricsCacheByScope[scopeKey] = finalMetrics;
      setTopbarRecoveryLine(topbarRecoveredMonth, finalMetrics.line1Value, finalMetrics.line1Label);
      setTopbarRecoveryLine(topbarMissedConverted, finalMetrics.line2Value, finalMetrics.line2Label);
      setTopbarRecoveryLine(topbarBookedAppointments, finalMetrics.line3Value, finalMetrics.line3Label);
    } catch {
      if (cached) {
        const m = deriveTopbarMetrics(cached, { savedContactSet: cachedContactSet });
        const keepCached = !!cachedMetrics && isZeroLikeTopbarMetrics(m) && !isZeroLikeTopbarMetrics(cachedMetrics);
        const finalMetrics = keepCached ? cachedMetrics : m;
        state.topbarMetricsCacheByScope[scopeKey] = finalMetrics;
        setTopbarRecoveryLine(topbarRecoveredMonth, finalMetrics.line1Value, finalMetrics.line1Label);
        setTopbarRecoveryLine(topbarMissedConverted, finalMetrics.line2Value, finalMetrics.line2Label);
        setTopbarRecoveryLine(topbarBookedAppointments, finalMetrics.line3Value, finalMetrics.line3Label);
      } else if (cachedMetrics) {
        setTopbarRecoveryLine(topbarRecoveredMonth, cachedMetrics.line1Value, cachedMetrics.line1Label);
        setTopbarRecoveryLine(topbarMissedConverted, cachedMetrics.line2Value, cachedMetrics.line2Label);
        setTopbarRecoveryLine(topbarBookedAppointments, cachedMetrics.line3Value, cachedMetrics.line3Label);
      }
    } finally {
      setTopbarRecoveryLoading(false);
    }
  }

  function deriveUserName(sess){
    const name = String(sess?.name || "").trim();
    if (name) return name;
    const email = String(sess?.email || "").trim();
    if (!email) return "Account";
    const local = email.split("@")[0] || "Account";
    return local
      .replace(/[._-]+/g, " ")
      .split(" ")
      .filter(Boolean)
      .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
      .join(" ");
  }

  function deriveInitials(name, email){
    const parts = String(name || "").trim().split(/\s+/).filter(Boolean);
    if (parts.length >= 2) return `${parts[0].charAt(0)}${parts[1].charAt(0)}`.toUpperCase();
    if (parts.length === 1 && parts[0].length >= 2) return parts[0].slice(0, 2).toUpperCase();
    const local = String(email || "").split("@")[0] || "AR";
    return local.replace(/[^a-z0-9]/gi, "").slice(0, 2).toUpperCase() || "AR";
  }

  function renderTopbarSettingsAction(configured) {
    if (!topbarQuickActionsBtn) return;
    const isLocked = configured === true;
    topbarQuickActionsBtn.classList.toggle("topbar-user-action-locked", isLocked);
    if (isLocked) {
      topbarQuickActionsBtn.innerHTML = `<span class="topbar-user-action-main">Settings</span><span class="topbar-user-action-meta"><span aria-hidden="true">&#128274;</span><span>Admin passcode required</span></span>`;
      return;
    }
    topbarQuickActionsBtn.textContent = "Settings";
  }

  async function refreshTopbarSettingsAction({ force = false } = {}) {
    if (!topbarQuickActionsBtn) return;
    const active = resolveActiveWorkspace();
    const scopeKey = String(active?.accountId || active?.to || getActiveTo() || "__default").trim() || "__default";
    if (!force && Object.prototype.hasOwnProperty.call(topbarAdminAccessCacheByScope, scopeKey)) {
      renderTopbarSettingsAction(topbarAdminAccessCacheByScope[scopeKey] === true);
      return;
    }
    try {
      const data = await apiGet("/api/account/admin-access");
      const configured = data?.configured === true;
      topbarAdminAccessCacheByScope[scopeKey] = configured;
      renderTopbarSettingsAction(configured);
    } catch {
      topbarAdminAccessCacheByScope[scopeKey] = false;
      renderTopbarSettingsAction(false);
    }
  }

  function refreshTopbarUserMenu() {
    const sess = getSession();
    const email = String(sess?.email || "admin@relay.local").trim();
    const roleRaw = String(sess?.role || "user").trim();
    const role = roleRaw ? roleRaw.charAt(0).toUpperCase() + roleRaw.slice(1) : "User";
    const name = deriveUserName(sess);
    const initials = deriveInitials(name, email);
    if (topbarUserAvatar) topbarUserAvatar.textContent = initials;
    if (topbarUserLabel) topbarUserLabel.textContent = name || "Account";
    if (topbarUserCardName) topbarUserCardName.textContent = name || "Account";
    if (topbarUserCardEmail) topbarUserCardEmail.textContent = email || "admin@relay.local";
    if (topbarUserCardRole) topbarUserCardRole.textContent = `Role: ${role}`;
    if (topbarThemeToggleBtn) {
      const currentTheme = String(document.documentElement.getAttribute("data-theme") || localStorage.getItem(THEME_KEY) || "dark").toLowerCase();
      topbarThemeToggleBtn.textContent = currentTheme === "light" ? "Switch to Dark Mode" : "Switch to Light Mode";
    }
    void refreshTopbarSettingsAction();
  }

  function resolveTopbarLogoSrc(raw) {
    const v = String(raw || "").trim();
    if (!v) return "";
    if (v === "/logos/main.png") return "/logos/main-topbar.png";
    if (/^https?:\/\//i.test(v) || v.startsWith("data:")) return v;
    if (v.startsWith("/api/")) return `${API_BASE}${v}`;
    return v;
  }

  function setTopbarLogoSrc(nextSrc) {
    if (!topbarLogoImg) return;
    const desired = String(nextSrc || defaultTopbarLogoSrc).trim() || defaultTopbarLogoSrc;
    const current = String(topbarLogoImg.getAttribute("src") || topbarLogoImg.src || "").trim();
    if (current === desired || current.endsWith(desired)) return;
    topbarLogoImg.src = desired;
  }

  async function refreshTopbarBrandLogo({ force = false } = {}) {
    if (!topbarLogoImg) return;
    const active = resolveActiveWorkspace();
    const scopeKey = String(active?.accountId || active?.to || getActiveTo() || "__default").trim() || "__default";
    if (!force && Object.prototype.hasOwnProperty.call(topbarLogoCacheByScope, scopeKey)) {
      const cached = String(topbarLogoCacheByScope[scopeKey] || "").trim();
      setTopbarLogoSrc(cached || defaultTopbarLogoSrc);
      return;
    }
    try {
      const to = String(active?.to || getActiveTo() || "").trim();
      if (!to) {
        setTopbarLogoSrc(defaultTopbarLogoSrc);
        return;
      }
      const acct = await loadAccountSettings(to);
      const logoUrl = String(acct?.workspace?.identity?.logoUrl || "").trim();
      const resolved = resolveTopbarLogoSrc(logoUrl);
      topbarLogoCacheByScope[scopeKey] = resolved || "";
      setTopbarLogoSrc(resolved || defaultTopbarLogoSrc);
    } catch {
      setTopbarLogoSrc(defaultTopbarLogoSrc);
    }
  }

  function closeNotificationsMenu() {
    if (!topbarNotifMenu) return;
    topbarNotifMenu.classList.add("hidden");
    topbarNotifMenu.setAttribute("aria-hidden", "true");
  }

  function openNotificationsMenu() {
    if (!topbarNotifMenu) return;
    topbarNotifMenu.classList.remove("hidden");
    topbarNotifMenu.setAttribute("aria-hidden", "false");
  }

  function renderNotifications({ markReadOnOpen = false } = {}) {
    if (!topbarNotifList) return;
    const scopeKey = getNotificationScopeKey();
    const items = getNotificationItems(24);
    const readTs = getNotificationReadTs(scopeKey);
    const unreadCount = items.filter((n) => Number(n.when || 0) > readTs).length;

    if (topbarBellBadge) {
      topbarBellBadge.textContent = String(Math.min(99, unreadCount));
      topbarBellBadge.classList.toggle("hidden", unreadCount <= 0);
    }

    if (!items.length) {
      topbarNotifList.innerHTML = `<div class="topbar-notif-empty">You're all caught up.</div>`;
      return;
    }

    topbarNotifList.innerHTML = items.map((item) => {
      const when = Number(item.when || 0);
      const unread = when > readTs;
      return `
        <button type="button"
          class="topbar-notif-item ${unread ? "is-unread" : ""}"
          data-notif-action="${escapeAttr(item.action || "")}"
          data-notif-convo-id="${escapeAttr(item.convoId || "")}"
          data-notif-when="${escapeAttr(String(when))}">
          <div class="topbar-notif-row">
            <span class="topbar-notif-title">${escapeHtml(item.title || "Update")}</span>
            <span class="topbar-notif-time">${escapeHtml(formatTimeAgo(when))}</span>
          </div>
          <div class="topbar-notif-detail">${escapeHtml(item.detail || "")}</div>
          <div class="topbar-notif-meta">${escapeHtml(String(item.severity || "normal").toUpperCase())}</div>
        </button>
      `;
    }).join("");

    if (markReadOnOpen) {
      const newestTs = Math.max(0, ...items.map((n) => Number(n.when || 0)));
      if (newestTs > readTs) {
        setNotificationReadTs(scopeKey, newestTs);
        if (topbarBellBadge) topbarBellBadge.classList.add("hidden");
      }
    }
  }

  function bindArcDock() {
    if (!arcDock || arcDock.dataset.bound === "true") return;
    arcDock.dataset.bound = "true";
    const supportsHover = !!window.matchMedia && window.matchMedia("(hover: hover) and (pointer: fine)").matches;
    if (supportsHover) {
      arcDock.classList.remove("is-open");
      arcDock.addEventListener("mouseenter", () => {
        if (arcDock.classList.contains("force-closed")) return;
        arcDock.classList.add("is-open");
      });
      arcDock.addEventListener("mouseleave", () => {
        arcDock.classList.remove("is-open");
        arcDock.classList.remove("force-closed");
      });
      return;
    }
    arcDock.classList.remove("is-open");
    arcDockHandle?.addEventListener("click", () => {
      arcDock.classList.remove("force-closed");
      arcDock.classList.toggle("is-open");
    });

    let startY = null;
    arcDock.addEventListener("touchstart", (e) => {
      const t = e.changedTouches && e.changedTouches[0];
      startY = t ? t.clientY : null;
    }, { passive: true });
    arcDock.addEventListener("touchend", (e) => {
      if (startY == null) return;
      const t = e.changedTouches && e.changedTouches[0];
      const endY = t ? t.clientY : startY;
      const delta = startY - endY;
      if (delta > 18) arcDock.classList.add("is-open");
      if (delta < -18) arcDock.classList.remove("is-open");
      startY = null;
    }, { passive: true });

    document.addEventListener("click", (e) => {
      if (!arcDock.classList.contains("is-open")) return;
      if (arcDock.contains(e.target)) return;
      arcDock.classList.remove("is-open");
    });
  }




  async function refreshTopbarHealth() {
    if (!topbarStatusPill) return;
    const role = String(authState?.user?.role || "").toLowerCase();
    const canViewStatus = role === "owner" || role === "admin" || role === "superadmin";
    topbarStatusPill.classList.toggle("hidden", !canViewStatus);
    if (!canViewStatus) return;
    topbarStatusPill.textContent = "Demo";
    topbarStatusPill.className = "topbar-status-pill";
    try {
      const res = await fetch(`${API_BASE}/health`, { method: "GET" });
      if (res.ok) {
        topbarStatusPill.textContent = "Connected";
        topbarStatusPill.className = "topbar-status-pill is-connected";
      } else {
        topbarStatusPill.textContent = "Degraded";
        topbarStatusPill.className = "topbar-status-pill is-degraded";
      }
    } catch {
      topbarStatusPill.textContent = "Demo";
      topbarStatusPill.className = "topbar-status-pill";
    }
  }

  function openUpgradePlanModalFromNav() {
    const detachedPlanModal = document.body.querySelector('#billingPlanModal[data-detached="true"]');
    if (detachedPlanModal) {
      detachedPlanModal.classList.remove("hidden");
      detachedPlanModal.setAttribute("aria-hidden", "false");
      return;
    }

    const previousView = (typeof state === "object" && state.view) ? state.view : "home";
    if (typeof state === "object") state.view = "settings";
    if (typeof render === "function") render();

    let attempts = 0;
    const tryBootstrapAndOpen = () => {
      attempts += 1;
      const planModal = document.getElementById("billingPlanModal");
      const planGrid = document.getElementById("billingPlanGrid");
      const isReady = Boolean(planModal && planGrid && planGrid.children && planGrid.children.length > 0);
      if (!isReady) {
        if (attempts < 80) {
          setTimeout(tryBootstrapAndOpen, 40);
          return;
        }
        if (typeof state === "object") state.view = previousView;
        if (typeof render === "function") render();
        return;
      }

      planModal.classList.remove("hidden");
      planModal.setAttribute("aria-hidden", "false");

      ["billingPlanModal", "billingInfoModal", "billingConfirmModal"].forEach((id) => {
        const el = document.getElementById(id);
        if (!el) return;
        if (el.parentElement !== document.body) {
          el.setAttribute("data-detached", "true");
          document.body.appendChild(el);
        } else {
          el.setAttribute("data-detached", "true");
        }
      });

      if (typeof state === "object") state.view = previousView;
      if (typeof render === "function") render();
    };

    setTimeout(tryBootstrapAndOpen, 0);
  }

  async function openBillDueCheckout() {
    try {
      const returnUrl = getBillingReturnUrl();
      const result = await apiPost("/api/billing/checkout", {
        returnUrl,
        planKey: normalizePlanKey(navBillDueState.planKey || "pro"),
        cadence: "monthly"
      });
      const url = String(result?.url || "").trim();
      if (url) {
        window.open(url, "_blank", "noopener,noreferrer");
        return;
      }
    } catch {}
    localStorage.setItem(SETTINGS_TAB_KEY, "billing");
    if (typeof state === "object") state.view = "settings";
    if (typeof render === "function") render();
  }

  if (navUpgradePlanBtn) {
    navUpgradePlanBtn.addEventListener("click", (e) => {
      e.preventDefault();
      e.stopPropagation();
      if (typeof state === "object") state.view = "settings";
      if (typeof render === "function") render();
      openUpgradePlanModalFromNav();
    });
  }
  arcDockUpgradeBtn?.addEventListener("click", (e) => {
    e.preventDefault();
    e.stopPropagation();
    if (typeof state === "object") state.view = "settings";
    if (typeof render === "function") render();
    openUpgradePlanModalFromNav();
  });

  if (navBillDueBtn) {
    navBillDueBtn.addEventListener("click", async (e) => {
      e.preventDefault();
      e.stopPropagation();
      await openBillDueCheckout();
    });
  }
  topbarBillDueBtn?.addEventListener("click", async (e) => {
    e.preventDefault();
    e.stopPropagation();
    await openBillDueCheckout();
  });

  syncNavBillDueButton();
  bindArcDock();


  document.addEventListener("keydown", (e) => {
    
    if (e.key === "Escape") {
      closeNotificationsMenu();
      if (topbarUserMenu?.open) topbarUserMenu.open = false;
    }
  });

  topbarBellBtn?.addEventListener("click", (e) => {
    e.preventDefault();
    e.stopPropagation();
    if (!topbarNotifMenu) return;
    const opening = topbarNotifMenu.classList.contains("hidden");
    if (opening) {
      renderNotifications();
      openNotificationsMenu();
      return;
    }
    closeNotificationsMenu();
  });
  topbarNotifMarkReadBtn?.addEventListener("click", () => {
    const scopeKey = getNotificationScopeKey();
    setNotificationReadTs(scopeKey, Date.now());
    renderNotifications();
  });
  topbarNotifList?.addEventListener("click", async (e) => {
    const row = e.target.closest("[data-notif-action]");
    if (!row) return;
    const action = String(row.getAttribute("data-notif-action") || "");
    const convoId = String(row.getAttribute("data-notif-convo-id") || "");
    const notifWhen = Number(row.getAttribute("data-notif-when") || 0);
    if (notifWhen > 0) {
      const scopeKey = getNotificationScopeKey();
      const currentRead = getNotificationReadTs(scopeKey);
      if (notifWhen > currentRead) setNotificationReadTs(scopeKey, notifWhen);
    }
    closeNotificationsMenu();
    if (action === "open-convo" && convoId) {
      state.activeThreadId = convoId;
      state.view = "messages";
      await render();
      return;
    }
    if (action === "open-billing") {
      await openBillDueCheckout();
      return;
    }
    if (action === "open-analytics") {
      state.view = "analytics";
      await render();
      return;
    }
  });
  document.addEventListener("click", (e) => {
    if (!topbarNotifMenu || topbarNotifMenu.classList.contains("hidden")) return;
    if (topbarNotifMenu.contains(e.target)) return;
    if (topbarBellBtn && topbarBellBtn.contains(e.target)) return;
    closeNotificationsMenu();
  });
  document.addEventListener("click", (e) => {
    if (!topbarUserMenu?.open) return;
    if (topbarUserMenu.contains(e.target)) return;
    topbarUserMenu.open = false;
  });
  topbarLogoutBtn?.addEventListener("click", async () => {
    await logoutAndShowLogin();
    if (topbarUserMenu?.open) topbarUserMenu.open = false;
  });
  topbarProfileBtn?.addEventListener("click", () => {
    localStorage.setItem(SETTINGS_TAB_KEY, "profile");
    if (typeof state === "object") state.view = "settings";
    if (typeof render === "function") render();
    if (topbarUserMenu?.open) topbarUserMenu.open = false;
  });
  topbarQuickActionsBtn?.addEventListener("click", () => {
    localStorage.setItem(SETTINGS_TAB_KEY, "workspace");
    if (typeof state === "object") state.view = "settings";
    if (typeof render === "function") render();
    if (topbarUserMenu?.open) topbarUserMenu.open = false;
  });
  topbarThemeToggleBtn?.addEventListener("click", () => {
    const currentTheme = String(document.documentElement.getAttribute("data-theme") || localStorage.getItem(THEME_KEY) || "dark").toLowerCase();
    applyTheme(currentTheme === "light" ? "dark" : "light");
    refreshTopbarUserMenu();
    if (topbarUserMenu?.open) topbarUserMenu.open = false;
  });
  window.addEventListener("relay:active-to-changed", () => {
    bumpAccountScopeVersion();
    state.activeTo = getActiveTo();
    resetWorkspaceScopedTransientState();
    closeTransientOverlays();
    state.analyticsSummaryCache = {};
    state.analyticsError = null;
    state.homeOverviewCache = {};
    state.homeOverviewError = null;
    state.homeFunnelCache = {};
    state.homeFunnelError = null;
    state.homeWinsCache = {};
    state.homeWinsError = null;
    state.outcomePacksCache = null;
    state.outcomePackError = null;
    state.revenueAccountSettings = null;
    state.revenueAccountError = null;
    refreshTopbarTenantPill();
    refreshTopbarBrandLogo({ force: true });
    if (!shouldSkipDashboardBootForView()) {
      refreshTopbarRecoveryStrip({ force: true });
    }
    refreshNavBillDueState({ force: true });
    renderNotifications();
    refreshTopbarUserMenu();
    void refreshTopbarSettingsAction({ force: true });
    void refreshOnboardingGate({ force: true }).then(() => {
      if (state.onboardingRequired && state.view !== "onboarding") {
        state.view = "onboarding";
      }
      render();
    });
  });
  window.addEventListener("relay:auth-role-changed", () => {
    refreshTopbarHealth();
    refreshTopbarBrandLogo({ force: true });
    if (!shouldSkipDashboardBootForView()) {
      refreshTopbarRecoveryStrip({ force: true });
    }
    renderNotifications();
    refreshTopbarUserMenu();
    void refreshTopbarSettingsAction({ force: true });
  });
  window.refreshTopbarNotificationsUI = () => {
    renderNotifications();
  };
  window.refreshTopbarRecoveryStrip = refreshTopbarRecoveryStrip;
  window.addEventListener("popstate", () => {
    const route = parseRoutePath(window.location.pathname);
    if (!route?.view) return;
    if (route.view === "settings" && route.panel) {
      localStorage.setItem(SETTINGS_TAB_ROUTE_KEY, route.panel);
    }
    window.__suppressRouteSyncOnce = true;
    state.view = route.view;
    render();
  });

  if (topbarHomeBtn) {
    topbarHomeBtn.addEventListener("click", () => {
      const homeNavBtn = document.querySelector('.nav-btn[data-view="home"]');
      if (homeNavBtn) {
        homeNavBtn.click();
        return;
      }
      if (typeof state === "object") state.view = "home";
      if (typeof render === "function") render();
    });
  }

  // Make layout correct on refresh
  if (typeof state === "object" && state.view) {
    document.body.classList.toggle("messages-mode", state.view === "messages");
    document.body.classList.toggle("schedule-mode", state.view === "schedule" || state.view === "schedule-booking");
  }
  refreshTopbarTenantPill();
  refreshTopbarBrandLogo({ force: true });
  if (!shouldSkipDashboardBootForView()) {
    refreshTopbarRecoveryStrip({ force: true });
  }
  refreshTopbarUserMenu();
  renderNotifications();
  refreshTopbarHealth();
});


function initDemoReadOnlyTour() {
  if (!IS_DEMO_MODE || window.__demoTourInitialized) return;
  window.__demoTourInitialized = true;

  const style = document.createElement("style");
  style.textContent = `
    .demo-ribbon { position: fixed; right: 18px; bottom: 18px; z-index: 120; display: flex; gap: 8px; align-items: center; padding: 10px 12px; border-radius: 999px; border: 1px solid rgba(255,255,255,0.2); background: rgba(8,14,28,0.88); backdrop-filter: blur(10px); color: #dce8ff; font-size: 12px; }
    .demo-ribbon b { color: #fff; }
    .demo-ribbon a, .demo-ribbon button { border: 1px solid rgba(255,255,255,0.2); border-radius: 999px; background: rgba(255,255,255,0.08); color: #fff; font-size: 12px; padding: 6px 10px; text-decoration: none; cursor: pointer; }
    .demo-tour-overlay { position: fixed; inset: 0; z-index: 130; pointer-events: auto; }
    .demo-tour-overlay.is-hotspots { display: none; }
    .demo-tour-spotlight { position: absolute; border: 2px solid rgba(92,181,255,0.95); border-radius: 14px; box-shadow: 0 0 0 9999px rgba(2,7,18,0.58), 0 0 28px rgba(10,132,255,0.45); transition: all 220ms ease; pointer-events: none; }
    .demo-tour-popover { position: absolute; width: min(420px, calc(100vw - 24px)); border-radius: 16px; border: 1px solid rgba(255,255,255,0.18); background: rgba(10,16,30,0.94); color: #eaf1ff; padding: 14px; box-shadow: 0 14px 44px rgba(0,0,0,0.42); pointer-events: auto; }
    .demo-tour-popover h3 { margin: 0 0 8px; font-size: 18px; letter-spacing: -0.01em; }
    .demo-tour-popover p { margin: 0; font-size: 14px; line-height: 1.5; color: #b8c8e6; }
    .demo-tour-actions { margin-top: 12px; display: flex; gap: 8px; justify-content: space-between; }
    .demo-tour-actions .left, .demo-tour-actions .right { display: flex; gap: 8px; }
    .demo-tour-actions button, .demo-tour-actions a { border: 1px solid rgba(255,255,255,0.2); border-radius: 999px; background: rgba(255,255,255,0.08); color: #fff; font-size: 12px; padding: 7px 10px; cursor: pointer; text-decoration: none; }
    .demo-tour-actions .primary { background: linear-gradient(180deg, #2290ff, #0067dc); border-color: transparent; }
    .demo-hotspot-layer { position: fixed; inset: 0; z-index: 129; pointer-events: none; }
    .demo-hotspot { position: fixed; width: 18px; height: 18px; border-radius: 999px; border: 1px solid rgba(92,181,255,0.9); background: rgba(10,132,255,0.82); box-shadow: 0 0 0 0 rgba(10,132,255,0.65); animation: demoHotspotPulse 1.7s ease-out infinite; pointer-events: auto; cursor: pointer; }
    .demo-hotspot:hover { transform: scale(1.07); }
    .demo-hotspot-panel { position: fixed; z-index: 131; width: min(390px, calc(100vw - 24px)); border-radius: 14px; border: 1px solid rgba(255,255,255,0.2); background: rgba(9,16,30,0.95); color: #eaf1ff; box-shadow: 0 16px 44px rgba(0,0,0,0.42); padding: 12px; pointer-events: auto; }
    .demo-hotspot-panel h4 { margin: 0 0 8px; font-size: 16px; }
    .demo-hotspot-panel p { margin: 0; color: #b8c8e6; font-size: 13px; line-height: 1.45; }
    .demo-hotspot-panel .actions { margin-top: 10px; display: flex; gap: 8px; flex-wrap: wrap; }
    .demo-hotspot-panel .actions button, .demo-hotspot-panel .actions a { border: 1px solid rgba(255,255,255,0.2); border-radius: 999px; background: rgba(255,255,255,0.08); color: #fff; font-size: 12px; padding: 6px 10px; text-decoration: none; cursor: pointer; }
    .demo-hotspot-panel .actions .primary { background: linear-gradient(180deg, #2290ff, #0067dc); border-color: transparent; }
    .demo-user-menu-preview {
      position: fixed;
      z-index: 129;
      display: none;
      pointer-events: none;
    }
    .demo-user-menu-preview.is-visible {
      display: block;
    }
    .demo-user-menu-preview .topbar-user-dropdown {
      position: static;
      min-width: 260px;
      display: grid !important;
      visibility: visible !important;
      opacity: 1 !important;
      pointer-events: none !important;
    }
    @keyframes demoHotspotPulse {
      0% { box-shadow: 0 0 0 0 rgba(10,132,255,0.65); }
      70% { box-shadow: 0 0 0 16px rgba(10,132,255,0); }
      100% { box-shadow: 0 0 0 0 rgba(10,132,255,0); }
    }
  `;
  document.head.appendChild(style);

  const ribbon = document.createElement("div");
  ribbon.className = "demo-ribbon";
  ribbon.innerHTML = `<span><b>Demo Mode</b> ï¿½ Read only</span><button type="button" id="demoRestartTourBtn">Restart Tour</button><a href="/home">Start With Arc Relay</a>`;
  document.body.appendChild(ribbon);

  const overlay = document.createElement("div");
  overlay.className = "demo-tour-overlay";
  overlay.innerHTML = `<div class="demo-tour-spotlight" id="demoTourSpotlight"></div><div class="demo-tour-popover" id="demoTourPopover"><h3 id="demoTourTitle">Demo Tour</h3><p id="demoTourBody">Walkthrough</p><div class="demo-tour-actions"><div class="left"><button type="button" id="demoTourBackBtn">Back</button><button type="button" id="demoTourSkipBtn">Skip</button></div><div class="right"><button type="button" class="primary" id="demoTourNextBtn">Next</button></div></div></div>`;
  document.body.appendChild(overlay);

  const demoUserMenuPreview = document.createElement("div");
  demoUserMenuPreview.className = "demo-user-menu-preview";
  document.body.appendChild(demoUserMenuPreview);

  const hotspotLayer = document.createElement("div");
  hotspotLayer.className = "demo-hotspot-layer";
  document.body.appendChild(hotspotLayer);

  const hotspotPanel = document.createElement("div");
  hotspotPanel.className = "demo-hotspot-panel";
  hotspotPanel.style.display = "none";
  hotspotPanel.innerHTML = `<h4 id="demoHotspotTitle">Product Surface</h4><p id="demoHotspotBody">Details</p><div class="actions"><button type="button" id="demoHotspotCloseBtn">Close</button><a class="primary" href="/home">Start With Arc Relay</a></div>`;
  document.body.appendChild(hotspotPanel);

  const steps = [
    { view: "home", selector: "#topbarRecoveryStrip", title: "Live revenue pulse", body: "This strip gives operators instant business outcome visibility." },
    { view: "home", selector: ".home-neo-kpis", title: "Executive KPI layer", body: "Revenue booked, appointments, contacted today, and leak exposure are grouped for quick decisions." },
    { view: "home", selector: "#demoTopbarQuickActionsPreview", title: "Locked settings access", body: "Settings in the profile menu are protected behind an admin passcode so normal employees and team members cannot change or view restricted workspace information." },
    { view: "home", selector: "#homeRecentLeadsTable .home-neo-table", title: "Prioritized lead queue", body: "Recent leads are actionable and click-through into conversation context." },
    { view: "messages", selector: ".inbox", title: "Execution workspace", body: "Your team runs follow-up here. In demo mode, mutating actions are blocked." },
    { view: "messages", selector: "#leadDetails", title: "Lead intelligence", body: "Each conversation carries context, service intent, and status." },
    { view: "contacts", selector: ".contacts-toolbar", title: "Contacts import and filtering", body: "Upload your contacts with a .vcf export from iPhone or Android, then use filters to isolate VIP, booked, new, or Do Not Reply records." },
    { view: "contacts", selector: "#contactDetail", title: "VIP and DNR controls", body: "Open any contact to mark VIP for priority handling or DNR to stop automated outbound replies while still allowing manual messages from your team." },
    { view: "schedule", selector: ".mc-shell", title: "Calendar and scheduling", body: "This demo calendar is pre-filled so you can see how booked jobs, reminders, and availability look across the schedule." },
    { view: "schedule", selector: ".mc-sidebar, .mc-toolbar", title: "Create bookings and import your calendar", body: "Use Create Booking for one-time customer appointments, Create Event for internal reminders or blocks, and Import help to export your current calendar as an .ics file and upload or subscribe it into Relay scheduling." },
    { view: "analytics", selector: ".analytics-page .analytics-grid, .analytics-grid, .analytics-kpis", title: "Conversion analytics", body: "Funnel health, response performance, and revenue intelligence." }
  ];

  const spotlight = document.getElementById("demoTourSpotlight");
  const popover = document.getElementById("demoTourPopover");
  const title = document.getElementById("demoTourTitle");
  const body = document.getElementById("demoTourBody");
  const nextBtn = document.getElementById("demoTourNextBtn");
  const backBtn = document.getElementById("demoTourBackBtn");
  const skipBtn = document.getElementById("demoTourSkipBtn");
  const restartBtn = document.getElementById("demoRestartTourBtn");
  const hotspotTitle = document.getElementById("demoHotspotTitle");
  const hotspotBody = document.getElementById("demoHotspotBody");
  const hotspotCloseBtn = document.getElementById("demoHotspotCloseBtn");

  let idx = 0;
  let running = true;
  let hotspotCleanupFns = [];

  function leaveDemo() {
    window.location.href = "/home";
  }

  const demoInspectGuard = (event) => {
    const key = String(event?.key || "").toLowerCase();
    const ctrl = event?.ctrlKey === true || event?.metaKey === true;
    const shift = event?.shiftKey === true;
    const blocked = key === "f12"
      || (ctrl && shift && (key === "i" || key === "j" || key === "c"))
      || (ctrl && key === "u")
      || (shift && key === "f10");
    if (!blocked) return;
    event.preventDefault();
    event.stopPropagation();
  };

  const demoContextMenuGuard = (event) => {
    event.preventDefault();
  };

  document.addEventListener("keydown", demoInspectGuard, true);
  document.addEventListener("contextmenu", demoContextMenuGuard, true);


  async function ensureView(view) {
    if (!view || String(state.view || "") === String(view)) return;
    state.view = view;
    await render();
  }
  async function findTarget(selector, retries = 16) {
    for (let i = 0; i < retries; i += 1) {
      const el = document.querySelector(selector);
      if (el) return el;
      await new Promise((r) => setTimeout(r, 70));
    }
    return null;
  }

  function syncDemoUserMenu(selector) {
    const userMenu = document.getElementById("topbarUserMenu");
    const userDropdown = userMenu?.querySelector(".topbar-user-dropdown");
    if (!demoUserMenuPreview) return;
    const shouldOpen = selector === "#demoTopbarQuickActionsPreview";
    demoUserMenuPreview.classList.remove("is-visible");
    demoUserMenuPreview.innerHTML = "";
    if (!shouldOpen || !userMenu || !userDropdown) return;
    const clone = userDropdown.cloneNode(true);
    clone.querySelectorAll("[id]").forEach((el) => {
      if (el.id === "topbarQuickActionsBtn") el.id = "demoTopbarQuickActionsPreview";
      else el.removeAttribute("id");
    });
    demoUserMenuPreview.appendChild(clone);
    const rect = userMenu.getBoundingClientRect();
    demoUserMenuPreview.style.left = `${Math.max(12, Math.round(rect.right - 260))}px`;
    demoUserMenuPreview.style.top = `${Math.round(rect.bottom + 8)}px`;
    demoUserMenuPreview.classList.add("is-visible");
  }


  async function primeContactsTourStep(selector) {
    if (!selector || (!selector.includes('#contactDetail') && !selector.includes('contactVipToggle') && !selector.includes('contactDnrToggle'))) return;
    const existingVip = document.querySelector('#contactVipToggle');
    const existingDnr = document.querySelector('#contactDnrToggle');
    if (existingVip || existingDnr) return;
    const firstRow = await findTarget('.contact-row', 24);
    if (!firstRow) return;
    try { firstRow.click(); } catch {}
    await new Promise((resolve) => requestAnimationFrame(() => requestAnimationFrame(resolve)));
  }

  async function showStep(i) {
    idx = Math.max(0, Math.min(steps.length - 1, i));
    const step = steps[idx];
    await ensureView(step.view);
    syncDemoUserMenu(step.selector);
    await new Promise((resolve) => requestAnimationFrame(() => requestAnimationFrame(resolve)));
    const target = await findTarget(step.selector);
    title.textContent = step.title;
    body.textContent = step.body;
    const popW = Math.min(420, window.innerWidth - 24);
    if (!target) {
      spotlight.style.left = `12px`;
      spotlight.style.top = `12px`;
      spotlight.style.width = `${Math.max(320, window.innerWidth - 24)}px`;
      spotlight.style.height = `${Math.max(220, Math.min(window.innerHeight - 24, 320))}px`;
      popover.style.width = `${popW}px`;
      popover.style.left = `${Math.max(12, Math.round((window.innerWidth - popW) / 2))}px`;
      popover.style.top = `24px`;
      backBtn.disabled = idx === 0;
      nextBtn.textContent = idx === steps.length - 1 ? "Finish" : "Next";
      return;
    }
    const shouldScrollTarget = step.selector !== "#homeRecentLeadsTable .home-neo-table";
    if (shouldScrollTarget) {
      try { target.scrollIntoView({ block: "center", inline: "nearest" }); } catch {}
    }
    const rect = target.getBoundingClientRect();
    const pad = 8;
    spotlight.style.left = `${Math.max(6, rect.left - pad)}px`;
    spotlight.style.top = `${Math.max(6, rect.top - pad)}px`;
    spotlight.style.width = `${Math.max(28, rect.width + pad * 2)}px`;
    spotlight.style.height = `${Math.max(28, rect.height + pad * 2)}px`;
    const below = rect.bottom + 14;
    const above = rect.top - 180;
    const preferAbovePopover = step.selector === "#demoTopbarQuickActionsPreview";
    const top = preferAbovePopover ? Math.max(12, above) : ((below + 180 < window.innerHeight) ? below : Math.max(12, above));
    const left = Math.min(window.innerWidth - popW - 12, Math.max(12, rect.left));
    popover.style.width = `${popW}px`;
    popover.style.left = `${left}px`;
    popover.style.top = `${top}px`;
    backBtn.disabled = idx === 0;
    nextBtn.textContent = idx === steps.length - 1 ? "Finish" : "Next";
  }

  function clearHotspots() {
    hotspotCleanupFns.forEach((fn) => {
      try { fn(); } catch {}
    });
    hotspotCleanupFns = [];
    hotspotLayer.innerHTML = "";
    hotspotPanel.style.display = "none";
  }

  async function showHotspots() {
    clearHotspots();
    overlay.classList.add("is-hotspots");
    const spots = [
      { view: "home", selector: "#topbarRecoveryStrip", title: "Revenue Pulse", body: "Real-time business outcomes for leadership and dispatch: recovered revenue, conversions, and booking momentum." },
      { view: "home", selector: ".home-neo-kpis", title: "KPI Command Layer", body: "Daily operator metrics that drive response prioritization and follow-up execution." },
      { view: "home", selector: "#demoTopbarQuickActionsPreview", title: "Locked settings", body: "The profile dropdown keeps Settings behind an admin passcode so standard employees cannot access protected configuration or sensitive account details." },
      { view: "messages", selector: ".inbox", title: "Execution Inbox", body: "Teams work live lead conversations here. Demo mode keeps this read-only while showing the real flow." },
      { view: "messages", selector: "#leadDetails", title: "Lead Context", body: "Per-lead details reduce back-and-forth and improve first-response quality." },
      { view: "contacts", selector: ".contacts-toolbar", title: "Contacts import", body: "Import a .vcf export from your current phone or address book and use filters to sort VIP, booked, new, and Do Not Reply contacts." },
      { view: "contacts", selector: "#contactDetail", title: "VIP and DNR flags", body: "Open any contact to mark VIP for priority handling or DNR to keep that record out of automated outbound messaging." },
      { view: "schedule", selector: ".mc-shell", title: "Calendar View", body: "See booked jobs, reminders, and live availability in one schedule surface." },
      { view: "schedule", selector: ".mc-sidebar, .mc-toolbar", title: "Booking and calendar import", body: "Create Booking opens a one-time booking form, Create Event adds internal calendar blocks, and Import help explains how to export your current calendar as .ics and bring it into Relay scheduling." },
      { view: "analytics", selector: ".analytics-grid", title: "Analytics Board", body: "Funnel, response speed, and revenue intelligence surfaces where to optimize next." }
    ];

    for (const spot of spots) {
      await ensureView(spot.view);
      const target = await findTarget(spot.selector);
      if (!target) continue;
      const rect = target.getBoundingClientRect();
      const btn = document.createElement("button");
      btn.type = "button";
      btn.className = "demo-hotspot";
      btn.style.left = `${Math.max(8, rect.left + Math.min(rect.width - 9, Math.max(9, rect.width * 0.78)))}px`;
      btn.style.top = `${Math.max(8, rect.top + Math.min(rect.height - 9, Math.max(9, rect.height * 0.22)))}px`;
      btn.title = spot.title;
      const clickFn = async () => {
        await ensureView(spot.view);
        const fresh = await findTarget(spot.selector, 8);
        if (!fresh) return;
        const r = fresh.getBoundingClientRect();
        hotspotTitle.textContent = spot.title;
        hotspotBody.textContent = spot.body;
        const panelW = Math.min(390, window.innerWidth - 24);
        const left = Math.min(window.innerWidth - panelW - 12, Math.max(12, r.left));
        const top = (r.bottom + 14 + 160 < window.innerHeight) ? r.bottom + 14 : Math.max(12, r.top - 170);
        hotspotPanel.style.width = `${panelW}px`;
        hotspotPanel.style.left = `${left}px`;
        hotspotPanel.style.top = `${top}px`;
        hotspotPanel.style.display = "block";
      };
      btn.addEventListener("click", clickFn);
      hotspotLayer.appendChild(btn);
      hotspotCleanupFns.push(() => btn.removeEventListener("click", clickFn));
    }
  }

  function closeTour() {
    running = false;
    clearHotspots();
    hotspotPanel.style.display = "none";
    overlay.remove();
  }

  nextBtn?.addEventListener("click", async () => {
    if (!running) return;
    if (idx >= steps.length - 1) { leaveDemo(); return; }
    await showStep(idx + 1);
  });
  backBtn?.addEventListener("click", async () => { if (running && idx > 0) await showStep(idx - 1); });
  skipBtn?.addEventListener("click", () => leaveDemo());
  restartBtn?.addEventListener("click", async () => {
    clearHotspots();
    if (!document.body.contains(overlay)) document.body.appendChild(overlay);
    overlay.classList.remove("is-hotspots");
    running = true;
    await showStep(0);
  });
  hotspotCloseBtn?.addEventListener("click", () => {
    hotspotPanel.style.display = "none";
  });
  window.addEventListener("resize", () => {
    if (running) showStep(idx).catch(() => {});
    else showHotspots().catch(() => {});
  });

  showStep(0).catch(() => {});
}
/* ==========
  Boot
========== */
initTheme();
bootAuthOverlay().then(async (ok) => {
  if (!ok) return;
  await handlePostLogin();
  if (IS_DEMO_MODE) initDemoReadOnlyTour();
});













  window.addEventListener("relay:workspace-branding-changed", () => {
    refreshTopbarBrandLogo({ force: true });
  });


























































































