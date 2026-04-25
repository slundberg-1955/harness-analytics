// Upcoming Actions inbox (M6) — drives the static page in
// templates/actions_inbox.html. Uses /portal/api/actions/inbox and
// reuses the shared deadline drawer from window.HarnessTimeline.

(function () {
  "use strict";

  const VALID_WINDOW = ["7", "30", "90", "all"];
  const VALID_ASSIGNEE = ["all", "me", "unassigned", "team"];
  const VALID_SEVERITY = ["all", "danger", "warn", "info"];
  // M0009: ``nar`` lets the inbox show only items that have been NAR'd
  // (manual or automatic). Default ``open`` continues to exclude NAR.
  const VALID_STATUS = ["open", "overdue", "snoozed", "nar", "all"];
  // Top-level inbox tabs. Maintenance fees and Paris-Convention windows
  // run on years-out cadences and would otherwise dominate Overdue, so
  // they live in dedicated tabs and are excluded from the prosecution
  // default.
  const VALID_CATEGORY = ["prosecution", "maintenance", "paris"];

  // M11: role precedence mirrors auth.ROLES so we can decide client-side
  // whether to show the "My team" chip without round-tripping the server.
  const ROLE_RANK = { VIEWER: 1, PARALEGAL: 2, ATTORNEY: 3, ADMIN: 4, OWNER: 5 };
  function roleAtLeast(actual, required) {
    return (ROLE_RANK[actual] || 0) >= (ROLE_RANK[required] || 99);
  }

  const STATE = {
    window: "30",
    assignee: "all",
    severity: "all",
    status: "open",
    category: "prosecution",
  };

  function escHtml(s) {
    if (s == null) return "";
    return String(s)
      .replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;").replace(/'/g, "&#39;");
  }
  function fmtDate(iso) { return iso ? iso.slice(0, 10) : "—"; }
  function pillClass(sev) {
    switch ((sev || "").toLowerCase()) {
      case "danger": return "tl-pill-danger";
      case "warn":   return "tl-pill-warn";
      case "info":   return "tl-pill-info";
      default:       return "tl-pill-muted";
    }
  }
  function daysFromToday(isoDate) {
    if (!isoDate) return null;
    const today = new Date(); today.setHours(0, 0, 0, 0);
    const d = new Date(isoDate);
    return Math.round((d - today) / (1000 * 60 * 60 * 24));
  }

  function readStateFromUrl() {
    const params = new URLSearchParams(window.location.search);
    if (VALID_WINDOW.includes(params.get("window")))     STATE.window   = params.get("window");
    if (VALID_ASSIGNEE.includes(params.get("assignee"))) STATE.assignee = params.get("assignee");
    if (VALID_SEVERITY.includes(params.get("severity"))) STATE.severity = params.get("severity");
    if (VALID_STATUS.includes(params.get("status")))     STATE.status   = params.get("status");
    if (VALID_CATEGORY.includes(params.get("category"))) STATE.category = params.get("category");
  }
  function writeStateToUrl() {
    const params = new URLSearchParams();
    Object.entries(STATE).forEach(([k, v]) => params.set(k, v));
    const url = `${window.location.pathname}?${params.toString()}`;
    window.history.replaceState({}, "", url);
  }

  function renderItem(item) {
    const sev = pillClass(item.severity);
    const days = daysFromToday(item.primary_date);
    const dayLabel = days == null ? "" :
      days < 0 ? `<span class="tl-pill tl-pill-danger"><span class="tl-pill-dot"></span>${Math.abs(days)}d overdue</span>` :
      days === 0 ? `<span class="tl-pill tl-pill-warn"><span class="tl-pill-dot"></span>due today</span>` :
      `<span class="tl-pill tl-pill-muted">in ${days}d</span>`;
    const assignee = item.assigned_user
      ? `<span class="tl-mono" style="font-size:11px;color:var(--tl-text-secondary);">${escHtml(item.assigned_user.name)}</span>`
      : `<span class="tl-mono" style="font-size:11px;color:var(--tl-text-tertiary);">unassigned</span>`;
    return `
      <div class="ai-row" data-deadline-id="${item.id}">
        <div class="ai-row-main">
          <div class="ai-row-title">
            <span class="tl-pill ${sev}"><span class="tl-pill-dot"></span>${escHtml(item.rule_code || "")}</span>
            <strong>${escHtml(item.description || item.primary_label || "Deadline")}</strong>
          </div>
          <div class="ai-row-meta">
            <a class="ai-row-app" href="/portal/matter/${encodeURIComponent(item.application_number || "")}">${escHtml(item.application_number || "")}</a>
            <span style="color:var(--tl-text-tertiary);">·</span>
            <span style="color:var(--tl-text-secondary);">${escHtml(item.application_title || "")}</span>
          </div>
        </div>
        <div class="ai-row-aside">
          <div class="ai-row-date tl-mono">${fmtDate(item.primary_date)}</div>
          <div>${dayLabel}</div>
          <div>${assignee}</div>
        </div>
        <button class="tl-btn tl-btn-ghost" data-action="open-drawer">Details</button>
      </div>`;
  }

  function renderBucket(b) {
    if (!b.items.length) {
      return `
        <section class="ai-bucket">
          <div class="ai-bucket-head">
            <h2>${escHtml(b.label)}</h2>
            <span class="tl-mono ai-count">0</span>
          </div>
          <div class="ai-empty">No deadlines in this bucket for the current filters.</div>
        </section>`;
    }
    return `
      <section class="ai-bucket">
        <div class="ai-bucket-head">
          <h2>${escHtml(b.label)}</h2>
          <span class="tl-mono ai-count">${b.count}</span>
        </div>
        <div class="ai-rows">${b.items.map(renderItem).join("")}</div>
      </section>`;
  }

  function render(payload) {
    const root = document.getElementById("ai-results");
    const meta = document.getElementById("ai-meta");
    if (meta) {
      meta.innerHTML = `
        <span class="ai-count-pill">${payload.total}</span>
        deadlines · as of ${escHtml(fmtDate(payload.as_of))}`;
    }
    if (!payload.buckets || !payload.buckets.length) {
      root.innerHTML = `<div class="ai-empty">No buckets returned.</div>`;
      return;
    }
    root.innerHTML = payload.buckets.map(renderBucket).join("");
    root.querySelectorAll('[data-action="open-drawer"]').forEach((btn) => {
      btn.addEventListener("click", (e) => {
        e.preventDefault();
        const row = btn.closest("[data-deadline-id]");
        const id = row && row.getAttribute("data-deadline-id");
        if (id && window.HarnessTimeline) window.HarnessTimeline.openDeadlineDrawer(id);
      });
    });
  }

  function fetchInbox() {
    const root = document.getElementById("ai-results");
    root.innerHTML = `<div class="tl-loading">Loading…</div>`;
    const params = new URLSearchParams(STATE);
    fetch(`/portal/api/actions/inbox?${params.toString()}`, { credentials: "same-origin" })
      .then((r) => {
        if (!r.ok) throw new Error(`HTTP ${r.status}`);
        return r.json();
      })
      .then(render)
      .catch((err) => {
        root.innerHTML = `<div class="tl-error">Failed to load inbox: ${escHtml(err.message || err)}</div>`;
      });
  }

  function bindFilterChips() {
    document.querySelectorAll("[data-filter-group]").forEach((group) => {
      const groupKey = group.getAttribute("data-filter-group");
      group.querySelectorAll("[data-filter-value]").forEach((chip) => {
        chip.addEventListener("click", () => {
          STATE[groupKey] = chip.getAttribute("data-filter-value");
          group.querySelectorAll("[data-filter-value]").forEach((c) => c.classList.toggle("active", c === chip));
          writeStateToUrl();
          fetchInbox();
        });
      });
    });
  }

  function applyStateToChips() {
    document.querySelectorAll("[data-filter-group]").forEach((group) => {
      const groupKey = group.getAttribute("data-filter-group");
      const current = STATE[groupKey];
      group.querySelectorAll("[data-filter-value]").forEach((c) => {
        c.classList.toggle("active", c.getAttribute("data-filter-value") === current);
      });
    });
  }

  function revealChipsForRole(role) {
    document.querySelectorAll("[data-min-role]").forEach((el) => {
      const required = el.getAttribute("data-min-role");
      if (roleAtLeast(role, required)) {
        el.hidden = false;
      } else {
        el.hidden = true;
        // If a hidden chip was the active value, fall back to "all".
        const group = el.closest("[data-filter-group]");
        if (!group) return;
        const groupKey = group.getAttribute("data-filter-group");
        if (STATE[groupKey] === el.getAttribute("data-filter-value")) {
          STATE[groupKey] = "all";
          applyStateToChips();
        }
      }
    });
  }

  function fetchMe() {
    return fetch("/portal/api/me", { credentials: "same-origin" })
      .then((r) => (r.ok ? r.json() : null))
      .then((me) => {
        if (me && me.role) {
          revealChipsForRole(me.role);
          const wrap = document.getElementById("ai-views");
          if (wrap) wrap.hidden = false;
        }
        return me;
      })
      .catch(() => null);
  }

  // ------------------------------------------------------------------
  // M12: saved views
  // ------------------------------------------------------------------

  const VIEWS_STATE = { items: [], current: null, loaded: false };

  function applySavedView(view) {
    if (!view || !view.params) return;
    Object.keys(STATE).forEach((k) => {
      if (view.params[k] != null) STATE[k] = String(view.params[k]);
    });
    VIEWS_STATE.current = view;
    const label = document.getElementById("ai-views-current");
    if (label) label.textContent = view.name;
    applyStateToChips();
    writeStateToUrl();
  }

  function renderViewsMenu() {
    const list = document.getElementById("ai-views-list");
    const empty = document.getElementById("ai-views-empty");
    if (!list || !empty) return;
    empty.hidden = VIEWS_STATE.items.length > 0;
    list.innerHTML = VIEWS_STATE.items.map((v) => `
      <li data-view-id="${v.id}">
        <button type="button" class="ai-view-default ${v.is_default ? "is-default" : ""}"
                data-act="default" title="${v.is_default ? "Default view" : "Make default"}"></button>
        <span class="ai-view-name" data-act="apply">${escHtml(v.name)}</span>
        <button type="button" class="ai-view-delete" data-act="delete" title="Delete view">×</button>
      </li>
    `).join("");
    list.querySelectorAll("[data-view-id]").forEach((li) => {
      const id = parseInt(li.getAttribute("data-view-id"), 10);
      li.querySelector('[data-act="apply"]').addEventListener("click", () => {
        const v = VIEWS_STATE.items.find((x) => x.id === id);
        if (!v) return;
        applySavedView(v);
        closeViewsMenu();
        fetchInbox();
      });
      li.querySelector('[data-act="default"]').addEventListener("click", (e) => {
        e.stopPropagation();
        fetch(`/portal/api/me/views/${id}/default`, {
          method: "POST",
          credentials: "same-origin",
          headers: { Accept: "application/json" },
        }).then((r) => r.ok ? r.json() : Promise.reject(new Error("HTTP " + r.status)))
          .then(() => loadSavedViews().then(renderViewsMenu));
      });
      li.querySelector('[data-act="delete"]').addEventListener("click", (e) => {
        e.stopPropagation();
        const v = VIEWS_STATE.items.find((x) => x.id === id);
        if (!v) return;
        if (!confirm(`Delete saved view "${v.name}"?`)) return;
        fetch(`/portal/api/me/views/${id}`, {
          method: "DELETE",
          credentials: "same-origin",
        }).then((r) => r.ok ? r.json() : Promise.reject(new Error("HTTP " + r.status)))
          .then(() => loadSavedViews().then(renderViewsMenu));
      });
    });
  }

  function loadSavedViews() {
    return fetch("/portal/api/me/views?surface=inbox", { credentials: "same-origin" })
      .then((r) => r.ok ? r.json() : { views: [] })
      .then((data) => { VIEWS_STATE.items = data.views || []; VIEWS_STATE.loaded = true; })
      .catch(() => { VIEWS_STATE.items = []; VIEWS_STATE.loaded = true; });
  }

  function openViewsMenu() {
    const menu = document.getElementById("ai-views-menu");
    const btn = document.getElementById("ai-views-btn");
    if (!menu || !btn) return;
    renderViewsMenu();
    menu.hidden = false;
    btn.setAttribute("aria-expanded", "true");
  }
  function closeViewsMenu() {
    const menu = document.getElementById("ai-views-menu");
    const btn = document.getElementById("ai-views-btn");
    if (!menu || !btn) return;
    menu.hidden = true;
    btn.setAttribute("aria-expanded", "false");
  }
  function toggleViewsMenu() {
    const menu = document.getElementById("ai-views-menu");
    if (!menu) return;
    if (menu.hidden) openViewsMenu(); else closeViewsMenu();
  }

  function bindViewsMenu() {
    const btn = document.getElementById("ai-views-btn");
    const saveBtn = document.getElementById("ai-views-save");
    if (btn) btn.addEventListener("click", (e) => { e.stopPropagation(); toggleViewsMenu(); });
    if (saveBtn) saveBtn.addEventListener("click", (e) => {
      e.stopPropagation();
      const proposed = (VIEWS_STATE.current && VIEWS_STATE.current.name) || "My view";
      const name = prompt("Save current filters as:", proposed);
      if (!name) return;
      fetch("/portal/api/me/views", {
        method: "POST",
        credentials: "same-origin",
        headers: { "Content-Type": "application/json", Accept: "application/json" },
        body: JSON.stringify({ surface: "inbox", name: name.trim(), params: { ...STATE } }),
      }).then((r) => r.ok ? r.json() : Promise.reject(new Error("HTTP " + r.status)))
        .then((view) => {
          VIEWS_STATE.current = view;
          const label = document.getElementById("ai-views-current");
          if (label) label.textContent = view.name;
          return loadSavedViews().then(renderViewsMenu);
        })
        .catch((err) => { alert("Could not save view: " + (err && err.message || err)); });
    });
    document.addEventListener("click", (e) => {
      const menu = document.getElementById("ai-views-menu");
      const wrap = document.getElementById("ai-views");
      if (!menu || !wrap || menu.hidden) return;
      if (!wrap.contains(e.target)) closeViewsMenu();
    });
  }

  function maybeApplyDefaultView() {
    // Only auto-apply if the URL had no filter params at all.
    const params = new URLSearchParams(window.location.search);
    const anyExplicit = ["window", "assignee", "severity", "status", "category"].some((k) => params.has(k));
    if (anyExplicit) return;
    const def = VIEWS_STATE.items.find((v) => v.is_default);
    if (def) applySavedView(def);
  }

  function init() {
    readStateFromUrl();
    applyStateToChips();
    bindFilterChips();
    bindViewsMenu();
    fetchMe()
      .then(loadSavedViews)
      .then(() => { maybeApplyDefaultView(); renderViewsMenu(); })
      .finally(fetchInbox);
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", init);
  } else {
    init();
  }
})();
