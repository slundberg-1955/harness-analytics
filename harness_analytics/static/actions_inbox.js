// Upcoming Actions inbox (M6) — drives the static page in
// templates/actions_inbox.html. Uses /portal/api/actions/inbox and
// reuses the shared deadline drawer from window.HarnessTimeline.

(function () {
  "use strict";

  const VALID_WINDOW = ["7", "30", "90", "all"];
  const VALID_ASSIGNEE = ["all", "me", "unassigned"];
  const VALID_SEVERITY = ["all", "danger", "warn", "info"];
  const VALID_STATUS = ["open", "overdue", "snoozed", "all"];

  const STATE = {
    window: "30",
    assignee: "all",
    severity: "all",
    status: "open",
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

  function init() {
    readStateFromUrl();
    applyStateToChips();
    bindFilterChips();
    fetchInbox();
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", init);
  } else {
    init();
  }
})();
