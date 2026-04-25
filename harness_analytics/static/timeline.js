// Prosecution Timeline (M5) — fetches /portal/api/timeline/{appnum}
// and renders response windows + milestones + informational cards into
// #prosecution-timeline. Designed to be the only consumer of the timeline
// API for now; M6 (inbox) and M7 (portfolio drawer) reuse the drawer
// helpers exposed at the bottom (window.HarnessTimeline.openDeadlineDrawer).

(function () {
  "use strict";

  const ROOT_ID = "prosecution-timeline";

  function fmtDate(iso) {
    if (!iso) return "—";
    return iso.slice(0, 10);
  }
  function fmtMoney(n) {
    if (n == null) return "—";
    return "$" + Number(n).toLocaleString();
  }
  function escHtml(s) {
    if (s == null) return "";
    return String(s)
      .replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;").replace(/'/g, "&#39;");
  }
  function severityClass(sev) {
    switch ((sev || "").toLowerCase()) {
      case "danger": return "is-danger";
      case "warn":   return "is-warn";
      case "info":   return "is-info";
      default:       return "";
    }
  }
  function pillClass(sev) {
    switch ((sev || "").toLowerCase()) {
      case "danger": return "tl-pill-danger";
      case "warn":   return "tl-pill-warn";
      case "success":return "tl-pill-success";
      case "info":   return "tl-pill-info";
      default:       return "tl-pill-muted";
    }
  }
  // M0009: status pills override severity for terminal states so NAR /
  // COMPLETED visually separate from open warn/danger items.
  function statusPillClass(status, sev) {
    const s = (status || "").toUpperCase();
    if (s === "NAR")        return "tl-pill-info tl-pill-nar";
    if (s === "COMPLETED")  return "tl-pill-success";
    if (s === "SUPERSEDED") return "tl-pill-muted";
    return pillClass(sev);
  }
  // FRPR / Paris-Convention follow-up: rows auto-cleared because the
  // 12-month window elapsed carry close_info.disposition === "deadline_passed".
  // Render a friendlier label than the raw status so attorneys can tell at
  // a glance that nothing was filed -- the calendar entry just expired.
  function statusPillText(status, closeInfo) {
    const s = (status || "").toUpperCase();
    if (s === "COMPLETED" && closeInfo && closeInfo.disposition === "deadline_passed") {
      return "Deadline Passed";
    }
    return s || "OPEN";
  }
  // M0009: friendlier action labels for the history list.
  function actionLabel(action) {
    switch ((action || "").toUpperCase()) {
      case "AUTO_COMPLETE":         return "Auto-completed by rule";
      case "AUTO_NAR":              return "Auto-NAR'd by rule";
      case "AUTO_DEADLINE_PASSED":  return "Auto-cleared (deadline passed)";
      case "NAR":                   return "Marked NAR";
      case "UN_NAR":                return "Reopened from NAR";
      case "REOPEN":                return "Reopened";
      case "COMPLETE":              return "Completed";
      default:                      return action || "";
    }
  }

  function renderRwCard(rw) {
    const sevCls = severityClass(rw.severity);
    const rows = (rw.rows || []).map((r) => `
      <tr class="${severityClass(r.severity)}">
        <td><span class="sev-chip ${severityClass(r.severity)}"></span>${escHtml(r.label || "")}</td>
        <td class="col-date">${fmtDate(r.date)}</td>
        <td class="col-fee">${r.fee_usd != null ? fmtMoney(r.fee_usd) : "—"}</td>
      </tr>
    `).join("");

    const dateCards = `
      <div class="tl-datecards">
        <div class="tl-datecard ${sevCls}">
          <div class="tl-datecard-label">Trigger</div>
          <div class="tl-datecard-date">${fmtDate(rw.trigger_date)}</div>
          <div class="tl-datecard-sub">${escHtml(rw.trigger_label || "")}</div>
        </div>
        <div class="tl-datecard ${sevCls}">
          <div class="tl-datecard-label">${escHtml(rw.primary_label || "Primary")}</div>
          <div class="tl-datecard-date">${fmtDate(rw.primary_date)}</div>
          <div class="tl-datecard-sub">Status: ${escHtml(rw.status || "OPEN")}</div>
        </div>
        <div class="tl-datecard">
          <div class="tl-datecard-label">Statutory bar</div>
          <div class="tl-datecard-date">${fmtDate(rw.statutory_bar_date)}</div>
          <div class="tl-datecard-sub">${rw.extendable ? "Extendable via 1.136(a)" : "Not extendable"}</div>
        </div>
      </div>
    `;

    const eot = rows ? `
      <div class="tl-eot">
        <table>
          <thead><tr><th>Step</th><th class="col-date">Date</th><th class="col-fee">Fee</th></tr></thead>
          <tbody>${rows}</tbody>
        </table>
      </div>` : "";

    const warnings = (rw.warnings && rw.warnings.length) ? `
      <div class="tl-warnings"><strong>Warnings:</strong> ${rw.warnings.map(escHtml).join("; ")}</div>` : "";

    const assigned = rw.assigned_user ? `
      <div class="tl-assignees">
        <span class="tl-avatar">${escHtml((rw.assigned_user.name || "?").slice(0, 2).toUpperCase())}</span>
        <span>${escHtml(rw.assigned_user.name)}</span>
      </div>` : `<div class="tl-assignees" style="color:var(--tl-text-tertiary);">Unassigned</div>`;

    return `
      <div class="tl-rwcard ${sevCls}" data-deadline-id="${rw.id}">
        <div class="tl-rwcard-head">
          <div>
            <h3 class="tl-rwcard-title">${escHtml(rw.description || rw.rule_code)}</h3>
            <div class="tl-rwcard-sub">
              <span class="code">${escHtml(rw.rule_code)}</span>
              · ${escHtml(rw.rule_kind || "")}
              · trigger ${fmtDate(rw.trigger_date)}
            </div>
          </div>
          <span class="tl-pill ${statusPillClass(rw.status, rw.severity)}">
            <span class="tl-pill-dot"></span>${escHtml(statusPillText(rw.status, rw.close_info))}
          </span>
          ${rw.verified ? `<span class="tl-verified-badge" title="Verified ${escHtml((rw.verified.verified_at || "").slice(0, 10))}">\u2713 Verified</span>` : ""}
        </div>
        ${dateCards}
        ${eot}
        ${warnings}
        <div class="tl-rwcard-footer">
          ${assigned}
          <div class="tl-btn-row">
            <button class="tl-btn tl-btn-ghost" data-action="open-drawer">Details</button>
          </div>
        </div>
        ${rw.authority ? `<div class="tl-authority">${escHtml(rw.authority)}</div>` : ""}
      </div>`;
  }

  function renderMilestone(m) {
    return `
      <div class="tl-milestone">
        <div class="tl-milestone-dot"></div>
        <div class="tl-milestone-date">${fmtDate(m.date)}</div>
        <div class="tl-milestone-label">${escHtml(m.label || "")}</div>
        <div class="tl-milestone-sub">${escHtml(m.source || "")}</div>
      </div>`;
  }

  function renderInfoCard(info) {
    const phases = (info.ids_phases || []).map((p) => `
      <tr>
        <td class="tl-mono">${escHtml(p.code || "")}</td>
        <td>${fmtDate(p.start)} → ${fmtDate(p.end)}</td>
        <td>${escHtml(p.label || "")}</td>
      </tr>`).join("");
    const idsBlock = phases ? `
      <table class="tl-eot" style="margin-top:8px;"><thead>
        <tr><th>Phase</th><th>Window</th><th>Note</th></tr></thead>
        <tbody>${phases}</tbody>
      </table>` : "";
    return `
      <div class="tl-info-card" data-deadline-id="${info.id}">
        <div class="tl-info-card-head">
          <div class="tl-info-card-title">${escHtml(info.description || info.rule_code)}</div>
          <span class="tl-pill ${statusPillClass(info.status, info.severity)}"><span class="tl-pill-dot"></span>${escHtml(statusPillText(info.status, info.close_info))}</span>
        </div>
        <div class="tl-info-card-body">
          <div><strong>${escHtml(info.primary_label || "Primary")}:</strong> ${fmtDate(info.primary_date)}</div>
          ${info.trigger_date ? `<div>Trigger: ${fmtDate(info.trigger_date)} (${escHtml(info.trigger_label || "")})</div>` : ""}
          ${info.user_note ? `<div style="margin-top:6px;font-style:italic;">${escHtml(info.user_note)}</div>` : ""}
        </div>
        ${idsBlock}
        <div class="tl-rwcard-footer" style="margin-top:10px;">
          <span class="tl-mono" style="font-size:11px;color:var(--tl-text-tertiary);">${escHtml(info.rule_code || "")}</span>
          <button class="tl-btn tl-btn-ghost" data-action="open-drawer">Details</button>
        </div>
      </div>`;
  }

  function render(root, payload) {
    const sp = payload.status_pill || { label: "—", severity: "muted" };
    const milestones = (payload.milestones || []).map(renderMilestone).join("") ||
      `<div style="padding:20px;color:var(--tl-text-tertiary);">No milestone events recorded yet.</div>`;
    const rwHtml = (payload.response_windows || []).map(renderRwCard).join("") ||
      `<div class="tl-info-card"><div class="tl-info-card-body">No open response windows. (Either no Office Action has been mailed, or all responses are filed.)</div></div>`;
    const infoHtml = (payload.informational || []).map(renderInfoCard).join("") ||
      `<div class="tl-info-card"><div class="tl-info-card-body">No informational deadlines computed for this matter.</div></div>`;

    root.innerHTML = `
      <div class="tl-disclaimer">
        <span class="tl-disclaimer-icon">!</span>
        <div><strong>${escHtml(payload.disclaimer || "Derived from USPTO file history.")}</strong>
          As-of ${escHtml(fmtDate(payload.as_of))}.</div>
      </div>

      <div style="margin-bottom: 20px;">
        <span class="tl-pill ${pillClass(sp.severity)}">
          <span class="tl-pill-dot"></span>${escHtml(sp.label)}
        </span>
      </div>

      <div class="tl-section">
        <div class="tl-section-head">
          <div class="tl-section-title">Response windows</div>
          <div class="tl-section-subtitle">${(payload.response_windows || []).length} open</div>
        </div>
        ${rwHtml}
      </div>

      <div class="tl-section">
        <div class="tl-section-head">
          <div class="tl-section-title">Milestones</div>
          <div class="tl-section-subtitle">${(payload.milestones || []).length} events</div>
        </div>
        <div class="tl-milestones">${milestones}</div>
      </div>

      <div class="tl-section">
        <div class="tl-section-head">
          <div class="tl-section-title">Informational deadlines</div>
          <div class="tl-section-subtitle">${(payload.informational || []).length} items</div>
        </div>
        <div class="tl-info-grid">${infoHtml}</div>
      </div>
    `;

    // Wire drawer-open buttons.
    root.querySelectorAll('[data-action="open-drawer"]').forEach((btn) => {
      btn.addEventListener("click", () => {
        const card = btn.closest("[data-deadline-id]");
        if (card) openDeadlineDrawer(card.getAttribute("data-deadline-id"));
      });
    });
  }

  // ---- shared drawer (used here + by inbox + by portfolio) ----------------

  function ensureDrawer() {
    let backdrop = document.getElementById("tl-drawer-backdrop");
    if (backdrop) return backdrop;
    backdrop = document.createElement("div");
    backdrop.id = "tl-drawer-backdrop";
    backdrop.className = "tl-drawer-backdrop";
    backdrop.innerHTML = `
      <div class="tl-drawer" id="tl-drawer">
        <div class="tl-drawer-head">
          <strong id="tl-drawer-title">Deadline detail</strong>
          <button type="button" class="tl-drawer-close" id="tl-drawer-close" aria-label="Close">×</button>
        </div>
        <div class="tl-drawer-body" id="tl-drawer-body"></div>
      </div>`;
    document.body.appendChild(backdrop);
    backdrop.addEventListener("click", (e) => { if (e.target === backdrop) closeDrawer(); });
    document.getElementById("tl-drawer-close").addEventListener("click", closeDrawer);
    document.addEventListener("keydown", (e) => { if (e.key === "Escape") closeDrawer(); });
    return backdrop;
  }
  function openDrawer() {
    const backdrop = ensureDrawer();
    backdrop.classList.add("open");
    document.getElementById("tl-drawer").classList.add("open");
  }
  function closeDrawer() {
    const backdrop = document.getElementById("tl-drawer-backdrop");
    if (!backdrop) return;
    backdrop.classList.remove("open");
    document.getElementById("tl-drawer").classList.remove("open");
  }

  function openDeadlineDrawer(deadlineId) {
    const backdrop = ensureDrawer();
    const body = document.getElementById("tl-drawer-body");
    const title = document.getElementById("tl-drawer-title");
    title.textContent = `Deadline #${deadlineId}`;
    body.innerHTML = `<div style="padding:24px;text-align:center;color:#888;">Loading…</div>`;
    openDrawer();
    fetch(`/portal/api/timeline/deadlines/${encodeURIComponent(deadlineId)}`, {
      credentials: "same-origin",
    })
      .then((r) => {
        if (!r.ok) throw new Error(`HTTP ${r.status}`);
        return r.json();
      })
      .then((cd) => {
        title.textContent = `${cd.rule_code || "Deadline"} — ${cd.application_number || ""}`;
        const rows = (cd.rows || []).map((r) => `
          <tr><td>${escHtml(r.label || "")}</td><td class="tl-mono">${fmtDate(r.date)}</td>
              <td style="text-align:right;">${r.fee_usd != null ? fmtMoney(r.fee_usd) : "—"}</td></tr>`).join("");
        const history = (cd.history || []).map((h) => `
          <div style="padding:8px 0;border-bottom:0.5px solid rgba(0,0,0,0.1);">
            <div><strong>${escHtml(actionLabel(h.action))}</strong> ${h.user ? `by ${escHtml(h.user.name)}` : ""}</div>
            <div class="tl-mono" style="font-size:11px;color:#888;">${escHtml(h.occurred_at || "")}</div>
            ${Object.keys(h.payload || {}).length ? `<details><summary>payload</summary><pre style="font-size:11px;overflow-x:auto;">${escHtml(JSON.stringify(h.payload, null, 2))}</pre></details>` : ""}
          </div>`).join("");
        // M0009: render the "Closed by rule …" subtitle in the drawer when
        // the deadline carries close_info (auto-close audit triplet).
        const ci = cd.close_info;
        const closeBlock = ci ? `
          <div style="margin-top:6px;padding:8px;background:rgba(0,0,0,0.03);border-radius:4px;">
            <div><strong>${escHtml(
              ci.disposition === "auto_complete" ? "Auto-completed by rule" :
              ci.disposition === "auto_nar" ? "Auto-NAR'd by rule" :
              ci.disposition === "deadline_passed" ? "Deadline passed (auto-cleared)" :
              ci.disposition === "manual_nar" ? "Marked NAR (manual)" :
              ci.disposition === "manual_complete" ? "Marked complete (manual)" :
              "Closed"
            )}</strong></div>
            ${ci.matched_pattern ? `<div class="tl-mono" style="font-size:11px;">pattern: ${escHtml(ci.matched_pattern)}</div>` : ""}
            ${ci.closed_by_ifw_document_id ? `<div class="tl-mono" style="font-size:11px;">IFW doc #${ci.closed_by_ifw_document_id}</div>` : ""}
            ${ci.closed_at ? `<div class="tl-mono" style="font-size:11px;color:#888;">at ${escHtml(ci.closed_at)}</div>` : ""}
          </div>` : "";
        body.innerHTML = `
          <div><strong>${escHtml(cd.application_title || "")}</strong></div>
          <div class="tl-mono" style="font-size:11px;color:#888;margin-bottom:12px;">${escHtml(cd.application_number || "")}</div>
          <div><strong>${escHtml(cd.primary_label || "")}</strong>: ${fmtDate(cd.primary_date)}
            ${cd.verified ? `<span class="tl-verified-badge" title="Verified by ${escHtml((cd.verified.verified_by && cd.verified.verified_by.name) || "attorney")} on ${escHtml((cd.verified.verified_at || "").slice(0, 10))}">\u2713 Verified</span>` : ""}
          </div>
          <div>Trigger: ${fmtDate(cd.trigger_date)} (${escHtml(cd.trigger_label || "")})</div>
          <div>Status: ${escHtml(statusPillText(cd.status, cd.close_info))}</div>
          ${closeBlock}
          ${cd.authority ? `<div class="tl-authority" style="margin-top:8px;">${escHtml(cd.authority)}</div>` : ""}
          ${rows ? `<table class="tl-eot" style="margin-top:14px;width:100%;border-collapse:collapse;"><thead><tr><th>Step</th><th>Date</th><th style="text-align:right;">Fee</th></tr></thead><tbody>${rows}</tbody></table>` : ""}
          <h4 style="margin-top:18px;">History</h4>
          ${history || `<div style="color:#888;">No history yet.</div>`}
        `;
      })
      .catch((err) => {
        body.innerHTML = `<div class="tl-error">Failed to load deadline: ${escHtml(err.message || err)}</div>`;
      });
  }

  // ---- bootstrap ----------------------------------------------------------

  function load() {
    const root = document.getElementById(ROOT_ID);
    if (!root) return;
    const appNumber = root.getAttribute("data-app-number");
    if (!appNumber) {
      root.innerHTML = `<div class="tl-error">Missing data-app-number on #${ROOT_ID}</div>`;
      return;
    }
    root.innerHTML = `<div class="tl-loading">Loading prosecution timeline…</div>`;
    fetch(`/portal/api/timeline/${encodeURIComponent(appNumber)}`, {
      credentials: "same-origin",
    })
      .then((r) => {
        if (!r.ok) throw new Error(`HTTP ${r.status}`);
        return r.json();
      })
      .then((payload) => render(root, payload))
      .catch((err) => {
        root.innerHTML = `<div class="tl-error">Failed to load timeline: ${escHtml(err.message || err)}</div>`;
      });
  }

  window.HarnessTimeline = {
    load,
    openDeadlineDrawer,
    closeDrawer,
  };

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", load);
  } else {
    load();
  }
})();
