/* M8 — Timeline rules admin page.
 *
 * Pure vanilla JS, no framework. Talks to:
 *   GET    /portal/api/admin/rules            → list rules + unmapped codes
 *   GET    /portal/api/admin/rules/{id}       → fetch one (used for refresh)
 *   PUT    /portal/api/admin/rules/{id}       → update + recompute
 *
 * Non-admins get 403 from those routes; we surface a clear message rather
 * than silently failing.
 */
(function () {
  "use strict";

  const state = {
    rules: [],
    unmapped: [],
    selectedId: null,
    filter: "all",     // all | active | overrides | inactive
    search: "",
    tenantId: "global",
    saving: false,
  };

  const KIND_OPTIONS = [
    "standard_oa",
    "hard_noa",
    "appeal_brief",
    "priority_later_of",
    "pct_national",
    "ids_phase",
    "maintenance",
    "soft_window",
    "informational",
  ];

  const PRIORITY_OPTIONS = [
    { v: "",          label: "(unset)" },
    { v: "CRITICAL",  label: "CRITICAL — abandonment if missed" },
    { v: "IMPORTANT", label: "IMPORTANT — fee/loss of rights" },
    { v: "ROUTINE",   label: "ROUTINE — inconvenience" },
  ];

  // ----- helpers ------------------------------------------------------------

  function escHtml(s) {
    return String(s == null ? "" : s).replace(/[&<>"']/g, (c) => ({
      "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;",
    })[c]);
  }
  function escAttr(s) { return escHtml(s); }

  function fetchJson(url, opts) {
    return fetch(url, Object.assign({ credentials: "same-origin",
      headers: { Accept: "application/json", "Content-Type": "application/json" } }, opts || {}))
      .then((r) => {
        if (r.status === 401) throw new Error("auth");
        if (r.status === 403) throw new Error("forbidden");
        if (!r.ok) throw new Error(`HTTP ${r.status}`);
        return r.json();
      });
  }

  // ----- rendering ----------------------------------------------------------

  function visibleRules() {
    let rows = state.rules.slice();
    if (state.filter === "active") rows = rows.filter((r) => r.active);
    else if (state.filter === "inactive") rows = rows.filter((r) => !r.active);
    else if (state.filter === "overrides") rows = rows.filter((r) => r.is_override);
    if (state.search) {
      const q = state.search.toLowerCase();
      rows = rows.filter((r) =>
        (r.code || "").toLowerCase().includes(q) ||
        (r.description || "").toLowerCase().includes(q) ||
        (r.aliases || []).some((a) => a.toLowerCase().includes(q))
      );
    }
    return rows;
  }

  function renderList() {
    const root = document.getElementById("ra-rule-list");
    const rows = visibleRules();
    if (!rows.length) {
      root.innerHTML = `<div class="ra-empty">No rules match.</div>`;
      return;
    }
    root.innerHTML = rows.map((r) => {
      const tags = [];
      tags.push(`<span class="ra-tag ra-tag-kind">${escHtml(r.kind || "")}</span>`);
      if (r.is_override) tags.push(`<span class="ra-tag ra-tag-override">override</span>`);
      if (!r.active) tags.push(`<span class="ra-tag ra-tag-inactive">inactive</span>`);
      const active = state.selectedId === r.id ? " active" : "";
      return `
        <div class="ra-rule-item${active}" data-rule-id="${r.id}">
          <div>
            <div class="ra-rule-code">${escHtml(r.code)}</div>
            <div class="ra-rule-desc">${escHtml(r.description || "")}</div>
          </div>
          <div class="ra-rule-tags">${tags.join("")}</div>
        </div>
      `;
    }).join("");
    root.querySelectorAll(".ra-rule-item").forEach((el) => {
      el.addEventListener("click", () => {
        state.selectedId = parseInt(el.getAttribute("data-rule-id"), 10);
        renderList();
        renderEditor();
      });
    });
  }

  function renderUnmapped() {
    const root = document.getElementById("ra-unmapped");
    if (!state.unmapped.length) {
      root.innerHTML = `<div class="ra-empty">No unmapped codes — every code seen has a rule.</div>`;
      return;
    }
    root.innerHTML = state.unmapped.map((u) => `
      <div class="ra-unmapped-item">
        <code>${escHtml(u.code)}</code>
        <span class="ra-unmapped-count">${u.count.toLocaleString()}</span>
      </div>
    `).join("");
  }

  function renderEditor() {
    const root = document.getElementById("ra-editor");
    const rule = state.rules.find((r) => r.id === state.selectedId);
    if (!rule) {
      root.innerHTML = `<div class="ra-empty">Select a rule from the list to edit it.</div>`;
      return;
    }

    const pillCls = rule.active ? "ra-pill-active" : "ra-pill-inactive";
    const pillLabel = rule.active ? "ACTIVE" : "INACTIVE";

    root.innerHTML = `
      <div class="ra-editor-head">
        <div>
          <h2 class="ra-editor-title">
            <span class="code">${escHtml(rule.code)}</span>
            <span>${escHtml(rule.description || "")}</span>
            <span class="pill ${pillCls}">${pillLabel}</span>
            ${rule.is_override ? `<span class="pill ra-pill-override">OVERRIDE</span>` : ""}
          </h2>
          <div class="ra-editor-meta">
            kind: ${escHtml(rule.kind)} · tenant_id: ${escHtml(rule.tenant_id)}
            ${rule.updated_at ? ` · updated ${escHtml(rule.updated_at.slice(0, 10))}` : ""}
          </div>
        </div>
        <div style="display:flex; gap:8px; align-items:flex-start;">
          <button type="button" class="ra-btn" data-act="reset">Reset</button>
          <button type="button" class="ra-btn ra-btn-primary" data-act="save">
            Save &amp; recompute
          </button>
        </div>
      </div>

      <div class="ra-warn-banner">
        Saving will queue a tenant-wide recompute for <strong>${escHtml(state.tenantId)}</strong>.
        Editing a global rule from a non-global tenant will create a tenant override automatically.
      </div>

      <div class="ra-status" id="ra-status"></div>

      <form id="ra-form">
        <div class="ra-field-group">
          <h3>Identity</h3>
          <div class="ra-grid">
            <div class="ra-field">
              <label>IFW code</label>
              <input type="text" value="${escAttr(rule.code)}" readonly />
              <div class="hint">Primary code is immutable.</div>
            </div>
            <div class="ra-field">
              <label>Kind</label>
              <select name="kind">
                ${KIND_OPTIONS.map((k) =>
                  `<option value="${escAttr(k)}"${k === rule.kind ? " selected" : ""}>${escHtml(k)}</option>`
                ).join("")}
              </select>
              <div class="hint">Changing kind alters which date math runs.</div>
            </div>
            <div class="ra-field" style="grid-column: span 2;">
              <label>Aliases (comma-separated)</label>
              <input type="text" name="aliases" value="${escAttr((rule.aliases || []).join(", "))}" />
              <div class="hint">IFW codes that map to this rule.</div>
            </div>
            <div class="ra-field" style="grid-column: span 2;">
              <label>Description</label>
              <input type="text" name="description" value="${escAttr(rule.description || "")}" />
            </div>
          </div>
        </div>

        <div class="ra-field-group">
          <h3>Timing</h3>
          <div class="ra-grid">
            ${numField("ssp_months", "SSP base period (months)", rule.ssp_months,
              "35 U.S.C. § 133 shortened statutory period")}
            ${numField("max_months", "Max extended period (months)", rule.max_months,
              "Statutory bar under 37 C.F.R. § 1.136(a)")}
            ${numField("from_filing_months", "From filing (months)", rule.from_filing_months, "")}
            ${numField("from_priority_months", "From priority (months)", rule.from_priority_months, "")}
            ${numField("base_months_from_priority", "Base from priority (months)", rule.base_months_from_priority, "")}
            ${numField("late_months_from_priority", "Late from priority (months)", rule.late_months_from_priority, "")}
            ${numField("due_months_from_grant", "Due from grant (months)", rule.due_months_from_grant, "")}
            ${numField("grace_months_from_grant", "Grace from grant (months)", rule.grace_months_from_grant, "")}
            <div class="ra-field ra-checkbox">
              <input type="checkbox" id="f-extendable" name="extendable" ${rule.extendable ? "checked" : ""} />
              <label for="f-extendable">Extendable under 37 C.F.R. § 1.136(a)</label>
            </div>
            <div class="ra-field ra-checkbox">
              <input type="checkbox" id="f-active" name="active" ${rule.active ? "checked" : ""} />
              <label for="f-active">Rule is active</label>
            </div>
          </div>
        </div>

        <div class="ra-field-group">
          <h3>Messaging</h3>
          <div class="ra-grid one">
            <div class="ra-field">
              <label>Trigger label (shown in UI)</label>
              <input type="text" name="trigger_label" value="${escAttr(rule.trigger_label || "")}" />
            </div>
            <div class="ra-field">
              <label>User note (plain-English)</label>
              <textarea name="user_note">${escHtml(rule.user_note || "")}</textarea>
            </div>
            <div class="ra-field">
              <label>Authority (citation string)</label>
              <input type="text" name="authority" value="${escAttr(rule.authority || "")}" />
            </div>
            <div class="ra-field">
              <label>Warnings (one per line)</label>
              <textarea name="warnings">${escHtml((rule.warnings || []).join("\n"))}</textarea>
            </div>
          </div>
        </div>

        <div class="ra-field-group">
          <h3>Classification</h3>
          <div class="ra-grid">
            <div class="ra-field">
              <label>Priority tier</label>
              <select name="priority_tier">
                ${PRIORITY_OPTIONS.map((o) =>
                  `<option value="${escAttr(o.v)}"${o.v === (rule.priority_tier || "") ? " selected" : ""}>${escHtml(o.label)}</option>`
                ).join("")}
              </select>
            </div>
            <div class="ra-field">
              <label>Patent type applicability</label>
              <input type="text" value="${escAttr((rule.patent_type_applicability || []).join(", "))}" readonly />
              <div class="hint">Edit via JSON seed for now.</div>
            </div>
          </div>
        </div>
      </form>
    `;

    root.querySelector('[data-act="save"]').addEventListener("click", saveRule);
    root.querySelector('[data-act="reset"]').addEventListener("click", () => renderEditor());
  }

  function numField(name, label, value, hint) {
    const v = value == null ? "" : String(value);
    return `
      <div class="ra-field">
        <label>${escHtml(label)}</label>
        <input type="number" name="${escAttr(name)}" value="${escAttr(v)}" />
        ${hint ? `<div class="hint">${escHtml(hint)}</div>` : ""}
      </div>
    `;
  }

  // ----- save ---------------------------------------------------------------

  function collectForm() {
    const form = document.getElementById("ra-form");
    if (!form) return null;
    const data = {};
    const els = form.querySelectorAll("input, select, textarea");
    els.forEach((el) => {
      const name = el.getAttribute("name");
      if (!name) return;
      if (el.type === "checkbox") {
        data[name] = el.checked;
      } else if (el.type === "number") {
        const v = el.value.trim();
        data[name] = v === "" ? null : parseInt(v, 10);
      } else if (name === "aliases") {
        data[name] = el.value.split(",").map((s) => s.trim()).filter(Boolean);
      } else if (name === "warnings") {
        data[name] = el.value.split("\n").map((s) => s.trim()).filter(Boolean);
      } else {
        data[name] = el.value;
      }
    });
    return data;
  }

  function setStatus(msg, kind) {
    const el = document.getElementById("ra-status");
    if (!el) return;
    if (!msg) { el.innerHTML = ""; return; }
    const cls = kind === "error" ? "ra-warn-banner" : "ra-status-banner";
    el.innerHTML = `<div class="${cls}">${escHtml(msg)}</div>`;
  }

  function saveRule() {
    if (state.saving) return;
    const rule = state.rules.find((r) => r.id === state.selectedId);
    if (!rule) return;
    const payload = collectForm();
    if (!payload) return;
    state.saving = true;
    setStatus("Saving and queuing recompute…", "info");
    fetchJson(`/portal/api/admin/rules/${rule.id}`, {
      method: "PUT",
      body: JSON.stringify(payload),
    }).then((updated) => {
      // Replace in cache (the id can change if a new override row was created).
      const idx = state.rules.findIndex((r) => r.id === rule.id);
      if (idx >= 0) state.rules[idx] = updated;
      else state.rules.unshift(updated);
      state.selectedId = updated.id;
      const recomp = updated.recompute || "queued";
      setStatus(`Saved. Tenant recompute: ${recomp}.`, "info");
      renderList();
      renderEditor();
    }).catch((err) => {
      setStatus(err && err.message === "forbidden"
        ? "You need ADMIN to save rules."
        : `Save failed: ${err && err.message ? err.message : err}`, "error");
    }).finally(() => { state.saving = false; });
  }

  // ----- bootstrap ----------------------------------------------------------

  function refresh() {
    return fetchJson("/portal/api/admin/rules").then((data) => {
      state.tenantId = data.tenant_id || "global";
      state.rules = data.rules || [];
      state.unmapped = data.unmapped_codes || [];
      const meta = document.getElementById("ra-meta");
      if (meta) {
        meta.textContent = `${state.rules.length} rules · ${state.unmapped.length} unmapped · tenant ${state.tenantId}`;
      }
      if (state.selectedId == null && state.rules.length) {
        state.selectedId = state.rules[0].id;
      }
      renderList();
      renderUnmapped();
      renderEditor();
    }).catch((err) => {
      const editor = document.getElementById("ra-editor");
      const list = document.getElementById("ra-rule-list");
      const msg = err && err.message === "forbidden"
        ? "You need ADMIN or OWNER to view this page."
        : err && err.message === "auth"
          ? "Please sign in again."
          : `Failed to load rules: ${err && err.message ? err.message : err}`;
      if (list) list.innerHTML = `<div class="ra-empty">${escHtml(msg)}</div>`;
      if (editor) editor.innerHTML = `<div class="ra-empty">${escHtml(msg)}</div>`;
    });
  }

  function bind() {
    document.querySelectorAll("[data-rule-filter]").forEach((el) => {
      el.addEventListener("click", () => {
        document.querySelectorAll("[data-rule-filter]").forEach((b) =>
          b.classList.remove("active"));
        el.classList.add("active");
        state.filter = el.getAttribute("data-rule-filter");
        renderList();
      });
    });
    const search = document.getElementById("ra-search");
    if (search) {
      search.addEventListener("input", () => {
        state.search = search.value.trim();
        renderList();
      });
    }
  }

  document.addEventListener("DOMContentLoaded", () => {
    bind();
    refresh();
  });
})();
