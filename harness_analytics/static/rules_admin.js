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
    activeTab: "rules", // rules | unmapped | supersession
    supersession: { pairs: [], loaded: false },
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
    const sideRoot = document.getElementById("ra-unmapped");
    const fullRoot = document.getElementById("ra-unmapped-full");
    const html = !state.unmapped.length
      ? `<div class="ra-empty">No unmapped codes — every code seen has a rule.</div>`
      : state.unmapped.map((u) => `
          <div class="ra-unmapped-item">
            <code>${escHtml(u.code)}</code>
            <span class="ra-unmapped-count">${u.count.toLocaleString()}</span>
          </div>
        `).join("");
    if (sideRoot) sideRoot.innerHTML = html;
    if (fullRoot) fullRoot.innerHTML = html;
  }

  function diffSet(rule) {
    return new Set(rule && rule.diff_fields ? rule.diff_fields : []);
  }

  function diffDot(field, diffs) {
    return diffs.has(field) ? `<span class="ra-diff-dot" title="Differs from global"></span>` : "";
  }

  function revertButton(field, diffs) {
    return diffs.has(field)
      ? ` <button type="button" class="ra-revert-field" data-revert="${escAttr(field)}">revert</button>`
      : "";
  }

  function renderDiffPanel(rule) {
    if (!rule.is_override || !rule.global_parent || !(rule.diff_fields || []).length) {
      return "";
    }
    const rows = rule.diff_fields.map((f) => `
      <tr>
        <td>${escHtml(f)}</td>
        <td class="ra-diff-global">${escHtml(formatValue(rule.global_parent[f]))}</td>
        <td class="ra-diff-tenant">${escHtml(formatValue(rule[f]))}</td>
      </tr>
    `).join("");
    return `
      <div class="ra-diff-panel" id="ra-diff-panel" hidden>
        <h4>Compare to global (${rule.diff_fields.length} field${rule.diff_fields.length === 1 ? "" : "s"} differ)</h4>
        <table class="ra-diff-table">
          <thead><tr><th>Field</th><th>Global</th><th>This tenant</th></tr></thead>
          <tbody>${rows}</tbody>
        </table>
      </div>
    `;
  }

  function formatValue(v) {
    if (v == null) return "—";
    if (Array.isArray(v)) return v.join(", ") || "—";
    if (typeof v === "boolean") return v ? "true" : "false";
    return String(v);
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
    const diffs = diffSet(rule);
    const overridePill = rule.is_override
      ? `<span class="pill ra-pill-override" data-act="toggle-diff" title="Click to compare with global">OVERRIDE${diffs.size ? ` · ${diffs.size}` : ""}</span>`
      : "";

    root.innerHTML = `
      <div class="ra-editor-head">
        <div>
          <h2 class="ra-editor-title">
            <span class="code">${escHtml(rule.code)}</span>
            <span>${escHtml(rule.description || "")}</span>
            <span class="pill ${pillCls}">${pillLabel}</span>
            ${overridePill}
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

      ${renderDiffPanel(rule)}

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
              <label>Kind${diffDot("kind", diffs)}${revertButton("kind", diffs)}</label>
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
              <label>Description${diffDot("description", diffs)}${revertButton("description", diffs)}</label>
              <input type="text" name="description" value="${escAttr(rule.description || "")}" />
            </div>
          </div>
        </div>

        <div class="ra-field-group">
          <h3>Timing</h3>
          <div class="ra-grid">
            ${numField("ssp_months", "SSP base period (months)", rule.ssp_months,
              "35 U.S.C. § 133 shortened statutory period", diffs)}
            ${numField("max_months", "Max extended period (months)", rule.max_months,
              "Statutory bar under 37 C.F.R. § 1.136(a)", diffs)}
            ${numField("from_filing_months", "From filing (months)", rule.from_filing_months, "", diffs)}
            ${numField("from_priority_months", "From priority (months)", rule.from_priority_months, "", diffs)}
            ${numField("base_months_from_priority", "Base from priority (months)", rule.base_months_from_priority, "", diffs)}
            ${numField("late_months_from_priority", "Late from priority (months)", rule.late_months_from_priority, "", diffs)}
            ${numField("due_months_from_grant", "Due from grant (months)", rule.due_months_from_grant, "", diffs)}
            ${numField("grace_months_from_grant", "Grace from grant (months)", rule.grace_months_from_grant, "", diffs)}
            <div class="ra-field ra-checkbox">
              <input type="checkbox" id="f-extendable" name="extendable" ${rule.extendable ? "checked" : ""} />
              <label for="f-extendable">Extendable under 37 C.F.R. § 1.136(a)${diffDot("extendable", diffs)}${revertButton("extendable", diffs)}</label>
            </div>
            <div class="ra-field ra-checkbox">
              <input type="checkbox" id="f-active" name="active" ${rule.active ? "checked" : ""} />
              <label for="f-active">Rule is active${diffDot("active", diffs)}${revertButton("active", diffs)}</label>
            </div>
          </div>
        </div>

        <div class="ra-field-group">
          <h3>Messaging</h3>
          <div class="ra-grid one">
            <div class="ra-field">
              <label>Trigger label (shown in UI)${diffDot("trigger_label", diffs)}${revertButton("trigger_label", diffs)}</label>
              <input type="text" name="trigger_label" value="${escAttr(rule.trigger_label || "")}" />
            </div>
            <div class="ra-field">
              <label>User note (plain-English)${diffDot("user_note", diffs)}${revertButton("user_note", diffs)}</label>
              <textarea name="user_note">${escHtml(rule.user_note || "")}</textarea>
            </div>
            <div class="ra-field">
              <label>Authority (citation string)${diffDot("authority", diffs)}${revertButton("authority", diffs)}</label>
              <input type="text" name="authority" value="${escAttr(rule.authority || "")}" />
            </div>
            <div class="ra-field">
              <label>Warnings (one per line)${diffDot("warnings", diffs)}${revertButton("warnings", diffs)}</label>
              <textarea name="warnings">${escHtml((rule.warnings || []).join("\n"))}</textarea>
            </div>
          </div>
        </div>

        <div class="ra-field-group">
          <h3>Classification</h3>
          <div class="ra-grid">
            <div class="ra-field">
              <label>Priority tier${diffDot("priority_tier", diffs)}${revertButton("priority_tier", diffs)}</label>
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

      <details class="ra-history" id="ra-history">
        <summary>Edit history</summary>
        <ul class="ra-history-list" id="ra-history-list">
          <li class="ra-empty">Open to load…</li>
        </ul>
      </details>
    `;

    root.querySelector('[data-act="save"]').addEventListener("click", saveRule);
    root.querySelector('[data-act="reset"]').addEventListener("click", () => renderEditor());
    const history = root.querySelector("#ra-history");
    if (history) history.addEventListener("toggle", () => {
      if (history.open) loadVersions(rule.id);
    });
    const togglePill = root.querySelector('[data-act="toggle-diff"]');
    if (togglePill) togglePill.addEventListener("click", () => {
      const panel = document.getElementById("ra-diff-panel");
      if (panel) panel.hidden = !panel.hidden;
    });
    root.querySelectorAll("[data-revert]").forEach((btn) => {
      btn.addEventListener("click", (e) => {
        e.preventDefault();
        revertField(rule, btn.getAttribute("data-revert"));
      });
    });
  }

  function revertField(rule, field) {
    if (!rule.global_parent) return;
    const value = rule.global_parent[field];
    if (!confirm(`Revert "${field}" to global value (${formatValue(value)})?`)) return;
    setStatus(`Reverting ${field} to global…`, "info");
    fetchJson(`/portal/api/admin/rules/${rule.id}`, {
      method: "PUT",
      body: JSON.stringify({ [field]: value }),
    }).then((updated) => {
      const idx = state.rules.findIndex((r) => r.id === rule.id);
      if (idx >= 0) state.rules[idx] = updated;
      state.selectedId = updated.id;
      setStatus(`Reverted ${field}. Recompute: ${updated.recompute || "queued"}.`, "info");
      renderList();
      renderEditor();
    }).catch((err) => {
      setStatus(`Revert failed: ${err && err.message ? err.message : err}`, "error");
    });
  }

  function numField(name, label, value, hint, diffs) {
    const v = value == null ? "" : String(value);
    const dot = diffs ? diffDot(name, diffs) : "";
    const revert = diffs ? revertButton(name, diffs) : "";
    return `
      <div class="ra-field">
        <label>${escHtml(label)}${dot}${revert}</label>
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

  // ----- M15: rule version history -----------------------------------------

  function loadVersions(ruleId) {
    const list = document.getElementById("ra-history-list");
    if (!list) return;
    list.innerHTML = `<li class="ra-empty">Loading history…</li>`;
    fetchJson(`/portal/api/admin/rules/${ruleId}/versions`)
      .then((data) => renderVersions(data.versions || [], ruleId))
      .catch((err) => {
        list.innerHTML = `<li class="ra-empty">Could not load history: ${escHtml(
          err && err.message ? err.message : err
        )}</li>`;
      });
  }

  function renderVersions(versions, ruleId) {
    const list = document.getElementById("ra-history-list");
    if (!list) return;
    if (!versions.length) {
      list.innerHTML = `<li class="ra-empty">No prior versions yet — this rule hasn't been edited via the admin UI.</li>`;
      return;
    }
    list.innerHTML = versions.map((v) => {
      const summary = (v.summary_fields || []).slice(0, 6).join(", ");
      const editor = v.edited_by_user_id ? `user #${v.edited_by_user_id}` : "system";
      return `
        <li class="ra-history-item" data-version="${v.version}">
          <div class="ra-history-version">v${v.version}</div>
          <div>
            <div class="ra-history-meta">${escHtml(v.edited_at || "")} · ${escHtml(editor)}</div>
            <div class="ra-history-fields">${escHtml(summary)}</div>
          </div>
          <button type="button" class="ra-history-revert" data-act="revert">Revert to this</button>
        </li>
      `;
    }).join("");
    list.querySelectorAll('[data-act="revert"]').forEach((btn) => {
      btn.addEventListener("click", () => {
        const li = btn.closest("[data-version]");
        const version = parseInt(li.getAttribute("data-version"), 10);
        if (!confirm(`Revert this rule to version v${version}? This will queue a recompute.`)) return;
        setStatus(`Reverting to v${version}…`, "info");
        fetchJson(`/portal/api/admin/rules/${ruleId}/revert/${version}`, { method: "POST" })
          .then((updated) => {
            const idx = state.rules.findIndex((r) => r.id === ruleId);
            if (idx >= 0) state.rules[idx] = updated;
            state.selectedId = updated.id;
            setStatus(`Reverted to v${version}. Recompute: ${updated.recompute || "queued"}.`, "info");
            renderList();
            renderEditor();
            // Re-open history so the user sees the new audit row.
            const history = document.getElementById("ra-history");
            if (history) { history.open = true; loadVersions(updated.id); }
          })
          .catch((err) => {
            setStatus(`Revert failed: ${err && err.message ? err.message : err}`, "error");
          });
      });
    });
  }

  // ----- M13: supersession tab ---------------------------------------------

  function loadSupersession() {
    return fetchJson("/portal/api/admin/supersession")
      .then((data) => {
        state.supersession.pairs = data.pairs || [];
        state.supersession.loaded = true;
        renderSupersession();
      })
      .catch((err) => {
        const tbody = document.getElementById("ra-sup-rows");
        if (tbody) tbody.innerHTML = `<tr><td colspan="4" class="ra-empty">${escHtml(
          err && err.message === "forbidden"
            ? "ADMIN required to view supersession map."
            : `Failed to load supersession: ${err && err.message ? err.message : err}`
        )}</td></tr>`;
      });
  }

  function renderSupersession() {
    const tbody = document.getElementById("ra-sup-rows");
    if (!tbody) return;
    if (!state.supersession.pairs.length) {
      tbody.innerHTML = `<tr><td colspan="4" class="ra-empty">No supersession pairs configured.</td></tr>`;
      return;
    }
    const isGlobalTenant = state.tenantId === "global";
    tbody.innerHTML = state.supersession.pairs.map((p) => {
      const scopeCls = p.is_global ? "ra-sup-scope" : "ra-sup-scope is-tenant";
      const scopeLabel = p.is_global ? "global" : "tenant";
      const canDelete = !p.is_global && !isGlobalTenant;
      return `
        <tr data-pair-id="${p.id}">
          <td>${escHtml(p.prev_kind)}</td>
          <td>${escHtml(p.new_kind)}</td>
          <td><span class="${scopeCls}">${scopeLabel}</span></td>
          <td>
            <button type="button" class="ra-sup-delete" data-act="delete"${canDelete ? "" : " disabled"} title="${canDelete ? "Delete tenant pair" : "Globals are read-only"}">Delete</button>
          </td>
        </tr>
      `;
    }).join("");
    tbody.querySelectorAll("[data-act='delete']").forEach((btn) => {
      btn.addEventListener("click", () => {
        const tr = btn.closest("[data-pair-id]");
        const id = parseInt(tr.getAttribute("data-pair-id"), 10);
        const pair = state.supersession.pairs.find((x) => x.id === id);
        if (!pair) return;
        if (!confirm(`Delete supersession pair ${pair.prev_kind} → ${pair.new_kind}?`)) return;
        fetchJson(`/portal/api/admin/supersession/${id}`, { method: "DELETE" })
          .then(loadSupersession)
          .catch((err) => {
            const msg = document.getElementById("ra-sup-msg");
            if (msg) msg.textContent = `Delete failed: ${err && err.message ? err.message : err}`;
          });
      });
    });
  }

  function bindSupersessionForm() {
    const form = document.getElementById("ra-sup-form");
    if (!form) return;
    form.addEventListener("submit", (e) => {
      e.preventDefault();
      const prev = document.getElementById("ra-sup-prev");
      const next = document.getElementById("ra-sup-new");
      const msg = document.getElementById("ra-sup-msg");
      if (!prev || !next || !msg) return;
      const body = JSON.stringify({
        prev_kind: prev.value.trim(),
        new_kind: next.value.trim(),
      });
      msg.textContent = "Saving…";
      fetchJson("/portal/api/admin/supersession", { method: "POST", body })
        .then(() => {
          prev.value = ""; next.value = "";
          msg.textContent = "Pair added.";
          loadSupersession();
        })
        .catch((err) => {
          msg.textContent = err && err.message === "forbidden"
            ? "Cannot add supersession pairs in the global tenant."
            : `Add failed: ${err && err.message ? err.message : err}`;
        });
    });
  }

  function switchTab(tab) {
    state.activeTab = tab;
    document.querySelectorAll("[data-tab]").forEach((el) => {
      const isActive = el.getAttribute("data-tab") === tab;
      el.classList.toggle("active", isActive);
      el.setAttribute("aria-selected", isActive ? "true" : "false");
    });
    document.querySelectorAll("[data-tab-pane]").forEach((el) => {
      el.hidden = el.getAttribute("data-tab-pane") !== tab;
    });
    if (tab === "supersession" && !state.supersession.loaded) {
      loadSupersession();
    }
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
    document.querySelectorAll("[data-tab]").forEach((el) => {
      el.addEventListener("click", () => switchTab(el.getAttribute("data-tab")));
    });
    bindSupersessionForm();
  }

  document.addEventListener("DOMContentLoaded", () => {
    bind();
    refresh();
  });
})();
