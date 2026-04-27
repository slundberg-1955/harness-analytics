/* Portfolio Explorer — vanilla JS, no build step. */
(function () {
  "use strict";

  // ---------------------------------------------------------------------
  // Column config
  // ---------------------------------------------------------------------
  const COLUMNS = [
    { key: "select",                 label: "",             sortable: false, align: "center", defaultVisible: true,  always: true },
    { key: "applicationNumber",      label: "App No.",      sortable: true,  align: "left",   defaultVisible: true,  render: renderAppNo, css: "mono" },
    { key: "inventionTitle",         label: "Title",        sortable: true,  align: "left",   defaultVisible: true,  render: renderTitle, css: "title" },
    { key: "applicationStatusCode",  label: "Status",       sortable: true,  align: "left",   defaultVisible: true,  render: renderStatus },
    { key: "filingDate",             label: "Filing",       sortable: true,  align: "left",   defaultVisible: true,  render: (r) => fmtDate(r.filingDate), css: "mono" },
    { key: "issueDate",              label: "Issue",        sortable: true,  align: "left",   defaultVisible: true,  render: (r) => fmtDate(r.issueDate), css: "mono" },
    { key: "patentNumber",           label: "Patent No.",   sortable: true,  align: "left",   defaultVisible: true,  render: (r) => r.patentNumber || "—", css: "mono" },
    { key: "groupArtUnit",           label: "Art Unit",     sortable: true,  align: "left",   defaultVisible: true,  render: (r) => r.groupArtUnit || "—", css: "mono" },
    { key: "examinerName",           label: "Examiner",     sortable: true,  align: "left",   defaultVisible: true,  render: (r) => titleCase(r.examinerName) || "—" },
    { key: "assigneeName",           label: "Assignee",     sortable: true,  align: "left",   defaultVisible: true,  render: (r) => titleCase(r.assigneeName) || "—" },
    { key: "isContinuation",         label: "Cont.",        sortable: true,  align: "center", defaultVisible: true,  render: renderTick },
    { key: "nonfinalOaCount",        label: "Nonfinal OAs", sortable: true,  align: "right",  defaultVisible: true,  render: (r) => r.nonfinalOaCount ?? 0, css: "num" },
    { key: "finalOaCount",           label: "Final OAs",    sortable: true,  align: "right",  defaultVisible: true,  render: (r) => r.finalOaCount ?? 0, css: "num" },
    { key: "interviewCount",         label: "Interviews",   sortable: true,  align: "right",  defaultVisible: true,  render: (r) => r.interviewCount ?? 0, css: "num" },
    { key: "rceCount",               label: "RCEs",         sortable: true,  align: "right",  defaultVisible: true,  render: renderRceCount, css: "num" },
    { key: "daysFilingToNoa",        label: "Days→NOA",     sortable: true,  align: "right",  defaultVisible: true,  render: (r) => fmtNumOrDash(r.daysFilingToNoa), css: "num" },
    { key: "daysFilingToIssue",      label: "Days→Issue",   sortable: true,  align: "right",  defaultVisible: true,  render: (r) => fmtNumOrDash(r.daysFilingToIssue), css: "num" },
    // Optional columns surfaced via the picker.
    { key: "customerNumber",         label: "Customer No.", sortable: false, align: "left",   defaultVisible: false, render: (r) => r.customerNumber || "—", css: "mono" },
    { key: "patentClass",            label: "Class",        sortable: false, align: "left",   defaultVisible: false, render: (r) => r.patentClass || "—", css: "mono" },
    { key: "totalSubstantiveOas",    label: "Total Subst. OAs", sortable: false, align: "right", defaultVisible: false, render: (r) => r.totalSubstantiveOas ?? 0, css: "num" },
    { key: "daysFilingToFirstOa",    label: "Days→First OA", sortable: false, align: "right",  defaultVisible: false, render: (r) => fmtNumOrDash(r.daysFilingToFirstOa), css: "num" },
    { key: "firstNoaDate",           label: "First NOA",    sortable: false, align: "left",   defaultVisible: false, render: (r) => fmtDate(r.firstNoaDate), css: "mono" },
    { key: "noaWithin90DaysOfInterview", label: "NOA ≤90d Intvw", sortable: false, align: "center", defaultVisible: false, render: renderBool },
    { key: "daysLastInterviewToNoa", label: "Days Intvw→NOA", sortable: false, align: "right", defaultVisible: false, render: (r) => fmtNumOrDash(r.daysLastInterviewToNoa), css: "num" },
    { key: "hasRestrictionCtrsCount", label: "CTRS",        sortable: false, align: "right",  defaultVisible: false, render: (r) => r.hasRestrictionCtrsCount ?? 0, css: "num" },
    { key: "ifwANeCount",            label: "IFW A.NE",     sortable: false, align: "right",  defaultVisible: false, render: (r) => r.ifwANeCount ?? 0, css: "num" },
    { key: "isJac",                  label: "JAC",          sortable: false, align: "center", defaultVisible: false, render: renderBool },
    { key: "officeName",             label: "Office",       sortable: false, align: "left",   defaultVisible: false, render: (r) => r.officeName || "—" },
    // M7: Prosecution Timeline summary columns. Default visible so the
    // attorney sees "what's next" without opening every matter. Clicking
    // the Next Action label opens the matter timeline tab in a new tab.
    { key: "nextDeadlineDate",       label: "Next Deadline", sortable: true, align: "left",   defaultVisible: true,  render: renderNextDeadline, css: "mono" },
    { key: "openDeadlineCount",      label: "Open Dl.",     sortable: true,  align: "right",  defaultVisible: true,  render: renderOpenCount, css: "num" },
    { key: "overdueDeadlineCount",   label: "Overdue",      sortable: true,  align: "right",  defaultVisible: true,  render: renderOverdueCount, css: "num" },
  ];

  function renderNextDeadline(r) {
    if (!r.nextDeadlineDate) return `<span class="muted">—</span>`;
    const sev = (r.nextDeadlineSeverity || "").toLowerCase();
    const cls = sev === "danger" ? "tone-rose" :
                sev === "warn"   ? "tone-amber" :
                sev === "info"   ? "tone-blue" : "tone-slate";
    const label = r.nextDeadlineLabel ? ` ${escapeHtml(r.nextDeadlineLabel)}` : "";
    const href = r.applicationNumber
      ? `/portal/matter/${encodeURIComponent(r.applicationNumber)}#prosecution-timeline-card`
      : "#";
    return `<a href="${href}" target="_blank" rel="noopener" class="pill ${cls}" style="text-decoration:none;" title="${escapeAttr(r.nextDeadlineLabel || "Open matter timeline")}">${fmtDate(r.nextDeadlineDate)}${label}</a>`;
  }
  function renderOpenCount(r) {
    const n = r.openDeadlineCount || 0;
    return n === 0 ? `<span class="muted">0</span>` : String(n);
  }
  function renderOverdueCount(r) {
    const n = r.overdueDeadlineCount || 0;
    if (n === 0) return `<span class="muted">0</span>`;
    return `<span class="pill tone-rose">${n}</span>`;
  }

  const COLUMN_STORAGE_KEY = "otto.portfolio.columns.v1";
  const SAVED_VIEWS_KEY = "otto.portfolio.savedViews.v1";

  // Each entry has:
  //   key   - URL/query param name (must match portfolio_api `_build_where`)
  //   label - chip label
  //   kind  - "multi" (default, multi-select dropdown w/ counts), "single"
  //           (radio), or "date" (single date input). Used by the popover.
  const FILTERABLE = [
    { key: "status",       label: "Status",       kind: "multi"  },
    { key: "issueYear",    label: "Issue Year",   kind: "multi"  },
    { key: "artUnit",      label: "Art Unit",     kind: "multi"  },
    { key: "examiner",     label: "Examiner",     kind: "multi"  },
    { key: "applicant",    label: "Applicant",    kind: "multi"  },
    { key: "hadInterview", label: "Had Interview",kind: "single" },
    { key: "rceCount",     label: "RCE Count",    kind: "single" },
    { key: "filingFrom",   label: "Filing ≥",     kind: "date"   },
    { key: "filingTo",     label: "Filing ≤",     kind: "date"   },
    // M7: timeline-driven filters.
    { key: "hasOpenDeadlines", label: "Has Open Deadlines", kind: "single" },
    { key: "dueWithin",        label: "Due Within",         kind: "single" },
  ];

  function filterableMeta(key) {
    return FILTERABLE.find((f) => f.key === key) || { key, label: key, kind: "multi" };
  }

  // ---------------------------------------------------------------------
  // State
  // ---------------------------------------------------------------------
  const state = {
    page: 1,
    pageSize: 50,
    rows: [],
    total: 0,
    kpis: null,
    charts: null,
    selected: new Set(),
    focusedIndex: -1,
    visibleColumns: loadVisibleColumns(),
    lastData: null,
    searchDebounce: null,
  };

  // ---------------------------------------------------------------------
  // Boot
  // ---------------------------------------------------------------------
  document.addEventListener("DOMContentLoaded", init);

  function init() {
    renderHead();
    renderFilterChips();
    hydrateSearchFromUrl();
    bindGlobalHandlers();
    refresh();
  }

  function bindGlobalHandlers() {
    document.getElementById("search-input").addEventListener("input", (e) => {
      if (state.searchDebounce) clearTimeout(state.searchDebounce);
      state.searchDebounce = setTimeout(() => {
        setParam("q", e.target.value.trim());
        state.page = 1;
        refresh();
      }, 300);
    });

    document.getElementById("export-csv-btn").addEventListener("click", exportCsv);
    document.getElementById("selection-export-btn").addEventListener("click", exportCsv);

    document.getElementById("columns-btn").addEventListener("click", toggleColumnPicker);
    document.getElementById("saved-views-btn").addEventListener("click", toggleSavedViews);

    document.getElementById("detail-close").addEventListener("click", closeDetail);
    document.getElementById("detail-overlay").addEventListener("click", closeDetail);

    document.getElementById("detail-view-xml").addEventListener("click", () => {
      if (_currentBiblioApp) openXmlModal(_currentBiblioApp.applicationNumber, _currentBiblioApp.title);
    });
    document.getElementById("xml-modal-close").addEventListener("click", closeXmlModal);
    document.getElementById("xml-modal-overlay").addEventListener("click", (e) => {
      // Click outside the inner .xml-modal closes the modal.
      if (e.target.id === "xml-modal-overlay") closeXmlModal();
    });
    document.getElementById("xml-modal-copy").addEventListener("click", copyXmlToClipboard);

    const searchInput = document.getElementById("xml-modal-search-input");
    searchInput.addEventListener("input", (e) => applyXmlSearch(e.target.value));
    searchInput.addEventListener("keydown", (e) => {
      if (e.key === "Enter") {
        e.preventDefault();
        moveXmlMatch(e.shiftKey ? -1 : 1);
      }
    });
    document.getElementById("xml-modal-search-prev").addEventListener("click", () => moveXmlMatch(-1));
    document.getElementById("xml-modal-search-next").addEventListener("click", () => moveXmlMatch(1));

    document.addEventListener("keydown", onKeydown);

    window.addEventListener("popstate", () => {
      hydrateSearchFromUrl();
      renderFilterChips();
      refresh();
    });

    document.addEventListener("click", (e) => {
      const picker = document.getElementById("column-picker");
      const savedViews = document.getElementById("saved-views-menu");
      if (!picker.hidden && !picker.contains(e.target) && e.target.id !== "columns-btn" && !e.target.closest("#columns-btn")) {
        picker.hidden = true;
      }
      if (!savedViews.hidden && !savedViews.contains(e.target) && e.target.id !== "saved-views-btn" && !e.target.closest("#saved-views-btn")) {
        savedViews.hidden = true;
      }
    });
  }

  function hydrateSearchFromUrl() {
    const params = new URLSearchParams(location.search);
    document.getElementById("search-input").value = params.get("q") || "";
    state.page = parseInt(params.get("page") || "1", 10) || 1;
  }

  // ---------------------------------------------------------------------
  // URL / filter params
  // ---------------------------------------------------------------------
  // Multi-select values are joined with this delimiter (instead of `,`) so
  // that values containing commas (e.g. "Charles Schwab & Co., Inc.") survive
  // the round-trip through the URL. We deliberately do NOT fall back to
  // comma-splitting for legacy URLs — splitting a free-text value on `,`
  // produces silent "+1" miss-filters that match thousands of unrelated rows.
  const MULTI_DELIM = "|";
  function splitMulti(raw) {
    if (raw == null || raw === "") return [];
    return String(raw).split(MULTI_DELIM).map((v) => v.trim()).filter(Boolean);
  }
  function getParams() {
    return new URLSearchParams(location.search);
  }
  function setParam(key, value) {
    const p = getParams();
    if (value === null || value === undefined || value === "" || (Array.isArray(value) && value.length === 0)) {
      p.delete(key);
    } else {
      p.set(key, Array.isArray(value) ? value.join(MULTI_DELIM) : String(value));
    }
    const qs = p.toString();
    history.replaceState(null, "", qs ? `?${qs}` : location.pathname);
  }
  function getActiveFilters() {
    const p = getParams();
    const result = {};
    for (const { key } of FILTERABLE) {
      const v = p.get(key);
      if (v !== null && v !== "") result[key] = v;
    }
    const q = p.get("q");
    if (q) result.q = q;
    const sort = p.get("sort");
    if (sort) result.sort = sort;
    const dir = p.get("dir");
    if (dir) result.dir = dir;
    return result;
  }

  // ---------------------------------------------------------------------
  // Data fetching
  // ---------------------------------------------------------------------
  async function refresh() {
    const params = getParams();
    params.set("page", String(state.page));
    params.set("pageSize", String(state.pageSize));
    const url = `/portal/api/portfolio?${params.toString()}`;
    try {
      const resp = await fetch(url, { credentials: "same-origin", headers: { Accept: "application/json" } });
      if (!resp.ok) {
        renderError(`Failed to load portfolio (${resp.status})`);
        return;
      }
      const data = await resp.json();
      state.rows = data.rows || [];
      state.total = data.total || 0;
      state.kpis = data.kpis || null;
      state.charts = data.charts || null;
      state.lastData = data;
      state.focusedIndex = state.rows.length ? 0 : -1;
      renderAll();
    } catch (err) {
      renderError("Network error loading portfolio");
      console.error(err);
    }
  }

  function renderError(msg) {
    document.getElementById("portfolio-tbody").innerHTML =
      `<tr><td colspan="${visibleColumns().length}" style="padding: 24px; text-align:center; color: var(--slate-500)">${escapeHtml(msg)}</td></tr>`;
  }

  // ---------------------------------------------------------------------
  // Render top-level
  // ---------------------------------------------------------------------
  function renderAll() {
    renderHead();
    renderKpis();
    renderBarsChart();
    renderDonut();
    renderSignals();
    renderFilterChips();
    renderBody();
    renderFoot();
    renderMeta();
  }

  function renderMeta() {
    const meta = document.getElementById("page-head-meta");
    const total = state.total;
    const now = state.lastData && state.lastData.rows[0] && state.lastData.rows[0].updatedAt
      ? new Date(state.lastData.rows[0].updatedAt)
      : new Date();
    const formatted = isNaN(now.getTime()) ? "" : ` · UPDATED ${now.toISOString().replace("T", " ").slice(0, 16)} UTC`;
    meta.textContent = `${total.toLocaleString()} APPLICATIONS${formatted}`;
  }

  // ---------------------------------------------------------------------
  // KPI band
  // ---------------------------------------------------------------------
  function renderKpis() {
    const k = state.kpis;
    const grid = document.getElementById("kpi-grid");
    if (!k) { grid.innerHTML = ""; return; }
    const accents = ["blue", "emerald", "violet", "amber", "slate", "rose"];
    const cards = [
      { label: "Total Apps", value: k.totalApps, sub: `${k.patentedCount} patented · ${k.pendingCount} pending` },
      (() => {
        const closed = k.patentedCount + k.abandonedCount;
        const trad = k.allowanceRateDeltaPctPts
          ? `${k.allowanceRateDeltaPctPts > 0 ? "▲" : "▼"} ${Math.abs(k.allowanceRateDeltaPctPts)} vs. prior period`
          : `${k.patentedCount} of ${closed} closed`;
        const a = k.chmAllowedNoRce || 0;
        const ca = k.chmAllowedWithRce || 0;
        const ab = k.chmAbandonedNoChild || 0;
        const chmDen = a + ca + ab;
        const chm = chmDen
          ? `CHM ${k.chmAllowanceRatePct}% · A ${a.toLocaleString()} · CA ${ca.toLocaleString()} · AB ${ab.toLocaleString()}`
          : "CHM —";
        const tip = "Traditional: Patented / (Patented + Abandoned). "
          + "CHM (Carley-Hegde-Marco): (A + CA) / (A + CA + AB) where A = allowed without RCE, "
          + "CA = allowed after \u22651 RCE, AB = abandoned without a Continuation/CIP/Divisional child. "
          + "Allowed = status Patented, NOA Mailed, or Issue Fee Verified.";
        return {
          label: "Allowance Rate",
          value: k.allowanceRatePct,
          unit: "%",
          sub: trad,
          subExtra: chm,
          subExtraClass: "kpi-sub-chm",
          subClass: k.allowanceRateDeltaPctPts > 0 ? "up" : k.allowanceRateDeltaPctPts < 0 ? "down" : "",
          tooltip: tip,
        };
      })(),
      { label: "Avg Days to NOA", value: k.avgDaysToNoa != null ? k.avgDaysToNoa : "—", sub: k.medianDaysToNoa != null ? `median ${k.medianDaysToNoa}` : "—" },
      { label: "Avg OA Count", value: k.avgOaCount, sub: `${k.appsWithAtLeastOneOa} of ${k.totalApps} received ≥ 1 OA` },
      { label: "Interview Rate", value: k.interviewRatePct, unit: "%", sub: `${k.interviewCount} of ${k.totalApps} matters` },
      { label: "RCE Rate", value: k.rceRatePct, unit: "%", sub: `${k.rceCount} RCE${k.rceCount === 1 ? "" : "s"} filed` },
      // M7: Deadlines Due (30d) KPI sourced from the patent_applications
      // view. The "open · overdue" subtitle gives quick context without
      // forcing the attorney to open the inbox.
      {
        label: "Deadlines Due (30d)",
        value: k.deadlinesDue30d != null ? k.deadlinesDue30d : "—",
        sub: `${k.openDeadlines || 0} open · ${k.overdueDeadlines || 0} overdue`,
        subClass: (k.overdueDeadlines || 0) > 0 ? "down" : "",
      },
    ];
    grid.innerHTML = cards.map((c, i) => `
      <div class="kpi" data-accent="${accents[i]}"${c.tooltip ? ` title="${escapeAttr(c.tooltip)}"` : ""}>
        <div class="kpi-accent-bar"></div>
        <div class="kpi-label">${escapeHtml(c.label)}</div>
        <div class="kpi-value">${escapeHtml(String(c.value))}${c.unit ? `<span class="kpi-unit">${c.unit}</span>` : ""}</div>
        <div class="kpi-sub ${c.subClass || ""}">${escapeHtml(c.sub)}</div>
        ${c.subExtra ? `<div class="kpi-sub ${c.subExtraClass || ""}">${escapeHtml(c.subExtra)}</div>` : ""}
      </div>
    `).join("");
  }

  // ---------------------------------------------------------------------
  // Days filing → NOA histogram
  // ---------------------------------------------------------------------
  function renderBarsChart() {
    const container = document.getElementById("bars-chart");
    const legend = document.getElementById("bars-legend");
    const hist = state.charts && state.charts.daysToNoaHistogram;

    if (!hist || !hist.bins || !hist.bins.length) {
      container.innerHTML = '<div class="empty-chart">No applications with an NOA in the current selection.</div>';
      legend.innerHTML = "";
      return;
    }

    const bins = hist.bins;
    const maxCount = Math.max(...bins.map((b) => b.count)) || 1;
    const total = hist.totalWithNoa || 0;
    const noNoa = hist.totalWithoutNoa || 0;
    const median = hist.median;
    const mean = hist.mean;

    // Position the median marker on the bar that contains it.
    let medianBinIdx = -1;
    if (median != null) {
      medianBinIdx = bins.findIndex((b) => median >= b.minDays && median <= b.maxDays);
    }

    container.innerHTML = `
      <div class="histogram">
        ${bins.map((b, i) => {
          const widthPct = Math.max(b.count > 0 ? 1 : 0, Math.round((b.count / maxCount) * 100));
          const isMedian = i === medianBinIdx;
          const title = `${b.label} · ${b.count.toLocaleString()} apps (${b.pct}%)`;
          return `
            <div class="hist-row${isMedian ? " hist-row-median" : ""}" title="${escapeAttr(title)}">
              <div class="hist-row-label">${escapeHtml(b.label)}</div>
              <div class="hist-row-bar-wrap">
                <div class="hist-row-bar" style="width:${widthPct}%"></div>
              </div>
              <div class="hist-row-count">${b.count.toLocaleString()}</div>
              <div class="hist-row-pct">${b.pct}%</div>
            </div>`;
        }).join("")}
      </div>`;

    const medianText = median != null
      ? `Median <strong>${median.toLocaleString()}</strong>d`
      : "";
    const meanText = mean != null
      ? `Mean <strong>${mean.toLocaleString()}</strong>d`
      : "";
    legend.innerHTML = `
      <span class="legend-dot"><span class="dot" style="background:var(--blue-600)"></span>${total} apps with NOA</span>
      ${noNoa ? `<span class="legend-dot"><span class="dot" style="background:var(--slate-200)"></span>${noNoa} apps without NOA</span>` : ""}
      ${medianText ? `<span class="legend-dot legend-stat">${medianText}</span>` : ""}
      ${meanText ? `<span class="legend-dot legend-stat">${meanText}</span>` : ""}
    `;
  }

  // ---------------------------------------------------------------------
  // Status mix list (formerly a donut + legend; now legend-only)
  // ---------------------------------------------------------------------
  const TONE_COLORS = {
    emerald: "var(--emerald-600)",
    blue: "var(--blue-600)",
    amber: "var(--amber-600)",
    rose: "var(--rose-600)",
    violet: "var(--violet-600)",
    slate: "var(--slate-400)",
  };
  // Status Mix list: show top STATUS_MIX_BASE rows; the rest collapse behind
  // a single expand toggle.
  const STATUS_MIX_BASE = 10;
  function renderDonut() {
    const totalEl = document.getElementById("status-mix-total");
    const legend = document.getElementById("donut-legend");
    const mix = (state.charts && state.charts.statusMix) || [];
    const total = mix.reduce((acc, e) => acc + e.count, 0);
    if (totalEl) {
      totalEl.textContent = total ? `${total.toLocaleString()} apps` : "";
    }
    if (!total) {
      legend.innerHTML = '<div class="empty-chart">No data.</div>';
      return;
    }

    const rowHtml = (entry, hidden) => {
      const color = TONE_COLORS[entry.tone] || "var(--slate-400)";
      const pct = Math.round((entry.count / total) * 100);
      return `
        <div class="status-row${hidden ? " status-row-extra" : ""}"${hidden ? " hidden" : ""} data-status="${entry.code != null ? entry.code : ""}" title="Filter table to: ${escapeAttr(entry.label)}">
          <span class="status-row-left"><span class="dot" style="background:${color}"></span>${escapeHtml(entry.label)}</span>
          <span class="status-row-count">${entry.count.toLocaleString()}</span>
          <span class="status-row-pct">${pct}%</span>
        </div>`;
    };

    const rowsHtml = mix.map((entry, idx) => rowHtml(entry, idx >= STATUS_MIX_BASE));
    const hiddenCount = Math.max(0, mix.length - STATUS_MIX_BASE);

    const toggleHtml = hiddenCount > 0
      ? `<div class="status-toggle-row"><button type="button" class="status-toggle" aria-expanded="false">Show ${hiddenCount} more <span class="status-toggle-caret" aria-hidden="true">▾</span></button></div>`
      : "";

    legend.innerHTML = rowsHtml.join("") + toggleHtml;

    legend.querySelectorAll(".status-row").forEach((row) => {
      row.addEventListener("click", () => {
        const code = row.getAttribute("data-status");
        if (!code) return;
        setParam("status", code);
        state.page = 1;
        refresh();
      });
    });

    const toggleBtn = legend.querySelector(".status-toggle");
    if (toggleBtn) {
      toggleBtn.addEventListener("click", (ev) => {
        ev.stopPropagation();
        const expanded = toggleBtn.getAttribute("aria-expanded") === "true";
        const next = !expanded;
        legend.querySelectorAll(".status-row-extra").forEach((row) => {
          row.hidden = !next;
        });
        toggleBtn.setAttribute("aria-expanded", next ? "true" : "false");
        toggleBtn.innerHTML = next
          ? `Show fewer <span class="status-toggle-caret" aria-hidden="true">▴</span>`
          : `Show ${hiddenCount} more <span class="status-toggle-caret" aria-hidden="true">▾</span>`;
      });
    }
  }

  // ---------------------------------------------------------------------
  // Prosecution signals
  // ---------------------------------------------------------------------
  function renderSignals() {
    const list = document.getElementById("signals-list");
    const s = (state.charts && state.charts.prosecutionSignals) || null;
    if (!s) { list.innerHTML = ""; return; }
    const rows = [
      { label: "Nonfinal OA count", value: fmtFloat(s.avgNonfinalOa) },
      { label: "Final OA count", value: fmtFloat(s.avgFinalOa) },
      { label: "Interview count", value: fmtFloat(s.avgInterviews) },
      { label: "NOA within 90d of intvw", value: `${s.noaWithin90DaysOfInterviewPct}%` },
      { label: "Continuations", value: `${s.continuationCount} / ${s.continuationTotal}` },
      { label: "JAC matters", value: String(s.jacCount) },
    ];
    list.innerHTML = rows.map((r) => `
      <div class="stat-row"><span class="stat-row-label">${escapeHtml(r.label)}</span><span class="stat-row-val">${escapeHtml(r.value)}</span></div>
    `).join("");
  }

  // ---------------------------------------------------------------------
  // Filter bar
  // ---------------------------------------------------------------------
  function renderFilterChips() {
    const host = document.getElementById("filter-chips");
    const active = getActiveFilters();
    const chips = [];
    FILTERABLE.forEach(({ key, label, kind }) => {
      if (active[key]) {
        const display = formatActiveFilterValue(key, kind, active[key]);
        chips.push(chipHtml(`${label}: ${display}`, true, key));
      } else {
        chips.push(chipHtml(`+ ${label}`, false, key));
      }
    });
    host.innerHTML = chips.join("");
    host.querySelectorAll(".filter-chip").forEach((el) => {
      el.addEventListener("click", (ev) => {
        const target = ev.target.closest(".filter-chip-x");
        const key = el.getAttribute("data-filter-key");
        if (target) {
          ev.stopPropagation();
          clearFilter(key);
          return;
        }
        openFilterPopover(el, key);
      });
    });
  }

  function clearFilter(key) {
    setParam(key, "");
    state.page = 1;
    closeFilterPopover();
    refresh();
  }

  function chipHtml(text, active, key) {
    const x = active
      ? '<span class="filter-chip-x" aria-label="Clear filter"><svg fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2.5" d="M6 18L18 6M6 6l12 12"/></svg></span>'
      : "";
    return `<button type="button" class="filter-chip${active ? " active" : ""}" data-filter-key="${key}">${escapeHtml(text)}${x}</button>`;
  }

  function formatActiveFilterValue(key, kind, raw) {
    if (kind === "single") {
      if (key === "hadInterview") return raw === "true" ? "Yes" : "No";
      if (key === "rceCount") return raw === "gte3" ? "3+" : raw;
      if (key === "hasOpenDeadlines") return raw === "true" ? "Yes" : "No";
      if (key === "dueWithin") {
        if (raw === "overdue") return "Overdue";
        if (raw === "7" || raw === "30" || raw === "90") return `≤ ${raw} days`;
      }
    }
    if (kind === "multi") {
      const parts = splitMulti(raw);
      if (parts.length <= 1) return parts[0] || raw;
      return `${parts[0]} +${parts.length - 1}`;
    }
    return raw;
  }

  // -------------------------------------------------------------------
  // Filter popover (replaces window.prompt() chip flow)
  // -------------------------------------------------------------------

  let _popoverEl = null;
  let _popoverState = null; // { key, kind, anchor, options, selected }

  function closeFilterPopover() {
    if (_popoverEl) {
      _popoverEl.remove();
      _popoverEl = null;
    }
    _popoverState = null;
    document.removeEventListener("mousedown", _onPopoverDocDown, true);
    document.removeEventListener("keydown", _onPopoverKeydown, true);
    window.removeEventListener("resize", _onPopoverReposition);
    window.removeEventListener("scroll", _onPopoverReposition, true);
  }

  function _onPopoverDocDown(ev) {
    if (!_popoverEl) return;
    if (_popoverEl.contains(ev.target)) return;
    if (_popoverState && _popoverState.anchor && _popoverState.anchor.contains(ev.target)) return;
    closeFilterPopover();
  }

  function _onPopoverKeydown(ev) {
    if (ev.key === "Escape") {
      ev.preventDefault();
      closeFilterPopover();
    }
  }

  function _onPopoverReposition() {
    if (_popoverEl && _popoverState && _popoverState.anchor) {
      _positionPopover(_popoverEl, _popoverState.anchor);
    }
  }

  function _positionPopover(el, anchor) {
    const rect = anchor.getBoundingClientRect();
    el.style.position = "fixed";
    const desiredWidth = el.offsetWidth || 280;
    let left = rect.left;
    if (left + desiredWidth > window.innerWidth - 8) {
      left = Math.max(8, window.innerWidth - desiredWidth - 8);
    }
    el.style.left = `${Math.round(left)}px`;
    const top = rect.bottom + 6;
    el.style.top = `${Math.round(top)}px`;
  }

  function openFilterPopover(anchorEl, key) {
    if (_popoverState && _popoverState.key === key) {
      closeFilterPopover();
      return;
    }
    closeFilterPopover();

    const meta = filterableMeta(key);
    const active = getActiveFilters()[key] || "";
    const selected = new Set(splitMulti(active));

    const root = document.createElement("div");
    root.className = "filter-popover";
    root.setAttribute("role", "dialog");
    root.setAttribute("aria-label", `${meta.label} filter`);
    root.innerHTML = `
      <div class="filter-popover-head">
        <div class="filter-popover-title">${escapeHtml(meta.label)}</div>
        <button type="button" class="filter-popover-close" aria-label="Close">
          <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2.5" d="M6 18L18 6M6 6l12 12"/></svg>
        </button>
      </div>
      <div class="filter-popover-body" data-body></div>
      <div class="filter-popover-foot">
        <button type="button" class="filter-popover-btn ghost" data-act="clear">Clear</button>
        <button type="button" class="filter-popover-btn primary" data-act="apply">Apply</button>
      </div>
    `;
    document.body.appendChild(root);
    _popoverEl = root;
    _popoverState = { key, kind: meta.kind, anchor: anchorEl, options: [], selected };

    root.querySelector(".filter-popover-close").addEventListener("click", () => closeFilterPopover());
    root.querySelector('[data-act="clear"]').addEventListener("click", () => clearFilter(key));
    root.querySelector('[data-act="apply"]').addEventListener("click", () => applyPopover());

    _positionPopover(root, anchorEl);
    document.addEventListener("mousedown", _onPopoverDocDown, true);
    document.addEventListener("keydown", _onPopoverKeydown, true);
    window.addEventListener("resize", _onPopoverReposition);
    window.addEventListener("scroll", _onPopoverReposition, true);

    const body = root.querySelector("[data-body]");
    body.innerHTML = `<div class="filter-popover-loading">Loading…</div>`;

    fetch(`/portal/api/portfolio/facets?key=${encodeURIComponent(key)}`, {
      credentials: "same-origin",
      headers: { Accept: "application/json" },
    })
      .then((r) => r.ok ? r.json() : Promise.reject(new Error(`HTTP ${r.status}`)))
      .then((data) => {
        if (!_popoverEl || _popoverState.key !== key) return;
        renderPopoverBody(body, data, meta, selected);
        _positionPopover(_popoverEl, anchorEl);
      })
      .catch(() => {
        if (!_popoverEl || _popoverState.key !== key) return;
        body.innerHTML = `<div class="filter-popover-empty">Couldn't load options.</div>`;
      });
  }

  function applyPopover() {
    if (!_popoverState) return;
    const { key, kind } = _popoverState;
    let value = "";
    if (kind === "date") {
      const input = _popoverEl.querySelector('input[type="date"]');
      value = input ? input.value : "";
    } else {
      const checked = Array.from(_popoverEl.querySelectorAll('input[type="checkbox"]:checked, input[type="radio"]:checked'));
      value = checked.map((c) => c.value).join(MULTI_DELIM);
    }
    closeFilterPopover();
    setParam(key, value || "");
    state.page = 1;
    refresh();
  }

  function renderPopoverBody(body, data, meta, selected) {
    if (meta.kind === "date" || data.kind === "date") {
      const cur = (getActiveFilters()[meta.key] || "").trim();
      body.innerHTML = `
        <div class="filter-popover-date">
          <input type="date" value="${escapeAttr(cur)}" min="${escapeAttr(data.min || "")}" max="${escapeAttr(data.max || "")}" />
          ${data.min || data.max ? `<div class="filter-popover-hint">Range: ${escapeHtml(data.min || "?")} → ${escapeHtml(data.max || "?")}</div>` : ""}
        </div>
      `;
      return;
    }
    const options = (data.options || []);
    if (!options.length) {
      body.innerHTML = `<div class="filter-popover-empty">No values.</div>`;
      return;
    }
    const isSingle = meta.kind === "single";
    const inputType = isSingle ? "radio" : "checkbox";
    const inputName = `flt_${meta.key}`;
    const showSearch = !isSingle && options.length > 8;
    const rows = options.map((o) => {
      const checked = selected.has(String(o.value)) ? "checked" : "";
      return `
        <label class="filter-popover-row" data-search="${escapeAttr((o.label || "").toLowerCase())}">
          <input type="${inputType}" name="${inputName}" value="${escapeAttr(String(o.value))}" ${checked} />
          <span class="filter-popover-row-label">${escapeHtml(o.label || String(o.value))}</span>
          ${typeof o.count === "number" ? `<span class="filter-popover-row-count">${o.count.toLocaleString()}</span>` : ""}
        </label>
      `;
    }).join("");

    body.innerHTML = `
      ${showSearch ? `<div class="filter-popover-search"><input type="text" placeholder="Search…" autocomplete="off" /></div>` : ""}
      <div class="filter-popover-list" role="listbox">${rows}</div>
    `;

    if (showSearch) {
      const searchInput = body.querySelector(".filter-popover-search input");
      searchInput.addEventListener("input", () => {
        const q = searchInput.value.trim().toLowerCase();
        body.querySelectorAll(".filter-popover-row").forEach((row) => {
          const hay = row.getAttribute("data-search") || "";
          row.style.display = !q || hay.indexOf(q) !== -1 ? "" : "none";
        });
      });
      // Autofocus search; helpful for examiner/applicant lists.
      setTimeout(() => searchInput.focus(), 0);
    }

    if (isSingle) {
      // Apply on select for single-choice filters (radio).
      body.querySelectorAll('input[type="radio"]').forEach((r) => {
        r.addEventListener("change", () => applyPopover());
      });
    }
  }

  // ---------------------------------------------------------------------
  // Table head + body
  // ---------------------------------------------------------------------
  function visibleColumns() {
    return COLUMNS.filter((c) => state.visibleColumns.has(c.key) || c.always);
  }

  function loadVisibleColumns() {
    try {
      const raw = localStorage.getItem(COLUMN_STORAGE_KEY);
      if (raw) return new Set(JSON.parse(raw));
    } catch (_) { /* ignore */ }
    return new Set(COLUMNS.filter((c) => c.defaultVisible).map((c) => c.key));
  }

  function saveVisibleColumns() {
    try {
      localStorage.setItem(COLUMN_STORAGE_KEY, JSON.stringify(Array.from(state.visibleColumns)));
    } catch (_) { /* ignore */ }
  }

  function renderHead() {
    const thead = document.getElementById("portfolio-thead");
    const params = getParams();
    const currentSort = params.get("sort") || "applicationNumber";
    const currentDir = params.get("dir") || "asc";
    const cols = visibleColumns();
    document.getElementById("columns-btn-label").textContent = `Columns (${cols.length})`;

    const cells = cols.map((c) => {
      if (c.key === "select") {
        const allSelected = state.rows.length > 0 && state.rows.every((r) => state.selected.has(r.applicationNumber));
        return `<th class="check-cell"><input type="checkbox" id="select-all" ${allSelected ? "checked" : ""} aria-label="Select all visible rows"></th>`;
      }
      const active = currentSort === c.key;
      const classes = [
        c.sortable ? "sortable" : "",
        active ? "active" : "",
        c.align === "right" ? "num" : "",
        c.align === "center" ? "center" : "",
      ].filter(Boolean).join(" ");
      const ariaSort = active ? (currentDir === "asc" ? "ascending" : "descending") : "none";
      const ind = active ? (currentDir === "asc" ? "▲" : "▼") : "↕";
      return `<th class="${classes}" data-col="${c.key}" aria-sort="${ariaSort}" ${c.sortable ? 'role="button"' : ""}>
        ${escapeHtml(c.label)} ${c.sortable ? `<span class="sort-ind">${ind}</span>` : ""}
      </th>`;
    });
    thead.innerHTML = `<tr>${cells.join("")}</tr>`;

    thead.querySelectorAll("th.sortable").forEach((th) => {
      th.addEventListener("click", () => {
        const col = th.getAttribute("data-col");
        const p = getParams();
        const same = p.get("sort") === col;
        const nextDir = same && p.get("dir") === "asc" ? "desc" : "asc";
        setParam("sort", col);
        setParam("dir", nextDir);
        state.page = 1;
        refresh();
      });
    });
    const selAll = document.getElementById("select-all");
    if (selAll) {
      selAll.addEventListener("change", (e) => {
        const checked = e.target.checked;
        state.rows.forEach((r) => {
          if (checked) state.selected.add(r.applicationNumber);
          else state.selected.delete(r.applicationNumber);
        });
        renderBody();
        renderSelectionBar();
      });
    }
  }

  function renderBody() {
    const tbody = document.getElementById("portfolio-tbody");
    if (!state.rows.length) {
      tbody.innerHTML = `<tr><td colspan="${visibleColumns().length}" style="padding: 36px; text-align:center; color: var(--slate-500)">No applications match the current filters.</td></tr>`;
      renderSelectionBar();
      return;
    }
    tbody.innerHTML = state.rows.map((row, i) => {
      const isSelected = state.selected.has(row.applicationNumber);
      const focused = i === state.focusedIndex ? " focused" : "";
      const cells = visibleColumns().map((c) => {
        if (c.key === "select") {
          return `<td class="check-cell"><input type="checkbox" data-select="${escapeAttr(row.applicationNumber)}" ${isSelected ? "checked" : ""} aria-label="Select ${escapeAttr(row.applicationNumber)}"></td>`;
        }
        const content = c.render ? c.render(row) : (row[c.key] ?? "—");
        const cls = [
          c.css || "",
          c.align === "right" ? "num" : "",
          c.align === "center" ? "center" : "",
        ].filter(Boolean).join(" ");
        return `<td class="${cls}">${content === null || content === undefined ? "—" : content}</td>`;
      }).join("");
      return `<tr data-app="${escapeAttr(row.applicationNumber)}" data-idx="${i}" class="${isSelected ? "selected" : ""}${focused}" tabindex="-1">${cells}</tr>`;
    }).join("");

    tbody.querySelectorAll("tr").forEach((tr) => {
      tr.addEventListener("click", (e) => {
        if (e.target.tagName === "INPUT") return;
        const idx = Number(tr.getAttribute("data-idx"));
        state.focusedIndex = idx;
        openDetail(state.rows[idx]);
        renderBody();
      });
    });
    tbody.querySelectorAll('input[type="checkbox"][data-select]').forEach((cb) => {
      cb.addEventListener("change", (e) => {
        const key = cb.getAttribute("data-select");
        if (cb.checked) state.selected.add(key);
        else state.selected.delete(key);
        const tr = cb.closest("tr");
        if (tr) tr.classList.toggle("selected", cb.checked);
        renderSelectionBar();
      });
      cb.addEventListener("click", (e) => e.stopPropagation());
    });
    renderSelectionBar();
  }

  function renderFoot() {
    const label = document.getElementById("table-foot-label");
    const pag = document.getElementById("pagination");
    const total = state.total;
    const start = total ? (state.page - 1) * state.pageSize + 1 : 0;
    const end = Math.min(state.page * state.pageSize, total);
    const selCount = state.selected.size;
    const cap = state.lastData && state.lastData.aggregateRowCap;
    const cappedNote = state.lastData && state.lastData.capped && cap
      ? ` · <span title="Set PORTFOLIO_AGG_ROW_CAP on the service to raise this limit." style="color: var(--amber-600)">capped at ${cap.toLocaleString()}</span>`
      : "";
    label.innerHTML = `Showing <strong>${start}–${end}</strong> of <strong>${total.toLocaleString()}</strong>${cappedNote}${selCount ? ` · ${selCount} selected` : ""}`;

    const pageCount = Math.max(1, Math.ceil(total / state.pageSize));
    const buttons = [];
    buttons.push(`<button ${state.page === 1 ? "disabled" : ""} data-page="${state.page - 1}">‹</button>`);
    const maxButtons = 7;
    let pStart = Math.max(1, state.page - Math.floor(maxButtons / 2));
    let pEnd = Math.min(pageCount, pStart + maxButtons - 1);
    pStart = Math.max(1, pEnd - maxButtons + 1);
    for (let p = pStart; p <= pEnd; p++) {
      buttons.push(`<button class="${p === state.page ? "active" : ""}" data-page="${p}">${p}</button>`);
    }
    buttons.push(`<button ${state.page === pageCount ? "disabled" : ""} data-page="${state.page + 1}">›</button>`);
    pag.innerHTML = buttons.join("");
    pag.querySelectorAll("button[data-page]").forEach((btn) => {
      btn.addEventListener("click", () => {
        if (btn.disabled) return;
        const p = Number(btn.getAttribute("data-page"));
        if (p >= 1 && p <= pageCount && p !== state.page) {
          state.page = p;
          refresh();
        }
      });
    });
  }

  function renderSelectionBar() {
    const bar = document.getElementById("selection-bar");
    const count = state.selected.size;
    if (!count) { bar.hidden = true; return; }
    bar.hidden = false;
    document.getElementById("selection-count").textContent = `${count} selected`;
  }

  // ---------------------------------------------------------------------
  // Cell renderers
  // ---------------------------------------------------------------------
  function renderAppNo(r) {
    const n = r.applicationNumber || "";
    return formatAppNumber(n);
  }
  function renderTitle(r) {
    return escapeHtml(titleCase(r.inventionTitle || "") || "—");
  }
  function renderStatus(r) {
    const tone = r.applicationStatusTone || "slate";
    const label = r.applicationStatusLabel || (r.applicationStatusText || "—");
    const sr = r.applicationStatusText ? `<span class="sr-only"> Status ${r.applicationStatusCode || ""}: ${escapeHtml(r.applicationStatusText)}</span>` : "";
    return `<span class="pill pill-${tone}">${escapeHtml(label)}</span>${sr}`;
  }
  function renderTick(r) {
    return r.isContinuation
      ? '<span class="tick-yes" aria-label="Yes">✓</span>'
      : '<span class="tick-no" aria-label="No">—</span>';
  }
  function renderBool(r, key) {
    const v = key ? r[key] : undefined;
    return v
      ? '<span class="tick-yes" aria-label="Yes">✓</span>'
      : '<span class="tick-no" aria-label="No">—</span>';
  }
  function renderRceCount(r) {
    const n = r.rceCount || 0;
    if (n <= 0) return 0;
    return `<span class="pill pill-amber">${n}</span>`;
  }

  // ---------------------------------------------------------------------
  // Slide-over (biblio)
  // ---------------------------------------------------------------------
  let _lastActiveElement = null;
  // Tracks the application currently open in the slide-over so the
  // "View XML" button knows which file to fetch.
  let _currentBiblioApp = null;
  async function openDetail(row) {
    _lastActiveElement = document.activeElement;
    _currentBiblioApp = {
      applicationNumber: row.applicationNumber,
      title: titleCase(row.inventionTitle || "") || row.applicationNumber || "",
    };
    const overlay = document.getElementById("detail-overlay");
    const panel = document.getElementById("detail-panel");
    overlay.hidden = false;
    panel.classList.add("open");
    panel.setAttribute("aria-hidden", "false");
    document.getElementById("detail-eyebrow").textContent = `Application · ${formatAppNumber(row.applicationNumber) || ""}`;
    document.getElementById("detail-title").textContent = _currentBiblioApp.title;
    const pills = [];
    if (row.applicationStatusLabel) pills.push(`<span class="pill pill-${row.applicationStatusTone || "slate"}">${escapeHtml(row.applicationStatusLabel)}</span>`);
    if (row.applicationStatusCode != null) pills.push(`<span class="pill pill-slate">Status ${row.applicationStatusCode}</span>`);
    if (row.isContinuation) pills.push(`<span class="pill pill-violet">Continuation</span>`);
    document.getElementById("detail-pills").innerHTML = pills.join("");
    const body = document.getElementById("detail-body");
    body.innerHTML = `<div class="empty-chart">Loading biblio…</div>`;
    panel.focus();

    try {
      const resp = await fetch(`/portal/api/applications/${encodeURIComponent(row.applicationNumber)}/biblio`, {
        credentials: "same-origin",
        headers: { Accept: "application/json" },
      });
      if (!resp.ok) {
        body.innerHTML = `<div class="empty-chart">Could not load biblio (${resp.status}).</div>`;
        return;
      }
      const biblio = await resp.json();
      body.innerHTML = renderBiblio(biblio);
    } catch (err) {
      console.error(err);
      body.innerHTML = `<div class="empty-chart">Network error loading biblio.</div>`;
    }
  }

  function closeDetail() {
    const overlay = document.getElementById("detail-overlay");
    const panel = document.getElementById("detail-panel");
    overlay.hidden = true;
    panel.classList.remove("open");
    panel.setAttribute("aria-hidden", "true");
    if (_lastActiveElement && typeof _lastActiveElement.focus === "function") {
      _lastActiveElement.focus();
    }
    _currentBiblioApp = null;
  }

  // ---------------------------------------------------------------------
  // XML viewer modal
  // ---------------------------------------------------------------------
  let _xmlModalLastActive = null;
  let _xmlModalRawText = "";
  // Pretty-printed text we actually render; kept so the search can re-build
  // the highlighted markup without re-fetching.
  let _xmlModalDisplayText = "";
  let _xmlMatchCount = 0;
  let _xmlMatchIndex = -1;

  function isXmlModalOpen() {
    const o = document.getElementById("xml-modal-overlay");
    return o && !o.hidden;
  }

  async function openXmlModal(applicationNumber, title) {
    if (!applicationNumber) return;
    _xmlModalLastActive = document.activeElement;
    const enc = encodeURIComponent(applicationNumber);
    const overlay = document.getElementById("xml-modal-overlay");
    const pre = document.getElementById("xml-modal-pre");
    const eyebrow = document.getElementById("xml-modal-eyebrow");
    const titleEl = document.getElementById("xml-modal-title");
    const dl = document.getElementById("xml-modal-download");

    eyebrow.textContent = `Application · ${formatAppNumber(applicationNumber) || applicationNumber}`;
    titleEl.textContent = title ? `${title} — Bibliographic XML` : "Bibliographic XML";
    dl.href = `/portal/matter/${enc}/xml`;
    const safeName = String(applicationNumber).replace(/[^\w.\-]+/g, "_").slice(0, 80);
    dl.setAttribute("download", `biblio_${safeName}.xml`);

    resetXmlSearch();
    pre.textContent = "Loading XML…";
    _xmlModalRawText = "";
    _xmlModalDisplayText = "";
    overlay.hidden = false;
    document.getElementById("xml-modal").focus();

    try {
      const resp = await fetch(`/portal/matter/${enc}/xml`, { credentials: "same-origin" });
      if (resp.status === 404) {
        pre.textContent = "Raw XML was not stored for this application (ingest may have used --no-xml-raw).";
        return;
      }
      if (!resp.ok) {
        pre.textContent = `Could not load XML (${resp.status}).`;
        return;
      }
      const text = await resp.text();
      _xmlModalRawText = text;
      _xmlModalDisplayText = prettyPrintXml(text);
      pre.textContent = _xmlModalDisplayText;
      // Re-apply any in-flight search term (typed before the fetch resolved).
      const term = document.getElementById("xml-modal-search-input").value;
      if (term) applyXmlSearch(term);
    } catch (err) {
      console.error(err);
      pre.textContent = "Network error loading XML.";
    }
  }

  function closeXmlModal() {
    const overlay = document.getElementById("xml-modal-overlay");
    overlay.hidden = true;
    if (_xmlModalLastActive && typeof _xmlModalLastActive.focus === "function") {
      _xmlModalLastActive.focus();
    }
  }

  // -------------------------- search --------------------------
  function resetXmlSearch() {
    _xmlMatchCount = 0;
    _xmlMatchIndex = -1;
    const input = document.getElementById("xml-modal-search-input");
    if (input) input.value = "";
    updateXmlSearchControls();
  }

  function escapeRegex(s) { return s.replace(/[.*+?^${}()|[\]\\]/g, "\\$&"); }

  function applyXmlSearch(term) {
    const pre = document.getElementById("xml-modal-pre");
    if (!_xmlModalDisplayText) {
      _xmlMatchCount = 0;
      _xmlMatchIndex = -1;
      updateXmlSearchControls();
      return;
    }
    const trimmed = (term || "").trim();
    if (!trimmed) {
      pre.textContent = _xmlModalDisplayText;
      _xmlMatchCount = 0;
      _xmlMatchIndex = -1;
      updateXmlSearchControls();
      return;
    }
    const re = new RegExp(escapeRegex(trimmed), "gi");
    const escaped = escapeHtml(_xmlModalDisplayText);
    // Re-run the regex on the escaped string so the indices align with what
    // we render. Search terms with special chars (<, &) are uncommon here but
    // we escape the term-as-found for safety when emitting <mark>.
    let count = 0;
    const html = escaped.replace(re, (match) => {
      count += 1;
      return `<mark data-xml-match="${count - 1}">${match}</mark>`;
    });
    pre.innerHTML = html;
    _xmlMatchCount = count;
    _xmlMatchIndex = count > 0 ? 0 : -1;
    highlightCurrentMatch();
    updateXmlSearchControls();
  }

  function moveXmlMatch(delta) {
    if (_xmlMatchCount === 0) return;
    _xmlMatchIndex = (_xmlMatchIndex + delta + _xmlMatchCount) % _xmlMatchCount;
    highlightCurrentMatch();
    updateXmlSearchControls();
  }

  function highlightCurrentMatch() {
    const pre = document.getElementById("xml-modal-pre");
    pre.querySelectorAll("mark.xml-match-current").forEach((el) => el.classList.remove("xml-match-current"));
    if (_xmlMatchIndex < 0) return;
    const el = pre.querySelector(`mark[data-xml-match="${_xmlMatchIndex}"]`);
    if (el) {
      el.classList.add("xml-match-current");
      el.scrollIntoView({ block: "center", inline: "nearest", behavior: "smooth" });
    }
  }

  function updateXmlSearchControls() {
    const count = document.getElementById("xml-modal-search-count");
    const prev = document.getElementById("xml-modal-search-prev");
    const next = document.getElementById("xml-modal-search-next");
    if (_xmlMatchCount === 0) {
      const term = document.getElementById("xml-modal-search-input").value.trim();
      count.textContent = term ? "0 / 0" : "";
    } else {
      count.textContent = `${_xmlMatchIndex + 1} / ${_xmlMatchCount}`;
    }
    const disabled = _xmlMatchCount === 0;
    prev.disabled = disabled;
    next.disabled = disabled;
  }

  async function copyXmlToClipboard() {
    const btn = document.getElementById("xml-modal-copy");
    const original = btn.textContent;
    const text = _xmlModalRawText || document.getElementById("xml-modal-pre").textContent || "";
    try {
      await navigator.clipboard.writeText(text);
      btn.textContent = "Copied";
    } catch (err) {
      console.error(err);
      btn.textContent = "Copy failed";
    }
    setTimeout(() => { btn.textContent = original; }, 1500);
  }

  // Lightweight XML pretty-printer. Handles nested tags, text content, comments,
  // <?xml?> processing instructions, and self-closing elements. Falls back to
  // the raw input if anything looks malformed.
  function prettyPrintXml(xml) {
    if (!xml || typeof xml !== "string") return "";
    const trimmed = xml.replace(/>\s+</g, "><").trim();
    if (!trimmed.startsWith("<")) return xml;
    const tokens = trimmed.match(/<!\[CDATA\[[\s\S]*?\]\]>|<!--[\s\S]*?-->|<\?[\s\S]*?\?>|<[^>]+>|[^<]+/g);
    if (!tokens) return xml;
    let depth = 0;
    const out = [];
    for (const tok of tokens) {
      if (tok.startsWith("<?") || tok.startsWith("<!--") || tok.startsWith("<![CDATA[")) {
        out.push("  ".repeat(depth) + tok);
      } else if (tok.startsWith("</")) {
        depth = Math.max(0, depth - 1);
        out.push("  ".repeat(depth) + tok);
      } else if (tok.startsWith("<")) {
        const selfClose = /\/\s*>$/.test(tok);
        out.push("  ".repeat(depth) + tok);
        if (!selfClose) depth += 1;
      } else {
        // Text content: append to the last line so leaf elements stay on
        // one line like <foo>bar</foo>.
        const trimmedText = tok.trim();
        if (!trimmedText) continue;
        if (out.length === 0) {
          out.push(trimmedText);
        } else {
          out[out.length - 1] += trimmedText;
          // The next closing tag should land on the same line too — collapse
          // by decrementing depth for the next iteration's indent.
          // We do this by pre-merging the upcoming </tag> via lookahead:
          // simplest is to just leave the text inline; the closing tag then
          // gets an indented line of its own which is acceptable.
        }
      }
    }
    return out.join("\n");
  }

  function renderBiblio(b) {
    if (!b) return "";
    const abd = b.applicationBibliographicData || {};
    const sections = [];

    const applicantNames = (Array.isArray(b.applicants) ? b.applicants : [])
      .map((a) => (a && a.legalEntityName) ? String(a.legalEntityName).trim() : "")
      .filter(Boolean);
    const inventorNames = (Array.isArray(b.inventors) ? b.inventors : [])
      .map((i) => personName(i && i.name).trim())
      .filter(Boolean);
    const applicantValueHtml = applicantNames.length
      ? applicantNames.map((n) => escapeHtml(n)).join("<br>")
      : "";
    const inventorValueHtml = inventorNames.length
      ? inventorNames.map((n) => escapeHtml(n)).join("<br>")
      : "";

    const bioPairs = [
      ["Application Number", fmtMono(formatAppNumber(b.applicationNumber))],
      ["Confirmation Number", fmtMono(abd.confirmationNumber)],
      ["Attorney Docket", fmtMono(abd.attorneyDocketNumber)],
      ["Customer Number", fmtMono(abd.customerNumber)],
      ["Filing Date", fmtMono(fmtDate(abd.filingDate))],
      ["Status", escapeHtml(abd.applicationStatusText ? `${abd.applicationStatusText}${abd.applicationStatusCode != null ? ` (Code ${abd.applicationStatusCode})` : ""}` : abd.applicationStatusCode != null ? `Code ${abd.applicationStatusCode}` : "")],
      ["Status Date", fmtMono(fmtDate(abd.applicationStatusDate))],
      ["Group Art Unit", fmtMono(abd.groupArtUnit)],
      ["Patent Class / Subclass", fmtMono(joinClass(abd.patentClass, abd.patentSubclass))],
      ["Subject Matter", abd.inventionSubjectMatterType ? escapeHtml(abd.inventionSubjectMatterType) : ""],
      ["Is Public", abd.isPublic === true ? "Yes" : abd.isPublic === false ? "No" : ""],
      [applicantNames.length > 1 ? "Applicants" : "Applicant", applicantValueHtml],
      [inventorNames.length === 1 ? "Inventor" : "Inventors", inventorValueHtml],
    ].filter((p) => p[1]);
    sections.push(dlSection("Bibliographic Data", bioPairs));

    const ex = abd.examinerName || {};
    if (ex.firstName || ex.lastName) {
      const meta = [
        abd.groupArtUnit ? `ART UNIT ${abd.groupArtUnit}` : null,
        abd.patentClass ? `CLASS ${abd.patentClass}` : null,
      ].filter(Boolean).join(" · ");
      sections.push(`
        <section class="detail-section">
          <div class="section-label">Examiner</div>
          <div class="person-card">
            <div class="name">${escapeHtml(personName(ex))}</div>
            ${meta ? `<div class="meta">${escapeHtml(meta)}</div>` : ""}
          </div>
        </section>
      `);
    }

    if (Array.isArray(abd.publications) && abd.publications.length) {
      const html = abd.publications.map((p) => {
        const year = p.publicationDate ? String(new Date(p.publicationDate).getUTCFullYear()) : "";
        const pairs = [
          ["Publication No.", fmtMono(`${year}${year ? "/" : ""}${p.sequenceNumber || ""}${p.kindCode ? ` ${p.kindCode}` : ""}`)],
          ["Publication Date", fmtMono(fmtDate(p.publicationDate))],
          ["Kind Code", fmtMono(p.kindCode)],
        ].filter((x) => x[1]);
        return dlBlock(pairs);
      }).join("");
      sections.push(`<section class="detail-section"><div class="section-label">Publication</div>${html}</section>`);
    }

    if (Array.isArray(b.inventors) && b.inventors.length) {
      const html = b.inventors.map((i) => {
        const meta = [
          i.region || i.city,
          i.countryName || i.countryCode,
          i.postalCode,
        ].filter(Boolean).join(" · ").toUpperCase();
        return `<div class="person-card">
          <div class="name">${escapeHtml(personName(i.name))}</div>
          ${meta ? `<div class="meta">${escapeHtml(meta)}</div>` : ""}
        </div>`;
      }).join("");
      sections.push(`<section class="detail-section"><div class="section-label">Inventors (${b.inventors.length})</div>${html}</section>`);
    }

    if (Array.isArray(b.applicants) && b.applicants.length) {
      const html = b.applicants.map((a) => {
        const lines = [];
        (a.addressLines || []).forEach((l) => l && lines.push(escapeHtml(l)));
        if (a.city || a.countryCode) {
          lines.push(escapeHtml([a.city, a.countryCode].filter(Boolean).join(" · ").toUpperCase()));
        }
        return `<div class="person-card">
          <div class="name">${escapeHtml(a.legalEntityName || "")}</div>
          ${lines.length ? `<div class="meta">${lines.join("<br>")}</div>` : ""}
        </div>`;
      }).join("");
      sections.push(`<section class="detail-section"><div class="section-label">Applicant</div>${html}</section>`);
    }

    // Continuity is the only section that always renders.
    const cont = b.continuity || { parents: [], children: [] };
    const parents = cont.parents || [];
    const children = cont.children || [];
    let contInner;
    if (parents.length || children.length) {
      const pPairs = [];
      parents.slice(0, 3).forEach((p) => {
        pPairs.push(["Parent Application", fmtMono(p.parentApplicationNumber || "")]);
        if (p.description) pPairs.push(["Relationship", escapeHtml(p.description)]);
        if (p.filingDate) pPairs.push(["Parent Filing Date", fmtMono(fmtDate(p.filingDate))]);
      });
      pPairs.push(["Child Applications", children.length
        ? children.map((c) => `<span class="dd mono">${escapeHtml(c.childApplicationNumber || "")}</span>`).join("<br>")
        : `<span class="empty">None</span>`]);
      contInner = dlBlock(pPairs);
    } else {
      contInner = dlBlock([["Parent Application", `<span class="empty">None</span>`], ["Child Applications", `<span class="empty">None</span>`]]);
    }
    sections.push(`<section class="detail-section"><div class="section-label">Continuity</div>${contInner}</section>`);

    if (Array.isArray(b.foreignPriorities) && b.foreignPriorities.length) {
      const html = b.foreignPriorities.map((f) => dlBlock([
        ["Country", escapeHtml([f.countryName, f.countryCode ? `(${f.countryCode})` : ""].filter(Boolean).join(" "))],
        ["Priority Number", fmtMono(f.priorityNumber)],
        ["Priority Date", fmtMono(fmtDate(f.priorityDate))],
      ].filter((x) => x[1]))).join('<div style="height:12px"></div>');
      sections.push(`<section class="detail-section"><div class="section-label">Foreign Priority</div>${html}</section>`);
    }

    if (Array.isArray(b.fileContentHistories) && b.fileContentHistories.length) {
      const items = b.fileContentHistories.map((e, idx) => `
        <div class="timeline-item${idx === 0 ? " latest" : ""}">
          <div class="timeline-date">${escapeHtml(fmtDate(e.transactionDate))}</div>
          <div class="timeline-desc">${escapeHtml(e.transactionDescription || "")}</div>
          ${(e.statusNumber || e.statusDescription)
            ? `<div class="timeline-code">${escapeHtml(["STATUS " + (e.statusNumber || ""), e.statusDescription || ""].filter(Boolean).join(" · ").toUpperCase())}</div>`
            : ""}
        </div>
      `).join("");
      sections.push(`<section class="detail-section"><div class="section-label">File Content History</div><div class="timeline">${items}</div></section>`);
    }

    if (Array.isArray(b.imageFileWrapper) && b.imageFileWrapper.length) {
      const rows = b.imageFileWrapper.map((d) => `
        <tr>
          <td class="mono">${escapeHtml(fmtDate(d.mailRoomDate))}</td>
          <td>${escapeHtml(d.documentDescription || "")}</td>
          <td class="mono">${escapeHtml(d.fileWrapperDocumentCode || "")}</td>
          <td class="mono">${escapeHtml(d.pageQuantity != null ? d.pageQuantity + " pp." : "")}</td>
        </tr>
      `).join("");
      sections.push(`
        <section class="detail-section">
          <div class="section-label">Image File Wrapper · ${b.imageFileWrapper.length} Document${b.imageFileWrapper.length === 1 ? "" : "s"}</div>
          <table class="ifw-table"><thead><tr><th>Date</th><th>Description</th><th>Code</th><th>Pages</th></tr></thead><tbody>${rows}</tbody></table>
        </section>
      `);
    }

    if (b.correspondence) {
      const c = b.correspondence;
      const addressHtml = [c.addressLine1, c.addressLine2, [c.city, c.postalCode].filter(Boolean).join(" ")]
        .filter(Boolean).map(escapeHtml).join("<br>");
      const pairs = [
        ["Firm", c.nameLine1 ? escapeHtml(c.nameLine1) : ""],
        ["Address", addressHtml],
        ["Country", c.countryName ? escapeHtml(c.countryName) : c.countryCode ? escapeHtml(c.countryCode) : ""],
      ].filter((p) => p[1]);
      if (pairs.length) sections.push(dlSection("Correspondence Address", pairs));
    }

    if (Array.isArray(b.attorneys) && b.attorneys.length) {
      const html = b.attorneys.map((a) => {
        const metaParts = [];
        if (a.registrationNumber) metaParts.push(`REG. ${a.registrationNumber}`);
        if (a.phones && a.phones.length) metaParts.push(a.phones[0]);
        if (a.status) metaParts.push(a.status);
        const meta = metaParts.join(" · ").toUpperCase();
        const statusUpper = (a.status || "").toUpperCase();
        const badPill = statusUpper === "INACTIVE" || statusUpper === "SUSPENDED"
          ? ` <span class="pill pill-rose">${escapeHtml(statusUpper)}</span>`
          : "";
        return `<div class="person-card">
          <div class="name">${escapeHtml(personName(a.name))}${badPill}</div>
          ${meta ? `<div class="meta">${escapeHtml(meta)}</div>` : ""}
        </div>`;
      }).join("");
      sections.push(`<section class="detail-section"><div class="section-label">Attorneys of Record (${b.attorneys.length})</div>${html}</section>`);
    }

    return sections.join("");
  }

  function dlSection(title, pairs) {
    return `<section class="detail-section"><div class="section-label">${escapeHtml(title)}</div>${dlBlock(pairs)}</section>`;
  }
  function dlBlock(pairs) {
    const rows = pairs.map(([k, v]) => `<dt class="dt">${escapeHtml(k)}</dt><dd class="dd">${v}</dd>`).join("");
    return `<dl class="dl">${rows}</dl>`;
  }

  // ---------------------------------------------------------------------
  // Column picker
  // ---------------------------------------------------------------------
  function toggleColumnPicker() {
    const picker = document.getElementById("column-picker");
    document.getElementById("saved-views-menu").hidden = true;
    if (!picker.hidden) { picker.hidden = true; return; }
    picker.innerHTML = `
      <h3>Columns</h3>
      ${COLUMNS.filter((c) => !c.always).map((c) => `
        <label>
          <input type="checkbox" data-col-key="${c.key}" ${state.visibleColumns.has(c.key) ? "checked" : ""}>
          ${escapeHtml(c.label)}
        </label>
      `).join("")}
    `;
    picker.hidden = false;
    picker.querySelectorAll("input[type=checkbox]").forEach((cb) => {
      cb.addEventListener("change", () => {
        const key = cb.getAttribute("data-col-key");
        if (cb.checked) state.visibleColumns.add(key);
        else state.visibleColumns.delete(key);
        saveVisibleColumns();
        renderHead();
        renderBody();
      });
    });
  }

  // ---------------------------------------------------------------------
  // Saved views (localStorage, last 5)
  // ---------------------------------------------------------------------
  function getSavedViews() {
    try {
      const raw = localStorage.getItem(SAVED_VIEWS_KEY);
      return raw ? JSON.parse(raw) : [];
    } catch (_) { return []; }
  }
  function setSavedViews(arr) {
    try { localStorage.setItem(SAVED_VIEWS_KEY, JSON.stringify(arr.slice(0, 5))); } catch (_) { /* ignore */ }
  }
  function toggleSavedViews() {
    const menu = document.getElementById("saved-views-menu");
    document.getElementById("column-picker").hidden = true;
    if (!menu.hidden) { menu.hidden = true; return; }
    const views = getSavedViews();
    menu.innerHTML = `
      <h3>Saved Views</h3>
      ${views.length ? views.map((v, i) => `
        <button type="button" data-view-idx="${i}">${escapeHtml(v.name)}</button>
      `).join("") : '<div class="empty-state">No saved views yet.</div>'}
      <div style="border-top:1px solid var(--slate-200); margin-top:8px; padding-top:8px;">
        <button type="button" id="save-current-view">Save current filters…</button>
      </div>
    `;
    menu.hidden = false;
    menu.querySelectorAll("button[data-view-idx]").forEach((btn) => {
      btn.addEventListener("click", () => {
        const v = views[Number(btn.getAttribute("data-view-idx"))];
        if (!v) return;
        history.replaceState(null, "", v.params ? `?${v.params}` : location.pathname);
        hydrateSearchFromUrl();
        renderFilterChips();
        menu.hidden = true;
        refresh();
      });
    });
    const saveBtn = document.getElementById("save-current-view");
    if (saveBtn) {
      saveBtn.addEventListener("click", () => {
        const name = window.prompt("Name this view:");
        if (!name) return;
        const arr = getSavedViews();
        arr.unshift({ name: name.trim(), params: location.search.replace(/^\?/, "") });
        setSavedViews(arr);
        toggleSavedViews(); toggleSavedViews();
      });
    }
  }

  // ---------------------------------------------------------------------
  // CSV export
  // ---------------------------------------------------------------------
  function exportCsv() {
    const params = getParams();
    // CSV endpoint ignores page/pageSize; strip them for clarity.
    params.delete("page");
    params.delete("pageSize");
    window.location.href = `/portal/api/portfolio.csv?${params.toString()}`;
  }

  // ---------------------------------------------------------------------
  // Keyboard shortcuts
  // ---------------------------------------------------------------------
  function onKeydown(e) {
    const panel = document.getElementById("detail-panel");
    const panelOpen = panel.classList.contains("open");
    // While the XML viewer is open, intercept ⌘F / Ctrl+F to focus its
    // in-modal search instead of triggering the browser find UI.
    if (isXmlModalOpen() && (e.metaKey || e.ctrlKey) && e.key.toLowerCase() === "f") {
      e.preventDefault();
      const input = document.getElementById("xml-modal-search-input");
      if (input) { input.focus(); input.select(); }
      return;
    }
    if (e.key === "Escape") {
      if (isXmlModalOpen()) { closeXmlModal(); e.preventDefault(); return; }
      if (panelOpen) { closeDetail(); e.preventDefault(); return; }
      const picker = document.getElementById("column-picker");
      if (!picker.hidden) { picker.hidden = true; e.preventDefault(); return; }
      const sv = document.getElementById("saved-views-menu");
      if (!sv.hidden) { sv.hidden = true; e.preventDefault(); return; }
    }
    if (document.activeElement && (document.activeElement.tagName === "INPUT" || document.activeElement.tagName === "TEXTAREA")) {
      return;
    }
    if (panelOpen) return;

    const rows = state.rows;
    if (!rows.length) return;
    if (e.key === "ArrowDown") {
      state.focusedIndex = Math.min(rows.length - 1, state.focusedIndex + 1);
      renderBody();
      scrollFocusedIntoView();
      e.preventDefault();
    } else if (e.key === "ArrowUp") {
      state.focusedIndex = Math.max(0, state.focusedIndex - 1);
      renderBody();
      scrollFocusedIntoView();
      e.preventDefault();
    } else if (e.key === "Enter") {
      if (state.focusedIndex >= 0) {
        openDetail(rows[state.focusedIndex]);
        e.preventDefault();
      }
    } else if ((e.key === "a" || e.key === "A") && (e.metaKey || e.ctrlKey)) {
      rows.forEach((r) => state.selected.add(r.applicationNumber));
      renderBody();
      e.preventDefault();
    } else if (e.key === "x" || e.key === "X") {
      if (state.focusedIndex >= 0) {
        const key = rows[state.focusedIndex].applicationNumber;
        if (state.selected.has(key)) state.selected.delete(key);
        else state.selected.add(key);
        renderBody();
        e.preventDefault();
      }
    }
  }

  function scrollFocusedIntoView() {
    const tr = document.querySelector(`tbody tr[data-idx="${state.focusedIndex}"]`);
    if (tr && tr.scrollIntoView) tr.scrollIntoView({ block: "nearest" });
  }

  // ---------------------------------------------------------------------
  // Utilities
  // ---------------------------------------------------------------------
  function formatAppNumber(n) {
    const s = String(n || "").replace(/\D/g, "");
    if (s.length >= 2) return `${s.slice(0, 2)}/${s.slice(2)}`.replace(/(\d{3})(\d+)$/, "$1,$2");
    return s || "";
  }
  function fmtDate(v) {
    if (!v) return "—";
    const s = String(v);
    return s.length >= 10 ? s.slice(0, 10) : s;
  }
  function fmtNumOrDash(v) {
    if (v == null) return "—";
    return Number(v).toLocaleString();
  }
  function fmtFloat(v) {
    if (v == null) return "—";
    return Number(v).toFixed(2);
  }
  function fmtMono(v) {
    return v ? `<span class="dd mono" style="display:inline">${escapeHtml(v)}</span>` : "";
  }
  function personName(p) {
    if (!p) return "";
    return [p.firstName, p.middleName, p.lastName].filter(Boolean).join(" ");
  }
  function joinClass(pc, psc) {
    if (pc && psc) return `${pc} / ${psc}`;
    return pc || psc || "";
  }
  function titleCase(s) {
    if (!s) return s;
    const t = String(s);
    if (!/[a-z]/.test(t) || t === t.toLowerCase()) {
      return t.replace(/\b\w/g, (c) => c.toUpperCase());
    }
    return t;
  }
  function escapeHtml(s) {
    if (s == null) return "";
    return String(s).replace(/[&<>"']/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));
  }
  function escapeAttr(s) { return escapeHtml(s); }
})();
