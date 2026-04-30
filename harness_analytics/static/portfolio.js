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
    statusMixBucket: "pending",
    // Allowance Analytics v2: page-level layout + analytics-window state.
    // ``tab`` selects the active tab (Overview/Charts/Allowance/Matters).
    // The four ``aa*`` keys track the recency-window controls on the
    // Allowance tab; they're applied to ``refresh()`` so the API can
    // compute the cohort-windowed KPIs / cohort trend / breakdowns.
    tab: "overview",
    aaCohortAxis: "filing",
    aaRecency: "5y",
    aaCustomStart: "",
    aaCustomEnd: "",
  };

  // Quick-preset definitions for the rail above the chip filters. Each
  // preset just sets one or more chip-equivalent params and re-fetches —
  // power-user shortcut for the high-frequency views, no new server-side
  // semantics. The match function decides which preset is currently active
  // (so we can show a highlighted state when the URL matches verbatim).
  const QUICK_PRESETS = [
    {
      key: "open",
      label: "Open",
      apply() { setParam("hasOpenDeadlines", "true"); setParam("status", null); setParam("dueWithin", null); },
      isActive(p) { return p.get("hasOpenDeadlines") === "true" && !p.get("dueWithin"); },
    },
    {
      key: "closed",
      label: "Closed",
      apply() { setParam("status", "150|161"); setParam("hasOpenDeadlines", null); setParam("dueWithin", null); },
      isActive(p) {
        const s = p.get("status") || "";
        const set = new Set(s.split("|"));
        return set.has("150") && set.has("161") && set.size === 2;
      },
    },
    {
      key: "deadlines",
      label: "Has open deadlines",
      apply() { setParam("hasOpenDeadlines", "true"); setParam("dueWithin", "60"); setParam("status", null); },
      isActive(p) { return p.get("hasOpenDeadlines") === "true" && p.get("dueWithin") === "60"; },
    },
    {
      key: "allowed",
      label: "Allowed",
      apply() { setParam("status", "150"); setParam("hasOpenDeadlines", null); setParam("dueWithin", null); },
      isActive(p) { return p.get("status") === "150"; },
    },
    {
      key: "abandoned",
      label: "Abandoned",
      apply() { setParam("status", "161"); setParam("hasOpenDeadlines", null); setParam("dueWithin", null); },
      isActive(p) { return p.get("status") === "161"; },
    },
    {
      key: "last5y",
      label: "Last 5y filings",
      apply() {
        const now = new Date();
        const from = new Date(now.getFullYear() - 5, now.getMonth(), now.getDate());
        setParam("filingFrom", from.toISOString().slice(0, 10));
        setParam("filingTo", null);
      },
      isActive(p) {
        const ff = p.get("filingFrom");
        if (!ff) return false;
        const now = new Date();
        const expected = new Date(now.getFullYear() - 5, now.getMonth(), now.getDate())
          .toISOString().slice(0, 10);
        return ff === expected;
      },
    },
  ];

  // ---------------------------------------------------------------------
  // Boot
  // ---------------------------------------------------------------------
  document.addEventListener("DOMContentLoaded", init);

  function init() {
    renderHead();
    renderFilterChips();
    hydrateSearchFromUrl();
    renderQuickPresets();
    renderTabs();
    renderAaSubBar();
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

    document.querySelectorAll("#status-mix-tabs .status-tab").forEach((btn) => {
      btn.addEventListener("click", () => {
        const bucket = btn.getAttribute("data-bucket");
        if (!bucket || bucket === state.statusMixBucket) return;
        state.statusMixBucket = bucket;
        renderDonut();
      });
    });

    // Top-level tab nav (Overview/Charts/Allowance/Matters).
    document.querySelectorAll("#page-tabs .tab").forEach((btn) => {
      btn.addEventListener("click", () => {
        const name = btn.getAttribute("data-tab");
        if (name) setActiveTab(name);
      });
    });

    // Quick-preset rail: each preset just nudges chip-equivalent params.
    document.getElementById("quick-presets").addEventListener("click", (e) => {
      const btn = e.target.closest(".quick-preset");
      if (!btn) return;
      const key = btn.getAttribute("data-preset");
      const preset = QUICK_PRESETS.find((p) => p.key === key);
      if (!preset) return;
      preset.apply();
      state.page = 1;
      renderQuickPresets();
      refresh();
    });

    // Allowance Analytics sub-bar: cohort axis (segs) + recency window
    // (pills) auto-apply on click — the previous "stage and click Apply"
    // model surprised users (the highlight changed but the numbers didn't).
    // Custom date pair is the one place we still require a manual Apply
    // because typing into a date input fires multiple change events.
    function applyAaParams() {
      setParam("cohortAxis", state.aaCohortAxis === "filing" ? null : state.aaCohortAxis);
      setParam("recency", state.aaRecency === "5y" ? null : state.aaRecency);
      setParam("customStart", state.aaRecency === "custom" ? state.aaCustomStart || null : null);
      setParam("customEnd", state.aaRecency === "custom" ? state.aaCustomEnd || null : null);
      refresh();
    }
    document.getElementById("aa-axis-segs").addEventListener("click", (e) => {
      const btn = e.target.closest(".seg");
      if (!btn) return;
      const axis = btn.getAttribute("data-axis");
      if (!axis || axis === state.aaCohortAxis) return;
      state.aaCohortAxis = axis;
      renderAaSubBar();
      applyAaParams();
    });
    document.getElementById("aa-recency-pills").addEventListener("click", (e) => {
      const btn = e.target.closest(".aa-pill");
      if (!btn) return;
      const r = btn.getAttribute("data-recency");
      if (!r || r === state.aaRecency) return;
      const wasCustom = state.aaRecency === "custom";
      state.aaRecency = r;
      renderAaSubBar();
      // "custom" reveals the date pair but does not refetch yet — wait for
      // the user to fill the inputs and click Apply. Switching AWAY from
      // custom (e.g. back to 5y) refetches immediately so the displayed
      // window matches the highlighted pill.
      if (r !== "custom") applyAaParams();
      else if (wasCustom) applyAaParams();
    });
    document.getElementById("aa-custom-start").addEventListener("change", (e) => {
      state.aaCustomStart = e.target.value || "";
    });
    document.getElementById("aa-custom-end").addEventListener("change", (e) => {
      state.aaCustomEnd = e.target.value || "";
    });
    document.getElementById("aa-apply-btn").addEventListener("click", applyAaParams);

    window.addEventListener("popstate", () => {
      hydrateSearchFromUrl();
      renderFilterChips();
      renderQuickPresets();
      renderTabs();
      renderAaSubBar();
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
    // Allowance Analytics v2 layout state.
    const tab = params.get("tab");
    if (tab && ["overview", "charts", "allowance", "extensions", "applicants", "matters"].includes(tab)) {
      state.tab = tab;
    }
    const axis = params.get("cohortAxis");
    if (axis && ["filing", "disposal", "noa"].includes(axis)) {
      state.aaCohortAxis = axis;
    }
    const recency = params.get("recency");
    if (recency && ["3y", "5y", "10y", "all", "custom"].includes(recency)) {
      state.aaRecency = recency;
    }
    state.aaCustomStart = params.get("customStart") || "";
    state.aaCustomEnd = params.get("customEnd") || "";
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
    // Always send the analytics window so the API computes the windowed
    // KPIs / cohort trend / breakdowns for the Allowance tab. The other
    // tabs ignore those response fields, so this is cheap and idempotent.
    params.set("cohortAxis", state.aaCohortAxis);
    params.set("recency", state.aaRecency);
    if (state.aaRecency === "custom") {
      if (state.aaCustomStart) params.set("customStart", state.aaCustomStart);
      if (state.aaCustomEnd) params.set("customEnd", state.aaCustomEnd);
    } else {
      params.delete("customStart");
      params.delete("customEnd");
    }
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
      // Allowance Analytics tab reads from a separately-computed,
      // recency-windowed KPI set so the dashboard band stays all-time
      // (per plan decision: recency only scopes the Allowance tab).
      state.analyticsKpis = data.analyticsKpis || data.kpis || null;
      state.charts = data.charts || null;
      state.lastData = data;
      // #region agent log — DEBUG-MODE: expose last response on window so
      // a console one-liner can dump diagnostics. Removed after we verify.
      try { window.__lastData = data; } catch (e) {}
      // #endregion
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
    renderCtnfResponseSpeed();
    renderFilterChips();
    renderQuickPresets();
    renderTabs();
    renderAllowanceTab();
    renderExtensionsTab();
    renderApplicantTrendsTab();
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
    // Scope badge: when the current selection differs from "all data" we
    // mention the windowed cohort sizes too. Spec §11 polish — counsel
    // wants to see at a glance which numbers the analytics are running on.
    const scope = state.lastData && state.lastData.scope;
    let scopeText = "";
    if (scope && scope.totalInWindow != null && scope.totalInWindow !== total) {
      scopeText = ` · WINDOW ${scope.totalInWindow.toLocaleString()} APPS · ${(scope.closedInWindow || 0).toLocaleString()} CLOSED`;
    }
    meta.textContent = `${total.toLocaleString()} APPLICATIONS${scopeText}${formatted}`;
  }

  // ---------------------------------------------------------------------
  // Tabs (Overview / Charts / Allowance / Matters)
  // ---------------------------------------------------------------------
  function renderTabs() {
    const nav = document.getElementById("page-tabs");
    if (!nav) return;
    nav.querySelectorAll(".tab").forEach((btn) => {
      const isActive = btn.getAttribute("data-tab") === state.tab;
      btn.classList.toggle("active", isActive);
      btn.setAttribute("aria-selected", isActive ? "true" : "false");
    });
    document.querySelectorAll(".tab-pane").forEach((pane) => {
      const isActive = pane.getAttribute("data-tab") === state.tab;
      pane.hidden = !isActive;
    });
  }

  function setActiveTab(name) {
    if (state.tab === name) return;
    state.tab = name;
    setParam("tab", name === "overview" ? null : name);
    renderTabs();
  }

  // ---------------------------------------------------------------------
  // Quick-preset rail (above filter chips)
  // ---------------------------------------------------------------------
  function renderQuickPresets() {
    const host = document.getElementById("quick-presets");
    if (!host) return;
    const params = getParams();
    host.innerHTML = QUICK_PRESETS
      .map((p) => {
        const active = p.isActive(params);
        return `<button type="button" class="quick-preset${active ? " active" : ""}" data-preset="${p.key}">${escapeHtml(p.label)}</button>`;
      })
      .join("");
  }

  // ---------------------------------------------------------------------
  // Allowance Analytics tab — sub-bar + content rendering
  // ---------------------------------------------------------------------
  function renderAllowanceTab() {
    renderAaSubBar();
    renderAaScopeLine();
    renderAaPrimary();
    renderAaTrendChart();
    renderAaSecondary();
    renderAaRejectionCountBreakdown();
    renderAaBreakdowns();
    // #region agent log — DEBUG-MODE diagnostic panel. Renders only when
    // ?debug=1 is in the URL. Dumps the per-cohort _diag block + top-level
    // _diag so we can see exactly what the server returned for cohortTrend
    // and the has_analytics_row distribution. Removed after verification.
    renderAaDebugPanel();
    // #endregion
  }

  // #region agent log
  function renderAaDebugPanel() {
    // Always-on for now (no ?debug=1 gate) — user has been unable to see
    // the gated panel. Will be removed after verification.
    const data = state.lastData || {};
    const trend = data.cohortTrend || [];
    const diag = data._diag || {};
    // Mirror to console so we have a fallback if the DOM panel fails to
    // render. The DEBUG_BUILD_TAG is unique to this commit so the user
    // can tell us if the latest deploy is actually live.
    try {
      console.log("[FAA-DEBUG-2026-04-30-c] _diag:", diag);
      console.log("[FAA-DEBUG-2026-04-30-c] cohortTrend:", trend);
    } catch (e) {}
    // Floats at top of viewport so the user cannot miss it regardless of
    // which tab they're on or how far they've scrolled. Includes a copy
    // button so they can paste the full JSON to me in one click.
    let host = document.getElementById("aa-debug-panel");
    if (!host) {
      host = document.createElement("div");
      host.id = "aa-debug-panel";
      host.style.cssText = "position:fixed;top:8px;left:8px;right:8px;z-index:99999;padding:12px 16px;background:#0f172a;color:#e2e8f0;font-family:ui-monospace,monospace;font-size:11px;border-radius:8px;box-shadow:0 8px 32px rgba(0,0,0,0.4);max-height:80vh;overflow:auto;line-height:1.5;border:2px solid #fbbf24;";
      document.body.appendChild(host);
    }
    const cellL = 'style="padding:4px 8px;text-align:left"';
    const cellR = 'style="padding:4px 8px;text-align:right"';
    const cellW = 'style="padding:4px 8px;text-align:right;color:#fbbf24"';
    const cellN = 'style="padding:4px 8px;text-align:right;color:#f87171"';
    const rows = trend.map((d) => {
      const x = d._diag || {};
      return `<tr>
        <td ${cellL}>${d.year}</td>
        <td ${cellR}>${d.n}</td>
        <td ${cellR}>${d.closed}</td>
        <td ${cellR}>${d.faaPct ?? "—"}%</td>
        <td ${cellR}>${x.allowedClass}</td>
        <td ${cellR}>${x.allowedHarTrue}</td>
        <td ${cellW}>${x.allowedHarFalse}</td>
        <td ${cellN}>${x.allowedHarNone}</td>
        <td ${cellR}>${x.preGuardFaaNum}</td>
        <td ${cellR}>${x.postGuardFaaNum}</td>
        <td ${cellR}>${x.faaExcluded}</td>
      </tr>`;
    }).join("");
    const fullJson = JSON.stringify({
      _diag: diag,
      cohortTrend: trend.map((d) => ({ y: d.year, n: d.n, closed: d.closed, faaPct: d.faaPct, ...d._diag })),
    }, null, 2);
    host.innerHTML = `
      <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:8px">
        <div style="font-weight:700;color:#fbbf24">DEBUG-MODE — cohort_trend diagnostics [build 2026-04-30-c]</div>
        <div>
          <button id="aa-debug-copy" style="background:#fbbf24;color:#0f172a;border:none;padding:4px 12px;border-radius:4px;font-weight:700;cursor:pointer;margin-right:8px">Copy JSON to clipboard</button>
          <button id="aa-debug-close" style="background:#475569;color:white;border:none;padding:4px 12px;border-radius:4px;cursor:pointer">×</button>
        </div>
      </div>
      <div>cohortAxis=<b>${diag.cohortAxis}</b> · preset=${diag.preset} · rowsTotal=${diag.rowsTotal} · rowsInWindow=${diag.rowsInWindow}</div>
      <div>has_analytics_row distribution across windowed rows: True=${diag.harTrue} · <span style="color:#fbbf24">False=${diag.harFalse}</span> · <span style="color:#f87171">None=${diag.harNone}</span></div>
      <div>headline FAA: ${diag.headlineFaaPct}% (${diag.headlineFaaCount}/${diag.headlineFaaDenom}) · excluded=${diag.headlineFaaExcluded}</div>
      <div style="margin-top:6px;font-size:10px;opacity:0.85">'has_analytics_row' in sampleRowKeys? <b style="color:${(diag.sampleRowKeys || []).includes('has_analytics_row') ? '#34d399' : '#f87171'}">${(diag.sampleRowKeys || []).includes('has_analytics_row') ? 'YES' : 'NO'}</b></div>
      <table style="margin-top:12px;border-collapse:collapse;font-size:11px">
        <thead><tr style="background:#1e293b">
          <th style="padding:4px 8px;text-align:left">Year</th>
          <th style="padding:4px 8px">n</th>
          <th style="padding:4px 8px">closed</th>
          <th style="padding:4px 8px">FAA</th>
          <th style="padding:4px 8px">allowedClass</th>
          <th style="padding:4px 8px">har=T</th>
          <th style="padding:4px 8px">har=F</th>
          <th style="padding:4px 8px">har=None</th>
          <th style="padding:4px 8px">preGuard</th>
          <th style="padding:4px 8px">postGuard</th>
          <th style="padding:4px 8px">excluded</th>
        </tr></thead>
        <tbody>${rows}</tbody>
      </table>
      <div style="margin-top:8px;font-size:10px;opacity:0.7">sampleRowKeys: ${(diag.sampleRowKeys || []).join(", ")}</div>
    `;
    const copyBtn = document.getElementById("aa-debug-copy");
    if (copyBtn) {
      copyBtn.addEventListener("click", () => {
        navigator.clipboard.writeText(fullJson).then(
          () => { copyBtn.textContent = "Copied!"; setTimeout(() => { copyBtn.textContent = "Copy JSON to clipboard"; }, 1500); },
          () => { copyBtn.textContent = "Copy failed"; }
        );
      });
    }
    const closeBtn = document.getElementById("aa-debug-close");
    if (closeBtn) closeBtn.addEventListener("click", () => host.remove());
  }
  // #endregion

  function renderAaSubBar() {
    document.querySelectorAll("#aa-axis-segs .seg").forEach((b) => {
      const on = b.getAttribute("data-axis") === state.aaCohortAxis;
      b.classList.toggle("active", on);
      b.setAttribute("aria-pressed", on ? "true" : "false");
    });
    document.querySelectorAll("#aa-recency-pills .aa-pill").forEach((b) => {
      const on = b.getAttribute("data-recency") === state.aaRecency;
      b.classList.toggle("active", on);
      b.setAttribute("aria-pressed", on ? "true" : "false");
    });
    const customRow = document.getElementById("aa-custom-range");
    if (customRow) customRow.hidden = state.aaRecency !== "custom";
    const cs = document.getElementById("aa-custom-start");
    const ce = document.getElementById("aa-custom-end");
    if (cs && cs.value !== state.aaCustomStart) cs.value = state.aaCustomStart || "";
    if (ce && ce.value !== state.aaCustomEnd) ce.value = state.aaCustomEnd || "";
  }

  function renderAaScopeLine() {
    const el = document.getElementById("aa-scope-line");
    if (!el) return;
    const data = state.lastData || {};
    const scope = data.scope || {};
    const rw = data.resolvedWindow || {};
    const axisLabel = ({ filing: "filing date", disposal: "disposal date", noa: "NOA mailed date" })[state.aaCohortAxis] || "filing date";
    const total = state.total || 0;
    const inWindow = scope.totalInWindow || 0;
    const closed = scope.closedInWindow || 0;
    let windowDesc;
    if (rw.start && rw.end) {
      windowDesc = `${rw.start} → ${rw.end}`;
    } else if (rw.preset === "all" || (!rw.start && !rw.end)) {
      windowDesc = "all-time";
    } else if (rw.start) {
      windowDesc = `since ${rw.start}`;
    } else if (rw.end) {
      windowDesc = `through ${rw.end}`;
    } else {
      windowDesc = "all-time";
    }
    el.innerHTML = `Analytics computed over <strong>${inWindow.toLocaleString()}</strong> of ${total.toLocaleString()} filtered apps in window (${closed.toLocaleString()} closed) · cohort axis: ${axisLabel} · window: ${escapeHtml(windowDesc)}.`;
  }

  function renderAaPrimary() {
    const host = document.getElementById("aa-primary");
    if (!host) return;
    const k = state.analyticsKpis;
    if (!k) { host.innerHTML = ""; return; }
    function pct(v) {
      if (v === null || v === undefined) return `<span class="aa-kpi-value muted">—</span>`;
      return `<span class="aa-kpi-value">${v}<span class="aa-kpi-pct">%</span></span>`;
    }
    function delta(v) {
      if (!v) return "";
      const cls = v > 0 ? "up" : "down";
      const arrow = v > 0 ? "▲" : "▼";
      return `<span class="aa-kpi-delta ${cls}">${arrow} ${Math.abs(v).toFixed(1)} pts</span>`;
    }
    const closed = (k.patentedCount || 0) + (k.abandonedCount || 0);
    const chmA = k.chmAllowedNoRce || 0;
    const chmCa = k.chmAllowedWithRce || 0;
    const chmAb = k.chmAbandonedNoChild || 0;
    const faaCount = k.faaCount || 0;
    const faaDenom = k.faaDenom || closed;
    const sctCount = k.singleCtnfCount || 0;
    const sctDenom = k.singleCtnfDenom || faaDenom;
    host.innerHTML = `
      <div class="aa-kpi">
        <div class="aa-kpi-label">Traditional Allowance Rate</div>
        ${pct(k.allowanceRatePct)}
        ${delta(k.allowanceRateDeltaPctPts)}
        <div class="aa-kpi-sub">${(k.patentedCount || 0).toLocaleString()} patented / ${closed.toLocaleString()} closed · USPTO formula (Patented / (Patented + Abandoned))</div>
      </div>
      <div class="aa-kpi">
        <div class="aa-kpi-label">CHM "True" Allowance Rate</div>
        ${pct(k.chmAllowanceRatePct)}
        ${delta(k.chmAllowanceRateDeltaPctPts)}
        <div class="aa-kpi-sub">A=${chmA} · CA=${chmCa} · AB=${chmAb} · excludes strategic abandonments</div>
      </div>
      <div class="aa-kpi">
        <div class="aa-kpi-label">Allowance w/ No Rejections</div>
        ${pct(k.faaPct)}
        ${delta(k.faaDeltaPctPts)}
        <div class="aa-kpi-sub">${faaCount.toLocaleString()} of ${faaDenom.toLocaleString()} allowances · examiner's first action was the NOA (0 OAs, 0 RCEs)</div>
        ${
          k.faaExcluded > 0
            ? `<div class="aa-kpi-warn" title="These applications have a status of Patented or Allowed but no row in application_analytics, so we can't verify whether they had an RCE or Final Rejection. Excluded from the numerator to avoid inflating the rate.">⚠ ${k.faaExcluded.toLocaleString()} allowed app${k.faaExcluded === 1 ? "" : "s"} excluded for incomplete prosecution data</div>`
            : ""
        }
      </div>
      <div class="aa-kpi">
        <div class="aa-kpi-label">Allowance after Single CTNF</div>
        ${pct(k.singleCtnfPct)}
        ${delta(k.singleCtnfDeltaPctPts)}
        <div class="aa-kpi-sub">${sctCount.toLocaleString()} of ${sctDenom.toLocaleString()} allowances · allowed after exactly 1 non-final OA, 0 FRs, 0 RCEs</div>
      </div>
    `;
  }

  // Pure-SVG cohort trend chart. Three polylines (Trad / CHM / FAA) over
  // cohort years; hollow circles + dashed connectors mark "maturing"
  // years where pending matters can still move the rate. Avoids any
  // charting dependency — keeps the codebase JS-dep-free.
  function renderAaTrendChart() {
    const host = document.getElementById("aa-trend-chart");
    if (!host) return;
    const data = state.lastData || {};
    const trend = data.cohortTrend || [];
    // Bottom padding bumped from 36 -> 50 to accommodate the new "n=closed"
    // sub-label under each year tick (data-coverage signal).
    const W = 920, H = 296, padL = 56, padR = 24, padT = 18, padB = 50;
    const innerW = W - padL - padR, innerH = H - padT - padB;
    if (!trend.length) {
      host.innerHTML = `<div class="empty-chart">No cohort data in the current window.</div>`;
      return;
    }
    const xs = trend.map((d) => d.year);
    const xMin = Math.min(...xs), xMax = Math.max(...xs);
    const xRange = Math.max(1, xMax - xMin);
    const x = (year) => padL + ((year - xMin) / xRange) * innerW;
    const y = (pct) => padT + innerH - (Math.max(0, Math.min(100, pct)) / 100) * innerH;

    function buildLine(field, color) {
      // Build the polyline as a series of "M"/"L" segments. A solid
      // segment connects two non-maturing points; a dashed segment
      // connects whenever EITHER endpoint is maturing (so a still-evolving
      // year visually disclaims the line into and out of it).
      const pts = trend.map((d) => ({ year: d.year, value: d[field], maturing: d.maturing }))
        .filter((p) => p.value !== null && p.value !== undefined);
      if (pts.length === 0) return "";
      let solid = `M ${x(pts[0].year)} ${y(pts[0].value)}`;
      const dashedSegs = [];
      for (let i = 1; i < pts.length; i++) {
        const a = pts[i - 1], b = pts[i];
        const seg = `M ${x(a.year)} ${y(a.value)} L ${x(b.year)} ${y(b.value)}`;
        if (a.maturing || b.maturing) {
          dashedSegs.push(seg);
        } else {
          solid += ` L ${x(b.year)} ${y(b.value)}`;
        }
      }
      const dots = pts.map((p) => {
        const cx = x(p.year), cy = y(p.value);
        if (p.maturing) {
          return `<circle cx="${cx}" cy="${cy}" r="4" fill="white" stroke="${color}" stroke-width="2"/>`;
        }
        return `<circle cx="${cx}" cy="${cy}" r="4" fill="${color}"/>`;
      }).join("");
      return `
        <path d="${solid}" stroke="${color}" stroke-width="2" fill="none"/>
        ${dashedSegs.map((s) => `<path d="${s}" stroke="${color}" stroke-width="2" fill="none" stroke-dasharray="4,3" opacity="0.7"/>`).join("")}
        ${dots}
      `;
    }

    // Maturing-cohort shaded background: covers the contiguous run of
    // years that are still evolving so counsel can see at a glance which
    // bars to read with caution.
    const maturingYears = trend.filter((d) => d.maturing).map((d) => d.year);
    let maturingRect = "";
    if (maturingYears.length) {
      const mMin = Math.min(...maturingYears), mMax = Math.max(...maturingYears);
      const left = x(mMin) - 12;
      const right = x(mMax) + 12;
      maturingRect = `<rect x="${left}" y="${padT}" width="${right - left}" height="${innerH}" fill="rgba(148, 163, 184, 0.10)"/>`;
    }

    const yTicks = [0, 25, 50, 75, 100].map((v) =>
      `<g><line x1="${padL}" y1="${y(v)}" x2="${W - padR}" y2="${y(v)}" stroke="#e2e8f0" stroke-width="1"/>` +
      `<text x="${padL - 8}" y="${y(v) + 4}" text-anchor="end" font-family="IBM Plex Mono, ui-monospace, monospace" font-size="10" fill="#64748b">${v}%</text></g>`
    ).join("");

    // Year labels + a small "n=closed" line directly under each year so
    // the survivorship-bias problem is visible at a glance: when 2024
    // shows 100% but n=12, the user can immediately see the FAA bar is
    // built on a tiny early-closing sample. Hover gives the full
    // breakdown including FAA-excluded count if any apps were dropped
    // for missing analytics data.
    const xLabels = trend.map((d) => {
      const cx = x(d.year);
      const label = String(d.year);
      const closedN = d.closed != null ? d.closed : 0;
      const excludedNote = d.faaExcluded
        ? ` · ${d.faaExcluded} excluded from FAA (no analytics row)`
        : "";
      const tip = `${d.year} · ${d.n} apps in cohort · ${closedN} closed${d.maturing ? " · maturing cohort" : ""}${excludedNote}`;
      return `
        <text x="${cx}" y="${H - padB + 18}" text-anchor="middle" font-family="IBM Plex Mono, ui-monospace, monospace" font-size="10" fill="#64748b"><title>${escapeHtml(tip)}</title>${label}</text>
        <text x="${cx}" y="${H - padB + 30}" text-anchor="middle" font-family="IBM Plex Mono, ui-monospace, monospace" font-size="9" fill="${closedN < 25 ? "#b45309" : "#94a3b8"}"><title>${escapeHtml(tip)}</title>n=${closedN}</text>
      `;
    }).join("");

    host.innerHTML = `
      <svg viewBox="0 0 ${W} ${H}" preserveAspectRatio="none">
        ${maturingRect}
        ${yTicks}
        ${buildLine("traditionalPct", "#0f172a")}
        ${buildLine("chmPct",         "#059669")}
        ${buildLine("singleCtnfPct",  "#7c3aed")}
        ${buildLine("faaPct",         "#d97706")}
        ${xLabels}
      </svg>
    `;
  }

  function renderAaSecondary() {
    const host = document.getElementById("aa-secondary-grid");
    if (!host) return;
    const k = state.analyticsKpis;
    if (!k) { host.innerHTML = ""; return; }
    function num(v, unit) {
      if (v === null || v === undefined) return `<div class="aa-scell-value muted">—</div>`;
      return `<div class="aa-scell-value">${v}${unit ? `<span class="aa-scell-unit">${unit}</span>` : ""}</div>`;
    }
    const tta = k.timeToAllowance || {};
    const rce = k.rceIntensity || {};
    const sa = k.strategicAbandonment || {};
    const fy = k.familyYield || {};
    const pen = k.pendency || {};
    const fp = k.foreignPriority || {};
    const cells = [
      {
        label: "Time to Allowance (Median)",
        body: num(tta.medianMonths, "mo"),
        sub: tta.medianMonths != null ? `p25 ${tta.p25Months ?? "—"} · p75 ${tta.p75Months ?? "—"} · n=${tta.n}` : "no allowed apps in window",
      },
      {
        label: "RCE Intensity (Avg / Allowed)",
        body: num(rce.avgRceAmongAllowed, ""),
        sub: rce.pctAllowancesWithRce != null ? `${rce.pctAllowancesWithRce}% of allowances had ≥1 RCE · n=${rce.n}` : "no allowed apps",
      },
      {
        label: "Strategic Abandonment Rate",
        body: num(sa.pct, "%"),
        sub: sa.totalAbandoned ? `${sa.withChild} of ${sa.totalAbandoned} abandoned filed a child` : "no abandonments in window",
      },
      {
        label: "Family Yield (Avg Other Patents)",
        body: num(fy.avg, ""),
        sub: fy.n ? `across ${fy.n} patented apps` : "no patented apps in window",
      },
      {
        label: "Pendency (Median, Pending)",
        body: num(pen.medianMonths, "mo"),
        sub: pen.n ? `${pen.n.toLocaleString()} pending matters` : "no pending matters in window",
      },
      {
        label: "Foreign Priority Share",
        body: num(fp.pct, "%"),
        sub: fp.total ? `${fp.n.toLocaleString()} of ${fp.total.toLocaleString()} apps` : "no apps in window",
      },
    ];
    host.innerHTML = cells.map((c) => `
      <div class="aa-scell">
        <div class="aa-scell-label">${escapeHtml(c.label)}</div>
        ${c.body}
        <div class="aa-scell-sub">${escapeHtml(c.sub)}</div>
      </div>
    `).join("");
  }

  // Distribution of allowances by total rejection count (CTNF + CTFR).
  // Renders a single horizontal stacked bar + a 5-row table whose share
  // column sums to 100% across the buckets. The "0 rejections" bucket
  // mirrors the headline "Allowance w/ No Rejections" KPI; the "1
  // rejection" bucket overlaps with "Allowance after Single CTNF"
  // (modulo solo-CTFR cases which are vanishingly rare in practice).
  function renderAaRejectionCountBreakdown() {
    const host = document.getElementById("aa-by-rejection-count");
    if (!host) return;
    const data = state.lastData || {};
    const rows = data.byRejectionCount || [];
    const totalAllowed = data.rejectionCountTotalAllowed || 0;
    const excluded = data.rejectionCountExcluded || 0;
    if (!rows.length || !rows.some((r) => r.count > 0)) {
      host.innerHTML = `<div class="empty-chart">No allowed apps in window.</div>`;
      return;
    }
    // Color ramp: green (clean) → amber → red (lots of rejections).
    const colors = {
      zero:     "#16a34a",
      one:      "#84cc16",
      two:      "#f59e0b",
      three:    "#f97316",
      fourPlus: "#dc2626",
    };
    const segs = rows
      .filter((r) => r.count > 0)
      .map((r) => `<span class="aa-rc-seg" style="width:${r.sharePct}%;background:${colors[r.key] || "#94a3b8"}" title="${escapeHtml(r.label)}: ${r.count.toLocaleString()} (${r.sharePct}%)"></span>`)
      .join("");
    const tableRows = rows.map((r) => {
      const swatch = `<span class="aa-rc-swatch" style="background:${colors[r.key] || "#94a3b8"}"></span>`;
      const barWidth = Math.max(0, Math.min(80, (r.sharePct || 0) * 0.8));
      return `<tr>
        <td>${swatch}${escapeHtml(r.label)}</td>
        <td class="aa-r">${r.count.toLocaleString()}</td>
        <td class="aa-r"><span class="aa-bd-bar" style="width:${barWidth}px;background:${colors[r.key] || "#94a3b8"};opacity:0.65"></span>${r.sharePct}%</td>
        <td class="aa-r">${r.medianMonths != null ? r.medianMonths : "—"}</td>
      </tr>`;
    }).join("");
    host.innerHTML = `
      <div class="aa-rc-stack">${segs}</div>
      <table class="aa-bd-table">
        <thead><tr><th>Bucket</th><th class="aa-r">Count</th><th class="aa-r">Share</th><th class="aa-r">Mo. Median</th></tr></thead>
        <tbody>${tableRows}</tbody>
        <tfoot>
          <tr>
            <td><strong>Total allowed (classified)</strong></td>
            <td class="aa-r"><strong>${totalAllowed.toLocaleString()}</strong></td>
            <td class="aa-r"><strong>100%</strong></td>
            <td class="aa-r">—</td>
          </tr>
        </tfoot>
      </table>
      ${
        excluded > 0
          ? `<div class="aa-bd-foot">⚠ ${excluded.toLocaleString()} allowed app${excluded === 1 ? "" : "s"} excluded — no <code>application_analytics</code> row, so we can't count their rejections. Shares above are denominated against the classifiable subset.</div>`
          : `<div class="aa-bd-foot">Rejections = non-final OAs + final rejections. RCEs are not counted as rejections (they're procedural). The "0 rejections" bucket equals the headline <em>Allowance w/ No Rejections</em> KPI; the "1 rejection" bucket is dominated by single-CTNF allowances.</div>`
      }
    `;
  }

  function renderAaBreakdowns() {
    const data = state.lastData || {};
    const auHost = document.getElementById("aa-by-art-unit");
    if (auHost) {
      const rows = data.byArtUnit || [];
      if (!rows.length) {
        auHost.innerHTML = `<div class="empty-chart">No closed apps grouped by art unit in window.</div>`;
      } else {
        const totalExcluded = rows.reduce((sum, r) => sum + (r.faaExcluded || 0), 0);
        auHost.innerHTML = `
          <table class="aa-bd-table">
            <thead><tr><th>Art Unit</th><th class="aa-r">Closed</th><th class="aa-r">Trad</th><th class="aa-r">CHM</th><th class="aa-r" title="Allowance with no rejections — examiner's first action was the NOA">FAA</th><th class="aa-r" title="Allowance after exactly 1 non-final OA, 0 FRs, 0 RCEs">1-CTNF</th><th class="aa-r">Mo. Med</th></tr></thead>
            <tbody>
              ${rows.map((r) => {
                const trad = r.tradPct;
                const barWidth = trad != null ? Math.max(0, Math.min(80, trad * 0.8)) : 0;
                // Asterisk + tooltip when this art unit had FAA-eligible apps
                // dropped for missing analytics data; tells the user the FAA
                // value here might be deflated.
                const faaSuffix = r.faaExcluded
                  ? `<span class="aa-bd-warn" title="${r.faaExcluded} allowed app${r.faaExcluded === 1 ? "" : "s"} in this art unit excluded from FAA (no analytics row)">*</span>`
                  : "";
                return `<tr>
                  <td>${escapeHtml(String(r.artUnit))}</td>
                  <td class="aa-r">${r.closed.toLocaleString()}</td>
                  <td class="aa-r">${trad != null ? `<span class="aa-bd-bar" style="width:${barWidth}px"></span>${trad}%` : "—"}</td>
                  <td class="aa-r">${r.chmPct != null ? r.chmPct + "%" : "—"}</td>
                  <td class="aa-r">${r.faaPct != null ? r.faaPct + "%" : "—"}${faaSuffix}</td>
                  <td class="aa-r">${r.singleCtnfPct != null ? r.singleCtnfPct + "%" : "—"}</td>
                  <td class="aa-r">${r.medianMonths != null ? r.medianMonths : "—"}</td>
                </tr>`;
              }).join("")}
            </tbody>
          </table>
          ${
            totalExcluded > 0
              ? `<div class="aa-bd-foot">* ${totalExcluded.toLocaleString()} allowed app${totalExcluded === 1 ? "" : "s"} (across all art units shown) excluded from FAA because their <code>application_analytics</code> row hasn't been computed.</div>`
              : ""
          }
        `;
      }
    }
    const pathHost = document.getElementById("aa-by-path");
    if (pathHost) {
      const rows = data.byPathToAllowance || [];
      const pathExcluded = data.pathExcluded || 0;
      const pathTotalAllowed = data.pathTotalAllowed || 0;
      if (!rows.length || !rows.some((r) => r.count > 0)) {
        pathHost.innerHTML = `<div class="empty-chart">No allowed apps in window.</div>`;
      } else {
        pathHost.innerHTML = `
          <table class="aa-bd-table">
            <thead><tr><th>Path</th><th class="aa-r">Count</th><th class="aa-r">Share</th><th class="aa-r">Mo. Median</th></tr></thead>
            <tbody>
              ${rows.map((r) => {
                const barWidth = r.sharePct != null ? Math.max(0, Math.min(80, r.sharePct * 0.8)) : 0;
                return `<tr>
                  <td>${escapeHtml(r.path)}</td>
                  <td class="aa-r">${r.count.toLocaleString()}</td>
                  <td class="aa-r"><span class="aa-bd-bar" style="width:${barWidth}px"></span>${r.sharePct}%</td>
                  <td class="aa-r">${r.medianMonths != null ? r.medianMonths : "—"}</td>
                </tr>`;
              }).join("")}
            </tbody>
          </table>
          ${
            pathExcluded > 0
              ? `<div class="aa-bd-foot">⚠ ${pathExcluded.toLocaleString()} of ${pathTotalAllowed.toLocaleString()} allowed app${pathTotalAllowed === 1 ? "" : "s"} not classified — no <code>application_analytics</code> row, so we can't tell which path they took. Shares above are denominated against the classifiable subset.</div>`
              : ""
          }
        `;
      }
    }
  }

  // ---------------------------------------------------------------------
  // Extensions tab — per-year extension counts (CTNF/CTFR/CTRS).
  //
  // Drives the stacked-bar chart + breakdown table. Data comes from the
  // backend's ``extensionsByYear`` field (computed by
  // ``extension_analytics.compute_extensions_by_year``). An "extension"
  // is a heuristic: any qualifying response (response/RCE/Notice of
  // Appeal) filed strictly more than 3 months after a CTNF/CTFR or 2
  // months after a Restriction Requirement, bucketed by the year of the
  // response.
  // ---------------------------------------------------------------------
  function renderExtensionsTab() {
    const data = state.lastData && state.lastData.extensionsByYear;
    renderExtensionsChart(data);
    renderExtensionsTable(data);
  }

  function renderExtensionsChart(data) {
    const host = document.getElementById("ext-chart");
    if (!host) return;
    const rows = (data && data.byYear) || [];
    if (!rows.length) {
      host.innerHTML = `<div class="ext-empty">No extensions of time detected in the current selection.</div>`;
      return;
    }
    const W = 920, H = 296, padL = 56, padR = 24, padT = 18, padB = 50;
    const innerW = W - padL - padR, innerH = H - padT - padB;
    const maxTotal = Math.max(1, ...rows.map((r) => r.total || 0));
    // Round the y-axis up to a nice number so the gridlines look clean.
    const niceMax = (() => {
      const candidates = [5, 10, 20, 25, 50, 100, 200, 250, 500, 1000, 2000, 5000];
      for (const c of candidates) { if (c >= maxTotal) return c; }
      return Math.ceil(maxTotal / 1000) * 1000;
    })();
    const yScale = (v) => padT + innerH - (v / niceMax) * innerH;
    const bandW = innerW / rows.length;
    const barW = Math.max(8, Math.min(48, bandW * 0.65));

    const colors = { ctnf: "#2563eb", ctfr: "#e11d48", restriction: "#d97706" };

    const yTicks = [0, 0.25, 0.5, 0.75, 1.0].map((p) => {
      const v = Math.round(niceMax * p);
      const y = yScale(v);
      return `<g><line x1="${padL}" y1="${y}" x2="${W - padR}" y2="${y}" stroke="#e2e8f0" stroke-width="1"/>`
        + `<text x="${padL - 8}" y="${y + 4}" text-anchor="end" font-family="IBM Plex Mono, ui-monospace, monospace" font-size="10" fill="#64748b">${v.toLocaleString()}</text></g>`;
    }).join("");

    const bars = rows.map((r, i) => {
      const cx = padL + bandW * (i + 0.5);
      const xLeft = cx - barW / 2;
      const ctnf = r.ctnf || 0;
      const ctfr = r.ctfr || 0;
      const ctrs = r.restriction || 0;
      // Track running total in value-space, then convert to pixels per
      // segment so per-segment rounding doesn't accumulate gaps.
      let runningValue = 0;
      const segs = [];
      function pushSeg(value, color) {
        if (value <= 0) return;
        const yBottom = yScale(runningValue);
        const yTop = yScale(runningValue + value);
        const h = Math.max(1, yBottom - yTop);
        segs.push(`<rect x="${xLeft}" y="${yTop}" width="${barW}" height="${h}" fill="${color}"/>`);
        runningValue += value;
      }
      pushSeg(ctnf, colors.ctnf);
      pushSeg(ctfr, colors.ctfr);
      pushSeg(ctrs, colors.restriction);

      const tipParts = [];
      if (ctnf) tipParts.push(`CTNF ${ctnf}`);
      if (ctfr) tipParts.push(`CTFR ${ctfr}`);
      if (ctrs) tipParts.push(`Restriction ${ctrs}`);
      const tip = `${r.year}${tipParts.length ? " · " + tipParts.join(" · ") : ""} · Total ${r.total}`;
      const totalLabel = r.total > 0
        ? `<text x="${cx}" y="${yScale(r.total) - 6}" text-anchor="middle" font-family="IBM Plex Mono, ui-monospace, monospace" font-size="10" fill="#475569">${r.total}</text>`
        : "";
      return `<g><title>${escapeHtml(tip)}</title>${segs.join("")}${totalLabel}</g>`;
    }).join("");

    const xLabels = rows.map((r, i) => {
      const cx = padL + bandW * (i + 0.5);
      return `<text x="${cx}" y="${H - padB + 18}" text-anchor="middle" font-family="IBM Plex Mono, ui-monospace, monospace" font-size="10" fill="#64748b">${r.year}</text>`;
    }).join("");

    host.innerHTML = `
      <svg viewBox="0 0 ${W} ${H}" preserveAspectRatio="none">
        ${yTicks}
        ${bars}
        ${xLabels}
      </svg>
    `;
  }

  function renderExtensionsTable(data) {
    const tbody = document.getElementById("ext-tbody");
    const tfoot = document.getElementById("ext-tfoot");
    const sub = document.getElementById("ext-sub");
    if (!tbody || !tfoot) return;
    const rows = (data && data.byYear) || [];
    const totals = (data && data.totals) || { ctnf: 0, ctfr: 0, restriction: 0, total: 0 };
    if (sub) {
      const apps = (data && data.appsContributing) || 0;
      sub.textContent = `YEAR THE LATE RESPONSE WAS FILED · CTNF/CTFR > 3 MONTHS · RESTRICTION > 2 MONTHS · ${apps.toLocaleString()} APP${apps === 1 ? "" : "S"} WITH ≥1 EXTENSION`;
    }
    if (!rows.length) {
      tbody.innerHTML = `<tr><td colspan="5" class="muted" style="padding:18px;text-align:center">No extensions detected.</td></tr>`;
      tfoot.innerHTML = "";
      return;
    }
    function cell(value) {
      const cls = value ? "ext-num" : "ext-num ext-zero";
      return `<td class="${cls}">${value ? value.toLocaleString() : "0"}</td>`;
    }
    tbody.innerHTML = rows.map((r) => `
      <tr>
        <td class="ext-year">${r.year}</td>
        ${cell(r.ctnf)}
        ${cell(r.ctfr)}
        ${cell(r.restriction)}
        <td class="ext-num ext-col-total">${(r.total || 0).toLocaleString()}</td>
      </tr>
    `).join("");
    tfoot.innerHTML = `
      <tr>
        <td>Total</td>
        <td class="ext-num">${(totals.ctnf || 0).toLocaleString()}</td>
        <td class="ext-num">${(totals.ctfr || 0).toLocaleString()}</td>
        <td class="ext-num">${(totals.restriction || 0).toLocaleString()}</td>
        <td class="ext-num">${(totals.total || 0).toLocaleString()}</td>
      </tr>
    `;
  }

  // ---------------------------------------------------------------------
  // Applicant Trends tab — per-year filing volume + YoY growth.
  //
  // Two views:
  //   * by-year  — portfolio totals, one row per filing year. Current year
  //                is YTD and compares to same-period prior year.
  //   * by-app   — top N applicants, last few years as columns + a Δ pill.
  // Backend payload lives at ``state.lastData.applicantTrends`` and is
  // produced by ``portfolio_aggregates.compute_applicant_trends``.
  // ---------------------------------------------------------------------
  function renderApplicantTrendsTab() {
    const data = state.lastData && state.lastData.applicantTrends;
    renderAtYearChart(data);
    renderAtYearTable(data);
    renderAtApplicantTable(data);
  }

  function fmtSignedInt(n) {
    if (n == null) return "—";
    if (n === 0) return "0";
    return (n > 0 ? "+" : "") + n.toLocaleString();
  }
  function fmtSignedPct(n) {
    if (n == null) return "—";
    if (n === 0) return "0.0%";
    return (n > 0 ? "+" : "") + n.toFixed(1) + "%";
  }
  function growthClass(n) {
    if (n == null || n === 0) return "at-delta-flat";
    return n > 0 ? "at-delta-up" : "at-delta-down";
  }

  function renderAtYearChart(data) {
    const host = document.getElementById("at-year-chart");
    if (!host) return;
    const rows = (data && data.byYear) || [];
    if (!rows.length) {
      host.innerHTML = `<div class="ext-empty">No filings detected in the current selection.</div>`;
      return;
    }
    const W = 920, H = 240, padL = 56, padR = 24, padT = 18, padB = 40;
    const innerW = W - padL - padR, innerH = H - padT - padB;
    const maxV = Math.max(1, ...rows.map((r) => r.filings || 0));
    const niceMax = (() => {
      const candidates = [5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000];
      for (const c of candidates) { if (c >= maxV) return c; }
      return Math.ceil(maxV / 1000) * 1000;
    })();
    const yScale = (v) => padT + innerH - (v / niceMax) * innerH;
    const bandW = innerW / rows.length;
    const barW = Math.max(8, Math.min(48, bandW * 0.65));

    const yTicks = [0, 0.25, 0.5, 0.75, 1.0].map((p) => {
      const v = Math.round(niceMax * p);
      const y = yScale(v);
      return `<g><line x1="${padL}" y1="${y}" x2="${W - padR}" y2="${y}" stroke="#e2e8f0" stroke-width="1"/>`
        + `<text x="${padL - 8}" y="${y + 4}" text-anchor="end" font-family="IBM Plex Mono, ui-monospace, monospace" font-size="10" fill="#64748b">${v.toLocaleString()}</text></g>`;
    }).join("");

    const bars = rows.map((r, i) => {
      const cx = padL + bandW * (i + 0.5);
      const xLeft = cx - barW / 2;
      const v = r.filings || 0;
      const yTop = yScale(v);
      const h = Math.max(1, yScale(0) - yTop);
      // Partial (current) year gets a hollow / outlined bar so attorneys
      // visually distinguish "in-progress YTD" from "settled prior year"
      // without reading the legend.
      const fill = r.isPartial ? "#dbeafe" : "#2563eb";
      const stroke = r.isPartial ? "#2563eb" : "transparent";
      const dashAttr = r.isPartial ? ' stroke-dasharray="3 3"' : "";
      const tip = `${r.year}${r.isPartial ? " (YTD)" : ""} · ${v.toLocaleString()} filing${v === 1 ? "" : "s"}`
        + (r.deltaPct != null ? ` · ${fmtSignedPct(r.deltaPct)} ${r.compareLabel || ""}` : "");
      const label = v > 0
        ? `<text x="${cx}" y="${yTop - 6}" text-anchor="middle" font-family="IBM Plex Mono, ui-monospace, monospace" font-size="10" fill="#475569">${v.toLocaleString()}</text>`
        : "";
      return `<g><title>${escapeHtml(tip)}</title>`
        + `<rect x="${xLeft}" y="${yTop}" width="${barW}" height="${h}" fill="${fill}" stroke="${stroke}" stroke-width="1.5"${dashAttr}/>`
        + `${label}</g>`;
    }).join("");

    const xLabels = rows.map((r, i) => {
      const cx = padL + bandW * (i + 0.5);
      const suffix = r.isPartial ? " YTD" : "";
      return `<text x="${cx}" y="${H - padB + 18}" text-anchor="middle" font-family="IBM Plex Mono, ui-monospace, monospace" font-size="10" fill="#64748b">${r.year}${suffix}</text>`;
    }).join("");

    host.innerHTML = `<svg viewBox="0 0 ${W} ${H}" preserveAspectRatio="none">${yTicks}${bars}${xLabels}</svg>`;
  }

  function renderAtYearTable(data) {
    const tbody = document.getElementById("at-year-tbody");
    const sub = document.getElementById("at-year-sub");
    if (!tbody) return;
    const rows = (data && data.byYear) || [];
    if (sub) {
      const total = (data && data.totalApplicantsWithFilings) || 0;
      const asOf = (data && data.asOf) || "";
      sub.textContent = `YEAR THE APPLICATION WAS FILED · ${total.toLocaleString()} APPLICANT${total === 1 ? "" : "S"} IN SELECTION · CURRENT YEAR IS YTD AS OF ${asOf}`;
    }
    if (!rows.length) {
      tbody.innerHTML = `<tr><td colspan="6" class="muted" style="padding:18px;text-align:center">No filings detected.</td></tr>`;
      return;
    }
    tbody.innerHTML = rows.map((r) => {
      const yearLabel = r.isPartial ? `${r.year} <span class="at-ytd-badge">YTD</span>` : String(r.year);
      const prior = r.priorFilings == null ? "—" : r.priorFilings.toLocaleString();
      const delta = fmtSignedInt(r.deltaAbs);
      const pct = fmtSignedPct(r.deltaPct);
      const deltaCls = growthClass(r.deltaAbs);
      const pctCls = growthClass(r.deltaPct);
      return `
        <tr>
          <td class="ext-year">${yearLabel}</td>
          <td class="ext-num">${(r.filings || 0).toLocaleString()}</td>
          <td class="ext-num ${r.priorFilings == null ? "ext-zero" : ""}">${prior}</td>
          <td class="ext-num ${deltaCls}">${delta}</td>
          <td class="ext-num ${pctCls}">${pct}</td>
          <td class="at-col-note">${escapeHtml(r.compareLabel || "—")}</td>
        </tr>
      `;
    }).join("");
  }

  function renderAtApplicantTable(data) {
    const thead = document.getElementById("at-applicant-thead");
    const tbody = document.getElementById("at-applicant-tbody");
    const sub = document.getElementById("at-app-sub");
    if (!thead || !tbody) return;
    const apps = (data && data.byApplicant) || [];
    const yearsShown = (data && data.yearsShown) || [];
    const currentYear = data && data.currentYear;
    if (sub) {
      const total = (data && data.totalApplicantsWithFilings) || 0;
      sub.textContent = `RANKED BY ${currentYear || "MOST RECENT YEAR"} FILINGS · SHOWING TOP ${apps.length} OF ${total.toLocaleString()} · YOY GROWTH ON THE RIGHT`;
    }
    if (!apps.length) {
      thead.innerHTML = "";
      tbody.innerHTML = `<tr><td class="muted" style="padding:18px;text-align:center">No applicants in the current selection.</td></tr>`;
      return;
    }
    const yearHeaders = yearsShown.map((y) => {
      const isCur = y === currentYear;
      return `<th class="ext-col-num">${y}${isCur ? ' <span class="at-ytd-badge">YTD</span>' : ""}</th>`;
    }).join("");
    thead.innerHTML = `
      <tr>
        <th class="at-col-applicant">Applicant</th>
        <th class="ext-col-num">Total</th>
        ${yearHeaders}
        <th class="ext-col-num">Δ</th>
        <th class="ext-col-num">Δ %</th>
      </tr>
    `;
    tbody.innerHTML = apps.map((a) => {
      const cells = (a.perYear || []).map((p) => {
        const v = p.filings || 0;
        const cls = v ? "ext-num" : "ext-num ext-zero";
        return `<td class="${cls}">${v ? v.toLocaleString() : "0"}</td>`;
      }).join("");
      const dCls = growthClass(a.deltaAbs);
      const pCls = growthClass(a.deltaPct);
      return `
        <tr>
          <td class="at-col-applicant" title="${escapeAttr(a.applicant)}">${escapeHtml(a.applicant)}</td>
          <td class="ext-num">${(a.total || 0).toLocaleString()}</td>
          ${cells}
          <td class="ext-num ${dCls}">${fmtSignedInt(a.deltaAbs)}</td>
          <td class="ext-num ${pCls}">${fmtSignedPct(a.deltaPct)}</td>
        </tr>
      `;
    }).join("");
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
          // Click target = open the Allowance Analytics tab so attorneys
          // can drill straight from the dashboard headline number into the
          // recency-windowed cohort trend, breakdowns, and methodology.
          tooltip: tip + " Click to open Allowance Analytics.",
          clickToTab: "allowance",
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
      <div class="kpi${c.clickToTab ? " kpi-clickable" : ""}" data-accent="${accents[i]}"${c.clickToTab ? ` data-click-tab="${c.clickToTab}" role="button" tabindex="0"` : ""}${c.tooltip ? ` title="${escapeAttr(c.tooltip)}"` : ""}>
        <div class="kpi-accent-bar"></div>
        <div class="kpi-label">${escapeHtml(c.label)}</div>
        <div class="kpi-value">${escapeHtml(String(c.value))}${c.unit ? `<span class="kpi-unit">${c.unit}</span>` : ""}</div>
        <div class="kpi-sub ${c.subClass || ""}">${escapeHtml(c.sub)}</div>
        ${c.subExtra ? `<div class="kpi-sub ${c.subExtraClass || ""}">${escapeHtml(c.subExtra)}</div>` : ""}
      </div>
    `).join("");
    grid.querySelectorAll(".kpi-clickable").forEach((card) => {
      const target = card.getAttribute("data-click-tab");
      if (!target) return;
      card.addEventListener("click", () => setActiveTab(target));
      card.addEventListener("keydown", (e) => {
        if (e.key === "Enter" || e.key === " ") {
          e.preventDefault();
          setActiveTab(target);
        }
      });
    });
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
  // CTNF response speed -> allowance chart
  //
  // For each non-final office action (CTNF), the backend already bucketed
  // the days from CTNF mail to the applicant's response and decided
  // whether the next examiner action was an NOA (allowed) or another
  // CTNF/CTFR (rejected). We render one row per bucket: a stacked
  // emerald/slate bar where the emerald share IS the allowance rate, plus
  // a "[pending]" striped tail to communicate cohorts whose verdict
  // hasn't landed yet.
  // ---------------------------------------------------------------------
  function renderCtnfResponseSpeed() {
    const container = document.getElementById("ctnf-speed-chart");
    const legend = document.getElementById("ctnf-speed-legend");
    const sub = document.getElementById("ctnf-speed-sub");
    const data = state.charts && state.charts.ctnfResponseSpeed;
    if (!container || !legend) return;
    if (!data || !data.buckets || !data.buckets.length || !data.totalEvents) {
      container.innerHTML = '<div class="ctnf-empty">No CTNF responses in the current selection.</div>';
      legend.innerHTML = "";
      if (sub) sub.textContent = "PER NON-FINAL OFFICE ACTION";
      return;
    }

    const buckets = data.buckets;
    const maxCount = Math.max(...buckets.map((b) => b.responses)) || 1;

    const rowsHtml = buckets.map((b) => {
      const widthPct = b.responses > 0
        ? Math.max(2, Math.round((b.responses / maxCount) * 100))
        : 0;
      const decided = b.allowed + b.rejected;
      const allowedShare = decided > 0 ? (b.allowed / decided) : 0;
      const allowedPct = (b.allowedPct || 0).toFixed(1);
      const rateClass = decided ? "" : " muted";
      const rateText = decided ? `${allowedPct}%` : "—";
      // Inside each bar, split allowed/rejected proportionally to decided
      // events; pending tags onto the right as a striped tail proportional
      // to its share of total responses in the bucket.
      const totalInBucket = b.responses;
      const allowedW = totalInBucket
        ? Math.round((b.allowed / totalInBucket) * 100)
        : 0;
      const rejectedW = totalInBucket
        ? Math.round((b.rejected / totalInBucket) * 100)
        : 0;
      const pendingW = Math.max(0, 100 - allowedW - rejectedW);
      const med = b.medianDaysResponseToNoa;
      const tipParts = [
        `${b.label}: ${totalInBucket.toLocaleString()} CTNF response${totalInBucket === 1 ? "" : "s"}`,
        `Allowed ${b.allowed.toLocaleString()} · Rejected ${b.rejected.toLocaleString()} · Pending ${b.pending.toLocaleString()}`,
      ];
      if (decided) tipParts.push(`Allowance rate ${allowedPct}% (excludes pending)`);
      if (med != null) tipParts.push(`Median days response → NOA: ${med}`);
      const title = tipParts.join("\n");

      return `
        <div class="ctnf-row" title="${escapeAttr(title)}">
          <div class="ctnf-row-label">${escapeHtml(b.label)}</div>
          <div class="ctnf-row-bar-wrap" style="width:${widthPct}%; max-width:100%;">
            ${allowedW > 0 ? `<div class="ctnf-row-bar-allowed" style="width:${allowedW}%"></div>` : ""}
            ${rejectedW > 0 ? `<div class="ctnf-row-bar-rejected" style="width:${rejectedW}%"></div>` : ""}
            ${pendingW > 0 ? `<div class="ctnf-row-bar-pending" style="width:${pendingW}%"></div>` : ""}
          </div>
          <div class="ctnf-row-rate${rateClass}">${rateText}</div>
          <div class="ctnf-row-count">${totalInBucket.toLocaleString()}</div>
        </div>`;
    }).join("");

    container.innerHTML = rowsHtml;

    if (sub) {
      const med = data.medianDaysToResponse;
      const medText = med != null ? ` · MEDIAN ${med}d TO RESPOND` : "";
      sub.textContent = `${data.totalEvents.toLocaleString()} CTNF EVENTS${medText}`;
    }

    const overall = data.overallAllowedPct || 0;
    const decidedTotal = data.totalAllowed + data.totalRejected;
    legend.innerHTML = `
      <span class="legend-dot"><span class="dot" style="background:var(--emerald-600)"></span>Allowed (${data.totalAllowed.toLocaleString()})</span>
      <span class="legend-dot"><span class="dot" style="background:var(--slate-500, #64748b)"></span>Rejected (${data.totalRejected.toLocaleString()})</span>
      ${data.totalPending ? `<span class="legend-dot"><span class="dot" style="background:var(--slate-200)"></span>Pending (${data.totalPending.toLocaleString()})</span>` : ""}
      <span class="legend-dot legend-stat">Overall <strong>${overall.toFixed(1)}%</strong> allowed${decidedTotal ? ` (n=${decidedTotal.toLocaleString()})` : ""}</span>
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
  // a single expand toggle ("Show N more" / "Show fewer").
  const STATUS_MIX_BASE = 20;

  // Classify a status-mix entry into a top-level bucket. "ended" covers
  // dispositions where prosecution is over (granted patents and dead apps);
  // "pending" is everything still live. The 150/161 codes match the same
  // patented/abandoned semantics used by compute_kpis on the Python side.
  function statusBucket(entry) {
    if (entry.code === 150 || entry.code === 161) return "ended";
    const label = (entry.label || "").toLowerCase();
    if (label.includes("patented") || label.includes("abandoned") ||
        label.includes("expired") || label.includes("issued")) {
      return "ended";
    }
    return "pending";
  }

  function renderDonut() {
    const totalEl = document.getElementById("status-mix-total");
    const legend = document.getElementById("donut-legend");
    const mixAll = (state.charts && state.charts.statusMix) || [];

    const buckets = { pending: [], ended: [] };
    mixAll.forEach((e) => buckets[statusBucket(e)].push(e));

    document.querySelectorAll("#status-mix-tabs .status-tab").forEach((btn) => {
      const b = btn.getAttribute("data-bucket");
      const count = (buckets[b] || []).reduce((acc, e) => acc + e.count, 0);
      const isActive = b === state.statusMixBucket;
      btn.classList.toggle("active", isActive);
      btn.setAttribute("aria-selected", isActive ? "true" : "false");
      btn.dataset.count = String(count);
    });

    const mix = buckets[state.statusMixBucket] || [];
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
