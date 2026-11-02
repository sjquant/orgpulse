"use strict";

const I18N = {
  en: {
    never: "Not synced yet.",
    sync: "Sync now", syncing: "Syncing…", syncDone: "Sync complete",
    syncProgress: (repos, prs) => `${repos} repositories · ${prs} PRs processed`,
    syncConnectionError: "Cannot check sync status. Reconnecting…",
    synced: (at) => `Synced ${at}`,
    groups: "Groups",
    people: "People",
    everyone: "Everyone",
    grain: { week: "Week", month: "Month" },
    last: (span) => `Last ${span}`,
    custom: "Custom range…", from: "Start date", until: "End date", apply: "Apply",
    invalidRange: "Start date must be on or before end date.",
    customComparison: (from, until) => `${from} – ${until} · Compared with the preceding equal-length range`,
    vsPrevious: (span) => `Compared with the previous ${span}`,
    vsTeam: (team) => `Compared with ${team}`,
    metric: {
      merged_per_person: "Merged PRs / person",
      lead_time: "Time to merge",
      size: "PR size",
      review_wait: "First review",
    },
    personMerged: "Merged PRs",
    averageMerged: { week: "Average PRs / week", month: "Average PRs / month" },
    averagePerPerson: { week: "Average PRs / person / week", month: "Average PRs / person / month" },
    partialCounts: "The current week or month shows PRs merged so far, without extrapolation.",
    columnMerged: { week: "Merged/wk", month: "Merged/mo" },
    definition: {
      merged_per_person: "Merged PRs ÷ people ÷ periods, counted by merge date.",
      lead_time: "Median time from opening a PR to merging it.",
      size: "Median lines added plus deleted per merged PR.",
      review_wait: "Median time from ready-for-review to the first review by someone else. Bots are ignored.",
    },
    unit: { week: "/wk", month: "/mo", lines: "lines" },
    chartNote: { week: "per week", month: "per month", median: "median" },
    team: "Team",
    teamValue: (value) => `team ${value}`,
    thisPeriod: "This period",
    previousPeriod: "Previous period",
    inProgress: "in progress",
    noData: "No merged PRs in this range.",
    person: "Person",
    versusTeam: (name) => `${name} vs team`,
    recent: "Recent PRs",
    pr: "PR",
    open: (name) => `Open ${name} →`,
    flagNote: "Well above the team's usual value",
    flagTitle: "Above 1.5× the team median for a person, or the team's 90th percentile for a PR",
  },
  ko: {
    never: "아직 동기화하지 않았습니다.",
    sync: "지금 동기화", syncing: "동기화 중…", syncDone: "동기화 완료",
    syncProgress: (repos, prs) => `${repos}개 저장소 · ${prs}개 PR 처리`,
    syncConnectionError: "동기화 상태를 확인할 수 없습니다. 다시 연결 중…",
    synced: (at) => `${at} 동기화`,
    groups: "그룹",
    people: "사람",
    everyone: "전체",
    grain: { week: "주", month: "월" },
    last: (span) => `최근 ${span}`,
    custom: "사용자 지정…", from: "시작일", until: "종료일", apply: "적용",
    invalidRange: "시작일은 종료일보다 늦을 수 없습니다.",
    customComparison: (from, until) => `${from} – ${until} · 직전 같은 길이 기간 대비`,
    vsPrevious: (span) => `이전 ${span} 대비`,
    vsTeam: (team) => `${team} 대비`,
    metric: {
      merged_per_person: "인당 머지 PR",
      lead_time: "머지까지 시간",
      size: "PR 크기",
      review_wait: "첫 리뷰까지",
    },
    personMerged: "머지 PR",
    averageMerged: { week: "주평균 머지 PR", month: "월평균 머지 PR" },
    averagePerPerson: { week: "인당 주평균 머지 PR", month: "인당 월평균 머지 PR" },
    partialCounts: "진행 중인 주·월은 현재까지 실제 병합된 누적값입니다.",
    columnMerged: { week: "머지/주", month: "머지/월" },
    definition: {
      merged_per_person: "머지된 PR 수 ÷ 인원 ÷ 기간. 머지일 기준입니다.",
      lead_time: "PR을 연 시점부터 머지까지 걸린 시간의 중앙값.",
      size: "머지된 PR의 추가+삭제 줄 수 중앙값.",
      review_wait: "리뷰 가능 상태부터 작성자가 아닌 사람의 첫 리뷰까지 걸린 시간의 중앙값. 봇은 제외합니다.",
    },
    unit: { week: "/주", month: "/월", lines: "줄" },
    chartNote: { week: "주당", month: "월당", median: "중앙값" },
    team: "팀",
    teamValue: (value) => `팀 ${value}`,
    thisPeriod: "이번 기간",
    previousPeriod: "이전 기간",
    inProgress: "진행 중",
    noData: "이 기간에 머지된 PR이 없습니다.",
    person: "사람",
    versusTeam: (name) => `${name} vs 팀`,
    recent: "최근 PR",
    pr: "PR",
    open: (name) => `${name} 화면 열기 →`,
    flagNote: "팀 평소보다 크게 높은 값",
    flagTitle: "사람은 팀 중앙값의 1.5배 초과, PR은 팀 상위 10%",
  },
};

const METRICS = ["merged_per_person", "lead_time", "size", "review_wait"];
const PR_METRICS = METRICS.slice(1);
const RANGES = { week: [4, 12, 26, 52, 104, 156], month: [3, 6, 12, 24, 36] };
const DEFAULT_COUNT = { week: 12, month: 6 };

const state = { group: null, person: null, grain: "week", count: 12, metric: "merged_per_person", focus: null, custom: false, from: "", until: "" };
let meta = null;
let report = null;
let t = I18N.en;
let syncStatus = null;
let syncTimer;

const $ = (id) => document.getElementById(id);
const present = (children) => children.filter((child) => child !== null && child !== undefined && child !== false);

function h(tag, attrs = {}, ...children) {
  const node = document.createElement(tag);
  for (const [key, value] of Object.entries(attrs)) {
    if (value === null || value === undefined) continue;
    if (key.startsWith("on")) node.addEventListener(key.slice(2), value);
    else node.setAttribute(key, value);
  }
  node.append(...present(children));
  return node;
}

function fill(node, ...children) {
  node.replaceChildren(...present(children));
}

function svg(tag, attrs = {}, text) {
  const node = document.createElementNS("http://www.w3.org/2000/svg", tag);
  for (const [key, value] of Object.entries(attrs)) node.setAttribute(key, value);
  if (text !== undefined) node.textContent = text;
  return node;
}

// ---- formatting -------------------------------------------------------

function fmtNumber(value, digits = 1) {
  if (value === null || value === undefined) return "–";
  return Number(value).toLocaleString(meta.locale, { maximumFractionDigits: digits });
}

function fmtUnit(value, unit, digits, unitDisplay = "narrow") {
  return new Intl.NumberFormat(meta.locale, { style: "unit", unit, unitDisplay, maximumFractionDigits: digits }).format(value);
}

function fmtHours(value) {
  if (value === null || value === undefined) return "–";
  if (value < 1) return fmtUnit(Math.round(value * 60), "minute", 0);
  if (value < 48) return fmtUnit(value, "hour", 1);
  return fmtUnit(value / 24, "day", 1);
}

function format(metric, value) {
  if (metric === "lead_time" || metric === "review_wait") return fmtHours(value);
  return fmtNumber(value, metric === "size" ? 0 : 1);
}

function unit(metric) {
  if (metric === "merged_per_person") return t.unit[state.grain];
  if (metric === "size") return t.unit.lines;
  return "";
}

function withUnit(metric, value) {
  return present([format(metric, value), unit(metric)]).join(" ");
}

function span(count = state.count) {
  return fmtUnit(count, state.grain, 0, "long");
}

function metricLabel(metric, summary = false) {
  if (metric !== "merged_per_person") return t.metric[metric];
  const person = report.scope.kind === "person";
  if (summary) return (person ? t.averageMerged : t.averagePerPerson)[state.grain];
  return person ? t.personMerged : t.metric[metric];
}

function columnLabel(metric) {
  return metric === "merged_per_person" ? t.columnMerged[state.grain] : t.metric[metric];
}

function initials(login) {
  return login.slice(0, 1).toUpperCase();
}

function dateLabel(iso) {
  const day = new Date(`${iso}T00:00:00`);
  const year = report.periods[0].start.slice(0, 4) !== report.periods.at(-1).start.slice(0, 4) ? "numeric" : undefined;
  if (state.grain === "month") return day.toLocaleDateString(meta.locale, { year, month: "short" });
  return day.toLocaleDateString(meta.locale, { year, month: "short", day: "numeric" });
}

function avatar(login, large = false) {
  const url = meta.avatars?.[login.toLowerCase()];
  return h("span", { class: `avatar${large ? " large" : ""}` }, initials(login),
    url ? h("img", { src: url, alt: "", loading: "lazy", decoding: "async", referrerpolicy: "no-referrer", onerror: (event) => event.target.remove() }) : null);
}

function renderSync() {
  $("synced").textContent = meta.synced_at ? t.synced(new Date(meta.synced_at).toLocaleString(meta.locale, { dateStyle: "medium", timeStyle: "short" })) : t.never;
  $("sync").disabled = !syncStatus?.available || syncStatus.state === "running";
  $("sync").textContent = syncStatus?.state === "running" ? t.syncing : t.sync;
  const progress = syncStatus?.repositories ? t.syncProgress(syncStatus.repositories, syncStatus.processed_prs) : "";
  $("sync-status").textContent = syncStatus?.state === "error" ? syncStatus.error : [syncStatus?.state === "success" ? t.syncDone : "", progress].filter(Boolean).join(" · ");
  $("sync-status").classList.toggle("error", syncStatus?.state === "error");
}

async function pollSync(refresh = false) {
  clearTimeout(syncTimer);
  const wasRunning = syncStatus?.state === "running";
  try {
    syncStatus = await getJSON("/api/sync");
    renderSync();
    if (syncStatus.state === "running") syncTimer = setTimeout(pollSync, 1500);
    else if (wasRunning || refresh) {
      meta = await getJSON("/api/meta");
      renderSync();
      await load();
    }
  } catch (error) {
    $("sync-status").textContent = t.syncConnectionError;
    syncTimer = setTimeout(pollSync, 3000);
  }
}

async function startSync() {
  $("sync").disabled = true;
  try {
    syncStatus = await getJSON("/api/sync", { method: "POST", headers: { "Content-Type": "application/json" }, body: "{}" });
    renderSync();
    await pollSync(true);
  } catch (error) {
    $("sync-status").textContent = error.message;
    await pollSync();
  }
}

// ---- state & data ------------------------------------------------------

async function getJSON(url, options = {}) {
  const response = await fetch(url, options);
  const payload = await response.json();
  if (!response.ok) throw new Error(payload.error || response.statusText);
  return payload;
}

function showError(message) {
  $("error").textContent = message;
  $("error").hidden = !message;
}

function readUrl() {
  const params = new URLSearchParams(location.search);
  if (params.get("grain") === "month") state.grain = "month";
  const count = Number(params.get("count"));
  state.count = RANGES[state.grain].includes(count) ? count : DEFAULT_COUNT[state.grain];
  if (params.has("from") && params.has("until")) {
    state.custom = true;
    state.from = params.get("from");
    state.until = params.get("until");
  }
  if (METRICS.includes(params.get("metric"))) state.metric = params.get("metric");
  if (params.get("person")) state.person = params.get("person");
  else if (meta.groups.includes(params.get("group"))) state.group = params.get("group");
  else if (meta.groups.length === 1) state.group = meta.groups[0];
}

function syncUrl() {
  const params = new URLSearchParams();
  if (state.person) params.set("person", state.person);
  else if (state.group) params.set("group", state.group);
  params.set("grain", state.grain);
  if (state.custom) {
    params.set("from", state.from);
    params.set("until", state.until);
  } else params.set("count", state.count);
  params.set("metric", state.metric);
  history.replaceState(null, "", `?${params}`);
  return params;
}

async function load() {
  const params = syncUrl();
  renderNav();
  try {
    report = await getJSON(`/api/report?${params}`);
    showError("");
  } catch (error) {
    showError(error.message);
    return;
  }
  const people = report.people || [];
  if (!people.some((row) => row.login === state.focus)) state.focus = people[0]?.login ?? null;
  render();
}

function select(scope) {
  state.group = scope.group ?? null;
  state.person = scope.person ?? null;
  state.focus = null;
  load();
}

// ---- layout ---------------------------------------------------------------

function render() {
  renderTitle();
  renderControls();
  renderKpis();
  renderChart();
  renderDetail();
  renderPanel();
}

function renderNav() {
  const people = meta.people.map((login) =>
    navItem(login, state.person?.toLowerCase() === login.toLowerCase(), () => select({ person: login }), avatar(login)),
  );
  fill(
    $("nav"),
    h(
      "div",
      { class: "nav-group" },
      h("div", { class: "nav-label" }, t.groups),
      meta.groups.length !== 1 ? navItem(t.everyone, !state.group && !state.person, () => select({})) : null,
      ...meta.groups.map((name) => navItem(name, state.group === name, () => select({ group: name }))),
    ),
    people.length ? h("div", { class: "nav-group" }, h("div", { class: "nav-label" }, t.people), ...people) : null,
  );
}

function navItem(label, current, onclick, icon = null) {
  return h("button", { type: "button", class: "nav-item", "aria-current": current ? "page" : null, onclick }, icon, label);
}

function renderTitle() {
  const { kind, name, team } = report.scope;
  if (kind === "person") {
    fill(
      $("title"),
      avatar(name, true),
      h("div", {}, h("h1", {}, name), h("p", {}, t.vsTeam(team || t.everyone))),
    );
  } else {
    fill($("title"), h("div", {}, h("h1", {}, name || t.everyone), h("p", {}, state.custom ? t.customComparison(state.from, state.until) : t.vsPrevious(span()))));
  }
}

function renderControls() {
  const count = $("count");
  fill(count, ...RANGES[state.grain].map((n) => h("option", { value: n }, t.last(span(n)))), h("option", { value: "custom" }, t.custom));
  count.value = state.custom ? "custom" : String(state.count);
  count.onchange = () => {
    state.custom = count.value === "custom";
    if (state.custom) {
      state.from ||= report.periods[0].start;
      state.until ||= meta.today;
      renderControls();
    } else {
      state.count = Number(count.value);
      load();
    }
  };
  $("custom-range").hidden = !state.custom;
  $("from-label").textContent = t.from;
  $("until-label").textContent = t.until;
  $("apply-range").textContent = t.apply;
  $("from").value = state.from;
  $("until").value = state.until;
  $("from").max = $("until").max = meta.today;
  $("custom-range").onsubmit = (event) => {
    event.preventDefault();
    if (!$("custom-range").reportValidity()) return;
    const from = $("from").value, until = $("until").value;
    if (from > until) { showError(t.invalidRange); return; }
    state.from = from;
    state.until = until;
    load();
  };
  fill(
    $("grain"),
    ...Object.entries(t.grain).map(([grain, label]) =>
      h(
        "button",
        {
          type: "button",
          "aria-pressed": String(grain === state.grain),
          onclick: () => {
            if (grain === state.grain) return;
            state.grain = grain;
            state.count = DEFAULT_COUNT[grain];
            load();
          },
        },
        label,
      ),
    ),
  );
}

// ---- KPIs -------------------------------------------------------------------

function renderKpis() {
  const { current, compare } = report.summary;
  const person = report.scope.kind === "person";
  fill(
    $("kpis"),
    ...METRICS.map((metric) => {
      const value = current?.[metric] ?? null;
      const other = compare?.[metric] ?? null;
      return h(
        "button",
        {
          type: "button",
          class: "kpi",
          title: t.definition[metric],
          "aria-pressed": String(metric === state.metric),
          onclick: () => {
            state.metric = metric;
            syncUrl();
            renderKpis();
            renderChart();
          },
        },
        h("span", { class: "kpi__label" }, metricLabel(metric, true)),
        h("span", { class: "kpi__value" }, format(metric, value), unit(metric) ? h("small", {}, unit(metric)) : null),
        person ? h("span", { class: "kpi__note" }, t.teamValue(withUnit(metric, other))) : delta(metric, value, other),
      );
    }),
  );
}

function delta(metric, value, before) {
  if (value === null || before === null) return null;
  const change = value - before;
  const shown = format(metric, Math.abs(change));
  // A change that rounds to zero at display precision is no change.
  if (shown === format(metric, 0)) return h("span", { class: "kpi__delta" }, shown);
  const better = change > 0 === (metric === "merged_per_person") ? "good" : "bad";
  return h("span", { class: `kpi__delta ${better}` }, `${change > 0 ? "▲ +" : "▼ −"}${shown}`);
}

// ---- chart -------------------------------------------------------------------

function renderChart() {
  const metric = state.metric;
  const periods = report.periods;
  const person = report.scope.kind === "person";
  const series = [
    { key: "current", name: person ? report.scope.name : t.thisPeriod, color: "var(--series)", area: true },
    { key: "compare", name: person ? t.team : t.previousPeriod, color: "var(--series-2)", dashed: true },
  ].filter((s) => periods.some((period) => period[s.key]?.[metric] != null));
  const note = metric === "merged_per_person" ? t.chartNote[state.grain] : t.chartNote.median;
  const host = h("div");
  const empty = !periods.some((period) => period.current?.merged);
  fill(
    $("chart"),
    h(
      "div",
      { class: "chart__head" },
      h("div", {}, h("h2", {}, metricLabel(metric)), h("p", {}, note)),
      h("div", { class: "legend" }, ...series.map((s) => h("span", {}, h("i", { class: s.dashed ? "dashed" : null, style: `--key:${s.color}` }), s.name))),
    ),
    empty ? h("p", { class: "empty" }, t.noData) : host,
    metric === "merged_per_person" && periods.some((period) => period.partial) ? h("p", { class: "muted" }, t.partialCounts) : null,
  );
  if (!empty) requestAnimationFrame(() => lineChart(host, metric, series, periods));
}

function lineChart(host, metric, series, periods) {
  const width = host.clientWidth || 640;
  const height = 220;
  const pad = { left: 52, right: 12, top: 12, bottom: 26 };
  const plotWidth = width - pad.left - pad.right;
  const plotHeight = height - pad.top - pad.bottom;
  const value = (period, key) => period[key]?.[metric] ?? null;
  const values = series.flatMap((s) => periods.map((p) => value(p, s.key))).filter((v) => v !== null);
  const max = niceMax(Math.max(0, ...values));
  const x = (i) => pad.left + (periods.length === 1 ? plotWidth / 2 : (i * plotWidth) / (periods.length - 1));
  const y = (v) => pad.top + plotHeight - (v / max) * plotHeight;

  const root = svg("svg", { viewBox: `0 0 ${width} ${height}`, role: "img", "aria-label": metricLabel(metric) });
  for (const tick of [0, max / 2, max]) {
    root.append(svg("line", { class: "grid", x1: pad.left, x2: width - pad.right, y1: y(tick), y2: y(tick) }));
    root.append(svg("text", { x: pad.left - 8, y: y(tick) + 4, "text-anchor": "end" }, format(metric, tick)));
  }
  const step = Math.ceil(periods.length / Math.max(2, Math.floor(plotWidth / 70)));
  periods.forEach((period, i) => {
    if ((periods.length - 1 - i) % step === 0) {
      root.append(svg("text", { x: x(i), y: height - 6, "text-anchor": "middle" }, dateLabel(period.start)));
    }
  });

  const lastPartial = periods.at(-1).partial;
  // Draw the comparison first so the current line sits on top.
  for (const s of [...series].reverse()) {
    const points = periods.map((p, i) => (value(p, s.key) === null ? null : [x(i), y(value(p, s.key))]));
    const settled = lastPartial && s.key === "current" ? points.slice(0, -1) : points;
    if (s.area) root.append(svg("path", { class: "area", d: areaFor(settled, y(0)) }));
    root.append(svg("path", { class: `line${s.dashed ? " dashed" : ""}`, d: pathFor(settled), stroke: s.color }));
    if (settled !== points && points.at(-1) && points.at(-2)) {
      root.append(svg("path", { class: "line dashed", d: pathFor(points.slice(-2)), stroke: s.color }));
    }
    if (periods.length <= 26) {
      for (const point of points) {
        if (point) root.append(svg("circle", { cx: point[0], cy: point[1], r: s.dashed ? 2.5 : 3.5, fill: s.color }));
      }
    }
  }

  const crosshair = svg("line", { class: "crosshair", y1: pad.top, y2: pad.top + plotHeight, visibility: "hidden" });
  root.append(crosshair);
  const overlay = svg("rect", { x: pad.left - 8, y: 0, width: plotWidth + 16, height, fill: "transparent" });
  root.append(overlay);
  let shown = -1;
  overlay.addEventListener("pointermove", (event) => {
    const box = root.getBoundingClientRect();
    const px = ((event.clientX - box.left) / box.width) * width;
    const index = periods.length === 1 ? 0 : Math.round(((px - pad.left) / plotWidth) * (periods.length - 1));
    const i = Math.min(periods.length - 1, Math.max(0, index));
    if (i !== shown) {
      shown = i;
      crosshair.setAttribute("x1", x(i));
      crosshair.setAttribute("x2", x(i));
      crosshair.setAttribute("visibility", "visible");
      fillTooltip(periods[i], metric, series);
    }
    placeTooltip(event);
  });
  overlay.addEventListener("pointerleave", () => {
    shown = -1;
    crosshair.setAttribute("visibility", "hidden");
    $("tooltip").hidden = true;
  });
  fill(host, root);
}

function pathFor(points) {
  let d = "";
  let drawing = false;
  for (const point of points) {
    if (!point) {
      drawing = false;
      continue;
    }
    d += `${drawing ? "L" : "M"}${point[0].toFixed(1)},${point[1].toFixed(1)}`;
    drawing = true;
  }
  return d;
}

function areaFor(points, baseline) {
  let d = "";
  let run = [];
  const flush = () => {
    if (run.length > 1) {
      d += `M${run[0][0].toFixed(1)},${baseline}${run.map(([px, py]) => `L${px.toFixed(1)},${py.toFixed(1)}`).join("")}L${run.at(-1)[0].toFixed(1)},${baseline}Z`;
    }
    run = [];
  };
  for (const point of points) {
    if (point) run.push(point);
    else flush();
  }
  flush();
  return d;
}

function niceMax(value) {
  if (value <= 0) return 1;
  const magnitude = 10 ** Math.floor(Math.log10(value));
  const nice = [1, 2, 2.5, 5, 10].find((m) => m * magnitude >= value);
  return nice * magnitude;
}

function fillTooltip(period, metric, series) {
  const tooltip = $("tooltip");
  fill(
    tooltip,
    h("strong", {}, `${dateLabel(period.start)}${period.partial ? ` · ${t.inProgress}` : ""}`),
    ...series.map((s) => h("div", { class: "row" }, h("i", { style: `--key:${s.color}` }), h("span", {}, s.name), h("b", {}, withUnit(metric, period[s.key]?.[metric] ?? null)))),
  );
  tooltip.hidden = false;
}

function placeTooltip(event) {
  const tooltip = $("tooltip");
  tooltip.style.left = `${Math.min(event.clientX + 14, window.innerWidth - tooltip.offsetWidth - 8)}px`;
  tooltip.style.top = `${event.clientY + 14}px`;
}

// ---- tables -------------------------------------------------------------------

function renderDetail() {
  if (report.scope.kind === "person") {
    dataTable(
      t.recent,
      [t.pr, ...PR_METRICS.map(columnLabel)],
      report.recent.map((pr) =>
        h(
          "tr",
          {},
          h("td", { class: "pr-title" }, h("a", { href: pr.url, target: "_blank", rel: "noopener" }, pr.title), h("span", {}, `${pr.repository} #${pr.number}`)),
          ...PR_METRICS.map((metric) => valueCell(metric, pr)),
        ),
      ),
      report.recent,
    );
    return;
  }
  const team = report.summary.current;
  dataTable(
    t.team,
    [t.person, ...METRICS.map(columnLabel)],
    report.people.map((row) =>
      h(
        "tr",
        {
          "aria-selected": String(row.login === state.focus),
          tabindex: 0,
          onclick: () => focusPerson(row.login),
          onkeydown: (event) => event.key === "Enter" && focusPerson(row.login),
        },
        h("td", {}, h("span", { class: "person" }, avatar(row.login), row.login)),
        ...METRICS.map((metric) => valueCell(metric, row)),
      ),
    ),
    report.people,
    team ? h("tfoot", {}, h("tr", {}, h("td", {}, t.team), ...METRICS.map((metric) => h("td", {}, format(metric, team[metric]))))) : null,
  );
}

function dataTable(title, headers, rows, records, foot = null) {
  fill(
    $("detail"),
    h("h2", {}, title),
    rows.length
      ? h(
          "div",
          { class: "table-wrap" },
          h(
            "table",
            { class: records === report.people ? "selectable" : null },
            h("thead", {}, h("tr", {}, ...headers.map((header) => h("th", {}, header)))),
            h("tbody", {}, ...rows),
            foot,
          ),
        )
      : h("p", { class: "empty" }, t.noData),
    records.some((record) => record.flags.length) ? h("p", { class: "table-note", title: t.flagTitle }, t.flagNote) : null,
  );
}

function valueCell(metric, record) {
  const flagged = record.flags.includes(metric);
  return h("td", { class: flagged ? "flag" : null, title: flagged ? t.flagTitle : null }, format(metric, record[metric]));
}

function focusPerson(login) {
  state.focus = login;
  renderDetail();
  renderPanel();
}

// ---- person panel ---------------------------------------------------------------

function renderPanel() {
  const panel = $("panel");
  const row = report.people?.find((person) => person.login === state.focus);
  panel.hidden = !row;
  $("app").classList.toggle("no-panel", !row);
  if (!row) return;
  const team = report.summary.current;
  fill(
    panel,
    h(
      "div",
      { class: "panel__who" },
      avatar(row.login, true),
      h("div", {}, h("h2", {}, row.login), report.scope.name ? h("p", { class: "muted" }, report.scope.name) : null),
    ),
    h(
      "div",
      {},
      h("h3", {}, t.versusTeam(row.login)),
      h(
        "div",
        { class: "versus" },
        ...METRICS.map((metric) =>
          h(
            "div",
            {},
            h("span", {}, columnLabel(metric)),
            h("b", { class: row.flags.includes(metric) ? "flag" : null }, format(metric, row[metric]), h("small", {}, ` vs ${format(metric, team?.[metric] ?? null)}`)),
          ),
        ),
      ),
    ),
    row.recent.length
      ? h(
          "div",
          {},
          h("h3", {}, t.recent),
          h(
            "div",
            { class: "recent" },
            ...row.recent.map((pr) =>
              h(
                "a",
                { href: pr.url, target: "_blank", rel: "noopener" },
                h("strong", {}, pr.title),
                h("span", { class: pr.flags.includes("size") ? "flag" : null }, `${pr.repository} #${pr.number} · ${withUnit("size", pr.size)}`),
              ),
            ),
          ),
        )
      : null,
    h("button", { type: "button", class: "open-person", onclick: () => select({ person: row.login }) }, t.open(row.login)),
  );
}

// ---- boot ---------------------------------------------------------------------

async function init() {
  meta = await getJSON("/api/meta");
  t = I18N[meta.locale] || I18N.en;
  document.documentElement.lang = meta.locale;
  document.title = `${meta.org} · orgpulse`;
  $("org").textContent = meta.org;
  renderSync();
  $("sync").onclick = startSync;
  readUrl();
  await load();
  await pollSync();
  let resizeTimer;
  window.addEventListener("resize", () => {
    clearTimeout(resizeTimer);
    resizeTimer = setTimeout(() => report && renderChart(), 150);
  });
}

init().catch((error) => showError(error.message));
