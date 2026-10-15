"use strict";

const I18N = {
  en: {
    never: "Not synced yet. Run `orgpulse sync`.",
    synced: (at) => `Synced ${at}`,
    groups: "Groups",
    people: "People",
    everyone: "Everyone",
    grain: { week: "Week", month: "Month" },
    last: (n, grain) => `Last ${n} ${grain === "week" ? "weeks" : "months"}`,
    headcount: (n) => `${n} ${n === 1 ? "person" : "people"}`,
    metric: {
      merged_per_person: { group: "Merged PRs / person", person: "Merged PRs" },
      lead_time: "Time to merge",
      size: "PR size",
      review_wait: "First review",
    },
    definition: {
      merged_per_person: "Merged PRs ÷ people ÷ periods, counted by merge date.",
      lead_time: "Median time from opening a PR to merging it.",
      size: "Median lines added plus deleted per merged PR.",
      review_wait: "Median time from ready-for-review to the first review by someone else. Bots are ignored.",
    },
    unit: { week: "/wk", month: "/mo", lines: "lines" },
    chartNote: { merged_per_person: (g) => (g === "week" ? "per week" : "per month"), other: "median" },
    vsPrevious: (n, grain) => `vs previous ${n} ${grain === "week" ? "weeks" : "months"}`,
    noPrevious: "No earlier data to compare",
    team: "Team",
    teamValue: (value) => `team ${value}`,
    thisPeriod: "This period",
    previousPeriod: "Previous period",
    teamMedian: "Team",
    inProgress: "in progress",
    noData: "No merged PRs in this range.",
    person: "Person",
    columns: { merged_per_person: (g) => (g === "week" ? "Merged/wk" : "Merged/mo"), lead_time: "Time to merge", size: "PR size", review_wait: "First review" },
    versusTeam: (name) => `${name} vs team`,
    recent: "Recent PRs",
    pr: "PR",
    lines: (n) => `${n} lines`,
    open: (name) => `Open ${name} →`,
    flagNote: "Above the team's usual range (90th percentile for a PR, 1.5× the team median for a person).",
    theme: { dark: "Switch to light theme", light: "Switch to dark theme" },
  },
  ko: {
    never: "아직 동기화하지 않았습니다. `orgpulse sync`를 실행하세요.",
    synced: (at) => `${at} 동기화`,
    groups: "그룹",
    people: "사람",
    everyone: "전체",
    grain: { week: "주", month: "월" },
    last: (n, grain) => `최근 ${n}${grain === "week" ? "주" : "개월"}`,
    headcount: (n) => `${n}명`,
    metric: {
      merged_per_person: { group: "인당 머지 PR", person: "머지 PR" },
      lead_time: "머지까지 시간",
      size: "PR 크기",
      review_wait: "첫 리뷰까지",
    },
    definition: {
      merged_per_person: "머지된 PR 수 ÷ 인원 ÷ 기간. 머지일 기준입니다.",
      lead_time: "PR을 연 시점부터 머지까지 걸린 시간의 중앙값.",
      size: "머지된 PR의 추가+삭제 줄 수 중앙값.",
      review_wait: "리뷰 가능 상태부터 작성자가 아닌 사람의 첫 리뷰까지 걸린 시간의 중앙값. 봇은 제외합니다.",
    },
    unit: { week: "/주", month: "/월", lines: "줄" },
    chartNote: { merged_per_person: (g) => (g === "week" ? "주당" : "월당"), other: "중앙값" },
    vsPrevious: (n, grain) => `이전 ${n}${grain === "week" ? "주" : "개월"} 대비`,
    noPrevious: "비교할 이전 데이터가 없습니다",
    team: "팀",
    teamValue: (value) => `팀 ${value}`,
    thisPeriod: "이번 기간",
    previousPeriod: "이전 기간",
    teamMedian: "팀",
    inProgress: "진행 중",
    noData: "이 기간에 머지된 PR이 없습니다.",
    person: "사람",
    columns: { merged_per_person: (g) => (g === "week" ? "머지/주" : "머지/월"), lead_time: "머지까지", size: "PR 크기", review_wait: "첫 리뷰" },
    versusTeam: (name) => `${name} vs 팀`,
    recent: "최근 PR",
    pr: "PR",
    lines: (n) => `${n}줄`,
    open: (name) => `${name} 화면 열기 →`,
    flagNote: "팀의 평소 범위를 벗어난 값 (PR은 팀 상위 10%, 사람은 팀 중앙값의 1.5배 초과).",
    theme: { dark: "라이트 테마로 바꾸기", light: "다크 테마로 바꾸기" },
  },
};

const METRICS = ["merged_per_person", "lead_time", "size", "review_wait"];
// Higher is better only for throughput.
const HIGHER_IS_BETTER = { merged_per_person: true, lead_time: false, size: false, review_wait: false };
const RANGES = { week: [4, 12, 26, 52], month: [3, 6, 12] };
const DEFAULT_COUNT = { week: 12, month: 6 };

const state = { group: null, person: null, grain: "week", count: 12, metric: "merged_per_person", focus: null };
let meta = null;
let report = null;
let t = I18N.en;

const $ = (id) => document.getElementById(id);

function h(tag, attrs = {}, ...children) {
  const node = document.createElement(tag);
  setAttrs(node, attrs);
  node.append(...children.filter((child) => child !== null && child !== undefined && child !== false));
  return node;
}

function svg(tag, attrs = {}, text) {
  const node = document.createElementNS("http://www.w3.org/2000/svg", tag);
  setAttrs(node, attrs);
  if (text !== undefined) node.textContent = text;
  return node;
}

function setAttrs(node, attrs) {
  for (const [key, value] of Object.entries(attrs)) {
    if (value === null || value === undefined) continue;
    if (key.startsWith("on")) node.addEventListener(key.slice(2), value);
    else if (key === "style") node.style.cssText = value;
    else node.setAttribute(key, value);
  }
}

// ---- formatting -------------------------------------------------------

function fmtNumber(value, digits = 1) {
  if (value === null || value === undefined) return "–";
  return Number(value).toLocaleString(meta.locale, { maximumFractionDigits: digits });
}

function fmtHours(value) {
  if (value === null || value === undefined) return "–";
  const ko = meta.locale === "ko";
  if (value < 1) return `${Math.round(value * 60)}${ko ? "분" : "m"}`;
  if (value < 48) return `${fmtNumber(value)}${ko ? "시간" : "h"}`;
  return `${fmtNumber(value / 24)}${ko ? "일" : "d"}`;
}

function format(metric, value) {
  if (metric === "lead_time" || metric === "review_wait") return fmtHours(value);
  if (metric === "size") return fmtNumber(value === null ? null : Math.round(value), 0);
  return fmtNumber(value);
}

function unit(metric) {
  if (metric === "merged_per_person") return t.unit[state.grain];
  if (metric === "size") return t.unit.lines;
  return "";
}

function metricLabel(metric) {
  const label = t.metric[metric];
  return typeof label === "string" ? label : label[state.person ? "person" : "group"];
}

function initials(login) {
  return login.slice(0, 1).toUpperCase();
}

function dateLabel(iso) {
  const day = new Date(`${iso}T00:00:00`);
  if (state.grain === "month") return day.toLocaleDateString(meta.locale, { month: "short" });
  return day.toLocaleDateString(meta.locale, { month: "short", day: "numeric" });
}

// ---- state & data ------------------------------------------------------

async function getJSON(url) {
  const response = await fetch(url);
  const payload = await response.json();
  if (!response.ok) throw new Error(payload.error || response.statusText);
  return payload;
}

function readUrl() {
  const params = new URLSearchParams(location.search);
  if (params.get("grain") === "month") state.grain = "month";
  state.count = RANGES[state.grain].includes(Number(params.get("count"))) ? Number(params.get("count")) : DEFAULT_COUNT[state.grain];
  if (METRICS.includes(params.get("metric"))) state.metric = params.get("metric");
  if (params.get("person")) state.person = params.get("person");
  else if (meta.groups.some((group) => group.name === params.get("group"))) state.group = params.get("group");
}

function query() {
  const params = new URLSearchParams();
  if (state.person) params.set("person", state.person);
  else if (state.group) params.set("group", state.group);
  params.set("grain", state.grain);
  params.set("count", state.count);
  params.set("metric", state.metric);
  return params;
}

async function load() {
  const params = query();
  history.replaceState(null, "", `?${params}`);
  renderNav();
  try {
    report = await getJSON(`/api/report?${params}`);
    $("error").hidden = true;
  } catch (error) {
    $("error").textContent = error.message;
    $("error").hidden = false;
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
  if (state.person) renderRecentTable();
  else renderTeamTable();
  renderPanel();
}

function renderNav() {
  const groupItems = [
    navItem(t.everyone, !state.group && !state.person, () => select({})),
    ...meta.groups.map((group) => navItem(group.name, state.group === group.name, () => select({ group: group.name }))),
  ];
  const peopleItems = meta.people.map((login) =>
    navItem(login, state.person?.toLowerCase() === login.toLowerCase(), () => select({ person: login }), h("span", { class: "avatar" }, initials(login))),
  );
  $("nav").replaceChildren(
    h("div", { class: "nav-group" }, h("div", { class: "nav-label" }, t.groups), ...groupItems),
    peopleItems.length ? h("div", { class: "nav-group" }, h("div", { class: "nav-label" }, t.people), ...peopleItems) : null,
  );
}

function navItem(label, current, onclick, icon = null) {
  return h("button", { type: "button", class: "nav-item", "aria-current": current ? "page" : null, onclick }, icon, label);
}

function renderTitle() {
  const range = t.last(state.count, state.grain);
  if (state.person) {
    const team = report.scope.team;
    $("title").replaceChildren(
      h("span", { class: "avatar large" }, initials(state.person)),
      h("div", {}, h("h1", {}, state.person), h("p", {}, [team, range].filter(Boolean).join(" · "))),
    );
  } else {
    $("title").replaceChildren(
      h("div", {}, h("h1", {}, state.group || t.everyone), h("p", {}, `${t.headcount(report.headcount)} · ${range}`)),
    );
  }
}

function renderControls() {
  const count = $("count");
  count.replaceChildren(...RANGES[state.grain].map((n) => h("option", { value: n }, t.last(n, state.grain))));
  count.value = String(state.count);
  count.onchange = () => {
    state.count = Number(count.value);
    load();
  };
  $("grain").replaceChildren(
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
  renderThemeButton();
}

function renderThemeButton() {
  const dark = currentTheme() === "dark";
  const button = $("theme");
  button.setAttribute("aria-label", t.theme[dark ? "dark" : "light"]);
  button.title = t.theme[dark ? "dark" : "light"];
  button.replaceChildren(dark ? sunIcon() : moonIcon());
  button.onclick = () => {
    const next = dark ? "light" : "dark";
    document.documentElement.dataset.theme = next;
    try {
      localStorage.setItem("orgpulse-theme", next);
    } catch {}
    renderThemeButton();
  };
}

function currentTheme() {
  const chosen = document.documentElement.dataset.theme;
  if (chosen) return chosen;
  return matchMedia("(prefers-color-scheme: dark)").matches ? "dark" : "light";
}

function sunIcon() {
  const root = svg("svg", { viewBox: "0 0 24 24", fill: "none", stroke: "currentColor", "stroke-width": 2, "stroke-linecap": "round" });
  root.append(svg("circle", { cx: 12, cy: 12, r: 4 }));
  for (let i = 0; i < 8; i++) {
    const angle = (i * Math.PI) / 4;
    const [c, s] = [Math.cos(angle), Math.sin(angle)];
    root.append(svg("line", { x1: 12 + 7 * c, y1: 12 + 7 * s, x2: 12 + 9.5 * c, y2: 12 + 9.5 * s }));
  }
  return root;
}

function moonIcon() {
  const root = svg("svg", { viewBox: "0 0 24 24", fill: "none", stroke: "currentColor", "stroke-width": 2, "stroke-linejoin": "round" });
  root.append(svg("path", { d: "M20 14.5A8 8 0 0 1 9.5 4a8 8 0 1 0 10.5 10.5Z" }));
  return root;
}

// ---- KPIs -------------------------------------------------------------------

function renderKpis() {
  const { current, previous, team } = report.summary;
  $("kpis").replaceChildren(
    ...METRICS.map((metric) => {
      const value = current?.[metric] ?? null;
      return h(
        "button",
        {
          type: "button",
          class: "kpi",
          title: t.definition[metric],
          "aria-pressed": String(metric === state.metric),
          onclick: () => {
            state.metric = metric;
            history.replaceState(null, "", `?${query()}`);
            renderKpis();
            renderChart();
          },
        },
        h("span", { class: "kpi__label" }, metricLabel(metric)),
        h("span", { class: "kpi__value" }, format(metric, value), unit(metric) ? h("small", {}, unit(metric)) : null),
        ...delta(metric, value, previous?.[metric] ?? null),
        team ? h("span", { class: "kpi__note" }, t.teamValue(`${format(metric, team[metric])}${unit(metric) ? ` ${unit(metric)}` : ""}`)) : null,
      );
    }),
  );
}

function delta(metric, value, before) {
  if (value === null || before === null) return [h("span", { class: "kpi__note" }, t.noPrevious)];
  const change = value - before;
  const arrow = change > 0 ? "▲" : change < 0 ? "▼" : "";
  const better = change === 0 ? null : change > 0 === HIGHER_IS_BETTER[metric];
  const sign = change > 0 ? "+" : change < 0 ? "−" : "";
  return [
    h("span", { class: `kpi__delta ${better === null ? "" : better ? "good" : "bad"}` }, `${arrow} ${sign}${format(metric, Math.abs(change))}`.trim()),
    h("span", { class: "kpi__note" }, t.vsPrevious(state.count, state.grain)),
  ];
}

// ---- chart -------------------------------------------------------------------

function renderChart() {
  const metric = state.metric;
  const periods = report.periods;
  const compareKey = state.person ? "team" : "previous";
  const series = [
    { key: "current", name: state.person ? state.person : t.thisPeriod, color: "var(--series)", area: true },
    { key: compareKey, name: state.person ? t.teamMedian : t.previousPeriod, color: "var(--series-2)", dashed: true },
  ];
  const note = metric === "merged_per_person" ? t.chartNote.merged_per_person(state.grain) : t.chartNote.other;
  const host = h("div");
  const empty = !periods.some((period) => period.current?.merged);
  $("chart").replaceChildren(
    h(
      "div",
      { class: "chart__head" },
      h("div", {}, h("h2", {}, metricLabel(metric)), h("p", {}, note)),
      h(
        "div",
        { class: "legend" },
        ...series.map((s) => h("span", {}, h("i", { class: s.dashed ? "dashed" : null, style: `--key:${s.color}` }), s.name)),
      ),
    ),
    empty ? h("p", { class: "empty" }, t.noData) : host,
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
  overlay.addEventListener("pointermove", (event) => {
    const box = root.getBoundingClientRect();
    const px = ((event.clientX - box.left) / box.width) * width;
    const index = periods.length === 1 ? 0 : Math.round(((px - pad.left) / plotWidth) * (periods.length - 1));
    const i = Math.min(periods.length - 1, Math.max(0, index));
    crosshair.setAttribute("x1", x(i));
    crosshair.setAttribute("x2", x(i));
    crosshair.setAttribute("visibility", "visible");
    showTooltip(event, periods[i], metric, series);
  });
  overlay.addEventListener("pointerleave", () => {
    crosshair.setAttribute("visibility", "hidden");
    $("tooltip").hidden = true;
  });
  host.replaceChildren(root);
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

function showTooltip(event, period, metric, series) {
  const tooltip = $("tooltip");
  const unitText = unit(metric) ? ` ${unit(metric)}` : "";
  tooltip.replaceChildren(
    h("strong", {}, `${dateLabel(period.start)}${period.partial ? ` · ${t.inProgress}` : ""}`),
    ...series.map((s) =>
      h("div", { class: "row" }, h("i", { style: `--key:${s.color}` }), h("span", {}, s.name), h("b", {}, `${format(metric, period[s.key]?.[metric] ?? null)}${unitText}`)),
    ),
  );
  tooltip.hidden = false;
  const left = Math.min(event.clientX + 14, window.innerWidth - tooltip.offsetWidth - 8);
  tooltip.style.left = `${left}px`;
  tooltip.style.top = `${event.clientY + 14}px`;
}

// ---- tables -------------------------------------------------------------------

function renderTeamTable() {
  const rows = report.people;
  const team = report.summary.current;
  const headers = [t.person, ...METRICS.map((metric) => (metric === "merged_per_person" ? t.columns[metric](state.grain) : t.columns[metric]))];
  $("detail").replaceChildren(
    h("h2", {}, t.team),
    rows.length
      ? h(
          "div",
          { class: "table-wrap" },
          h(
            "table",
            { class: "selectable" },
            h("thead", {}, h("tr", {}, ...headers.map((header) => h("th", {}, header)))),
            h(
              "tbody",
              {},
              ...rows.map((row) =>
                h(
                  "tr",
                  {
                    "aria-selected": String(row.login === state.focus),
                    tabindex: 0,
                    onclick: () => focusPerson(row.login),
                    onkeydown: (event) => event.key === "Enter" && focusPerson(row.login),
                  },
                  h("td", {}, h("span", { class: "person" }, h("span", { class: "avatar" }, initials(row.login)), row.login)),
                  ...METRICS.map((metric) => h("td", { class: row.flags.includes(metric) ? "flag" : null }, format(metric, row[metric]))),
                ),
              ),
            ),
            team ? h("tfoot", {}, h("tr", {}, h("td", {}, t.team), ...METRICS.map((metric) => h("td", {}, format(metric, team[metric]))))) : null,
          ),
        )
      : h("p", { class: "empty" }, t.noData),
    rows.some((row) => row.flags.length) ? h("p", { class: "table-note" }, t.flagNote) : null,
  );
}

function focusPerson(login) {
  state.focus = login;
  renderTeamTable();
  renderPanel();
}

function renderRecentTable() {
  const rows = report.recent;
  const headers = [t.pr, t.columns.size, t.columns.lead_time, t.columns.review_wait];
  $("detail").replaceChildren(
    h("h2", {}, t.recent),
    rows.length
      ? h(
          "div",
          { class: "table-wrap" },
          h(
            "table",
            {},
            h("thead", {}, h("tr", {}, ...headers.map((header) => h("th", {}, header)))),
            h(
              "tbody",
              {},
              ...rows.map((pr) =>
                h(
                  "tr",
                  {},
                  h(
                    "td",
                    { class: "pr-title" },
                    h("a", { href: pr.url, target: "_blank", rel: "noopener" }, pr.title),
                    h("span", {}, `${pr.repository} #${pr.number}`),
                  ),
                  ...["size", "lead_time", "review_wait"].map((metric) => h("td", { class: pr.flags.includes(metric) ? "flag" : null }, format(metric, pr[metric]))),
                ),
              ),
            ),
          ),
        )
      : h("p", { class: "empty" }, t.noData),
    rows.some((pr) => pr.flags.length) ? h("p", { class: "table-note" }, t.flagNote) : null,
  );
}

// ---- person panel ---------------------------------------------------------------

function renderPanel() {
  const panel = $("panel");
  const row = !state.person && report.people.find((person) => person.login === state.focus);
  panel.hidden = !row;
  $("app").classList.toggle("no-panel", !row);
  if (!row) return;
  const team = report.summary.current;
  panel.replaceChildren(
    h(
      "div",
      { class: "panel__who" },
      h("span", { class: "avatar large" }, initials(row.login)),
      h("div", {}, h("h2", {}, row.login), h("p", { class: "muted" }, `@${row.login}${state.group ? ` · ${state.group}` : ""}`)),
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
            h("span", {}, metric === "merged_per_person" ? t.columns[metric](state.grain) : t.columns[metric]),
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
                h("span", { class: pr.flags.includes("size") ? "flag" : null }, `${pr.repository} #${pr.number} · ${t.lines(format("size", pr.size))}`),
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
  $("synced").textContent = meta.synced_at ? t.synced(new Date(meta.synced_at).toLocaleString(meta.locale, { dateStyle: "medium", timeStyle: "short" })) : t.never;
  readUrl();
  await load();
  let resizeTimer;
  window.addEventListener("resize", () => {
    clearTimeout(resizeTimer);
    resizeTimer = setTimeout(() => report && renderChart(), 150);
  });
  matchMedia("(prefers-color-scheme: dark)").addEventListener("change", () => renderThemeButton());
}

init().catch((error) => {
  $("error").textContent = error.message;
  $("error").hidden = false;
});
