"use strict";

const I18N = {
  en: {
    never: "Not synced yet — run `orgpulse sync`.",
    synced: (at) => `Synced ${at}`,
    everyone: "Everyone",
    custom: "Selected people",
    group: (name) => `Group: ${name}`,
    grain: { week: "Week", month: "Month" },
    stat: { median: "Median", p90: "p90" },
    last: (n, grain) => `Last ${n} ${grain === "week" ? "weeks" : "months"}`,
    perPeriod: { week: "per week", month: "per month" },
    previous: "prev",
    unreviewed: (p) => `${p}% merged without review`,
    active: (n) => `${n} active`,
    chart: {
      throughput: "PRs per active person",
      merged: "Merged",
      opened: "Opened",
      size: "PR size (changed lines)",
      commits: "Commits per PR",
      lead_time: "Time to merge",
      from_open: "From open",
      from_first_commit: "From first commit",
      review_wait: "Wait for first review",
    },
    partial: "in progress",
    aiMarker: "AI",
    people: "People",
    columns: {
      login: "Person",
      merged: "Merged",
      rate: (g) => `/${g === "week" ? "wk" : "mo"}`,
      size: "Size",
      commits: "Commits",
      lead_time: "To merge",
      review_wait: "Review wait",
    },
    ai: "AI adoption",
    aiNote: (days, n, excluded) =>
      `Medians from the ${days} days before each person's adoption date vs. the most recent ${days} days. ` +
      `${n} people compared` +
      (excluded.length ? `; excluded (no merged PRs in one window): ${excluded.join(", ")}.` : "."),
    mergedPerWeek: "Merged PRs per person per week",
    before: "Before",
    after: "Recent",
    change: "Change",
    adoptedOn: "Adopted",
    noData: "No pull requests in this range.",
    definitions: [
      ["Active person", "Someone who opened or merged at least one PR in the period. Per-person rates divide by active people."],
      ["Time to merge", "From PR creation, or from the PR's first authored commit, to merge."],
      ["Wait for first review", "From ready-for-review to the first review by someone other than the author. Bots are ignored."],
      ["Buckets", "Merged-PR metrics use the merge date; opened counts use the creation date. Dashed segments are periods still in progress."],
    ],
  },
  ko: {
    never: "아직 동기화하지 않았습니다 — `orgpulse sync`를 실행하세요.",
    synced: (at) => `${at} 동기화`,
    everyone: "전체",
    custom: "선택한 사람",
    group: (name) => `그룹: ${name}`,
    grain: { week: "주", month: "월" },
    stat: { median: "중앙값", p90: "p90" },
    last: (n, grain) => `최근 ${n}${grain === "week" ? "주" : "개월"}`,
    perPeriod: { week: "주당", month: "월당" },
    previous: "이전",
    unreviewed: (p) => `리뷰 없이 머지 ${p}%`,
    active: (n) => `활성 ${n}명`,
    chart: {
      throughput: "활성 인원 1인당 PR",
      merged: "머지",
      opened: "오픈",
      size: "PR 크기 (변경 라인)",
      commits: "PR당 커밋",
      lead_time: "머지까지 걸린 시간",
      from_open: "오픈부터",
      from_first_commit: "첫 커밋부터",
      review_wait: "첫 리뷰 대기",
    },
    partial: "진행 중",
    aiMarker: "AI",
    people: "사람별",
    columns: {
      login: "사람",
      merged: "머지",
      rate: (g) => (g === "week" ? "/주" : "/월"),
      size: "크기",
      commits: "커밋",
      lead_time: "머지까지",
      review_wait: "리뷰 대기",
    },
    ai: "AI 도입 효과",
    aiNote: (days, n, excluded) =>
      `사람별 도입일 이전 ${days}일과 최근 ${days}일의 중앙값을 비교합니다. ${n}명 비교` +
      (excluded.length ? `, 제외(한쪽 구간에 머지 PR 없음): ${excluded.join(", ")}.` : "."),
    mergedPerWeek: "1인 주당 머지 PR",
    before: "도입 전",
    after: "최근",
    change: "변화",
    adoptedOn: "도입일",
    noData: "이 기간에 PR이 없습니다.",
    definitions: [
      ["활성 인원", "해당 기간에 PR을 하나 이상 열거나 머지한 사람. 1인당 지표는 활성 인원으로 나눕니다."],
      ["머지까지 걸린 시간", "PR 생성 시각 또는 PR의 첫 커밋 작성 시각부터 머지까지."],
      ["첫 리뷰 대기", "리뷰 가능 상태가 된 뒤 작성자가 아닌 사람이 처음 리뷰하기까지. 봇은 제외합니다."],
      ["집계 기준", "머지 지표는 머지일, 오픈 수는 생성일 기준입니다. 점선 구간은 아직 진행 중인 기간입니다."],
    ],
  },
};

const RANGES = { week: [8, 13, 26, 52], month: [3, 6, 12, 24] };
const DEFAULT_COUNT = { week: 13, month: 6 };

const state = { scope: "all", people: new Set(), grain: "week", stat: "median", count: 13 };
let meta = null;
let report = null;
let t = I18N.en;

const $ = (id) => document.getElementById(id);

function h(tag, attrs = {}, ...children) {
  const node = document.createElement(tag);
  setAttrs(node, attrs);
  node.append(...children.filter((child) => child !== null && child !== undefined));
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

function fmtInt(value) {
  return fmtNumber(value === null ? null : Math.round(value), 0);
}

function fmtHours(value) {
  if (value === null || value === undefined) return "–";
  const ko = meta.locale === "ko";
  if (value < 1) return `${Math.round(value * 60)}${ko ? "분" : "m"}`;
  if (value < 48) return `${fmtNumber(value)}${ko ? "시간" : "h"}`;
  return `${fmtNumber(value / 24)}${ko ? "일" : "d"}`;
}

function fmtChange(current, previous) {
  if (current === null || previous === null || previous === undefined || previous === 0) return "";
  const change = ((current - previous) / previous) * 100;
  const arrow = change > 0 ? "▲" : change < 0 ? "▼" : "";
  return `${arrow}${fmtNumber(Math.abs(change), 0)}%`;
}

function periodLabel(period) {
  if (state.grain === "month") return period.key.slice(2).replace("-", ".");
  return period.start.slice(5).replace("-", "/");
}

function periodRange(period) {
  return state.grain === "month" ? period.key : `${period.start} – ${period.end.slice(5)}`;
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
  if (params.get("stat") === "p90") state.stat = "p90";
  state.count = Number(params.get("count")) || DEFAULT_COUNT[state.grain];
  if (params.has("group") && meta.groups[params.get("group")]) {
    state.scope = `group:${params.get("group")}`;
    state.people = new Set(meta.groups[params.get("group")]);
  } else if (params.get("people")) {
    state.scope = "custom";
    state.people = new Set(params.get("people").split(","));
  }
}

function query() {
  const params = new URLSearchParams({ grain: state.grain, count: state.count });
  if (state.stat === "p90") params.set("stat", "p90");
  if (state.scope.startsWith("group:")) params.set("group", state.scope.slice(6));
  else if (state.scope === "custom") params.set("people", [...state.people].join(","));
  return params;
}

async function load() {
  const params = query();
  history.replaceState(null, "", `?${params}`);
  try {
    report = await getJSON(`/api/report?${params}`);
    $("error").hidden = true;
  } catch (error) {
    $("error").textContent = error.message;
    $("error").hidden = false;
    return;
  }
  render();
}

function setScope(scope, people) {
  state.scope = scope;
  state.people = new Set(people);
  renderControls();
  load();
}

// ---- controls ------------------------------------------------------------

function renderControls() {
  const scope = $("scope");
  scope.replaceChildren(
    h("option", { value: "all" }, t.everyone),
    ...Object.keys(meta.groups).map((name) => h("option", { value: `group:${name}` }, t.group(name))),
    // "custom" is a state reached by clicking people, shown here only as a label.
    state.scope === "custom" ? h("option", { value: "custom" }, t.custom) : null,
  );
  scope.value = state.scope;
  scope.onchange = () => {
    const value = scope.value;
    if (value === "all") setScope("all", []);
    else setScope(value, meta.groups[value.slice(6)]);
  };

  segmented($("grain"), t.grain, state.grain, (grain) => {
    state.grain = grain;
    state.count = DEFAULT_COUNT[grain];
    renderControls();
    load();
  });
  segmented($("stat"), t.stat, state.stat, (stat) => {
    state.stat = stat;
    renderControls();
    history.replaceState(null, "", `?${query()}`);
    render();
  });

  const count = $("count");
  const options = RANGES[state.grain].includes(state.count)
    ? RANGES[state.grain]
    : [...RANGES[state.grain], state.count].sort((a, b) => a - b);
  count.replaceChildren(...options.map((n) => h("option", { value: n }, t.last(n, state.grain))));
  count.value = String(state.count);
  count.onchange = () => {
    state.count = Number(count.value);
    load();
  };

  const known = new Set(meta.people.map((login) => login.toLowerCase()));
  const extra = [...state.people].filter((login) => !known.has(login.toLowerCase()));
  const selected = new Set([...state.people].map((login) => login.toLowerCase()));
  $("people-chips").replaceChildren(
    ...[...meta.people, ...extra].map((login) =>
      h(
        "button",
        {
          type: "button",
          class: "chip",
          "aria-pressed": String(selected.has(login.toLowerCase())),
          onclick: () => togglePerson(login),
        },
        login,
      ),
    ),
  );
}

function togglePerson(login) {
  const people = state.scope === "all" ? new Set() : new Set(state.people);
  const match = [...people].find((member) => member.toLowerCase() === login.toLowerCase());
  if (match) people.delete(match);
  else people.add(login);
  if (people.size === 0) setScope("all", []);
  else setScope("custom", people);
}

function segmented(container, labels, current, onSelect) {
  container.replaceChildren(
    ...Object.entries(labels).map(([value, label]) =>
      h(
        "button",
        { type: "button", "aria-pressed": String(value === current), onclick: () => onSelect(value) },
        label,
      ),
    ),
  );
}

// ---- report --------------------------------------------------------------

function render() {
  renderTrends();
  renderAi();
  renderPeople();
}

function statKey(base) {
  return `${base}_${state.stat}`;
}

function renderTrends() {
  const periods = report.periods;
  if (!periods.some((period) => period.merged || period.opened)) {
    $("trends").replaceChildren(h("p", { class: "muted" }, t.noData));
    return;
  }
  const stat = t.stat[state.stat];
  const charts = [
    {
      title: t.chart.throughput,
      note: t.perPeriod[state.grain],
      format: (v) => fmtNumber(v),
      series: [
        { key: "merged_per_person", name: t.chart.merged, color: "var(--series-1)" },
        { key: "opened_per_person", name: t.chart.opened, color: "var(--series-2)" },
      ],
    },
    { title: t.chart.size, note: stat, format: fmtInt, series: [{ key: statKey("size"), name: stat, color: "var(--series-1)" }] },
    { title: t.chart.commits, note: stat, format: (v) => fmtNumber(v), series: [{ key: statKey("commits"), name: stat, color: "var(--series-1)" }] },
    {
      title: t.chart.lead_time,
      note: stat,
      format: fmtHours,
      series: [
        { key: statKey("lead_time"), name: t.chart.from_open, color: "var(--series-1)" },
        { key: statKey("cycle_time"), name: t.chart.from_first_commit, color: "var(--series-2)" },
      ],
    },
    {
      title: t.chart.review_wait,
      note: stat,
      format: fmtHours,
      series: [{ key: statKey("review_wait"), name: stat, color: "var(--series-1)" }],
      extra: (summary) =>
        summary.unreviewed_share === null ? null : t.unreviewed(fmtNumber(summary.unreviewed_share, 0)),
    },
  ];
  $("trends").replaceChildren(...charts.map((chart) => chartCard(chart, periods)));
}

function chartCard(chart, periods) {
  const { current, previous } = report.summary;
  const keyed = chart.series.length > 1;
  const extra = chart.extra?.(current);
  const card = h(
    "div",
    { class: "chart" },
    h("div", { class: "chart__head" }, h("h2", {}, chart.title), h("p", { class: "muted" }, chart.note)),
    h(
      "div",
      { class: "chart__summary" },
      ...chart.series.map((s) =>
        h(
          "span",
          { class: keyed ? "summary keyed" : "summary", style: `--key:${s.color}` },
          keyed ? h("span", { class: "summary__name" }, s.name) : null,
          h("strong", {}, chart.format(current[s.key])),
          h("span", { class: "muted" }, `${t.previous} ${chart.format(previous[s.key])} ${fmtChange(current[s.key], previous[s.key])}`),
        ),
      ),
      extra ? h("span", { class: "muted" }, extra) : null,
    ),
  );
  const host = h("div");
  card.append(host);
  requestAnimationFrame(() => lineChart(host, chart, periods));
  return card;
}

function lineChart(host, chart, periods) {
  const width = host.clientWidth || 600;
  const height = 150;
  const pad = { left: 48, right: 8, top: 10, bottom: 22 };
  const plotWidth = width - pad.left - pad.right;
  const plotHeight = height - pad.top - pad.bottom;
  const values = chart.series.flatMap((s) => periods.map((p) => p[s.key])).filter((v) => v !== null);
  const max = niceMax(Math.max(0, ...values));
  const x = (i) => pad.left + (periods.length === 1 ? plotWidth / 2 : (i * plotWidth) / (periods.length - 1));
  const y = (v) => pad.top + plotHeight - (v / max) * plotHeight;

  const root = svg("svg", { viewBox: `0 0 ${width} ${height}`, role: "img", "aria-label": chart.title });
  for (const tick of [0, max / 2, max]) {
    root.append(svg("line", { class: "grid", x1: pad.left, x2: width - pad.right, y1: y(tick), y2: y(tick) }));
    root.append(svg("text", { x: pad.left - 6, y: y(tick) + 4, "text-anchor": "end" }, chart.format(tick)));
  }
  const step = Math.ceil(periods.length / 7);
  periods.forEach((period, i) => {
    if ((periods.length - 1 - i) % step === 0) {
      root.append(svg("text", { x: x(i), y: height - 4, "text-anchor": "middle" }, periodLabel(period)));
    }
  });

  const adoptedOn = report.ai?.adopted_on;
  const markerIndex = adoptedOn ? periods.findIndex((p) => p.start <= adoptedOn && adoptedOn <= p.end) : -1;
  if (markerIndex >= 0) {
    root.append(svg("line", { class: "marker", x1: x(markerIndex), x2: x(markerIndex), y1: pad.top, y2: pad.top + plotHeight }));
    root.append(svg("text", { class: "marker-label", x: x(markerIndex) + 4, y: pad.top + 8 }, t.aiMarker));
  }

  const lastPartial = periods.at(-1).partial;
  for (const series of chart.series) {
    const points = periods.map((p, i) => (p[series.key] === null ? null : [x(i), y(p[series.key])]));
    const settled = lastPartial ? points.slice(0, -1) : points;
    root.append(svg("path", { class: "line", d: pathFor(settled), stroke: series.color }));
    if (lastPartial && points.at(-1) && points.at(-2)) {
      root.append(svg("path", { class: "line partial", d: pathFor(points.slice(-2)), stroke: series.color }));
    }
    points.forEach((point) => {
      if (point && periods.length <= 26) {
        root.append(svg("circle", { cx: point[0], cy: point[1], r: 2.5, fill: series.color }));
      }
    });
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
    showTooltip(event, periods[i], chart);
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

function niceMax(value) {
  if (value <= 0) return 1;
  const magnitude = 10 ** Math.floor(Math.log10(value));
  const nice = [1, 2, 2.5, 5, 10].find((m) => m * magnitude >= value);
  return nice * magnitude;
}

function showTooltip(event, period, chart) {
  const tooltip = $("tooltip");
  tooltip.replaceChildren(
    h("strong", {}, `${periodRange(period)}${period.partial ? ` · ${t.partial}` : ""}`),
    ...chart.series.map((s) =>
      h("div", { class: "row" }, h("i", { style: `--key:${s.color}` }), h("b", {}, chart.format(period[s.key])), h("span", {}, s.name)),
    ),
    h(
      "div",
      { class: "row" },
      h("span", {}, `${t.chart.merged} ${period.merged} · ${t.chart.opened} ${period.opened} · ${t.active(period.authors)}`),
    ),
  );
  tooltip.hidden = false;
  const left = Math.min(event.clientX + 12, window.innerWidth - tooltip.offsetWidth - 8);
  tooltip.style.left = `${left}px`;
  tooltip.style.top = `${event.clientY + 12}px`;
}

function renderAi() {
  const section = $("ai");
  const ai = report.ai;
  section.hidden = !ai;
  if (!ai) return;
  const rows = [
    [t.mergedPerWeek, "merged_per_person", (v) => fmtNumber(v)],
    [t.chart.size, "size_median", fmtInt],
    [t.chart.commits, "commits_median", (v) => fmtNumber(v)],
    [t.chart.lead_time, "lead_time_median", fmtHours],
    [t.chart.review_wait, "review_wait_median", fmtHours],
  ];
  const perPersonDates = ai.people.some((row) => row.adopted_on !== ai.adopted_on);
  section.replaceChildren(
    h("h2", {}, t.ai),
    h("p", { class: "section-note muted" }, t.aiNote(ai.window_days, ai.people.length, ai.excluded)),
    table(
      ["", t.before, t.after, t.change],
      rows.map(([label, key, format]) => [label, format(ai.before[key]), format(ai.after[key]), fmtChange(ai.after[key], ai.before[key])]),
    ),
    ai.people.length
      ? h(
          "div",
          { style: "margin-top:8px" },
          table(
            [t.columns.login, perPersonDates ? t.adoptedOn : null, t.columns.rate("week"), t.columns.size, t.columns.lead_time].filter(Boolean),
            ai.people.map((row) =>
              [
                personButton(row.login),
                perPersonDates ? row.adopted_on : null,
                `${fmtNumber(row.before.merged_per_person)} → ${fmtNumber(row.after.merged_per_person)}`,
                `${fmtInt(row.before.size_median)} → ${fmtInt(row.after.size_median)}`,
                `${fmtHours(row.before.lead_time_median)} → ${fmtHours(row.after.lead_time_median)}`,
              ].filter((cell) => cell !== null),
            ),
          ),
        )
      : null,
  );
}

function renderPeople() {
  $("people").replaceChildren(
    h("h2", {}, t.people),
    report.people.length
      ? table(
          [t.columns.login, t.columns.merged, t.columns.rate(state.grain), t.columns.size, t.columns.commits, t.columns.lead_time, t.columns.review_wait],
          report.people.map((row) => [
            personButton(row.login),
            fmtInt(row.merged),
            fmtNumber(row.merged_per_person),
            fmtInt(row[statKey("size")]),
            fmtNumber(row[statKey("commits")]),
            fmtHours(row[statKey("lead_time")]),
            fmtHours(row[statKey("review_wait")]),
          ]),
        )
      : h("p", { class: "muted" }, t.noData),
  );
  $("definitions").replaceChildren(
    h("dl", {}, ...t.definitions.flatMap(([term, text]) => [h("dt", {}, term), h("dd", {}, text)])),
  );
}

function personButton(login) {
  return h("button", { type: "button", onclick: () => setScope("custom", [login]) }, login);
}

function table(headers, rows) {
  return h(
    "div",
    { class: "table-wrap" },
    h(
      "table",
      {},
      h("thead", {}, h("tr", {}, ...headers.map((header) => h("th", {}, header)))),
      h("tbody", {}, ...rows.map((cells) => h("tr", {}, ...cells.map((cell) => h("td", {}, cell))))),
    ),
  );
}

// ---- boot ----------------------------------------------------------------

async function init() {
  meta = await getJSON("/api/meta");
  t = I18N[meta.locale] || I18N.en;
  document.documentElement.lang = meta.locale;
  $("org").textContent = `${meta.org} · orgpulse`;
  document.title = `${meta.org} · orgpulse`;
  $("synced").textContent = meta.synced_at ? t.synced(new Date(meta.synced_at).toLocaleString(meta.locale)) : t.never;
  readUrl();
  renderControls();
  await load();
  let resizeTimer;
  window.addEventListener("resize", () => {
    clearTimeout(resizeTimer);
    resizeTimer = setTimeout(() => report && renderTrends(), 150);
  });
}

init().catch((error) => {
  $("error").textContent = error.message;
  $("error").hidden = false;
});
