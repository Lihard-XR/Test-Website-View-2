/* =========================================================
 * machine_detail.js
 * 설비 상세 페이지
 * - 현재 설비 상태 조회
 * - 툴 상세 / 차트 조회
 * - Replay 동기화 반영
 * - Three.js 3D 장면 제어
 * ========================================================= */

import * as THREE from "https://esm.sh/three@0.160.0";
import { OrbitControls } from "https://esm.sh/three@0.160.0/examples/jsm/controls/OrbitControls.js";
import { FBXLoader } from "https://esm.sh/three@0.160.0/examples/jsm/loaders/FBXLoader.js";

/* ---------------------------------------------------------
 * Replay sync
 * --------------------------------------------------------- */
const REPLAY_SYNC_CHANNEL = "factory-replay-sync";
const REPLAY_SYNC_STORAGE_KEY = "factory-replay-state";

const replayChannel =
  typeof BroadcastChannel !== "undefined"
    ? new BroadcastChannel(REPLAY_SYNC_CHANNEL)
    : null;

/* ---------------------------------------------------------
 * Utils
 * --------------------------------------------------------- */
const $ = (sel) => document.querySelector(sel);

function setText(sel, v, d = "-") {
  const el = $(sel);
  if (!el) return;
  el.textContent = v === null || v === undefined || v === "" ? d : v;
}

function fmt(n) {
  if (n === null || n === undefined || n === "" || Number.isNaN(Number(n))) {
    return "-";
  }
  return Number(n).toLocaleString("ko-KR");
}

function safeNumber(v, d = 0) {
  const n = Number(v);
  return Number.isFinite(n) ? n : d;
}

function statusToUI(status) {
  const s = (status || "").toString().toUpperCase();

  if (s.includes("START")) return { label: "START", color: "#00b050" };
  if (s.includes("READY")) return { label: "READY", color: "#f2a900" };
  if (s.includes("OFF")) return { label: "OFFLINE", color: "#8c8c8c" };
  if (s.includes("STOP")) return { label: "STOP", color: "#e60000" };

  return { label: s || "UNKNOWN", color: "#666" };
}

async function fetchJson(url) {
  const res = await fetch(url, { cache: "no-store" });
  if (!res.ok) {
    throw new Error(`HTTP ${res.status}`);
  }
  return res.json();
}

/* ---------------------------------------------------------
 * DOM
 * --------------------------------------------------------- */
const titleEl = $("#machineTitle") || $(".title");
const statusCircleEl = $("#machineStatusBadge") || $(".panel-3d > .circle");

const machineIpTextEl = $("#machineIpText");

const chartEl = $("#chart");
const chartModalBackdrop = $("#chartModalBackdrop");
const chartModalCloseBtn = $("#chartModalCloseBtn");
const chartModalCanvas = $("#chartModalCanvas");
const chartModalMeta = $("#chartModalMeta");

const toolOpenBtn = $("#toolOpenBtn");
const toolCloseBtn = $("#toolCloseBtn");
const toolDrawer = $("#toolDrawer");
const backdrop = $("#backdrop");
const toolList = $("#toolList");

/* ---------------------------------------------------------
 * Page state
 * --------------------------------------------------------- */
function getReplaySyncState() {
  try {
    const raw = localStorage.getItem(REPLAY_SYNC_STORAGE_KEY);
    if (!raw) return { mode: "live", at: null };

    const parsed = JSON.parse(raw);
    if (parsed?.mode === "replay" && parsed?.at) {
      return { mode: "replay", at: parsed.at };
    }
  } catch (e) {
    console.warn("[detail] failed to parse replay sync state:", e);
  }

  return { mode: "live", at: null };
}

const pageState = {
  currentMachine: null,
  currentToolNo: 0,
  selectedToolNo: 0,
  pollTimer: null,
  isRefreshing: false,
  lastChartData: { labels: [], load: [], rpm: [] },
  sync: getReplaySyncState(),
};

/* ---------------------------------------------------------
 * Runtime values for 3D scene
 * --------------------------------------------------------- */
let rpm = 0;
let feed = 0;
let torq = 0;
let machineStatus = "UNKNOWN";
let productSpinVel = 0;

/* ---------------------------------------------------------
 * Drawer
 * --------------------------------------------------------- */
function openDrawer() {
  if (!toolDrawer || !backdrop) return;

  toolDrawer.classList.add("open");
  backdrop.classList.add("open");

  toolOpenBtn?.classList.add("open");
  toolOpenBtn?.setAttribute("aria-expanded", "true");
  toolDrawer.setAttribute("aria-hidden", "false");
}

function closeDrawer() {
  if (!toolDrawer || !backdrop) return;

  toolDrawer.classList.remove("open");
  backdrop.classList.remove("open");

  toolOpenBtn?.classList.remove("open");
  toolOpenBtn?.setAttribute("aria-expanded", "false");
  toolDrawer.setAttribute("aria-hidden", "true");
}

function markSelectedToolButton() {
  if (!toolList) return;

  toolList.querySelectorAll(".tool-item").forEach((btn) => {
    const isSelected =
      Number(btn.dataset.toolNo) === Number(pageState.selectedToolNo);

    btn.style.background = isSelected ? "#dfe9ff" : "#fff";
    btn.style.borderColor = isSelected ? "#2f63d8" : "rgba(0,0,0,.4)";
    btn.setAttribute("aria-pressed", isSelected ? "true" : "false");
  });
}

toolOpenBtn?.addEventListener("click", openDrawer);
toolCloseBtn?.addEventListener("click", closeDrawer);
backdrop?.addEventListener("click", closeDrawer);

/* ---------------------------------------------------------
 * Modal
 * --------------------------------------------------------- */
let lastFocusedBeforeChartModal = null;

function openChartModal() {
  if (!chartModalBackdrop) return;

  lastFocusedBeforeChartModal = document.activeElement;

  const toolNo = Number(pageState.selectedToolNo || pageState.currentToolNo || 0);
  const machineName = pageState.currentMachine?.name || window.MACHINE_IP || "-";

  if (chartModalMeta) {
    chartModalMeta.textContent = `${machineName} 설비 · 툴 ${toolNo || "-"} · 부하율 / RPM`;
  }

  chartModalBackdrop.classList.add("open");
  chartModalBackdrop.setAttribute("aria-hidden", "false");
  chartModalCloseBtn?.focus();

  drawChartToCanvas(chartModalCanvas, pageState.lastChartData, true);
}

function closeChartModal() {
  if (!chartModalBackdrop) return;

  const returnTarget = lastFocusedBeforeChartModal || chartEl;
  returnTarget?.focus?.();

  chartModalBackdrop.classList.remove("open");
  chartModalBackdrop.setAttribute("aria-hidden", "true");
}

chartEl?.addEventListener("click", openChartModal);
chartModalCloseBtn?.addEventListener("click", closeChartModal);

chartModalBackdrop?.addEventListener("click", (e) => {
  if (e.target === chartModalBackdrop) {
    closeChartModal();
  }
});

window.addEventListener("keydown", (e) => {
  if (e.key === "Escape") {
    closeChartModal();
  }
});

window.addEventListener("resize", () => {
  drawChartToCanvas(chartEl, pageState.lastChartData, false);

  if (chartModalBackdrop?.classList.contains("open")) {
    drawChartToCanvas(chartModalCanvas, pageState.lastChartData, true);
  }
});

/* ---------------------------------------------------------
 * API
 * --------------------------------------------------------- */
function buildReplayQuery() {
  return pageState.sync.mode === "replay" && pageState.sync.at
    ? `?at=${encodeURIComponent(pageState.sync.at)}`
    : "";
}

function appendReplayQuery(url) {
  const replayQuery = buildReplayQuery();
  if (!replayQuery) return url;
  return `${url}${replayQuery}`;
}

async function fetchCurrentMachine() {
  const url = appendReplayQuery(`/api/machine/${window.MACHINE_IP}/current`);
  return fetchJson(url);
}

async function fetchToolDetail(toolNo) {
  const url = appendReplayQuery(`/api/machine/${window.MACHINE_IP}/tool/${toolNo}`);
  return fetchJson(url);
}

async function fetchToolChart(toolNo) {
  const replayQuery = buildReplayQuery();
  const joiner = replayQuery ? "&" : "?";
  const url =
    `/api/machine/${window.MACHINE_IP}/tool/${toolNo}/chart` +
    `${replayQuery}${joiner}limit=60`;

  return fetchJson(url);
}

async function loadToolList() {
  if (!toolList) return;

  const url = appendReplayQuery(`/api/machine/${window.MACHINE_IP}/tools`);
  const data = await fetchJson(url);

  toolList.innerHTML = "";

  (data.tools || []).forEach((toolNo) => {
    const btn = document.createElement("button");
    btn.className = "tool-item";
    btn.type = "button";
    btn.textContent = String(toolNo);
    btn.dataset.toolNo = String(toolNo);
    btn.setAttribute("aria-pressed", "false");

    btn.addEventListener("click", async () => {
      await selectTool(toolNo);
    });

    toolList.appendChild(btn);
  });

  markSelectedToolButton();
}

/* ---------------------------------------------------------
 * Apply data to UI
 * --------------------------------------------------------- */
function applyMachineHeader(machine) {
  if (!machine) return;

  if (titleEl) {
    titleEl.textContent = machine.name ? `${machine.name}` : "설비 상세";
  }

  if (statusCircleEl) {
    const ui = statusToUI(machine.status);
    statusCircleEl.textContent = ui.label;
    statusCircleEl.style.background = ui.color;
    statusCircleEl.setAttribute("aria-label", `설비 상태 ${ui.label}`);
  }

  if (machineIpTextEl) {
    machineIpTextEl.textContent = window.MACHINE_IP || "-";
  }

  machineStatus = machine.status || "UNKNOWN";
}

function syncTopToolValue() {
  const toolToShow = Number(pageState.selectedToolNo || pageState.currentToolNo || 0);
  setText("#toolValue", toolToShow ? fmt(toolToShow) : "-", "-");
}

function applyToolDetail(detail) {
  const selectedToolNo = pageState.selectedToolNo || detail.toolNo || 0;

  setText("#toolValue", selectedToolNo ? fmt(selectedToolNo) : "-", "-");
  setText("#loadValue", fmt(detail.loadPct));
  setText("#rpmValue", fmt(detail.rpm));
  setText("#prodTime", fmt(detail.prodMin));
  setText("#programNo", fmt(detail.program));
  setText("#alarmCode", fmt(detail.alarm));
  setText("#toolNo", fmt(detail.machineToolNo));

  rpm = safeNumber(detail.rpm, 0);
  feed = safeNumber(detail.feed, 0);
  torq = safeNumber(detail.loadPct, 0);
  machineStatus = detail.status || "UNKNOWN";

  if (isMachineRunning()) {
    productSpinVel = rpmToRadPerSec(rpm);
  }
}

async function renderChart(toolNo) {
  try {
    const chartData = await fetchToolChart(toolNo);

    pageState.lastChartData = {
      labels: Array.isArray(chartData.labels) ? chartData.labels : [],
      load: Array.isArray(chartData.load) ? chartData.load : [],
      rpm: Array.isArray(chartData.rpm) ? chartData.rpm : [],
    };
  } catch (e) {
    console.warn("[chart] fetch error:", e);
    pageState.lastChartData = { labels: [], load: [], rpm: [] };
  }

  drawChartToCanvas(chartEl, pageState.lastChartData, false);

  if (chartModalBackdrop?.classList.contains("open")) {
    drawChartToCanvas(chartModalCanvas, pageState.lastChartData, true);
  }
}

async function renderSelectedToolDetail() {
  const toolNo = Number(pageState.selectedToolNo || pageState.currentToolNo || 0);

  if (!toolNo) {
    syncTopToolValue();
    pageState.lastChartData = { labels: [], load: [], rpm: [] };
    drawChartToCanvas(chartEl, pageState.lastChartData, false);
    return;
  }

  const detail = await fetchToolDetail(toolNo);
  applyToolDetail(detail);

  await renderChart(toolNo);
  markSelectedToolButton();
}

async function selectTool(toolNo) {
  pageState.selectedToolNo = Number(toolNo || 0);
  syncTopToolValue();

  await renderSelectedToolDetail();
  closeDrawer();
}

async function refreshMachineAndDetail() {
  if (pageState.isRefreshing) return;
  pageState.isRefreshing = true;

  try {
    const current = await fetchCurrentMachine();

    pageState.currentMachine = current;
    pageState.currentToolNo = Number(current.currentToolNo || 0);

    // 사용자가 아직 직접 선택하지 않았으면 현재 툴 기준으로 맞춤
    if (!pageState.selectedToolNo && pageState.currentToolNo) {
      pageState.selectedToolNo = pageState.currentToolNo;
    }

    applyMachineHeader(current);
    syncTopToolValue();
    await renderSelectedToolDetail();
  } catch (e) {
    console.warn("[detail] refresh failed:", e);
  } finally {
    pageState.isRefreshing = false;
  }
}

function startPolling() {
  stopPolling();

  const tick = async () => {
    await refreshMachineAndDetail();
    pageState.pollTimer = setTimeout(tick, 2000);
  };

  tick();
}

function stopPolling() {
  if (pageState.pollTimer) {
    clearTimeout(pageState.pollTimer);
    pageState.pollTimer = null;
  }
}

function applyReplaySyncState(nextState) {
  const mode = nextState?.mode === "replay" && nextState?.at ? "replay" : "live";
  const at = mode === "replay" ? nextState.at : null;

  const changed = pageState.sync.mode !== mode || pageState.sync.at !== at;
  pageState.sync = { mode, at };

  if (changed && !pageState.isRefreshing) {
    refreshMachineAndDetail();
    loadToolList().catch((e) => console.warn("[tools] replay sync load failed:", e));
  }
}

replayChannel?.addEventListener("message", (event) => {
  applyReplaySyncState(event.data);
});

window.addEventListener("storage", (event) => {
  if (event.key === REPLAY_SYNC_STORAGE_KEY) {
    applyReplaySyncState(getReplaySyncState());
  }
});

/* ---------------------------------------------------------
 * Chart
 * --------------------------------------------------------- */
function fitCanvas(canvas, ctx) {
  if (!canvas || !ctx) return false;

  const width = Math.floor(canvas.clientWidth);
  const height = Math.floor(canvas.clientHeight);
  if (!width || !height) return false;

  const dpr = Math.max(1, window.devicePixelRatio || 1);
  const displayWidth = Math.floor(width * dpr);
  const displayHeight = Math.floor(height * dpr);

  // 크기가 바뀐 경우에만 내부 버퍼 갱신
  if (canvas.width !== displayWidth || canvas.height !== displayHeight) {
    canvas.width = displayWidth;
    canvas.height = displayHeight;
  }

  ctx.setTransform(1, 0, 0, 1, 0, 0);
  ctx.scale(dpr, dpr);

  return true;
}

function drawEmptyChart(canvas, ctx, message = "데이터가 없습니다.") {
  const rect = canvas.getBoundingClientRect();
  const w = rect.width;
  const h = rect.height;

  ctx.clearRect(0, 0, w, h);
  ctx.fillStyle = "#f2f2f2";
  ctx.fillRect(0, 0, w, h);

  ctx.fillStyle = "#555";
  ctx.font = "14px system-ui";
  ctx.textAlign = "center";
  ctx.textBaseline = "middle";
  ctx.fillText(message, w / 2, h / 2);
}

function drawChartToCanvas(
  canvas,
  { labels = [], load = [], rpm = [] } = {},
  enlarged = false
) {
  const ctx = canvas?.getContext("2d");
  if (!canvas || !ctx) return;
  if (!fitCanvas(canvas, ctx)) return;

  const rect = canvas.getBoundingClientRect();
  const w = rect.width;
  const h = rect.height;

  ctx.clearRect(0, 0, w, h);
  ctx.fillStyle = "#f2f2f2";
  ctx.fillRect(0, 0, w, h);

  const count = Math.min(labels.length, load.length, rpm.length);
  if (!count) {
    drawEmptyChart(canvas, ctx);
    return;
  }

  const labelsSafe = labels.slice(0, count);
  const loadSafe = load.slice(0, count).map((v) => safeNumber(v, 0));
  const rpmSafe = rpm.slice(0, count).map((v) => safeNumber(v, 0));

  const padL = enlarged ? 64 : 46;
  const padR = enlarged ? 24 : 16;
  const padT = enlarged ? 26 : 18;
  const padB = enlarged ? 54 : 42;

  const plotW = w - padL - padR;
  const plotH = h - padT - padB;

  if (plotW <= 0 || plotH <= 0) return;

  const all = [...loadSafe, ...rpmSafe];
  let minV = Math.min(...all);
  let maxV = Math.max(...all);

  if (minV === maxV) {
    minV -= 1;
    maxV += 1;
  }

  const zeroRatio = (maxV - 0) / (maxV - minV);
  const y0 = padT + zeroRatio * plotH;

  // 수평 grid
  ctx.strokeStyle = "rgba(0,0,0,0.18)";
  ctx.lineWidth = 1;

  for (let i = 0; i <= 5; i++) {
    const y = padT + (plotH / 5) * i;
    ctx.beginPath();
    ctx.moveTo(padL, y);
    ctx.lineTo(padL + plotW, y);
    ctx.stroke();
  }

  // 수직 grid
  const xStep = count > 1 ? plotW / (count - 1) : 0;

  for (let i = 0; i < count; i++) {
    const x = padL + xStep * i;
    ctx.beginPath();
    ctx.moveTo(x, padT);
    ctx.lineTo(x, padT + plotH);
    ctx.stroke();
  }

  // 0축
  if (y0 >= padT && y0 <= padT + plotH) {
    ctx.strokeStyle = "rgba(0,0,0,0.28)";
    ctx.beginPath();
    ctx.moveTo(padL, y0);
    ctx.lineTo(padL + plotW, y0);
    ctx.stroke();
  }

  // x축 라벨
  ctx.fillStyle = "rgba(0,0,0,0.78)";
  ctx.font = enlarged ? "16px system-ui" : "13px system-ui";
  ctx.textAlign = "center";
  ctx.textBaseline = "top";

  const targetLabelCount = enlarged ? 12 : 7;
  const labelEvery = Math.max(1, Math.ceil(count / targetLabelCount));

  for (let i = 0; i < count; i++) {
    const isLast = i === count - 1;
    if (i % labelEvery !== 0 && !isLast) continue;

    const x = padL + xStep * i;
    ctx.fillText(labelsSafe[i] || "", x, padT + plotH + (enlarged ? 14 : 12));
  }

  function drawLine(series, color, width = 3) {
    if (!series.length) return;

    ctx.strokeStyle = color;
    ctx.lineWidth = width;
    ctx.beginPath();

    series.forEach((v, i) => {
      const x = padL + xStep * i;
      const y = padT + ((maxV - v) / (maxV - minV)) * plotH;

      if (i === 0) ctx.moveTo(x, y);
      else ctx.lineTo(x, y);
    });

    ctx.stroke();
  }

  drawLine(loadSafe, "#ff8a00", enlarged ? 3.5 : 3);
  drawLine(rpmSafe, "#00b8ff", enlarged ? 3.5 : 3);
}

/* ---------------------------------------------------------
 * Three.js
 * --------------------------------------------------------- */
let scene;
let camera;
let renderer;
let controls;
let clock;
let resizeObserver = null;

let threeMount;
let placeholder;
let hudState;

let toolRoot = null;
let productRoot = null;

// 이동 기준점
const basePos = new THREE.Vector3(0, 2, -2.2);
const BOUNDS_MIN = basePos.clone();
const BOUNDS_MAX = basePos.clone().addScalar(1);

const HOME_TOOL_POS = new THREE.Vector3(
  (BOUNDS_MIN.x + BOUNDS_MAX.x) * 0.5,
  (BOUNDS_MIN.y + BOUNDS_MAX.y) * 0.5,
  (BOUNDS_MIN.z + BOUNDS_MAX.z) * 0.5
);

// 툴 회전 속도
const TOOL_ROT_SPEED = 0.8;

// 시간 프리셋
const TOOL_ROTATE_TIME = [1.5, 2.5];
const TOOL_MOVE_TIME = [1.2, 2.0];
const TOOL_PAUSE_TIME = [0.5, 1.0];

const PRODUCT_ROTATE_TIME = [0.8, 1.6];
const PRODUCT_PAUSE_TIME = [0.6, 1.2];

function randRange(min, max) {
  return min + Math.random() * (max - min);
}

function hud(text) {
  if (hudState) {
    hudState.textContent = text;
  }
}

function isMachineRunning() {
  const s = (machineStatus || "").toString().toUpperCase();
  return s.includes("START");
}

function shouldReturnHome() {
  const s = (machineStatus || "").toString().toUpperCase();
  return s.includes("STOP") || s.includes("OFFLINE");
}

function clampToBounds(v) {
  v.x = Math.min(BOUNDS_MAX.x, Math.max(BOUNDS_MIN.x, v.x));
  v.y = Math.min(BOUNDS_MAX.y, Math.max(BOUNDS_MIN.y, v.y));
  v.z = Math.min(BOUNDS_MAX.z, Math.max(BOUNDS_MIN.z, v.z));
  return v;
}

// 제품 회전 속도 계산
function rpmToRadPerSec(rpmVal) {
  return Math.max(0, Math.min(2.5, rpmVal / 1200));
}

/* ---------------------------------------------------------
 * Tool state
 * --------------------------------------------------------- */
const toolState = {
  mode: "pause",
  moveAxis: "z",
  moveDir: 1,
  moveStart: new THREE.Vector3(),
  moveTarget: new THREE.Vector3(),
  timer: 0,
  dur: 0,
};

function toolSetPause() {
  toolState.mode = "pause";
  toolState.timer = 0;
  toolState.dur = randRange(TOOL_PAUSE_TIME[0], TOOL_PAUSE_TIME[1]);
  hud("상태: pause");
}

function toolSetRotate() {
  toolState.mode = "rotate";
  toolState.timer = 0;
  toolState.dur = randRange(TOOL_ROTATE_TIME[0], TOOL_ROTATE_TIME[1]);
  hud("상태: rotate");
}

function toolSetMove(axis, dir) {
  if (!toolRoot) {
    toolSetPause();
    return;
  }

  toolState.mode = "move";
  toolState.timer = 0;
  toolState.dur = randRange(TOOL_MOVE_TIME[0], TOOL_MOVE_TIME[1]);
  toolState.moveAxis = axis;
  toolState.moveDir = dir;

  toolState.moveStart.copy(toolRoot.position);
  clampToBounds(toolState.moveStart);

  toolState.moveTarget.copy(toolState.moveStart);

  if (axis === "y") {
    const targetY = dir > 0 ? BOUNDS_MAX.y : BOUNDS_MIN.y;
    const finalY =
      Math.abs(toolState.moveStart.y - targetY) < 1e-6
        ? dir > 0
          ? BOUNDS_MIN.y
          : BOUNDS_MAX.y
        : targetY;

    toolState.moveTarget.y = finalY;
  } else {
    const targetZ = dir > 0 ? BOUNDS_MAX.z : BOUNDS_MIN.z;
    const finalZ =
      Math.abs(toolState.moveStart.z - targetZ) < 1e-6
        ? dir > 0
          ? BOUNDS_MIN.z
          : BOUNDS_MAX.z
        : targetZ;

    toolState.moveTarget.z = finalZ;
  }

  clampToBounds(toolState.moveTarget);
  hud(`상태: move (${axis} ${dir > 0 ? "+" : "-"})`);
}

/* ---------------------------------------------------------
 * Product state
 * --------------------------------------------------------- */
const productState = {
  mode: "pause",
  timer: 0,
  dur: 0,
};

function productSetPause() {
  productState.mode = "pause";
  productState.timer = 0;
  productState.dur = randRange(PRODUCT_PAUSE_TIME[0], PRODUCT_PAUSE_TIME[1]);
}

function productSetRotate() {
  productState.mode = "rotate";
  productState.timer = 0;
  productState.dur = randRange(PRODUCT_ROTATE_TIME[0], PRODUCT_ROTATE_TIME[1]);
}

/* ---------------------------------------------------------
 * Step sequence
 * --------------------------------------------------------- */
const STEPS = [
  { product: { mode: "pause" }, tool: { mode: "rotate" } },
  { tool: { mode: "pause" } },
  { tool: { mode: "move", axis: "z", dir: +1 } },
  { tool: { mode: "pause" } },
  { tool: { mode: "move", axis: "y", dir: -1 } },
  { tool: { mode: "pause" } },
  { product: { mode: "rotate" } },
  { product: { mode: "pause" } },
  { tool: { mode: "move", axis: "y", dir: +1 } },
  { tool: { mode: "pause" } },
  { tool: { mode: "move", axis: "z", dir: -1 } },
  { tool: { mode: "pause" } },
  { tool: { mode: "rotate" } },
];

let stepIndex = 0;
let stepTimer = 0;
let stepDur = 0;

function computeStepDuration(def) {
  let dur = 0;

  if (def.tool) {
    if (def.tool.mode === "rotate") {
      dur = Math.max(dur, randRange(TOOL_ROTATE_TIME[0], TOOL_ROTATE_TIME[1]));
    }
    if (def.tool.mode === "move") {
      dur = Math.max(dur, randRange(TOOL_MOVE_TIME[0], TOOL_MOVE_TIME[1]));
    }
    if (def.tool.mode === "pause") {
      dur = Math.max(dur, randRange(TOOL_PAUSE_TIME[0], TOOL_PAUSE_TIME[1]));
    }
  }

  if (def.product) {
    if (def.product.mode === "rotate") {
      dur = Math.max(dur, randRange(PRODUCT_ROTATE_TIME[0], PRODUCT_ROTATE_TIME[1]));
    }
    if (def.product.mode === "pause") {
      dur = Math.max(dur, randRange(PRODUCT_PAUSE_TIME[0], PRODUCT_PAUSE_TIME[1]));
    }
  }

  return Math.max(0.4, dur);
}

function applyStep(def) {
  if (def.tool) {
    const mode = def.tool.mode;
    if (mode === "pause") toolSetPause();
    if (mode === "rotate") toolSetRotate();
    if (mode === "move") toolSetMove(def.tool.axis, def.tool.dir);
  }

  if (def.product) {
    const mode = def.product.mode;
    if (mode === "pause") productSetPause();
    if (mode === "rotate") productSetRotate();
  }

  stepTimer = 0;
  stepDur = computeStepDuration(def);
}

function returnToolHome(dt) {
  if (!toolRoot) return;

  const lerpAlpha = 1 - Math.exp(-dt * 1.8);
  toolRoot.position.lerp(HOME_TOOL_POS, lerpAlpha);
  clampToBounds(toolRoot.position);

  // 자세도 천천히 원위치 복귀
  toolRoot.rotation.y = THREE.MathUtils.lerp(toolRoot.rotation.y, 0, lerpAlpha * 0.6);
  toolRoot.rotation.x = THREE.MathUtils.lerp(toolRoot.rotation.x, 0, lerpAlpha * 0.6);
  toolRoot.rotation.z = THREE.MathUtils.lerp(toolRoot.rotation.z, 0, lerpAlpha * 0.6);
}

function slowStopProduct(dt) {
  if (!productRoot) return;

  const smooth = 1 - Math.exp(-dt * 2.2);
  productSpinVel = THREE.MathUtils.lerp(productSpinVel, 0, smooth);

  if (Math.abs(productSpinVel) < 0.0001) {
    productSpinVel = 0;
  }

  productRoot.rotation.y += productSpinVel * dt;
}

function updateProductMaterial() {
  if (!productRoot) return;

  // 부하율 기반 색상 보간
  const t01 = Math.max(0, Math.min(1, torq / 10));
  const hue = 140 * (1 - t01);
  const color = new THREE.Color(`hsl(${hue}, 80%, 55%)`);

  productRoot.traverse((o) => {
    if (o.isMesh && o.material && "color" in o.material) {
      o.material.color.copy(color);
    }
  });
}

function initThree() {
  threeMount = $("#threeCanvas");
  placeholder = $("#threePlaceholder");
  hudState = $("#hudState");

  if (!threeMount) {
    throw new Error("#threeCanvas not found");
  }

  clock = new THREE.Clock();

  scene = new THREE.Scene();
  scene.background = new THREE.Color(0xf3f3f3);

  camera = new THREE.PerspectiveCamera(55, 1, 0.1, 1000);
  camera.position.set(4.3, 3.5, 5.8);

  renderer = new THREE.WebGLRenderer({
    antialias: true,
    powerPreference: "high-performance",
  });
  renderer.setPixelRatio(Math.min(window.devicePixelRatio || 1, 2));
  renderer.shadowMap.enabled = true;
  renderer.outputColorSpace = THREE.SRGBColorSpace;

  threeMount.appendChild(renderer.domElement);

  const resize = () => {
    if (!threeMount || !camera || !renderer) return;

    const rect = threeMount.getBoundingClientRect();
    const w = Math.max(1, rect.width);
    const h = Math.max(1, rect.height);

    camera.aspect = w / h;
    camera.updateProjectionMatrix();
    renderer.setSize(w, h, false);
  };

  window.addEventListener("resize", resize);

  resizeObserver = new ResizeObserver(resize);
  resizeObserver.observe(threeMount);

  controls = new OrbitControls(camera, renderer.domElement);
  controls.target.set(0, 0.15, 0);

  controls.enableDamping = true;
  controls.dampingFactor = 0.08;

  controls.zoomSpeed = 0.08;
  controls.panSpeed = 0.5;
  controls.rotateSpeed = 0.5;

  controls.minPolarAngle = 0.15;
  controls.maxPolarAngle = Math.PI / 2 - 0.05;

  controls.minDistance = 2.8;
  controls.maxDistance = 12;

  renderer.domElement.addEventListener(
    "wheel",
    (e) => {
      e.preventDefault();

      if (!camera || !controls) return;

      const zoomStep = 0.06;
      const dir = e.deltaY > 0 ? 1 : -1;

      const offset = camera.position.clone().sub(controls.target);
      const distance = offset.length();

      let nextDistance = distance * (1 + dir * zoomStep);
      nextDistance = Math.max(
        controls.minDistance,
        Math.min(controls.maxDistance, nextDistance)
      );

      offset.setLength(nextDistance);
      camera.position.copy(controls.target.clone().add(offset));
      controls.update();
    },
    { passive: false }
  );

  const hemi = new THREE.HemisphereLight(0xaad1ff, 0x0b0f14, 0.9);
  scene.add(hemi);

  const dir = new THREE.DirectionalLight(0xffffff, 0.8);
  dir.position.set(3, 5, 2);
  dir.castShadow = true;
  scene.add(dir);

  const groundMat = new THREE.MeshStandardMaterial({
    color: 0xe8edf3,
    metalness: 0.08,
    roughness: 0.92,
    polygonOffset: true,
    polygonOffsetFactor: 1,
    polygonOffsetUnits: 1,
  });

  const ground = new THREE.Mesh(
    new THREE.PlaneGeometry(40, 40),
    groundMat
  );
  ground.rotation.x = -Math.PI / 2;
  ground.position.y = -0.62;
  ground.receiveShadow = true;
  scene.add(ground);

  const grid = new THREE.GridHelper(40, 40, 0x36404d, 0x36404d);
  grid.position.y = -0.48;
  scene.add(grid);

  const axes = new THREE.AxesHelper(1.5);
  axes.position.y = -0.49;
  scene.add(axes);

  const base = new THREE.Mesh(
    new THREE.BoxGeometry(3.8, 0.4, 2.4),
    new THREE.MeshStandardMaterial({
      color: 0xf3f3f3,
      metalness: 0.1,
      roughness: 0.9,
    })
  );
  base.position.set(0, -0.3, 0);
  base.castShadow = true;
  base.receiveShadow = true;
  scene.add(base);

  if (placeholder?.style) {
    placeholder.style.display = "none";
  }

  loadFbxModels();
  applyStep(STEPS[stepIndex]);
  resize();
  animate();
}

function loadFbxModels() {
  const loader = new FBXLoader();

  loader.load(
    "/static/assets/CNC_tool.fbx",
    (fbx) => {
      toolRoot = fbx;
      toolRoot.scale.set(0.01, 0.01, 0.01);
      toolRoot.position.copy(HOME_TOOL_POS);

      toolRoot.traverse((o) => {
        if (o.isMesh) {
          o.castShadow = true;
          o.receiveShadow = true;
          o.material = new THREE.MeshStandardMaterial({
            color: 0x999999,
            metalness: 0.7,
            roughness: 0.4,
          });
        }
      });

      scene.add(toolRoot);
    },
    undefined,
    (err) => console.error("FBX load error (CNC_tool):", err)
  );

  loader.load(
    "/static/assets/CNC_product.fbx",
    (fbx) => {
      productRoot = fbx;
      productRoot.scale.set(0.01, 0.01, 0.01);
      productRoot.position.set(0, 0, 0);

      productRoot.traverse((o) => {
        if (o.isMesh) {
          o.castShadow = true;
          o.receiveShadow = true;
        }
      });

      scene.add(productRoot);
    },
    undefined,
    (err) => console.error("FBX load error (CNC_product):", err)
  );
}

function animate() {
  requestAnimationFrame(animate);

  if (!renderer || !scene || !camera || !clock) return;

  const dt = clock.getDelta();
  controls?.update();

  const running = isMachineRunning();
  const returnHome = shouldReturnHome();

  if (running) {
    if (toolRoot) {
      toolState.timer += dt;

      if (toolState.mode === "rotate") {
        toolRoot.rotation.set(
          0,
          toolRoot.rotation.y + TOOL_ROT_SPEED * dt,
          toolRoot.rotation.z
        );
      } else if (toolState.mode === "move") {
        const t01 = Math.min(1, toolState.timer / Math.max(toolState.dur, 0.001));
        toolRoot.position.lerpVectors(toolState.moveStart, toolState.moveTarget, t01);
        clampToBounds(toolRoot.position);
      }
    }

    if (productRoot) {
      const targetSpin = rpmToRadPerSec(rpm);
      const smooth = 1 - Math.exp(-dt * 3.0);
      productSpinVel = THREE.MathUtils.lerp(productSpinVel, targetSpin, smooth);
      productRoot.rotation.y += productSpinVel * dt;
    }

    stepTimer += dt;
    if (stepTimer >= stepDur) {
      stepIndex = (stepIndex + 1) % STEPS.length;
      applyStep(STEPS[stepIndex]);
    }

    hud(`상태: ${machineStatus}`);
  } else if (returnHome) {
    returnToolHome(dt);
    slowStopProduct(dt);
    hud(`상태: ${machineStatus} · home`);
  } else {
    slowStopProduct(dt);
    hud(`상태: ${machineStatus}`);
  }

  updateProductMaterial();
  renderer.render(scene, camera);
}

/* ---------------------------------------------------------
 * Boot
 * --------------------------------------------------------- */
async function boot() {
  initThree();
  setText("#toolValue", "-", "-");

  try {
    await refreshMachineAndDetail();
  } catch (e) {
    console.warn("[detail] initial refresh failed:", e);
  }

  try {
    await loadToolList();
  } catch (e) {
    console.warn("[tools] load failed:", e);
  }

  startPolling();
}

window.addEventListener("beforeunload", () => {
  stopPolling();
  replayChannel?.close?.();
  resizeObserver?.disconnect?.();
});

window.addEventListener("DOMContentLoaded", () => {
  boot().catch((err) => console.error(err));
});
