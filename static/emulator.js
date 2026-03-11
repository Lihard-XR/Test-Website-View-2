const REPLAY_SYNC_CHANNEL = "factory-replay-sync";
const REPLAY_SYNC_STORAGE_KEY = "factory-replay-state";
const replayChannel = typeof BroadcastChannel !== "undefined"
  ? new BroadcastChannel(REPLAY_SYNC_CHANNEL)
  : null;

function statusToUI(status) {
  const s = (status || "").toString().toUpperCase();
  if (s.includes("START")) return { label: "START", color: "#00b050" };
  if (s.includes("READY")) return { label: "READY", color: "#f2a900" };
  if (s.includes("OFF")) return { label: "OFFLINE", color: "#8c8c8c" };
  if (s.includes("STOP")) return { label: "STOP", color: "#e60000" };
  return { label: s || "UNKNOWN", color: "#666" };
}

function safe(v, d = "-") {
  return (v === null || v === undefined || v === "") ? d : v;
}

function fmtNumber(v) {
  if (v === null || v === undefined || v === "" || Number.isNaN(Number(v))) return "-";
  return Number(v).toLocaleString("en-US");
}

function renderReplayCards(rows) {
  const grid = document.getElementById("replayGrid");
  grid.innerHTML = "";

  rows.forEach((r) => {
    const ui = statusToUI(r.status);
    const util = r.utilization_rate !== null && r.utilization_rate !== undefined
      ? Number(r.utilization_rate).toFixed(1)
      : "-";

    const partCount = fmtNumber(
      r.part_count_today !== null && r.part_count_today !== undefined
        ? r.part_count_today
        : r.part_count
    );

    const opMin = fmtNumber(r.total_operating_min ?? r.operating_min);
    const onum = safe(r.onum, "-");
    const toolNo = safe(r.tool_no, "-");
    const alarm = safe(r.alarm, "0");

    const el = document.createElement("div");
    el.className = "card";
    el.innerHTML = `
      <div class="head">${safe(r.name)}</div>
      <div class="body">
        <div class="toprow">
          <div class="circle" style="background:${ui.color}">${ui.label}</div>
          <div class="side">
            <div class="sideitem">
              <div class="k">가동률</div>
              <div class="v">${util}<span class="unit"> %</span></div>
            </div>
            <div class="sdiv"></div>
            <div class="sideitem">
              <div class="k">생산량</div>
              <div class="v">${partCount}<span class="unit"> EA</span></div>
            </div>
          </div>
        </div>
        <div class="stats">
          <div class="cell">
            <div class="k">생산시간</div>
            <div class="smallv">${opMin} 분</div>
          </div>
          <div class="cell">
            <div class="k">프로그램</div>
            <div class="smallv">${onum}</div>
          </div>
          <div class="cell">
            <div class="k">알람코드</div>
            <div class="smallv">${alarm}</div>
          </div>
          <div class="cell">
            <div class="k">툴번호</div>
            <div class="smallv">${toolNo}</div>
          </div>
        </div>
      </div>
    `;
    grid.appendChild(el);
  });
}

const state = {
  speed: 1,
  timerId: null,
  secondsFromStart: 0,
  maxSeconds: 0,
  baseDate: null,
  playing: false,
  stepSec: Number(window.REPLAY_STEP_SEC || 5),
  playToken: 0,
  isFetchingFrame: false,
};

const els = {
  date: document.getElementById("targetDate"),
  time: document.getElementById("targetTime"),
  slider: document.getElementById("timelineSlider"),
  status: document.getElementById("emulatorStatus"),
  rangeStart: document.getElementById("rangeStart"),
  rangeEnd: document.getElementById("rangeEnd"),
  currentTimestamp: document.getElementById("currentTimestamp"),
  readInterval: document.getElementById("readInterval"),
  play: document.getElementById("playBtn"),
  pause: document.getElementById("pauseBtn"),
  stop: document.getElementById("stopBtn"),
  speedGroup: document.getElementById("speedGroup"),
};

function pad(n) {
  return String(n).padStart(2, "0");
}

function formatDateInput(d) {
  return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())}`;
}

function formatTimeInput(d) {
  return `${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}`;
}

function formatLocalDateTime(dateObj) {
  return `${formatDateInput(dateObj)} ${formatTimeInput(dateObj)}`;
}

function parseServerDateTime(value) {
  if (!value) return null;
  const normalized = value.trim().replace(" ", "T");
  const d = new Date(normalized);
  return Number.isNaN(d.getTime()) ? null : d;
}

function combineInputsToDate() {
  if (!els.date.value || !els.time.value) return null;

  const [year, month, day] = els.date.value.split("-").map(Number);
  const [hour, minute, second] = els.time.value.split(":").map(Number);

  const d = new Date(
    year,
    (month || 1) - 1,
    day || 1,
    hour || 0,
    minute || 0,
    second || 0,
    0
  );

  return Number.isNaN(d.getTime()) ? null : d;
}

function publishReplayState(mode = "replay") {
  const payload = mode === "replay"
    ? { mode: "replay", at: formatLocalDateTime(combineInputsToDate()) }
    : { mode: "live", at: null };

  try {
    localStorage.setItem(REPLAY_SYNC_STORAGE_KEY, JSON.stringify(payload));
  } catch (e) {
    console.warn("[emulator] failed to persist replay state:", e);
  }
  replayChannel?.postMessage(payload);
}

function updateUiByState() {
  els.slider.value = String(state.secondsFromStart);
  els.currentTimestamp.textContent = state.baseDate
    ? formatLocalDateTime(new Date(state.baseDate.getTime() + state.secondsFromStart * 1000))
    : "-";
  els.readInterval.textContent = `기본 ${state.stepSec}초 간격 · 재생 x${state.speed}`;
}

async function fetchRangeByDate(dateStr) {
  const res = await fetch(`/api/replay/range?date=${encodeURIComponent(dateStr)}`, { cache: "no-store" });
  return res.json();
}

async function fetchSnapshot(dateObj) {
  const localAt = formatLocalDateTime(dateObj);
  const res = await fetch(`/api/replay/snapshot?at=${encodeURIComponent(localAt)}`, { cache: "no-store" });
  return res.json();
}

async function syncRangeFromDate(dateStr) {
  const data = await fetchRangeByDate(dateStr);
  if (!data.ok) {
    els.status.textContent = data.message || "기록 범위를 불러오지 못했습니다.";
    return false;
  }

  const minDate = parseServerDateTime(data.min);
  const maxDate = parseServerDateTime(data.max);

  if (!minDate || !maxDate) {
    els.status.textContent = "기록 범위 날짜 파싱에 실패했습니다.";
    return false;
  }

  state.baseDate = minDate;
  state.maxSeconds = Math.max(0, Math.floor((maxDate.getTime() - minDate.getTime()) / 1000));

  els.slider.min = "0";
  els.slider.max = String(state.maxSeconds);
  els.slider.step = String(state.stepSec);
  els.rangeStart.textContent = formatTimeInput(minDate);
  els.rangeEnd.textContent = formatTimeInput(maxDate);
  els.status.textContent = `${dateStr} 기록 범위 로드 완료`;
  return true;
}

async function loadCurrentFrame(requestToken = state.playToken) {
  const dt = combineInputsToDate();
  if (!dt) return;

  if (state.isFetchingFrame) return;
  state.isFetchingFrame = true;

  try {
    const data = await fetchSnapshot(dt);
    if (requestToken !== state.playToken) return;

    if (!data.ok) {
      els.status.textContent = data.message || "스냅샷을 불러오지 못했습니다.";
      return;
    }

    renderReplayCards(data.rows || []);
    const tsText = data.target_at || formatLocalDateTime(dt);
    els.currentTimestamp.textContent = tsText;
    els.status.textContent = `${tsText} 기준 ${data.count}개 설비 조회`;
    publishReplayState("replay");
  } finally {
    state.isFetchingFrame = false;
  }
}

function setCurrentTimeFromOffset(offsetSec) {
  if (!state.baseDate) return;

  state.secondsFromStart = Math.max(0, Math.min(offsetSec, state.maxSeconds));
  const current = new Date(state.baseDate.getTime() + state.secondsFromStart * 1000);

  els.date.value = formatDateInput(current);
  els.time.value = formatTimeInput(current);
  updateUiByState();
}

async function moveToOffset(offsetSec, requestToken = state.playToken) {
  setCurrentTimeFromOffset(offsetSec);
  await loadCurrentFrame(requestToken);
}

function stopPlayback() {
  state.playing = false;
  state.playToken += 1;

  if (state.timerId) {
    clearTimeout(state.timerId);
    state.timerId = null;
  }
}

async function playbackLoop(loopToken) {
  if (!state.playing) return;
  if (loopToken !== state.playToken) return;

  if (state.secondsFromStart >= state.maxSeconds) {
    stopPlayback();
    return;
  }

  await moveToOffset(
    Math.min(state.secondsFromStart + state.stepSec, state.maxSeconds),
    loopToken
  );

  if (!state.playing) return;
  if (loopToken !== state.playToken) return;

  state.timerId = setTimeout(() => {
    playbackLoop(loopToken);
  }, 1000 / state.speed);
}

function startPlayback() {
  stopPlayback();
  state.playing = true;

  const loopToken = state.playToken;
  playbackLoop(loopToken);
}

async function initDefaultDate() {
  const now = new Date();
  els.date.value = formatDateInput(now);
  els.time.value = formatTimeInput(now);

  const ok = await syncRangeFromDate(els.date.value);
  if (!ok) return;

  const picked = combineInputsToDate();
  const clamped = picked && state.baseDate
    ? Math.max(
        0,
        Math.min(
          Math.floor((picked.getTime() - state.baseDate.getTime()) / 1000),
          state.maxSeconds
        )
      )
    : 0;

  await moveToOffset(clamped);
}

els.date.addEventListener("change", async () => {
  stopPlayback();
  const ok = await syncRangeFromDate(els.date.value);
  if (!ok) return;
  await moveToOffset(0);
});

els.time.addEventListener("change", async () => {
  stopPlayback();
  const picked = combineInputsToDate();
  if (!picked || !state.baseDate) return;

  const offsetSec = Math.floor((picked.getTime() - state.baseDate.getTime()) / 1000);
  await moveToOffset(offsetSec);
});

els.slider.addEventListener("input", async (e) => {
  stopPlayback();
  await moveToOffset(Number(e.target.value));
});

els.play.addEventListener("click", () => startPlayback());
els.pause.addEventListener("click", () => stopPlayback());
els.stop.addEventListener("click", async () => {
  stopPlayback();
  await moveToOffset(0);
});

els.speedGroup.addEventListener("click", (e) => {
  const btn = e.target.closest(".speed-btn");
  if (!btn) return;

  state.speed = Number(btn.dataset.speed || 1);
  document.querySelectorAll(".speed-btn").forEach((node) => node.classList.remove("active"));
  btn.classList.add("active");
  updateUiByState();

  if (state.playing) startPlayback();
});

window.addEventListener("beforeunload", () => {
  publishReplayState("live");
});

initDefaultDate();
