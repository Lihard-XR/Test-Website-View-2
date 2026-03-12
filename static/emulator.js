/* =========================================================
 * emulator.js
 * - 재생 범위 조회
 * - 특정 시점 스냅샷 조회
 * - 재생 / 일시정지 / 정지
 * - 대시보드와 replay 상태 동기화
 * ========================================================= */

const REPLAY_SYNC_CHANNEL = "factory-replay-sync";
const REPLAY_SYNC_STORAGE_KEY = "factory-replay-state";

const replayChannel =
  typeof BroadcastChannel !== "undefined"
    ? new BroadcastChannel(REPLAY_SYNC_CHANNEL)
    : null;

/* ---------------------------------------------------------
 * 공통 유틸
 * --------------------------------------------------------- */

// 설비 상태를 UI 표시값으로 변환
function statusToUI(status) {
  const s = (status || "").toString().toUpperCase();

  if (s.includes("START")) return { label: "START", color: "#00b050" };
  if (s.includes("READY")) return { label: "READY", color: "#f2a900" };
  if (s.includes("OFF")) return { label: "OFFLINE", color: "#8c8c8c" };
  if (s.includes("STOP")) return { label: "STOP", color: "#e60000" };

  return { label: s || "UNKNOWN", color: "#666" };
}

// null/undefined/빈 문자열 방어
function safe(v, d = "-") {
  return v === null || v === undefined || v === "" ? d : v;
}

// 숫자 포맷
function fmtNumber(v) {
  if (v === null || v === undefined || v === "" || Number.isNaN(Number(v))) {
    return "-";
  }
  return Number(v).toLocaleString("en-US");
}

// HTML 삽입용 최소 escape
function escapeHtml(value) {
  return String(value)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

// 공통 fetch + JSON 파싱
async function fetchJson(url) {
  const res = await fetch(url, { cache: "no-store" });

  let data = null;
  try {
    data = await res.json();
  } catch (err) {
    throw new Error(`JSON 파싱 실패: ${err.message}`);
  }

  if (!res.ok) {
    throw new Error(data?.message || `HTTP ${res.status}`);
  }

  return data;
}

/* ---------------------------------------------------------
 * DOM
 * --------------------------------------------------------- */
const els = {
  date: document.getElementById("targetDate"),
  time: document.getElementById("targetTime"),
  slider: document.getElementById("timelineSlider"),
  status: document.getElementById("emulatorStatus"),
  rangeStart: document.getElementById("rangeStart"),
  rangeEnd: document.getElementById("rangeEnd"),
  currentTimestamp: document.getElementById("currentTimestamp"),
  readInterval: document.getElementById("readInterval"),
  fetchInterval: document.getElementById("fetchInterval"),
  play: document.getElementById("playBtn"),
  pause: document.getElementById("pauseBtn"),
  stop: document.getElementById("stopBtn"),
  speedGroup: document.getElementById("speedGroup"),
  replayGrid: document.getElementById("replayGrid"),
};

function hasRequiredElements() {
  return !!(
    els.date &&
    els.time &&
    els.slider &&
    els.status &&
    els.rangeStart &&
    els.rangeEnd &&
    els.currentTimestamp &&
    els.readInterval &&
    els.fetchInterval &&
    els.play &&
    els.pause &&
    els.stop &&
    els.speedGroup &&
    els.replayGrid
  );
}

/* ---------------------------------------------------------
 * 카드 렌더링
 * --------------------------------------------------------- */
function renderReplayCards(rows) {
  if (!els.replayGrid) return;

  els.replayGrid.innerHTML = "";

  rows.forEach((r) => {
    const ui = statusToUI(r.status);

    const util =
      r.utilization_rate !== null && r.utilization_rate !== undefined
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

    // 서버 응답값은 escape 후 삽입
    el.innerHTML = `
      <div class="head">${escapeHtml(safe(r.name))}</div>
      <div class="body">
        <div class="toprow">
          <div class="circle" style="background:${ui.color}">${escapeHtml(ui.label)}</div>
          <div class="side">
            <div class="sideitem">
              <div class="k">가동률</div>
              <div class="v">${escapeHtml(util)}<span class="unit"> %</span></div>
            </div>
            <div class="sdiv"></div>
            <div class="sideitem">
              <div class="k">생산량</div>
              <div class="v">${escapeHtml(partCount)}<span class="unit"> EA</span></div>
            </div>
          </div>
        </div>
        <div class="stats">
          <div class="cell">
            <div class="k">생산시간</div>
            <div class="smallv">${escapeHtml(opMin)} 분</div>
          </div>
          <div class="cell">
            <div class="k">프로그램</div>
            <div class="smallv">${escapeHtml(onum)}</div>
          </div>
          <div class="cell">
            <div class="k">알람코드</div>
            <div class="smallv">${escapeHtml(alarm)}</div>
          </div>
          <div class="cell">
            <div class="k">툴번호</div>
            <div class="smallv">${escapeHtml(toolNo)}</div>
          </div>
        </div>
      </div>
    `;

    els.replayGrid.appendChild(el);
  });
}

/* ---------------------------------------------------------
 * 상태
 * --------------------------------------------------------- */
const state = {
  speed: 1,
  timerId: null,
  secondsFromStart: 0,
  maxSeconds: 0,
  baseDate: null,
  playing: false,
  stepSec: Math.max(1, Number(window.REPLAY_STEP_SEC || 5)),
  playToken: 0,
  isFetchingFrame: false,
};

/* ---------------------------------------------------------
 * 날짜/시간 포맷
 * --------------------------------------------------------- */
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
  if (!(dateObj instanceof Date) || Number.isNaN(dateObj.getTime())) {
    return null;
  }
  return `${formatDateInput(dateObj)} ${formatTimeInput(dateObj)}`;
}

function parseServerDateTime(value) {
  if (!value) return null;

  const normalized = value.trim().replace(" ", "T");
  const d = new Date(normalized);

  return Number.isNaN(d.getTime()) ? null : d;
}

function combineInputsToDate() {
  if (!els.date?.value || !els.time?.value) return null;

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

/* ---------------------------------------------------------
 * Replay 상태 브로드캐스트
 * --------------------------------------------------------- */
function publishReplayState(mode = "replay") {
  let payload;

  if (mode === "replay") {
    const current = combineInputsToDate();
    const at = formatLocalDateTime(current);

    if (!at) {
      console.warn("[emulator] publish replay skipped: invalid datetime");
      return;
    }

    payload = { mode: "replay", at };
  } else {
    payload = { mode: "live", at: null };
  }

  try {
    localStorage.setItem(REPLAY_SYNC_STORAGE_KEY, JSON.stringify(payload));
  } catch (e) {
    console.warn("[emulator] failed to persist replay state:", e);
  }

  replayChannel?.postMessage(payload);
}

/* ---------------------------------------------------------
 * UI 반영
 * --------------------------------------------------------- */
function updateUiByState() {
  if (!hasRequiredElements()) return;

  els.slider.value = String(state.secondsFromStart);

  els.currentTimestamp.textContent = state.baseDate
    ? formatLocalDateTime(
        new Date(state.baseDate.getTime() + state.secondsFromStart * 1000)
      ) || "-"
    : "-";

  els.readInterval.textContent = `기본 ${state.stepSec}초 간격 · 재생 x${state.speed}`;
}

function updateSpeedButtons() {
  document.querySelectorAll(".speed-btn").forEach((btn) => {
    const isActive = Number(btn.dataset.speed || 1) === state.speed;
    btn.classList.toggle("active", isActive);
    btn.setAttribute("aria-pressed", isActive ? "true" : "false");
  });
}

/* ---------------------------------------------------------
 * API
 * --------------------------------------------------------- */
async function fetchRangeByDate(dateStr) {
  return fetchJson(`/api/replay/range?date=${encodeURIComponent(dateStr)}`);
}

async function fetchSnapshot(dateObj) {
  const localAt = formatLocalDateTime(dateObj);
  if (!localAt) {
    throw new Error("유효한 날짜/시간이 아닙니다.");
  }

  return fetchJson(`/api/replay/snapshot?at=${encodeURIComponent(localAt)}`);
}

/* ---------------------------------------------------------
 * 범위 동기화
 * --------------------------------------------------------- */
async function syncRangeFromDate(dateStr) {
  try {
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
    state.maxSeconds = Math.max(
      0,
      Math.floor((maxDate.getTime() - minDate.getTime()) / 1000)
    );

    els.slider.min = "0";
    els.slider.max = String(state.maxSeconds);
    els.slider.step = String(state.stepSec);

    els.rangeStart.textContent = formatTimeInput(minDate);
    els.rangeEnd.textContent = formatTimeInput(maxDate);
    els.status.textContent = `${dateStr} 기록 범위 로드 완료`;

    return true;
  } catch (err) {
    console.error("[emulator] range load failed:", err);
    els.status.textContent = err.message || "기록 범위를 불러오지 못했습니다.";
    return false;
  }
}

/* ---------------------------------------------------------
 * 현재 프레임 로드
 * --------------------------------------------------------- */
async function loadCurrentFrame(requestToken = state.playToken) {
  const dt = combineInputsToDate();
  if (!dt) return;

  if (state.isFetchingFrame) return;
  state.isFetchingFrame = true;

  try {
    const data = await fetchSnapshot(dt);

    // 재생 토큰이 바뀌었으면 이전 요청 결과는 무시
    if (requestToken !== state.playToken) return;

    if (!data.ok) {
      els.status.textContent = data.message || "스냅샷을 불러오지 못했습니다.";
      return;
    }

    renderReplayCards(Array.isArray(data.rows) ? data.rows : []);

    const tsText = data.target_at || formatLocalDateTime(dt) || "-";
    els.currentTimestamp.textContent = tsText;
    els.status.textContent = `${tsText} 기준 ${data.count ?? 0}개 설비 조회`;

    publishReplayState("replay");
  } catch (err) {
    console.error("[emulator] snapshot load failed:", err);
    els.status.textContent = err.message || "스냅샷을 불러오지 못했습니다.";
  } finally {
    state.isFetchingFrame = false;
  }
}

/* ---------------------------------------------------------
 * 재생 위치 이동
 * --------------------------------------------------------- */
function setCurrentTimeFromOffset(offsetSec) {
  if (!state.baseDate) return;

  state.secondsFromStart = Math.max(0, Math.min(offsetSec, state.maxSeconds));

  const current = new Date(
    state.baseDate.getTime() + state.secondsFromStart * 1000
  );

  els.date.value = formatDateInput(current);
  els.time.value = formatTimeInput(current);

  updateUiByState();
}

async function moveToOffset(offsetSec, requestToken = state.playToken) {
  setCurrentTimeFromOffset(offsetSec);
  await loadCurrentFrame(requestToken);
}

/* ---------------------------------------------------------
 * 재생 제어
 * --------------------------------------------------------- */
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
  // 기존 재생 중지 + 새 토큰 발급
  stopPlayback();

  state.playing = true;
  const loopToken = state.playToken;

  playbackLoop(loopToken);
}

/* ---------------------------------------------------------
 * 초기 날짜 설정
 * --------------------------------------------------------- */
async function initDefaultDate() {
  const now = new Date();

  els.date.value = formatDateInput(now);
  els.time.value = formatTimeInput(now);
  els.slider.step = String(state.stepSec);

  updateSpeedButtons();

  const ok = await syncRangeFromDate(els.date.value);
  if (!ok) return;

  const picked = combineInputsToDate();

  const clamped =
    picked && state.baseDate
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

/* ---------------------------------------------------------
 * 이벤트 바인딩
 * --------------------------------------------------------- */
function bindEvents() {
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

    const offsetSec = Math.floor(
      (picked.getTime() - state.baseDate.getTime()) / 1000
    );

    await moveToOffset(offsetSec);
  });

  // 슬라이더 이동 중 바로 프레임 갱신
  els.slider.addEventListener("input", async (e) => {
    stopPlayback();
    await moveToOffset(Number(e.target.value));
  });

  els.play.addEventListener("click", () => {
    startPlayback();
  });

  els.pause.addEventListener("click", () => {
    stopPlayback();
  });

  els.stop.addEventListener("click", async () => {
    stopPlayback();
    await moveToOffset(0);
  });

  els.speedGroup.addEventListener("click", (e) => {
    const btn = e.target.closest(".speed-btn");
    if (!btn) return;

    state.speed = Math.max(1, Number(btn.dataset.speed || 1));
    updateSpeedButtons();
    updateUiByState();

    if (state.playing) {
      startPlayback();
    }
  });

  window.addEventListener("beforeunload", () => {
    stopPlayback();
    publishReplayState("live");
    replayChannel?.close?.();
  });
}

/* ---------------------------------------------------------
 * 초기 실행
 * --------------------------------------------------------- */
async function boot() {
  if (!hasRequiredElements()) {
    console.warn("[emulator] required elements not found");
    return;
  }

  updateUiByState();
  bindEvents();
  await initDefaultDate();
}

boot().catch((err) => {
  console.error("[emulator] boot failed:", err);
  if (els.status) {
    els.status.textContent = err.message || "초기화에 실패했습니다.";
  }
});
