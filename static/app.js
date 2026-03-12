const REPLAY_SYNC_CHANNEL = "factory-replay-sync";
const REPLAY_SYNC_STORAGE_KEY = "factory-replay-state";

const replayChannel =
  typeof BroadcastChannel !== "undefined"
    ? new BroadcastChannel(REPLAY_SYNC_CHANNEL)
    : null;

/* --------------------------------------------------
 * 공통 유틸
 * -------------------------------------------------- */

// 상태 문자열을 UI 표시용 값으로 변환
function statusToUI(status) {
  const s = (status || "").toString().toUpperCase();

  if (s.includes("START")) return { label: "START", color: "#00b050" };
  if (s.includes("READY")) return { label: "READY", color: "#f2a900" };
  if (s.includes("OFF")) return { label: "OFFLINE", color: "#8c8c8c" };
  if (s.includes("STOP")) return { label: "STOP", color: "#e60000" };

  return { label: s || "UNKNOWN", color: "#666" };
}

// null/undefined/빈문자 방어
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

// 숫자 문자열을 안전하게 변환
function toFiniteNumber(value, fallback = 0) {
  const n = Number(value);
  return Number.isFinite(n) ? n : fallback;
}

// 공통 fetch JSON
async function fetchJson(url, options = {}) {
  const res = await fetch(url, { cache: "no-store", ...options });

  let data = null;
  try {
    data = await res.json();
  } catch (err) {
    throw new Error(`JSON 파싱 실패: ${err.message}`);
  }

  return { res, data };
}

/* --------------------------------------------------
 * 리플레이 동기화 상태
 * -------------------------------------------------- */

function getReplaySyncState() {
  try {
    const raw = localStorage.getItem(REPLAY_SYNC_STORAGE_KEY);
    if (!raw) return { mode: "live", at: null };

    const parsed = JSON.parse(raw);
    if (parsed?.mode === "replay" && parsed?.at) {
      return { mode: "replay", at: parsed.at };
    }
  } catch (e) {
    console.warn("[dashboard] failed to parse replay sync state:", e);
  }

  return { mode: "live", at: null };
}

const appState = {
  sync: getReplaySyncState(),
};

let running = false;

/* --------------------------------------------------
 * 메인 카드 렌더링
 * -------------------------------------------------- */

function render(rows) {
  const grid = document.getElementById("grid");
  if (!grid) return;

  grid.innerHTML = "";

  rows.forEach((r) => {
    const ui = statusToUI(r.status);
    const name = safe(r.name);
    const util =
      r.utilization_rate !== null && r.utilization_rate !== undefined
        ? toFiniteNumber(r.utilization_rate).toFixed(1)
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
    el.style.cursor = "pointer";

    // 서버값 삽입 구간은 escape 처리
    el.innerHTML = `
      <div class="head">${escapeHtml(name)}</div>
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

    el.addEventListener("click", () => {
      const ip = safe(r.ip, "");
      if (!ip) return;
      window.location.href = `/machine/${encodeURIComponent(ip)}`;
    });

    grid.appendChild(el);
  });
}

/* --------------------------------------------------
 * 대시보드 데이터 조회
 * -------------------------------------------------- */

async function fetchDashboardData() {
  if (appState.sync.mode === "replay" && appState.sync.at) {
    const { res, data } = await fetchJson(
      `/api/replay/snapshot?at=${encodeURIComponent(appState.sync.at)}`
    );
    return { res, data, mode: "replay" };
  }

  const { res, data } = await fetchJson("/api/machines");
  return { res, data, mode: "live" };
}

async function tickOnce() {
  if (running) return;
  running = true;

  const lastUpdateEl = document.getElementById("lastUpdate");
  const t0 = performance.now();

  try {
    const { res, data, mode } = await fetchDashboardData();

    if (res.status === 503) {
      if (lastUpdateEl) {
        lastUpdateEl.innerText = `Warming up... ${data?.detail || ""}`;
      }
      return;
    }

    if (!res.ok) {
      throw new Error(data?.message || `HTTP ${res.status}`);
    }

    if (Array.isArray(data?.rows)) {
      render(data.rows);

      const dt = Math.round(performance.now() - t0);

      if (mode === "replay") {
        if (lastUpdateEl) {
          lastUpdateEl.innerText =
            `Replay: ${data.target_at || appState.sync.at} ` +
            `(api ${dt}ms, ${data.count ?? data.rows.length} machines)`;
        }
      } else {
        const refreshedAt = data.refreshed_at
          ? new Date(data.refreshed_at * 1000).toLocaleTimeString()
          : "-";

        if (lastUpdateEl) {
          lastUpdateEl.innerText =
            `Last update: ${new Date().toLocaleString()} ` +
            `(api ${dt}ms, server refreshed ${refreshedAt})`;
        }
      }
    } else {
      throw new Error("응답 형식이 올바르지 않습니다.");
    }
  } catch (e) {
    if (lastUpdateEl) {
      lastUpdateEl.innerText = `Fetch error: ${e.message || e}`;
    }
    console.error("[dashboard] tickOnce error:", e);
  } finally {
    running = false;
    setTimeout(tickOnce, 2000);
  }
}

function applyReplaySyncState(nextState) {
  const mode = nextState?.mode === "replay" && nextState?.at ? "replay" : "live";
  const at = mode === "replay" ? nextState.at : null;

  const changed = appState.sync.mode !== mode || appState.sync.at !== at;
  appState.sync = { mode, at };

  if (changed && !running) {
    tickOnce();
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

/* --------------------------------------------------
 * 에뮬레이터 팝업
 * -------------------------------------------------- */

function openEmulatorWindow() {
  const width = 1320;
  const height = 860;
  const left = Math.max(0, Math.round((window.screen.width - width) / 2));
  const top = Math.max(0, Math.round((window.screen.height - height) / 2));

  const features = [
    `width=${width}`,
    `height=${height}`,
    `left=${left}`,
    `top=${top}`,
    "resizable=yes",
    "scrollbars=yes",
  ].join(",");

  const popup = window.open("/emulator", "factory_emulator", features);
  if (popup) {
    popup.focus();
  }
}

document
  .getElementById("emulatorLauncher")
  ?.addEventListener("click", openEmulatorWindow);

/* --------------------------------------------------
 * 설비가동 현황 모달
 * -------------------------------------------------- */

const operationEls = {
  openBtn: document.getElementById("operationStatusBtn"),
  backdrop: document.getElementById("operationModalBackdrop"),
  closeBtn: document.getElementById("operationModalClose"),
  startDate: document.getElementById("operationStartDate"),
  endDate: document.getElementById("operationEndDate"),
  keyword: document.getElementById("operationKeyword"),
  searchBtn: document.getElementById("operationSearchBtn"),
  excelBtn: document.getElementById("operationExcelBtn"),
  tableBody: document.getElementById("operationTableBody"),
  tableFoot: document.getElementById("operationTableFoot"),
  chart: document.getElementById("operationChart"),
};

function formatDateOnly(d) {
  const yyyy = d.getFullYear();
  const mm = String(d.getMonth() + 1).padStart(2, "0");
  const dd = String(d.getDate()).padStart(2, "0");
  return `${yyyy}-${mm}-${dd}`;
}

function hasOperationModalElements() {
  return !!(
    operationEls.openBtn &&
    operationEls.backdrop &&
    operationEls.closeBtn &&
    operationEls.startDate &&
    operationEls.endDate &&
    operationEls.keyword &&
    operationEls.searchBtn &&
    operationEls.excelBtn &&
    operationEls.tableBody &&
    operationEls.chart
  );
}

// 기본 조회 기간 설정
function initOperationModalDates() {
  if (!operationEls.startDate || !operationEls.endDate) return;

  const today = new Date();
  const weekAgo = new Date();
  weekAgo.setDate(today.getDate() - 7);

  operationEls.startDate.value = formatDateOnly(weekAgo);
  operationEls.endDate.value = formatDateOnly(today);
}

// 모달 열기
let lastFocusedBeforeModal = null;

function openOperationModal() {
  if (!operationEls.backdrop) return;

  lastFocusedBeforeModal = document.activeElement;

  operationEls.backdrop.classList.add("open");
  operationEls.backdrop.setAttribute("aria-hidden", "false");
  operationEls.openBtn?.setAttribute("aria-expanded", "true");

  operationEls.closeBtn?.focus();
}

// 모달 닫기
function closeOperationModal() {
  if (!operationEls.backdrop) return;

  const returnTarget = lastFocusedBeforeModal || operationEls.openBtn;
  returnTarget?.focus();

  operationEls.backdrop.classList.remove("open");
  operationEls.backdrop.setAttribute("aria-hidden", "true");
  operationEls.openBtn?.setAttribute("aria-expanded", "false");
}

// 조회 조건 검증
function validateOperationFilters() {
  const startDate = operationEls.startDate?.value || "";
  const endDate = operationEls.endDate?.value || "";

  if (!startDate || !endDate) {
    alert("시작일과 종료일을 모두 입력해 주세요.");
    return false;
  }

  if (startDate > endDate) {
    alert("시작일은 종료일보다 클 수 없습니다.");
    return false;
  }

  return true;
}

// 합계 행 렌더링
function renderOperationSummary(rows) {
  if (!operationEls.tableFoot) return;

  operationEls.tableFoot.innerHTML = "";

  if (!rows.length) return;

  const totalOperating = rows.reduce(
    (sum, row) => sum + toFiniteNumber(row.operating_min, 0),
    0
  );
  const totalDowntime = rows.reduce(
    (sum, row) => sum + toFiniteNumber(row.downtime_min, 0),
    0
  );
  const avgUtil =
    rows.length > 0
      ? rows.reduce((sum, row) => sum + toFiniteNumber(row.utilization_rate, 0), 0) / rows.length
      : 0;

  const tr = document.createElement("tr");
  tr.className = "summary-row";
  tr.innerHTML = `
    <td colspan="2">합계 / 평균</td>
    <td>${avgUtil.toFixed(1)}</td>
    <td>${totalOperating.toFixed(1)}</td>
    <td>${totalDowntime.toFixed(1)}</td>
  `;

  operationEls.tableFoot.appendChild(tr);
}

// 표 렌더링
function renderOperationTable(rows) {
  if (!operationEls.tableBody) return;

  operationEls.tableBody.innerHTML = "";

  if (!rows.length) {
    operationEls.tableBody.innerHTML = `
      <tr>
        <td colspan="5" class="empty-cell">조회 결과가 없습니다.</td>
      </tr>
    `;
    renderOperationSummary([]);
    return;
  }

  rows.forEach((row) => {
    const tr = document.createElement("tr");

    tr.innerHTML = `
      <td>${escapeHtml(row.name ?? "-")}</td>
      <td>${escapeHtml(row.ip ?? "-")}</td>
      <td>${toFiniteNumber(row.utilization_rate, 0).toFixed(1)}</td>
      <td>${toFiniteNumber(row.operating_min, 0).toFixed(1)}</td>
      <td>${toFiniteNumber(row.downtime_min, 0).toFixed(1)}</td>
    `;

    operationEls.tableBody.appendChild(tr);
  });

  renderOperationSummary(rows);
}

// 차트 빈 상태 표시
function drawChartEmptyState(ctx, w, h, message = "조회 결과가 없습니다.") {
  ctx.clearRect(0, 0, w, h);
  ctx.fillStyle = "#f4f7fb";
  ctx.fillRect(0, 0, w, h);

  ctx.fillStyle = "#667085";
  ctx.font = "14px sans-serif";
  ctx.textAlign = "center";
  ctx.textBaseline = "middle";
  ctx.fillText(message, w / 2, h / 2);
}

// 차트 렌더링
function renderOperationChart(rows) {
  if (!operationEls.chart) return;

  const canvas = operationEls.chart;
  const ctx = canvas.getContext("2d");
  if (!ctx) return;

  const rect = canvas.getBoundingClientRect();
  if (!rect.width || !rect.height) return;

  const dpr = Math.max(1, window.devicePixelRatio || 1);
  canvas.width = Math.floor(rect.width * dpr);
  canvas.height = Math.floor(rect.height * dpr);

  // 이전 transform 누적 방지
  ctx.setTransform(1, 0, 0, 1, 0, 0);
  ctx.scale(dpr, dpr);

  const w = rect.width;
  const h = rect.height;

  if (!rows.length) {
    drawChartEmptyState(ctx, w, h);
    return;
  }

  ctx.clearRect(0, 0, w, h);
  ctx.fillStyle = "#f4f7fb";
  ctx.fillRect(0, 0, w, h);

  const padL = 60;
  const padR = 20;
  const padT = 20;
  const padB = 60;
  const plotW = w - padL - padR;
  const plotH = h - padT - padB;

  const maxVal = 100;
  const count = rows.length;

  // 막대/간격 자동 계산
  const barGap = count > 1 ? Math.max(8, Math.min(18, plotW * 0.02)) : 0;
  const barWidth = Math.max(18, (plotW - barGap * (count - 1)) / count);

  // 그리드
  ctx.strokeStyle = "rgba(0,0,0,0.15)";
  ctx.lineWidth = 1;

  for (let i = 0; i <= 5; i++) {
    const y = padT + (plotH / 5) * i;

    ctx.beginPath();
    ctx.moveTo(padL, y);
    ctx.lineTo(padL + plotW, y);
    ctx.stroke();

    const val = 100 - i * 20;
    ctx.fillStyle = "#445";
    ctx.font = "12px sans-serif";
    ctx.textAlign = "right";
    ctx.textBaseline = "middle";
    ctx.fillText(String(val), padL - 8, y);
  }

  // 막대
  rows.forEach((row, idx) => {
    const x = padL + idx * (barWidth + barGap);
    const value = Math.max(0, Math.min(100, toFiniteNumber(row.utilization_rate, 0)));
    const barH = (value / maxVal) * plotH;
    const y = padT + plotH - barH;

    ctx.fillStyle = "#5b8def";
    ctx.fillRect(x, y, barWidth, barH);

    ctx.fillStyle = "#223";
    ctx.font = "12px sans-serif";
    ctx.textAlign = "center";
    ctx.textBaseline = "bottom";
    ctx.fillText(value.toFixed(1), x + barWidth / 2, y - 4);

    ctx.save();
    ctx.translate(x + barWidth / 2, padT + plotH + 18);
    ctx.rotate(-0.35);
    ctx.textBaseline = "middle";
    ctx.fillText(String(row.name ?? "-"), 0, 0);
    ctx.restore();
  });
}

// 가동 현황 조회
async function loadOperationStatus() {
  if (!hasOperationModalElements()) return;
  if (!validateOperationFilters()) return;

  const startDate = operationEls.startDate.value;
  const endDate = operationEls.endDate.value;
  const keyword = operationEls.keyword.value.trim();

  const qs = new URLSearchParams({
    start_date: startDate,
    end_date: endDate,
    keyword,
  });

  try {
    const { res, data } = await fetchJson(`/api/operation-status?${qs.toString()}`);

    if (!res.ok || !data?.ok) {
      throw new Error(data?.message || `HTTP ${res.status}`);
    }

    const rows = Array.isArray(data.rows) ? data.rows : [];
    renderOperationTable(rows);
    renderOperationChart(rows);
  } catch (err) {
    console.error("[operation status] load failed:", err);
    alert(err.message || "설비가동 현황 조회 실패");
    renderOperationTable([]);
    renderOperationChart([]);
  }
}

// 엑셀 다운로드
function downloadOperationExcel() {
  if (!hasOperationModalElements()) return;
  if (!validateOperationFilters()) return;

  const startDate = operationEls.startDate.value;
  const endDate = operationEls.endDate.value;
  const keyword = operationEls.keyword.value.trim();

  const qs = new URLSearchParams({
    start_date: startDate,
    end_date: endDate,
    keyword,
  });

  window.location.href = `/api/operation-status/export?${qs.toString()}`;
}

/* --------------------------------------------------
 * 이벤트 바인딩
 * -------------------------------------------------- */

if (hasOperationModalElements()) {
  operationEls.openBtn.addEventListener("click", async (e) => {
    e.preventDefault();
    openOperationModal();
    await loadOperationStatus();
  });

  operationEls.closeBtn.addEventListener("click", closeOperationModal);

  operationEls.backdrop.addEventListener("click", (e) => {
    if (e.target === operationEls.backdrop) {
      closeOperationModal();
    }
  });

  operationEls.searchBtn.addEventListener("click", loadOperationStatus);
  operationEls.excelBtn.addEventListener("click", downloadOperationExcel);

  operationEls.keyword.addEventListener("keydown", (e) => {
    if (e.key === "Enter") {
      e.preventDefault();
      loadOperationStatus();
    }
  });

  window.addEventListener("keydown", (e) => {
    if (e.key === "Escape" && operationEls.backdrop.classList.contains("open")) {
      closeOperationModal();
    }
  });

  // 모달 열린 상태에서 창 크기 변경 시 차트 재렌더
  window.addEventListener("resize", () => {
    if (operationEls.backdrop.classList.contains("open")) {
      const rows = Array.from(operationEls.tableBody.querySelectorAll("tr"))
        .filter((tr) => !tr.querySelector(".empty-cell"))
        .map((tr) => {
          const tds = tr.querySelectorAll("td");
          return {
            name: tds[0]?.textContent ?? "-",
            utilization_rate: tds[2]?.textContent ?? 0,
          };
        });

      renderOperationChart(rows);
    }
  });

  initOperationModalDates();
} else {
  console.warn("[operation modal] required elements not found");
}

/* --------------------------------------------------
 * 초기 실행
 * -------------------------------------------------- */

tickOnce();
