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
  if (v === null || v === undefined || v === "" || isNaN(v)) return "-";
  return Number(v).toLocaleString("en-US");
}

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

function render(rows) {
  const grid = document.getElementById("grid");
  grid.innerHTML = "";

  rows.forEach(r => {
    const ui = statusToUI(r.status);
    const name = safe(r.name);
    const util = (r.utilization_rate !== null && r.utilization_rate !== undefined)
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
      <div class="head">${name}</div>
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

    el.style.cursor = "pointer";
    el.addEventListener("click", () => {
      const ip = safe(r.ip, "");
      if (!ip) return;
      window.location.href = `/machine/${encodeURIComponent(ip)}`;
    });

    grid.appendChild(el);
  });
}

let running = false;

async function fetchDashboardData() {
  if (appState.sync.mode === "replay" && appState.sync.at) {
    const res = await fetch(`/api/replay/snapshot?at=${encodeURIComponent(appState.sync.at)}`, { cache: "no-store" });
    const data = await res.json();
    return { res, data, mode: "replay" };
  }

  const res = await fetch("/api/machines", { cache: "no-store" });
  const data = await res.json();
  return { res, data, mode: "live" };
}

async function tickOnce() {
  if (running) return;
  running = true;

  const t0 = performance.now();
  try {
    const { res, data, mode } = await fetchDashboardData();

    if (res.status === 503) {
      document.getElementById("lastUpdate").innerText = "Warming up... " + (data.detail || "");
    } else if (data.rows) {
      render(data.rows);
      const dt = Math.round(performance.now() - t0);
      if (mode === "replay") {
        document.getElementById("lastUpdate").innerText =
          `Replay: ${data.target_at || appState.sync.at} (api ${dt}ms, ${data.count} machines)`;
      } else {
        const refreshedAt = data.refreshed_at
          ? new Date(data.refreshed_at * 1000).toLocaleTimeString()
          : "-";
        document.getElementById("lastUpdate").innerText =
          `Last update: ${new Date().toLocaleString()} (api ${dt}ms, server refreshed ${refreshedAt})`;
      }
    } else {
      document.getElementById("lastUpdate").innerText = "Error: " + JSON.stringify(data);
    }
  } catch (e) {
    document.getElementById("lastUpdate").innerText = "Fetch error: " + e;
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
  if (popup) popup.focus();
}

document.getElementById("emulatorLauncher")?.addEventListener("click", openEmulatorWindow);

tickOnce();
