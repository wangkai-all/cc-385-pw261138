const $ = (sel) => document.querySelector(sel);

const state = {
  current: null,
  preview: null,
  lastPreviewKey: null,
};

async function api(path, options = {}) {
  const response = await fetch(path, {
    headers: { "Content-Type": "application/json" },
    ...options,
  });
  let body = null;
  try {
    body = await response.json();
  } catch (err) {
    body = { error: "non-JSON response" };
  }
  if (!response.ok) {
    const error = new Error(body.error || response.statusText);
    error.body = body;
    throw error;
  }
  return body;
}

function toast(message, kind = "info") {
  const el = $("#toast");
  el.textContent = message;
  el.className = `toast ${kind}`;
  el.hidden = false;
  clearTimeout(toast.timer);
  toast.timer = setTimeout(() => { el.hidden = true; }, 5000);
}

function esc(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;");
}

function pill(text, kind = "") {
  return `<span class="pill ${kind}">${esc(text)}</span>`;
}

async function refresh() {
  try {
    const snapshot = await api("/api/state");
    state.current = snapshot;
    renderCards(snapshot);
    renderCheckpoint(snapshot);
    renderLedger(snapshot);
    renderRuns(snapshot);
    await renderEvents();
  } catch (err) {
    toast(`refresh failed: ${err.message}`, "error");
  }
}

function renderCards(snapshot) {
  const m = snapshot.metrics;
  const live = m.checkpoint
    ? `${m.checkpoint.rule}@${m.checkpoint.offset}`
    : (m.live_raw ? `${m.live_raw.rule}@${m.live_raw.last_offset} (legacy)` : "none");
  const cards = [
    ["events", m.events],
    ["pending", m.pending],
    ["contributions", m.contributions],
    ["retractions", snapshot.publisher.retractions ?? 0],
    ["checkpoint", live, m.checkpoint ? "good" : "warn"],
    ["worker", snapshot.worker_ready.ok ? "ready" : "blocked", snapshot.worker_ready.ok ? "good" : "bad"],
  ];
  $("#cards").innerHTML = cards.map(([label, value, kind]) => `
    <div class="card ${kind || ""}">
      <div class="card-label">${esc(label)}</div>
      <div class="card-value">${esc(value)}</div>
    </div>`).join("");
}

function renderCheckpoint(snapshot) {
  const m = snapshot.metrics;
  const lines = [
    `live v2 checkpoint : ${m.checkpoint ? `${m.checkpoint.rule} @ offset ${m.checkpoint.offset}` : "—"}`,
    `legacy on disk     : ${m.live_raw ? `${m.live_raw.format} rule ${m.live_raw.rule} @ ${m.live_raw.last_offset}` : (m.legacy ? `${m.legacy.format} rule ${m.legacy.rule} @ ${m.legacy.last_offset}` : "—")}`,
    `events in log      : ${m.events}`,
    `pending for worker : ${m.pending}`,
    `contributions      : ${m.contributions} ${JSON.stringify(m.contributions_by_rule)}`,
    `emit_from (live)   : ${m.checkpoint ? m.checkpoint.emit_from : "—"}`,
    "",
    snapshot.worker_ready.ok
      ? "worker can read the live checkpoint."
      : `worker refuses to start:\n  ${snapshot.worker_ready.reasons.join("\n  ")}`,
  ];
  $("#checkpoint-body").innerHTML = `<pre>${esc(lines.join("\n"))}</pre>`;
}

async function renderEvents() {
  const { events } = await api("/api/events?after=0");
  const rows = events.map((e) => {
    const kind = e.reversal_of ? pill("reversal", "warn") : "";
    return `<tr>
      <td>${e.offset}</td>
      <td>${esc(e.id)}</td>
      <td>${esc(e.account ?? "—")}</td>
      <td>${e.reversal_of ? esc(e.reversal_of) : esc(e.amount)}</td>
      <td>${kind}</td>
    </tr>`;
  }).join("");
  $("#events").innerHTML = `<table><thead><tr><th>#</th><th>id</th><th>account</th><th>amount / reverses</th><th></th></tr></thead><tbody>${rows}</tbody></table>`;
}

function renderLedger(snapshot) {
  const p = snapshot.publisher;
  const accounts = Object.entries(p.accounts || {}).map(([acct, v]) => `
    <tr><td>${esc(acct)}</td><td>${v.flag ?? 0}</td><td>${v.clear ?? 0}</td><td>${v.retracted ?? 0}</td><td>${esc(v.rule ?? "")}</td><td>${v.last_offset ?? 0}</td></tr>`).join("");
  $("#ledger").innerHTML = `
    <p>consumed through offset <b>${p.consumed_through}</b> · applied keys <b>${p.applied}</b> · retractions <b>${p.retractions}</b></p>
    <p>by rule: <span class="mono">${esc(JSON.stringify(p.by_rule))}</span></p>
    <table><thead><tr><th>account</th><th>flag</th><th>clear</th><th>retracted</th><th>rule</th><th>last offset</th></tr></thead>
    <tbody>${accounts}</tbody></table>`;
}

function renderRuns(snapshot) {
  if (!snapshot.runs.length) {
    $("#runs").textContent = "no runs yet";
    return;
  }
  const rows = snapshot.runs.slice().reverse().map((r) => `
    <tr>
      <td><a href="#" data-run="${esc(r.id)}">${esc(r.id)}</a></td>
      <td>${pill(r.status, r.status === "committed" || r.status === "recovered" ? "good" : r.status === "refused" ? "bad" : r.status === "crashed" ? "warn" : "")}</td>
      <td>${esc(r.created_at)}</td>
      <td>${r.source_offset ?? "—"} → ${r.target_offset ?? "—"}</td>
      <td>${esc(r.crash_at ?? "")}</td>
    </tr>`).join("");
  $("#runs").innerHTML = `<table><thead><tr><th>run</th><th>status</th><th>created</th><th>offset</th><th>crash</th></tr></thead><tbody>${rows}</tbody></table>`;
  document.querySelectorAll("[data-run]").forEach((el) => {
    el.addEventListener("click", async (event) => {
      event.preventDefault();
      const run = await api(`/api/runs/${el.dataset.run}`);
      showRun(run);
    });
  });
}

function showRun(run) {
  state.preview = run;
  const body = $("#upgrade-body");
  if (!run.converted_checkpoint) {
    body.innerHTML = `<div class="callout bad"><b>${esc(run.status)}</b><ul>${run.reasons.map((r) => `<li>${esc(r)}</li>`).join("")}</ul></div>`;
    return;
  }
  const report = run.report || {};
  const totals = run.diff.new.totals;
  const accountRows = run.diff.accounts.map((a) => `
    <tr>
      <td>${esc(a.account)}</td>
      <td class="mono">${esc(JSON.stringify(a.old_window))}</td>
      <td class="mono">${esc(JSON.stringify(a.new_window))}</td>
      <td>${a.window_equal === true ? pill("matches log", "good") : a.window_equal === false ? pill("differs", "bad") : pill("n/a (replay)", "")}</td>
      <td>${a.new_latched ? pill("latched", "warn") : "—"}</td>
      <td>${a.new_total_amount ?? "—"}</td>
    </tr>`).join("");
  const suppressed = (run.suppressed || []).map((c) => `${c.kind} ${c.account}@${c.offset}`).join(", ") || "none";
  const pending = (run.pending_preview || []).map((c) => `${c.kind} ${c.account}@${c.offset}`).join(", ") || "none";
  const verified = run.verified
    ? (run.verified.ok ? pill("verified", "good") : pill("verification failed", "bad"))
    : "";
  body.innerHTML = `
    <div class="callout ${run.status === "refused" ? "bad" : "good"}">
      <b>${esc(run.id)}</b> · ${pill(run.status, run.status === "refused" ? "bad" : "good")} ${verified}
      <div class="small">source ${esc(run.source_kind)} ${esc(run.source_rule)}@${esc(run.source_offset)} →
        ${esc(run.target_rule)}@${esc(run.target_offset)} · new contributions start at offset ${esc(run.emit_from)}</div>
      <div class="small">events replayed: ${esc(report.events_replayed ?? "—")} · accounts: ${esc(report.accounts ?? "—")} · digest <span class="mono">${esc(run.converted_digest || "")}</span></div>
    </div>
    <div class="row">
      <label>crash at
        <select id="commit-crash">
          <option value="none">none</option>
          <option value="before_write">before_write</option>
          <option value="after_write">after_write</option>
          <option value="before_replace">before_replace</option>
          <option value="after_replace">after_replace</option>
        </select>
      </label>
      <button id="commit" class="btn primary">Commit checkpoint</button>
      <button id="recover" class="btn warn">Recover from crash</button>
      <button id="verify" class="btn">Verify</button>
      <button id="show-diff" class="btn">Raw diff JSON</button>
    </div>
    ${(run.warnings || []).length ? `
      <div class="callout bad">
        <b>Uncontributed offsets</b>: ${esc(run.warnings.join(", "))} — these offsets
        settled by the log replay have no published contribution from the old rule.
        The new rule does not invent them; they are reported for reconciliation.
      </div>` : ""}
    <div class="grid">
      <div>
        <h3>State diff (v1 windows → v2 account state)</h3>
        <table><thead><tr><th>account</th><th>v1 window</th><th>v2 window</th><th>check</th><th>latch</th><th>cumulative</th></tr></thead>
        <tbody>${accountRows}</tbody></table>
        <p class="small">new totals: <span class="mono">${esc(JSON.stringify(totals))}</span></p>
        <p class="small">emit_from: <b>${esc(run.converted_checkpoint.emit_from)}</b> — first offset the new rule may contribute for.</p>
      </div>
      <div>
        <h3>What will not be double-counted</h3>
        <p class="small">v2 would also have produced these inside the already-settled prefix (${esc(run.source_rule)}@${esc(run.source_offset)}); they are <b>suppressed</b>:</p>
        <p class="mono small">${esc(suppressed)}</p>
        <h3>Pending input preview</h3>
        <p class="small">Contributions the worker will emit for input that arrived during the upgrade:</p>
        <p class="mono small">${esc(pending)}</p>
      </div>
    </div>`;
  $("#commit").addEventListener("click", () => commitRun(run.id, $("#commit-crash").value));
  $("#recover").addEventListener("click", () => recoverRun(run.id));
  $("#verify").addEventListener("click", () => verifyRun(run.id));
  $("#show-diff").addEventListener("click", () => {
    $("#diff-body").innerHTML = `<pre class="mono small">${esc(JSON.stringify(run.diff, null, 2))}</pre>`;
    $("#diff-dialog").showModal();
  });
}

async function previewUpgrade() {
  const mode = $("#upgrade-mode").value;
  const payload = { mode };
  if (mode === "replay") payload.replay_offset = Number($("#replay-offset").value);
  try {
    const run = await api("/api/upgrade/preview", { method: "POST", body: JSON.stringify(payload) });
    state.lastPreviewKey = run.id;
    showRun(run);
    toast(run.status === "refused" ? "preview refused — see reasons" : "preview ready", run.status === "refused" ? "error" : "info");
    await refresh();
  } catch (err) {
    toast(`preview failed: ${err.message}`, "error");
  }
}

async function commitRun(runId, crashAt) {
  try {
    const run = await api("/api/upgrade/commit", {
      method: "POST",
      body: JSON.stringify({ run_id: runId, crash_at: crashAt, expected_digest: state.preview?.converted_digest }),
    });
    showRun(run);
    toast(run.status === "crashed" ? `crashed at ${run.crash_at} — recover to proceed` : `commit ${run.status}`,
      run.status === "crashed" ? "warn" : "info");
    await refresh();
  } catch (err) {
    toast(`commit failed: ${err.message}`, "error");
  }
}

async function recoverRun(runId) {
  try {
    const run = await api("/api/upgrade/recover", { method: "POST", body: JSON.stringify({ run_id: runId }) });
    showRun(run);
    const action = run.recovery?.action || "ok";
    const label = {
      reapplied: "reapplied the previewed checkpoint",
      already_committed: "already committed — nothing to redo",
      superseded: "worker already moved past the upgrade point; live checkpoint kept and proven",
    }[action] || action;
    toast(`recovered: ${label}`, "info");
    await refresh();
  } catch (err) {
    toast(`recover failed: ${err.message}`, "error");
  }
}

async function verifyRun(runId) {
  try {
    const result = await api("/api/upgrade/verify", { method: "POST", body: JSON.stringify({ run_id: runId }) });
    toast(result.ok ? "verification passed" : `verification failed: ${result.reasons.join("; ")}`, result.ok ? "info" : "error");
  } catch (err) {
    toast(`verify failed: ${err.message}`, "error");
  }
}

function bind() {
  $("#refresh").addEventListener("click", refresh);
  $("#reset").addEventListener("click", async () => {
    if (!confirm("Reseed the demo data directory? All progress is lost.")) return;
    await api("/api/reset", { method: "POST", body: "{}" });
    $("#upgrade-body").innerHTML = '<p class="hint">Reseeded. Preview an upgrade.</p>';
    toast("demo data reseeded", "info");
    await refresh();
  });
  $("#worker-step").addEventListener("click", async () => {
    try {
      const result = await api("/api/worker/step", {
        method: "POST",
        body: JSON.stringify({ batch: Number($("#worker-batch").value), crash_at: $("#worker-crash").value }),
      });
      toast(result.crashed ? `worker crashed at ${result.crashed}` : `processed ${result.processed} events (${result.contributions} contributions)`,
        result.crashed ? "warn" : "info");
      await refresh();
    } catch (err) {
      const reasons = err.body?.reasons;
      toast(reasons ? `worker refused: ${reasons.join("; ")}` : `worker: ${err.message}`, "warn");
      await refresh();
    }
  });
  $("#publisher-step").addEventListener("click", async () => {
    const result = await api("/api/publisher/step", { method: "POST", body: JSON.stringify({}) });
    toast(`published ${result.new} new contributions (through offset ${result.consumed_through})`, "info");
    await refresh();
  });
  $("#publisher-crash").addEventListener("click", async () => {
    const result = await api("/api/publisher/step", { method: "POST", body: JSON.stringify({ crash_before_ack: true }) });
    toast(`publisher died before ack after applying ${result.new} — rerun to prove no double count`, "warn");
    await refresh();
  });
  $("#ingest-form").addEventListener("submit", async (event) => {
    event.preventDefault();
    try {
      const ev = await api("/api/ingest", {
        method: "POST",
        body: JSON.stringify({
          id: `web-${Date.now()}-${Math.floor(Math.random() * 1000)}`,
          account: $("#ingest-account").value.trim(),
          amount: Number($("#ingest-amount").value),
        }),
      });
      toast(`ingested ${ev.id} at offset ${ev.offset}`, "info");
      await refresh();
    } catch (err) {
      toast(`ingest: ${err.message}`, "error");
    }
  });
  $("#reverse-form").addEventListener("submit", async (event) => {
    event.preventDefault();
    try {
      const ev = await api("/api/ingest", {
        method: "POST",
        body: JSON.stringify({
          id: `rev-${Date.now()}`,
          reversal_of: $("#reverse-id").value.trim(),
        }),
      });
      toast(`ingested reversal ${ev.id} at offset ${ev.offset}`, "info");
      await refresh();
    } catch (err) {
      toast(`reversal: ${err.message}`, "error");
    }
  });
  $("#preview").addEventListener("click", previewUpgrade);
  $("#upgrade-mode").addEventListener("change", (event) => {
    $("#replay-offset").disabled = event.target.value !== "replay";
  });
  $("#replay-offset").disabled = true;
}

bind();
refresh();
