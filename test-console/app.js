const suiteGrid = document.querySelector("#suite-grid");
const evidenceGrid = document.querySelector("#evidence-grid");
const output = document.querySelector("#output");
const summary = document.querySelector("#run-summary");
const globalStatus = document.querySelector("#global-status");
const copyButton = document.querySelector("#copy-output");

let state;
let selectedRunId;
let lastOutputLength = 0;

const labels = {
  running: "Executando",
  passed: "Aprovado",
  failed: "Falhou",
};

function escapeHtml(value) {
  return String(value)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
}

function formatDuration(value) {
  if (value === undefined) return "em andamento";
  if (value < 1000) return `${value} ms`;
  const seconds = Math.round(value / 1000);
  return seconds < 60
    ? `${seconds} s`
    : `${Math.floor(seconds / 60)} min ${seconds % 60} s`;
}

function latestRun(suiteId) {
  return state.runs.find((run) => run.suiteId === suiteId);
}

function renderSuites() {
  suiteGrid.innerHTML = state.suites
    .map((suite) => {
      const run = latestRun(suite.id);
      const active = state.activeRunId === run?.id;
      const status = run
        ? `<span class="status status--${run.status}">${labels[run.status]}</span>`
        : "";
      return `
        <article class="suite-card ${active ? "suite-card--active" : ""}">
          <div class="suite-card__top">
            <span class="suite-card__eyebrow">${escapeHtml(suite.eyebrow)}</span>
            ${status || `<span class="tag ${suite.requiresDocker ? "" : "tag--local"}">${suite.requiresDocker ? "Docker" : "Local"}</span>`}
          </div>
          <h3>${escapeHtml(suite.name)}</h3>
          <p>${escapeHtml(suite.description)}</p>
          <button
            class="button button--primary"
            type="button"
            data-suite-id="${suite.id}"
            ${state.activeRunId ? "disabled" : ""}
          >${active ? "Executando" : "Executar suíte"}</button>
        </article>`;
    })
    .join("");

  suiteGrid.querySelectorAll("[data-suite-id]").forEach((button) => {
    button.addEventListener("click", () => startRun(button.dataset.suiteId));
  });
}

function renderEvidence() {
  const visible = state.suites.filter((suite) =>
    ["unit", "integration", "concurrency"].includes(suite.id),
  );
  evidenceGrid.innerHTML = visible
    .map(
      (suite) => `
        <article class="evidence-card">
          <h3>${escapeHtml(suite.name)}</h3>
          <ul>${suite.requirements.map((item) => `<li>${escapeHtml(item)}</li>`).join("")}</ul>
        </article>`,
    )
    .join("");
}

function renderConsole() {
  const run =
    state.runs.find((candidate) => candidate.id === selectedRunId) ??
    state.runs[0];
  if (!run) {
    summary.innerHTML =
      '<span class="status status--idle">Aguardando</span><span>Selecione uma suíte para começar.</span>';
    output.textContent = "Nenhuma execução iniciada.";
    copyButton.disabled = true;
    globalStatus.textContent =
      "Pronto para executar. Apenas uma suíte roda por vez.";
    return;
  }
  selectedRunId = run.id;
  const suite = state.suites.find((candidate) => candidate.id === run.suiteId);
  const resultText =
    run.status === "running"
      ? "logs em tempo real"
      : `${run.passed} aprovados · ${run.failed} falhas`;
  summary.innerHTML = `
    <span class="status status--${run.status}">${labels[run.status]}</span>
    <span>${escapeHtml(suite.name)} · ${formatDuration(run.durationMs)} · ${resultText}</span>`;
  const shouldFollow =
    run.id === state.activeRunId && run.output.length !== lastOutputLength;
  output.textContent = run.output || "Preparando execução...";
  if (shouldFollow) output.scrollTop = output.scrollHeight;
  lastOutputLength = run.output.length;
  copyButton.disabled = !run.output;
  globalStatus.textContent = state.activeRunId
    ? `${suite.name} em execução. Os demais comandos estão bloqueados.`
    : `Última execução: ${suite.name}, ${labels[run.status].toLowerCase()}.`;
}

async function refresh() {
  try {
    const response = await fetch(`/api/state?ts=${Date.now()}`, {
      cache: "no-store",
    });
    state = await response.json();
    renderSuites();
    renderEvidence();
    renderConsole();
  } catch (error) {
    globalStatus.textContent = `Falha ao consultar o runner: ${error.message}`;
  }
}

async function startRun(suiteId) {
  globalStatus.textContent = "Iniciando validação...";
  const response = await fetch("/api/runs", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ suiteId }),
  });
  const body = await response.json();
  if (!response.ok) {
    globalStatus.textContent =
      body.error ?? "Não foi possível iniciar a validação.";
    return;
  }
  selectedRunId = body.id;
  lastOutputLength = 0;
  await refresh();
}

copyButton.addEventListener("click", async () => {
  const previous = copyButton.textContent;
  try {
    await navigator.clipboard.writeText(output.textContent);
    copyButton.textContent = "Copiado";
  } catch {
    copyButton.textContent = "Falha ao copiar";
  }
  window.setTimeout(() => (copyButton.textContent = previous), 1400);
});

await refresh();
window.setInterval(refresh, 700);
