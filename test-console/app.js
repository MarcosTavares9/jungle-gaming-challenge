const suiteGrid = document.querySelector("#suite-grid");
const evidenceGrid = document.querySelector("#evidence-grid");
const output = document.querySelector("#output");
const summary = document.querySelector("#run-summary");
const globalStatus = document.querySelector("#global-status");
const copyButton = document.querySelector("#copy-output");
const scenarioStage = document.querySelector("#scenario-stage");
const scenarioTitle = document.querySelector("#scenario-title");
const scenarioDescription = document.querySelector("#scenario-description");
const scenarioEventTitle = document.querySelector("#scenario-event-title");
const scenarioEventDetail = document.querySelector("#scenario-event-detail");

let state;
let selectedRunId;
let lastOutputLength = 0;
let lastScenarioKey;
let lastTraceKey;

const traceTargets = [
  "source",
  "format",
  "types",
  "money",
  "wallet",
  "transaction",
  "assertion",
  "http",
  "queue",
  "application",
  "database",
  "outbox",
  "ingress",
];

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

function packets(count = 12) {
  return Array.from(
    { length: count },
    (_, index) =>
      `<span class="scenario-packet" style="--delay: ${index * 90}ms; --lane: ${(index % 4) * 12}px"></span>`,
  ).join("");
}

function shortValue(value) {
  if (value === undefined || value === null || value === "") return undefined;
  const text = String(value);
  return text.length > 18 ? `${text.slice(0, 8)}…${text.slice(-6)}` : text;
}

function compactDetail(parts) {
  return parts.filter(Boolean).join(" · ");
}

function jsonTrace(line) {
  const start = line.indexOf("{");
  if (start === -1) return undefined;
  let entry;
  try {
    entry = JSON.parse(line.slice(start));
  } catch {
    return undefined;
  }

  const details = compactDetail([
    entry.status,
    entry.code ?? entry.failureCode,
    entry.idempotentReplay === true ? "replay idempotente" : undefined,
    entry.receives ? `${entry.receives} recebimentos` : undefined,
    entry.attempts ? `tentativa ${entry.attempts}` : undefined,
    shortValue(entry.transactionId)
      ? `tx ${shortValue(entry.transactionId)}`
      : undefined,
  ]);
  const events = {
    message_processing_started: [
      "queue",
      "Mensagem recebida do SQS",
      compactDetail([
        shortValue(entry.messageId)
          ? `mensagem ${shortValue(entry.messageId)}`
          : undefined,
        shortValue(entry.walletId)
          ? `wallet ${shortValue(entry.walletId)}`
          : undefined,
      ]),
    ],
    transaction_result: ["wallet", "Resultado financeiro persistido", details],
    reference_retry_result: ["application", "Referência reprocessada", details],
    transaction_failure_recorded: [
      "database",
      "Falha definitiva persistida",
      details,
    ],
    message_failure_finalized: [
      "queue",
      "Falha finalizada e mensagem confirmada",
      details,
    ],
    failure_recording_retry: [
      "database",
      "Persistência da falha será repetida",
      details,
    ],
    message_retry: ["queue", "Retry agendado no SQS", details],
    message_dead_lettered: [
      "queue",
      "Mensagem encaminhada para a DLQ",
      details,
    ],
    worker_stopping: [
      "application",
      "Worker finalizando com segurança",
      "Aguardando o processamento em andamento.",
    ],
    worker_stopped: [
      "application",
      "Worker encerrado",
      "Shutdown concluído sem abandonar o lote.",
    ],
  };
  const match = events[entry.event];
  if (!match) return undefined;
  return { target: match[0], title: match[1], detail: match[2] };
}

function testTarget(line) {
  if (/dlq|dead.?letter/i.test(line)) return "queue";
  if (/sqs|fila|queue|redelivery|ack|mensage/i.test(line)) return "queue";
  if (/outbox|publisher/i.test(line)) return "outbox";
  if (/migration|postgres|schema|constraint|ledger|banco|sql/i.test(line))
    return "database";
  if (/wallet|saldo|d[eé]bito|aposta|wager|refund|rollback/i.test(line))
    return "wallet";
  if (/process|inst[aâ]ncia|container|sigterm|worker/i.test(line))
    return "application";
  if (/money|dinheiro|centavo/i.test(line)) return "money";
  if (/transa[cç][aã]o|transaction/i.test(line)) return "transaction";
  return "assertion";
}

function textTrace(line) {
  const clean = line.replace(/^\[(?:stdout|stderr)\]\s*/, "").trim();
  if (!clean) return undefined;
  if (/prettier|checking formatting|code style/i.test(clean))
    return {
      target: "format",
      title: "Formatação verificada",
      detail: clean,
    };
  if (/tsc --noEmit|typescript/i.test(clean))
    return {
      target: "types",
      title: "Tipos verificados",
      detail: clean,
    };
  if (/migration|migrat/i.test(clean))
    return {
      target: "database",
      title: "Migração do banco processada",
      detail: clean,
    };
  if (/Container .* (Starting|Healthy|Stopping|Stopped|Removed)/i.test(clean)) {
    const target = /postgres/i.test(clean)
      ? "database"
      : /localstack|sqs/i.test(clean)
        ? "queue"
        : "application";
    const action = /Healthy/i.test(clean)
      ? "saudável"
      : /Starting/i.test(clean)
        ? "iniciando"
        : /Stopping/i.test(clean)
          ? "finalizando"
          : "encerrado";
    return {
      target,
      title: `Contêiner ${action}`,
      detail: clean,
    };
  }
  const passed = clean.match(/^\(pass\)\s+(.+?)(?:\s+\[\d.+ms\])?$/i);
  if (passed)
    return {
      target: testTarget(passed[1]),
      title: "Teste aprovado",
      detail: passed[1],
    };
  const failed = clean.match(/^\(fail\)\s+(.+?)(?:\s+\[\d.+ms\])?$/i);
  if (failed)
    return {
      target: testTarget(failed[1]),
      title: "Teste falhou",
      detail: failed[1],
    };
  const testSummary = clean.match(/^(\d+) (pass|fail)/i);
  if (testSummary)
    return {
      target: "assertion",
      title:
        testSummary[2].toLowerCase() === "pass"
          ? "Testes aprovados"
          : "Falhas detectadas",
      detail: clean,
    };
  if (/^Ran \d+ tests?/i.test(clean))
    return {
      target: "assertion",
      title: "Suíte finalizada",
      detail: clean,
    };
  return undefined;
}

function deriveTrace(run) {
  if (!run?.output)
    return {
      key: "idle",
      target: undefined,
      title: "Aguardando logs",
      detail: "O próximo evento reconhecido aparecerá aqui.",
    };
  const lines = run.output.split("\n");
  for (let index = lines.length - 1; index >= 0; index -= 1) {
    const line = lines[index];
    const trace = jsonTrace(line) ?? textTrace(line);
    if (trace)
      return {
        ...trace,
        key: `${run.output.length}:${index}:${trace.target}:${trace.title}`,
      };
  }
  return {
    key: `${run.output.length}:raw`,
    target: undefined,
    title: "Log recebido",
    detail: "Linha mantida no console, sem inferência visual.",
  };
}

function scenarioMarkup(suiteId, status, passed = 0) {
  const result = status === "passed" ? "scenario-node--success" : "";
  if (suiteId === "quality")
    return `<div class="scenario-flow scenario-flow--quality">
      <div class="scenario-node" data-target="source">Código</div><span class="scenario-arrow"></span>
      <div class="scenario-stack"><div class="scenario-node ${result}" data-target="format">Prettier</div><div class="scenario-node ${result}" data-target="types">TypeScript</div></div>
    </div>`;
  if (suiteId === "unit")
    return `<div class="scenario-flow scenario-flow--unit">
      <div class="scenario-stack"><div class="scenario-node" data-target="money">Money</div><div class="scenario-node" data-target="wallet">Wallet</div><div class="scenario-node" data-target="transaction">Transação</div></div>
      <span class="scenario-arrow"></span><div class="scenario-node ${result}" data-target="assertion">${status === "passed" ? `${passed} checks` : "Invariantes"}</div>
    </div>`;
  if (suiteId === "integration")
    return `<div class="scenario-flow scenario-flow--integration">
      <div class="scenario-stack"><div class="scenario-node" data-target="http">HTTP</div><div class="scenario-node" data-target="queue">SQS</div></div>
      <span class="scenario-arrow"></span><div class="scenario-node scenario-node--app" data-target="application">Aplicação</div><span class="scenario-arrow"></span>
      <div class="scenario-stack"><div class="scenario-node ${result}" data-target="database">PostgreSQL</div><div class="scenario-node ${result}" data-target="outbox">Outbox</div></div>
    </div>`;
  if (suiteId === "concurrency")
    return `<div class="scenario-concurrency">
      <div class="scenario-source" data-target="ingress"><strong>50</strong><span>envios</span>${packets()}</div>
      <div class="scenario-processes" data-target="application"><span>P1</span><span>P2</span><span>P3</span></div>
      <div class="scenario-wallet ${result}" data-target="wallet"><span>Wallet</span><strong>${status === "passed" ? "75,00" : "100,00"}</strong></div>
      <div class="scenario-results" data-target="assertion"><span>${status === "passed" ? "1 débito" : "disputa"}</span><span>${status === "passed" ? "49 replays" : "lock"}</span></div>
    </div>`;
  if (suiteId === "resilience")
    return `<div class="scenario-flow scenario-flow--resilience">
      <div class="scenario-processes" data-target="application"><span>A1</span><span>A2</span><span>A3</span></div><span class="scenario-arrow"></span>
      <div class="scenario-stack"><div class="scenario-node scenario-node--fault ${result}" data-target="database">PostgreSQL</div><div class="scenario-node scenario-node--fault ${result}" data-target="queue">SQS</div></div>
    </div>`;
  if (suiteId === "full")
    return `<div class="scenario-pipeline">${[
      ["Formato", "format"],
      ["Tipos", "types"],
      ["Unidade", "assertion"],
      ["Integração", "application"],
      ["Concorrência", "wallet"],
      ["Resiliência", "queue"],
    ]
      .map(
        ([step, target], index) =>
          `<span class="${result}" data-target="${target}" style="--index: ${index}">${step}</span>`,
      )
      .join("")}</div>`;
  return `<div class="scenario-overview"><span>Entradas</span><i></i><span>Processos</span><i></i><span>Wallet e ledger</span></div>`;
}

function renderScenario(suite, run) {
  const suiteId = suite?.id ?? "overview";
  const status = run?.status ?? "idle";
  const key = `${suiteId}:${status}:${run?.passed ?? 0}`;
  if (key !== lastScenarioKey) {
    lastScenarioKey = key;
    lastTraceKey = undefined;
    scenarioTitle.textContent = suite?.name ?? "Topologia da validação";
    scenarioStage.className = `scenario-stage scenario-stage--${suiteId} scenario-stage--${status}`;
    scenarioStage.innerHTML = scenarioMarkup(suiteId, status, run?.passed ?? 0);
  }

  const trace = deriveTrace(run);
  if (trace.key !== lastTraceKey) {
    lastTraceKey = trace.key;
    traceTargets.forEach((target) =>
      scenarioStage.classList.remove(`scenario-stage--target-${target}`),
    );
    scenarioStage.classList.remove("scenario-stage--observed");
    if (trace.target) {
      scenarioStage.classList.add(`scenario-stage--target-${trace.target}`);
      void scenarioStage.offsetWidth;
      scenarioStage.classList.add("scenario-stage--observed");
    }
    scenarioEventTitle.textContent = trace.title;
    scenarioEventDetail.textContent = trace.detail;
  }
  scenarioDescription.textContent = suite
    ? status === "running"
      ? `${suite.name} em execução. Cada destaque é acionado por um evento reconhecido nos logs reais abaixo.`
      : `${suite.name}: ${run.passed} testes aprovados e ${run.failed} falhas. O último destaque permanece ligado ao evento observado.`
    : "Selecione uma suíte para acompanhar os logs reais pela topologia.";
}

function renderConsole() {
  const run =
    state.runs.find((candidate) => candidate.id === state.activeRunId) ??
    state.runs.find((candidate) => candidate.id === selectedRunId) ??
    state.runs[0];
  if (!run) {
    summary.innerHTML =
      '<span class="status status--idle">Aguardando</span><span>Selecione uma suíte para começar.</span>';
    output.textContent = "Nenhuma execução iniciada.";
    copyButton.disabled = true;
    renderScenario();
    globalStatus.textContent =
      "Pronto para executar. Apenas uma suíte roda por vez.";
    return;
  }
  selectedRunId = run.id;
  const suite = state.suites.find((candidate) => candidate.id === run.suiteId);
  renderScenario(suite, run);
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

document.addEventListener("visibilitychange", () => {
  scenarioStage.classList.toggle("scenario-stage--paused", document.hidden);
});

await refresh();
window.setInterval(refresh, 700);
