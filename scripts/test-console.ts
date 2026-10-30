import { fileURLToPath } from "node:url";

type SuiteId =
  | "quality"
  | "unit"
  | "integration"
  | "concurrency"
  | "resilience"
  | "full";

type Suite = {
  id: SuiteId;
  name: string;
  eyebrow: string;
  description: string;
  command: string[];
  displayCommand: string;
  requiresDocker: boolean;
  requirements: string[];
};

type RunStatus = "running" | "passed" | "failed";

type Run = {
  id: string;
  suiteId: SuiteId;
  status: RunStatus;
  startedAt: string;
  finishedAt?: string;
  durationMs?: number;
  exitCode?: number;
  output: string;
  passed: number;
  failed: number;
};

const root = new URL("../", import.meta.url);
const projectRoot = fileURLToPath(root);
const assets = new URL("../test-console/", import.meta.url);
const maxOutput = 2_000_000;
const runs: Run[] = [];
let active: { run: Run; child: ReturnType<typeof Bun.spawn> } | undefined;

const suites: Suite[] = [
  {
    id: "quality",
    name: "Qualidade estática",
    eyebrow: "Pré-validação",
    description:
      "Confere estilo, tipos e contratos compiláveis sem subir infraestrutura.",
    command: [process.execPath, "run", "validate:static"],
    displayCommand: "bun run validate:static",
    requiresDocker: false,
    requirements: ["Prettier", "TypeScript strict"],
  },
  {
    id: "unit",
    name: "Testes unitários",
    eyebrow: "Obrigatório",
    description:
      "Exercita as invariantes financeiras e os contratos do domínio.",
    command: [process.execPath, "run", "test:unit"],
    displayCommand: "bun run test:unit",
    requiresDocker: false,
    requirements: [
      "Money: escala e entradas inválidas",
      "Invariantes da Wallet",
      "BET, WIN, LOSS, REFUND e ROLLBACK",
      "Conflito de moeda",
      "Idempotency key divergente",
    ],
  },
  {
    id: "integration",
    name: "Integração real",
    eyebrow: "Obrigatório",
    description:
      "Provisiona PostgreSQL e LocalStack reais em recursos isolados.",
    command: [process.execPath, "run", "test:integration"],
    displayCommand: "bun run test:integration",
    requiresDocker: true,
    requirements: [
      "Migrations e constraints",
      "Atomicidade wallet, ledger, inbox e outbox",
      "Inbox e redelivery",
      "Publishers concorrentes",
      "Retry, DLQ e recuperação",
    ],
  },
  {
    id: "concurrency",
    name: "Concorrência",
    eyebrow: "Cenários críticos",
    description:
      "Filtra os cenários de paralelismo real exigidos no enunciado.",
    command: [process.execPath, "run", "test:concurrency"],
    displayCommand: "bun run test:concurrency",
    requiresDocker: true,
    requirements: [
      "50 apostas em paralelo, um débito",
      "Disputa de saldo na mesma wallet",
      "Wallets distintas em paralelo",
      "Três processos simultâneos",
      "Commit antes do ack e reinício",
      "Dois publishers na mesma outbox",
      "Reversão antes da referência",
    ],
  },
  {
    id: "resilience",
    name: "Resiliência",
    eyebrow: "Falhas reais",
    description:
      "Sobe três containers e injeta indisponibilidade e sinais de processo.",
    command: [process.execPath, "run", "test:resilience"],
    displayCommand: "bun run test:resilience",
    requiresDocker: true,
    requirements: [
      "Três instâncias",
      "Queda e retorno do PostgreSQL",
      "Indisponibilidade do SQS",
      "SIGTERM durante processamento",
    ],
  },
  {
    id: "full",
    name: "Validação completa",
    eyebrow: "Entrega",
    description: "Executa qualidade estática e as três suítes em sequência.",
    command: [process.execPath, "run", "validate"],
    displayCommand: "bun run validate",
    requiresDocker: true,
    requirements: [
      "Formatação",
      "Tipos",
      "Unidade",
      "Integração",
      "Resiliência",
    ],
  },
];

function json(value: unknown, status = 200) {
  return Response.json(value, {
    status,
    headers: { "cache-control": "no-store" },
  });
}

function append(run: Run, chunk: string) {
  run.output += chunk.replaceAll("\r\n", "\n");
  if (run.output.length > maxOutput) run.output = run.output.slice(-maxOutput);
}

function countResults(run: Run) {
  const matches = [
    ...run.output.matchAll(/(?:^|\n)\s*(\d+) (pass|fail)(?:\n|$)/g),
  ];
  run.passed = matches.reduce(
    (total, match) => total + (match[2] === "pass" ? Number(match[1]) : 0),
    0,
  );
  run.failed = matches.reduce(
    (total, match) => total + (match[2] === "fail" ? Number(match[1]) : 0),
    0,
  );
}

async function collect(
  stream: ReadableStream<Uint8Array>,
  run: Run,
  prefix = "",
) {
  const reader = stream.getReader();
  const decoder = new TextDecoder();
  while (true) {
    const { done, value } = await reader.read();
    if (done) break;
    append(run, prefix + decoder.decode(value, { stream: true }));
  }
  append(run, decoder.decode());
}

async function execute(run: Run, suite: Suite) {
  append(run, `$ ${suite.displayCommand}\n\n`);
  try {
    const child = Bun.spawn(suite.command, {
      cwd: projectRoot,
      env: { ...process.env, FORCE_COLOR: "0", NO_COLOR: "1" },
      stdin: "ignore",
      stdout: "pipe",
      stderr: "pipe",
    });
    active = { run, child };
    await Promise.all([
      collect(child.stdout, run),
      collect(child.stderr, run, "[stderr] "),
    ]);
    run.exitCode = await child.exited;
    run.status = run.exitCode === 0 ? "passed" : "failed";
  } catch (error) {
    run.exitCode = 1;
    run.status = "failed";
    append(
      run,
      `\n[runner] ${error instanceof Error ? error.message : String(error)}\n`,
    );
  } finally {
    countResults(run);
    run.finishedAt = new Date().toISOString();
    run.durationMs = Date.parse(run.finishedAt) - Date.parse(run.startedAt);
    if (active?.run.id === run.id) active = undefined;
  }
}

function staticFile(name: string, type: string) {
  return new Response(Bun.file(new URL(name, assets)), {
    headers: { "content-type": type, "cache-control": "no-store" },
  });
}

const port = Number(process.env.TEST_CONSOLE_PORT ?? 4173);
if (!Number.isInteger(port) || port < 1024 || port > 65535)
  throw new Error("TEST_CONSOLE_PORT deve estar entre 1024 e 65535");

const server = Bun.serve({
  hostname: "127.0.0.1",
  port,
  async fetch(request) {
    const url = new URL(request.url);
    if (request.method === "GET" && url.pathname === "/")
      return staticFile("index.html", "text/html; charset=utf-8");
    if (request.method === "GET" && url.pathname === "/styles.css")
      return staticFile("styles.css", "text/css; charset=utf-8");
    if (request.method === "GET" && url.pathname === "/app.js")
      return staticFile("app.js", "text/javascript; charset=utf-8");
    if (request.method === "GET" && url.pathname === "/api/state")
      return json({
        suites: suites.map(({ command: _, ...suite }) => suite),
        runs,
        activeRunId: active?.run.id ?? null,
      });
    if (request.method === "POST" && url.pathname === "/api/runs") {
      if (active)
        return json({ error: "Já existe uma validação em execução" }, 409);
      const body = (await request.json().catch(() => null)) as {
        suiteId?: SuiteId;
      } | null;
      const suite = suites.find((candidate) => candidate.id === body?.suiteId);
      if (!suite) return json({ error: "Suíte desconhecida" }, 400);
      const run: Run = {
        id: crypto.randomUUID(),
        suiteId: suite.id,
        status: "running",
        startedAt: new Date().toISOString(),
        output: "",
        passed: 0,
        failed: 0,
      };
      runs.unshift(run);
      runs.splice(20);
      void execute(run, suite);
      return json(run, 202);
    }
    return json({ error: "Rota não encontrada" }, 404);
  },
});

console.log(
  `Test Console disponível em http://${server.hostname}:${server.port}`,
);
