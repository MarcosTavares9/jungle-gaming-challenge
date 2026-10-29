// The project name is generated here; lifecycle commands never target the development stack.
export {};
const suffix = crypto.randomUUID().replaceAll("-", "");
const project = `jungle-resilience-${suffix.slice(0, 12)}`;
if (!/^jungle-resilience-[a-f0-9]{12}$/.test(project))
  throw new Error("Projeto de testes inválido");
const env = {
  ...process.env,
  TEST_DATABASE_NAME: `jungle_test_${suffix}`,
  TEST_QUEUE_PREFIX: `test-${suffix.slice(0, 12)}-`,
};
const args = [
  "compose",
  "-p",
  project,
  "-f",
  "compose.yaml",
  "-f",
  "compose.resilience.yaml",
];
async function docker(extra: string[], capture = false) {
  const child = Bun.spawn(["docker", ...args, ...extra], {
    env,
    stdout: capture ? "pipe" : "inherit",
    stderr: "inherit",
  });
  const output = capture
    ? new Response(child.stdout).text()
    : Promise.resolve("");
  const code = await child.exited;
  const text = await output;
  if (code !== 0) throw new Error(`docker ${extra[0]} encerrou com ${code}`);
  return text;
}
try {
  await docker(["up", "--build", "-d", "--wait", "--scale", "app=3"]);
  const rows = (await docker(["ps", "--format", "json"], true))
    .trim()
    .split(/\r?\n/)
    .map(
      (line) =>
        JSON.parse(line) as {
          ID: string;
          Service: string;
          Publishers: { TargetPort: number; PublishedPort: number }[];
        },
    );
  const port = (service: string, target: number) => {
    const value = rows
      .find((r) => r.Service === service)
      ?.Publishers.find((p) => p.TargetPort === target)?.PublishedPort;
    if (!value) throw new Error(`Porta não encontrada: ${service}:${target}`);
    return value;
  };
  const apps = rows
    .filter((r) => r.Service === "app")
    .map((r) => ({
      id: r.ID,
      base: `http://127.0.0.1:${r.Publishers.find((p) => p.TargetPort === 3000)!.PublishedPort}`,
    }));
  if (apps.length !== 3)
    throw new Error(`Esperadas 3 instâncias, recebidas ${apps.length}`);
  const tests = Bun.spawn(
    [process.execPath, "test", "tests/resilience", "--timeout", "90000"],
    {
      env: {
        ...env,
        RESILIENCE_TEST_RUN: "true",
        TEST_COMPOSE_PROJECT: project,
        TEST_CONTAINERS: JSON.stringify(apps),
        DATABASE_URL: `postgresql://jungle:jungle@127.0.0.1:${port("postgres", 5432)}/${env.TEST_DATABASE_NAME}`,
        SQS_ENDPOINT: `http://127.0.0.1:${port("localstack", 4566)}`,
        SQS_QUEUE_PREFIX: env.TEST_QUEUE_PREFIX,
      },
      stdout: "inherit",
      stderr: "inherit",
    },
  );
  process.exitCode = await tests.exited;
  if (process.exitCode !== 0) await docker(["logs", "--tail", "80"]);
} catch (error) {
  process.exitCode = 1;
  console.error(error);
  await docker(["logs", "--tail", "50"]).catch(() => {});
} finally {
  await docker(["down", "--volumes", "--remove-orphans"]);
}
