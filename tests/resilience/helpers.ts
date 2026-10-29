import { eventually } from "../integration/helpers";
export interface Container {
  id: string;
  base: string;
}
export const containers: Container[] = JSON.parse(
  process.env.TEST_CONTAINERS ?? "[]",
);
export async function refreshContainers() {
  const rows = (await compose(["ps", "--format", "json"]))
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
  for (const container of containers) {
    const row = rows.find((r) => r.Service === "app" && r.ID === container.id);
    if (row)
      container.base = `http://127.0.0.1:${row.Publishers.find((p) => p.TargetPort === 3000)!.PublishedPort}`;
  }
}
export async function docker(args: string[]) {
  const child = Bun.spawn(["docker", ...args], {
    stdout: "pipe",
    stderr: "pipe",
  });
  const out = new Response(child.stdout).text();
  const err = new Response(child.stderr).text();
  const code = await child.exited;
  const [output, errors] = await Promise.all([out, err]);
  if (code !== 0) throw new Error(`docker ${args[0]}: ${errors}`);
  return output;
}
export function compose(args: string[]) {
  const project = process.env.TEST_COMPOSE_PROJECT ?? "";
  if (!/^jungle-resilience-[a-f0-9]{12}$/.test(project))
    throw new Error("Projeto isolado não informado");
  return docker([
    "compose",
    "-p",
    project,
    "-f",
    "compose.yaml",
    "-f",
    "compose.resilience.yaml",
    ...args,
  ]);
}
export function logs(container: Container): Promise<Record<string, unknown>[]> {
  return docker(["logs", container.id]).then((text) =>
    text.split(/\r?\n/).flatMap((line) => {
      try {
        return [JSON.parse(line) as Record<string, unknown>];
      } catch {
        return [];
      }
    }),
  );
}
export async function ready() {
  await eventually(
    async () =>
      Promise.all(
        containers.map(async (c) => {
          try {
            return (
              await fetch(c.base + "/health/ready", {
                signal: AbortSignal.timeout(7000),
              })
            ).status;
          } catch {
            return 0;
          }
        }),
      ),
    (statuses) => statuses.length === 3 && statuses.every((s) => s === 200),
    30_000,
  );
}
export async function post<T>(
  container: Container,
  path: string,
  body: unknown,
  key?: string,
) {
  const response = await fetch(container.base + path, {
    method: "POST",
    headers: {
      "content-type": "application/json",
      ...(key ? { "idempotency-key": key } : {}),
    },
    body: JSON.stringify(body),
    signal: AbortSignal.timeout(15_000),
  });
  return { status: response.status, body: (await response.json()) as T };
}
