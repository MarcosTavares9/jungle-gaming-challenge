import { openDatabase } from "../../src/infrastructure/database";
import { Queues } from "../../src/infrastructure/queues";
import { Workers } from "../../src/infrastructure/workers";
import { Processor } from "../../src/application/processor";
if (process.env.INTEGRATION_TEST_RUN !== "true")
  throw new Error("Fixture exclusiva da suíte de integração");
const db = await openDatabase();
const queues = new Queues();
try {
  const start = Number(process.env.PUBLISH_AT ?? 0);
  if (start > Date.now()) await Bun.sleep(start - Date.now());
  const count = await new Workers(db, new Processor(db), queues).publish();
  console.log(JSON.stringify({ count }));
} finally {
  queues.close();
  await db.close(true);
}
