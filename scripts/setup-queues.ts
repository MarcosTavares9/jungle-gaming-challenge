import { Queues } from "../src/infrastructure/queues";
const queues = new Queues();
try {
  await queues.setup();
  console.log("Filas prontas.");
} finally {
  queues.close();
}
