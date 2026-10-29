import { openDatabase } from "../src/infrastructure/database";
const db = await openDatabase();
try {
  if (process.argv[2] === "down") await db.migrator.down();
  else await db.migrator.up();
} finally {
  await db.close(true);
}
