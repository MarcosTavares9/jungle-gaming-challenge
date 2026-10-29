import { EntitySchema, type Transaction } from "@mikro-orm/core";
import { Migrator } from "@mikro-orm/migrations";
import { MikroORM, type EntityManager } from "@mikro-orm/postgresql";
import { config } from "./config";
import { Migration202610290001 } from "./migrations/Migration202610290001";

// ORM records stay outside the domain. Numeric columns remain decimal strings.
export class WalletRecord {
  id!: string;
  playerId!: string;
  currency!: string;
  balance!: string;
  version!: number;
  createdAt!: Date;
  updatedAt!: Date;
}
const walletEntity = new EntitySchema({
  class: WalletRecord,
  tableName: "wallets",
  properties: {
    id: { type: "uuid", primary: true },
    playerId: { type: "uuid" },
    currency: { type: "string", length: 3 },
    balance: { type: "decimal", precision: 18, scale: 2 },
    version: { type: "integer" },
    createdAt: { type: "datetime" },
    updatedAt: { type: "datetime" },
  },
});
export async function openDatabase() {
  return MikroORM.init({
    clientUrl: config.DATABASE_URL,
    entities: [walletEntity],
    extensions: [Migrator],
    pool: { min: 0, max: 10 },
    migrations: {
      migrationsList: [Migration202610290001],
      transactional: true,
      allOrNothing: true,
    },
  });
}
export type Database = Awaited<ReturnType<typeof openDatabase>>;
export class Sql {
  constructor(public readonly em: EntityManager) {}
  async query<T extends object = Record<string, unknown>>(
    sql: string,
    params: unknown[] = [],
  ): Promise<T[]> {
    return this.em
      .getConnection()
      .execute<
        T[]
      >(sql, params, "all", this.em.getTransactionContext() as Transaction);
  }
}
