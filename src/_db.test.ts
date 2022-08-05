import { Writable } from "node:stream";
import open, { Database } from "better-sqlite3";
import { ArrayLogger } from "@giancosta86/unified-logging";
import { SqliteWritableBuilder } from ".";
import { Bear } from "./_bears.test";
import { Chipmunk } from "./_chipmunks.test";

export async function withTestDb(
  consumer: (db: Database) => Promise<void>
): Promise<void> {
  const db: Database = open(":memory:");

  db.exec(`
    CREATE TABLE bears (
      name TEXT NOT NULL,
      age INTEGER NOT NULL,
      PRIMARY KEY (name)
    );

    CREATE TABLE chipmunks (
      name TEXT NOT NULL,
      gathered_nuts INTEGER NOT NULL,
      PRIMARY KEY (name)
    );
  `);

  try {
    await consumer(db);
  } finally {
    db.close();
  }
}

export function createTestWritableBuilder(): [
  SqliteWritableBuilder,
  ArrayLogger
] {
  const logger = new ArrayLogger();
  const builder = new SqliteWritableBuilder()
    .withLogger(logger)
    .withType<Bear>(
      "bear",
      "INSERT INTO bears (name, age) VALUES (?, ?)",
      bear => [bear.name, bear.age]
    )
    .withType<Chipmunk>(
      "chipmunk",
      "INSERT INTO chipmunks (name, gathered_nuts) VALUES (?, ?)",
      chipmunk => [chipmunk.name, chipmunk.gatheredNuts]
    );

  return [builder, logger];
}

export function createTestWritable(
  db: Database,
  maxObjectsInTransaction: number
): [Writable, ArrayLogger] {
  const [builder, logger] = createTestWritableBuilder();

  const writable = builder
    .withMaxObjectsInTransaction(maxObjectsInTransaction)
    .build(db);

  return [writable, logger];
}

export function replaceDbWithCrashingFake(sqlWritable: Writable): void {
  const fakeDb = {
    exec() {
      throw new Error("Test exec error!");
    }
  };

  (sqlWritable as any).db = fakeDb;
}
