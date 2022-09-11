import { Database } from "better-sqlite3";
import { Logger } from "@giancosta86/unified-logging";
import {
  ObjectMapper,
  Serializer,
  SqliteWritable,
  SqliteWritableSettings,
  StatementArguments
} from "./SqliteWritable";

export type TypedObject = { type: string };

class SerializerBlueprint {
  constructor(
    private readonly sql: string,
    private readonly mapper: ObjectMapper
  ) {}

  build(db: Database): Serializer {
    return new Serializer(db.prepare(this.sql), this.mapper);
  }
}

export class SqliteWritableBuilder {
  private readonly serializerBlueprintsByType = new Map<
    string,
    SerializerBlueprint
  >();

  private maxObjectsInTransaction = 5000;

  private logger?: Logger;

  private highWaterMark?: number;
  private signal?: AbortSignal;

  withType<T extends TypedObject>(
    type: T["type"],
    sql: string,
    mapper: (source: T) => StatementArguments
  ): this {
    this.serializerBlueprintsByType.set(
      type,
      new SerializerBlueprint(sql, mapper as ObjectMapper)
    );

    return this;
  }

  withSafeType<T extends TypedObject>(
    type: T["type"],
    tableName: string,
    columns: readonly string[],
    mapper: (source: T) => StatementArguments
  ): this {
    const sql = `
    INSERT OR IGNORE INTO ${tableName}
    (${columns.join(", ")})
    VALUES
    (${Array(columns.length).fill("?").join(", ")})`;

    return this.withType<T>(type, sql, mapper);
  }

  withMaxObjectsInTransaction(maxObjectsInTransaction: number): this {
    this.maxObjectsInTransaction = maxObjectsInTransaction;
    return this;
  }

  withLogger(logger?: Logger): this {
    this.logger = logger;
    return this;
  }

  withHighWaterMark(highWaterMark?: number): this {
    this.highWaterMark = highWaterMark;
    return this;
  }

  withSignal(signal?: AbortSignal): this {
    this.signal = signal;
    return this;
  }

  build(db: Database): SqliteWritable {
    if (this.maxObjectsInTransaction <= 0) {
      throw new Error(
        `Invalid max objects in transaction: ${this.maxObjectsInTransaction}`
      );
    }

    const serializersByType = new Map<string, Serializer>(
      Array.from(this.serializerBlueprintsByType.entries()).map(
        ([type, blueprint]) => [type, blueprint.build(db)]
      )
    );

    const settings: SqliteWritableSettings = {
      db,
      serializersByType,
      maxObjectsInTransaction: this.maxObjectsInTransaction,
      logger: this.logger,
      highWaterMark: this.highWaterMark,
      signal: this.signal
    };

    return new SqliteWritable(settings);
  }
}
