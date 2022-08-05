import { Writable } from "node:stream";
import { Database, Statement } from "better-sqlite3";
import { Logger } from "@giancosta86/unified-logging";
import { formatError } from "@giancosta86/format-error";

const TYPE_FIELD = "type";

export type TypedObject = { type: string };

export type StatementArguments = readonly unknown[];
export type ObjectMapper = (source: object) => StatementArguments;

class SerializerBlueprint {
  constructor(
    private readonly sql: string,
    private readonly mapper: ObjectMapper
  ) {}

  build(db: Database): Serializer {
    return new Serializer(db.prepare(this.sql), this.mapper);
  }
}

class Serializer {
  constructor(
    private readonly statement: Statement,
    private readonly mapper: ObjectMapper
  ) {}

  serialize(source: object) {
    const statementArguments = this.mapper(source);

    this.statement.run(statementArguments);
  }
}

type SqliteWritableSettings = Readonly<{
  db: Database;
  serializersByType: Map<string, Serializer>;
  maxObjectsInTransaction: number;
  logger?: Logger;
  highWaterMark?: number;
  signal?: AbortSignal;
}>;

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

class SqliteWritable extends Writable {
  private readonly db: Database;
  private readonly serializersByType: Map<string, Serializer>;
  private readonly maxObjectsInTransaction: number;
  private readonly logger?: Logger;

  private inTransaction = false;
  private objectsInTransaction = 0;

  constructor(settings: SqliteWritableSettings) {
    super({
      objectMode: true,
      highWaterMark: settings.highWaterMark,
      signal: settings.signal
    });

    this.db = settings.db;
    this.serializersByType = settings.serializersByType;
    this.maxObjectsInTransaction = settings.maxObjectsInTransaction;
    this.logger = settings.logger;
  }

  override _write(
    chunk: unknown,
    _encoding: BufferEncoding,
    callback: (error?: Error | null | undefined) => void
  ): void {
    try {
      this.runInTransaction(() => this.tryToSaveToDb(chunk));
    } catch (err) {
      return callback(err as any);
    }

    callback();
  }

  override _writev?(
    chunks: Array<{
      chunk: unknown;
      encoding: BufferEncoding;
    }>,
    callback: (error?: Error | null) => void
  ): void {
    try {
      this.runInTransaction(() => {
        for (const { chunk } of chunks) {
          this.tryToSaveToDb(chunk);
        }
      });
    } catch (err) {
      return callback(err as any);
    }

    callback();
  }

  private runInTransaction(dbOperations: () => void): void {
    if (!this.inTransaction) {
      this.logger?.debug("Beginning transaction...");
      this.db.exec("BEGIN TRANSACTION");
      this.inTransaction = true;
    }

    dbOperations();

    if (this.objectsInTransaction >= this.maxObjectsInTransaction) {
      this.logger?.debug(
        `Committing transaction with ${this.objectsInTransaction} objects...`
      );
      this.db.exec("COMMIT");
      this.objectsInTransaction = 0;
      this.inTransaction = false;
    }
  }

  private tryToSaveToDb(foundObject: unknown): void {
    if (typeof foundObject != "object" || foundObject === null) {
      this.logger?.error("Encountered non-object value");
      return;
    }

    const objectType = Reflect.get(foundObject, TYPE_FIELD);

    if (!objectType || typeof objectType != "string") {
      this.logger?.error(
        `Encountered object without the '${TYPE_FIELD}' string field`
      );
      return;
    }

    const serializer = this.serializersByType.get(objectType);
    if (!serializer) {
      this.logger?.error(`Unregistered type: '${objectType}'`);
      return;
    }

    try {
      serializer.serialize(foundObject);
      this.objectsInTransaction++;
    } catch (err) {
      this.logger?.error(formatError(err));
      return;
    }
  }

  override _final(callback: (error?: Error | null | undefined) => void): void {
    try {
      if (this.objectsInTransaction) {
        this.logger?.debug(
          `Final commit with ${this.objectsInTransaction} objects...`
        );
        this.db.exec("COMMIT");
        this.objectsInTransaction = 0;
        this.inTransaction = false;
      }
    } catch (err) {
      return callback(err as any);
    }

    callback();
  }
}
