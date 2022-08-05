import { Readable } from "node:stream";
import { pipeline } from "node:stream/promises";
import { ArrayLogger } from "@giancosta86/unified-logging";
import { SqliteWritableBuilder } from ".";
import {
  replaceDbWithCrashingFake,
  createTestWritable,
  createTestWritableBuilder,
  withTestDb
} from "./_db.test";
import {
  getTransactionProperties,
  TransactionProperties
} from "./_transactions.test";
import { expectStreamError } from "./_stream.test";
import { bubu, bozo, fakeYogi, retrieveBears, yogi } from "./_bears.test";
import { cip, ciop, retrieveChipmunks } from "./_chipmunks.test";

describe("SQLite writable", () => {
  describe("construction", () => {
    describe("when the max number of objects in transaction is 0", () => {
      it("should throw", () =>
        withTestDb(async db => {
          expect(() => createTestWritable(db, 0)).toThrow(
            "Invalid max objects in transaction: 0"
          );
        }));
    });

    describe("when the max number of objects in transaction is < 0", () => {
      it("should throw", () =>
        withTestDb(async db => {
          expect(() => createTestWritable(db, -7)).toThrow(
            "Invalid max objects in transaction: -7"
          );
        }));
    });

    it("should pass the highWaterMark setting", () =>
      withTestDb(async db => {
        const bearsWritable = new SqliteWritableBuilder()
          .withHighWaterMark(231)
          .build(db);

        expect(bearsWritable.writableHighWaterMark).toBe(231);
      }));

    it("should pass the signal setting", () =>
      withTestDb(async db => {
        const abortController = new AbortController();

        const [builder, logger] = createTestWritableBuilder();

        const bearsWritable = builder
          .withMaxObjectsInTransaction(1)
          .withSignal(abortController.signal)
          .build(db);

        bearsWritable.write(yogi);

        abortController.abort();

        bearsWritable.write(bubu);
        bearsWritable.write(bozo);
        bearsWritable.end();

        expect(getTransactionProperties(logger)).toEqual<TransactionProperties>(
          {
            beginnings: 1,
            objectsSavedInIntermediateCommits: [1],
            objectsSavedInFinalCommit: 0,
            sqlErrors: 0
          }
        );

        expect(logger.errorMessages).toEqual([]);
      }));
  });

  describe("when receiving an int from upstream", () => {
    it("should log, without error events", () =>
      withTestDb(async db => {
        const [writable, logger] = createTestWritable(db, 1000);

        await pipeline(Readable.from([90]), writable);

        expect(getTransactionProperties(logger)).toEqual<TransactionProperties>(
          {
            beginnings: 1,
            objectsSavedInIntermediateCommits: [],
            objectsSavedInFinalCommit: 0,
            sqlErrors: 0
          }
        );

        expect(logger.errorMessages).toEqual(["Encountered non-object value"]);
      }));
  });

  describe("when receiving an object without the 'type' field", () => {
    it("should log, without error events", () =>
      withTestDb(async db => {
        const [writable, logger] = createTestWritable(db, 1000);

        await pipeline(Readable.from([{ name: "Dodo" }]), writable);

        expect(getTransactionProperties(logger)).toEqual<TransactionProperties>(
          {
            beginnings: 1,
            objectsSavedInIntermediateCommits: [],
            objectsSavedInFinalCommit: 0,
            sqlErrors: 0
          }
        );

        expect(logger.errorMessages).toEqual([
          "Encountered object without the 'type' string field"
        ]);
      }));
  });

  describe("when receiving an unregistered object", () => {
    it("should log, without error events", () =>
      withTestDb(async db => {
        const logger = new ArrayLogger();

        const writable = new SqliteWritableBuilder()
          .withLogger(logger)
          .build(db);

        await pipeline(Readable.from([yogi]), writable);

        expect(getTransactionProperties(logger)).toEqual<TransactionProperties>(
          {
            beginnings: 1,
            objectsSavedInIntermediateCommits: [],
            objectsSavedInFinalCommit: 0,
            sqlErrors: 0
          }
        );
        expect(logger.errorMessages).toEqual(["Unregistered type: 'bear'"]);
      }));
  });

  describe("when the objects written are less than the max in transaction", () => {
    it("should serialize a single object", () =>
      withTestDb(async db => {
        const [writable, logger] = createTestWritable(db, 1000);

        const sourceBears = new Set([yogi]);

        await pipeline(Readable.from(sourceBears), writable);

        const retrievedBears = retrieveBears(db);

        expect(getTransactionProperties(logger)).toEqual<TransactionProperties>(
          {
            beginnings: 1,
            objectsSavedInIntermediateCommits: [],
            objectsSavedInFinalCommit: 1,
            sqlErrors: 0
          }
        );

        expect(retrievedBears).toEqual(sourceBears);

        expect(logger.errorMessages).toEqual([]);
      }));

    it("should serialize multiple objects of the same type", () =>
      withTestDb(async db => {
        const [writable, logger] = createTestWritable(db, 1000);

        const sourceBears = new Set([yogi, bubu, bozo]);

        await pipeline(Readable.from(sourceBears), writable);

        const retrievedBears = retrieveBears(db);

        expect(getTransactionProperties(logger)).toEqual<TransactionProperties>(
          {
            beginnings: 1,
            objectsSavedInIntermediateCommits: [],
            objectsSavedInFinalCommit: 3,
            sqlErrors: 0
          }
        );

        expect(retrievedBears).toEqual(sourceBears);

        expect(logger.errorMessages).toEqual([]);
      }));

    it("should serialize multiple objects of different types", () =>
      withTestDb(async db => {
        const [writable, logger] = createTestWritable(db, 1000);

        const sourceAnimals = new Set([yogi, cip, bubu, bozo, ciop]);

        await pipeline(Readable.from(sourceAnimals), writable);

        const retrievedBears = retrieveBears(db);
        const retrievedChipmunks = retrieveChipmunks(db);

        expect(getTransactionProperties(logger)).toEqual<TransactionProperties>(
          {
            beginnings: 1,
            objectsSavedInIntermediateCommits: [],
            objectsSavedInFinalCommit: 5,
            sqlErrors: 0
          }
        );

        expect(retrievedBears).toEqual(new Set([yogi, bubu, bozo]));
        expect(retrievedChipmunks).toEqual(new Set([cip, ciop]));

        expect(logger.errorMessages).toEqual([]);
      }));

    describe("when a SQL error occurs", () => {
      it("should go on without interrupting the flow", () =>
        withTestDb(async db => {
          const [writable, logger] = createTestWritable(db, 1000);

          const sourceBears = new Set([fakeYogi, bubu]);

          const expectedBears = new Set([bubu]);

          await pipeline(Readable.from(sourceBears), writable);

          const retrievedBears = retrieveBears(db);

          expect(
            getTransactionProperties(logger)
          ).toEqual<TransactionProperties>({
            beginnings: 1,
            objectsSavedInIntermediateCommits: [],
            objectsSavedInFinalCommit: 1,
            sqlErrors: 1
          });

          expect(retrievedBears).toEqual(expectedBears);

          expect(logger.errorMessages.length).toBe(1);
        }));
    });
  });

  describe("when the objects written are more than the max in transaction", () => {
    it("should serialize a quantized number of objects of the same type", () =>
      withTestDb(async db => {
        const [writable, logger] = createTestWritable(db, 2);

        const sourceBears = new Set([yogi, bubu]);

        await pipeline(Readable.from(sourceBears), writable);

        const retrievedBears = retrieveBears(db);

        expect(getTransactionProperties(logger)).toEqual<TransactionProperties>(
          {
            beginnings: 1,
            objectsSavedInIntermediateCommits: [2],
            objectsSavedInFinalCommit: 0,
            sqlErrors: 0
          }
        );

        expect(retrievedBears).toEqual(sourceBears);

        expect(logger.errorMessages).toEqual([]);
      }));

    it("should serialize a non-quantized number of objects of the same type", () =>
      withTestDb(async db => {
        const [writable, logger] = createTestWritable(db, 2);

        const sourceBears = new Set([yogi, bubu, bozo]);

        await pipeline(Readable.from(sourceBears), writable);

        const retrievedBears = retrieveBears(db);

        expect(getTransactionProperties(logger)).toEqual<TransactionProperties>(
          {
            beginnings: 2,
            objectsSavedInIntermediateCommits: [2],
            objectsSavedInFinalCommit: 1,
            sqlErrors: 0
          }
        );

        expect(retrievedBears).toEqual(sourceBears);

        expect(logger.errorMessages).toEqual([]);
      }));

    it("should serialize a quantized number of objects of different types", () =>
      withTestDb(async db => {
        const [writable, logger] = createTestWritable(db, 2);

        const sourceAnimals = new Set([ciop, yogi, bubu, cip]);

        await pipeline(Readable.from(sourceAnimals), writable);

        const retrievedBears = retrieveBears(db);
        const retrievedChipmunks = retrieveChipmunks(db);

        expect(getTransactionProperties(logger)).toEqual<TransactionProperties>(
          {
            beginnings: 2,
            objectsSavedInIntermediateCommits: [2, 2],
            objectsSavedInFinalCommit: 0,
            sqlErrors: 0
          }
        );

        expect(retrievedBears).toEqual(new Set([yogi, bubu]));
        expect(retrievedChipmunks).toEqual(new Set([cip, ciop]));

        expect(logger.errorMessages).toEqual([]);
      }));

    it("should serialize a non-quantized number of objects of different types", () =>
      withTestDb(async db => {
        const [writable, logger] = createTestWritable(db, 2);

        const sourceAnimals = new Set([ciop, yogi, bubu, cip, bozo]);

        await pipeline(Readable.from(sourceAnimals), writable);

        const retrievedBears = retrieveBears(db);
        const retrievedChipmunks = retrieveChipmunks(db);

        expect(getTransactionProperties(logger)).toEqual<TransactionProperties>(
          {
            beginnings: 3,
            objectsSavedInIntermediateCommits: [2, 2],
            objectsSavedInFinalCommit: 1,
            sqlErrors: 0
          }
        );

        expect(retrievedBears).toEqual(new Set([yogi, bubu, bozo]));
        expect(retrievedChipmunks).toEqual(new Set([cip, ciop]));

        expect(logger.errorMessages).toEqual([]);
      }));

    describe("when a SQL error occurs", () => {
      it("should go on without interrupting the flow", () =>
        withTestDb(async db => {
          const [writable, logger] = createTestWritable(db, 2);

          const sourceAnimals = new Set([
            ciop,
            yogi,
            bubu,
            fakeYogi,
            cip,
            bozo
          ]);

          await pipeline(Readable.from(sourceAnimals), writable);

          const retrievedBears = retrieveBears(db);
          const retrievedChipmunks = retrieveChipmunks(db);

          expect(
            getTransactionProperties(logger)
          ).toEqual<TransactionProperties>({
            beginnings: 3,
            objectsSavedInIntermediateCommits: [2, 2],
            objectsSavedInFinalCommit: 1,
            sqlErrors: 1
          });

          expect(retrievedBears).toEqual(new Set([yogi, bubu, bozo]));
          expect(retrievedChipmunks).toEqual(new Set([cip, ciop]));

          expect(logger.errorMessages.length).toBe(1);
        }));
    });
  });

  describe("when a non-SQL error occurs", () => {
    describe("when writing", () => {
      it("should emit an error event", () =>
        withTestDb(async db => {
          const [writable] = createTestWritable(db, 1000);

          replaceDbWithCrashingFake(writable);

          await expectStreamError(
            writable,
            () => {
              writable.write(yogi);
            },
            err => {
              expect(err.message).toMatch("Test exec error!");
            }
          );
        }));
    });

    describe("when ending", () => {
      it("should emit an error event", () =>
        withTestDb(async db => {
          const [writable] = createTestWritable(db, 1000);

          writable.write(yogi);

          replaceDbWithCrashingFake(writable);

          await expectStreamError(
            writable,
            () => writable.end(),
            err => {
              expect(err.message).toMatch("Test exec error!");
            }
          );
        }));
    });
  });

  describe("when in cork mode", () => {
    it("should serialize objects of the same type", () =>
      withTestDb(async db => {
        const [writable, logger] = createTestWritable(db, 2);

        const sourceBears = new Set([yogi, bubu, bozo]);

        writable.cork();

        sourceBears.forEach(bear => writable.write(bear));

        writable.end();

        const retrievedBears = retrieveBears(db);

        expect(getTransactionProperties(logger)).toEqual<TransactionProperties>(
          {
            beginnings: 1,
            objectsSavedInIntermediateCommits: [3],
            objectsSavedInFinalCommit: 0,
            sqlErrors: 0
          }
        );

        expect(retrievedBears).toEqual(sourceBears);
      }));

    it("should serialize objects of different types", () =>
      withTestDb(async db => {
        const [writable, logger] = createTestWritable(db, 2);

        const sourceAnimals = new Set([ciop, yogi, bubu, cip, bozo]);

        writable.cork();

        sourceAnimals.forEach(animal => writable.write(animal));

        writable.end();

        const retrievedBears = retrieveBears(db);
        const retrievedChipmunks = retrieveChipmunks(db);

        expect(getTransactionProperties(logger)).toEqual<TransactionProperties>(
          {
            beginnings: 1,
            objectsSavedInIntermediateCommits: [5],
            objectsSavedInFinalCommit: 0,
            sqlErrors: 0
          }
        );

        expect(retrievedBears).toEqual(new Set([yogi, bubu, bozo]));
        expect(retrievedChipmunks).toEqual(new Set([cip, ciop]));
      }));

    it("should continue serializing despite SQL errors", () =>
      withTestDb(async db => {
        const [writable, logger] = createTestWritable(db, 2);

        const sourceBears = new Set([bubu, fakeYogi, bozo]);
        const expectedBears = new Set([bubu, bozo]);

        writable.cork();

        sourceBears.forEach(bear => writable.write(bear));

        writable.end();

        const retrievedBears = retrieveBears(db);

        expect(getTransactionProperties(logger)).toEqual<TransactionProperties>(
          {
            beginnings: 1,
            objectsSavedInIntermediateCommits: [2],
            objectsSavedInFinalCommit: 0,
            sqlErrors: 1
          }
        );

        expect(retrievedBears).toEqual(expectedBears);
      }));

    describe("when a non-SQL error occurs during uncork()", () => {
      it("should emit an error event", () =>
        withTestDb(async db => {
          const [writable, logger] = createTestWritable(db, 2);

          const sourceBears = new Set([bubu, yogi, bozo]);

          writable.cork();

          sourceBears.forEach(bear => writable.write(bear));

          replaceDbWithCrashingFake(writable);

          expectStreamError(
            writable,
            () => writable.uncork(),
            err => {
              expect(err.message).toBe("Test exec error!");
            }
          );

          expect(logger.errorMessages).toEqual([]);
        }));
    });
  });

  describe("when destroyed via an Error", () => {
    it("should emit the error", () =>
      withTestDb(async db => {
        const testError = new Error("This is just a test error");

        const writable = new SqliteWritableBuilder().build(db);

        await expectStreamError(
          writable,
          () => writable.destroy(testError),
          err => {
            expect(err).toBe(testError);
          }
        );
      }));

    it("should support non-Error objects", () =>
      withTestDb(async db => {
        const writable = new SqliteWritableBuilder().build(db);

        await expectStreamError(
          writable,
          () => (writable as any).destroy(90),
          err => {
            expect(err).toBe(90);
          }
        );
      }));
  });
});
