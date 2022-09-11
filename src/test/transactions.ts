import { ArrayLogger } from "@giancosta86/unified-logging";

export type TransactionProperties = Readonly<{
  beginnings: number;
  objectsSavedInIntermediateCommits: readonly number[];
  objectsSavedInFinalCommit: number;
  sqlErrors: number;
}>;

export function getTransactionProperties(
  logger: ArrayLogger
): TransactionProperties {
  return {
    beginnings: getTransactionBeginnings(logger),
    objectsSavedInIntermediateCommits:
      getObjectsSavedInIntermediateCommits(logger),
    objectsSavedInFinalCommit: getObjectsSavedInFinalCommit(logger),
    sqlErrors: getSqliteErrors(logger)
  };
}

function getTransactionBeginnings(logger: ArrayLogger): number {
  return logger.debugMessages.filter(
    message => message == "Beginning transaction..."
  ).length;
}

const INTERMEDIATE_SAVED_OBJECTS_REGEX =
  /Committing transaction with (\d+) objects.../;

function getObjectsSavedInIntermediateCommits(
  logger: ArrayLogger
): readonly number[] {
  return logger.debugMessages.flatMap(message => {
    const match = INTERMEDIATE_SAVED_OBJECTS_REGEX.exec(message);
    return match ? [Number(match[1])] : [];
  });
}

const FINAL_SAVED_OBJECTS_REGEX = /Final commit with (\d+) objects.../;

function getObjectsSavedInFinalCommit(logger: ArrayLogger): number {
  let result: number | undefined = undefined;

  logger.debugMessages.forEach(message => {
    const match = FINAL_SAVED_OBJECTS_REGEX.exec(message);

    if (match) {
      if (result !== undefined) {
        throw new Error("More than just one final transaction!");
      }

      result = Number(match[1]);
    }
  });

  return result ?? 0;
}

function getSqliteErrors(logger: ArrayLogger): number {
  return logger.errorMessages.filter(message => message.match(/^SqliteError/))
    .length;
}
