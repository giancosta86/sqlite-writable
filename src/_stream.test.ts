import { Stream } from "node:stream";

export function expectStreamError(
  stream: Stream,
  actions: () => void,
  expectations: (error: Error) => void
): Promise<void> {
  return new Promise<void>((resolve, reject) => {
    stream.on("error", err => {
      try {
        expectations(err);
      } catch (expectationsErr) {
        return reject(expectationsErr);
      }

      resolve();
    });

    actions();
  });
}
