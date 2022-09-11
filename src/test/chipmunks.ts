import { Database } from "better-sqlite3";

export type Chipmunk = Readonly<{
  type: "chipmunk";
  name: string;
  gatheredNuts: number;
}>;

export const cip: Chipmunk = {
  type: "chipmunk",
  name: "Cip",
  gatheredNuts: 90
};

export const ciop: Chipmunk = {
  type: "chipmunk",
  name: "Ciop",
  gatheredNuts: 92
};

export function retrieveChipmunks(db: Database): ReadonlySet<Chipmunk> {
  return new Set<Chipmunk>(
    db
      .prepare("SELECT * FROM chipmunks")
      .all()
      .map(
        chipmunkRow =>
          ({
            type: "chipmunk",
            name: chipmunkRow.name,
            gatheredNuts: chipmunkRow.gathered_nuts
          } as Chipmunk)
      )
  );
}
