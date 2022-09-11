import { Database } from "better-sqlite3";

export type Bear = Readonly<{
  type: "bear";
  name: string;
  age: number | null;
}>;

export const yogi: Bear = {
  type: "bear",
  name: "Yogi",
  age: 36
};

export const bubu: Bear = {
  type: "bear",
  name: "Bubu",
  age: 34
};

export const bozo: Bear = {
  type: "bear",
  name: "Bozo",
  age: 31
};

export const fakeYogi: Bear = {
  type: "bear",
  name: "Yogi",
  age: null
};

export function retrieveBears(db: Database): ReadonlySet<Bear> {
  return new Set<Bear>(
    db
      .prepare("SELECT * FROM bears")
      .all()
      .map(
        bearRow =>
          ({
            ...bearRow,
            type: "bear"
          } as Bear)
      )
  );
}
