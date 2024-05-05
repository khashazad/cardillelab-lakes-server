import db from "../db/conn.mjs";

export const createExportTaskAsync = async (req, res, next) => {
  const collections = (await db.listCollections().toArray()).map(
    (col) => col.name,
  );

  res.send(collections);
};

export const getAllExportTasksAsync = (req, res, next) => {};
