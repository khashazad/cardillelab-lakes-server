import db from "../lib/db/conn.js";
import { exportQueue } from "../lib/bull/bull.js";

export const createExportTaskAsync = async (req, res, next) => {
  const { name, ...configurations } = req.body;

  const regex = new RegExp(`^c(${req.body.fishnets.join("|")})_l8_\\d{2,3}$`);

  const collections = (await db.listCollections().toArray())
    .map((col) => col.name)
    .filter((col) => regex.test(col));

  const filePath = `/Users/khxsh/Desktop/${name}.csv`;

  const exportCollection = db.collection("exports");

  const newExportJob = {
    name,
    configurations,
    createdOn: new Date(),
    status: "Submitted",
    file: filePath,
  };

  await exportCollection.insertOne(newExportJob);

  exportQueue.add({
    filePath,
    collections,
    ...req.body,
    exportJobId: newExportJob._id,
  });

  res.send(req.body).status(200);
};

export const getAllExportTasksAsync = async (req, res, next) => {
  const exportCollection = db.collection("exports");

  const exports = await exportCollection.find().toArray();

  res.send(exports).status(200);
};
