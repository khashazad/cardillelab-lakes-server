import db from "../lib/db/conn.js";
import { exportQueue } from "../lib/bull/bull.js";
import fs from "fs";
import { ObjectId } from "mongodb";
import { unlink } from "node:fs";

export const createExportTaskAsync = async (req, res, next) => {
  const { name, ...filters } = req.body;

  const regex = new RegExp(`^c(${req.body.fishnets.join("|")})_l8_\\d{2,3}$`);

  const collections = (await db.listCollections().toArray())
    .map((col) => col.name)
    .filter((col) => regex.test(col));

  const filePath = `/tmp/${name}.csv`;

  const exportCollection = db.collection("exports");

  const newExportJob = {
    name,
    filters,
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

export const downloadExportFileAsync = async (req, res, next) => {
  const exportId = req.params.exportId;

  const exportsCollections = db.collection("exports");

  const exportTask = await exportsCollections.findOne({
    _id: new ObjectId(exportId),
  });

  res.setHeader(
    "Content-Disposition",
    `attachment; filename=${`\"${exportTask.file.split("/").at(-1)}\"`}`,
  );
  res.setHeader("Content-Type", "text/csv");

  const fileStream = fs.createReadStream(exportTask.file);
  fileStream.pipe(res);
};

export const deleteExportTaskAsync = async (req, res, next) => {
  const exportId = req.params.exportId;

  const exportsCollection = db.collection("exports");

  const filter = { _id: new ObjectId(exportId) };

  const exportTask = await exportsCollection.findOne(filter);

  unlink(exportTask.file, (err) => {
    if (err) console.log(err);
  });

  await exportsCollection.deleteOne(filter);

  res.status(200).end();
};
