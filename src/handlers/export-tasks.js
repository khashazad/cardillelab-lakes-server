import db from "../lib/db/conn.js";
import { exportQueue } from "../lib/bull/bull.js";

export const createExportTaskAsync = async (req, res, next) => {
  const regex = /^c(1|2|3)_l8_\d{2,3}/;
  const collections = (await db.listCollections().toArray())
    .map((col) => col.name)
    .filter((col) => regex.test(col));

  exportQueue.add({
    filePath: "/Users/khxsh/Desktop/lakes-export-2.csv",
    collections: ["c3_l8_1", "c3_l8_10"],
    bands: [
      "sr_band1",
      "sr_band2",
      "sr_band3",
      "sr_band4",
      "sr_band5",
      "sr_band6",
      "sr_band7",
      "st_band10",
      "qa_pixel",
      "qa_radsat",
    ],
    cloudThreshold: 50,
    buffers: [60],
  });

  res.send().status(200);
};

export const getAllExportTasksAsync = (req, res, next) => {};
