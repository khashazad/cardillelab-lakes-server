import express from "express";
import "./loadEnvironment.mjs";
import {
  createExportTaskAsync,
  getAllExportTasksAsync,
} from "./handlers/export-tasks.mjs";

import * as fastcsv from "fast-csv";
import * as fs from "fs";

import { Agenda } from "agenda";

const app = express();
const port = process.env.PORT || 4000;
const mongo_connection_string =
  process.env.MONGO_URI || "mongodb://localhost:27017/Lakes";

app.get("/exports", getAllExportTasksAsync);
app.post("/exports", createExportTaskAsync);

const agenda = new Agenda({
  db: { address: mongo_connection_string },
  collection: "exportTasks",
  maxConcurrency: 5,
  defaultConcurrency: 4,
});

agenda.define("export", (job) => {
  const { collections, pipeline, filePath } = job.attr.data;

  mongodb.connect(
    mongo_connection_string,
    { useNewUrlParser: true, useUnifiedTopology: true },
    (err, client) => {
      if (err) throw err;

      const db = client.db("Lakes");

      const ws = fs.createWriteStream(filePath);

      for (const col of collections) {
        db[col].aggregate(pipeline).toArray((err, data) => {
          if (err) throw err;

          fastcsv.write(data).pipe(ws);
        });
      }

      client.close();
    },
  );
});

await agenda.start();

app.listen(port, () => {
  console.log(`[server]: Server is running at http://localhost:${port}`);
});
