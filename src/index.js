import express from "express";
import "./lib/loadEnvironment.js";
import {
  createExportTaskAsync,
  getAllExportTasksAsync,
} from "./handlers/export-tasks.js";
import "./lib/bull/bull.js";

const app = express();
const port = process.env.PORT || 4000;

app.get("/exports", getAllExportTasksAsync);
app.post("/exports", createExportTaskAsync);

app.listen(port, () => {
  console.log(`[server]: Server is running at http://localhost:${port}`);
});
