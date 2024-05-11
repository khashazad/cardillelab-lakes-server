import express from "express";
import "./lib/loadEnvironment.js";
import {
  createExportTaskAsync,
  getAllExportTasksAsync,
} from "./handlers/export-tasks.js";
import "./lib/bull/bull.js";
import cors from "cors";
import bodyParser from "body-parser";

const app = express();
const port = process.env.PORT || 4000;

app.use(cors());
app.use(bodyParser.json());

app.get("/exports", getAllExportTasksAsync);
app.post("/exports", createExportTaskAsync);

app.listen(port, () => {
  console.log(`[server]: Server is running at http://localhost:${port}`);
});
