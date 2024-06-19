import * as fastcsv from "fast-csv";
import * as fs from "fs";
import { getEnvVariable } from "../lib/utils.js";

import { MongoClient } from "mongodb";

module.exports = async function (job, done) {
  const { collections, filePath, bands, cloudCoverThreshold, years, buffers } =
    job.data;

  const connectionString = getEnvVariable(MONGO_URI);

  const client = new MongoClient(connectionString);

  const db = client.db("Lakes");

  const ws = fs.createWriteStream(filePath);

  for (const col of collections) {
    const fishnet = col.split("_")[0][1];

    const fishId = col.split("_")[2];

    for (const buffer of buffers) {
      const pipeline = [
        {
          $match: { "image.year": { $in: years } },
        },
      ];

      if (cloudCoverThreshold)
        pipeline.push({
          $match: { "image.cloud_cover": { $lt: cloudCoverThreshold } },
        });

      const projectStage = {
        _id: 0,
        hylak_id: 1,
        fishnet: fishnet,
        fish_id: fishId,
        buffer: buffer,
        image_sat: "Landsat8",
        image_id: "$image.id",
        image_date: "$image.date",
        cloud_cover: "$image.cloud_cover",
      };

      for (const band of bands) {
        projectStage[`${band}_${buffer}`] = `$${band}.${buffer}`;
      }

      pipeline.push({ $project: projectStage });

      const data = await db.collection(col).aggregate(pipeline).toArray();

      fastcsv.write(data).pipe(ws);
    }
  }

  client.close();

  done();
};
