import Queue from "bull";
import { MongoClient } from "mongodb";
import createCsvWriter from "csv-writer";

export const exportQueue = new Queue("Export", "redis://127.0.0.1:6379");

exportQueue.process(5, async function (job) {
  const { collections, filePath, bands, cloudThreshold, buffers } = job.data;

  const mongo_connection_string = process.env.MONGO_URI || "";

  const client = new MongoClient(mongo_connection_string);

  const db = client.db("Lakes");

  const csvWriter = createCsvWriter.createObjectCsvWriter({
    path: filePath,
    header: [
      { id: "hylak_id", title: "hylak_id" },
      { id: "fishnet", title: "fishnet" },
      { id: "fish_id", title: "fish_id" },
      { id: "buffer", titel: "buffer" },
      { id: "image_sat", title: "image_sat" },
      { id: "image_id", title: "image_id" },
      { id: "image_date", title: "image_date" },
      { id: "cloud_cover", title: "cloud_cover" },
      { id: "sr_band1", title: "sr_band1" },
      { id: "sr_band2", title: "sr_band2" },
      { id: "sr_band3", title: "sr_band3" },
      { id: "sr_band4", title: "sr_band4" },
      { id: "sr_band5", title: "sr_band5" },
      { id: "sr_band6", title: "sr_band6" },
      { id: "sr_band7", title: "sr_band7" },
      { id: "st_band10", title: "st_band10" },
      { id: "qa_pixel", title: "qa_pixel" },
      { id: "qa_radsat", title: "qa_radsat" },
    ],
  });

  for (const col of collections) {
    for (const buffer of buffers) {
      const fishnet = col.split("_")[0][1];

      const fishId = col.split("_")[2];

      const pipeline = [];

      if (cloudThreshold)
        pipeline.push({
          $match: { "image.cloud_cover": { $lt: cloudThreshold } },
        });

      const projectStage = {
        _id: 0,
        fishnet: fishnet,
        fish_id: fishId,
        hylak_id: 1,
        buffer: buffer,
        image_sat: "Landsat8",
        image_id: "$image.id",
        image_date: "$image.date",
        cloud_cover: "$image.cloud_cover",
      };

      for (const band of bands) {
        projectStage[`${band}`] = `$${band}.${buffer}`;
      }

      pipeline.push({ $project: projectStage });

      const records = await db.collection(col).aggregate(pipeline).toArray();

      await csvWriter.writeRecords(records);
    }
  }

  return Promise.resolve();
});
