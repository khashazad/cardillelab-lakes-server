import Queue from "bull";
import { MongoClient, ObjectId } from "mongodb";
import createCsvWriter from "csv-writer";
import fs from "fs";

export const exportQueue = new Queue(
  "Export",
  `redis://${process.env.REDIS_HOST}:${process.env.REDIS_PORT}`,
);

const extractIdsFromCollectionName = (collection) => {
  const parts = collection.split("_");

  const fishnet = parts[0][1];
  const fishId = parts[2];

  return { fishnet, fishId };
};

const buildAggregationPipeline = (config) => {
  const { buffer, bands, cloudCoverThreshold, years, fishnet, fishId } = config;

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
    buffer: String(buffer),
    image_sat: "Landsat8",
    image_id: "$image.id",
    image_date: "$image.date",
    cloud_cover: "$image.cloud_cover",
  };

  for (const band of bands) {
    projectStage[`${band}`] = `$${band}.${buffer}`;
  }

  pipeline.push({ $project: projectStage });

  return pipeline;
};

exportQueue.process(5, async function (job) {
  const { collections, filePath, buffers, exportJobId, ...config } = job.data;

  const client = new MongoClient(process.env.MONGO_URI);

  const db = client.db(process.env.MONGO_DATABASE);

  const exportCollection = db.collection("exports");

  await exportCollection.updateOne(
    { _id: new ObjectId(exportJobId) },
    { $set: { status: "In Progress" } },
  );

  try {
    const csvWriter = createCsvWriter.createObjectCsvWriter({
      path: filePath,
      header: [
        { id: "hylak_id", title: "hylak_id" },
        { id: "fishnet", title: "fishnet" },
        { id: "fish_id", title: "fish_id" },
        { id: "buffer", title: "buffer" },
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

    let processed = 0;

    for (const col of collections) {
      for (const buffer of buffers) {
        const pipeline = buildAggregationPipeline({
          ...config,
          ...extractIdsFromCollectionName(col),
          buffer,
        });

        const records = await db.collection(col).aggregate(pipeline).toArray();

        await csvWriter.writeRecords(records);
      }

      await exportCollection.updateOne(
        { _id: new ObjectId(exportJobId) },
        {
          $set: {
            progress: Math.floor((++processed * 100) / collections.length),
          },
        },
      );
    }

    const fileSize = (await fs.promises.stat(filePath)).size;

    await exportCollection.updateOne(
      { _id: new ObjectId(exportJobId) },
      { $set: { status: "Completed", completedOn: new Date(), fileSize } },
    );
  } catch (error) {
    await exportCollection.updateOne(
      { _id: new ObjectId(exportJobId) },
      {
        $set: { status: "Failed", error: error.message, failedOn: new Date() },
      },
    );
  }

  return Promise.resolve();
});
