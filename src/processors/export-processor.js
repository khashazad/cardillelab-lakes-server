import * as fastcsv from "fast-csv";
import * as fs from "fs";

module.exports = function (job) {
  // Do some heavy work
  const { collections, filePath, config } = job.data;

  mongodb.connect(
    mongo_connection_string,
    { useNewUrlParser: true, useUnifiedTopology: true },
    (err, client) => {
      if (err) throw err;

      const db = client.db("Lakes");

      const ws = fs.createWriteStream(filePath);

      for (const col of collections) {
        const fishnet = col.split("_")[0][1];

        const fishId = col.split("_")[2];

        const pipeline = [];

        const cloudThreshold = 50;

        const bands = [
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
        ];

        if (cloudThreshold)
          pipeline.push({
            $match: { "image.cloud_cover": { $lt: cloudThreshold } },
          });

        const projectStage = {
          _id: 0,
          hylak_id: 1,
          fishnet: fishnet,
          fish_id: fishId,
          image_sat: "Landsat8",
          image_id: "$image.id",
          image_date: "$image.date",
          cloud_cover: "$image.cloud_cover",
        };

        for (const band of bands) {
          projectStage[`${band}_${buffer}`] = `$${band}.${buffer}`;
        }

        pipeline.push(projectStage);

        db[col].aggregate(pipeline).toArray((err, data) => {
          if (err) throw err;

          fastcsv.write(data).pipe(ws);
        });
      }

      client.close();
    },
  );

  return Promise.resolve();
};
