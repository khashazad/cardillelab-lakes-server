import { MongoClient } from "mongodb";

const connectionString =
  process.env.MONGO_URI ||
  "mongodb://lakesadmin:lakeharvest2021@10.5.0.2:27017";

const client = new MongoClient(connectionString);
let conn;
try {
  conn = await client.connect();
} catch (e) {
  console.error(e);
}
let db = conn.db("Lakes");
export default db;
