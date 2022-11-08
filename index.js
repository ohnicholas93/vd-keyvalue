let DEBUG = (function(){
  let timestamp = function(){};
  timestamp.toString = function(){ return `[${(new Date).toLocaleString()}] `; };

  return { log: console.log.bind(console, '%s', timestamp) };
})();


const path = require('path');
require('dotenv').config({ path: path.resolve(process.cwd(), '.env') });
const { exit } = require('process');

const { MongoClient } = require("mongodb");
const https = require('https');
const fs = require('fs');
const express = require("express");
const app = express();

app.use(express.json());
app.use((req, res, next) => {
  res.append('Access-Control-Allow-Origin', req.headers.origin);
  res.append('Access-Control-Allow-Headers', 'Content-Type');
  res.append('Access-Control-Allow-Credentials',  'true');
  next();
});

const mClient = new MongoClient(process.env.DB_CONN_STRING);
let db, privateKey, certificate;
const ERRORS = {
  1: "Multiple or no keys provided.",
  2: "Server error.",
  3: "No key specified.",
  4: "Key not found in database.",
  5: "No data available for specified timestamp.",
}

try {
  privateKey = fs.readFileSync( '/etc/letsencrypt/live/srv1.nitrous.dev/privkey.pem' );
  certificate = fs.readFileSync( '/etc/letsencrypt/live/srv1.nitrous.dev/fullchain.pem' );
} catch (err) {}

function recursiveBinarySearch(versions, time, left, right) {
  if (left > right) return null;

  const idx = parseInt((right - left) / 2) + left;
  if ((versions[idx] <= time) && 
    ((!versions[idx + 1]) || (versions[idx + 1] > time))) return versions[idx];
  else {
    let res;
    if (versions[idx] > time) res = recursiveBinarySearch(versions, time, left, idx - 1);
    else res = recursiveBinarySearch(versions, time, idx + 1, right);
    return res;
  }
}

function getVersion(versions, time) {
  const arrSize = versions.length;
  if (versions[arrSize - 1] && (versions[arrSize - 1] <= time)) return versions[arrSize - 1];
  if (versions[arrSize - 2] && (versions[arrSize - 2] <= time)) return versions[arrSize - 2];
  if (versions[arrSize - 3] && (versions[arrSize - 3] <= time)) return versions[arrSize - 3];
  if (versions[arrSize - 4] && (versions[arrSize - 4] <= time)) return versions[arrSize - 4];
  if (versions[arrSize - 5] && (versions[arrSize - 5] <= time)) return versions[arrSize - 5];
  if (versions[0] > time) return null;

  return recursiveBinarySearch(versions, time, 0, arrSize - 6);
}

app.post("/object", async (req, res) => {
  let dataKey = Object.keys(req.body);
  if (dataKey.length !== 1) {
    res.status(400).json({ "errorCode": 1, "errorMsg": ERRORS[1] });
    return;
  }

  dataKey = dataKey[0];
  const postVersion = Date.now();

  try {
    const insertRes = await db.collection("data").insertOne({ 
      '_id': `${dataKey}0${postVersion}`,
      'v': req.body[dataKey]
    });

    if (insertRes.acknowledged === true) {
      db.collection("versions").updateOne(
        { _id: dataKey },
        { $push: {
            versions: postVersion
        }},
        { upsert: true }
      );

      res.status(200).json({
        "key": dataKey,
        "value": req.body[dataKey],
        "timestamp": postVersion
      })

    } else {
      res.status(500).json({ "errorCode": 2, "errorMsg": ERRORS[2] });
    }
    

  } catch (err) {
    DEBUG.log(err.message);
    res.status(500).json({ "errorCode": 2, "errorMsg": ERRORS[2] });
  }

});

app.get("/object/:key", async (req, res) => {
  if (!req.params.key) {
    res.status(400).json({ "errorCode": 3, "errorMsg": ERRORS[3] });
    return;
  }

  try {
    const callTime = req.query.timestamp ? req.query.timestamp : Date.now();
    const keyVersions = await db.collection("versions").findOne({
      "_id": req.params.key
    });
    if (!keyVersions) {
      res.status(400).json({ "errorCode": 4, "errorMsg": ERRORS[4] });
      return;
    }
    const ver = getVersion(keyVersions.versions, callTime);
    if (ver == null) {
      res.status(400).json({ "errorCode": 5, "errorMsg": ERRORS[5] });
      return;
    }
    const queryRes = await db.collection("data").findOne(
      {
        "_id": `${req.params.key}0${ver}`
      },
      {
        projection: {
          "_id": false,
          "time": false,
          'k': false,
        }
      }
    );

    res.status(200).json({
      "value": queryRes.v
    });

  } catch (err) {
    DEBUG.log(err);
    res.status(500).json({ "errorCode": 2, "errorMsg": ERRORS[2] });
  }
});

async function startServer() {
  try {
    await mClient.connect();
    db = mClient.db("vd-keyvalue");

    if (privateKey && certificate) {
      https.createServer({
        key: privateKey,
        cert: certificate
      }, app).listen(process.env.SERVER_PORT);
      DEBUG.log(`Listening with HTTPS on ${process.env.SERVER_PORT}`);
    } else {
      app.listen(process.env.SERVER_PORT);
      DEBUG.log(`Listening with HTTP on ${process.env.SERVER_PORT}`);
    }
  } catch (err) {
    DEBUG.log(err);
    DEBUG.log("Failed to start server.");
  }

}

async function cleanup() {
  mClient.close();
  DEBUG.log("Successfully closed MongoDB connection.");
  DEBUG.log("Shutting down server.");
  exit();
}

process.on('SIGINT', cleanup);
process.on('SIGTERM', cleanup);

startServer();