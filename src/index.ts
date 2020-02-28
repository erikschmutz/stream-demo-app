import "reflect-metadata";
const Kinesis = require("lifion-kinesis");
import * as AWS from "aws-sdk";
import dotenv = require("dotenv");
const express = require("express");
const app = express();
var cors = require("cors");
dotenv.config();

const region = process.env.AWS_REGION;
const streamName = process.env.KINESIS_STREAM_NAME;
const accessKeyId = process.env.AWS_ACCESS_KEY_ID;
const secretAccessKey = process.env.AWS_SECRET_KEY_ID;
const port = process.env.PORT;
let dataEntries: { [key: string]: number }[] = [];

AWS.config.update({
  region,
  accessKeyId,
  secretAccessKey
});

const bootstrap = async () => {
  const kinesis = new Kinesis({
    streamName
  });
  kinesis.on("data", (data: any) => {
    data.records.map(() => {});
    const newData: { [key: string]: number } = {};
    data.records.map((v: any) => {
      newData[v.data.ITEM] = newData[v.ITEM] + 1 || 1;
    });

    const prev = dataEntries[dataEntries.length - 1] || {};

    for (const i in { ...prev, ...newData }) {
      newData[i] = (newData[i] || 0) + (prev[i] || 0);
    }
    newData.TIME = Date.now();
    dataEntries.push(newData);
  });

  app.use(cors());

  app.get("/", (req: any, res: any) => {
    res.send("Hello World");
  });

  app.get("/products/data", (req: any, res: any) => {
    res.send(JSON.stringify(dataEntries));
  });

  app.get("/products/clear", (req: any, res: any) => {
    res.send("Cleared list");
    dataEntries = [];
  });

  app.listen(port, () => {
    console.info(`Server is listening on port ${port}.`);
  });

  console.log("Service started...");

  await kinesis.startConsumer();
};
bootstrap();
