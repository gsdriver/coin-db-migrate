/* tslint:disable-next-line */
const config = require("dotenv").config();
import * as logger from "./logger";
import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { GetObjectCommand, S3Client } from "@aws-sdk/client-s3";
import { PutCommand, DynamoDBDocumentClient } from "@aws-sdk/lib-dynamodb";

interface CoinPrice {
  grade: number;
  price: number;
}

interface CoinIssue {
  name: string; // Eventually will break out year, mintmark, variety
  variety?: string;
  prices: CoinPrice[];
}

interface CoinSeries {
  name: string;
  issues: CoinIssue[];
  price_as_of: Date;
}

const zeroPad = (d: number) => {
  return (`0${d}`).slice(-2);
};

const formatDate = (d: Date | undefined): string => {
  if (!d) {
    return "";
  }

  return `${d.getFullYear()}-${zeroPad(d.getMonth() + 1)}-${zeroPad(d.getDate())}`;
};

const readFromS3 = async (bucket: string, key: string): Promise<string> => {
  let value: any;
  const region = "us-west-2";
  const client = new S3Client({ region });

  try {
    const streamToString = (stream: any) =>
      new Promise((resolve, reject) => {
        const chunks: any[] = [];
        stream.on("data", (chunk: any) => chunks.push(chunk));
        stream.on("error", reject);
        stream.on("end", () => resolve(Buffer.concat(chunks).toString("utf8")));
      });

    const command = new GetObjectCommand({
      Bucket: bucket,
      Key: key,
    });

    const { Body } = await client.send(command);
    value = await streamToString(Body);
  }
  catch (e) {
    logger.info("Error reading from S3", { value });
  }

  return value;
};

// Write a function that writes a CoinIssue to DynamoDB
const writeToDynamo = async (seriesName: string, price_as_of: Date, coinIssue: CoinIssue): Promise<void> => {
  // Primary Key is seriesName|coinIssue.name|coinIssue.variety
  const primaryKey = coinIssue.variety ? `${seriesName}|${coinIssue.name}|${coinIssue.variety}` : `${seriesName}|${coinIssue.name}`;
  
  // Create price JSON object from coinIssues.prices
  const prices: any = {};
  coinIssue.prices.forEach((price: CoinPrice) => {
    prices[price.grade] = price.price;
  });

  // Write the item to DynamoDB
  try {
    const client = new DynamoDBClient({});
    const docClient = DynamoDBDocumentClient.from(client);
    
    const command = new PutCommand({
      TableName: process.env.DYNAMODB_TABLE,
      Item: {
        coin: primaryKey,
        price_as_of: formatDate(price_as_of),
        prices: JSON.stringify(prices),
      },
    });
  
    await docClient.send(command);
  }
  catch (e) {
    logger.info("Error writing to DynamoDB", { seriesName, price_as_of, coinIssue });
  }
};

exports.handler = async (event: any, context: any) => {
  logger.info("received event", { event });

  // Get the object from the event and show its content type
  const bucket = event.Records[0].s3.bucket.name;
  const key = decodeURIComponent(event.Records[0].s3.object.key.replace(/\+/g, ' '));
  const params = {
      Bucket: bucket,
      Key: key,
  };

  try {
      // Let's read the list from S3 -- if not present, try to generate and read
      let dataStr: string = await readFromS3(bucket, key);
      const seriesName: string = (key.split("/")[1]).split(".csv")[0];
      const price_as_of: Date = new Date(key.split("/")[0]);

      // First line is year, variety, and grade values
      const lines = dataStr.split("\n");
      const header = lines[0].split(",");
      let l: number;
      for (l = 1; l < lines.length; l++) {
        const issue: string[] = lines[l].split(",");
        if (issue.length >= header.length) {
          const coinIssue: CoinIssue = { name: issue[0], variety: issue[1], prices: [] };
          let p: number;
          for (p = 2; p < issue.length; p++) {
            coinIssue.prices.push({ grade: parseInt(header[p], 10), price: parseInt(issue[p], 10) });
          }

          // And write this coin to DynamoDB
          await writeToDynamo(seriesName, price_as_of, coinIssue);
        }
      }
  } catch (err) {
      console.log(err);
      const message = `Error getting object ${key} from bucket ${bucket}. Make sure they exist and your bucket is in the same region as this function.`;
      console.log(message);
      throw new Error(message);
  }
};
