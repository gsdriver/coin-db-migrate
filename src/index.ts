/* tslint:disable-next-line */
const config = require("dotenv").config();
import * as logger from "./logger";
import { CoinIssue, CoinPrice, formatDate, listKeys, readFromS3 } from "./utils";
import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { PutObjectCommand, S3Client } from "@aws-sdk/client-s3";
import { PutCommand, DynamoDBDocumentClient } from "@aws-sdk/lib-dynamodb";

const LASTRUNFILE = "coin-db-migrate/latest.txt";

// Write a function that writes a CoinIssue to DynamoDB
const writeToDynamo = async (seriesName: string, price_as_of: Date, coinIssue: CoinIssue): Promise<void> => {
  // Primary Key is seriesName|coinIssue.name|coinIssue.variety
  const primaryKey = coinIssue.variety ? `${seriesName}|${coinIssue.name}|${coinIssue.variety}` : `${seriesName}|${coinIssue.name}`;

  // Create price JSON object from coinIssues.prices
  const prices: any = {};
  coinIssue.prices.forEach((price: CoinPrice) => {
    prices[price.grade] = price.price;
  });

  const params = {
    TableName: process.env.DYNAMODB_TABLE,
    Item: {
      coin: primaryKey,
      price_as_of: formatDate(price_as_of),
      prices: JSON.stringify(prices),
    },
  };

  // Write the item to DynamoDB
  try {
    const client = new DynamoDBClient({});
    const docClient = DynamoDBDocumentClient.from(client);
    const command = new PutCommand(params);

    await docClient.send(command);
  }
  catch (e) {
    logger.info("Error writing info to DynamoDB", { seriesName, price_as_of, coinIssue, params });
    logger.error((e as any)?.message, "Error writing to DynamoDB");
  }
};

const processKey = async (bucket: string, keyName: string): Promise<void> => {
  // Get the object from the event and show its content type
  const key = decodeURIComponent(keyName.replace(/\+/g, ' '));
  const params = {
      Bucket: bucket,
      Key: key,
  };

  try {
      // Let's read the list from S3 -- if not present, try to generate and read
      const dataStr: string = await readFromS3(bucket, key);
      const seriesName: string = (key.split("/")[1]).split(".csv")[0];
      const price_as_of: Date = new Date(key.split("/")[0]);

      // First line is year, variety, and grade values
      const lines = dataStr.split("\n");
      const header = lines[0].split(",");
      let l: number;
      for (l = 1; l < lines.length; l++) {
        const issue: string[] = lines[l].split(",");
        if ((issue.length >= header.length) && !!issue[0]?.length) {
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
      const message = `Error getting object ${key} from bucket ${bucket}. Make sure they exist and your bucket is in the same region as this function.`;
      logger.info(message, { err });
      throw new Error(message);
  }
};

const runBackfill = async (): Promise<void> => {
  // Get the list of keys from S3
  const keyList: string[] = await listKeys();

  // Which one are we on?
  let lastRunNum: number | undefined;
  try {
    const lastRun: string = await readFromS3(process.env.S3_CONFIG_BUCKET!, LASTRUNFILE);

    if (lastRun === "COMPLETE") {
      // We are done - no need to process anything
      logger.info("Done with backfill", { lastRun });
    } else {
      lastRunNum = parseInt(lastRun, 10);
    }
  } catch(e) {
    logger.error((e as any)?.message, "Error getting last run number");
  }

  // Process this key if it isn't beyond the end of the date range
  if (lastRunNum !== undefined) {
    logger.info("Getting key", { lastRunNum, key: keyList[lastRunNum] });
    await processKey(process.env.S3_BUCKET!, keyList[lastRunNum]);

    // Now save to S3
    const client: S3Client = new S3Client({
      region: "us-west-2",
    });

    const command = new PutObjectCommand({
      Body: lastRunNum < keyList.length - 1 ? (lastRunNum + 1).toString() : "COMPLETE",
      Bucket: process.env.S3_CONFIG_BUCKET,
      Key: LASTRUNFILE,
    });
    await client.send(command);
  }
};

exports.handler = async (event: any, context: any) => {
  logger.info("received event", { event });

  if (event.runBackfill) {
    await runBackfill();
  } else if (event.Records?.length && event.Records[0].eventSource === "aws:s3") {
    await processKey(event.Records[0].s3.bucket.name, event.Records[0].s3.object.key);
  } else {
    logger.info("Event not recognized", { event });
  }
};
