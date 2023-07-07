import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { QueryCommand, QueryCommandInput, ScanCommand, ScanCommandInput, DynamoDBDocumentClient } from "@aws-sdk/lib-dynamodb";
import { GetObjectCommand, ListObjectsV2Command, S3Client } from "@aws-sdk/client-s3";
import * as logger from "./logger";

export interface CoinPrice {
  grade: number;
  price: number;
}

export interface CoinIssue {
  name: string; // Eventually will break out year, mintmark, variety
  variety?: string;
  prices: CoinPrice[];
}

export interface CoinSeries {
  name: string;
  issues: CoinIssue[];
  price_as_of: Date;
}

const SERIESFILE = "serieslist.json";

// This function lists all the keys in the S3 bucket
export const listKeys = async(): Promise<string[]> => {
  // Loop thru to read in all keys
  let keyList: string[] = [];

  try {
    let data: any;

    const client: any = new S3Client({
      region: "us-west-2",
    });

    await (async function loop(firstRun, token): Promise<any> {
      const params: any = { Bucket: process.env.S3_BUCKET };
      if (firstRun || token) {
        if (token) {
          params.ContinuationToken = token;
        }

        const command = new ListObjectsV2Command(params);
        data = await client.send(command);
        keyList = keyList.concat(data.Contents.map((d: any) => d.Key));
        if (data.NextContinuationToken) {
          return loop(false, data.NextContinuationToken);
        }
      }
    }(true, null));

    // Ignore the series file
    keyList = keyList.filter((key: string) => key !== SERIESFILE);
  } catch (e) {
    // There's an error - clear the keylist and try again later
    logger.error((e as any)?.message, "Problem reading pricing files");
    keyList = [];
  }

  // Sort the keylist so it's in date order
  keyList.sort((a, b) => {
    const aDate = new Date(a.split("/")[0]);
    const bDate = new Date(b.split("/")[0]);

    if (aDate.getTime() === bDate.getTime()) {
      return a.split("/")[1].localeCompare(b.split("/")[1]);
    } else {
      return aDate.getTime() - bDate.getTime();
    }
  });
  return keyList;
};

const zeroPad = (d: number) => {
  return (`0${d}`).slice(-2);
};

export const formatDate = (d: Date | undefined): string => {
  if (!d) {
    return "";
  }

  return `${d.getFullYear()}-${zeroPad(d.getMonth() + 1)}-${zeroPad(d.getDate())}`;
};

export const readFromS3 = async (bucket: string, key: string): Promise<string> => {
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

// Reads in a price history for the given coin series, issue, and variety
// If no exact match on the variety is found, we will attempt to find a match
// ignoring the variety
export const readPriceHistory = async (series: string, issue: string, variety: string | undefined): Promise<{ price_as_of: Date, prices: CoinPrice[] }[]> => {
  const history: { price_as_of: Date, prices: CoinPrice[] }[] = [];

  // Let's read this in from DynamoDB
  const params: QueryCommandInput = {
    TableName: process.env.DYNAMODB_TABLE,
    KeyConditionExpression: "coin = :coin",
    ExpressionAttributeValues: {
      ":coin": `${series}|${issue}${variety ? `|${variety}` : ""}`,
    },
  };

  // Read items from DynamoDB
  try {
    const client = new DynamoDBClient({});
    const docClient = DynamoDBDocumentClient.from(client);
    const command = new QueryCommand(params);
    let results = await docClient.send(command);

    if (!results.Items?.length) {
      // There was no match - let's see if we can find a key ignoring the variety
      const scanParams: ScanCommandInput = {
        TableName: process.env.DYNAMODB_TABLE,
        FilterExpression: "begins_with(coin, :coin)",
        ExpressionAttributeValues: {
          ":coin": `${series}|${issue}`,
        }
      };

      results = await docClient.send(new ScanCommand(scanParams));
    }

    results.Items?.forEach((item: any) => {
      const prices: CoinPrice[] = [];
      const priceObj = JSON.parse(item.prices);
      Object.keys(priceObj).forEach((key: string) => {
        prices.push({ grade: parseInt(key, 10), price: priceObj[key] });
      });
      history.push({
        price_as_of: new Date(item.price_as_of),
        prices,
      });
    });
  }
  catch (e) {
    logger.info("Error querying info from DynamoDB", { series, issue, variety, params });
    logger.error((e as any)?.message, "Error reading from DynamoDB");
  }

  return history;
};
