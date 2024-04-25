import Fastify from "fastify";

import { InfluxDB, Point, flux } from "@influxdata/influxdb-client";
import { env } from "./env";

const fastify = Fastify();

const client = new InfluxDB({
  url: env.INFLUXDB_URL,
  token: env.INFLUXDB_TOKEN,
});

fastify.get("/", async () => {
  const queryApi = client.getQueryApi(env.INFLUXDB_ORG);
  const measurement = "temperature";
  const query = flux`from(bucket: ${env.INFLUXDB_BUCKET})
    |> range(start: 0)
    |> filter(fn: (r)=> r._measurement == ${measurement})`;

  const response: any = [];
  const data = await new Promise((resolve, reject) => {
    queryApi.queryRows(query, {
      next(row, tableMeta) {
        const output = tableMeta.toObject(row);
        response.push(output);
      },
      error: reject,
      complete() {
        resolve(response.map(mapPoints));
      },
    });
  });

  return {
    query: query.toString(),
    data,
  };
});

fastify.get("/point", async () => {
  const writeApi = client.getWriteApi(env.INFLUXDB_ORG, env.INFLUXDB_BUCKET, "ns");

  const writeDate = new Date();

  const equip1 = new Point("temperature")
    .floatField("value", Math.floor(Math.random() * 10))
    .timestamp(writeDate);

  const equip2 = new Point("energy")
    .floatField("value", Math.floor(Math.random() * 10))
    .timestamp(writeDate);

  writeApi.writePoints([equip1, equip2]);
  await writeApi.close();

  return { equip1, equip2 };
});

async function bootstrap() {
  await fastify.listen({ port: 3000 });
  console.log("API running at: http://localhost:3000/");
}

try {
  bootstrap();
} catch (err) {
  fastify.log.error(err);
  process.exit(1);
}

function mapPoints(point: Record<string, string>) {
  return {
    name: point._measurement,
    timestamp: point._time,
    value: point._value,
  };
}
