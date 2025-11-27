import * as grpc from "@grpc/grpc-js";

import { PluginServiceClient, ServerMessage, ClientMessage } from "./generated/plugin";


const client = new PluginServiceClient(
  "127.0.0.1:50051",
  grpc.credentials.createInsecure()
);

function connect(): Promise<void> {
  return new Promise((resolve) => {
    const stream = client.session();

    console.log("Connected, listening for events...");

    const ack = (pgLsn: number): ClientMessage => {
      return {
          ack: {
            pgLsn
          }
        }
      }

    stream.on("data", (msg: ServerMessage) => {
      if (msg.insert) {
        console.log(`INSERT ${msg.insert.schema}.${msg.insert.table}: ${msg.insert.jsonPayload}`);
        stream.write(ack(msg.insert.pgLsn));
      } else if (msg.update) {
        console.log(`UPDATE ${msg.update.schema}.${msg.update.table}: ${msg.update.jsonPayload}`);
        stream.write(ack(msg.update.pgLsn));
      } else if (msg.delete) {
        console.log(`DELETE ${msg.delete.schema}.${msg.delete.table}: ${msg.delete.jsonPayload}`);
        stream.write(ack(msg.delete.pgLsn));
      } else if (msg.truncate) {
        console.log(`TRUNCATE ${msg.truncate.schema}.${msg.truncate.table}`);
        stream.write(ack(msg.truncate.pgLsn));
      }
    });

    const cleanup = () => {
      try {
        stream?.cancel();
      } catch (_) {}
      resolve();
    };

    stream.on("error", (err: Error & { code?: number }) => {
      console.error("Stream error:", err.message);
      cleanup();
    });

    stream.on("end", () => {
      console.log("Stream ended");
      cleanup();
    });
  });
}

function wait(ms: number) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

(async () => {
  while (true) {
    await connect();
    console.log("Disconnected — reconnecting in 2s...");
    await wait(2000);
  }
})();
