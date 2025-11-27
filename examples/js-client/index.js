import grpc from "@grpc/grpc-js";
import protoLoader from "@grpc/proto-loader";

const pkgDef = protoLoader.loadSync("../../proto/plugin.proto");
const plugin = grpc.loadPackageDefinition(pkgDef).plugin;

const client = new plugin.PluginService(
  "127.0.0.1:50051",
  grpc.credentials.createInsecure()
);

function connect() {
  return new Promise((resolve) => {
    const stream = client.Session();

    console.log("Connected, listening for events...");

    stream.on("data", (msg) => {
      if (msg.insert) {
        console.log(
          `INSERT ${msg.insert.schema}.${msg.insert.table}: ${msg.insert.jsonPayload}`
        );
        stream.write({ ack: { pgLsn: msg.insert.pgLsn } });
      } else if (msg.update) {
        console.log(
          `UPDATE ${msg.update.schema}.${msg.update.table}: ${msg.update.jsonPayload}`
        );
        stream.write({ ack: { pgLsn: msg.update.pgLsn } });
      } else if (msg.delete) {
        console.log(
          `DELETE ${msg.delete.schema}.${msg.delete.table}: ${msg.delete.jsonPayload}`
        );
        stream.write({ ack: { pgLsn: msg.delete.pgLsn } });
      } else if (msg.truncate) {
        console.log(`TRUNCATE ${msg.truncate.schema}.${msg.truncate.table}`);
        stream.write({ ack: { pgLsn: msg.truncate.pgLsn } });
      }
    });

    const cleanup = () => {
      try {
        stream.cancel();
      } catch (_) {}
      resolve(); // resolves the promise so the loop continues
    };

    stream.on("error", (err) => {
      console.error("Stream error:", err.message);

      cleanup();
    });

    stream.on("end", () => {
      console.log("Stream ended");

      cleanup();
    });
  });
}

function wait(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

(async () => {
  while (true) {
    await connect();
    console.log("Disconnected — reconnecting in 2s...");
    await wait(2000);
  }
})();
