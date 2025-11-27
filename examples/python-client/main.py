import grpc
import time
import threading

import plugin_pb2
import plugin_pb2_grpc


def connect():
    channel = grpc.insecure_channel("127.0.0.1:50051")
    stub = plugin_pb2_grpc.PluginServiceStub(channel)

    ack_queue = []
    ack_cv = threading.Condition()

    # Generator that yields outgoing ack messages
    def request_stream():
        while True:
            with ack_cv:
                if ack_queue:
                    msg = ack_queue.pop(0)
                    yield msg
                else:
                    ack_cv.wait(timeout=0.1)

    # Open streaming RPC
    stream = stub.Session(request_stream())
    print("Connected, listening for events...")

    try:
        for msg in stream:
            # INSERT
            if msg.HasField("insert"):
                ev = msg.insert
                print(f"INSERT {ev.schema}.{ev.table}: {ev.json_payload}")

                with ack_cv:
                    ack_queue.append(
                        plugin_pb2.ClientMessage(
                            ack=plugin_pb2.ClientAck(pg_lsn=ev.pg_lsn)
                        )
                    )
                    ack_cv.notify()

            # UPDATE
            elif msg.HasField("update"):
                ev = msg.update
                print(f"UPDATE {ev.schema}.{ev.table}: {ev.json_payload}")

                with ack_cv:
                    ack_queue.append(
                        plugin_pb2.ClientMessage(
                            ack=plugin_pb2.ClientAck(pg_lsn=ev.pg_lsn)
                        )
                    )
                    ack_cv.notify()

            # DELETE
            elif msg.HasField("delete"):
                ev = msg.delete
                print(f"DELETE {ev.schema}.{ev.table}: {ev.json_payload}")

                with ack_cv:
                    ack_queue.append(
                        plugin_pb2.ClientMessage(
                            ack=plugin_pb2.ClientAck(pg_lsn=ev.pg_lsn)
                        )
                    )
                    ack_cv.notify()

            # TRUNCATE
            elif msg.HasField("truncate"):
                ev = msg.truncate
                print(f"TRUNCATE {ev.schema}.{ev.table}")

                with ack_cv:
                    ack_queue.append(
                        plugin_pb2.ClientMessage(
                            ack=plugin_pb2.ClientAck(pg_lsn=ev.pg_lsn)
                        )
                    )
                    ack_cv.notify()

    except grpc.RpcError as e:
        print("Stream error:", e.details())

    finally:
        print("Stream closed, reconnecting...")
        channel.close()


def wait(ms):
    time.sleep(ms / 1000)


if __name__ == "__main__":
    while True:
        connect()
        wait(2000)
