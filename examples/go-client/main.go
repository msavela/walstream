package main

import (
	"context"
	"fmt"
	"log"
	"time"

	plugin "pluginclient/generated/plugin"

	"google.golang.org/grpc"
)

func main() {
	for {
		if err := connect(); err != nil {
			log.Printf("Disconnected — reconnecting in 2s... (%v)", err)
			time.Sleep(2 * time.Second)
		}
	}
}

func connect() error {
	ctx := context.Background()

	conn, err := grpc.Dial("127.0.0.1:50051", grpc.WithInsecure())
	if err != nil {
		return fmt.Errorf("failed to dial: %w", err)
	}
	defer conn.Close()

	client := plugin.NewPluginServiceClient(conn)

	stream, err := client.Session(ctx)
	if err != nil {
		return fmt.Errorf("failed to open session: %w", err)
	}

	log.Println("Connected, listening for events...")

	// Reader loop
	for {
		msg, err := stream.Recv()
		if err != nil {
			return err // triggers reconnect
		}

		switch x := msg.Msg.(type) {

		case *plugin.ServerMessage_Insert:
			fmt.Printf("INSERT %s.%s: %s\n",
				x.Insert.Schema, x.Insert.Table, x.Insert.JsonPayload)

			ack := &plugin.ClientMessage{
				Msg: &plugin.ClientMessage_Ack{
					Ack: &plugin.ClientAck{PgLsn: x.Insert.PgLsn},
				},
			}
			stream.Send(ack)

		case *plugin.ServerMessage_Update:
			fmt.Printf("UPDATE %s.%s: %s\n",
				x.Update.Schema, x.Update.Table, x.Update.JsonPayload)

			ack := &plugin.ClientMessage{
				Msg: &plugin.ClientMessage_Ack{
					Ack: &plugin.ClientAck{PgLsn: x.Update.PgLsn},
				},
			}
			stream.Send(ack)

		case *plugin.ServerMessage_Delete:
			fmt.Printf("DELETE %s.%s: %s\n",
				x.Delete.Schema, x.Delete.Table, x.Delete.JsonPayload)

			ack := &plugin.ClientMessage{
				Msg: &plugin.ClientMessage_Ack{
					Ack: &plugin.ClientAck{PgLsn: x.Delete.PgLsn},
				},
			}
			stream.Send(ack)

		case *plugin.ServerMessage_Truncate:
			fmt.Printf("TRUNCATE %s.%s\n",
				x.Truncate.Schema, x.Truncate.Table)

			ack := &plugin.ClientMessage{
				Msg: &plugin.ClientMessage_Ack{
					Ack: &plugin.ClientAck{PgLsn: x.Truncate.PgLsn},
				},
			}
			stream.Send(ack)
		}
	}
}
