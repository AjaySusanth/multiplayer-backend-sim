package session
import (
	"encoding/json"
	"log/slog"
	"time"
	"github.com/gorilla/websocket"
)
const (
	writeWait      = 10 * time.Second
	pongWait       = 60 * time.Second
	pingPeriod     = (pongWait * 9) / 10
	maxMessageSize = 512
)

func (c *Client) RunReadPump(conn *websocket.Conn, room *Room, logger *slog.Logger) {
	defer func ()  {
		room.Unregister <- c
		conn.Close()
	}()

	conn.SetReadLimit(maxMessageSize)
	conn.SetReadDeadline(time.Now().Add(pongWait))
	conn.SetPongHandler(func(string) error { conn.SetReadDeadline(time.Now().Add(pongWait)); return nil })

	for {
		_,message,err := conn.ReadMessage()
		if err!= nil {
			if websocket.IsUnexpectedCloseError(err,websocket.CloseGoingAway,websocket.CloseAbnormalClosure) {
				logger.Error("websocket closed unexpectedly", "error", err)
			}
			break
		}
		var cmd struct {
			Action string `json:"action"`
		}
		if err := json.Unmarshal(message, &cmd); err == nil {
			// Send the keystroke to the Room's traffic cop!
			room.Commands <- PlayerCommand{
				PlayerID: c.PlayerID,
				Action:   cmd.Action,
			}
		}
	}
}


func (c *Client) RunWritePump(conn *websocket.Conn) {
	ticker := time.NewTicker(pingPeriod)
	defer func() {
		ticker.Stop()
		conn.Close()
	}()

	for {
		select{
		case message,ok := <-c.Send:
			conn.SetWriteDeadline(time.Now().Add(writeWait))
			if !ok {
				conn.WriteMessage(websocket.CloseMessage,[]byte{})
				return
			}

			w,err := conn.NextWriter(websocket.TextMessage)
			if err!=nil {
				return
			}
			w.Write(message)

			if err:= w.Close(); err!= nil {
				return
			}

		case <-ticker.C:
			conn.SetWriteDeadline(time.Now().Add(writeWait))
			if err:=conn.WriteMessage(websocket.PingMessage,nil);err!=nil {
				return
			}
		}
	}
}