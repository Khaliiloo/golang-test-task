package message

type Message struct {
	Sender   string `json:"sender" form:"sender"`
	Receiver string `json:"receiver" form:"receiver"`
	Message  string `json:"message" form:"message"`
}
