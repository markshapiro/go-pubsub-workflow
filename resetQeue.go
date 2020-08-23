package main

import (
	"fmt"
	wf "go-pubsub-workflow/pubSubWorkflow"
	"time"
)

func main() {

	wf := wf.New("test_queue")

	wf.Subscribe("foo1", foo1)

	err := wf.Connect("amqp://guest:guest@localhost:5672", "127.0.0.1:6379")
	if err != nil {
		panic(err)
	}
	defer wf.Close()

	go func() {
		err = wf.StartListening()
		if err != nil {
			panic(err)
		}
	}()

	wf.Publish("foo1", "")
	wf.Publish("foo1", "")
	wf.Publish("foo1", "")
	wf.Publish("foo1", "")
	wf.Publish("foo1", "")
	time.Sleep(time.Second * 1)

	wf.Reset()

	select {}
}
func foo1(data string, events []wf.Event) ([]wf.Action, []wf.EventPublish, error) {
	fmt.Println(" in foo1")
	time.Sleep(time.Microsecond * 100)
	return wf.PublishNext("foo1", data), nil, nil
}
