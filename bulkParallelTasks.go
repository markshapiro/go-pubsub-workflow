package main

import (
	"fmt"
	wf "go-pubsub-workflow/pubSubWorkflow"
	"math/rand"
)

func main() {

	wf := wf.New("my_queue_1")

	wf.Subscribe("foo1", foo1)
	wf.Subscribe("foo2", foo2)
	wf.Subscribe("foo3", foo3)

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

	select {}
}

func foo1(data string, events []wf.Event) ([]wf.Action, []wf.EventListener, error) {
	fmt.Printf("in foo1: data = %s\n", data)
	return wf.PublishNext("foo2", "task_1", "foo2", "task_2", "foo2", "task_3", "foo2", "task_4", "foo2", "task_5"),
		[]wf.EventListener{
			wf.NewEventListener("foo3", "some data", "task_1_done", "task_2_done", "task_3_done", "task_4_done", "task_5_done"),
		},
		nil
}

func foo2(taskName string, events []wf.Event) ([]wf.Action, []wf.EventListener, error) {
	randomNum := fmt.Sprintf("%d", rand.Intn(100))
	fmt.Printf("foo2: task name: %s, generated num: %s\n", taskName, randomNum)
	return wf.EmitEvents(taskName+"_done", randomNum), nil, nil
}

func foo3(data string, events []wf.Event) ([]wf.Action, []wf.EventListener, error) {
	fmt.Printf("in foo3: data = %s\n", data)
	for _, ev := range events {
		fmt.Printf("received num %s from event %s\n", ev.Data, ev.Name)
	}
	return nil, nil, nil
}
