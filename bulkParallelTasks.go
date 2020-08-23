package main

import (
	"errors"
	"fmt"
	wf "go-pubsub-workflow/pubSubWorkflow"
	"math/rand"
	"time"
)

func main() {

	wf := wf.New("bulk_parallel")

	wf.Subscribe("start", start)
	wf.Subscribe("compute", compute)
	wf.Subscribe("finalize", finalize)

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

	wf.Publish("start", "")

	select {}
}

func start(data string, events []wf.Event) ([]wf.Action, []wf.EventPublish, error) {
	fmt.Printf("in start")
	return wf.PublishNext("compute", "task_1", "compute", "task_2", "compute", "task_3", "compute", "task_4", "compute", "task_5"),
		[]wf.EventPublish{
			wf.PublishOnEvents("finalize", "some data", "task_1_done", "task_2_done", "task_3_done", "task_4_done", "task_5_done"),
		},
		nil
}

func compute(taskName string, events []wf.Event) ([]wf.Action, []wf.EventPublish, error) {
	s1 := rand.NewSource(time.Now().UnixNano())
	r1 := rand.New(s1)

	if r1.Intn(2) == 0 {
		fmt.Printf("compute task %s returns error, will rerun\n", taskName)
		return nil, nil, errors.New("error")
	}

	randomNum := fmt.Sprintf("%d", r1.Intn(1000))
	fmt.Printf("compute: task name: %s, generated num: %s\n", taskName, randomNum)
	return wf.EmitEvents(taskName+"_done", randomNum), nil, nil
}

func finalize(data string, events []wf.Event) ([]wf.Action, []wf.EventPublish, error) {
	fmt.Printf("in foo3: data = %s\n", data)
	for _, ev := range events {
		fmt.Printf("received num %s from event %s\n", ev.Data, ev.Name)
	}
	return nil, nil, nil
}
