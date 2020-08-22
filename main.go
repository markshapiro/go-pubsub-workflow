package main

import (
	"fmt"
	"go-pubsub-workflow/pubSubWorkflow"
)

func main() {

	wf := pubSubWorkflow.New("my_queue_1")

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

	wf.Publish("foo1", "111")

	// procId, _ := wf.CallFnOnNewProcess(pubSubWorkflow.FnCall{FnId: "foo4", Data: "111"})
	// time.Sleep(time.Second * 5)
	// wf.StopProcess(procId)

	select {}
}

//var count = 0

func foo1(data string, events []pubSubWorkflow.Event) ([]pubSubWorkflow.Publish, []pubSubWorkflow.EventTrigger, error) {
	fmt.Println("####### foo1:", data)

	// time.Sleep(time.Second * 1)
	// count++
	// if count < 3 {
	// 	return nil, nil, errors.New("test")
	// }

	// return pubSubWorkflow.PublishNext("foo2", "1122", "foo2", "111444", "foo3", "333"), nil, nil

	return pubSubWorkflow.PublishNext("foo2", "1122"), []pubSubWorkflow.EventTrigger{
		pubSubWorkflow.EventTrigger{[]string{"AA", "BB"}, "foo3", "data1"},
		pubSubWorkflow.EventTrigger{[]string{"BB", "CC"}, "foo3", "data1"},
	}, nil
}

func foo2(data string, events []pubSubWorkflow.Event) ([]pubSubWorkflow.Publish, []pubSubWorkflow.EventTrigger, error) {
	fmt.Println("####### foo2:", data)
	return nil, nil, nil
}

func foo3(data string, events []pubSubWorkflow.Event) ([]pubSubWorkflow.Publish, []pubSubWorkflow.EventTrigger, error) {
	fmt.Println("####### foo3:", data)
	return nil, nil, nil
}
