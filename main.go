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
	wf.Subscribe("foo4", foo4)
	wf.Subscribe("foo5", foo5)

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

func foo1(data string, events []pubSubWorkflow.Event) ([]pubSubWorkflow.Action, []pubSubWorkflow.EventTrigger, error) {
	fmt.Println("####### foo1:", data)

	// time.Sleep(time.Second * 1)
	// count++
	// if count < 3 {
	// 	return nil, nil, errors.New("test")
	// }

	// return pubSubWorkflow.PublishNext("foo2", "1122", "foo2", "111444", "foo3", "333"), nil, nil

	return pubSubWorkflow.PublishNext("foo2", "1122", "foo3", "2233"),
		[]pubSubWorkflow.EventTrigger{
			pubSubWorkflow.NewEventTrigger("foo4", "data from foo1", "AA", "BB"),
			//pubSubWorkflow.NewEventTrigger("foo4", "data from foo1", "BB", "CC"),
			//pubSubWorkflow.NewEventTrigger("foo5", "data from foo1", "BB", "CC"),
		},
		nil
}

func foo2(data string, events []pubSubWorkflow.Event) ([]pubSubWorkflow.Action, []pubSubWorkflow.EventTrigger, error) {
	fmt.Println("####### foo2:", data)
	return pubSubWorkflow.EmitEvents("AA", "data_AA"), nil, nil
}

func foo3(data string, events []pubSubWorkflow.Event) ([]pubSubWorkflow.Action, []pubSubWorkflow.EventTrigger, error) {
	fmt.Println("####### foo3:", data, events)
	return pubSubWorkflow.EmitEvents("BB", "data_BB", "CC", "data_CC"), nil, nil
}

func foo4(data string, events []pubSubWorkflow.Event) ([]pubSubWorkflow.Action, []pubSubWorkflow.EventTrigger, error) {
	fmt.Println("####### foo4:", data, events)
	return nil, nil, nil
}

func foo5(data string, events []pubSubWorkflow.Event) ([]pubSubWorkflow.Action, []pubSubWorkflow.EventTrigger, error) {
	fmt.Println("####### foo5:", data, events)
	return nil, nil, nil
}
