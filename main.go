package main

import (
	"fmt"
	wf "go-pubsub-workflow/pubSubWorkflow"
)

func main() {

	wf := wf.New("my_queue_1")

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

	// procId, _ := wf.CallFnOnNewProcess(wf.FnCall{FnId: "foo4", Data: "111"})
	// time.Sleep(time.Second * 5)
	// wf.StopProcess(procId)

	select {}
}

//var count = 0

func foo1(data string, events []wf.Event) ([]wf.Action, []wf.EventListener, error) {
	fmt.Println("####### foo1:", data)

	// time.Sleep(time.Second * 1)
	// count++
	// if count < 3 {
	// 	return nil, nil, errors.New("test")
	// }

	//return wf.PublishNext("foo2", "1122", "foo2", "111444", "foo3", "333"), nil, nil

	return wf.PublishNext("foo2", "1122", "foo3", "2233"),
		[]wf.EventListener{
			wf.NewEventListener("foo4", "data from foo1 #1", "AA", "BB"),
			wf.NewEventListener("foo4", "data from foo1 #2", "BB", "CC", "DD"),
			wf.NewEventListener("foo5", "data from foo1 #3", "BB", "CC"),
		},
		nil
}

func foo2(data string, events []wf.Event) ([]wf.Action, []wf.EventListener, error) {
	fmt.Println("####### foo2:", data)
	return wf.EmitEvents("AA", "data_AA", "DD", "data_DD"), nil, nil
	//return nil, nil, nil
}

func foo3(data string, events []wf.Event) ([]wf.Action, []wf.EventListener, error) {
	fmt.Println("####### foo3:", data, events)
	return wf.EmitEvents("BB", "data_BB", "CC", "data_CC"), nil, nil
	//return nil, nil, nil
}

func foo4(data string, events []wf.Event) ([]wf.Action, []wf.EventListener, error) {
	fmt.Println("####### foo4:", data, events)
	return nil, nil, nil
}

func foo5(data string, events []wf.Event) ([]wf.Action, []wf.EventListener, error) {
	fmt.Println("####### foo5:", data, events)
	return nil, nil, nil
}
