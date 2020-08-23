package main

import (
	"fmt"
	wf "go-pubsub-workflow/pubSubWorkflow"
	"time"
)

func main() {

	go func() {
		wf1 := wf.New("microservice1")
		wf1.Subscribe("handler1", microservice1method)
		err := wf1.Connect("amqp://guest:guest@localhost:5672", "127.0.0.1:6379")
		if err != nil {
			panic(err)
		}
		defer wf1.Close()

		go func() {
			err = wf1.StartListening()
			if err != nil {
				panic(err)
			}
		}()

		wf1.Publish("handler1", "first message to microservice 1")

		select {}
	}()

	go func() {
		wf2 := wf.New("microservice2")
		wf2.Subscribe("handler2", microservice2method)
		err := wf2.Connect("amqp://guest:guest@localhost:5672", "127.0.0.1:6379")
		if err != nil {
			panic(err)
		}
		defer wf2.Close()

		go func() {
			err = wf2.StartListening()
			if err != nil {
				panic(err)
			}
		}()

		select {}
	}()

	go func() {
		wf2 := wf.New("microservice2")
		wf2.Subscribe("handler2", microservice2method)
		err := wf2.Connect("amqp://guest:guest@localhost:5672", "127.0.0.1:6379")
		if err != nil {
			panic(err)
		}
		defer wf2.Close()

		go func() {
			err = wf2.StartListening()
			if err != nil {
				panic(err)
			}
		}()

		select {}
	}()

	select {}
}

func microservice1method(data string, events []wf.Event) ([]wf.Action, []wf.EventListener, error) {
	fmt.Println("in microservice 1:", data)
	time.Sleep(time.Second * 1)
	if data == "from micoservice 2" {
		return wf.PublishNext("microservice2.handler2", "last message from micoservice 1"), nil, nil
	}
	return wf.PublishNext("microservice2.handler2", "from micoservice 1"), nil, nil
}

func microservice2method(data string, events []wf.Event) ([]wf.Action, []wf.EventListener, error) {
	fmt.Println("in microservice 2:", data)
	time.Sleep(time.Second * 1)
	if data == "last message from micoservice 1" {
		return nil, nil, nil
	}
	return wf.PublishNext("microservice1.handler1", "from micoservice 2"), nil, nil
}
