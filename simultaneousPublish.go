package main

import (
	"encoding/json"
	"fmt"
	wf "go-pubsub-workflow/pubSubWorkflow"
)

type UserRegistration struct {
	Name           string
	Email          string
	CreditCardInfo string
	Facebook       string
}

func (m UserRegistration) toJson() string {
	jsonStr, _ := json.Marshal(m)
	return string(jsonStr)
}

func (m *UserRegistration) fromJson(data string) {
	json.Unmarshal([]byte(data), m)
}

func main() {

	var mywf wf.PubSubWorkflow

	for i := 0; i < 20; i++ {
		mywf = runUserService()
		defer mywf.Close()
	}

	for i := 0; i < 2; i++ {
		docWf := runDocumentGenService()
		defer docWf.Close()
	}

	go mywf.Publish("registerUser", UserRegistration{"user 1", "user1@mail.com", "credit card 1", "/user1.1111"}.toJson())
	go mywf.Publish("registerUser", UserRegistration{"user 2", "user2@mail.com", "credit card 2", "/user2.222"}.toJson())
	go mywf.Publish("registerUser", UserRegistration{"user 3", "user3@mail.com", "credit card 3", "/user2.3333333"}.toJson())
	go mywf.Publish("registerUser", UserRegistration{"user 4", "user4@mail.com", "credit card 4", "/user4.44444"}.toJson())
	go mywf.Publish("registerUser", UserRegistration{"user 5", "user5@mail.com", "credit card 5", "/user5.55"}.toJson())

	select {}
}

func runUserService() wf.PubSubWorkflow {
	wf := wf.New("userService")
	wf.Subscribe("registerUser", registerUser)
	wf.Subscribe("verifyEmail", verifyEmail)
	wf.Subscribe("verifyCard", verifyCard)
	wf.Subscribe("finalize", finalize)

	err := wf.Connect("amqp://guest:guest@localhost:5672", "127.0.0.1:6379")
	if err != nil {
		panic(err)
	}

	go func() {
		err = wf.StartListening()
		if err != nil {
			panic(err)
		}
	}()
	return wf
}

func runDocumentGenService() wf.PubSubWorkflow {
	wf := wf.New("documentGenService")
	wf.Subscribe("generateDoc", generateDoc)
	err := wf.Connect("amqp://guest:guest@localhost:5672", "127.0.0.1:6379")
	if err != nil {
		panic(err)
	}

	go func() {
		err = wf.StartListening()
		if err != nil {
			panic(err)
		}
	}()
	return wf
}

func registerUser(data string, events []wf.Event) ([]wf.Action, []wf.PublishTrigger, error) {
	var user UserRegistration
	user.fromJson(data)
	return wf.PublishNext("verifyEmail", user.Email, "verifyCard", user.CreditCardInfo, "documentGenService.generateDoc", user.Name),
		[]wf.PublishTrigger{
			wf.PublishOnEvents("finalize", data, "emailVerified", "cardVerified", "documentGenerated"),
		},
		nil
}

func verifyEmail(data string, events []wf.Event) ([]wf.Action, []wf.PublishTrigger, error) {
	fmt.Println("verifyEmail:", data)
	return wf.EmitEvents("emailVerified", data+" verified"), nil, nil
}

func verifyCard(data string, events []wf.Event) ([]wf.Action, []wf.PublishTrigger, error) {
	fmt.Println("verifyCard:", data)
	return wf.EmitEvents("cardVerified", data+" verified"), nil, nil
}

func finalize(data string, events []wf.Event) ([]wf.Action, []wf.PublishTrigger, error) {
	fmt.Printf("#################################\nFINALIZED: %v\nRECEIVED EVENTS: %v\n", data, events)
	return nil, nil, nil
}

func generateDoc(data string, events []wf.Event) ([]wf.Action, []wf.PublishTrigger, error) {
	fmt.Println("generateDoc:", data)
	return wf.EmitEvents("documentGenerated", "document for user "+data), nil, nil
}
