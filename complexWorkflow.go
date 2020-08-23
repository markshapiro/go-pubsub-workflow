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

	for i := 0; i < 10; i++ {
		mywf = runUserService()
		defer mywf.Close()
	}

	mywf.Publish("registerUser", UserRegistration{"user 1", "user1@mail.com", "credit card 1", "/user1.123"}.toJson())
	//mywf.Publish("registerUser", UserRegistration{"user 2", "user2@mail.com", "credit card 2", "/user2.456"}.toJson())
	//mywf.Publish("registerUser", UserRegistration{"user 3", "user3@mail.com", "credit card 3", "/user2.456"}.toJson())

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

func randomTimeSleep() {
	// s1 := rand.NewSource(time.Now().UnixNano())
	// r1 := rand.New(s1)
	// time.Sleep(time.Millisecond * time.Duration((r1.Intn(1) + 1)))
}

func registerUser(data string, events []wf.Event) ([]wf.Action, []wf.EventListener, error) {
	fmt.Println("registerUser:", data)
	var user UserRegistration
	user.fromJson(data)
	randomTimeSleep()
	return wf.PublishNext("verifyEmail", user.Email, "verifyCard", user.CreditCardInfo),
		[]wf.EventListener{
			wf.NewEventListener("finalize", data, "emailVerified", "cardVerified"),
		},
		nil
}

func verifyEmail(data string, events []wf.Event) ([]wf.Action, []wf.EventListener, error) {
	fmt.Println("verifyEmail:", data)
	randomTimeSleep()
	return wf.EmitEvents("emailVerified", data+" verified"), nil, nil
}

func verifyCard(data string, events []wf.Event) ([]wf.Action, []wf.EventListener, error) {
	fmt.Println("verifyCard:", data)
	randomTimeSleep()
	return wf.EmitEvents("cardVerified", data+" verified"), nil, nil
}

func finalize(data string, events []wf.Event) ([]wf.Action, []wf.EventListener, error) {
	fmt.Println("###############################")
	fmt.Println("FINALIZED:", data)
	fmt.Println("RECEIVED EVENTS: ", events)
	return nil, nil, nil
}
