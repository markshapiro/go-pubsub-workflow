package pubSubWorkflow

import (
	"errors"
	"fmt"
	"strings"

	amqpWrapper "github.com/markshapiro/go-pubsub-workflow/amqp"

	redisWrapper "github.com/markshapiro/go-pubsub-workflow/redis"

	"github.com/go-redis/redis"
	"github.com/streadway/amqp"
)

var (
	CALL_ALREADY_CREATED = errors.New("CALL_ALREADY_CREATED")
	HANDLER_NOT_FOUND    = errors.New("HANDLER_NOT_FOUND")
)

func New(queueId string) PubSubWorkflow {
	amqpConn := amqpWrapper.GetInstance()
	var handlerInfos = []handlerInfo{}
	pubSubWorkflow := pubSubWorkflow{
		queueId,
		amqpConn,
		&redis.Client{},
		&handlerInfos,
	}
	return pubSubWorkflow
}

func (wf pubSubWorkflow) Connect(amqpUrl, redisUrl string) error {
	err := wf.amqpConn.Connect(amqpUrl)
	if err != nil {
		return err
	}
	*wf.redisConn = *redisWrapper.GetInstance(redisUrl)

	conf := wf.redisConn.ConfigGet("appendonly")

	if conf.Err() != nil {
		return conf.Err()
	}

	if conf.String() != "config get appendonly: [appendonly yes]" {
		fmt.Println(fmt.Errorf("Warning: redis appendonly is not set, it is recommended to set 'appendonly yes' for maximum durability").Error())
	}

	if err != nil {
		return err
	}

	return nil
}

func (wf pubSubWorkflow) StartListening() error {
	err := wf.amqpConn.CreateQueue(wf.queueId)
	if err != nil {
		return err
	}
	amqpMsgs, err := wf.amqpConn.GetChannel().Consume(
		wf.queueId, // queue
		"",         // consumer
		false,      // auto-ack
		false,      // exclusive
		false,      // no-local
		false,      // no-wait
		nil,        // args
	)
	if err != nil {
		return err
	}

	for amqpMsg := range amqpMsgs {

		var msg message

		err := msg.UnmarshalBinary(amqpMsg.Body)
		if err != nil {
			return err
		}

		err = wf.processMsg(msg)
		if err != nil {
			if err == CALL_ALREADY_CREATED || err == HANDLER_NOT_FOUND {
				if err == HANDLER_NOT_FOUND {
					fmt.Println(fmt.Errorf("Error: method not found %s destined for queue %s", msg.Subject, wf.queueId).Error())
				}
				amqpMsg.Reject(false)
			} else {
				amqpMsg.Nack(false, true)
			}
			continue
		}

		amqpMsg.Ack(false)
	}

	return nil
}

func (wf pubSubWorkflow) processMsg(msg message) error {
	var handlerFn handlerFunc
	for _, handler := range *wf.handlers {
		if msg.Subject == handler.subject {
			handlerFn = handler.handlerFn
		}
	}
	if handlerFn == nil {
		return HANDLER_NOT_FOUND
	}

	/*
		msg.MessageId will be used to claim the sole right to be pro(and repro-)cessed, if two messages with same CallId
		enter this section, the one who sets messageId first will proceed and the other one will be discarded,
		when a message is requeued, it will check if the value it set before matches with its msg.messageId as a requirement to proceed.
		this way we ensure that we don't run same task twice when the handler that published the task before was requeued itsef.
	*/
	cmd := wf.redisConn.HSetNX(fmt.Sprintf("call.%s.data", msg.CallId), "verificationId", msg.MessageId)
	if cmd.Err() != nil && cmd.Err() != redis.Nil {
		return cmd.Err()
	}
	if cmd.Val() == false {
		// we enter here if handler was called twice, due to its own or its callers requeue
		getCmd := wf.redisConn.HGet(fmt.Sprintf("call.%s.data", msg.CallId), "verificationId")
		if getCmd.Err() != nil && cmd.Err() != redis.Nil {
			return getCmd.Err()
		}
		firstMessageId, err := getCmd.Int64()
		if err != nil {
			return err
		}
		// if values match, means the message itself was requeued (due to loss of connection or crash for example)
		if firstMessageId != msg.MessageId {
			return CALL_ALREADY_CREATED
		}
	}

	nextActions, nextPublishTriggers, err := handlerFn(msg.Args.Data, msg.Args.Events)
	if err != nil {
		return err
	}

	for ind := range nextPublishTriggers {
		/*
			all publish triggers should be assigned unique id, to be used as CallId (of message of triggered publish),
			this removes possibility of task call duplication when there's race condition between event emitters,
			since storing emitted events and then checking for next publishes to trigger is not atomic.
		*/
		nextPublishTriggers[ind].PublishTriggerId, err = wf.getUniqueNum()
		if nextPublishTriggers[ind].QueueId == "" {
			nextPublishTriggers[ind].QueueId = wf.queueId
		}
		if err != nil {
			return err
		}
	}

	result := storedResult{nextActions, nextPublishTriggers}

	storeCmd := wf.redisConn.HSetNX(fmt.Sprintf("call.%s.data", msg.CallId), "result", result)
	if storeCmd.Err() != nil && cmd.Err() != redis.Nil {
		return storeCmd.Err()
	}

	/*
		in case message is requeued and previous response of handler (actions/event triggers) was already calculated & stored,
		we ignore the newer response and take the first stored, because it could have had requeued in the middle of publishing
		of next messages, and since constuction of CallIds depends solely on handler response (actions), we want to continue publishing from
		where the handler stopped before it was requeued, to prevent inconsistent publishing.
	*/
	if cmd.Val() == false {
		getResultCmd := wf.redisConn.HGet(fmt.Sprintf("call.%s.data", msg.CallId), "result")
		if getResultCmd.Err() != nil && getResultCmd.Err() != redis.Nil {
			return getResultCmd.Err()
		}

		var prevStoredResult storedResult
		err = getResultCmd.Scan(&prevStoredResult)
		if err != nil {
			return err
		}

		nextActions = prevStoredResult.Actions
		nextPublishTriggers = prevStoredResult.PublishTriggers
	}

	return wf.processCallsAndStoreTriggers(msg, nextActions, nextPublishTriggers)
}

func (wf pubSubWorkflow) processCallsAndStoreTriggers(msg message, nextActions []Action, nextPublishTriggers []PublishTrigger) error {

	for _, publishTrigger := range nextPublishTriggers {
		cmd := wf.redisConn.SAdd(fmt.Sprintf("session.%d.publishTriggers", msg.SessionId), publishTrigger)
		if cmd.Err() != nil && cmd.Err() == redis.Nil {
			return cmd.Err()
		}
	}

	err := wf.publishCalls(msg, nextActions)
	if err != nil {
		return err
	}

	err = wf.emitEvents(msg, nextActions)
	if err != nil {
		return err
	}

	return nil
}

func (wf pubSubWorkflow) publishCalls(msg message, nextActions []Action) error {
	subjectCounts := make(map[string]int)
	for _, action := range nextActions {
		if action.Type == Publish {
			// msg.CallId should be calculated deterministically and based on returned nextActions,
			// so that we could discard duplicate calls by publishing a second message with same CallId but different messageId
			// (see beginning of processMsg() where each message claims the right to be proccessed by being the first one to set MessageId)
			nextCallId := fmt.Sprintf("%d.publish.%s.%d", msg.MessageId, action.Subject, subjectCounts[action.Subject])
			nextMessageId, err := wf.getUniqueNum()
			if err != nil {
				return err
			}
			nextMsg := message{nextCallId, nextMessageId, msg.SessionId, action.Subject, Args{action.Data, nil}}
			err = wf.publish(nextMsg, action.QueueId)
			if err != nil {
				return err
			}
			subjectCounts[action.Subject]++
		}
	}
	return nil
}

func (wf pubSubWorkflow) emitEvents(msg message, nextActions []Action) error {
	err := wf.storeEvents(msg, nextActions)
	if err != nil {
		return err
	}

	membersCmd := wf.redisConn.SMembers(fmt.Sprintf("session.%d.publishTriggers", msg.SessionId))
	if membersCmd.Err() != nil && membersCmd.Err() == redis.Nil {
		return membersCmd.Err()
	}
	var allPublishesTriggers []PublishTrigger

	err = membersCmd.ScanSlice(&allPublishesTriggers)
	if err != nil {
		return err
	}

	for _, publishTrigger := range allPublishesTriggers {
		var eventArgs []Event
		for _, triggeringEvent := range publishTrigger.Events {
			for _, action := range nextActions {
				if action.Type == EmitEvent && action.Event == triggeringEvent {
					eventArgs = append(eventArgs, Event{action.Event, action.Data})
				}
			}
		}
		if len(eventArgs) > 0 {
			var eventsNotInActions = []string{}
			for _, event := range publishTrigger.Events {
				exists := false
				for _, arg := range eventArgs {
					if arg.Name == event {
						exists = true
					}
				}
				if !exists {
					eventsNotInActions = append(eventsNotInActions, event)
				}
			}

			if len(eventsNotInActions) > 0 {
				cmd := wf.redisConn.Eval("return redis.call('HMGET', "+fmt.Sprintf("'session.%d.events'", msg.SessionId)+", '"+strings.Join(eventsNotInActions, "', '")+"')", []string{})
				if cmd.Err() != nil && cmd.Err() == redis.Nil {
					return cmd.Err()
				}
				values := cmd.Val().([]interface{})
				for valInd, val := range values {
					if val != nil {
						eventArgs = append(eventArgs, Event{eventsNotInActions[valInd], val.(string)})
					}
				}
			}

			if len(eventArgs) == len(publishTrigger.Events) {
				nextMessageId, err := wf.getUniqueNum()
				if err != nil {
					return err
				}

				// as mentioned above, msg.CallId should be calculated deterministically
				nextCallId := fmt.Sprintf("event.%d", publishTrigger.PublishTriggerId)
				nextMsg := message{nextCallId, nextMessageId, msg.SessionId, publishTrigger.Subject, Args{publishTrigger.Data, eventArgs}}
				err = wf.publish(nextMsg, publishTrigger.QueueId)
				if err != nil {
					return err
				}
			}
		}
	}

	return nil
}

func (wf pubSubWorkflow) storeEvents(msg message, nextActions []Action) error {
	var addEvents = []string{}
	for _, action := range nextActions {
		if action.Type == EmitEvent {
			addEvents = append(addEvents, "'"+action.Event+"'", "'"+action.Data+"'")
		}
	}
	if len(addEvents) > 0 {
		cmd := wf.redisConn.Eval("return redis.call('HSET', "+fmt.Sprintf("'session.%d.events', ", msg.SessionId)+strings.Join(addEvents, ", ")+")", []string{})
		if cmd.Err() != nil && cmd.Err() == redis.Nil {
			return cmd.Err()
		}
	}
	return nil
}

func (wf pubSubWorkflow) Publish(subject, data string, uniqueCallId ...string) error {
	var callId string

	if len(uniqueCallId) >= 1 {
		callId = uniqueCallId[0]
	} else {
		callIdNo, err := wf.getUniqueNum()
		if err != nil {
			return err
		}
		callId = fmt.Sprintf("%d", callIdNo)
	}

	messageId, err := wf.getUniqueNum()
	if err != nil {
		return err
	}

	sessionId, err := wf.getUniqueNum()
	if err != nil {
		return err
	}

	err = wf.publish(message{callId, messageId, sessionId, subject, Args{data, nil}}, "")
	if err != nil {
		return err
	}

	return nil
}

func (wf pubSubWorkflow) Subscribe(subject string, handler handlerFunc) error {
	if strings.Contains(subject, ".") {
		return errors.New("dot '.' symbol is reserved")
	}
	*wf.handlers = append(*wf.handlers, handlerInfo{subject, handler})
	return nil
}

func (wf pubSubWorkflow) getUniqueNum() (int64, error) {
	cmd := wf.redisConn.Incr("pubSubWorkflow.uniqueNum")
	if cmd.Err() != nil {
		return 0, cmd.Err()
	}
	return cmd.Result()
}

func (wf pubSubWorkflow) publish(msg message, queueId string) error {
	var err error
	msg.MessageId, err = wf.getUniqueNum()
	if err != nil {
		return err
	}

	bytes, err := msg.MarshalBinary()
	if err != nil {
		return err
	}

	if queueId == "" {
		queueId = wf.queueId
	}

	return wf.amqpConn.GetChannel().Publish(
		"",      // exchange
		queueId, // routing key
		false,   // mandatory
		false,   // immediate
		amqp.Publishing{
			ContentType: "application/json",
			Body:        bytes,
		})
}

func (wf pubSubWorkflow) Close() error {
	err := wf.amqpConn.GetChannel().Close()
	if err != nil {
		return err
	}
	err = wf.amqpConn.Close()
	if err != nil {
		return err
	}
	err = wf.redisConn.Close()
	if err != nil {
		return err
	}
	return nil
}

func (wf pubSubWorkflow) Reset() error {
	_, err := wf.amqpConn.GetChannel().QueuePurge(wf.queueId, true)
	amqpMsgs, err := wf.amqpConn.GetChannel().Consume(
		wf.queueId, // queue
		"",         // consumer
		true,       // auto-ack
		false,      // exclusive
		false,      // no-local
		true,       // no-wait
		nil,        // args
	)
	if err != nil {
		return err
	}
	for _ = range amqpMsgs {
	}
	return nil
}

func PublishNext(data ...string) []Action {
	var result []Action
	for ind := range data {
		if ind%2 == 0 {
			queueId, subject := getQueueAndSubject(data[ind])
			result = append(result, Action{Publish, queueId, "", subject, data[ind+1]})
		}
	}
	return result
}

func PublishOnEvents(dest string, data string, events ...string) PublishTrigger {
	queueId, subject := getQueueAndSubject(dest)
	return PublishTrigger{
		Events:  events,
		QueueId: queueId,
		Subject: subject,
		Data:    data,
	}
}

func getQueueAndSubject(dest string) (string, string) {
	dotSymbol := strings.Index(dest, ".")
	if dotSymbol >= 0 {
		return dest[:dotSymbol], dest[dotSymbol+1:]
	}
	return "", dest
}

func EmitEvents(data ...string) []Action {
	var result []Action
	for ind := range data {
		if ind%2 == 0 {
			result = append(result, Action{EmitEvent, "", data[ind], "", data[ind+1]})
		}
	}
	return result
}
