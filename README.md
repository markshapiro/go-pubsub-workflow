# go-pubsub-workflow

a durable distributed pubsub with option to construct dynamic workflows (condition based process forks & joins)

each task within workflow is ran by republishing a message to execute next tasks once the first task (also ran by publish) finishes.
<br>this ensures that each task within workflow is reprocessed if service crashes, and the next will run when the first one completes.

the library leverages rabbitmq for its durability and redis for its distributed key value store to give means to publish subsequent tasks exactly once, by preventing duplicate task execution calls when the previous task is requeued & reprocessed and publishes subsequent tasks twice.

the library also introduces task triggering events, to implement joins of parallel processes, each parallel task can then emit an event once it completes, and together with events emitted by other parallel tasks triggers a subsequent (joined) task that runs once all parallel processes complete.

### how it works

each task call and its subsequent task calls are assigned a unique identifier that is used to determine if it has already been scheduled.
the task ids will always be assigned with same values they were before the reprocessing, this way when a task crashes in the middle scheduling of next tasks, the next requeue will reschedule the rest without scheduling the ones that were already called.

### setup

`go get -u github.com/markshapiro/go-pubsub-workflow`

```go
import (
	wf "github.com/markshapiro/go-pubsub-workflow"
)
```

you will next need redis and amqp running, it is recommended that appendonly flag is set for redis for maximum durability.

### how to use

create instance & subscribe to tasks
```go
    wf := wf.New("queue_name_1")

    wf.Subscribe("task1", foo1)
    wf.Subscribe("task2", foo2)
    ...
    wf.Subscribe("taskN", fooN)
```

define methods
``` go
    func foo1(data string, events []wf.Event) ([]wf.Action, []wf.PublishTrigger, error) {
        return wf.PublishNext("foo2", "1122", "foo3", "2233"), nil, nil
    }
```

connect & listen to pusblishes
```go
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
```

publish to initiate workflow
```go
    wf.Publish("task1", "some data")
```

### known bugs / improvements
- introduce usage of redis pipelines
- global events (currently tasks are only triggered by event emitters that trace back to the same publish handler calls, in other words within the same process "session")
<br/>this is very usefull when dealing with external events (such as for example intercepting delivered package events in purchase workflow)
- cleanup of space/unused data
- currently there is a possibility that a task will overlap with subsequent/next tasks, this can happen when the handler call is requeued after it already published messages to run subsequent tasks.
- possiblity to use redis for pubsub and removing the need for amqp.
- deferred/delayed calls, even though you could just use `time.Sleep()`
- any new ideas/bugs/pull requests are welcome