# go-pubsub-workflow

a durable distributed pubsub with option to construct dynamic workflows (condition based process forks & joins)

each task within workflow is ran by republishing messages to execute next tasks once the first task (also ran by publish) finishes.
<br>this ensures that each task within workflow is reprocessed if service crashes, and the next will run when the first one completes.

the library leverages rabbitmq for its durability and redis for its distributed key value store to give means to publish subsequent tasks exactly once, by preventing duplicate task execution when the previous task is requeued & reprocessed and publishes subsequent tasks twice.

the library also introduces task triggering events, to implement joins of parallel processes, each parallel task can then emit an event once it completes, and together with events emitted by other parallel tasks triggers a subsequent (joining) task that runs once all parallel processes complete.

### how it works

each task call and its subsequent task calls are assigned a unique identifier that is used to determine if it has already been scheduled.
the task ids will always be assigned the same values they were before the reprocessing, this way when a task crashes in the middle scheduling of next tasks, the next requeue will reschedule the rest without scheduling the ones that were already called.

### setup

`go get -u github.com/markshapiro/go-pubsub-workflow`

```go
import (
	wf "github.com/markshapiro/go-pubsub-workflow"
)
```

you will next need redis and amqp running, it is recommended that appendonly flag is set for redis for maximum durability.

### how to use

create instance and provide the name of internal queue to be listened to, each microservice should use different queue name.
<br>scheduling tasks to other miscroservices is possible and will be explained later
```go
wfInstance := wf.New("queue_name_1")
```
define each task in workflow by providing name of task and its handler function:
```go
wfInstance.Subscribe("task1", task1)
wfInstance.Subscribe("task2", task2)
...
wfInstance.Subscribe("taskN", taskN)
```
define handler functions from previous step for each task and which consequtive tasks should be ran:
``` go
func task1(data string, events []wf.Event) ([]wf.Action, []wf.PublishTrigger, error) {
    return wf.PublishNext("task2", "some data", "task3", "some data"), nil, nil
}

func task2(data string, events []wf.Event) ([]wf.Action, []wf.PublishTrigger, error) {
    return wf.PublishNext("task4", "some data"), nil, nil
}
```
connect & listen to calls:
```go
err := wfInstance.Connect("amqp://guest:guest@localhost:5672", "127.0.0.1:6379")
if err != nil {
    panic(err)
}
defer wfInstance.Close()
go func() {
    err = wfInstance.StartListening()
    if err != nil {
        panic(err)
    }
}()
```
publish message to start running workflow:
```go
wfInstance.Publish("task1", "some data")
```

### define workflow

let's define a simple process forking:
```go
func task1(data string, events []wf.Event) ([]wf.Action, []wf.PublishTrigger, error) {
    //
    // function body
    //
    return wf.PublishNext(
        "task2", "data passed to first arg of taks 2 handler function",
        "task3", "data passed to first arg of taks 3 handler function",
        "task4", "data passed to first arg of taks 4 handler function"
    ), nil, nil
}
```
this way when handler finishes, 3 consequtive parallel tasks will be scheduled to run exactly once, even if requeue happens.
<br/>the string specified after name of each task will be passed as first `data` argument in their handler.
<br>you can return different tasks to publish in different cases, but if the handler is requeued after the `PublishNext` result was already stored internally, the new result will be ignored for sake of consistency, since some calls could have already been published before the requeue.

let's see now how we can join parallel processes by introducing events.
<br/>by defining event triggered task (returned as 2nd parameter) that will run once all 3 events `event_1`, `event_2` and `event_3` are emitted, more precisely the task will run exactly once when the last of them is emitted:
```go
func someTask(data string, events []wf.Event) ([]wf.Action, []wf.PublishTrigger, error) {
    // function body
    return nil,
    []wf.PublishTrigger{
        wf.PublishOnEvents("joinedTaskName", "some data", "event_1", "event_2", "event_3"),
    },
    nil
}
```
to emit event, return it as first parameter same as with `PublishOnEvents`
```go
func someOtherTask(taskName string, events []wf.Event) ([]wf.Action, []wf.PublishTrigger, error) {
    // function body
    return wf.EmitEvents("event_1", "event data"), nil, nil
}
```
to emit events and also publish next tasks you can do:
```go
func someOtherTask(taskName string, events []wf.Event) ([]wf.Action, []wf.PublishTrigger, error) {
    // function body
    return append(
        wf.EmitEvents("event_1", "event data"),
        wf.PublishNext( ... )...
    ), nil, nil
}
```
once the joining task runs, it will receive string value (under `data`) specified right after task name in `PublishOnEvents`,
and array of events (in our case of length 3) as second argument, each containing name of event and data passed right after that event name in `EmitEvents`:
```go
func someTask(data string, events []wf.Event) ([]wf.Action, []wf.PublishTrigger, error) {
    // function body
    return nil, nil, nil
}
```
in order to run task triggered by events, make sure that the last of the events is emitted in one of the subsequent tasks, it doesnt have to be directly after, but can also be emitted many steps later.

note on events: tasks are only tiggered by events emitted by task calls that can be traced back to same publish handler call as the task call that defined `PublishOnEvents`, meaning emitting event by calling another `wfInstance.Publish` won't trigger task in current one, this is because it would be hard to scale tasks globally between all workflow sessions, for this reason names of events can be static, next `wfInstance.Publish` will ignore all events called in previous publish handler calls.
<br/>Events do transcend microservice queues though, if you define a trigger and then call task of different microservice (in one of subsequent tasks few steps later) that emits triggering event, it will still trigger the task (whose trigger was defined few steps earlier).

in order to call task of other microservice that listens to different queue, provide its queue name before the dot as prefix:
```go
func someTask(data string, events []wf.Event) ([]wf.Action, []wf.PublishTrigger, error) {
    return wf.PublishNext("other_service_queue.task1", "some data"), nil, nil
}
```
this way handler of `task1` of microservice that listens to queue `other_service_queue` will be called:
```go
wfInstance := wf.New("other_service_queue")
wfInstance.Subscribe("task1", task1)
```

### known bugs / improvements
- introduce usage of redis pipelines
- use bson.Marshal/Unmarshal instead of json when storing data to redis/sending content
- global events (currently tasks are only triggered by event emitters that trace back to the same publish handler calls, in other words within the same process "session")
<br/>this is very usefull when dealing with external events (such as for example intercepting delivered package events in purchase workflow)
- cleanup of space/unused data
- currently there is a possibility that a task will overlap with subsequent/next tasks, this can happen when the handler call is requeued after it already published messages to run subsequent tasks.
- possiblity to use redis for pubsub and removing the need for amqp.
- deferred/delayed calls, even though you could just use `time.Sleep()`
- any new ideas/bugs/pull requests are welcome