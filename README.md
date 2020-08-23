# go-pubsub-workflow

a durable distributed pubsub with option to construct dynamic workflows (condition based process forks & joins)
<br>each task within workflow is ran by republishing a message to execute next tasks when the first task (also ran by publish) finishes.
<br>this ensures that each task within workflow is reprocessed if service crashes, and the next will run when the first one completes.
<br>the library leverages rabbitmq for its durability and redis for its distributed key store to give means to publish subsequent tasks
exactly once, and prevent duplicate calls of next tasks if the previous task is requeued/reprocessed.

### how it works
.....
...

### setup
.....
...

### how to use
.....
...

### known bugs / improvements
- introduce usage of redis pipelines
- global events (currently tasks are only tiggered by event emitters that trace back to the same publish handler calls, in other words within the same process "session")
<br/>this is very usefull when dealing with external events (such as for example intercepting delivered package events in purchase workflow)
- cleanup of space/unused data
- currently there is a possibility of a publish handler call to overlap with subsequent/next publish handler calls,
this can happen when the handler call is requeued after it already published messages to run subsequent tasks.
- possiblity to use redis for pubsub and removing the need for amqp.
- deferred/delayed calls, even though you could just use `time.Sleep()`
- any new ideas/bug reports/pull requests are appreciated