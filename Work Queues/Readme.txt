The main idea behind Work Queues (aka: Task Queues) is to avoid doing a resource-intensive task immediately and having to wait for it to complete. Instead we schedule the task to be done later. We encapsulate a task as a message and send it to a queue. A worker process running in the background will pop the tasks and eventually execute the job. When you run many workers the tasks will be shared between them.

This concept is especially useful in web applications where it's impossible to handle a complex task during a short HTTP request window.

It sends message to the workers by following Round-robin dispatching. 


-A producer is a user application that sends messages.
-A queue is a buffer that stores messages.
-A consumer is a user application that receives messages.
