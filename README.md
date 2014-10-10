kafka-bulk-consumer
===================


The problem:
------------

* You're using Kafka
* You want to process lots of messages in a highly concurrent manor
* You want an at-least-once processing guarantee  

A single Kafka consumer is capable of processing a large number of messages very quickly. This
is largely due to batching behaviour under the hood though. Specifically, the only control you have over 
acknowledging messages as "processed" is to say "I acknowledge all the messages you've given me so far".

If you're processing multiple messages concurrently, this form of acknowledgement prevents a 
per-message delivery guarantee.

So you could:
* Set the batch size to 1
* Instantiate multiple kafka consumers

But if you assume most message processing is successful, this hurts your processing rate, since you're 
throwing out most of kafka's per-client-instance processing speed. Further, the number of consumer 
instances is limited by the partition count for your topic.

The solution:
-------------

This library attempts to still use, but conceal, kafka's batching requirement while still 
providing a mechanism for at-least-once delivery guarantees.
In order to accomplish this, the following requirements must be acceptable:

* at-LEAST-once, not exactly-once! A given message may well get processed more than once.
* No message order guarantee, even within a partition.
* The KeyedMessages in your topic must currently be of type \[String,Array\[Byte]]
* Akka
* To avoid message processing pauses, multiple consumers may still be needed.

This works by handing out messages from kafka's batch, and accepting per-message acknowledgement. 
Periodically, the acknowledgement state of the outstanding messages is evaluated, and any 
unacknowledged or NACK'ed messages are *re-queued* before the batch is acknowledged.

To avoid pauses while the batch state is evaluated and committed, a small pool of consumers may
be maintained.

