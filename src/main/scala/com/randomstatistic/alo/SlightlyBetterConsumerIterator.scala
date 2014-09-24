package com.randomstatistic.alo

import kafka.consumer.{ConsumerTimeoutException, ConsumerIterator}
import kafka.message.MessageAndMetadata

/**
 * The Kafka 8.1 ConsumerIterator.hasNext() never returns false.
 * Instead, it blocks hasNext for consumer.timeout.ms, (which could be forever) then throws a ConsumerTimeoutException.
 * This acts slightly more like an iterator in that it returns false sometimes,
 * but, hasNext STILL BLOCKS for max(consumer.timeout.ms, next message arrival)
 * @param consumerIterator
 * @tparam Key
 * @tparam Value
 */
case class SlightlyBetterConsumerIterator[Key, Value](consumerIterator: ConsumerIterator[Key, Value]) extends Iterator[MessageAndMetadata[Key, Value]] {
  // you'll want a short consumer.timeout.ms
  override def hasNext: Boolean = try {
    consumerIterator.hasNext()
  } catch {
    case cte: ConsumerTimeoutException => false
  }

  override def next() = consumerIterator.next()
}

