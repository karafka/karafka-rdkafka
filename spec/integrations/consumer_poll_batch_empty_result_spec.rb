# frozen_string_literal: true

# This integration test verifies that `Consumer#poll_batch` and `Consumer#poll_batch_nb` return one
# shared frozen empty array when no messages arrive, while a batch with messages stays a new,
# mutable array.
#
# Uses librdkafka's mock cluster, so it needs no Kafka broker. One partition holds two messages.
#
# Scenarios:
# 1. `#poll_batch` returns both messages in a mutable array.
# 2. Once the partition is drained, `#poll_batch` and `#poll_batch_nb` return the same frozen empty
#    array, and mutating it raises `FrozenError`.
#
# Exit codes:
# - 0: the batch with messages is mutable and every empty poll returns the same frozen array
# - 1: messages missing, a frozen batch with messages, or a new or mutable empty result

require_relative "support/mock_cluster"
require "securerandom"

TOPIC = "it-consumer-poll-batch-empty-result-#{SecureRandom.uuid}"
TIMEOUT = 30

def now
  Process.clock_gettime(Process::CLOCK_MONOTONIC)
end

def wait_for_messages(consumer, count)
  deadline = now + TIMEOUT
  messages = []

  while now < deadline && messages.size < count
    batch = consumer.poll_batch(100, max_items: 10)

    exit(1) if !batch.empty? && batch.frozen?

    messages.concat(batch)
  end

  messages
end

MockCluster.run do |cluster|
  cluster.create_topic(TOPIC, 1)

  producer = cluster.config.producer
  2.times { |i| producer.produce(topic: TOPIC, payload: "payload #{i}", partition: 0).wait }
  producer.close

  consumer = cluster.config(
    "group.id": "it-consumer-poll-batch-empty-result-#{SecureRandom.uuid}",
    "auto.offset.reset": "earliest"
  ).consumer

  begin
    consumer.subscribe(TOPIC)

    exit(1) unless wait_for_messages(consumer, 2).map(&:payload) == ["payload 0", "payload 1"]

    empty = consumer.poll_batch(100, max_items: 10)

    exit(1) unless empty.empty? && empty.frozen?
    exit(1) unless consumer.poll_batch(100, max_items: 10).equal?(empty)
    exit(1) unless consumer.poll_batch_nb(0, max_items: 10).equal?(empty)

    begin
      empty << :item
      exit(1)
    rescue FrozenError
      nil
    end
  ensure
    consumer.close
  end
end
