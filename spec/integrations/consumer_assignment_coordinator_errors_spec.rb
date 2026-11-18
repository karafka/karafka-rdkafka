# frozen_string_literal: true

# This integration test verifies that a subscribed consumer starts consuming its assigned
# partition from the committed offset when the committed-offset fetch of the assignment fails
# with a coordinator error, as it can while the group coordinator moves to another broker.
#
# The broker returns a request-level NOT_COORDINATOR or COORDINATOR_NOT_AVAILABLE on that
# OffsetFetch with no partitions, so the assigned partitions get no offsets unless the fetch is
# retried (confluentinc/librdkafka#5586,
# `dist/patches/rdkafka_offset_fetch_retry_coordinator.patch`).
#
# Uses librdkafka's mock cluster, so it needs no Kafka broker.
#
# Exit codes:
# - 0: for each error the first consumed message is the one at the committed offset
# - 1: otherwise

require_relative "support/mock_cluster"
require "securerandom"

COMMITTED_OFFSET = 3
WAIT_MS = 15_000

ERRORS = %i[not_coordinator coordinator_not_available].freeze

def first_offset(consumer)
  deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + WAIT_MS / 1_000.0

  while Process.clock_gettime(Process::CLOCK_MONOTONIC) < deadline
    begin
      message = consumer.poll(100)
    rescue Rdkafka::RdkafkaError
      next
    end

    return message.offset if message
  end

  nil
end

ERRORS.each do |error|
  topic = "it-consumer-assignment-coordinator-errors-#{SecureRandom.hex(6)}"
  offset = nil

  MockCluster.run do |cluster|
    cluster.create_topic(topic, 1)

    producer = cluster.config.producer
    6.times { |i| producer.produce(topic: topic, payload: i.to_s, partition: 0).wait }

    consumer = cluster.config(
      "group.id": "it-consumer-assignment-coordinator-errors-#{SecureRandom.hex(6)}",
      "enable.auto.commit": false,
      "auto.offset.reset": "earliest"
    ).consumer

    list = Rdkafka::Consumer::TopicPartitionList.new
    list.add_topic_and_partitions_with_offsets(topic, 0 => COMMITTED_OFFSET)
    consumer.commit(list, false)

    cluster.push_request_errors(:offset_fetch, error)
    consumer.subscribe(topic)

    offset = first_offset(consumer)
  ensure
    consumer&.close
    producer&.close
  end

  exit(1) unless offset == COMMITTED_OFFSET
end
