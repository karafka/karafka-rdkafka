# frozen_string_literal: true

# This integration test verifies that a message that cannot be built (its headers are corrupted)
# is not lost silently by `ShareConsumer#poll`.
#
# The share consumer has acquired the record. When the Ruby message cannot be built, the
# `RdkafkaError` returned inline in the batch holds the topic, partition and offset of the record,
# so the caller can acknowledge it in explicit acknowledgement mode.
#
# Uses librdkafka's mock cluster, so it needs no Kafka broker. Partition 1 of a 2-partition topic
# holds two valid messages and then a message whose headers cannot be parsed.
#
# Scenarios:
# 1. Polling reaches the corrupted message: the returned `RdkafkaError` has the topic, partition
#    and offset of that message in `details`.
# 2. The caller releases the record with these details: the next poll gets the same record again
#    and returns the same error.
#
# Exit codes:
# - 0: the error had the message details and the released record was delivered again
# - 1: an error without the details, or the record was not delivered again

require_relative "support/mock_cluster"
require_relative "support/corrupted_record_batch"
require "securerandom"

$stdout.sync = true

TOPIC = "it-share-consumer-message-build-error-#{SecureRandom.uuid}"
PARTITION = 1
TIMEOUT = 30

Record = Struct.new(:topic, :partition, :offset, keyword_init: true)

def now
  Process.clock_gettime(Process::CLOCK_MONOTONIC)
end

def wait_for_build_error(consumer)
  deadline = now + TIMEOUT

  while now < deadline
    consumer.poll(100).each do |result|
      return result if result.is_a?(Rdkafka::RdkafkaError)

      consumer.acknowledge(result, :accept)
    end
  end

  nil
end

MockCluster.run do |cluster|
  cluster.create_topic(TOPIC, 2)
  cluster.share_group_auto_offset_reset = :earliest

  producer = cluster.config.producer
  2.times { |i| producer.produce(topic: TOPIC, payload: "payload #{i}", partition: PARTITION).wait }
  producer.close

  offset = cluster.produce_record_batch(TOPIC, PARTITION, CorruptedRecordBatch.build)
  details = { topic: TOPIC, partition: PARTITION, offset: offset }

  consumer = cluster.config(
    "group.id": "it-share-consumer-message-build-error-#{SecureRandom.uuid}",
    "share.acknowledgement.mode": "explicit"
  ).share_consumer

  begin
    consumer.subscribe(TOPIC)

    error = wait_for_build_error(consumer)

    exit(1) unless error&.details == details

    consumer.acknowledge(Record.new(**error.details), :release)
    consumer.commit_sync

    exit(1) unless wait_for_build_error(consumer)&.details == details
  ensure
    consumer.close
  end
end
