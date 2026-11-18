# frozen_string_literal: true

# This integration test verifies that a consumer starts consuming every partition from its
# committed offset when the assignment grows while the committed-offset fetch of the assignment
# is in flight, and that fetch fails with a coordinator error, as it can when a cooperative
# rebalance runs while the group coordinator moves to another broker.
#
# librdkafka does not retry that OffsetFetch once the assignment changed, and the broker returns
# the error with no partitions. The queried partitions then stayed waiting for a reply that never
# came, and blocked the offset fetch of the partitions assigned later, so none of them was
# consumed (confluentinc/librdkafka#5586). With
# `dist/patches/rdkafka_offset_fetch_retry_coordinator.patch` librdkafka queries them again.
#
# The mock cluster delays the first OffsetFetch reply by 2 seconds and fails it. Partition 0 is
# assigned before the request, partition 1 while it is in flight. Each error is tested on its
# own mock cluster.
#
# Uses librdkafka's mock cluster, so it needs no Kafka broker.
#
# Exit codes:
# - 0: for each error the first consumed message of both partitions is at the committed offset
# - 1: otherwise

require_relative "support/mock_cluster"
require "securerandom"

COMMITTED_OFFSET = 3
WAIT_SECONDS = 15

ERRORS = %i[not_coordinator coordinator_not_available].freeze

# librdkafka incremental assignment API, not part of the public bindings
module IncrementalAssignBindings
  extend FFI::Library

  ffi_lib Rdkafka::Bindings.ffi_libraries.map(&:name)

  attach_function :rd_kafka_incremental_assign, %i[pointer pointer], :pointer
end

def incremental_assign(consumer, topic, partition)
  list = Rdkafka::Consumer::TopicPartitionList.new
  list.add_topic(topic, [partition])
  native_list = list.to_native_tpl

  consumer.instance_variable_get(:@native_kafka).with_inner do |inner|
    exit(1) unless IncrementalAssignBindings.rd_kafka_incremental_assign(inner, native_list).null?
  end
ensure
  Rdkafka::Bindings.rd_kafka_topic_partition_list_destroy(native_list) if native_list
end

def first_offsets(consumer)
  offsets = {}
  deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + WAIT_SECONDS

  while offsets.size < 2 && Process.clock_gettime(Process::CLOCK_MONOTONIC) < deadline
    begin
      message = consumer.poll(100)
    rescue Rdkafka::RdkafkaError
      next
    end

    offsets[message.partition] ||= message.offset if message
  end

  offsets
end

ERRORS.each do |error|
  topic = "it-consumer-incremental-assign-coordinator-errors-#{SecureRandom.hex(6)}"
  offsets = nil

  MockCluster.run do |cluster|
    cluster.create_topic(topic, 2)

    producer = cluster.config.producer
    2.times do |partition|
      6.times { |i| producer.produce(topic: topic, payload: i.to_s, partition: partition).wait }
    end

    consumer = cluster.config(
      "group.id": "it-consumer-incremental-assign-coordinator-errors-#{SecureRandom.hex(6)}",
      "enable.auto.commit": false,
      "auto.offset.reset": "earliest"
    ).consumer

    list = Rdkafka::Consumer::TopicPartitionList.new
    list.add_topic_and_partitions_with_offsets(topic, 0 => COMMITTED_OFFSET, 1 => COMMITTED_OFFSET)
    consumer.commit(list, false)

    cluster.delay_request_error(:offset_fetch, error, 2_000)

    incremental_assign(consumer, topic, 0)
    sleep(0.5)
    incremental_assign(consumer, topic, 1)

    offsets = first_offsets(consumer)
  ensure
    consumer&.close
    producer&.close
  end

  exit(1) unless offsets == { 0 => COMMITTED_OFFSET, 1 => COMMITTED_OFFSET }
end
