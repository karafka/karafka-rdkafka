# frozen_string_literal: true

# This integration test verifies that `#committed` of a consumer with `group.id` retries
# coordinator errors (NOT_COORDINATOR and COORDINATOR_NOT_AVAILABLE) on OffsetFetch: it reads the
# committed offset once the coordinator answers, and raises `timed_out` when the coordinator keeps
# failing until the timeout.
#
# The retry re-queries the coordinator through the consumer group, the path librdkafka's
# OffsetFetch fix for clients without one guards (confluentinc/librdkafka#5611). Before the
# OffsetFetch coordinator retry (confluentinc/librdkafka#5589), `#committed` raised these errors
# at once.
#
# Uses librdkafka's mock cluster, so it needs no Kafka broker. A crash in librdkafka kills the
# process with a signal instead of a 0 or 1 exit code.
#
# Exit codes:
# - 0: the offset was read after each error, and persistent errors raised `timed_out`
# - 1: otherwise

require_relative "support/mock_cluster"
require "securerandom"

TOPIC = "it-consumer-group-coordinator-errors-#{SecureRandom.hex(6)}"
WAIT_MS = 10_000
ERRORS = %i[coordinator_not_available not_coordinator].freeze

def tpl(offset = nil)
  Rdkafka::Consumer::TopicPartitionList.new.tap do |list|
    if offset
      list.add_topic_and_partitions_with_offsets(TOPIC, 0 => offset)
    else
      list.add_topic(TOPIC, [0])
    end
  end
end

MockCluster.run do |cluster|
  cluster.create_topic(TOPIC, 1)
  consumer = cluster.config(
    "group.id": "it-consumer-group-coordinator-errors-#{SecureRandom.hex(6)}",
    "enable.auto.commit": false
  ).consumer

  consumer.commit(tpl(3), false)

  ERRORS.each do |error|
    cluster.push_request_errors(:offset_fetch, error)

    exit(1) unless consumer.committed(tpl, WAIT_MS).to_h.fetch(TOPIC).first.offset == 3
  end

  cluster.push_request_errors(:offset_fetch, *Array.new(1_000, :not_coordinator))

  begin
    consumer.committed(tpl, 1_000)
    exit(1)
  rescue Rdkafka::RdkafkaError => e
    exit(1) unless e.code == :timed_out
  end
ensure
  consumer&.close
end
