# frozen_string_literal: true

# This integration test verifies that a cooperative consumer (classic protocol) closed while its
# incremental unassign is still completing sends LeaveGroup, instead of leaving the coordinator to
# evict it after `session.timeout.ms`.
#
# After `unsubscribe`, the rebalance callback revokes the partitions with an incremental unassign
# and librdkafka commits the stored offsets of the removed partitions. When `close` ran while that
# commit was in flight, librdkafka finished the unassign without leaving the group.
# karafka-rdkafka carries `dist/patches/rdkafka_cgrp_classic_leave_on_terminate.patch` for it.
#
# Scenarios, each on its own mock cluster, with the response to the revoke-time OffsetCommit
# delayed by 500 ms and by 50 ms. `close` waits for that commit, so a `close` shorter than half the
# delay means the commit was no longer in flight and the scenario did not run.
#
# Uses librdkafka's mock cluster, so it needs no Kafka broker.
#
# Exit codes:
# - 0: in every scenario the commit was in flight at `close`, and `close` sent exactly one
#   LeaveGroup and returned within `session.timeout.ms`
# - 1: otherwise

require_relative "support/mock_cluster"
require "securerandom"

TOPIC = "it-consumer-close-during-incremental-unassign-#{SecureRandom.hex(6)}"
SESSION_TIMEOUT_MS = 6_000
COMMIT_DELAYS_MS = [500, 50].freeze
MESSAGES = 10

# Records that the partitions were revoked
class RevokeListener
  attr_reader :revoked

  def on_partitions_revoked(_list)
    @revoked = true
  end
end

def now
  Process.clock_gettime(Process::CLOCK_MONOTONIC)
end

COMMIT_DELAYS_MS.each do |commit_delay_ms|
  MockCluster.run do |cluster|
    cluster.create_topic(TOPIC, 1)

    producer = cluster.config.producer

    begin
      MESSAGES.times { producer.produce(topic: TOPIC, payload: "payload", partition: 0).wait }
    ensure
      producer.close
    end

    listener = RevokeListener.new
    config = cluster.config(
      "group.id": "it-consumer-close-during-incremental-unassign-#{SecureRandom.hex(6)}",
      "partition.assignment.strategy": "cooperative-sticky",
      "session.timeout.ms": SESSION_TIMEOUT_MS,
      "heartbeat.interval.ms": 500,
      "auto.offset.reset": "earliest",
      "enable.auto.commit": true,
      "auto.commit.interval.ms": 600_000
    )
    config.consumer_rebalance_listener = listener
    consumer = config.consumer

    begin
      consumer.subscribe(TOPIC)

      consumed = 0
      deadline = now + 10

      while consumed < MESSAGES
        exit(1) if now > deadline

        consumed += 1 if consumer.poll(100)
      end

      cluster.delay_requests(:offset_commit, commit_delay_ms)
      cluster.start_request_tracking

      consumer.unsubscribe

      deadline = now + 10

      until listener.revoked
        exit(1) if now > deadline

        consumer.poll(10)
      end

      exit(1) unless consumer.assignment.empty?

      started_at = now
      consumer.close
      close_ms = (now - started_at) * 1_000

      exit(1) unless close_ms >= commit_delay_ms / 2
      exit(1) unless cluster.request_timestamps(:leave_group).size == 1
      exit(1) unless close_ms < SESSION_TIMEOUT_MS
    ensure
      consumer.close
    end
  end
end
