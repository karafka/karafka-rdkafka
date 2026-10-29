# frozen_string_literal: true

# This integration test verifies that `ShareConsumer#close` returns when it runs while the first
# partition assignment waits for the metadata of its topic.
#
# A new member can get its first assignment from a heartbeat before its metadata cache knows the
# assigned topic id. librdkafka then requests the topic metadata and applies the assignment when
# the response arrives. The close used to hang forever when it started while that request was in
# flight: the response applied the assignment after the member had left the group, and the
# closing group waited for that partition to be released. The bundled librdkafka patch
# (dist/patches/rdkafka_cgrp_late_assignment_after_leave.patch) drops the late assignment.
#
# The scenario:
# 1. the consumer connects and finds its coordinator
# 2. all later Metadata responses are delayed
# 3. the consumer subscribes and gets its assignment, whose metadata request is now in flight
# 4. the next heartbeats, including the leave heartbeat of the close, are delayed longer, so the
#    metadata response arrives while the close still waits for the leave
# 5. the consumer closes before the metadata response arrives
#
# Uses librdkafka's mock cluster, so it needs no Kafka broker.
#
# Exit codes:
# - 0: the close returned
# - 1: the close did not return in time, or the scenario did not reach the close window

require_relative "support/mock_cluster"
require "securerandom"

$stdout.sync = true

TOPIC = "it-share-consumer-close-late-assignment-#{SecureRandom.hex(6)}"
# Keeps the metadata request in flight until the close has left the group
METADATA_DELAY_MS = 1_000
# Keeps the close waiting for its leave heartbeat until after the metadata response
LEAVE_DELAY_MS = 3_000
WAIT_TIMEOUT = 10
CLOSE_TIMEOUT = 20

def fail!(message)
  warn "FAIL: #{message}"
  # A hung close holds the native client, so a regular exit would hang too
  exit!(1)
end

def wait_until(description)
  deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + WAIT_TIMEOUT

  until yield
    fail!("timed out waiting for #{description}") if Process.clock_gettime(Process::CLOCK_MONOTONIC) > deadline

    sleep(0.05)
  end
end

def connected?(stats)
  return false unless stats
  return false unless stats.dig("cgrp", "state") == "up"

  brokers = stats["brokers"].values
  brokers.any? { |broker| broker["nodeid"] == 1 } && brokers.all? { |broker| broker["state"] == "UP" }
end

MockCluster.run do |cluster|
  cluster.create_topic(TOPIC, 1)
  cluster.start_request_tracking

  stats = nil
  Rdkafka::Config.statistics_callback = ->(published) { stats = published }

  consumer = cluster.config(
    "group.id": "it-share-consumer-close-late-assignment-#{SecureRandom.hex(6)}",
    "statistics.interval.ms": 100
  ).share_consumer

  # Metadata delayed before this point would also delay finding the coordinator
  wait_until("the coordinator and broker connections") do
    consumer.events_poll(100)
    connected?(stats)
  end
  Rdkafka::Config.statistics_callback = nil

  cluster.delay_requests(:metadata, METADATA_DELAY_MS, count: 10)
  consumer.subscribe(TOPIC)

  assignment_metadata = nil
  wait_until("the metadata request of the assignment") do
    heartbeat = cluster.request_timestamps(:share_group_heartbeat).first
    assignment_metadata = heartbeat && cluster.request_timestamps(:metadata).find { |metadata| metadata > heartbeat }
  end

  # Covers a regular heartbeat sent before the leave heartbeat
  cluster.delay_requests(:share_group_heartbeat, LEAVE_DELAY_MS, count: 2)

  closer = Thread.new { consumer.close }

  fail!("ShareConsumer#close did not return within #{CLOSE_TIMEOUT}s") unless closer.join(CLOSE_TIMEOUT)

  # A leave that reached the mock after the metadata response left the group too late to test
  # the race, and the close passes either way
  leave = cluster.request_timestamps(:share_group_heartbeat).last
  metadata_response = assignment_metadata + (METADATA_DELAY_MS * 1_000)

  fail!("the leave heartbeat came after the metadata response: close window missed") unless leave < metadata_response
end

puts "ShareConsumer#close returned"
