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
# 3. the consumer subscribes and gets its assignment. librdkafka logs the metadata request of
#    the assignment, which is now in flight
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
require "logger"
require "securerandom"

$stdout.sync = true

TOPIC = "it-share-consumer-close-late-assignment-#{SecureRandom.hex(6)}"
# Keeps the metadata request in flight until the close has left the group
METADATA_DELAY_MS = 1_000
# Keeps the close waiting for its leave heartbeat until after the metadata response
LEAVE_DELAY_MS = 3_000
# More than the leave needs, so heartbeats sent before it cannot use up the delays
HEARTBEAT_DELAYS = 5
WAIT_TIMEOUT = 10
CLOSE_TIMEOUT = 20
# librdkafka logs this when it requests the metadata of the topics of a new target assignment
ASSIGNMENT_METADATA_LOG = /Request metadata for \d+ topic\(s\): ConsumerGroupHeartbeat API Response/

# Keeps the librdkafka log lines so the spec can wait for one
class LogLines < Logger
  def initialize
    super(nil)
    @mutex = Mutex.new
    @lines = []
  end

  def add(_severity, message = nil, progname = nil)
    message ||= block_given? ? yield : progname
    @mutex.synchronize { @lines << message.to_s }
    true
  end

  # @param pattern [Regexp]
  # @return [Boolean] whether any line seen so far matches
  def seen?(pattern)
    @mutex.synchronize { @lines.any? { |line| line.match?(pattern) } }
  end
end

def fail!(message)
  warn "FAIL: #{message}"
  # A hung close holds the native client, so a regular exit would hang too
  exit!(1)
end

def monotonic
  Process.clock_gettime(Process::CLOCK_MONOTONIC)
end

def wait_until(description)
  deadline = monotonic + WAIT_TIMEOUT

  until yield
    fail!("timed out waiting for #{description}") if monotonic > deadline

    sleep(0.05)
  end
end

def connected?(stats)
  return false unless stats
  return false unless stats.dig("cgrp", "state") == "up"

  brokers = stats["brokers"].values
  brokers.any? { |broker| broker["nodeid"] == 1 } && brokers.all? { |broker| broker["state"] == "UP" }
end

logs = LogLines.new
Rdkafka::Config.logger = logs

MockCluster.run do |cluster|
  cluster.create_topic(TOPIC, 1)
  cluster.start_request_tracking

  stats = nil
  Rdkafka::Config.statistics_callback = ->(published) { stats = published }

  consumer = cluster.config(
    "group.id": "it-share-consumer-close-late-assignment-#{SecureRandom.hex(6)}",
    "statistics.interval.ms": 100,
    debug: "metadata"
  ).share_consumer

  # Metadata delayed before this point would also delay finding the coordinator
  wait_until("the coordinator and broker connections") do
    consumer.events_poll(100)
    connected?(stats)
  end
  Rdkafka::Config.statistics_callback = nil

  cluster.delay_requests(:metadata, METADATA_DELAY_MS, count: 10)
  consumer.subscribe(TOPIC)

  # Other metadata requests (e.g. one started by subscribe) cannot be told apart from the one of
  # the assignment in the tracked requests, so wait for librdkafka to log the latter
  wait_until("the metadata request of the assignment") do
    consumer.events_poll(100)
    logs.seen?(ASSIGNMENT_METADATA_LOG)
  end

  cluster.delay_requests(:share_group_heartbeat, LEAVE_DELAY_MS, count: HEARTBEAT_DELAYS)

  heartbeats_before_close = cluster.request_timestamps(:share_group_heartbeat).size
  close_started = monotonic
  closer = Thread.new { consumer.close }

  fail!("ShareConsumer#close did not return within #{CLOSE_TIMEOUT}s") unless closer.join(CLOSE_TIMEOUT)

  close_duration = monotonic - close_started

  # Nothing sends a heartbeat after the leave, so the last one is the leave once one arrived
  # after the close started
  heartbeats = cluster.request_timestamps(:share_group_heartbeat)
  fail!("the leave heartbeat never reached the mock cluster") unless heartbeats.size > heartbeats_before_close

  # A close that returned before its delayed leave response did not wait across the metadata
  # response, so it did not test the race
  if close_duration < LEAVE_DELAY_MS / 1_000.0
    fail!("the close returned in #{close_duration.round(2)}s, before its delayed leave response")
  end

  # The close must start before the metadata response of the assignment. Every metadata response
  # leaves the mock at least METADATA_DELAY_MS after its request, and the request of the
  # assignment is the first metadata request after the join or a later one, so this bound can
  # only be lower than the real response time. A leave before it left the group in time.
  leave = heartbeats.last
  join = heartbeats.first
  first_metadata_after_join = cluster.request_timestamps(:metadata).find { |metadata| metadata > join }

  fail!("no metadata request reached the mock cluster after the join") unless first_metadata_after_join

  unless leave < first_metadata_after_join + (METADATA_DELAY_MS * 1_000)
    fail!("the leave heartbeat came after the metadata response: close window missed")
  end
end

puts "ShareConsumer#close returned"
