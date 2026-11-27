# frozen_string_literal: true

# This integration test verifies that an exception raised while librdkafka delivers statistics
# is logged and does not reach the client's user.
#
# Requires a running Kafka broker at localhost:9092.
#
# Exit codes:
# - 0: both clients kept serving the raising callback, nothing was raised and the error was logged
# - 1: statistics stopped, `Consumer#poll` raised, the error was not logged (or the process
#   aborted from the polling thread)

require "rdkafka"
require "securerandom"
require "stringio"

BOOTSTRAP = "localhost:9092"
EXPECTED_CALLS = 5
LOG_LINE = "Unhandled exception: RuntimeError - boom in statistics callback"

log = StringIO.new
Rdkafka::Config.logger = Logger.new(log)

calls = Queue.new
Rdkafka::Config.statistics_callback = lambda do |_stats|
  calls << true
  raise "boom in statistics callback"
end

def wait_for(calls, deadline)
  sleep(0.1) while calls.size < EXPECTED_CALLS && Process.clock_gettime(Process::CLOCK_MONOTONIC) < deadline
end

producer = Rdkafka::Config.new(
  "bootstrap.servers": BOOTSTRAP,
  "statistics.interval.ms": 100
).producer

begin
  wait_for(calls, Process.clock_gettime(Process::CLOCK_MONOTONIC) + 10)
ensure
  producer.close
end

exit(1) if calls.size < EXPECTED_CALLS
exit(1) unless log.string.include?(LOG_LINE)

calls.clear
log.truncate(0)
log.rewind

consumer = Rdkafka::Config.new(
  "bootstrap.servers": BOOTSTRAP,
  "group.id": "statistics-callback-raise-#{SecureRandom.hex(6)}",
  "statistics.interval.ms": 100
).consumer

deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + 10

begin
  while calls.size < EXPECTED_CALLS && Process.clock_gettime(Process::CLOCK_MONOTONIC) < deadline
    consumer.poll(100)
  end
rescue RuntimeError
  exit(1)
ensure
  consumer.close
end

exit(1) if calls.size < EXPECTED_CALLS
exit(1) unless log.string.include?(LOG_LINE)

exit(0)
