# frozen_string_literal: true

# This integration test verifies that an exception raised while librdkafka delivers statistics
# is logged and does not reach the client's user.
#
# An exception from the user callback, or from parsing the statistics JSON (json 3.x raised on the
# duplicate broker names librdkafka can report), used to escape the FFI callback:
#   - a producer serves statistics on its polling thread (`abort_on_exception = true`), so the
#     whole process aborted, and
#   - a consumer serves them in `Consumer#poll`, so `poll` raised the error to its caller.
# It is now logged and swallowed, matching the delivery callback.
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

$stdout.sync = true

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

if calls.size < EXPECTED_CALLS
  warn "FAIL: only #{calls.size} producer statistics deliveries; the polling thread stopped"
  exit(1)
end

unless log.string.include?(LOG_LINE)
  warn "FAIL: the producer statistics callback error was not logged"
  exit(1)
end

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
rescue RuntimeError => e
  warn "FAIL: Consumer#poll raised the statistics callback error: #{e.message}"
  exit(1)
ensure
  consumer.close
end

if calls.size < EXPECTED_CALLS
  warn "FAIL: only #{calls.size} consumer statistics deliveries"
  exit(1)
end

unless log.string.include?(LOG_LINE)
  warn "FAIL: the consumer statistics callback error was not logged"
  exit(1)
end

puts "PASS: the producer and the consumer survived raising statistics deliveries"
exit(0)
