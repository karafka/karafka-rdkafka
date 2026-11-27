# frozen_string_literal: true

# This integration test verifies that an exception raised while a producer's polling thread
# delivers statistics does not kill the process.
#
# The statistics callback runs on the polling thread, which has `abort_on_exception = true`. An
# exception from the user callback, or from parsing the statistics JSON (json 3.x raised on the
# duplicate broker names librdkafka can report), escaped the FFI callback and aborted the whole
# process. It is now logged and swallowed, matching the delivery callback.
#
# Requires a running Kafka broker at localhost:9092.
#
# Exit codes:
# - 0: the process survived the raising callback and statistics kept being delivered
# - 1: statistics stopped, the error was not logged (or the process aborted from the polling
#   thread)

require "rdkafka"
require "stringio"

$stdout.sync = true

BOOTSTRAP = "localhost:9092"
EXPECTED_CALLS = 5

log = StringIO.new
Rdkafka::Config.logger = Logger.new(log)

calls = Queue.new
Rdkafka::Config.statistics_callback = lambda do |_stats|
  calls << true
  raise "boom in statistics callback"
end

producer = Rdkafka::Config.new(
  "bootstrap.servers": BOOTSTRAP,
  "statistics.interval.ms": 100
).producer

deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + 10

begin
  sleep(0.1) while calls.size < EXPECTED_CALLS && Process.clock_gettime(Process::CLOCK_MONOTONIC) < deadline
ensure
  producer.close
end

if calls.size < EXPECTED_CALLS
  warn "FAIL: only #{calls.size} statistics deliveries; the polling thread stopped"
  exit(1)
end

unless log.string.include?("Unhandled exception: RuntimeError - boom in statistics callback")
  warn "FAIL: the statistics callback error was not logged"
  exit(1)
end

puts "PASS: the process survived #{calls.size} raising statistics deliveries"
exit(0)
