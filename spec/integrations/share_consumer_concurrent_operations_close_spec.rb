# frozen_string_literal: true

# This integration test verifies that a share consumer used from many threads at once still counts
# its operations in progress exactly, so `ShareConsumer#close` returns once the threads are done.
#
# `ShareConsumer#with_native` counts the calls that use the native handle, and `#close` waits for
# that count to reach zero before it destroys the handle. The increment and the decrement ran under
# two different mutexes. On JRuby, which has no GVL, concurrent updates were lost: the count drifted
# away from zero and `close` waited forever while it held the access mutex, so every later call
# blocked too.
#
# Threads call `ShareConsumer#events_poll_nb` in a loop behind a start barrier. After they finish,
# `close` must return within a timeout: a count drifted either way never reaches zero.
#
# No broker is required: `events_poll_nb(0)` serves only the local queue.
#
# Exit codes:
# - 0: `close` returned
# - 1: `close` did not return

require "rdkafka"

$stdout.sync = true

THREADS = 16
RUN_FOR = 3
CLOSE_TIMEOUT = 10

share_consumer = Rdkafka::Config.new(
  "bootstrap.servers": "127.0.0.1:9999",
  "group.id": "share-concurrent-close-#{rand(1 << 30)}"
).share_consumer

barrier = Queue.new
stop = false
calls = Array.new(THREADS, 0)

workers = Array.new(THREADS) do |i|
  Thread.new do
    barrier.pop

    until stop
      share_consumer.events_poll_nb(0)
      calls[i] += 1
    end
  end
end

THREADS.times { barrier << true }
sleep(RUN_FOR)
stop = true
workers.each(&:join)

closer = Thread.new { share_consumer.close }
closed = !closer.join(CLOSE_TIMEOUT).nil?

puts "Calls: #{calls.sum}, closed: #{closed}"

unless closed
  warn "FAIL: close did not return, the operations in progress count drifted"
  # A normal exit runs the at_exit close, which waits for the stuck close
  exit!(1)
end

puts "PASS: close returned, the operations in progress count stayed exact"
exit(0)
