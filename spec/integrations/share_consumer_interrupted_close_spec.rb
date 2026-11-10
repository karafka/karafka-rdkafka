# frozen_string_literal: true

# This integration test verifies that `ShareConsumer#close` returns after calls on the share
# consumer were interrupted by `Timeout` and by signal trap handlers that raised.
#
# `ShareConsumer` counts the calls that use the native handle, and `#close` waits for that count to
# reach zero. An interrupt between the increment and the decrement could leave the count above
# zero, so `close` waited forever.
#
# A thread calls `ShareConsumer#subscription` in a loop under a very short `Timeout`. Then the main
# thread calls it in a loop while another process sends it a signal every 0.5 ms (a thread would
# get the GVL too rarely), and the trap handler raises. After that, `close` must return within a
# timeout.
#
# No broker is required: `subscription` reads only local state.
#
# Exit codes:
# - 0: `close` returned
# - 1: `close` did not return, or no call was interrupted

require "rdkafka"
require "timeout"

RUN_FOR = 2
CLOSE_TIMEOUT = 20

Trapped = Class.new(StandardError)

consumer = Rdkafka::Config.new(
  "bootstrap.servers": "127.0.0.1:9999",
  "group.id": "share-consumer-interrupted-close",
  "socket.timeout.ms": 5_000
).share_consumer

timed_out = Thread.new do
  count = 0
  deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + RUN_FOR

  while Process.clock_gettime(Process::CLOCK_MONOTONIC) < deadline
    begin
      Timeout.timeout(0.00001) { consumer.subscription }
    rescue Timeout::Error
      count += 1
    end
  end

  count
end.value

exit(1) if timed_out.zero?

raised = 0
# Raises once per arming, so no raise lands in the `rescue` below
armed = false

trap("USR1") do
  if armed
    armed = false
    raise Trapped
  end
end

sender = spawn(RbConfig.ruby, "-e", "loop { Process.kill('USR1', #{Process.pid}); sleep(0.0005) }")

deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + RUN_FOR

begin
  while Process.clock_gettime(Process::CLOCK_MONOTONIC) < deadline
    armed = true
    consumer.subscription
  end

  armed = false
rescue Trapped
  raised += 1
  retry
end

Process.kill("KILL", sender)
Process.wait(sender)
trap("USR1", "IGNORE")

exit(1) if raised.zero?

closer = Thread.new { consumer.close }

# A normal exit runs the share consumer finalizer, which waits for the stuck close
exit!(1) if closer.join(CLOSE_TIMEOUT).nil?
