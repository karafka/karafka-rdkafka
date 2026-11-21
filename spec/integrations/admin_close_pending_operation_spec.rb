# frozen_string_literal: true

# This integration test verifies that `Admin#close` returns while an admin operation is still
# pending, and that the operation's handle then fails with `destroy`.
#
# `rd_kafka_destroy` fails the pending operation and runs the background event callback on the
# librdkafka background thread. That callback needs the GVL. `rd_kafka_destroy` was attached
# without `blocking: true`, so the closing thread held the GVL while it waited for the background
# thread, and the process froze.
#
# No broker is required: the operation stays pending because the broker is unreachable.
#
# Exit codes:
# - 0: `close` returned and the handle failed with `destroy`
# - 1: `close` did not return, or the handle did not fail with `destroy`

require "rdkafka"

CLOSE_TIMEOUT = 15

admin = Rdkafka::Config.new("bootstrap.servers": "127.0.0.1:9095").admin

handle = admin.create_topic("it-admin-close-pending-#{rand(1_000_000)}", 1, 1)

sleep(0.5)

closer = Thread.new { admin.close }

# A normal exit runs the admin finalizer, which waits for the stuck close
exit!(1) if closer.join(CLOSE_TIMEOUT).nil?

begin
  handle.wait(max_wait_timeout_ms: 5_000)
  exit(1)
rescue Rdkafka::RdkafkaError => e
  exit(1) unless e.code == :destroy
end
