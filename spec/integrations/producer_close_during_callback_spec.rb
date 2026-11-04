# frozen_string_literal: true

# This integration test verifies that `Producer#close` returns while a callback on the polling
# thread uses the native handle.
#
# `close` waits for the polling thread to finish. When it waited with exclusive access to the
# native handle, a callback on that thread (here the oauthbearer token refresh) that called
# `#oauthbearer_set_token` waited for that access, so the polling thread never finished and `close`
# never returned.
#
# The first token is short-lived, so librdkafka asks for a new one on the polling thread. That
# refresh lets `close` start, then sets the token.
#
# No Kafka broker is needed: the token is only stored on the client.
#
# Exit codes:
# - 0: `close` returned and the refresh on the polling thread set its token
# - 1: `close` did not return or the refresh failed

require "rdkafka"

$stdout.sync = true

CLOSE_TIMEOUT = 10

refreshes = 0
closing = Queue.new
results = Queue.new

producer = nil

Rdkafka::Config.oauthbearer_token_refresh_callback = lambda do |_config, _client_name|
  refreshes += 1

  # The first refresh runs in the initial poll of `#start`, the next ones on the polling thread
  if refreshes > 1
    closing << true
    sleep(1)
  end

  results << producer.oauthbearer_set_token(
    token: "token",
    lifetime_ms: (Time.now.to_f * 1_000).to_i + ((refreshes == 1) ? 2_000 : 900_000),
    principal_name: "kafka-cluster"
  )
end

producer = Rdkafka::Config.new(
  "bootstrap.servers": "127.0.0.1:9",
  "security.protocol": "sasl_ssl",
  "sasl.mechanisms": "OAUTHBEARER"
).producer(native_kafka_auto_start: false)

producer.start
closing.pop

closer = Thread.new { producer.close }

exit(1) unless closer.join(CLOSE_TIMEOUT)
exit(1) unless results.size == 2
exit(1) unless Array.new(2) { results.pop }.all?(&:zero?)
