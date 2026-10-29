# frozen_string_literal: true

# This integration test verifies that `#oauthbearer_set_token` passes its extension count and
# error buffer size to librdkafka as full `size_t` values.
#
# Both arguments used to be bound as 32-bit `int`. On JRuby the upper half of the 64-bit
# argument could then hold garbage, so librdkafka saw a wrong extension count (the call failed
# with `invalid_arg` or read past the extensions) or a wrong error buffer size (the error string
# came back truncated).
#
# No Kafka broker is needed: the token is only stored on the client.
#
# Scenarios:
# 1. A client with SASL/OAUTHBEARER accepts the token, with and without extensions, every time
# 2. A client without SASL/OAUTHBEARER returns `__STATE` with the full error string every time
#
# Exit codes:
# - 0: every call returned the expected result
# - 1: a call returned an unexpected result

require "rdkafka"

$stdout.sync = true

ITERATIONS = 2_000
EXPECTED_ERROR = "SASL/OAUTHBEARER is not the configured authentication mechanism"

producer = Rdkafka::Config.new(
  "bootstrap.servers": "127.0.0.1:9",
  "security.protocol": "sasl_ssl",
  "sasl.mechanisms": "OAUTHBEARER"
).producer

failures = []

begin
  ITERATIONS.times do |i|
    response = producer.oauthbearer_set_token(
      token: "token",
      lifetime_ms: (Time.now.to_i + 900) * 1000,
      principal_name: "kafka-cluster",
      extensions: i.even? ? nil : { "foo" => "bar" }
    )

    failures << "iteration #{i}: response #{response}" unless response.zero?
  end
ensure
  producer.close
end

native_client = Rdkafka::Bindings.rd_kafka_new(:rd_kafka_producer, nil, nil, 0)

begin
  ITERATIONS.times do |i|
    error_buffer = FFI::MemoryPointer.from_string(" " * 256)
    response = Rdkafka::Bindings.rd_kafka_oauthbearer_set_token(
      native_client, "token", (Time.now.to_i + 900) * 1000, "kafka-cluster", nil, 0, error_buffer, 256
    )
    error = error_buffer.read_string

    next if response == Rdkafka::Bindings::RD_KAFKA_RESP_ERR__STATE && error == EXPECTED_ERROR

    failures << "iteration #{i}: response #{response}, error #{error.inspect}"
  end
ensure
  Rdkafka::Bindings.rd_kafka_destroy(native_client)
end

unless failures.empty?
  warn "FAIL: #{failures.size} of #{ITERATIONS * 2} calls failed, first: #{failures.first}"
  exit(1)
end

puts "PASS: #{ITERATIONS * 2} token set calls returned the expected result"
exit(0)
