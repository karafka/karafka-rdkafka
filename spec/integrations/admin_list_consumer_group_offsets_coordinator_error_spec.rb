# frozen_string_literal: true

# This integration test verifies that `Admin#list_consumer_group_offsets` raises a retriable error,
# instead of crashing the process, when the group coordinator is not known or has moved.
#
# On `not_coordinator` and `coordinator_not_available`, librdkafka 2.16.0 asks the client's own
# consumer group to look the coordinator up again. An admin client has no consumer group, so it
# dereferenced NULL and the process died with SIGSEGV. The bundled librdkafka carries the upstream
# fix (confluentinc/librdkafka#5611) as a patch until a release includes it.
#
# librdkafka's mock cluster returns the error for the OffsetFetch request, so no broker is needed.
#
# Exit codes:
# - 0: each coordinator error was raised, and the next call succeeded
# - 1: an error was not raised, or the next call failed
# - a crash (SIGSEGV): the patch is missing

require "rdkafka"
require "securerandom"

$stdout.sync = true

OFFSET_FETCH_API_KEY = 9

COORDINATOR_ERRORS = {
  not_coordinator: 16,
  coordinator_not_available: 15
}.freeze

# librdkafka mock cluster API, not part of the gem bindings
module MockBindings
  extend FFI::Library

  ffi_lib Rdkafka::Bindings.ffi_libraries.map(&:name)

  attach_function :rd_kafka_handle_mock_cluster, [:pointer], :pointer
  attach_function :rd_kafka_mock_push_request_errors, [:pointer, :int16, :size_t, :varargs], :void
end

def fail!(message)
  warn "FAIL: #{message}"
  exit(1)
end

group_name = "it-list-cg-offsets-coordinator-#{SecureRandom.hex(6)}"

admin = Rdkafka::Config.new("test.mock.num.brokers": 1).admin

begin
  mock_cluster = admin.instance_variable_get(:@native_kafka).with_inner do |inner|
    MockBindings.rd_kafka_handle_mock_cluster(inner)
  end

  fail!("no mock cluster") if mock_cluster.null?

  COORDINATOR_ERRORS.each do |name, code|
    MockBindings.rd_kafka_mock_push_request_errors(mock_cluster, OFFSET_FETCH_API_KEY, 1, :int, code)

    begin
      admin.list_consumer_group_offsets(group_name).wait(max_wait_timeout_ms: 15_000)
      fail!("#{name} was not raised")
    rescue Rdkafka::RdkafkaError => e
      fail!("expected #{name}, got #{e.code}") unless e.code == name

      puts "Raised: #{e.code}"
    end

    report = admin.list_consumer_group_offsets(group_name).wait(max_wait_timeout_ms: 15_000)

    fail!("expected no partitions after #{name}, got #{report.partitions.inspect}") unless report.partitions.empty?
  end
ensure
  admin.close
end

puts "PASS: coordinator errors were raised without a crash"
exit(0)
