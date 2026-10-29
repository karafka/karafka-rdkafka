# frozen_string_literal: true

# This integration test verifies that `Admin#list_consumer_group_offsets` raises a coordinator error,
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

require_relative "support/mock_cluster"
require "securerandom"

$stdout.sync = true

COORDINATOR_ERRORS = %i[not_coordinator coordinator_not_available].freeze

group_name = "it-list-cg-offsets-coordinator-#{SecureRandom.hex(6)}"

MockCluster.run do |cluster|
  admin = cluster.config.admin

  COORDINATOR_ERRORS.each do |name|
    cluster.push_request_errors(:offset_fetch, name)

    begin
      admin.list_consumer_group_offsets(group_name).wait(max_wait_timeout_ms: 15_000)
      warn "FAIL: #{name} was not raised"
      exit(1)
    rescue Rdkafka::RdkafkaError => e
      unless e.code == name
        warn "FAIL: expected #{name}, got #{e.code}"
        exit(1)
      end

      puts "Raised: #{e.code}"
    end

    report = admin.list_consumer_group_offsets(group_name).wait(max_wait_timeout_ms: 15_000)

    unless report.partitions.empty?
      warn "FAIL: expected no partitions after #{name}, got #{report.partitions.inspect}"
      exit(1)
    end
  end
ensure
  admin&.close
end

puts "PASS: coordinator errors were raised without a crash"
exit(0)
