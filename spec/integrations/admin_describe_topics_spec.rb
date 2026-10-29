# frozen_string_literal: true

# This integration test shows how to describe topics with `Admin#describe_topics` and verifies the
# result end to end.
#
# `describe_topics` returns one entry per requested topic: its name, its topic id (KIP-516, base64),
# whether it is internal, and every partition with its leader, replicas and in-sync replicas (as
# broker nodes). A topic that cannot be described does not fail the request; its entry carries an
# `:error` and a `nil` topic id instead.
#
# Requires a running Kafka broker at localhost:9092.
#
# Exit codes:
# - 0: the topics were described correctly
# - 1: a described value was missing or wrong

require "rdkafka"
require "securerandom"

$stdout.sync = true

PARTITIONS = 3

# A topic that was just created can be missing from the broker's metadata for a moment, so retry
# while the new topic is reported as unknown
def describe_topics_settled(admin, topic_name, topic_names)
  deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + 10

  loop do
    report = admin.describe_topics(topic_names).wait(max_wait_timeout_ms: 15_000)
    error = report.topics.find { |topic| topic[:name] == topic_name }&.fetch(:error)

    return report unless error&.code == :unknown_topic_or_part
    return report if Process.clock_gettime(Process::CLOCK_MONOTONIC) > deadline

    sleep(0.1)
  end
end

admin = Rdkafka::Config.new("bootstrap.servers": "localhost:9092").admin
topic_name = "it-describe-topics-#{SecureRandom.hex(6)}"
missing_topic_name = "it-describe-topics-missing-#{SecureRandom.hex(6)}"

begin
  admin.create_topic(topic_name, PARTITIONS, 1).wait(max_wait_timeout_ms: 15_000)

  nodes = admin.describe_cluster.wait(max_wait_timeout_ms: 15_000).nodes
  report = describe_topics_settled(admin, topic_name, [topic_name, missing_topic_name])
  topics = report.topics.to_h { |topic| [topic[:name], topic] }

  topic = topics.fetch(topic_name) do
    warn "FAIL: #{topic_name} was not described"
    exit(1)
  end

  puts "Topic: #{topic[:name]} id=#{topic[:topic_id]} internal=#{topic[:is_internal]}"
  topic[:partitions].each do |partition|
    puts "  Partition #{partition[:partition]}: leader=#{partition[:leader]&.fetch(:id)} " \
         "replicas=#{partition[:replicas].map { |node| node[:id] }} isr=#{partition[:isr].map { |node| node[:id] }}"
  end

  unless topic[:error].nil?
    warn "FAIL: unexpected error: #{topic[:error].inspect}"
    exit(1)
  end
  if topic[:topic_id].to_s.empty?
    warn "FAIL: topic id is empty"
    exit(1)
  end
  unless topic[:is_internal] == false
    warn "FAIL: topic is reported as internal"
    exit(1)
  end

  indexes = topic[:partitions].map { |partition| partition[:partition] }
  unless indexes == (0...PARTITIONS).to_a
    warn "FAIL: expected partitions #{(0...PARTITIONS).to_a}, got #{indexes}"
    exit(1)
  end

  topic[:partitions].each do |partition|
    unless nodes.include?(partition[:leader])
      warn "FAIL: leader #{partition[:leader].inspect} is not a cluster node"
      exit(1)
    end
    unless !partition[:replicas].empty? && (partition[:replicas] - nodes).empty?
      warn "FAIL: replicas #{partition[:replicas].inspect} are not cluster nodes"
      exit(1)
    end
    unless !partition[:isr].empty? && (partition[:isr] - partition[:replicas]).empty?
      warn "FAIL: isr #{partition[:isr].inspect} is not a subset of replicas"
      exit(1)
    end
  end

  missing = topics.fetch(missing_topic_name) do
    warn "FAIL: #{missing_topic_name} was not described"
    exit(1)
  end

  puts "Missing topic: #{missing[:name]} error=#{missing[:error]&.code.inspect} id=#{missing[:topic_id].inspect}"

  unless missing[:error].is_a?(Rdkafka::RdkafkaError)
    warn "FAIL: missing topic has no error"
    exit(1)
  end
  unless missing[:error].code == :unknown_topic_or_part
    warn "FAIL: missing topic has an unexpected error: #{missing[:error].code}"
    exit(1)
  end
  unless missing[:topic_id].nil?
    warn "FAIL: missing topic has a topic id: #{missing[:topic_id]}"
    exit(1)
  end
ensure
  begin
    admin.delete_topic(topic_name).wait(max_wait_timeout_ms: 15_000)
  rescue Rdkafka::RdkafkaError
    nil
  end

  admin.close
end

puts "PASS: the topics were described correctly"
exit(0)
