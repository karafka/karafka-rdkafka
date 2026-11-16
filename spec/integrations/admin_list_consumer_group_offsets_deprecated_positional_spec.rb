# frozen_string_literal: true

# This integration test verifies that code written against the deprecated positional partition list
# of `Admin#list_consumer_group_offsets` keeps working until v1.0.0.
#
# A positional list (or a positional nil) returns the same report as the `topic_partition_list:`
# keyword and emits one deprecation warning per call. The keyword form and an omitted list do not
# warn. Giving the list both ways raises `ArgumentError`.
#
# Requires a running Kafka broker at localhost:9092.
#
# Exit codes:
# - 0: the positional list gave the keyword's offsets and warned, and only it warned
# - 1: an offset was wrong, a call warned when it should not or did not warn when it should, or
#   giving the list both ways did not raise

require "rdkafka"
require "securerandom"
require "stringio"

PARTITIONS = 3
COMMITTED = { 0 => 4, 1 => 7 }.freeze

# A broker that just started may not know the group coordinator yet
def settled
  deadline = Time.now + 30

  begin
    yield.wait(max_wait_timeout_ms: 15_000)
  rescue Rdkafka::RdkafkaError => e
    coordinator_errors = %i[coordinator_load_in_progress not_coordinator coordinator_not_available]
    raise unless coordinator_errors.include?(e.code) && Time.now < deadline

    sleep(0.5)
    retry
  end
end

def capture_stderr
  stderr = $stderr
  $stderr = StringIO.new

  begin
    result = yield
    [result, $stderr.string]
  ensure
    $stderr = stderr
  end
end

def offsets(report)
  report.partitions.to_h { |part| [part[:partition], part[:offset]] }
end

topic_name = "it-list-cg-offsets-deprecated-#{SecureRandom.hex(6)}"
group_name = "it-list-cg-offsets-deprecated-#{SecureRandom.hex(6)}"

admin = Rdkafka::Config.new("bootstrap.servers": "localhost:9092").admin
consumer = Rdkafka::Config.new(
  "bootstrap.servers": "localhost:9092",
  "group.id": group_name,
  "enable.auto.commit": false
).consumer

begin
  admin.create_topic(topic_name, PARTITIONS, 1).wait(max_wait_timeout_ms: 15_000)

  # The topic must be in the metadata before we subscribe to it
  deadline = Time.now + 30
  begin
    admin.metadata(topic_name)
  rescue Rdkafka::RdkafkaError => e
    raise unless e.code == :unknown_topic_or_part && Time.now < deadline

    sleep(0.1)
    retry
  end

  committed_tpl = Rdkafka::Consumer::TopicPartitionList.new
  committed_tpl.add_topic_and_partitions_with_offsets(topic_name, COMMITTED)
  consumer.subscribe(topic_name)

  deadline = Time.now + 30
  consumer.poll(100) while consumer.assignment.empty? && Time.now < deadline
  exit(1) if consumer.assignment.empty?

  consumer.commit(committed_tpl)
  consumer.close

  tpl = Rdkafka::Consumer::TopicPartitionList.new
  tpl.add_topic(topic_name, [1, 2])

  # Scenario 1: the keyword form and an omitted list do not warn
  (keyword, omitted), warnings = capture_stderr do
    [
      settled { admin.list_consumer_group_offsets(group_name, topic_partition_list: tpl) },
      settled { admin.list_consumer_group_offsets(group_name) }
    ]
  end

  exit(1) unless offsets(keyword) == { 1 => 7, 2 => nil }
  exit(1) unless offsets(omitted) == COMMITTED
  exit(1) unless warnings.empty?

  # Scenario 2: a positional list gives the keyword's report and warns once per call
  (positional, positional_nil), warnings = capture_stderr do
    [
      admin.list_consumer_group_offsets(group_name, tpl).wait(max_wait_timeout_ms: 15_000),
      admin.list_consumer_group_offsets(group_name, nil).wait(max_wait_timeout_ms: 15_000)
    ]
  end

  exit(1) unless offsets(positional) == offsets(keyword)
  exit(1) unless offsets(positional_nil) == COMMITTED
  exit(1) unless warnings.scan(/^DEPRECATION WARNING: .*will be removed in v1\.0\.0\.$/).size == 2

  # Scenario 3: the list given both ways raises
  begin
    admin.list_consumer_group_offsets(group_name, tpl, topic_partition_list: tpl)
    exit(1)
  rescue ArgumentError
    nil
  end
ensure
  consumer.close

  begin
    admin.delete_topic(topic_name).wait(max_wait_timeout_ms: 15_000)
  rescue Rdkafka::RdkafkaError
    nil
  end

  admin.close
end

exit(0)
