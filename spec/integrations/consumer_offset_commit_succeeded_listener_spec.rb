# frozen_string_literal: true

# This integration test verifies that an offset commit listener with only
# `#on_offsets_commit_succeeded` gets only the successful commits, and that the commit offsets
# are not converted to Ruby for the other commits.
#
# librdkafka passes an entry for every assigned partition to the offset commit callback, also for
# a commit with nothing new (`no_offset`). Converting it on every commit costs time and
# allocations that grow with the assignment, so a listener that only wants successes should not
# pay for the other commits.
#
# Requires a running Kafka broker at localhost:9092.
#
# Scenarios:
# 1. Commit asynchronously an offset of a topic that does not exist: not reported.
# 2. Commit synchronously with nothing new stored (`no_offset`): not reported.
# 3. Consume a message and commit its offset asynchronously: reported with that offset.
# The commit offsets are converted to Ruby once, for scenario 3 only.
#
# Exit codes:
# - 0: only the successful commit was reported
# - 1: a commit was reported when it should not be, or the successful commit is missing or wrong

require "rdkafka"
require "securerandom"

BOOTSTRAP = "localhost:9092"
TOPIC = "it-offset-commit-succeeded-listener-#{SecureRandom.uuid}"
MISSING_TOPIC = "it-offset-commit-succeeded-listener-missing-#{SecureRandom.uuid}"
TIMEOUT = 30

# Collects the successful commits
class SucceededCommitListener
  attr_reader :results

  def initialize
    @results = Queue.new
  end

  def on_offsets_commit_succeeded(list)
    @results << list
  end
end

# Counts the conversions of native commit offsets to Ruby
module FromNativeTplCounter
  class << self
    attr_accessor :count
  end

  self.count = 0

  def from_native_tpl(*)
    FromNativeTplCounter.count += 1
    super
  end
end

Rdkafka::Consumer::TopicPartitionList.singleton_class.prepend(FromNativeTplCounter)

def now
  Process.clock_gettime(Process::CLOCK_MONOTONIC)
end

admin = Rdkafka::Config.new("bootstrap.servers": BOOTSTRAP).admin
admin.create_topic(TOPIC, 1, 1).wait

# The topic must be in the metadata before we produce to it
deadline = now + TIMEOUT
begin
  admin.metadata(TOPIC)
rescue Rdkafka::RdkafkaError
  raise if now > deadline

  sleep(0.1)
  retry
end

producer = Rdkafka::Config.new("bootstrap.servers": BOOTSTRAP).producer
producer.produce(topic: TOPIC, payload: "payload", partition: 0).wait

listener = SucceededCommitListener.new
config = Rdkafka::Config.new(
  "bootstrap.servers": BOOTSTRAP,
  "group.id": "it-offset-commit-succeeded-listener-#{SecureRandom.uuid}",
  "auto.offset.reset": "earliest",
  "enable.auto.commit": false,
  "enable.auto.offset.store": false
)
config.consumer_offset_commit_listener = listener
consumer = config.consumer

begin
  consumer.subscribe(TOPIC)

  message = nil
  deadline = now + TIMEOUT
  message = consumer.poll(100) while message.nil? && now < deadline

  exit(1) if message.nil?

  # Assignment also converts a list, so count only from the commits on
  FromNativeTplCounter.count = 0

  # 1. A failed async commit
  missing = Rdkafka::Consumer::TopicPartitionList.new
  missing.add_topic_and_partitions_with_offsets(MISSING_TOPIC, 0 => 1)
  consumer.commit(missing, true)

  # 2. A commit with nothing new stored. Sync, as an async commit reads the stored offsets only
  # when librdkafka serves it, so it could also commit the offset stored in step 3
  begin
    consumer.commit(nil, false)
  rescue Rdkafka::RdkafkaError => e
    raise unless e.code == :no_offset
  end

  # 3. A successful async commit
  consumer.store_offset(message)
  consumer.commit(nil, true)

  deadline = now + TIMEOUT
  consumer.poll(100) while listener.results.empty? && now < deadline

  exit(1) if listener.results.empty?

  list = listener.results.pop
  offsets = list.to_h.fetch(TOPIC, []).map(&:offset)

  exit(1) unless offsets == [message.offset + 1]

  # Results are served in order, so the other commits were served before the successful one
  exit(1) unless listener.results.empty?

  # The list built for the reported success is the only conversion
  exit(1) unless FromNativeTplCounter.count == 1
ensure
  consumer.close
  producer.close
  admin.delete_topic(TOPIC).wait
  admin.close
end

exit(0)
