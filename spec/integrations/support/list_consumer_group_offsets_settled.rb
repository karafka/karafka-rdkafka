# frozen_string_literal: true

require "rdkafka"

# Lists consumer group offsets, retrying while the group coordinator is not ready yet. A broker
# that just started may not know the group coordinator for a while.
#
# @param admin [Rdkafka::Admin] admin client
# @param group_name [String] consumer group to list offsets for
# @param topic_partition_list [Rdkafka::Consumer::TopicPartitionList, nil] partitions to list
# @return [Object] the result of the list consumer group offsets handle
def list_consumer_group_offsets_settled(admin, group_name, topic_partition_list = nil)
  deadline = Time.now + 30

  begin
    admin.list_consumer_group_offsets(group_name, topic_partition_list).wait(max_wait_timeout_ms: 15_000)
  rescue Rdkafka::RdkafkaError => e
    coordinator_errors = %i[coordinator_load_in_progress not_coordinator coordinator_not_available]
    raise unless coordinator_errors.include?(e.code) && Time.now < deadline

    sleep(0.5)
    retry
  end
end
