# frozen_string_literal: true

require "rdkafka"

# Helpers for share consumer specs
module ShareGroup
  class << self
    # Makes a share group start from the earliest offset.
    #
    # `share.auto.offset.reset` is a broker-side group config defaulting to latest. Set it to
    # earliest before the group first attaches so records produced beforehand are delivered.
    #
    # @param admin [Rdkafka::Admin] admin client
    # @param group [String] share group name
    def start_from_earliest(admin, group)
      admin.incremental_alter_configs(
        [
          {
            resource_type: Rdkafka::Bindings::RD_KAFKA_RESOURCE_GROUP,
            resource_name: group,
            configs: [{ name: "share.auto.offset.reset", value: "earliest", op_type: 0 }]
          }
        ]
      ).wait(max_wait_timeout_ms: 15_000)
    end
  end
end
