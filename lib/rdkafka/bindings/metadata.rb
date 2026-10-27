# frozen_string_literal: true

module Rdkafka
  module Bindings
    # Metadata

    attach_function :rd_kafka_name, [:pointer], :string
    # Both return a newly allocated string the caller must release with rd_kafka_mem_free; they
    # are attached as :pointer (not :string) so we can free them instead of leaking. clusterid
    # also takes a timeout_ms (the previous arity was wrong, leaving that argument as garbage).
    attach_function :rd_kafka_memberid, [:pointer], :pointer, blocking: true
    attach_function :rd_kafka_clusterid, [:pointer, :int], :pointer, blocking: true
    attach_function :rd_kafka_mem_free, [:pointer, :pointer], :void
    attach_function :rd_kafka_metadata, [:pointer, :int, :pointer, :pointer, :int], :int, blocking: true
    attach_function :rd_kafka_metadata_destroy, [:pointer], :void, blocking: true
  end
end
