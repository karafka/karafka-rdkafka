# frozen_string_literal: true

module Rdkafka
  module Bindings
    # Share consumer (KIP-932, preview)
    #
    # The share consumer is a distinct librdkafka handle type (rd_kafka_share_t) created with
    # rd_kafka_share_consumer_new instead of rd_kafka_new. It is single-threaded by design and
    # librdkafka itself rejects concurrent access with RD_KAFKA_RESP_ERR__CONFLICT.

    # Minimal mirror of the head of librdkafka's `struct rd_kafka_share_s`, whose first member is
    # the wrapped `rd_kafka_t` (`rkshare_rk`). `rd_kafka_share_t` has no public accessors of its
    # own - not even for the client name - but the handle it wraps is a normal `rd_kafka_t` and is
    # the very handle librdkafka passes to the statistics, error and OAuthBearer callbacks. Reading
    # it lets a share consumer report a name that matches its statistics `name` field, which
    # `rd_kafka_name` cannot do when handed the `rd_kafka_share_t` directly (it would read an
    # unrelated field and return garbage). This mirrors how the gem already maps other librdkafka
    # structs by layout for the pinned librdkafka version.
    class NativeShareConsumer < FFI::Struct
      layout :rkshare_rk, :pointer
    end

    # Acknowledge types for share consumer records
    RD_KAFKA_SHARE_ACKNOWLEDGE_TYPE_ACCEPT = 1
    RD_KAFKA_SHARE_ACKNOWLEDGE_TYPE_RELEASE = 2
    RD_KAFKA_SHARE_ACKNOWLEDGE_TYPE_REJECT = 3

    attach_function :rd_kafka_share_consumer_new, [:pointer, :pointer, :size_t], :pointer
    attach_function :rd_kafka_share_consumer_close, [:pointer], :pointer, blocking: true
    attach_function :rd_kafka_share_consumer_closed, [:pointer], :int
    attach_function :rd_kafka_share_destroy, [:pointer], :pointer, blocking: true
    attach_function :rd_kafka_share_set_log_queue, [:pointer, :pointer], :pointer

    attach_function :rd_kafka_share_subscribe, [:pointer, :pointer], :int, blocking: true
    attach_function :rd_kafka_share_unsubscribe, [:pointer], :int, blocking: true
    attach_function :rd_kafka_share_subscription, [:pointer, :pointer], :int, blocking: true

    attach_function :rd_kafka_share_poll, [:pointer, :int, :pointer], :pointer, blocking: true
    attach_function :rd_kafka_messages_count, [:pointer], :size_t
    attach_function :rd_kafka_messages_get, [:pointer, :size_t], :pointer
    attach_function :rd_kafka_messages_destroy, [:pointer], :void
    attach_function :rd_kafka_message_delivery_count, [:pointer], :int16

    # The by-coordinates acknowledge variant is the only one we bind: Ruby messages are copies
    # that do not retain the native rd_kafka_message_t pointer, so the message-pointer variants
    # (rd_kafka_share_acknowledge / rd_kafka_share_acknowledge_type) cannot be used safely here.
    attach_function :rd_kafka_share_acknowledge_offset, [:pointer, :string, :int32, :int64, :int], :int

    attach_function :rd_kafka_share_commit_sync, [:pointer, :int, :pointer], :pointer, blocking: true
    attach_function :rd_kafka_share_commit_async, [:pointer], :pointer

    callback :share_acknowledgement_commit_cb, [:pointer, :pointer, :int, :pointer], :void
    attach_function :rd_kafka_share_set_acknowledgement_commit_cb, [:pointer, :share_acknowledgement_commit_cb, :pointer], :pointer

    # Accessors for the rd_kafka_share_partition_offsets_list_t handed to the acknowledgement
    # commit callback. The list is owned by librdkafka for the duration of the callback: read it
    # inside the callback only, never destroy or retain it.
    attach_function :rd_kafka_share_partition_offsets_list_count, [:pointer], :size_t
    attach_function :rd_kafka_share_partition_offsets_list_get, [:pointer, :size_t], :pointer
    attach_function :rd_kafka_share_partition_offsets_partition, [:pointer], :pointer
    attach_function :rd_kafka_share_partition_offsets_offsets, [:pointer], :pointer
    attach_function :rd_kafka_share_partition_offsets_offsets_cnt, [:pointer], :size_t

    attach_function :rd_kafka_share_oauthbearer_set_token, [:pointer, :string, :int64, :string, :pointer, :size_t, :pointer, :size_t], :int
    attach_function :rd_kafka_share_oauthbearer_set_token_failure, [:pointer, :string], :int
  end
end
