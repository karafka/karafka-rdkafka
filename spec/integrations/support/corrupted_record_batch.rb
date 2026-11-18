# frozen_string_literal: true

# A record batch (magic 2) with one record whose headers claim one header with a 100 bytes long
# key but hold only one byte of it, so a consumer cannot build a message from it. The CRC stays 0:
# consumers check it only with `check.crcs`. Append it with `MockCluster#produce_record_batch`.
module CorruptedRecordBatch
  class << self
    # @return [String] encoded record batch
    def build
      payload = "corrupted"
      headers = varint(1) + varint(100) + "k"
      record = [0].pack("c") + varint(0) + varint(0) + varint(-1) + varint(payload.bytesize) +
        payload + headers
      records = varint(record.bytesize) + record

      timestamp = (Time.now.to_f * 1_000).to_i
      # Attributes, last offset delta, first and max timestamp, producer id and epoch, base
      # sequence and record count
      after_crc = [0, 0, timestamp, timestamp, -1, -1, -1, 1].pack("s>l>q>q>q>s>l>l>") + records
      # Partition leader epoch, magic and CRC
      after_length = [-1, 2, 0].pack("l>cL>") + after_crc

      [0, after_length.bytesize].pack("q>l>") + after_length
    end

    private

    # Zigzag varint, as in Kafka records
    #
    # @param value [Integer]
    # @return [String]
    def varint(value)
      value = (value << 1) ^ (value >> 63)
      bytes = []

      while value >= 0x80
        bytes << ((value & 0x7f) | 0x80)
        value >>= 7
      end

      (bytes << value).pack("C*")
    end
  end
end
