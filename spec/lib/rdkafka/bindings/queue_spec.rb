# frozen_string_literal: true

require 'spec_helper'

describe 'Rdkafka::Bindings queue functions' do
  let(:config) do
    rdkafka_consumer_config.tap do |config|
      config.consumer_poll_set = false
    end
  end
  let(:consumer) { config.consumer }
  let(:producer) { rdkafka_producer_config.producer }

  after { consumer.close }
  after { producer.close }

  describe 'rd_kafka_queue_get_consumer' do
    it 'should return a valid queue pointer' do
      queue_ptr = consumer.instance_variable_get(:@native_kafka).with_inner do |inner|
        Rdkafka::Bindings.rd_kafka_queue_get_consumer(inner)
      end

      expect(queue_ptr).to be_a(FFI::Pointer)
      expect(queue_ptr.null?).to be false

      Rdkafka::Bindings.rd_kafka_queue_destroy(queue_ptr)
    end
  end

  describe 'rd_kafka_queue_get_main' do
    it 'should return a valid queue pointer for producer' do
      queue_ptr = producer.instance_variable_get(:@native_kafka).with_inner do |inner|
        Rdkafka::Bindings.rd_kafka_queue_get_main(inner)
      end

      expect(queue_ptr).to be_a(FFI::Pointer)
      expect(queue_ptr.null?).to be false

      Rdkafka::Bindings.rd_kafka_queue_destroy(queue_ptr)
    end
  end

  describe 'rd_kafka_queue_io_event_enable' do
    let(:read_fd) { @pipe[0] }
    let(:write_fd) { @pipe[1] }

    before do
      @pipe = IO.pipe
      # Set write end to non-blocking as required by librdkafka
      write_fd.fcntl(Fcntl::F_SETFL, Fcntl::O_NONBLOCK)
    end

    after do
      read_fd.close rescue nil
      write_fd.close rescue nil
    end

    it 'should enable IO events with a payload' do
      queue_ptr = consumer.instance_variable_get(:@native_kafka).with_inner do |inner|
        Rdkafka::Bindings.rd_kafka_queue_get_consumer(inner)
      end

      begin
        payload = "X"
        payload_ptr = FFI::MemoryPointer.from_string(payload)

        expect {
          Rdkafka::Bindings.rd_kafka_queue_io_event_enable(
            queue_ptr,
            write_fd.fileno,
            payload_ptr,
            payload.bytesize
          )
        }.not_to raise_error
      ensure
        Rdkafka::Bindings.rd_kafka_queue_destroy(queue_ptr)
      end
    end

    it 'should disable IO events when fd is -1' do
      queue_ptr = consumer.instance_variable_get(:@native_kafka).with_inner do |inner|
        Rdkafka::Bindings.rd_kafka_queue_get_consumer(inner)
      end

      begin
        # Enable first
        payload = "X"
        payload_ptr = FFI::MemoryPointer.from_string(payload)
        Rdkafka::Bindings.rd_kafka_queue_io_event_enable(
          queue_ptr,
          write_fd.fileno,
          payload_ptr,
          payload.bytesize
        )

        # Then disable
        expect {
          Rdkafka::Bindings.rd_kafka_queue_io_event_enable(
            queue_ptr,
            -1,
            FFI::Pointer::NULL,
            0
          )
        }.not_to raise_error
      ensure
        Rdkafka::Bindings.rd_kafka_queue_destroy(queue_ptr)
      end
    end

    it 'should work with empty payload' do
      queue_ptr = consumer.instance_variable_get(:@native_kafka).with_inner do |inner|
        Rdkafka::Bindings.rd_kafka_queue_get_consumer(inner)
      end

      begin
        expect {
          Rdkafka::Bindings.rd_kafka_queue_io_event_enable(
            queue_ptr,
            write_fd.fileno,
            FFI::Pointer::NULL,
            0
          )
        }.not_to raise_error
      ensure
        Rdkafka::Bindings.rd_kafka_queue_destroy(queue_ptr)
      end
    end
  end

  describe 'Consumer API integration' do
    let(:read_fd) { @pipe[0] }
    let(:write_fd) { @pipe[1] }

    before do
      @pipe = IO.pipe
      # Set write end to non-blocking as required by librdkafka
      write_fd.fcntl(Fcntl::F_SETFL, Fcntl::O_NONBLOCK)
    end

    after do
      read_fd.close rescue nil
      write_fd.close rescue nil
    end

    it 'should enable IO events using consumer API' do
      queue_ptr = consumer.consumer_queue_pointer

      expect {
        consumer.enable_queue_io_event(
          queue_ptr: queue_ptr,
          fd: write_fd.fileno
        )
      }.not_to raise_error
    end

    it 'should write to fd when message becomes available' do
      consumer.subscribe(TestTopics.consume_test_topic)
      wait_for_assignment(consumer)

      queue_ptr = consumer.consumer_queue_pointer

      # Enable IO events using Consumer API
      consumer.enable_queue_io_event(
        queue_ptr: queue_ptr,
        fd: write_fd.fileno
      )

      # Produce multiple messages
      3.times do |i|
        producer.produce(
          topic: TestTopics.consume_test_topic,
          payload: "test payload #{i}",
          partition: 0
        ).wait
      end

      # Should receive ONE notification on the pipe (queue went from empty to non-empty)
      ready = IO.select([read_fd], nil, nil, 5)
      expect(ready).not_to be_nil

      # Read the notification payload (hardcoded "x")
      notification = read_fd.read_nonblock(1)
      expect(notification).to eq("x")

      # Drain all messages from the queue
      messages = []
      loop do
        message = consumer.poll(100)
        break unless message
        messages << message
      end

      # Should have received all 3 messages
      expect(messages.length).to eq(3)
      messages.each_with_index do |message, i|
        expect(message.payload).to eq("test payload #{i}")
      end

      # Verify no additional notification was written (fd should be empty)
      expect {
        read_fd.read_nonblock(1)
      }.to raise_error(IO::WaitReadable)
    end
  end

  describe 'integration with IO.select' do
    it 'should enable event-driven consumption pattern' do
      read_fd, write_fd = IO.pipe
      write_fd.fcntl(Fcntl::F_SETFL, Fcntl::O_NONBLOCK)

      begin
        queue_ptr = consumer.consumer_queue_pointer

        consumer.enable_queue_io_event(
          queue_ptr: queue_ptr,
          fd: write_fd.fileno
        )

        consumer.subscribe(TestTopics.consume_test_topic)
        wait_for_assignment(consumer)

        # Produce multiple messages
        3.times do |i|
          producer.produce(
            topic: TestTopics.consume_test_topic,
            payload: "message #{i}",
            partition: 0
          ).wait
        end

        messages_received = 0

        # Wait for notification (should only get ONE for all 3 messages)
        ready = IO.select([read_fd], nil, nil, 5)
        expect(ready).not_to be_nil

        # Clear notification
        read_fd.read_nonblock(1) rescue nil

        # IMPORTANT: Drain the entire queue
        loop do
          message = consumer.poll(100)
          break unless message
          messages_received += 1
          expect(message.payload).to match(/message \d/)
        end

        # Should have received all 3 messages from one notification
        expect(messages_received).to eq(3)

        # Verify no additional notification (fd should be empty now)
        expect {
          read_fd.read_nonblock(1)
        }.to raise_error(IO::WaitReadable)

        # Queue cleanup happens automatically in Consumer#close
      ensure
        read_fd.close rescue nil
        write_fd.close rescue nil
      end
    end

    it 'should require draining queue to receive new notifications' do
      read_fd, write_fd = IO.pipe
      write_fd.fcntl(Fcntl::F_SETFL, Fcntl::O_NONBLOCK)

      begin
        queue_ptr = consumer.consumer_queue_pointer

        consumer.enable_queue_io_event(
          queue_ptr: queue_ptr,
          fd: write_fd.fileno
        )

        consumer.subscribe(TestTopics.consume_test_topic)
        wait_for_assignment(consumer)

        # Produce 2 messages
        2.times do |i|
          producer.produce(
            topic: TestTopics.consume_test_topic,
            payload: "first batch #{i}",
            partition: 0
          ).wait
        end

        # Get notification for first batch
        ready = IO.select([read_fd], nil, nil, 5)
        expect(ready).not_to be_nil
        read_fd.read_nonblock(1) rescue nil

        # Poll only ONE message (don't drain)
        message = consumer.poll(100)
        expect(message).not_to be_nil
        expect(message.payload).to eq("first batch 0")

        # Produce more messages while queue is non-empty
        2.times do |i|
          producer.produce(
            topic: TestTopics.consume_test_topic,
            payload: "second batch #{i}",
            partition: 0
          ).wait
        end

        # Should NOT get a new notification because queue never became empty
        ready = IO.select([read_fd], nil, nil, 1)
        expect(ready).to be_nil

        # Now drain the entire queue
        messages = []
        loop do
          message = consumer.poll(100)
          break unless message
          messages << message
        end

        # Should have 3 more messages (1 from first batch + 2 from second batch)
        expect(messages.length).to eq(3)

        # Now produce new messages after draining
        producer.produce(
          topic: TestTopics.consume_test_topic,
          payload: "third batch",
          partition: 0
        ).wait

        # Should get NEW notification because queue was empty
        ready = IO.select([read_fd], nil, nil, 5)
        expect(ready).not_to be_nil
        read_fd.read_nonblock(1) rescue nil

        message = consumer.poll(100)
        expect(message).not_to be_nil
        expect(message.payload).to eq("third batch")
      ensure
        read_fd.close rescue nil
        write_fd.close rescue nil
      end
    end

    it 'should handle both consumer and main queues with IO events' do
      consumer_read_fd, consumer_write_fd = IO.pipe
      main_read_fd, main_write_fd = IO.pipe
      consumer_write_fd.fcntl(Fcntl::F_SETFL, Fcntl::O_NONBLOCK)
      main_write_fd.fcntl(Fcntl::F_SETFL, Fcntl::O_NONBLOCK)

      begin
        # Get both queues using Consumer API
        consumer_queue_ptr = consumer.consumer_queue_pointer
        main_queue_ptr = consumer.main_queue_pointer

        # Enable IO events on both queues using Consumer API
        consumer.enable_queue_io_event(
          queue_ptr: consumer_queue_ptr,
          fd: consumer_write_fd.fileno
        )

        consumer.enable_queue_io_event(
          queue_ptr: main_queue_ptr,
          fd: main_write_fd.fileno
        )

        consumer.subscribe(TestTopics.consume_test_topic)
        wait_for_assignment(consumer)

        # Produce a message
        producer.produce(
          topic: TestTopics.consume_test_topic,
          payload: "test",
          partition: 0
        ).wait

        # Wait for either queue to have data
        ready = IO.select([consumer_read_fd, main_read_fd], nil, nil, 5)
        expect(ready).not_to be_nil

        # Queue cleanup happens automatically in Consumer#close
      ensure
        consumer_read_fd.close rescue nil
        consumer_write_fd.close rescue nil
        main_read_fd.close rescue nil
        main_write_fd.close rescue nil
      end
    end
  end
end
