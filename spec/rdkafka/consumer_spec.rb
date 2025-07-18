# frozen_string_literal: true

require "ostruct"
require 'securerandom'

describe Rdkafka::Consumer do
  let(:consumer) { rdkafka_consumer_config.consumer }
  let(:producer) { rdkafka_producer_config.producer }

  after { consumer.close }
  after { producer.close }

  describe '#name' do
    it { expect(consumer.name).to include('rdkafka#consumer-') }
  end

  describe 'consumer without auto-start' do
    let(:consumer) { rdkafka_consumer_config.consumer(native_kafka_auto_start: false) }

    it 'expect to be able to start it later and close' do
      consumer.start
      consumer.close
    end

    it 'expect to be able to close it without starting' do
      consumer.close
    end
  end

  describe "#subscribe, #unsubscribe and #subscription" do
    it "should subscribe, unsubscribe and return the subscription" do
      expect(consumer.subscription).to be_empty

      consumer.subscribe("consume_test_topic")

      expect(consumer.subscription).not_to be_empty
      expected_subscription = Rdkafka::Consumer::TopicPartitionList.new.tap do |list|
        list.add_topic("consume_test_topic")
      end
      expect(consumer.subscription).to eq expected_subscription

      consumer.unsubscribe

      expect(consumer.subscription).to be_empty
    end

    it "should raise an error when subscribing fails" do
      expect(Rdkafka::Bindings).to receive(:rd_kafka_subscribe).and_return(20)

      expect {
        consumer.subscribe("consume_test_topic")
      }.to raise_error(Rdkafka::RdkafkaError)
    end

    it "should raise an error when unsubscribing fails" do
      expect(Rdkafka::Bindings).to receive(:rd_kafka_unsubscribe).and_return(20)

      expect {
        consumer.unsubscribe
      }.to raise_error(Rdkafka::RdkafkaError)
    end

    it "should raise an error when fetching the subscription fails" do
      expect(Rdkafka::Bindings).to receive(:rd_kafka_subscription).and_return(20)

      expect {
        consumer.subscription
      }.to raise_error(Rdkafka::RdkafkaError)
    end

    context "when using consumer without the poll set" do
      let(:consumer) do
        config = rdkafka_consumer_config
        config.consumer_poll_set = false
        config.consumer
      end

      it "should subscribe, unsubscribe and return the subscription" do
        expect(consumer.subscription).to be_empty

        consumer.subscribe("consume_test_topic")

        expect(consumer.subscription).not_to be_empty
        expected_subscription = Rdkafka::Consumer::TopicPartitionList.new.tap do |list|
          list.add_topic("consume_test_topic")
        end
        expect(consumer.subscription).to eq expected_subscription

        consumer.unsubscribe

        expect(consumer.subscription).to be_empty
      end
    end
  end

  describe "#pause and #resume" do
    context "subscription" do
      let(:timeout) { 2000 }

      before { consumer.subscribe("consume_test_topic") }
      after { consumer.unsubscribe }

      it "should pause and then resume" do
        # 1. partitions are assigned
        wait_for_assignment(consumer)
        expect(consumer.assignment).not_to be_empty

        # 2. send a first message
        send_one_message

        # 3. ensure that message is successfully consumed
        records = consumer.poll(timeout)
        expect(records).not_to be_nil
        consumer.commit

        # 4. send a second message
        send_one_message

        # 5. pause the subscription
        tpl = Rdkafka::Consumer::TopicPartitionList.new
        tpl.add_topic("consume_test_topic", (0..2))
        consumer.pause(tpl)

        # 6. ensure that messages are not available
        records = consumer.poll(timeout)
        expect(records).to be_nil

        # 7. resume the subscription
        tpl = Rdkafka::Consumer::TopicPartitionList.new
        tpl.add_topic("consume_test_topic", (0..2))
        consumer.resume(tpl)

        # 8. ensure that message is successfully consumed
        records = consumer.poll(timeout)
        expect(records).not_to be_nil
      end
    end

    it "should raise when not TopicPartitionList" do
      expect { consumer.pause(true) }.to raise_error(TypeError)
      expect { consumer.resume(true) }.to raise_error(TypeError)
    end

    it "should raise an error when pausing fails" do
      list = Rdkafka::Consumer::TopicPartitionList.new.tap { |tpl| tpl.add_topic('topic', (0..1)) }

      expect(Rdkafka::Bindings).to receive(:rd_kafka_pause_partitions).and_return(20)
      expect {
        consumer.pause(list)
      }.to raise_error do |err|
        expect(err).to be_instance_of(Rdkafka::RdkafkaTopicPartitionListError)
        expect(err.topic_partition_list).to be
      end
    end

    it "should raise an error when resume fails" do
      expect(Rdkafka::Bindings).to receive(:rd_kafka_resume_partitions).and_return(20)
      expect {
        consumer.resume(Rdkafka::Consumer::TopicPartitionList.new)
      }.to raise_error Rdkafka::RdkafkaError
    end

    def send_one_message
      producer.produce(
        topic:     "consume_test_topic",
        payload:   "payload 1",
        key:       "key 1"
      ).wait
    end
  end

  describe "#seek" do
    let(:topic) { "it-#{SecureRandom.uuid}" }

    before do
      admin = rdkafka_producer_config.admin
      admin.create_topic(topic, 1, 1).wait
      admin.close
    end

    it "should raise an error when seeking fails" do
      fake_msg = OpenStruct.new(topic: topic, partition: 0, offset: 0)

      expect(Rdkafka::Bindings).to receive(:rd_kafka_seek).and_return(20)
      expect {
        consumer.seek(fake_msg)
      }.to raise_error Rdkafka::RdkafkaError
    end

    context "subscription" do
      let(:timeout) { 1000 }
      # Some specs here test the manual offset commit hence we want to ensure, that we have some
      # offsets in-memory that we can manually commit
      let(:consumer) { rdkafka_consumer_config('auto.commit.interval.ms': 60_000).consumer }

      before do
        consumer.subscribe(topic)

        # 1. partitions are assigned
        wait_for_assignment(consumer)
        expect(consumer.assignment).not_to be_empty

        # 2. eat unrelated messages
        while(consumer.poll(timeout)) do; end
      end
      after { consumer.unsubscribe }

      def send_one_message(val)
        producer.produce(
          topic:     topic,
          payload:   "payload #{val}",
          key:       "key 1",
          partition: 0
        ).wait
      end

      it "works when a partition is paused" do
        # 3. get reference message
        send_one_message(:a)
        message1 = consumer.poll(timeout)
        expect(message1&.payload).to eq "payload a"

        # 4. pause the subscription
        tpl = Rdkafka::Consumer::TopicPartitionList.new
        tpl.add_topic(topic, 1)
        consumer.pause(tpl)

        # 5. seek to previous message
        consumer.seek(message1)

        # 6. resume the subscription
        tpl = Rdkafka::Consumer::TopicPartitionList.new
        tpl.add_topic(topic, 1)
        consumer.resume(tpl)

        # 7. ensure same message is read again
        message2 = consumer.poll(timeout)

        # This is needed because `enable.auto.offset.store` is true but when running in CI that
        # is overloaded, offset store lags
        sleep(1)

        consumer.commit
        expect(message1.offset).to eq message2.offset
        expect(message1.payload).to eq message2.payload
      end

      it "allows skipping messages" do
        # 3. send messages
        send_one_message(:a)
        send_one_message(:b)
        send_one_message(:c)

        # 4. get reference message
        message = consumer.poll(timeout)
        expect(message&.payload).to eq "payload a"

        # 5. seek over one message
        fake_msg = message.dup
        fake_msg.instance_variable_set(:@offset, fake_msg.offset + 2)
        consumer.seek(fake_msg)

        # 6. ensure that only one message is available
        records = consumer.poll(timeout)
        expect(records&.payload).to eq "payload c"
        records = consumer.poll(timeout)
        expect(records).to be_nil
      end
    end
  end

  describe "#seek_by" do
    let(:consumer) { rdkafka_consumer_config('auto.commit.interval.ms': 60_000).consumer }
    let(:topic) { "it-#{SecureRandom.uuid}" }
    let(:partition) { 0 }
    let(:offset) { 0 }

    before do
      admin = rdkafka_producer_config.admin
      admin.create_topic(topic, 1, 1).wait
      admin.close
    end

    it "should raise an error when seeking fails" do
      expect(Rdkafka::Bindings).to receive(:rd_kafka_seek).and_return(20)
      expect {
        consumer.seek_by(topic, partition, offset)
      }.to raise_error Rdkafka::RdkafkaError
    end

    context "subscription" do
      let(:timeout) { 1000 }

      before do
        consumer.subscribe(topic)

        # 1. partitions are assigned
        wait_for_assignment(consumer)
        expect(consumer.assignment).not_to be_empty

        # 2. eat unrelated messages
        while(consumer.poll(timeout)) do; end
      end

      after { consumer.unsubscribe }

      def send_one_message(val)
        producer.produce(
          topic:     topic,
          payload:   "payload #{val}",
          key:       "key 1",
          partition: 0
        ).wait
      end

      it "works when a partition is paused" do
        # 3. get reference message
        send_one_message(:a)
        message1 = consumer.poll(timeout)
        expect(message1&.payload).to eq "payload a"

        # 4. pause the subscription
        tpl = Rdkafka::Consumer::TopicPartitionList.new
        tpl.add_topic(topic, 1)
        consumer.pause(tpl)

        # 5. seek by the previous message fields
        consumer.seek_by(message1.topic, message1.partition, message1.offset)

        # 6. resume the subscription
        tpl = Rdkafka::Consumer::TopicPartitionList.new
        tpl.add_topic(topic, 1)
        consumer.resume(tpl)

        # 7. ensure same message is read again
        message2 = consumer.poll(timeout)

        # This is needed because `enable.auto.offset.store` is true but when running in CI that
        # is overloaded, offset store lags
        sleep(2)

        consumer.commit
        expect(message1.offset).to eq message2.offset
        expect(message1.payload).to eq message2.payload
      end

      it "allows skipping messages" do
        # 3. send messages
        send_one_message(:a)
        send_one_message(:b)
        send_one_message(:c)

        # 4. get reference message
        message = consumer.poll(timeout)
        expect(message&.payload).to eq "payload a"

        # 5. seek over one message
        consumer.seek_by(message.topic, message.partition, message.offset + 2)

        # 6. ensure that only one message is available
        records = consumer.poll(timeout)
        expect(records&.payload).to eq "payload c"
        records = consumer.poll(timeout)
        expect(records).to be_nil
      end
    end
  end

  describe "#assign and #assignment" do
    it "should return an empty assignment if nothing is assigned" do
      expect(consumer.assignment).to be_empty
    end

    it "should only accept a topic partition list in assign" do
      expect {
        consumer.assign("list")
      }.to raise_error TypeError
    end

    it "should raise an error when assigning fails" do
      expect(Rdkafka::Bindings).to receive(:rd_kafka_assign).and_return(20)
      expect {
        consumer.assign(Rdkafka::Consumer::TopicPartitionList.new)
      }.to raise_error Rdkafka::RdkafkaError
    end

    it "should assign specific topic/partitions and return that assignment" do
      tpl = Rdkafka::Consumer::TopicPartitionList.new
      tpl.add_topic("consume_test_topic", (0..2))
      consumer.assign(tpl)

      assignment = consumer.assignment
      expect(assignment).not_to be_empty
      expect(assignment.to_h["consume_test_topic"].length).to eq 3
    end

    it "should return the assignment when subscribed" do
      # Make sure there's a message
      producer.produce(
        topic:     "consume_test_topic",
        payload:   "payload 1",
        key:       "key 1",
        partition: 0
      ).wait

      # Subscribe and poll until partitions are assigned
      consumer.subscribe("consume_test_topic")
      100.times do
        consumer.poll(100)
        break unless consumer.assignment.empty?
      end

      assignment = consumer.assignment
      expect(assignment).not_to be_empty
      expect(assignment.to_h["consume_test_topic"].length).to eq 3
    end

    it "should raise an error when getting assignment fails" do
      expect(Rdkafka::Bindings).to receive(:rd_kafka_assignment).and_return(20)
      expect {
        consumer.assignment
      }.to raise_error Rdkafka::RdkafkaError
    end
  end

  describe '#assignment_lost?' do
    it "should not return true as we do have an assignment" do
      consumer.subscribe("consume_test_topic")
      expected_subscription = Rdkafka::Consumer::TopicPartitionList.new.tap do |list|
        list.add_topic("consume_test_topic")
      end

      expect(consumer.assignment_lost?).to eq false
      consumer.unsubscribe
    end

    it "should not return true after voluntary unsubscribing" do
      consumer.subscribe("consume_test_topic")
      expected_subscription = Rdkafka::Consumer::TopicPartitionList.new.tap do |list|
        list.add_topic("consume_test_topic")
      end

      consumer.unsubscribe
      expect(consumer.assignment_lost?).to eq false
    end
  end

  describe "#close" do
    it "should close a consumer" do
      consumer.subscribe("consume_test_topic")
      100.times do |i|
        producer.produce(
          topic:     "consume_test_topic",
          payload:   "payload #{i}",
          key:       "key #{i}",
          partition: 0
        ).wait
      end
      consumer.close
      expect {
        consumer.poll(100)
      }.to raise_error(Rdkafka::ClosedConsumerError, /poll/)
    end

    context 'when there are outgoing operations in other threads' do
      it 'should wait and not crash' do
        times = []

        # Run a long running poll
        thread = Thread.new do
          times << Time.now
          consumer.subscribe("empty_test_topic")
          times << Time.now
          consumer.poll(1_000)
          times << Time.now
        end

        # Make sure it starts before we close
        sleep(0.1)
        consumer.close
        close_time = Time.now
        thread.join

        times.each { |op_time| expect(op_time).to be < close_time }
      end
    end
  end

  describe "#position, #commit, #committed and #store_offset" do
    # Make sure there are messages to work with
    let!(:report) do
      producer.produce(
        topic:     "consume_test_topic",
        payload:   "payload 1",
        key:       "key 1",
        partition: 0
      ).wait
    end

    let(:message) do
      wait_for_message(
        topic: "consume_test_topic",
        delivery_report: report,
        consumer: consumer
      )
    end

    describe "#position" do
      it "should only accept a topic partition list in position if not nil" do
        expect {
          consumer.position("list")
        }.to raise_error TypeError
      end
    end

    describe "#committed" do
      it "should only accept a topic partition list in commit if not nil" do
        expect {
          consumer.commit("list")
        }.to raise_error TypeError
      end

      it "should commit in sync mode" do
        expect {
          consumer.commit(nil, true)
        }.not_to raise_error
      end
    end

    context "with a committed consumer" do
      before :all do
        # Make sure there are some messages.
        handles = []
        producer = rdkafka_config.producer
        10.times do
          (0..2).each do |i|
            handles << producer.produce(
              topic:     "consume_test_topic",
              payload:   "payload 1",
              key:       "key 1",
              partition: i
            )
          end
        end
        handles.each(&:wait)
        producer.close
      end

      before do
        consumer.subscribe("consume_test_topic")
        wait_for_assignment(consumer)
        list = Rdkafka::Consumer::TopicPartitionList.new.tap do |list|
          list.add_topic_and_partitions_with_offsets("consume_test_topic", 0 => 1, 1 => 1, 2 => 1)
        end
        consumer.commit(list)
      end

      it "should commit a specific topic partion list" do
        list = Rdkafka::Consumer::TopicPartitionList.new.tap do |list|
          list.add_topic_and_partitions_with_offsets("consume_test_topic", 0 => 1, 1 => 2, 2 => 3)
        end
        consumer.commit(list)

        partitions = consumer.committed(list).to_h["consume_test_topic"]
        expect(partitions[0].offset).to eq 1
        expect(partitions[1].offset).to eq 2
        expect(partitions[2].offset).to eq 3
      end

      it "should raise an error when committing fails" do
        expect(Rdkafka::Bindings).to receive(:rd_kafka_commit).and_return(20)

        expect {
          consumer.commit
        }.to raise_error(Rdkafka::RdkafkaError)
      end

      describe "#committed" do
        it "should fetch the committed offsets for the current assignment" do
          partitions = consumer.committed.to_h["consume_test_topic"]
          expect(partitions).not_to be_nil
          expect(partitions[0].offset).to eq 1
        end

        it "should fetch the committed offsets for a specified topic partition list" do
          list = Rdkafka::Consumer::TopicPartitionList.new.tap do |list|
            list.add_topic("consume_test_topic", [0, 1, 2])
          end
          partitions = consumer.committed(list).to_h["consume_test_topic"]
          expect(partitions).not_to be_nil
          expect(partitions[0].offset).to eq 1
          expect(partitions[1].offset).to eq 1
          expect(partitions[2].offset).to eq 1
        end

        it "should raise an error when getting committed fails" do
          expect(Rdkafka::Bindings).to receive(:rd_kafka_committed).and_return(20)
          list = Rdkafka::Consumer::TopicPartitionList.new.tap do |list|
            list.add_topic("consume_test_topic", [0, 1, 2])
          end
          expect {
            consumer.committed(list)
          }.to raise_error Rdkafka::RdkafkaError
        end
      end

      describe "#store_offset" do
        let(:consumer) { rdkafka_consumer_config('enable.auto.offset.store': false).consumer }
        let(:metadata) { SecureRandom.uuid }
        let(:group_id) { SecureRandom.uuid }
        let(:base_config) do
          {
            'group.id': group_id,
            'enable.auto.offset.store': false,
            'enable.auto.commit': false
          }
        end

        before do
          @new_consumer = rdkafka_consumer_config(base_config).consumer
          @new_consumer.subscribe("consume_test_topic")
          wait_for_assignment(@new_consumer)
        end

        after do
          @new_consumer.close
        end

        it "should store the offset for a message" do
          @new_consumer.store_offset(message)
          @new_consumer.commit

          # TODO use position here, should be at offset

          list = Rdkafka::Consumer::TopicPartitionList.new.tap do |list|
            list.add_topic("consume_test_topic", [0, 1, 2])
          end
          partitions = @new_consumer.committed(list).to_h["consume_test_topic"]
          expect(partitions).not_to be_nil
          expect(partitions[message.partition].offset).to eq(message.offset + 1)
        end

        it "should store the offset for a message with metadata" do
          @new_consumer.store_offset(message, metadata)
          @new_consumer.commit
          @new_consumer.close

          meta_consumer = rdkafka_consumer_config(base_config).consumer
          meta_consumer.subscribe("consume_test_topic")
          wait_for_assignment(meta_consumer)
          meta_consumer.poll(1_000)
          expect(meta_consumer.committed.to_h[message.topic][message.partition].metadata).to eq(metadata)
          meta_consumer.close
        end

        it "should raise an error with invalid input" do
          allow(message).to receive(:partition).and_return(9999)
          expect {
            @new_consumer.store_offset(message)
          }.to raise_error Rdkafka::RdkafkaError
        end

        describe "#position" do
          it "should fetch the positions for the current assignment" do
            consumer.store_offset(message)

            partitions = consumer.position.to_h["consume_test_topic"]
            expect(partitions).not_to be_nil
            expect(partitions[0].offset).to eq message.offset + 1
          end

          it "should fetch the positions for a specified assignment" do
            consumer.store_offset(message)

            list = Rdkafka::Consumer::TopicPartitionList.new.tap do |list|
              list.add_topic_and_partitions_with_offsets("consume_test_topic", 0 => nil, 1 => nil, 2 => nil)
            end
            partitions = consumer.position(list).to_h["consume_test_topic"]
            expect(partitions).not_to be_nil
            expect(partitions[0].offset).to eq message.offset + 1
          end

          it "should raise an error when getting the position fails" do
            expect(Rdkafka::Bindings).to receive(:rd_kafka_position).and_return(20)

            expect {
              consumer.position
            }.to raise_error(Rdkafka::RdkafkaError)
          end
        end

        context "when trying to use with enable.auto.offset.store set to true" do
          let(:consumer) { rdkafka_consumer_config('enable.auto.offset.store': true).consumer }

          it "expect to raise invalid configuration error" do
            expect { consumer.store_offset(message) }.to raise_error(Rdkafka::RdkafkaError, /invalid_arg/)
          end
        end
      end
    end
  end

  describe "#query_watermark_offsets" do
    it "should return the watermark offsets" do
      # Make sure there's a message
      producer.produce(
        topic:     "watermarks_test_topic",
        payload:   "payload 1",
        key:       "key 1",
        partition: 0
      ).wait

      low, high = consumer.query_watermark_offsets("watermarks_test_topic", 0, 5000)
      expect(low).to eq 0
      expect(high).to be > 0
    end

    it "should raise an error when querying offsets fails" do
      expect(Rdkafka::Bindings).to receive(:rd_kafka_query_watermark_offsets).and_return(20)
      expect {
        consumer.query_watermark_offsets("consume_test_topic", 0, 5000)
      }.to raise_error Rdkafka::RdkafkaError
    end
  end

  describe "#lag" do
    let(:consumer) { rdkafka_consumer_config(:"enable.partition.eof" => true).consumer }

    it "should calculate the consumer lag" do
      # Make sure there's a message in every partition and
      # wait for the message to make sure everything is committed.
      (0..2).each do |i|
        producer.produce(
          topic:     "consume_test_topic",
          key:       "key lag #{i}",
          partition: i
        ).wait
      end

      # Consume to the end
      consumer.subscribe("consume_test_topic")
      eof_count = 0
      loop do
        begin
          consumer.poll(100)
        rescue Rdkafka::RdkafkaError => error
          if error.is_partition_eof?
            eof_count += 1
          end
          break if eof_count == 3
        end
      end

      # Commit
      consumer.commit

      # Create list to fetch lag for. TODO creating the list will not be necessary
      # after committed uses the subscription.
      list = consumer.committed(Rdkafka::Consumer::TopicPartitionList.new.tap do |l|
        l.add_topic("consume_test_topic", (0..2))
      end)

      # Lag should be 0 now
      lag = consumer.lag(list)
      expected_lag = {
        "consume_test_topic" => {
          0 => 0,
          1 => 0,
          2 => 0
        }
      }
      expect(lag).to eq(expected_lag)

      # Produce message on every topic again
      (0..2).each do |i|
        producer.produce(
          topic:     "consume_test_topic",
          key:       "key lag #{i}",
          partition: i
        ).wait
      end

      # Lag should be 1 now
      lag = consumer.lag(list)
      expected_lag = {
        "consume_test_topic" => {
          0 => 1,
          1 => 1,
          2 => 1
        }
      }
      expect(lag).to eq(expected_lag)
    end

    it "returns nil if there are no messages on the topic" do
      list = consumer.committed(Rdkafka::Consumer::TopicPartitionList.new.tap do |l|
        l.add_topic("consume_test_topic", (0..2))
      end)

      lag = consumer.lag(list)
      expected_lag = {
        "consume_test_topic" => {}
      }
      expect(lag).to eq(expected_lag)
    end
  end

  describe "#cluster_id" do
    it 'should return the current ClusterId' do
      consumer.subscribe("consume_test_topic")
      wait_for_assignment(consumer)
      expect(consumer.cluster_id).not_to be_empty
    end
  end

  describe "#member_id" do
    it 'should return the current MemberId' do
      consumer.subscribe("consume_test_topic")
      wait_for_assignment(consumer)
      expect(consumer.member_id).to start_with('rdkafka-')
    end
  end

  describe "#poll" do
    it "should return nil if there is no subscription" do
      expect(consumer.poll(1000)).to be_nil
    end

    it "should return nil if there are no messages" do
      consumer.subscribe("empty_test_topic")
      expect(consumer.poll(1000)).to be_nil
    end

    it "should return a message if there is one" do
      topic = "it-#{SecureRandom.uuid}"

      producer.produce(
        topic:     topic,
        payload:   "payload 1",
        key:       "key 1"
      ).wait
      consumer.subscribe(topic)
      message = consumer.each {|m| break m}

      expect(message).to be_a Rdkafka::Consumer::Message
      expect(message.payload).to eq('payload 1')
      expect(message.key).to eq('key 1')
    end

    it "should raise an error when polling fails" do
      message = Rdkafka::Bindings::Message.new.tap do |message|
        message[:err] = 20
      end
      message_pointer = message.to_ptr
      expect(Rdkafka::Bindings).to receive(:rd_kafka_consumer_poll).and_return(message_pointer)
      expect(Rdkafka::Bindings).to receive(:rd_kafka_message_destroy).with(message_pointer)
      expect {
        consumer.poll(100)
      }.to raise_error Rdkafka::RdkafkaError
    end

    it "expect to raise error when polling non-existing topic" do
      missing_topic = SecureRandom.uuid
      consumer.subscribe(missing_topic)

      expect {
        consumer.poll(1_000)
      }.to raise_error Rdkafka::RdkafkaError, /Subscribed topic not available: #{missing_topic}/
    end
  end

  describe "#poll with headers" do
    it "should return message with headers using string keys (when produced with symbol keys)" do
      report = producer.produce(
        topic:     "consume_test_topic",
        key:       "key headers",
        headers:   { foo: 'bar' }
      ).wait

      message = wait_for_message(topic: "consume_test_topic", consumer: consumer, delivery_report: report)
      expect(message).to be
      expect(message.key).to eq('key headers')
      expect(message.headers).to include('foo' => 'bar')
    end

    it "should return message with headers using string keys (when produced with string keys)" do
      report = producer.produce(
        topic:     "consume_test_topic",
        key:       "key headers",
        headers:   { 'foo' => 'bar' }
      ).wait

      message = wait_for_message(topic: "consume_test_topic", consumer: consumer, delivery_report: report)
      expect(message).to be
      expect(message.key).to eq('key headers')
      expect(message.headers).to include('foo' => 'bar')
    end

    it "should return message with no headers" do
      report = producer.produce(
        topic:     "consume_test_topic",
        key:       "key no headers",
        headers:   nil
      ).wait

      message = wait_for_message(topic: "consume_test_topic", consumer: consumer, delivery_report: report)
      expect(message).to be
      expect(message.key).to eq('key no headers')
      expect(message.headers).to be_empty
    end

    it "should raise an error when message headers aren't readable" do
      expect(Rdkafka::Bindings).to receive(:rd_kafka_message_headers).with(any_args) { 1 }

      report = producer.produce(
        topic:     "consume_test_topic",
        key:       "key err headers",
        headers:   nil
      ).wait

      expect {
        wait_for_message(topic: "consume_test_topic", consumer: consumer, delivery_report: report)
      }.to raise_error do |err|
        expect(err).to be_instance_of(Rdkafka::RdkafkaError)
        expect(err.message).to start_with("Error reading message headers")
      end
    end

    it "should raise an error when the first message header aren't readable" do
      expect(Rdkafka::Bindings).to receive(:rd_kafka_header_get_all).with(any_args) { 1 }

      report = producer.produce(
        topic:     "consume_test_topic",
        key:       "key err headers",
        headers:   { foo: 'bar' }
      ).wait

      expect {
        wait_for_message(topic: "consume_test_topic", consumer: consumer, delivery_report: report)
      }.to raise_error do |err|
        expect(err).to be_instance_of(Rdkafka::RdkafkaError)
        expect(err.message).to start_with("Error reading a message header at index 0")
      end
    end
  end

  describe "#each" do
    it "should yield messages" do
      handles = []
      10.times do
        handles << producer.produce(
          topic:     "consume_test_topic",
          payload:   "payload 1",
          key:       "key 1",
          partition: 0
        )
      end
      handles.each(&:wait)

      consumer.subscribe("consume_test_topic")
      # Check the first 10 messages. Then close the consumer, which
      # should break the each loop.
      consumer.each_with_index do |message, i|
        expect(message).to be_a Rdkafka::Consumer::Message
        break if i == 10
      end
      consumer.close
    end
  end

  describe "#each_batch" do
    it 'expect to raise an error' do
      expect do
        consumer.each_batch {}
      end.to raise_error(NotImplementedError)
    end
  end

  describe "#offsets_for_times" do
    it "should raise when not TopicPartitionList" do
      expect { consumer.offsets_for_times([]) }.to raise_error(TypeError)
    end

    it "should raise an error when offsets_for_times fails" do
      tpl = Rdkafka::Consumer::TopicPartitionList.new

      expect(Rdkafka::Bindings).to receive(:rd_kafka_offsets_for_times).and_return(7)

      expect { consumer.offsets_for_times(tpl) }.to raise_error(Rdkafka::RdkafkaError)
    end

    context "when subscribed" do
      let(:timeout) { 1000 }

      before do
        consumer.subscribe("consume_test_topic")

        # 1. partitions are assigned
        wait_for_assignment(consumer)
        expect(consumer.assignment).not_to be_empty

        # 2. eat unrelated messages
        while(consumer.poll(timeout)) do; end
      end

      after { consumer.unsubscribe }

      def send_one_message(val)
        producer.produce(
          topic:     "consume_test_topic",
          payload:   "payload #{val}",
          key:       "key 0",
          partition: 0
        ).wait
      end

      it "returns a TopicParticionList with updated offsets" do
        send_one_message("a")
        send_one_message("b")
        send_one_message("c")

        consumer.poll(timeout)
        message = consumer.poll(timeout)
        consumer.poll(timeout)

        tpl = Rdkafka::Consumer::TopicPartitionList.new.tap do |list|
          list.add_topic_and_partitions_with_offsets(
            "consume_test_topic",
            [
              [0, message.timestamp]
            ]
          )
        end

        tpl_response = consumer.offsets_for_times(tpl)

        expect(tpl_response.to_h["consume_test_topic"][0].offset).to eq message.offset
      end
    end
  end

  # Only relevant in case of a consumer with separate queues
  describe '#events_poll' do
    let(:stats) { [] }

    before { Rdkafka::Config.statistics_callback = ->(published) { stats << published } }

    after { Rdkafka::Config.statistics_callback = nil }

    let(:consumer) do
      config = rdkafka_consumer_config('statistics.interval.ms': 500)
      config.consumer_poll_set = false
      config.consumer
    end

    it "expect to run events_poll, operate and propagate stats on events_poll and not poll" do
      consumer.subscribe("consume_test_topic")
      consumer.poll(1_000)
      expect(stats).to be_empty
      consumer.events_poll(-1)
      expect(stats).not_to be_empty
    end
  end

  describe '#consumer_group_metadata_pointer' do
    let(:pointer) { consumer.consumer_group_metadata_pointer }

    after { Rdkafka::Bindings.rd_kafka_consumer_group_metadata_destroy(pointer) }

    it 'expect to return a pointer' do
      expect(pointer).to be_a(FFI::Pointer)
    end
  end

  describe "a rebalance listener" do
    let(:consumer) do
      config = rdkafka_consumer_config
      config.consumer_rebalance_listener = listener
      config.consumer
    end

    context "with a working listener" do
      let(:listener) do
        Struct.new(:queue) do
          def on_partitions_assigned(list)
            collect(:assign, list)
          end

          def on_partitions_revoked(list)
            collect(:revoke, list)
          end

          def collect(name, list)
            partitions = list.to_h.map { |key, values| [key, values.map(&:partition)] }.flatten
            queue << ([name] + partitions)
          end
        end.new([])
      end

      it "should get notifications" do
        notify_listener(listener)

        expect(listener.queue).to eq([
          [:assign, "consume_test_topic", 0, 1, 2],
          [:revoke, "consume_test_topic", 0, 1, 2]
        ])
      end
    end

    context "with a broken listener" do
      let(:listener) do
        Struct.new(:queue) do
          def on_partitions_assigned(list)
            queue << :assigned
            raise 'boom'
          end

          def on_partitions_revoked(list)
            queue << :revoked
            raise 'boom'
          end
        end.new([])
      end

      it 'should handle callback exceptions' do
        notify_listener(listener)

        expect(listener.queue).to eq([:assigned, :revoked])
      end
    end
  end

  context "methods that should not be called after a consumer has been closed" do
    before do
      consumer.close
    end

    # Affected methods and a non-invalid set of parameters for the method
    {
        :subscribe               => [ nil ],
        :unsubscribe             => nil,
        :pause                   => [ nil ],
        :resume                  => [ nil ],
        :subscription            => nil,
        :assign                  => [ nil ],
        :assignment              => nil,
        :committed               => [],
        :query_watermark_offsets => [ nil, nil ],
        :assignment_lost?        => []
    }.each do |method, args|
      it "raises an exception if #{method} is called" do
        expect {
          if args.nil?
            consumer.public_send(method)
          else
            consumer.public_send(method, *args)
          end
        }.to raise_exception(Rdkafka::ClosedConsumerError, /#{method.to_s}/)
      end
    end
  end

  it "provides a finalizer that closes the native kafka client" do
    expect(consumer.closed?).to eq(false)

    consumer.finalizer.call("some-ignored-object-id")

    expect(consumer.closed?).to eq(true)
  end

  context "when the rebalance protocol is cooperative" do
    let(:consumer) do
      config = rdkafka_consumer_config(
        {
          :"partition.assignment.strategy" => "cooperative-sticky",
          :"debug" => "consumer",
        }
      )
      config.consumer_rebalance_listener = listener
      config.consumer
    end

    let(:listener) do
      Struct.new(:queue) do
        def on_partitions_assigned(list)
          collect(:assign, list)
        end

        def on_partitions_revoked(list)
          collect(:revoke, list)
        end

        def collect(name, list)
          partitions = list.to_h.map { |key, values| [key, values.map(&:partition)] }.flatten
          queue << ([name] + partitions)
        end
      end.new([])
    end

    it "should be able to assign and unassign partitions using the cooperative partition assignment APIs" do
      notify_listener(listener) do
        handles = []
        10.times do
          handles << producer.produce(
            topic:     "consume_test_topic",
            payload:   "payload 1",
            key:       "key 1",
            partition: 0
          )
        end
        handles.each(&:wait)

        consumer.subscribe("consume_test_topic")
        # Check the first 10 messages. Then close the consumer, which
        # should break the each loop.
        consumer.each_with_index do |message, i|
          expect(message).to be_a Rdkafka::Consumer::Message
          break if i == 10
        end
      end

      expect(listener.queue).to eq([
        [:assign, "consume_test_topic", 0, 1, 2],
        [:revoke, "consume_test_topic", 0, 1, 2]
      ])
    end
  end

  describe '#oauthbearer_set_token' do
    context 'when sasl not configured' do
      it 'should return RD_KAFKA_RESP_ERR__STATE' do
        response = consumer.oauthbearer_set_token(
          token: "foo",
          lifetime_ms: Time.now.to_i*1000 + 900 * 1000,
          principal_name: "kafka-cluster"
        )
        expect(response).to eq(Rdkafka::Bindings::RD_KAFKA_RESP_ERR__STATE)
      end
    end

    context 'when sasl configured' do
      before do
        $consumer_sasl = rdkafka_producer_config(
          "security.protocol": "sasl_ssl",
          "sasl.mechanisms": 'OAUTHBEARER'
        ).consumer
      end

      after do
        $consumer_sasl.close
      end

      it 'should succeed' do

        response = $consumer_sasl.oauthbearer_set_token(
          token: "foo",
          lifetime_ms: Time.now.to_i*1000 + 900 * 1000,
          principal_name: "kafka-cluster"
        )
        expect(response).to eq(0)
      end
    end
  end

  describe "when reaching eof on a topic and eof reporting enabled" do
    let(:consumer) { rdkafka_consumer_config(:"enable.partition.eof" => true).consumer }

    it "should return proper details" do
      (0..2).each do |i|
        producer.produce(
          topic:     "consume_test_topic",
          key:       "key lag #{i}",
          partition: i
        ).wait
      end

      # Consume to the end
      consumer.subscribe("consume_test_topic")
      eof_count = 0
      eof_error = nil

      loop do
        begin
          consumer.poll(100)
        rescue Rdkafka::RdkafkaError => error
          if error.is_partition_eof?
            eof_error = error
          end
          break if eof_error
        end
      end

      expect(eof_error.code).to eq(:partition_eof)
    end
  end
end
