# frozen_string_literal: true

RSpec.describe "statistics.unassigned.include config" do
  let(:stats) { [] }

  before do
    Rdkafka::Config.statistics_callback = ->(published) { stats << published }
  end

  after do
    Rdkafka::Config.statistics_callback = nil
  end

  def wait_for_stats(count = 1, timeout: 5)
    (timeout * 20).times do
      break if stats.size >= count
      sleep 0.05
    end
  end

  context "when set to false for a producer" do
    let(:producer) do
      rdkafka_producer_config(
        "statistics.interval.ms": 100,
        "statistics.unassigned.include": false
      ).producer
    end

    after { producer.close }

    it "accepts the config without error" do
      expect(producer).to be_a(Rdkafka::Producer)
    end

    it "still emits statistics with root-level keys" do
      producer.produce(topic: "test", payload: "test").wait
      wait_for_stats

      stat = stats.last
      expect(stat).to have_key("name")
      expect(stat).to have_key("type")
      expect(stat).to have_key("ts")
      expect(stat).to have_key("brokers")
      expect(stat).to have_key("topics")
      expect(stat["type"]).to eq("producer")
    end

    it "emits empty topics section" do
      producer.produce(topic: "test", payload: "test").wait
      wait_for_stats

      expect(stats.last["topics"]).to be_empty
    end

    it "preserves root-level aggregate totals" do
      producer.produce(topic: "test", payload: "test").wait
      wait_for_stats(2)

      stat = stats.last
      expect(stat["txmsgs"]).to be_a(Integer)
      expect(stat["rxmsgs"]).to be_a(Integer)
    end

    it "preserves broker statistics" do
      producer.produce(topic: "test", payload: "test").wait
      wait_for_stats

      brokers = stats.last["brokers"]
      expect(brokers).not_to be_empty
      broker = brokers.values.first
      expect(broker).to have_key("tx")
      expect(broker).to have_key("rx")
    end
  end

  context "when set to false for a consumer" do
    let(:consumer) do
      rdkafka_consumer_config(
        "statistics.interval.ms": 100,
        "statistics.unassigned.include": false
      ).consumer
    end

    after { consumer.close }

    def poll_until_stats(consumer, count = 1, timeout: 5)
      (timeout * 20).times do
        break if stats.size >= count
        consumer.poll(50)
      end
    end

    it "accepts the config without error" do
      expect(consumer).to be_a(Rdkafka::Consumer)
    end

    it "still emits statistics with root-level keys" do
      consumer.subscribe("test")
      poll_until_stats(consumer)

      stat = stats.last
      expect(stat).to have_key("name")
      expect(stat).to have_key("type")
      expect(stat).to have_key("brokers")
      expect(stat["type"]).to eq("consumer")
    end

    it "preserves consumer group statistics" do
      consumer.subscribe("test")
      poll_until_stats(consumer, 2)

      stat = stats.select { |s| s["cgrp"] }.last
      expect(stat).not_to be_nil
      expect(stat["cgrp"]).to have_key("state")
    end

    it "preserves topic data for consumers" do
      consumer.subscribe("test")
      # Consumer needs time to join group, get partitions assigned, and start
      # fetching before topics appear in filtered stats
      (15 * 20).times do
        break if stats.any? { |s| !s["topics"].empty? }
        consumer.poll(50)
      end

      stat = stats.select { |s| !s["topics"].empty? }.last
      expect(stat).not_to be_nil
      expect(stat["topics"]).to have_key("test")
    end
  end

  context "when set to true (default behavior) for a producer" do
    let(:producer) do
      rdkafka_producer_config(
        "statistics.interval.ms": 100,
        "statistics.unassigned.include": true
      ).producer
    end

    after { producer.close }

    it "emits topics with partitions" do
      producer.produce(topic: "test", payload: "test").wait
      wait_for_stats(2)

      stat = stats.select { |s| !s["topics"].empty? }.last
      expect(stat).not_to be_nil
      expect(stat["topics"]).to have_key("test")
      expect(stat["topics"]["test"]["partitions"]).not_to be_empty
    end

    it "preserves root-level keys and broker data" do
      producer.produce(topic: "test", payload: "test").wait
      wait_for_stats(2)

      stat = stats.last
      expect(stat).to have_key("name")
      expect(stat).to have_key("ts")
      expect(stat).to have_key("brokers")
      expect(stat["brokers"]).not_to be_empty
      expect(stat["txmsgs"]).to be_a(Integer)
      expect(stat["rxmsgs"]).to be_a(Integer)
    end
  end

  context "when set to true (default behavior) for a consumer" do
    let(:consumer) do
      rdkafka_consumer_config(
        "statistics.interval.ms": 100,
        "statistics.unassigned.include": true
      ).consumer
    end

    after { consumer.close }

    def poll_until(consumer, timeout: 15)
      (timeout * 20).times do
        break if yield
        consumer.poll(50)
      end
    end

    it "emits topics with partitions" do
      consumer.subscribe("test")
      poll_until(consumer) { stats.any? { |s| !s["topics"].empty? } }

      stat = stats.select { |s| !s["topics"].empty? }.last
      expect(stat).not_to be_nil
      expect(stat["topics"]).to have_key("test")
      expect(stat["topics"]["test"]["partitions"]).not_to be_empty
    end

    it "preserves consumer group statistics" do
      consumer.subscribe("test")
      poll_until(consumer) { stats.any? { |s| s["cgrp"] } }

      stat = stats.select { |s| s["cgrp"] }.last
      expect(stat).not_to be_nil
      expect(stat["cgrp"]).to have_key("state")
    end

    it "preserves root-level keys and broker data" do
      consumer.subscribe("test")
      poll_until(consumer) { stats.size >= 2 }

      stat = stats.last
      expect(stat).to have_key("name")
      expect(stat).to have_key("type")
      expect(stat).to have_key("brokers")
      expect(stat["brokers"]).not_to be_empty
      expect(stat["type"]).to eq("consumer")
      expect(stat["rxmsgs"]).to be_a(Integer)
    end
  end
end
