# frozen_string_literal: true

unless ENV["CI"] == "true"
  require "simplecov"
  SimpleCov.start do
    add_filter "/spec/"
  end
end

require "pry"
require "rspec"
require "rdkafka"
require "timeout"
require "securerandom"

def rdkafka_base_config
  {
    :"bootstrap.servers" => "localhost:9092",
    # Display statistics and refresh often just to cover those in specs
    :'statistics.interval.ms' => 1_000,
    :'topic.metadata.refresh.interval.ms' => 1_000
  }
end

def rdkafka_config(config_overrides={})
  # Generate the base config
  config = rdkafka_base_config
  # Merge overrides
  config.merge!(config_overrides)
  # Return it
  Rdkafka::Config.new(config)
end

def rdkafka_consumer_config(config_overrides={})
  # Generate the base config
  config = rdkafka_base_config
  # Add consumer specific fields to it
  config[:"auto.offset.reset"] = "earliest"
  config[:"enable.partition.eof"] = false
  config[:"group.id"] = "ruby-test-#{SecureRandom.uuid}"
  # Enable debug mode if required
  if ENV["DEBUG_CONSUMER"]
    config[:debug] = "cgrp,topic,fetch"
  end
  # Merge overrides
  config.merge!(config_overrides)
  # Return it
  Rdkafka::Config.new(config)
end

def rdkafka_producer_config(config_overrides={})
  # Generate the base config
  config = rdkafka_base_config
  # Enable debug mode if required
  if ENV["DEBUG_PRODUCER"]
    config[:debug] = "broker,topic,msg"
  end
  # Merge overrides
  config.merge!(config_overrides)
  # Return it
  Rdkafka::Config.new(config)
end

def new_native_client
  config = rdkafka_consumer_config
  config.send(:native_kafka, config.send(:native_config), :rd_kafka_producer)
end

def new_native_topic(topic_name="topic_name", native_client: )
  Rdkafka::Bindings.rd_kafka_topic_new(
    native_client,
    topic_name,
    nil
  )
end

def wait_for_message(topic:, delivery_report:, timeout_in_seconds: 30, consumer: nil)
  new_consumer = consumer.nil?
  consumer ||= rdkafka_consumer_config('allow.auto.create.topics': true).consumer
  consumer.subscribe(topic)
  timeout = Time.now.to_i + timeout_in_seconds
  retry_count = 0
  max_retries = 10

  loop do
    if timeout <= Time.now.to_i
      raise "Timeout of #{timeout_in_seconds} seconds reached in wait_for_message"
    end

    begin
      message = consumer.poll(100)
      if message &&
          message.partition == delivery_report.partition &&
          message.offset == delivery_report.offset
        return message
      end
    rescue Rdkafka::RdkafkaError => e
      if e.code == :unknown_topic_or_part && retry_count < max_retries
        retry_count += 1
        sleep(0.1) # Small delay before retry
        next
      else
        raise
      end
    end
  end
ensure
  consumer.close if new_consumer
end

def wait_for_assignment(consumer)
  10.times do
    break if !consumer.assignment.empty?
    sleep 1
  end
end

def wait_for_unassignment(consumer)
  10.times do
    break if consumer.assignment.empty?
    sleep 1
  end
end

def notify_listener(listener, &block)
  # 1. subscribe and poll
  consumer.subscribe("consume_test_topic")
  wait_for_assignment(consumer)
  consumer.poll(100)

  block.call if block

  # 2. unsubscribe
  consumer.unsubscribe
  wait_for_unassignment(consumer)
  consumer.close
end

RSpec.configure do |config|
  config.filter_run focus: true
  config.run_all_when_everything_filtered = true

  config.before(:each) do
    Rdkafka::Config.statistics_callback = nil
    # We need to clear it so state does not leak between specs
    Rdkafka::Producer.partitions_count_cache.to_h.clear
  end

  config.before(:suite) do
    admin = rdkafka_config.admin
    {
        consume_test_topic:      3,
        empty_test_topic:        3,
        load_test_topic:         3,
        produce_test_topic:      3,
        rake_test_topic:         3,
        watermarks_test_topic:   3,
        partitioner_test_topic: 25,
        example_topic:           1
    }.each do |topic, partitions|
      create_topic_handle = admin.create_topic(topic.to_s, partitions, 1)
      begin
        create_topic_handle.wait(max_wait_timeout: 1.0)
      rescue Rdkafka::RdkafkaError => ex
        raise unless ex.message.match?(/topic_already_exists/)
      end
    end
    admin.close
  end

  config.around(:each) do |example|
    # Timeout specs after a minute. If they take longer
    # they are probably stuck
    Timeout::timeout(60) do
      example.run
    end
  end
end

class RdKafkaTestConsumer
  def self.with
    consumer = Rdkafka::Bindings.rd_kafka_new(
      :rd_kafka_consumer,
      nil,
      nil,
      0
    )
    yield consumer
  ensure
    Rdkafka::Bindings.rd_kafka_consumer_close(consumer)
    Rdkafka::Bindings.rd_kafka_destroy(consumer)
  end
end
