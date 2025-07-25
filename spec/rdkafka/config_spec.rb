# frozen_string_literal: true

describe Rdkafka::Config do
  context "logger" do
    it "should have a default logger" do
      expect(Rdkafka::Config.logger).to be_a Logger
    end

    it "should set the logger" do
      logger = Logger.new(STDOUT)
      expect(Rdkafka::Config.logger).not_to eq logger
      Rdkafka::Config.logger = logger
      expect(Rdkafka::Config.logger).to eq logger
    end

    it "should not accept a nil logger" do
      expect {
        Rdkafka::Config.logger = nil
      }.to raise_error(Rdkafka::Config::NoLoggerError)
    end

    it "supports logging queue" do
      log = StringIO.new
      Rdkafka::Config.logger = Logger.new(log)
      Rdkafka::Config.ensure_log_thread

      Rdkafka::Config.log_queue << [Logger::FATAL, "I love testing"]
      20.times do
        break if log.string != ""
        sleep 0.05
      end

      expect(log.string).to include "FATAL -- : I love testing"
    end

    unless RUBY_PLATFORM == 'java'
      it "expect to start new logger thread after fork and work" do
        reader, writer = IO.pipe

        pid = fork do
          $stdout.reopen(writer)
          Rdkafka::Config.logger = Logger.new($stdout)
          reader.close
          producer = rdkafka_producer_config(debug: 'all').producer
          producer.close
          writer.close
          sleep(1)
        end

        writer.close
        Process.wait(pid)
        output = reader.read
        expect(output.split("\n").size).to be >= 20
      end
    end
  end

  context "statistics callback" do
    context "with a proc/lambda" do
      it "should set the callback" do
        expect {
          Rdkafka::Config.statistics_callback = lambda do |stats|
            puts stats
          end
        }.not_to raise_error
        expect(Rdkafka::Config.statistics_callback).to respond_to :call
      end
    end

    context "with a callable object" do
      it "should set the callback" do
        callback = Class.new do
          def call(stats); end
        end
        expect {
          Rdkafka::Config.statistics_callback = callback.new
        }.not_to raise_error
        expect(Rdkafka::Config.statistics_callback).to respond_to :call
      end
    end

    it "should not accept a callback that's not callable" do
      expect {
        Rdkafka::Config.statistics_callback = 'a string'
      }.to raise_error(TypeError)
    end
  end

  context "error callback" do
    context "with a proc/lambda" do
      it "should set the callback" do
        expect {
          Rdkafka::Config.error_callback = lambda do |error|
            puts error
          end
        }.not_to raise_error
        expect(Rdkafka::Config.error_callback).to respond_to :call
      end
    end

    context "with a callable object" do
      it "should set the callback" do
        callback = Class.new do
          def call(stats); end
        end
        expect {
          Rdkafka::Config.error_callback = callback.new
        }.not_to raise_error
        expect(Rdkafka::Config.error_callback).to respond_to :call
      end
    end

    it "should not accept a callback that's not callable" do
      expect {
        Rdkafka::Config.error_callback = 'a string'
      }.to raise_error(TypeError)
    end
  end

  context "oauthbearer calllback" do
    context "with a proc/lambda" do
      it "should set the callback" do
        expect {
          Rdkafka::Config.oauthbearer_token_refresh_callback = lambda do |config, client_name|
            puts config
            puts client_name
          end
        }.not_to raise_error
        expect(Rdkafka::Config.oauthbearer_token_refresh_callback).to respond_to :call
      end
    end

    context "with a callable object" do
      it "should set the callback" do
        callback = Class.new do
          def call(config, client_name); end
        end

        expect {
          Rdkafka::Config.oauthbearer_token_refresh_callback = callback.new
        }.not_to raise_error
        expect(Rdkafka::Config.oauthbearer_token_refresh_callback).to respond_to :call
      end
    end

    it "should not accept a callback that's not callable" do
      expect {
        Rdkafka::Config.oauthbearer_token_refresh_callback = 'not a callback'
      }.to raise_error(TypeError)
    end
  end

  context "configuration" do
    it "should store configuration" do
      config = Rdkafka::Config.new
      config[:"key"] = 'value'
      expect(config[:"key"]).to eq 'value'
    end

    it "should use default configuration" do
      config = Rdkafka::Config.new
      expect(config[:"api.version.request"]).to eq nil
    end

    it "should create a consumer with valid config" do
      consumer = rdkafka_consumer_config.consumer
      expect(consumer).to be_a Rdkafka::Consumer
      consumer.close
    end

    it "should create a consumer with consumer_poll_set set to false" do
      config = rdkafka_consumer_config
      config.consumer_poll_set = false
      consumer = config.consumer
      expect(consumer).to be_a Rdkafka::Consumer
      consumer.close
    end

    it "should raise an error when creating a consumer with invalid config" do
      config = Rdkafka::Config.new('invalid.key' => 'value')
      expect {
        config.consumer
      }.to raise_error(Rdkafka::Config::ConfigError, "No such configuration property: \"invalid.key\"")
    end

    it "should raise an error when creating a consumer with a nil key in the config" do
      config = Rdkafka::Config.new(nil => 'value')
      expect {
        config.consumer
      }.to raise_error(Rdkafka::Config::ConfigError, "No such configuration property: \"\"")
    end

    it "should treat a nil value as blank" do
      config = Rdkafka::Config.new('security.protocol' => nil)
      expect {
        config.consumer
        config.producer
      }.to raise_error(Rdkafka::Config::ConfigError, "Configuration property \"security.protocol\" cannot be set to empty value")
    end

    it "should create a producer with valid config" do
      producer = rdkafka_consumer_config.producer
      expect(producer).to be_a Rdkafka::Producer
      producer.close
    end

    it "should raise an error when creating a producer with invalid config" do
      config = Rdkafka::Config.new('invalid.key' => 'value')
      expect {
        config.producer
      }.to raise_error(Rdkafka::Config::ConfigError, "No such configuration property: \"invalid.key\"")
    end

    it "allows string partitioner key" do
      expect(Rdkafka::Producer).to receive(:new).with(kind_of(Rdkafka::NativeKafka), "murmur2").and_call_original
      config = Rdkafka::Config.new("partitioner" => "murmur2")
      config.producer.close
    end

    it "allows symbol partitioner key" do
      expect(Rdkafka::Producer).to receive(:new).with(kind_of(Rdkafka::NativeKafka), "murmur2").and_call_original
      config = Rdkafka::Config.new(:partitioner => "murmur2")
      config.producer.close
    end

    it "should allow configuring zstd compression" do
      config = Rdkafka::Config.new('compression.codec' => 'zstd')
      begin
        producer = config.producer
        expect(producer).to be_a Rdkafka::Producer
        producer.close
      rescue Rdkafka::Config::ConfigError => ex
        pending "Zstd compression not supported on this machine"
        raise ex
      end
    end

    it "should raise an error when client creation fails for a consumer" do
      config = Rdkafka::Config.new(
        "security.protocol" => "SSL",
        "ssl.ca.location" => "/nonsense"
      )
      expect {
        config.consumer
      }.to raise_error(Rdkafka::Config::ClientCreationError, /ssl.ca.location failed(.*)/)
    end

    it "should raise an error when client creation fails for a producer" do
      config = Rdkafka::Config.new(
        "security.protocol" => "SSL",
        "ssl.ca.location" => "/nonsense"
      )
      expect {
        config.producer
      }.to raise_error(Rdkafka::Config::ClientCreationError, /ssl.ca.location failed(.*)/)
    end
  end
end
