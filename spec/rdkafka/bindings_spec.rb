# frozen_string_literal: true

require 'zlib'

describe Rdkafka::Bindings do
  it "should load librdkafka" do
    expect(Rdkafka::Bindings.ffi_libraries.map(&:name).first).to include "librdkafka"
  end

  describe ".lib_extension" do
    it "should know the lib extension for darwin" do
      stub_const('RbConfig::CONFIG', 'host_os' =>'darwin')
      expect(Rdkafka::Bindings.lib_extension).to eq "dylib"
    end

    it "should know the lib extension for linux" do
      stub_const('RbConfig::CONFIG', 'host_os' =>'linux')
      expect(Rdkafka::Bindings.lib_extension).to eq "so"
    end
  end

  it "should successfully call librdkafka" do
    expect {
      Rdkafka::Bindings.rd_kafka_conf_new
    }.not_to raise_error
  end

  describe "log callback" do
    let(:log_queue) { Rdkafka::Config.log_queue }
    before do
      allow(log_queue).to receive(:<<)
    end

    it "should log fatal messages" do
      Rdkafka::Bindings::LogCallback.call(nil, 0, nil, "log line")
      expect(log_queue).to have_received(:<<).with([Logger::FATAL, "rdkafka: log line"])
    end

    it "should log fatal messages" do
      Rdkafka::Bindings::LogCallback.call(nil, 1, nil, "log line")
      expect(log_queue).to have_received(:<<).with([Logger::FATAL, "rdkafka: log line"])
    end

    it "should log fatal messages" do
      Rdkafka::Bindings::LogCallback.call(nil, 2, nil, "log line")
      expect(log_queue).to have_received(:<<).with([Logger::FATAL, "rdkafka: log line"])
    end

    it "should log error messages" do
      Rdkafka::Bindings::LogCallback.call(nil, 3, nil, "log line")
      expect(log_queue).to have_received(:<<).with([Logger::ERROR, "rdkafka: log line"])
    end

    it "should log warning messages" do
      Rdkafka::Bindings::LogCallback.call(nil, 4, nil, "log line")
      expect(log_queue).to have_received(:<<).with([Logger::WARN, "rdkafka: log line"])
    end

    it "should log info messages" do
      Rdkafka::Bindings::LogCallback.call(nil, 5, nil, "log line")
      expect(log_queue).to have_received(:<<).with([Logger::INFO, "rdkafka: log line"])
    end

    it "should log info messages" do
      Rdkafka::Bindings::LogCallback.call(nil, 6, nil, "log line")
      expect(log_queue).to have_received(:<<).with([Logger::INFO, "rdkafka: log line"])
    end

    it "should log debug messages" do
      Rdkafka::Bindings::LogCallback.call(nil, 7, nil, "log line")
      expect(log_queue).to have_received(:<<).with([Logger::DEBUG, "rdkafka: log line"])
    end

    it "should log unknown messages" do
      Rdkafka::Bindings::LogCallback.call(nil, 100, nil, "log line")
      expect(log_queue).to have_received(:<<).with([Logger::UNKNOWN, "rdkafka: log line"])
    end
  end

  describe "stats callback" do
    context "without a stats callback" do
      it "should do nothing" do
        expect {
          Rdkafka::Bindings::StatsCallback.call(nil, "{}", 2, nil)
        }.not_to raise_error
      end
    end

    context "with a stats callback" do
      before do
        Rdkafka::Config.statistics_callback = lambda do |stats|
          $received_stats = stats
        end
      end

      it "should call the stats callback with a stats hash" do
        Rdkafka::Bindings::StatsCallback.call(nil, "{\"received\":1}", 13, nil)
        expect($received_stats).to eq({'received' => 1})
      end
    end
  end

  describe "error callback" do
    context "without an error callback" do
      it "should do nothing" do
        expect {
          Rdkafka::Bindings::ErrorCallback.call(nil, 1, "error", nil)
        }.not_to raise_error
      end
    end

    context "with an error callback" do
      before do
        Rdkafka::Config.error_callback = lambda do |error|
          $received_error = error
        end
      end

      it "should call the error callback with an Rdkafka::Error" do
        Rdkafka::Bindings::ErrorCallback.call(nil, 8, "Broker not available", nil)
        expect($received_error.code).to eq(:broker_not_available)
        expect($received_error.broker_message).to eq("Broker not available")
      end
    end
  end

  describe "oauthbearer set token" do
    context "with args" do
      before do
        DEFAULT_TOKEN_EXPIRY_SECONDS = 900
        $token_value = "token"
        $md_lifetime_ms = Time.now.to_i*1000 + DEFAULT_TOKEN_EXPIRY_SECONDS * 1000
        $md_principal_name = "kafka-cluster"
        $extensions = nil
        $extension_size = 0
        $error_buffer = FFI::MemoryPointer.from_string(" " * 256)
      end

      it "should set token or capture failure" do
        RdKafkaTestConsumer.with do |consumer_ptr|
          response = Rdkafka::Bindings.rd_kafka_oauthbearer_set_token(consumer_ptr, $token_value, $md_lifetime_ms, $md_principal_name, $extensions, $extension_size, $error_buffer, 256)
          expect(response).to eq(Rdkafka::Bindings::RD_KAFKA_RESP_ERR__STATE)
          expect($error_buffer.read_string).to eq("SASL/OAUTHBEARER is not the configured authentication mechanism")
        end
      end
    end
  end

  describe "oauthbearer set token failure" do

    context "without args" do

      it "should fail" do
        expect {
          Rdkafka::Bindings.rd_kafka_oauthbearer_set_token_failure
        }.to raise_error(ArgumentError)
      end
    end

    context "with args" do
      it "should succeed" do
        expect {
          errstr = "error"
          RdKafkaTestConsumer.with do |consumer_ptr|
            Rdkafka::Bindings.rd_kafka_oauthbearer_set_token_failure(consumer_ptr, errstr)
          end
        }.to_not raise_error
      end
    end
  end

  describe "oauthbearer callback" do
    context "without an oauthbearer callback" do
      it "should do nothing" do
        expect {
          Rdkafka::Bindings::OAuthbearerTokenRefreshCallback.call(nil, "", nil)
        }.not_to raise_error
      end
    end

    context "with an oauthbearer callback" do
      before do
        Rdkafka::Config.oauthbearer_token_refresh_callback = lambda do |config, client_name|
          $received_config = config
          $received_client_name = client_name
        end
      end

      it "should call the oauth bearer callback and receive config and client name" do
        RdKafkaTestConsumer.with do |consumer_ptr|
          Rdkafka::Bindings::OAuthbearerTokenRefreshCallback.call(consumer_ptr, "{}", nil)
            expect($received_config).to eq("{}")
            expect($received_client_name).to match(/consumer/)
        end
      end
    end
  end
end
