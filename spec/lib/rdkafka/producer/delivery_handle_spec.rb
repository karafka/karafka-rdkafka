# frozen_string_literal: true

RSpec.describe Rdkafka::Producer::DeliveryHandle do
  let(:handle) do
    described_class.new.tap do |handle|
      handle[:pending] = pending_handle
      handle[:response] = response
      handle[:partition] = 2
      handle[:offset] = 100
      handle.topic = TestTopics.unique
    end
  end

  let(:response) { 0 }

  describe "#wait" do
    let(:pending_handle) { true }

    it "waits until the timeout and then raise an error" do
      expect {
        handle.wait(max_wait_timeout_ms: 100)
      }.to raise_error Rdkafka::Producer::DeliveryHandle::WaitTimeoutError, /delivery/
    end

    context "when not pending anymore and no error" do
      let(:pending_handle) { false }

      it "returns a delivery report" do
        report = handle.wait

        expect(report.partition).to eq(2)
        expect(report.offset).to eq(100)
        expect(report.topic_name).to eq(handle.topic)
      end

      it "waits without a timeout" do
        report = handle.wait(max_wait_timeout_ms: nil)

        expect(report.partition).to eq(2)
        expect(report.offset).to eq(100)
        expect(report.topic_name).to eq(handle.topic)
      end
    end

    context "when not pending anymore and the delivery failed with an error text" do
      let(:pending_handle) { false }
      let(:response) { Rdkafka::Bindings::RD_KAFKA_RESP_ERR_INVALID_PRODUCER_EPOCH }

      before { handle.broker_message = "Compacted topic cannot accept message without key" }

      it "raises an error with the text" do
        expect { handle.wait }.to raise_error(Rdkafka::RdkafkaError) { |error|
          expect(error.code).to eq(:invalid_producer_epoch)
          expect(error.broker_message).to eq("Compacted topic cannot accept message without key")
          expect(error.message).to eq(
            "Broker: Producer attempted an operation with an old epoch (invalid_producer_epoch)\n" \
            "Compacted topic cannot accept message without key"
          )
        }
      end
    end
  end

  describe "#create_result" do
    let(:pending_handle) { false }
    let(:report) { handle.create_result }

    context "when response is 0" do
      it { expect(report.error).to be_nil }
      it { expect(report).to be_an_instance_of(Rdkafka::Producer::DeliveryReport) }
    end

    context "when response is not 0" do
      let(:response) { 1 }

      it { expect(report.error).to eq(Rdkafka::RdkafkaError.new(response)) }
      it { expect(report.broker_message).to be_nil }
      it { expect(report).to be_an_instance_of(Rdkafka::Producer::FailedDeliveryReport) }
    end

    context "when the delivery callback set an error text" do
      let(:response) { Rdkafka::Bindings::RD_KAFKA_RESP_ERR_INVALID_PRODUCER_EPOCH }

      before { handle.broker_message = "Compacted topic cannot accept message without key" }

      it "passes it to the report and its error" do
        expect(report.broker_message).to eq("Compacted topic cannot accept message without key")
        expect(report.error.broker_message).to eq("Compacted topic cannot accept message without key")
      end

      it "puts it on the second line of the error message" do
        expect(report.error.message).to eq(
          "Broker: Producer attempted an operation with an old epoch (invalid_producer_epoch)\n" \
          "Compacted topic cannot accept message without key"
        )
      end
    end

    context "when the delivery callback set status, latency and broker id" do
      before do
        handle[:status] = Rdkafka::Bindings::RD_KAFKA_MSG_STATUS_PERSISTED
        handle[:latency] = 2_000
        handle[:broker_id] = 1
      end

      it "passes them to the report" do
        expect(report.status).to eq(Rdkafka::Bindings::RD_KAFKA_MSG_STATUS_PERSISTED)
        expect(report.latency).to eq(2_000)
        expect(report.broker_id).to eq(1)
      end
    end
  end
end
