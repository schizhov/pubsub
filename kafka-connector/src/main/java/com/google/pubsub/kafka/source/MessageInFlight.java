package com.google.pubsub.kafka.source;

import com.google.cloud.pubsub.v1.AckReplyConsumer;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

public class MessageInFlight {
  private static final Logger log = LoggerFactory.getLogger(MessageInFlight.class);

  private final String messageId;
  private final SourceRecord record;
  private final AckReplyConsumer ackReplyConsumer;

  public MessageInFlight(String messageId, SourceRecord record, AckReplyConsumer ackReplyConsumer) {
    Objects.requireNonNull(messageId, "messageId cannot be null");
    Objects.requireNonNull(record, "record cannot be null");
    Objects.requireNonNull(ackReplyConsumer, "ackReplyConsumer cannot be null");
    this.messageId = messageId;
    this.record = record;
    this.ackReplyConsumer = ackReplyConsumer;
  }

  public String getMessageId() {
    return messageId;
  }

  public SourceRecord getRecord() {
    return record;
  }

  public void ack() {
    ackReplyConsumer.ack();
    log.trace("Acked {}", record.key());
  }

  public void nack() {
    ackReplyConsumer.nack();
    log.trace("Nacked {}", record.key());
  }

  @Override
  public String toString() {
    return "MessageInFlight{" +
            "messageId='" + messageId + '\'' +
            ", record=" + record +
            ", ackReplyConsumer=" + ackReplyConsumer +
            '}';
  }
}
