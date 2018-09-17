// Copyright 2016 Google Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
////////////////////////////////////////////////////////////////////////////////
package com.google.pubsub.kafka.source;

import com.google.api.core.ApiService;
import com.google.api.gax.batching.FlowControlSettings;
import com.google.api.gax.batching.FlowController;
import com.google.api.gax.core.FixedExecutorProvider;
import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.protobuf.ByteString;
import com.google.protobuf.util.Timestamps;
import com.google.pubsub.kafka.common.ConnectorUtils;
import com.google.pubsub.v1.ProjectSubscriptionName;
import com.google.pubsub.v1.PubsubMessage;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * A {@link SourceTask} used by a {@link CloudPubSubSourceConnector} to write messages to <a
 * href="http://kafka.apache.org/">Apache Kafka</a>. Due to at-last-once semantics in Google
 * Cloud Pub/Sub duplicates in Kafka are possible.
 */
public class CloudPubSubSourceTask extends SourceTask {

  private static final Logger log = LoggerFactory.getLogger(CloudPubSubSourceTask.class);

  private final Set<String> standardAttributes = new HashSet<>();
  private final Map<String, MessageInFlight> messagesInFlight = new ConcurrentHashMap<>();
  private final Map<String, MessageInFlight> received = new ConcurrentHashMap<>();
  private final Object subscriberLock = new Object();

  private volatile String projectId;
  private volatile String subscriptionId;
  private volatile String kafkaTopic;
  private volatile String kafkaMessageKeyAttribute;
  private volatile String kafkaMessageTimestampAttribute;
  private volatile int cpsMaxBatchSize;

  private volatile Subscriber subscriber;

  private final AtomicLong messageCounter =new AtomicLong();
  private final AtomicLong ackCounter =new AtomicLong();
  private final AtomicLong nackCounter =new AtomicLong();
  private final AtomicBoolean stopping = new AtomicBoolean();

  public CloudPubSubSourceTask() {
  }

  @Override
  public String version() {
    return new CloudPubSubSourceConnector().version();
  }

  @Override
  public void start(Map<String, String> props) {
    Map<String, Object> validatedProps = new CloudPubSubSourceConnector().config().parse(props);
    projectId = validatedProps.get(ConnectorUtils.CPS_PROJECT_CONFIG).toString();
    subscriptionId = validatedProps.get(CloudPubSubSourceConnector.CPS_SUBSCRIPTION_CONFIG).toString();
    kafkaTopic = validatedProps.get(CloudPubSubSourceConnector.KAFKA_TOPIC_CONFIG).toString();
    cpsMaxBatchSize =
            (Integer) validatedProps.get(CloudPubSubSourceConnector.CPS_MAX_BATCH_SIZE_CONFIG);
    kafkaMessageKeyAttribute =
            (String) validatedProps.get(CloudPubSubSourceConnector.KAFKA_MESSAGE_KEY_CONFIG);
    kafkaMessageTimestampAttribute =
            (String) validatedProps.get(CloudPubSubSourceConnector.KAFKA_MESSAGE_TIMESTAMP_CONFIG);
    standardAttributes.add(kafkaMessageKeyAttribute);
    standardAttributes.add(kafkaMessageTimestampAttribute);

    synchronized (subscriberLock) {
      if (subscriber != null) {
        throw new IllegalStateException("Starting already started task");
      }

      FlowControlSettings flowControlSettings =
              FlowControlSettings.newBuilder()
                      .setMaxOutstandingElementCount((long) cpsMaxBatchSize)
                      .setMaxOutstandingRequestBytes(1_000_000_000L)
                      .setLimitExceededBehavior(FlowController.LimitExceededBehavior.Block)
                      .build();

      subscriber = Subscriber.newBuilder(ProjectSubscriptionName.of(projectId, subscriptionId), this::onPubsubMessageReceived)
              .setFlowControlSettings(flowControlSettings)
              /*.setExecutorProvider(FixedExecutorProvider.create(Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
                @Override
                public Thread newThread(Runnable r) {
                  Thread t = new Thread(r, "bom-exec");
                  t.setDaemon(true);
                  return t;
                }
              })))*/
              /*.setSystemExecutorProvider(FixedExecutorProvider.create(Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
                @Override
                public Thread newThread(Runnable r) {
                  Thread t = new Thread(r, "bom-sys");
                  t.setDaemon(true);
                  return t;
                }
              })))*/
              .setParallelPullCount(1) //TODO configure
              .build();

      subscriber.addListener(
              new Subscriber.Listener() {

                @Override
                public void stopping(ApiService.State from) {
                  log.info("Stopping: {}", from);
                }

                @Override
                public void terminated(ApiService.State from) {
                  log.info("Terminated: {}", from);
                }

                public void failed(Subscriber.State from, Throwable failure) {
                  log.error("Failed: {}", from, failure);
                }
              },
              MoreExecutors.directExecutor()
      );


      subscriber.startAsync();
      //Executors.newScheduledThreadPool(1).schedule(this::stop, 10, TimeUnit.SECONDS);

    }
    log.info("Started with {}", props);
  }



  @Override
  public void commitRecord(SourceRecord record) {
    commitRecord(record, true);
  }

  public void commitRecord(SourceRecord record, boolean ack) {
    String messageId = (String) record.sourceOffset().get(subscriptionId);
    MessageInFlight m = messagesInFlight.get(messageId);
    if (m != null) {
      if (ack) {
        m.ack();
        ackCounter.incrementAndGet();
      }
      else {
        m.nack();
        nackCounter.incrementAndGet();
      }
      messagesInFlight.remove(messageId);

      //log.trace("Acked[{}] {}", ack, record.key());
    } else {
      log.error("Nothing to ack[{}] for {}", ack, record.key());
    }
    //logCounters();
  }

  private void logCounters() {
    log.info("M{},A{},N{},D{}",messageCounter.get(), ackCounter.get(), nackCounter.get(), messageCounter.get()-ackCounter.get()-nackCounter.get());
  }

  @Override
  public List<SourceRecord> poll() {
    Map<String, MessageInFlight> batch = new HashMap<>();
    batch.putAll(received);
    messagesInFlight.putAll(batch);
    received.keySet().removeAll(batch.keySet());
    return batch.values().stream().map(MessageInFlight::getRecord).collect(Collectors.toList());
  }

  private Long getLongValue(String timestamp) {
    if (timestamp == null) {
      return null;
    }
    try {
      return Long.valueOf(timestamp);
    } catch (NumberFormatException e) {
      log.error("Error while converting `{}` to number", timestamp, e);
    }
    return null;
  }

  @Override
  public void commit() {
    log.info("Committing. Stopping ={}", stopping.get());
    logCounters();
    if (stopping.get()) {
      poll().forEach(r -> commitRecord(r, false));
      while (!subscriber.state().equals(ApiService.State.TERMINATED)) {
        try {
          subscriber.awaitTerminated(1000, TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
          log.warn("Failed to stop {} in time", subscriber, e);
          logCounters();
        }
      }
    }
    logCounters();
    log.info("Committed. Stopping ={}", stopping.get());
  }


  @Override
  public void stop() {
    log.info("Stopping");
    stopping.set(true);
    synchronized (subscriberLock) {
      //long waitMs = 5000;//TODO timeout configurable
      if (subscriber != null) {
        log.info("Stopping subscriber {}", messageCounter.get());

        subscriber.stopAsync();

        /*while (!subscriber.state().equals(ApiService.State.TERMINATED)) {
          try {
            subscriber.awaitTerminated(waitMs, TimeUnit.MILLISECONDS);
          } catch (TimeoutException e) {
            log.warn("Failed to stop {} in time", subscriber, e);
            List<SourceRecord> records = poll();
            messagesInFlight.values().forEach(r -> commitRecord( r.getRecord(), false));
            records.forEach(r -> commitRecord(r, false));
            log.info("Stopping. Nacked {} records. Subscriber is {}. Waiting {}ms", records.size(), subscriber.state(), waitMs);
            logCounters();
          }
        }*/
      }
    }
    logCounters();
  }

  private void onPubsubMessageReceived(PubsubMessage message, AckReplyConsumer ackReplyConsumer) {
    MessageInFlight m = new MessageInFlight(message.getMessageId(), convert(message), ackReplyConsumer);
    received.put(message.getMessageId(), m);
    Map<String, String> messageAttributes = message.getAttributes();
    String key = messageAttributes.get(kafkaMessageKeyAttribute);
    messageCounter.incrementAndGet();
    log.trace("Received {}, {}", key, messageCounter.get());
  }

  SourceRecord convert(PubsubMessage message) {
    Map<String, String> messageId = Collections.singletonMap(subscriptionId, message.getMessageId());
    Map<String, String> messageAttributes = message.getAttributes();
    String key = messageAttributes.get(kafkaMessageKeyAttribute);
    Long timestamp = getLongValue(messageAttributes.get(kafkaMessageTimestampAttribute));
    if (timestamp == null) {
      timestamp = Timestamps.toMillis(message.getPublishTime());
    }
    ByteString messageData = message.getData();
    byte[] messageBytes = messageData.toByteArray();

    boolean hasCustomAttributes = !standardAttributes.containsAll(messageAttributes.keySet());

    if (hasCustomAttributes) {
      SchemaBuilder valueSchemaBuilder = SchemaBuilder.struct().field(
              ConnectorUtils.KAFKA_MESSAGE_CPS_BODY_FIELD,
              Schema.BYTES_SCHEMA);

      for (Entry<String, String> attribute :
              messageAttributes.entrySet()) {
        if (!attribute.getKey().equals(kafkaMessageKeyAttribute)) {
          valueSchemaBuilder.field(attribute.getKey(),
                  Schema.STRING_SCHEMA);
        }
      }

      Schema valueSchema = valueSchemaBuilder.build();
      Struct value =
              new Struct(valueSchema)
                      .put(ConnectorUtils.KAFKA_MESSAGE_CPS_BODY_FIELD,
                              messageBytes);
      for (Field field : valueSchema.fields()) {
        if (!field.name().equals(
                ConnectorUtils.KAFKA_MESSAGE_CPS_BODY_FIELD)) {
          value.put(field.name(), messageAttributes.get(field.name()));
        }
      }
      return new SourceRecord(
              null,
              messageId,
              kafkaTopic,
              null,
              Schema.OPTIONAL_STRING_SCHEMA,
              key,
              valueSchema,
              value,
              timestamp);
    } else {
      return new SourceRecord(
              null,
              messageId,
              kafkaTopic,
              null,
              Schema.OPTIONAL_STRING_SCHEMA,
              key,
              Schema.BYTES_SCHEMA,
              messageBytes,
              timestamp);
    }
  }

}
