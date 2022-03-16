/*
 * Copyright (c) 2021 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.source.kafka;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Lists;
import io.airbyte.commons.util.AutoCloseableIterator;
import io.airbyte.commons.util.AutoCloseableIterators;
import io.airbyte.integrations.BaseConnector;
import io.airbyte.integrations.base.IntegrationRunner;
import io.airbyte.integrations.base.Source;
import io.airbyte.protocol.models.AirbyteCatalog;
import io.airbyte.protocol.models.AirbyteConnectionStatus;
import io.airbyte.protocol.models.AirbyteConnectionStatus.Status;
import io.airbyte.protocol.models.AirbyteMessage;
import io.airbyte.protocol.models.AirbyteRecordMessage;
import io.airbyte.protocol.models.AirbyteStream;
import io.airbyte.protocol.models.CatalogHelpers;
import io.airbyte.protocol.models.ConfiguredAirbyteCatalog;
import io.airbyte.protocol.models.Field;
import io.airbyte.protocol.models.JsonSchemaType;
import io.airbyte.protocol.models.SyncMode;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class KafkaSource extends BaseConnector implements Source {
  private static final Logger LOGGER = LoggerFactory.getLogger(KafkaSource.class);

  private Set<String> topicsToSubscribe;

  @Override
  public AirbyteConnectionStatus check(final JsonNode config) {
    try {
      final String testTopic = config.has("test_topic") ? config.get("test_topic").asText() : "";
      final JsonNode subscription = config.get("subscription");
      if (!testTopic.isBlank()) {
        if (config.get("message_format").asText().equals("JSON")) {
          final KafkaSourceJson kafkaSourceJson = new KafkaSourceJson();
          final KafkaConsumer<String, JsonNode> consumer = kafkaSourceJson.buildKafkaConsumer(config);
          final String topicPattern = subscription.get("topic_pattern").asText();
          final Set<String> topicsToSubscribe = consumer.listTopics().keySet().stream()
            .filter(topic -> topic.matches(topicPattern))
            .collect(Collectors.toSet());
          consumer.subscribe(Pattern.compile(config.get("test_topic").asText()));
          consumer.listTopics();
          consumer.close();
        } else
        if (config.get("message_format").asText().equals("AVRO")) {
          final KafkaSourceAvro kafkaSourceAvro = new KafkaSourceAvro();
          final KafkaConsumer<String, GenericRecord> consumer = kafkaSourceAvro.buildKafkaConsumer(config);
          final String topicPattern = subscription.get("topic_pattern").asText();
          final Set<String> topicsToSubscribe = consumer.listTopics().keySet().stream()
            .filter(topic -> topic.matches(topicPattern))
            .collect(Collectors.toSet());
          consumer.subscribe(Pattern.compile(config.get("test_topic").asText()));
          consumer.listTopics();
          consumer.close();
        }
      }
      return new AirbyteConnectionStatus().withStatus(Status.SUCCEEDED);
    } catch (final Exception e) {
      LOGGER.error("Exception attempting to connect to the Kafka brokers: ", e);
      return new AirbyteConnectionStatus()
          .withStatus(Status.FAILED)
          .withMessage("Could not connect to the Kafka brokers with provided configuration. \n" + e.getMessage());
    }
  }

  @Override
  public AirbyteCatalog discover(final JsonNode config) throws Exception {
    
    final KafkaSourceJson kafkaSourceJson = new KafkaSourceJson();

    final JsonNode subscription = config.get("subscription");
    final String topicPattern = subscription.get("topic_pattern").asText();
    final KafkaConsumer<String, JsonNode> consumer = kafkaSourceJson.buildKafkaConsumer(config);
    final Set<String> topicsToSubscribe = consumer.listTopics().keySet().stream()
                .filter(topic -> topic.matches(topicPattern))
                .collect(Collectors.toSet());

    final List<AirbyteStream> streams = topicsToSubscribe.stream().map(topic -> CatalogHelpers
        .createAirbyteStream(topic, Field.of("value", JsonSchemaType.STRING))
        .withSupportedSyncModes(Lists.newArrayList(SyncMode.FULL_REFRESH, SyncMode.INCREMENTAL)))
        .collect(Collectors.toList());
    return new AirbyteCatalog().withStreams(streams);
  }

  @Override
  public AutoCloseableIterator<AirbyteMessage> read(final JsonNode config, final ConfiguredAirbyteCatalog catalog, final JsonNode state)
      throws Exception {
    final AirbyteConnectionStatus check = check(config);
    if (check.getStatus().equals(AirbyteConnectionStatus.Status.FAILED)) {
      throw new RuntimeException("Unable establish a connection: " + check.getMessage());
    }
    if (config.get("message_format").asText().equals("JSON")) {
      final KafkaSourceJson kafkaSourceJson = new KafkaSourceJson();
      return kafkaSourceJson.consumeMessages(config);
    } else {
      final KafkaSourceAvro kafkaSourceAvro = new KafkaSourceAvro();
      return kafkaSourceAvro.consumeMessages(config);
    }
  }

  public static void main(final String[] args) throws Exception {
    final Source source = new KafkaSource();
    LOGGER.info("Starting source: {}", KafkaSource.class);
    new IntegrationRunner(source).run(args);
    LOGGER.info("Completed source: {}", KafkaSource.class);
  }
}