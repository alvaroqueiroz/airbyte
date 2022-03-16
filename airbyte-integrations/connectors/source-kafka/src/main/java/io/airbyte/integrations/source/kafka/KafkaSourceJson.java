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
import org.apache.kafka.common.serialization.StringDeserializer;
import java.time.Instant;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.config.SaslConfigs;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableMap;
import io.airbyte.commons.json.Jsons;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaSourceJson {
  protected static final Logger LOGGER = LoggerFactory.getLogger(KafkaSourceJson.class);

  private Set<String> topicsToSubscribe;

  private static Map<String, Object> propertiesByProtocol(final JsonNode config) {
    final JsonNode protocolConfig = config.get("protocol");
    LOGGER.info("Kafka protocol config: {}", protocolConfig.toString());
    final KafkaProtocol protocol = KafkaProtocol.valueOf(protocolConfig.get("security_protocol").asText().toUpperCase());
    final ImmutableMap.Builder<String, Object> builder = ImmutableMap.<String, Object>builder()
        .put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, protocol.toString());

    switch (protocol) {
      case PLAINTEXT -> {}
      case SASL_SSL, SASL_PLAINTEXT -> {
        builder.put(SaslConfigs.SASL_JAAS_CONFIG, protocolConfig.get("sasl_jaas_config").asText());
        builder.put(SaslConfigs.SASL_MECHANISM, protocolConfig.get("sasl_mechanism").asText());
      }
      default -> throw new RuntimeException("Unexpected Kafka protocol: " + Jsons.serialize(protocol));
    }

    return builder.build();
  }

  protected KafkaConsumer<String, JsonNode> buildKafkaConsumer(final JsonNode config) {
    final Map<String, Object> props = new HashMap<>();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.get("bootstrap_servers").asText());
    props.put(ConsumerConfig.GROUP_ID_CONFIG,
        config.has("group_id") ? config.get("group_id").asText() : null);
    props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG,
        config.has("max_poll_records") ? config.get("max_poll_records").intValue() : null);
    props.putAll(propertiesByProtocol(config));
    props.put(ConsumerConfig.CLIENT_ID_CONFIG,
        config.has("client_id") ? config.get("client_id").asText() : null);
    props.put(ConsumerConfig.CLIENT_DNS_LOOKUP_CONFIG, config.get("client_dns_lookup").asText());
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, config.get("enable_auto_commit").booleanValue());
    props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG,
        config.has("auto_commit_interval_ms") ? config.get("auto_commit_interval_ms").intValue() : null);
    props.put(ConsumerConfig.RETRY_BACKOFF_MS_CONFIG,
        config.has("retry_backoff_ms") ? config.get("retry_backoff_ms").intValue() : null);
    props.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG,
        config.has("request_timeout_ms") ? config.get("request_timeout_ms").intValue() : null);
    props.put(ConsumerConfig.RECEIVE_BUFFER_CONFIG,
        config.has("receive_buffer_bytes") ? config.get("receive_buffer_bytes").intValue() : null);
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
        config.has("auto_offset_reset") ? config.get("auto_offset_reset").asText() : null);
    props.put("schema.registry.url",
        config.has("schema_registry_url") ? config.get("schema_registry_url").asText() : null);
    props.put("basic.auth.credentials.source",
        config.has("schema_registry_basic_auth_user_info") ? "USER_INFO" : null);
    props.put("basic.auth.user.info",
        config.has("schema_registry_basic_auth_user_info") ? config.get("schema_registry_basic_auth_user_info").asText() : null);
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class.getName());

    final Map<String, Object> filteredProps = props.entrySet().stream()
        .filter(entry -> entry.getValue() != null && !entry.getValue().toString().isBlank())
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

    return new KafkaConsumer<>(filteredProps);
  }


    public AutoCloseableIterator<AirbyteMessage> consumeMessages(final JsonNode config){
      final KafkaConsumer<String, JsonNode> consumer = buildKafkaConsumer(config);
      final List<ConsumerRecord<String, JsonNode>> recordsList = new ArrayList<>();

      final JsonNode subscription = config.get("subscription");
      LOGGER.info("Kafka subscribe method: {}", subscription.toString());
      switch (subscription.get("subscription_type").asText()) {
        case "subscribe" -> {
          final String topicPattern = subscription.get("topic_pattern").asText();
          consumer.subscribe(Pattern.compile(topicPattern));
          topicsToSubscribe = consumer.listTopics().keySet().stream()
              .filter(topic -> topic.matches(topicPattern))
              .collect(Collectors.toSet());
        }
        case "assign" -> {
          topicsToSubscribe = new HashSet<>();
          final String topicPartitions = subscription.get("topic_partitions").asText();
          final String[] topicPartitionsStr = topicPartitions.replaceAll("\\s+", "").split(",");
          final List<TopicPartition> topicPartitionList = Arrays.stream(topicPartitionsStr).map(topicPartition -> {
            final String[] pair = topicPartition.split(":");
            topicsToSubscribe.add(pair[0]);
            return new TopicPartition(pair[0], Integer.parseInt(pair[1]));
          }).collect(Collectors.toList());
          LOGGER.info("Topic-partition list: {}", topicPartitionList);
          consumer.assign(topicPartitionList);
        }
      }

      final int retry = config.has("repeated_calls") ? config.get("repeated_calls").intValue() : 0;
      int pollCount = 0;
      while (true) {
        final ConsumerRecords<String, JsonNode> consumerRecords = consumer.poll(Duration.of(100, ChronoUnit.MILLIS));
        if (consumerRecords.count() == 0) {
          pollCount++;
          if (pollCount > retry) {
            break;
          }
        }

        consumerRecords.forEach(record -> {
          LOGGER.info("Consumer Record: key - {}, value - {}, partition - {}, offset - {}",
              record.key(), record.value(), record.partition(), record.offset());
          recordsList.add(record);
        });
        consumer.commitAsync();
      }
      consumer.close();
      final Iterator<ConsumerRecord<String, JsonNode>> iterator = recordsList.iterator();

      return AutoCloseableIterators.fromIterator(new AbstractIterator<>() {

        @Override
        protected AirbyteMessage computeNext() {
          if (iterator.hasNext()) {
            final ConsumerRecord<String, JsonNode> record = iterator.next();
            return new AirbyteMessage()
                .withType(AirbyteMessage.Type.RECORD)
                .withRecord(new AirbyteRecordMessage()
                    .withStream(record.topic())
                    .withEmittedAt(Instant.now().toEpochMilli())
                    .withData(record.value()));
          }

          return endOfData();
        }

      });
  }
}