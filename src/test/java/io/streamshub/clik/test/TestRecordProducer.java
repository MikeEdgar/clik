package io.streamshub.clik.test;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.IntStream;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.StringSerializer;
import org.jboss.logging.Logger;

public interface TestRecordProducer {

    static final Logger LOGGER = Logger.getLogger(TestRecordProducer.class);

    record TestHeader(
            String key,
            byte[] value
    ) {
        public static TestHeader of(String key, String value) {
            return new TestHeader(key, value.getBytes(StandardCharsets.UTF_8));
        }
    }

    record TestRecord(
            String topic,
            Integer partition,
            Long timestamp,
            List<TestHeader> headers,
            String key,
            String value
    ) {
        public static TestRecord of(String topic, String value) {
            return new TestRecord(topic, null, null, Collections.emptyList(), null, value);
        }

        public static TestRecord of(String topic, Integer partition, String value) {
            return new TestRecord(topic, partition, null, Collections.emptyList(), null, value);
        }

        public static TestRecord of(String topic, List<TestHeader> headers, String value) {
            return new TestRecord(topic, null, null, headers, null, value);
        }

        public static TestRecord of(String topic, String key, String value) {
            return new TestRecord(topic, null, null, Collections.emptyList(), key, value);
        }

        public static TestRecord of(String topic, Long timestamp, String key, String value) {
            return new TestRecord(topic, null, timestamp, Collections.emptyList(), key, value);
        }
    }

    String kafkaBootstrapServers();

    default void produceRecords(TestRecord... records) throws Exception {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrapServers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        CountDownLatch counter = new CountDownLatch(records.length);
        Function<TestRecord, Integer> partition;

        try (Producer<String, String> producer = new KafkaProducer<>(props)) {
            if (Arrays.stream(records).allMatch(rec -> rec.partition() == null)) {
                /*
                 * When none of the records specifies a partition, distribute them
                 * round robin to all partitions in the topic.
                 */
                String topic = Arrays.stream(records).map(TestRecord::topic)
                        .distinct()
                        .reduce((_, _) -> {
                            throw new IllegalStateException("More than one topic name not supported");
                        })
                        .orElse(null);
                int partitionCount = producer.partitionsFor(topic).size();
                AtomicInteger recordNumber = new AtomicInteger(0);
                partition = _ -> recordNumber.getAndIncrement() % partitionCount;
            } else {
                partition = TestRecord::partition;
            }

            for (TestRecord message : records) {
                var rec = new ProducerRecord<String, String>(
                        message.topic(),
                        partition.apply(message),
                        message.timestamp(),
                        message.key(),
                        message.value(),
                        message.headers().stream()
                            .reduce((Headers) new RecordHeaders(),
                                    (headers, header) -> headers.add(header.key(), header.value()),
                                    (h1, _) -> h1));

                producer.send(rec, (metadata, exception) -> {
                    if (exception != null) {
                        LOGGER.infof(exception, "Exception writing record %s", rec);
                    } else {
                        LOGGER.debugf("Wrote record at offset %d with timestamp %s: %s",
                                metadata.offset(),
                                Instant.ofEpochMilli(metadata.timestamp()),
                                rec);
                    }
                    counter.countDown();
                });
            }

            counter.await(10, TimeUnit.SECONDS);
        }
    }

    default void produceMessages(String topic, int count) throws Exception {
        var records = IntStream.range(0, count)
                .mapToObj(i -> TestRecord.of(topic, "key-" + i, "value-" + i))
                .toArray(TestRecord[]::new);
        produceRecords(records);
    }

    default void produceMessagesWithTimestamps(
            String topic,
            int count,
            long baseTimestamp,
            long incrementMs) throws Exception {

        var records = IntStream.range(0, count)
                .mapToObj(i -> {
                    long timestamp = baseTimestamp + (i * incrementMs);
                    return TestRecord.of(topic, timestamp, "key-" + i, "value-" + i);
                })
                .toArray(TestRecord[]::new);
        produceRecords(records);
    }

    default void produceMessages(String topic, String... messages) throws Exception {
        var records = Arrays.stream(messages)
                .map(msg -> TestRecord.of(topic, msg))
                .toArray(TestRecord[]::new);
        produceRecords(records);
    }

    /**
     * Helper method to produce messages to a specific partition
     */
    default void produceMessagesToPartition(String topic, int partition, String... messages) throws Exception {
        var records = Arrays.stream(messages)
                .map(msg -> TestRecord.of(topic, partition, msg))
                .toArray(TestRecord[]::new);
        produceRecords(records);
    }

    /**
     * Helper method to produce messages with headers
     */
    default void produceMessagesWithHeaders(String topic, List<TestHeader> headers, String... messages) throws Exception {
        var records = Arrays.stream(messages)
                .map(msg -> TestRecord.of(topic, headers, msg))
                .toArray(TestRecord[]::new);
        produceRecords(records);
    }
}
