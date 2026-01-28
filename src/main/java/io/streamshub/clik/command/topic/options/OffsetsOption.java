package io.streamshub.clik.command.topic.options;

import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.format.DateTimeParseException;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.function.LongSupplier;

import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.common.requests.ListOffsetsRequest;

import picocli.CommandLine.ITypeConverter;

public record OffsetsOption(String option, OffsetSpec spec) {
    private static final String EARLIEST = "earliest";
    private static final String LATEST = "latest";
    private static final String MAX_TIMESTAMP = "max-timestamp";
    private static final String EARLIEST_LOCAL = "earliest-local";
    private static final String LATEST_TIERED = "latest-tiered";
    //private static final String EARLIEST_PENDING_UPLOAD = "earliest-pending-upload";

    public static class Candidates implements Iterable<String> {
        @Override
        public Iterator<String> iterator() {
            return List.of(EARLIEST, LATEST, MAX_TIMESTAMP, EARLIEST_LOCAL, LATEST_TIERED).iterator();
        }
    }

    public static class Converter implements ITypeConverter<OffsetsOption> {
        @Override
        public OffsetsOption convert(String option) throws Exception {
            return valueOf(option);
        }

        public static OffsetsOption valueOf(String option) {
            var spec = switch (option) {
                case EARLIEST:
                    yield OffsetSpec.earliest();
                case LATEST:
                    yield OffsetSpec.latest();
                case MAX_TIMESTAMP:
                    yield OffsetSpec.maxTimestamp();
                case EARLIEST_LOCAL:
                    yield OffsetSpec.earliestLocal();
                case LATEST_TIERED:
                    yield OffsetSpec.latestTiered();
                //case EARLIEST_PENDING_UPLOAD:
                //    yield OffsetSpec.earliestPendingUpload();
                default:
                    long timestamp;

                    timestamp = parseFromDatetime(() -> Long.parseLong(option))
                            .or(() -> parseFromDatetime(() -> OffsetDateTime.parse(option).toInstant().toEpochMilli()))
                            .or(() -> parseFromDatetime(() -> Instant.now().minus(Duration.parse(option)).toEpochMilli()))
                            .orElseThrow(() -> new IllegalArgumentException("not a valid datetime or duration: " + option));

                    if (timestamp == ListOffsetsRequest.EARLIEST_TIMESTAMP) {
                        yield OffsetSpec.earliest();
                    } else if (timestamp == ListOffsetsRequest.LATEST_TIMESTAMP) {
                        yield OffsetSpec.latest();
                    } else if (timestamp == ListOffsetsRequest.MAX_TIMESTAMP) {
                        yield OffsetSpec.maxTimestamp();
                    } else if (timestamp == ListOffsetsRequest.EARLIEST_LOCAL_TIMESTAMP) {
                        yield OffsetSpec.earliestLocal();
                    } else if (timestamp == ListOffsetsRequest.LATEST_TIERED_TIMESTAMP) {
                        yield OffsetSpec.latestTiered();
                    //} else if (timestamp == ListOffsetsRequest.EARLIEST_PENDING_UPLOAD_TIMESTAMP) {
                    //    yield OffsetSpec.earliestPendingUpload();
                    } else {
                        yield OffsetSpec.forTimestamp(timestamp);
                    }
            };

            return new OffsetsOption(option, spec);
        }

        private static Optional<Long> parseFromDatetime(LongSupplier source) {
            try {
                return Optional.of(source.getAsLong());
            } catch (DateTimeParseException | NumberFormatException _) {
                return Optional.empty();
            }
        }
    }
}
