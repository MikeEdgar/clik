package io.streamshub.clik.kafka.model;

import java.util.Optional;

import org.apache.kafka.clients.admin.SharePartitionOffsetInfo;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;

public record GroupOffsetInfo(
        long offset,
        Optional<Integer> leaderEpoch,
        /**
         * Consumer Groups only
         */
        Optional<String> metadata,
        /**
         * Share Groups only
         */
        Optional<Long> lag
) {


    public static GroupOffsetInfo fromOffsetAndMetadata(OffsetAndMetadata info) {
        return new GroupOffsetInfo(
                info.offset(),
                info.leaderEpoch(),
                Optional.ofNullable(info.metadata()),
                Optional.empty()
        );
    }

    public static GroupOffsetInfo fromSharePartitionOffset(SharePartitionOffsetInfo info) {
        return new GroupOffsetInfo(
                info.startOffset(),
                info.leaderEpoch(),
                Optional.empty(),
                info.lag()
        );
    }
}
