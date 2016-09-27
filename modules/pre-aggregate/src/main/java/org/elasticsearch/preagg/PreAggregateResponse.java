/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.preagg;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class PreAggregateResponse extends ActionResponse implements ToXContent {
    public static final ParseField TOOK = new ParseField("took");
    public static final ParseField DOC_COUNT = new ParseField("doc_count");
    public static final ParseField BUCKETS = new ParseField("buckets");

    private final List<Bucket> buckets;
    private final long took;
    private final long docCount;

    public PreAggregateResponse() {
        this.buckets = null;
        this.took = -1;
        this.docCount = -1;
    }

    public PreAggregateResponse(List<Bucket> aggregations, long took, long docCount) {
        this.buckets = aggregations;
        this.took = took;
        this.docCount = docCount;
    }

    public PreAggregateResponse(StreamInput in) throws IOException {
        this.buckets = in.readList(Bucket::new);
        this.took = in.readVLong();
        this.docCount = in.readVLong();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeList(buckets);
        out.writeVLong(took);
        out.writeVLong(docCount);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field(TOOK.getPreferredName(), took);
        builder.field(DOC_COUNT.getPreferredName(), docCount);
        builder.field(BUCKETS.getPreferredName(), buckets);
        return builder;
    }

    public static final class Bucket implements ToXContent, Writeable {
        public static final ParseField DATE = new ParseField("date");
        public static final ParseField DETECTOR_RECORDS = new ParseField("detector_records");

        private final long date;
        private final long docCount;
        private final List<List<DetectorRecord>> detectorRecords;

        public Bucket(long date, long docCount, List<List<DetectorRecord>> detectorRecords) {
            this.date = date;
            this.docCount = docCount;
            this.detectorRecords = detectorRecords;
        }

        public Bucket(StreamInput in) throws IOException {
            date = in.readLong();
            docCount = in.readVLong();
            int size = in.readVInt();
            List<List<DetectorRecord>> detectorRecords = new ArrayList<>(size);
            for (int i = 0; i < size; i++) {
                detectorRecords.add(in.readList(DetectorRecord::new));
            }
            this.detectorRecords = detectorRecords;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeLong(date);
            out.writeVLong(docCount);
            out.writeVInt(detectorRecords.size());
            for (List<DetectorRecord> records : detectorRecords) {
                out.writeList(records);
            }
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(DATE.getPreferredName(), date);
            builder.field(DOC_COUNT.getPreferredName(), docCount);
            builder.field(DETECTOR_RECORDS.getPreferredName(), detectorRecords);
            builder.endObject();
            return builder;
        }

        public long getDate() {
            return date;
        }

        public long getDocCount() {
            return docCount;
        }

        public List<List<DetectorRecord>> getDetectorRecords() {
            return detectorRecords;
        }

        @Override
        public int hashCode() {
            return Objects.hash(date, docCount, detectorRecords);
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            Bucket other = (Bucket) obj;
            return Objects.equals(date, other.date)
                    && Objects.equals(detectorRecords, other.detectorRecords)
                    && Objects.equals(docCount, other.docCount);
        }
    }

    public static final class DetectorRecord implements ToXContent, Writeable {

        public static final class Builder {
            private AggregateValue functionValue;
            private String byValue;
            private String overValue;
            private String partitionValue;
            private List<List<AggregateValue>> influencerValues;

            public Builder functionValue(AggregateValue functionValue) {
                this.functionValue = functionValue;
                return this;
            }

            public Builder byValue(String byValue) {
                this.byValue = byValue;
                return this;
            }

            public Builder overValue(String overValue) {
                this.overValue = overValue;
                return this;
            }

            public Builder partitionValue(String partitionValue) {
                this.partitionValue = partitionValue;
                return this;
            }

            public Builder influencerValues(List<List<AggregateValue>> influencerValues) {
                this.influencerValues = influencerValues;
                return this;
            }

            public DetectorRecord build() {
                return new DetectorRecord(functionValue, byValue, overValue, partitionValue, influencerValues);
            }
        }

        public static final ParseField FUNCTION_VALUE = new ParseField("function_value");
        public static final ParseField BY_VALUE = new ParseField("by_value");
        public static final ParseField OVER_VALUE = new ParseField("over_value");
        public static final ParseField PARTITION_VALUE = new ParseField("partition_value");
        public static final ParseField COUNT_VALUE = new ParseField("count_value");
        public static final ParseField INFLUENCER_VALUES = new ParseField("influencer_values");

        private final AggregateValue functionValue;
        private final String byValue;
        private final String overValue;
        private final String partitionValue;
        private final List<List<AggregateValue>> influencerValues;

        public DetectorRecord(AggregateValue functionValue, String byValue, String overValue, String partitionValue,
                List<List<AggregateValue>> influencerValues) {
            this.functionValue = functionValue;
            this.byValue = byValue;
            this.overValue = overValue;
            this.partitionValue = partitionValue;
            this.influencerValues = influencerValues;
        }

        public DetectorRecord(StreamInput in) throws IOException {
            this.functionValue = new AggregateValue(in);
            this.byValue = in.readString();
            this.overValue = in.readString();
            this.partitionValue = in.readString();
            int size = in.readVInt();
            List<List<AggregateValue>> influencerValues = new ArrayList<>(size);
            for (int i = 0; i < size; i++) {
                influencerValues.add(in.readList(AggregateValue::new));
            }
            this.influencerValues = influencerValues;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(byValue);
            out.writeString(overValue);
            out.writeString(partitionValue);
            functionValue.writeTo(out);
            out.writeVInt(influencerValues.size());
            for (List<AggregateValue> values : influencerValues) {
                out.writeList(values);
            }
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(FUNCTION_VALUE.getPreferredName(), functionValue);
            builder.field(BY_VALUE.getPreferredName(), byValue);
            builder.field(OVER_VALUE.getPreferredName(), overValue);
            builder.field(PARTITION_VALUE.getPreferredName(), partitionValue);
            builder.field(INFLUENCER_VALUES.getPreferredName(), influencerValues);
            builder.endObject();
            return builder;
        }

        public AggregateValue getFunctionValue() {
            return functionValue;
        }

        public String getByValue() {
            return byValue;
        }

        public String getOverValue() {
            return overValue;
        }

        public String getPartitionValue() {
            return partitionValue;
        }

        public List<List<AggregateValue>> getInfluencerValues() {
            return influencerValues;
        }
    }

    public static final class AggregateValue implements Writeable, ToXContent {

        public static final ParseField KEY = new ParseField("key");

        private final String key;
        private final Object value;
        private final long count;

        public AggregateValue(Object value, long count) {
            this(null, value, count);
        }

        public AggregateValue(String key, Object value, long count) {
            this.key = key;
            this.value = value;
            this.count = count;
        }

        public AggregateValue(StreamInput in) throws IOException {
            this.key = in.readOptionalString();
            this.value = in.readGenericValue();
            this.count = in.readVLong();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeOptionalString(key);
            out.writeGenericValue(value);
            out.writeVLong(count);
        }

        public Object getValue() {
            return value;
        }

        public long getCount() {
            return count;
        }

        public String getKey() {
            return key;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            if (this.key != null) {
                builder.field(KEY.getPreferredName(), key);
            }
            builder.field(DetectorRecord.FUNCTION_VALUE.getPreferredName(), value);
            builder.field(DetectorRecord.COUNT_VALUE.getPreferredName(), count);
            builder.endObject();
            return builder;
        }
    }
}
