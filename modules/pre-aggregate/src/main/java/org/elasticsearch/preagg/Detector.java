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

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParseFieldMatcher;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import java.io.IOException;
import java.util.Objects;

public class Detector implements ToXContent, Writeable {
    public static final ParseField FIELD_NAME = new ParseField("field_name");
    public static final ParseField DETECTOR_DESCRIPTION = new ParseField("detector_description");
    public static final ParseField FUNCTION = new ParseField("function");
    public static final ParseField BY_FIELD = new ParseField("by_field");
    public static final ParseField OVER_FIELD = new ParseField("over_field");
    public static final ParseField PARTITION_FIELD = new ParseField("partition_field");

    private final String fieldName;
    private final String detectorDescription;
    private final String function;
    private final String byField;
    private final String overField;
    private final String partitionField;

    public Detector(String fieldName, String detectorDescription, String function, String byField, String overField,
            String partitionField) {
        this.fieldName = fieldName;
        this.detectorDescription = detectorDescription;
        this.function = function;
        this.byField = byField;
        this.overField = overField;
        this.partitionField = partitionField;
    }

    public Detector(XContentParser parser, ParseFieldMatcher parseFieldMatcher) throws IOException {
        String fieldName = null;
        String detectorDescription = null;
        String function = null;
        String byField = null;
        String overField = null;
        String partitionField = null;

        XContentParser.Token token = parser.currentToken();
        if (token != XContentParser.Token.START_OBJECT) {
            throw new ElasticsearchParseException("expected a string value or an object, but found [{}] instead", token);
        }
        String cfn = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                cfn = parser.currentName();
            } else if (parseFieldMatcher.match(cfn, FIELD_NAME)) {
                if (token == XContentParser.Token.VALUE_STRING) {
                    fieldName = parser.text();
                } else {
                    throw new ElasticsearchParseException("unexpected type [{}] for field [{}]", token, cfn);
                }
            } else if (parseFieldMatcher.match(cfn, DETECTOR_DESCRIPTION)) {
                if (token == XContentParser.Token.VALUE_STRING) {
                    detectorDescription = parser.text();
                } else {
                    throw new ElasticsearchParseException("unexpected type [{}] for field [{}]", token, cfn);
                }
            } else if (parseFieldMatcher.match(cfn, FUNCTION)) {
                if (token == XContentParser.Token.VALUE_STRING) {
                    function = parser.text();
                } else {
                    throw new ElasticsearchParseException("unexpected type [{}] for field [{}]", token, cfn);
                }
            } else if (parseFieldMatcher.match(cfn, BY_FIELD)) {
                if (token == XContentParser.Token.VALUE_STRING) {
                    byField = parser.text();
                } else {
                    throw new ElasticsearchParseException("unexpected type [{}] for field [{}]", token, cfn);
                }
            } else if (parseFieldMatcher.match(cfn, OVER_FIELD)) {
                if (token == XContentParser.Token.VALUE_STRING) {
                    overField = parser.text();
                } else {
                    throw new ElasticsearchParseException("unexpected type [{}] for field [{}]", token, cfn);
                }
            } else if (parseFieldMatcher.match(cfn, PARTITION_FIELD)) {
                if (token == XContentParser.Token.VALUE_STRING) {
                    partitionField = parser.text();
                } else {
                    throw new ElasticsearchParseException("unexpected type [{}] for field [{}]", token, cfn);
                }
            } else {
                throw new ElasticsearchParseException("unexpected field [{}]", cfn);
            }
        }
        this.fieldName = fieldName;
        this.detectorDescription = detectorDescription;
        this.function = function;
        this.byField = byField;
        this.overField = overField;
        this.partitionField = partitionField;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(DETECTOR_DESCRIPTION.getPreferredName(), detectorDescription);
        builder.field(FIELD_NAME.getPreferredName(), fieldName);
        builder.field(FUNCTION.getPreferredName(), function);
        builder.field(BY_FIELD.getPreferredName(), byField);
        builder.field(OVER_FIELD.getPreferredName(), overField);
        builder.field(PARTITION_FIELD.getPreferredName(), partitionField);
        builder.endObject();
        return builder;
    }

    public Detector(StreamInput in) throws IOException {
        detectorDescription = in.readOptionalString();
        fieldName = in.readOptionalString();
        function = in.readOptionalString();
        byField = in.readOptionalString();
        overField = in.readOptionalString();
        partitionField = in.readOptionalString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalString(detectorDescription);
        out.writeOptionalString(fieldName);
        out.writeOptionalString(function);
        out.writeOptionalString(byField);
        out.writeOptionalString(overField);
        out.writeOptionalString(partitionField);
    }

    public String getFieldName() {
        return fieldName;
    }

    public String getDetectorDescription() {
        return detectorDescription;
    }

    public String getFunction() {
        return function;
    }

    public String getByField() {
        return byField;
    }

    public String getOverField() {
        return overField;
    }

    public String getPartitionField() {
        return partitionField;
    }

    @Override
    public int hashCode() {
        return Objects.hash(detectorDescription, fieldName, function, byField, overField, partitionField);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if ((obj instanceof Detector) == false) {
            return false;
        }
        Detector other = (Detector) obj;
        return Objects.equals(detectorDescription, other.detectorDescription)
                && Objects.equals(fieldName, other.fieldName)
                && Objects.equals(function, other.function)
                && Objects.equals(byField, other.byField)
                && Objects.equals(overField, other.overField)
                && Objects.equals(partitionField, other.partitionField);
    }

}