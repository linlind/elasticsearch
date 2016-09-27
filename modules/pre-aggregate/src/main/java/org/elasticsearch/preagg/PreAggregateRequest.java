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
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParseFieldMatcher;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.indices.query.IndicesQueriesRegistry;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class PreAggregateRequest extends ActionRequest<PreAggregateRequest> {

    public static final ParseField QUERY = new ParseField("query");
    public static final ParseField DETECTORS = new ParseField("detectors");
    public static final ParseField INFLUENCERS = new ParseField("influencers");
    public static final ParseField TIME_FIELD = new ParseField("time_field");
    public static final ParseField FROM = new ParseField("from");
    public static final ParseField TO = new ParseField("to");
    public static final ParseField BUCKET_SPAN = new ParseField("bucket_span");

    private String indices;
    private QueryBuilder query;
    private List<Detector> detectors;
    private List<String> influencers;
    private String timeField;
    private long from;
    private long to;
    private long bucketSpan;

    PreAggregateRequest() {
    }

    PreAggregateRequest(String indices, XContentParser parser, ParseFieldMatcher parseFieldMatcher,
            IndicesQueriesRegistry indicesQueriesRegistry)
            throws IOException {
        this.indices = indices;
        QueryParseContext context = new QueryParseContext(indicesQueriesRegistry, parser, parseFieldMatcher);
        XContentParser.Token token = parser.nextToken();
        if (token != XContentParser.Token.START_OBJECT) {
            throw new ElasticsearchParseException("expected a string value or an object, but found [{}] instead", token);
        }
        String cfn = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                cfn = parser.currentName();
            } else if (parseFieldMatcher.match(cfn, QUERY)) {
                if (token == XContentParser.Token.START_OBJECT) {
                    query = context.parseInnerQueryBuilder().orElse(QueryBuilders.matchAllQuery());
                } else {
                    throw new ElasticsearchParseException("unexpected type [{}] for field [{}]", token, cfn);
                }
            } else if (parseFieldMatcher.match(cfn, DETECTORS)) {
                if (token == XContentParser.Token.START_ARRAY) {
                    List<Detector> detectors = new ArrayList<>();
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        detectors.add(new Detector(parser, parseFieldMatcher));
                    }
                    this.detectors = detectors;
                } else {
                    throw new ElasticsearchParseException("unexpected type [{}] for field [{}]", token, cfn);
                }
            } else if (parseFieldMatcher.match(cfn, INFLUENCERS)) {
                if (token == XContentParser.Token.START_ARRAY) {
                    List<String> influencers = new ArrayList<>();
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        influencers.add(parser.text());
                    }
                    this.influencers = influencers;
                } else {
                    throw new ElasticsearchParseException("unexpected type [{}] for field [{}]", token, cfn);
                }
            } else if (parseFieldMatcher.match(cfn, TIME_FIELD)) {
                if (token == XContentParser.Token.VALUE_STRING) {
                    timeField = parser.text();
                } else {
                    throw new ElasticsearchParseException("unexpected type [{}] for field [{}]", token, cfn);
                }
            } else if (parseFieldMatcher.match(cfn, FROM)) {
                if (token == XContentParser.Token.VALUE_NUMBER) {
                    from = parser.longValue();
                } else {
                    throw new ElasticsearchParseException("unexpected type [{}] for field [{}]", token, cfn);
                }
            } else if (parseFieldMatcher.match(cfn, TO)) {
                if (token == XContentParser.Token.VALUE_NUMBER) {
                    to = parser.longValue();
                } else {
                    throw new ElasticsearchParseException("unexpected type [{}] for field [{}]", token, cfn);
                }
            } else if (parseFieldMatcher.match(cfn, BUCKET_SPAN)) {
                if (token == XContentParser.Token.VALUE_NUMBER) {
                    bucketSpan = parser.longValue();
                } else {
                    throw new ElasticsearchParseException("unexpected type [{}] for field [{}]", token, cfn);
                }
            } else {
                throw new ElasticsearchParseException("unexpected field [{}]", cfn);
            }
        }
    }

    public PreAggregateRequest(String indices) {
        this.indices = indices;
    }

    public String getIndices() {
        return indices;
    }
    public QueryBuilder getQuery() {
        return query;
    }

    public PreAggregateRequest setQuery(QueryBuilder query) {
        this.query = query;
        return this;
    }

    public List<Detector> getDetectors() {
        return detectors;
    }

    public PreAggregateRequest setDetectors(List<Detector> detectors) {
        this.detectors = detectors;
        return this;
    }

    public List<String> getInfluencers() {
        return influencers;
    }

    public PreAggregateRequest setInfluencers(List<String> influencers) {
        this.influencers = influencers;
        return this;
    }

    public String getTimeField() {
        return timeField;
    }

    public PreAggregateRequest setTimeField(String timeField) {
        this.timeField = timeField;
        return this;
    }

    public long getFrom() {
        return from;
    }

    public PreAggregateRequest setFrom(long from) {
        this.from = from;
        return this;
    }

    public long getTo() {
        return to;
    }

    public PreAggregateRequest setTo(long to) {
        this.to = to;
        return this;
    }

    public long getBucketSpan() {
        return bucketSpan;
    }

    public PreAggregateRequest setBucketSpan(long bucketSpan) {
        this.bucketSpan = bucketSpan;
        return this;
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(bucketSpan);
        out.writeList(detectors);
        out.writeLong(from);
        out.writeString(indices);
        out.writeStringArray(influencers.toArray(new String[influencers.size()]));
        out.writeNamedWriteable(query);
        out.writeString(timeField);
        out.writeLong(to);
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        bucketSpan = in.readVLong();
        detectors = in.readList(Detector::new);
        from = in.readLong();
        indices = in.readString();
        influencers = Arrays.asList(in.readStringArray());
        query = in.readNamedWriteable(QueryBuilder.class);
        timeField = in.readString();
        to = in.readLong();
    }

}
