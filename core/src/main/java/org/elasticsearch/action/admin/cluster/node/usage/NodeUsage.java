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

package org.elasticsearch.action.admin.cluster.node.usage;

import org.elasticsearch.action.support.nodes.BaseNodeResponse;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Map;

public class NodeUsage extends BaseNodeResponse implements ToXContent {

    private long timestamp;
    private long sinceTime;
    private Map<String, Long> actionUsage;

    NodeUsage() {
    }

    public static NodeUsage readNodeStats(StreamInput in) throws IOException {
        NodeUsage nodeInfo = new NodeUsage();
        nodeInfo.readFrom(in);
        return nodeInfo;
    }

    public NodeUsage(DiscoveryNode node, long timestamp, long sinceTime, Map<String, Long> actionUsage) {
        super(node);
        this.timestamp = timestamp;
        this.sinceTime = sinceTime;
        this.actionUsage = actionUsage;
    }

    public long getTimestamp() {
        return timestamp;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field("since", sinceTime);
        builder.field("usage");
        builder.map(actionUsage);
        return builder;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        timestamp = in.readLong();
        sinceTime = in.readLong();
        actionUsage = (Map<String, Long>) in.readGenericValue();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeLong(timestamp);
        out.writeLong(sinceTime);
        out.writeGenericValue(actionUsage);
    }

}
