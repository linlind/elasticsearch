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

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.indices.query.IndicesQueriesRegistry;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestRequest.Method;
import org.elasticsearch.rest.action.RestActions;
import org.elasticsearch.rest.action.RestToXContentListener;

public class RestPreAggregateAction extends BaseRestHandler {

    private IndicesQueriesRegistry indicesQueriesRegistry;

    @Inject
    public RestPreAggregateAction(Settings settings, RestController controller, IndicesQueriesRegistry indicesQueriesRegistry) {
        super(settings);
        this.indicesQueriesRegistry = indicesQueriesRegistry;
        controller.registerHandler(Method.GET, "_preagg", this);
        controller.registerHandler(Method.POST, "_preagg", this);
        controller.registerHandler(Method.GET, "{indices}/_preagg", this);
        controller.registerHandler(Method.POST, "{indices}/_preagg", this);
    }

    @Override
    public void handleRequest(RestRequest request, RestChannel channel, NodeClient client) throws Exception {
        BytesReference requestBytes = RestActions.getRestContent(request);
        String indices = request.param("indices", "_all");
        try (XContentParser parser = XContentFactory.xContent(requestBytes).createParser(requestBytes)) {
            PreAggregateRequest preAggRequest = new PreAggregateRequest(indices, parser, parseFieldMatcher, indicesQueriesRegistry);
            client.execute(PreAggregateAction.INSTANCE, preAggRequest, new RestToXContentListener<PreAggregateResponse>(channel));
        }
    }

}
