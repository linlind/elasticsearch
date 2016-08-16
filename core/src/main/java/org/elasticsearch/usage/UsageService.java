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

package org.elasticsearch.usage;

import org.elasticsearch.action.admin.cluster.node.usage.NodeUsage;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.discovery.Discovery;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class UsageService extends AbstractComponent {

    private final Discovery discovery;
    private final Map<String, AtomicLong> actionUsage;
    private long sinceTime;

    @Inject
    public UsageService(Discovery discovery, Settings settings) {
        super(settings);
        this.discovery = discovery;
        this.actionUsage = new ConcurrentHashMap<>();
        this.sinceTime = System.currentTimeMillis();
    }

    public void addActionCall(String actionName) {
        AtomicLong counter = actionUsage.computeIfAbsent(actionName, key -> new AtomicLong());
        counter.getAndIncrement();
    }

    public NodeUsage getUsageStats() {
        Map<String, Long> actionUsageMap = new HashMap<>();
        actionUsage.forEach((key, value) -> {
            actionUsageMap.put(key, value.get());
        });
        return new NodeUsage(discovery.localNode(), System.currentTimeMillis(), sinceTime, actionUsageMap);
    }

}
