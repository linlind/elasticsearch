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

package org.elasticsearch.search.aggregations.bucket.geogrid;

import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregation.Type;
import org.elasticsearch.search.aggregations.NonCollectingAggregator;
import org.elasticsearch.search.aggregations.bucket.geogrid.GeoGridAggregatorBuilder.CellIdSource;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSource.GeoPoint;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregatorFactory;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;

import java.io.IOException;
import java.util.Collections;

public class GeoHashGridAggregatorFactory extends ValuesSourceAggregatorFactory<ValuesSource.GeoPoint, GeoHashGridAggregatorFactory> {

    private final int precision;
    private final int requiredSize;
    private final int shardSize;

    public GeoHashGridAggregatorFactory(String name, Type type, ValuesSourceConfig<GeoPoint> config, int precision, int requiredSize,
            int shardSize, AggregationContext context, AggregatorFactory<?> parent, AggregatorFactories.Builder subFactoriesBuilder)
            throws IOException {
        super(name, type, config, context, parent, subFactoriesBuilder);
        this.precision = precision;
        this.requiredSize = requiredSize;
        this.shardSize = shardSize;
    }

    @Override
    protected Aggregator createUnmapped(Aggregator parent) throws IOException {
        final InternalAggregation aggregation = new InternalGeoHashGrid(name, requiredSize,
                Collections.<InternalGeoHashGrid.Bucket> emptyList());
        return new NonCollectingAggregator(name, context, parent) {
            @Override
            public InternalAggregation buildEmptyAggregation() {
                return aggregation;
            }
        };
    }

    @Override
    protected Aggregator doCreateInternal(final ValuesSource.GeoPoint valuesSource, Aggregator parent, boolean collectsFromSingleBucket)
            throws IOException {
        if (collectsFromSingleBucket == false) {
            return asMultiBucketAggregator(this, context, parent);
        }
        CellIdSource cellIdSource = new CellIdSource(valuesSource, precision);
        return new GeoHashGridAggregator(name, factories, cellIdSource, requiredSize, shardSize, context, parent);

    }

}
