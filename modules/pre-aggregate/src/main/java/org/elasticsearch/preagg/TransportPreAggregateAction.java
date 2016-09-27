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

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.TransportSearchAction;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.preagg.PreAggregateResponse.AggregateValue;
import org.elasticsearch.preagg.PreAggregateResponse.Bucket;
import org.elasticsearch.preagg.PreAggregateResponse.DetectorRecord;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms.Order;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.NumericMetricsAggregation;
import org.elasticsearch.search.aggregations.metrics.percentiles.Percentiles;
import org.elasticsearch.search.aggregations.metrics.stats.extended.ExtendedStats;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.joda.time.DateTime;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class TransportPreAggregateAction extends HandledTransportAction<PreAggregateRequest, PreAggregateResponse> {

    private static final String INFLUENCER_AGG_NAME = "influencer";
    private static final String COUNT_AGG_NAME = "count";
    private static final String FUNCTION_AGG_NAME = "function";
    private static final String PARTITION_AGG_NAME = "Partition";
    private static final String OVER_AGG_NAME = "Over";
    private static final String BY_AGG_NAME = "By";
    private static final String HISTO_AGG_NAME = "histo";
    private TransportSearchAction searchAction;

    @Inject
    public TransportPreAggregateAction(Settings settings, ThreadPool threadPool, TransportSearchAction transportSearchAction,
            TransportService transportService, ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver) {
        super(settings, PreAggregateAction.NAME, threadPool, transportService, actionFilters, indexNameExpressionResolver,
                PreAggregateRequest::new);
        this.searchAction = transportSearchAction;
    }

    @Override
    protected void doExecute(PreAggregateRequest request, ActionListener<PreAggregateResponse> listener) {
        long start = System.currentTimeMillis();
        SearchRequest searchRequest = new SearchRequest(request.getIndices());
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.size(0);
        BoolQueryBuilder completeQuery = QueryBuilders.boolQuery();
        completeQuery.must(request.getQuery());
        completeQuery.must(QueryBuilders.rangeQuery(request.getTimeField()).from(request.getFrom()).to(request.getTo()));
        sourceBuilder.query(completeQuery);
        AggregationBuilder histoAgg = AggregationBuilders.dateHistogram(HISTO_AGG_NAME).interval(request.getBucketSpan())
                .field(request.getTimeField());
        for (int i = 0; i < request.getDetectors().size(); i++) {
            Detector detector = request.getDetectors().get(i);
            AggregationBuilder detectorAgg = histoAgg;

            if (detector.getByField() != null) {
                TermsAggregationBuilder newAgg = AggregationBuilders.terms(BY_AGG_NAME + "-" + detector.getByField() + "-" + i)
                        .field(detector.getByField()).size(1000).order(Order.term(true))
                        .setMetaData(Collections.singletonMap(Detector.DETECTOR_DESCRIPTION.getPreferredName(), i));
                detectorAgg.subAggregation(newAgg);
                detectorAgg = newAgg;
            }
            if (detector.getOverField() != null) {
                TermsAggregationBuilder newAgg = AggregationBuilders.terms(OVER_AGG_NAME + "-" + detector.getOverField() + "-" + i)
                        .field(detector.getOverField()).size(1000).order(Order.term(true))
                        .setMetaData(Collections.singletonMap(Detector.DETECTOR_DESCRIPTION.getPreferredName(), i));
                detectorAgg = detectorAgg.subAggregation(newAgg);
                detectorAgg = newAgg;
            }
            if (detector.getPartitionField() != null) {
                TermsAggregationBuilder newAgg = AggregationBuilders
                        .terms(PARTITION_AGG_NAME + "-" + detector.getPartitionField() + "-" + i).field(detector.getPartitionField())
                        .size(1000).order(Order.term(true))
                        .setMetaData(Collections.singletonMap(Detector.DETECTOR_DESCRIPTION.getPreferredName(), i));
                detectorAgg = detectorAgg.subAggregation(newAgg);
                detectorAgg = newAgg;
            }
            List<AggregationBuilder> functionAggs = createDetectorLeafAggs(i, detector, request.getInfluencers());
            for (AggregationBuilder functionAgg : functionAggs) {
                detectorAgg.subAggregation(functionAgg);
            }
        }
        sourceBuilder.aggregation(histoAgg);
        searchRequest.source(sourceBuilder);
        searchAction.execute(searchRequest, new ActionListener<SearchResponse>() {

            @Override
            public void onResponse(SearchResponse response) {
                long end = System.currentTimeMillis();
                long took = end - start;
                listener.onResponse(new PreAggregateResponse(
                        buildBuckets(response.getAggregations(), request.getDetectors(), request.getInfluencers()), took,
                        response.getHits().totalHits()));
            }

            @Override
            public void onFailure(Exception e) {
                e.printStackTrace();
                listener.onFailure(e);
            }
        });
    }

    private List<Bucket> buildBuckets(Aggregations aggregations, List<Detector> detectors, List<String> influencers) {
        Histogram histo = aggregations.get(HISTO_AGG_NAME);
        List<Histogram.Bucket> histoBuckets = histo.getBuckets();
        List<Bucket> buckets = new ArrayList<>(histoBuckets.size());
        for (Histogram.Bucket histoBucket : histoBuckets) {
            DateTime date = (DateTime) histoBucket.getKey();
            List<List<DetectorRecord>> records = new ArrayList<>(detectors.size());
            DetectorRecord.Builder detectorRecordBuilder = new DetectorRecord.Builder();
            for (int i = 0; i < detectors.size(); i++) {
                List<DetectorRecord> detectorRecords = new ArrayList<>();
                Detector detector = detectors.get(i);
                buildByDetectorRecords(i, detector, influencers, detectorRecordBuilder, histoBucket.getAggregations(), detectorRecords);
                records.add(detectorRecords);
            }
            Bucket bucket = new Bucket(date.getMillis(), histoBucket.getDocCount(), records);
            buckets.add(bucket);
        }
        return buckets;
    }

    private void buildByDetectorRecords(int index, Detector detector, List<String> influencers,
            DetectorRecord.Builder detectorRecordBuilder, Aggregations currentAggs, List<DetectorRecord> detectorRecords) {
        if (detector.getByField() != null) {
            Terms byAgg = currentAggs.get(BY_AGG_NAME + "-" + detector.getByField() + "-" + index);
            List<Terms.Bucket> byBuckets = byAgg.getBuckets();
            for (Terms.Bucket bucket : byBuckets) {
                detectorRecordBuilder.byValue(bucket.getKeyAsString());
                buildOverDetectorRecords(index, detector, influencers, detectorRecordBuilder, bucket.getAggregations(), detectorRecords);
            }
        } else {
            buildOverDetectorRecords(index, detector, influencers, detectorRecordBuilder, currentAggs, detectorRecords);
        }
    }

    private void buildOverDetectorRecords(int index, Detector detector, List<String> influencers,
            DetectorRecord.Builder detectorRecordBuilder, Aggregations currentAggs, List<DetectorRecord> detectorRecords) {
        if (detector.getOverField() != null) {
            Terms overAgg = currentAggs.get(OVER_AGG_NAME + "-" + detector.getOverField() + "-" + index);
            List<Terms.Bucket> overBuckets = overAgg.getBuckets();
            for (Terms.Bucket bucket : overBuckets) {
                detectorRecordBuilder.overValue(bucket.getKeyAsString());
                buildPartitionDetectorRecords(index, detector, influencers, detectorRecordBuilder, bucket.getAggregations(),
                        detectorRecords);
            }
        } else {
            buildPartitionDetectorRecords(index, detector, influencers, detectorRecordBuilder, currentAggs, detectorRecords);
        }
    }

    private void buildPartitionDetectorRecords(int index, Detector detector, List<String> influencers,
            DetectorRecord.Builder detectorRecordBuilder, Aggregations currentAggs, List<DetectorRecord> detectorRecords) {
        if (detector.getPartitionField() != null) {
            Terms partitionAgg = currentAggs.get(PARTITION_AGG_NAME + "-" + detector.getPartitionField() + "-" + index);
            List<Terms.Bucket> partitionBuckets = partitionAgg.getBuckets();
            for (Terms.Bucket bucket : partitionBuckets) {
                detectorRecordBuilder.partitionValue(bucket.getKeyAsString());
                buildLeafDetectorRecords(index, detector, influencers, detectorRecordBuilder, bucket.getAggregations(), detectorRecords);
            }
        } else {
            buildLeafDetectorRecords(index, detector, influencers, detectorRecordBuilder, currentAggs, detectorRecords);
        }
    }

    private void buildLeafDetectorRecords(int index, Detector detector, List<String> influencers,
            DetectorRecord.Builder detectorRecordBuilder, Aggregations leafAggs, List<DetectorRecord> detectorRecords) {
        NumericMetricsAggregation.SingleValue countAgg = leafAggs.get(COUNT_AGG_NAME + "-" + index);
        AggregateValue aggValue = new AggregateValue(getFunctionAggValue(index, detector, leafAggs), (long) countAgg.value());
        detectorRecordBuilder.functionValue(aggValue);
        // NOCOMMIT add influencer data here
        List<List<AggregateValue>> influencerValues = new ArrayList<>(influencers.size());
        for (String influencer : influencers) {
            Terms influencerTerms = leafAggs.get(INFLUENCER_AGG_NAME + "-" + influencer + "-" + index);
            List<Terms.Bucket> influencerTermsBuckets = influencerTerms.getBuckets();
            List<AggregateValue> values = new ArrayList<>(influencerTermsBuckets.size());
            for (Terms.Bucket termsBucket : influencerTermsBuckets) {
                String key = termsBucket.getKeyAsString();
                Aggregations influencerSubAggs = termsBucket.getAggregations();
                NumericMetricsAggregation.SingleValue influencerCountAgg = influencerSubAggs.get(COUNT_AGG_NAME + "-" + index);
                double influencerFunctionAggValue = getFunctionAggValue(index, detector, leafAggs);
                AggregateValue influencerValue = new AggregateValue(key, influencerFunctionAggValue, (long) influencerCountAgg.value());
                values.add(influencerValue);
            }
            influencerValues.add(values);
        }
        detectorRecordBuilder.influencerValues(influencerValues);
        detectorRecords.add(detectorRecordBuilder.build());
    }

    private double getFunctionAggValue(int index, Detector detector, Aggregations leafAggs) {
        Aggregation functionAgg = leafAggs.get(FUNCTION_AGG_NAME + "-" + detector.getFunction() + "-" + index);
        switch (detector.getFunction()) {
        case "count":
        case "mean":
        case "min":
        case "max":
        case "sum":
        case "dc":
            NumericMetricsAggregation.SingleValue metricAgg = (NumericMetricsAggregation.SingleValue) functionAgg;
            return metricAgg.value();
        case "median":
            Percentiles percentileAgg = (Percentiles) functionAgg;
            return percentileAgg.percentile(50.0);
        case "varp":
            ExtendedStats extendedStatsAgg = (ExtendedStats) functionAgg;
            return extendedStatsAgg.getVariance();
        case "info_content":
            throw new UnsupportedOperationException(detector.getFunction() + " not yet implemented");
        default:
            throw new IllegalStateException(detector.getFunction() + " not supported");
        }
    }

    private List<AggregationBuilder> createDetectorLeafAggs(int index, Detector detector, List<String> influencers) {
        List<AggregationBuilder> aggs = new ArrayList<>();
        AggregationBuilder countAgg = AggregationBuilders.count(COUNT_AGG_NAME + "-" + index).field("_index")
                .setMetaData(Collections.singletonMap(Detector.DETECTOR_DESCRIPTION.getPreferredName(), index));
        aggs.add(countAgg);
        AggregationBuilder detectorAgg = createFunctionAgg(index, detector);
        aggs.add(detectorAgg);
        for (String influencer : influencers) {
            AggregationBuilder influencersAgg = AggregationBuilders.terms(INFLUENCER_AGG_NAME + "-" + influencer + "-" + index)
                    .field(influencer).size(1000)
                    .setMetaData(Collections.singletonMap(Detector.DETECTOR_DESCRIPTION.getPreferredName(), index));
            influencersAgg.subAggregation(AggregationBuilders.count(COUNT_AGG_NAME + "-" + index).field("_index")
                    .setMetaData(Collections.singletonMap(Detector.DETECTOR_DESCRIPTION.getPreferredName(), index)));
            influencersAgg.subAggregation(createFunctionAgg(index, detector));
            aggs.add(influencersAgg);
        }
        return aggs;
    }

    private AggregationBuilder createFunctionAgg(int index, Detector detector) {
        AggregationBuilder detectorAgg;
        switch (detector.getFunction()) {
        case "count":
            // TODO optimise this. It runs on the _index field at the moment
            // as we know this will only have 1 value per doc.
            detectorAgg = AggregationBuilders.count(FUNCTION_AGG_NAME + "-" + detector.getFunction() + "-" + index).field("_index");
            break;
        case "mean":
            detectorAgg = AggregationBuilders.avg(FUNCTION_AGG_NAME + "-" + detector.getFunction() + "-" + index)
                    .field(detector.getFieldName());
            break;
        case "min":
            detectorAgg = AggregationBuilders.min(FUNCTION_AGG_NAME + "-" + detector.getFunction() + "-" + index)
                    .field(detector.getFieldName());
            break;
        case "max":
            detectorAgg = AggregationBuilders.max(FUNCTION_AGG_NAME + "-" + detector.getFunction() + "-" + index)
                    .field(detector.getFieldName());
            break;
        case "dc":
            detectorAgg = AggregationBuilders.cardinality(FUNCTION_AGG_NAME + "-" + detector.getFunction() + "-" + index)
                    .field(detector.getFieldName());
            break;
        case "median":
            detectorAgg = AggregationBuilders.percentiles(FUNCTION_AGG_NAME + "-" + detector.getFunction() + "-" + index)
                    .field(detector.getFieldName()).percentiles(50.0);
            break;
        case "sum":
            detectorAgg = AggregationBuilders.sum(FUNCTION_AGG_NAME + "-" + detector.getFunction() + "-" + index)
                    .field(detector.getFieldName());
            break;
        case "varp":
            detectorAgg = AggregationBuilders.extendedStats(FUNCTION_AGG_NAME + "-" + detector.getFunction() + "-" + index)
                    .field(detector.getFieldName());
            break;
        case "info_content":
            detectorAgg = AggregationBuilders.terms(FUNCTION_AGG_NAME + "-" + detector.getFunction() + "-" + index)
                    .field(detector.getFieldName()).size(1000).order(Order.term(true));
            break;
        default:
            throw new IllegalStateException(detector.getFunction() + " not supported");
        }
        detectorAgg.setMetaData(Collections.singletonMap(Detector.DETECTOR_DESCRIPTION.getPreferredName(), index));
        return detectorAgg;
    }
}
