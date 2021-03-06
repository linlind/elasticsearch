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

package org.elasticsearch.cluster.routing.allocation;

import org.elasticsearch.cluster.ClusterInfoService;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.health.ClusterStateHealth;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.routing.AllocationId;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.RoutingNodes;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.routing.UnassignedInfo.AllocationStatus;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation.Result;
import org.elasticsearch.cluster.routing.allocation.allocator.ShardsAllocator;
import org.elasticsearch.cluster.routing.allocation.command.AllocationCommands;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDeciders;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.gateway.GatewayAllocator;
import org.elasticsearch.index.shard.ShardId;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.elasticsearch.cluster.routing.UnassignedInfo.INDEX_DELAYED_NODE_LEFT_TIMEOUT_SETTING;


/**
 * This service manages the node allocation of a cluster. For this reason the
 * {@link AllocationService} keeps {@link AllocationDeciders} to choose nodes
 * for shard allocation. This class also manages new nodes joining the cluster
 * and rerouting of shards.
 */
public class AllocationService extends AbstractComponent {

    private final AllocationDeciders allocationDeciders;
    private final GatewayAllocator gatewayAllocator;
    private final ShardsAllocator shardsAllocator;
    private final ClusterInfoService clusterInfoService;
    private final ClusterName clusterName;

    @Inject
    public AllocationService(Settings settings, AllocationDeciders allocationDeciders, GatewayAllocator gatewayAllocator,
                             ShardsAllocator shardsAllocator, ClusterInfoService clusterInfoService) {
        super(settings);
        this.allocationDeciders = allocationDeciders;
        this.gatewayAllocator = gatewayAllocator;
        this.shardsAllocator = shardsAllocator;
        this.clusterInfoService = clusterInfoService;
        clusterName = ClusterName.CLUSTER_NAME_SETTING.get(settings);
    }

    /**
     * Applies the started shards. Note, only initializing ShardRouting instances that exist in the routing table should be
     * provided as parameter and no duplicates should be contained.
     * <p>
     * If the same instance of the routing table is returned, then no change has been made.</p>
     */
    public Result applyStartedShards(ClusterState clusterState, List<ShardRouting> startedShards) {
        return applyStartedShards(clusterState, startedShards, true);
    }

    public Result applyStartedShards(ClusterState clusterState, List<ShardRouting> startedShards, boolean withReroute) {
        if (startedShards.isEmpty()) {
            return new Result(false, clusterState.routingTable(), clusterState.metaData());
        }
        RoutingNodes routingNodes = getMutableRoutingNodes(clusterState);
        // shuffle the unassigned nodes, just so we won't have things like poison failed shards
        routingNodes.unassigned().shuffle();
        StartedRerouteAllocation allocation = new StartedRerouteAllocation(allocationDeciders, routingNodes, clusterState, startedShards,
            clusterInfoService.getClusterInfo(), currentNanoTime());
        applyStartedShards(allocation, startedShards);
        gatewayAllocator.applyStartedShards(allocation);
        if (withReroute) {
            reroute(allocation);
        }
        String startedShardsAsString = firstListElementsToCommaDelimitedString(startedShards, s -> s.shardId().toString());
        return buildResultAndLogHealthChange(allocation, "shards started [" + startedShardsAsString + "] ...");
    }

    protected Result buildResultAndLogHealthChange(RoutingAllocation allocation, String reason) {
        return buildResultAndLogHealthChange(allocation, reason, new RoutingExplanations());

    }

    protected Result buildResultAndLogHealthChange(RoutingAllocation allocation, String reason, RoutingExplanations explanations) {
        MetaData oldMetaData = allocation.metaData();
        RoutingTable oldRoutingTable = allocation.routingTable();
        RoutingNodes newRoutingNodes = allocation.routingNodes();
        final RoutingTable newRoutingTable = new RoutingTable.Builder().updateNodes(oldRoutingTable.version(), newRoutingNodes).build();
        MetaData newMetaData = updateMetaDataWithRoutingTable(oldMetaData, oldRoutingTable, newRoutingTable);
        assert newRoutingTable.validate(newMetaData); // validates the routing table is coherent with the cluster state metadata
        logClusterHealthStateChange(
            new ClusterStateHealth(ClusterState.builder(clusterName).
                metaData(allocation.metaData()).routingTable(allocation.routingTable()).build()),
            new ClusterStateHealth(ClusterState.builder(clusterName).
                metaData(newMetaData).routingTable(newRoutingTable).build()),
            reason
        );
        return new Result(true, newRoutingTable, newMetaData, explanations);
    }

    /**
     * Updates the current {@link MetaData} based on the newly created {@link RoutingTable}. Specifically
     * we update {@link IndexMetaData#getActiveAllocationIds()} and {@link IndexMetaData#primaryTerm(int)} based on
     * the changes made during this allocation.
     *
     * @param oldMetaData     {@link MetaData} object from before the routing table was changed.
     * @param oldRoutingTable {@link RoutingTable} from before the  change.
     * @param newRoutingTable new {@link RoutingTable} created by the allocation change
     * @return adapted {@link MetaData}, potentially the original one if no change was needed.
     */
    static MetaData updateMetaDataWithRoutingTable(MetaData oldMetaData, RoutingTable oldRoutingTable, RoutingTable newRoutingTable) {
        MetaData.Builder metaDataBuilder = null;
        for (IndexRoutingTable newIndexTable : newRoutingTable) {
            final IndexMetaData oldIndexMetaData = oldMetaData.index(newIndexTable.getIndex());
            if (oldIndexMetaData == null) {
                throw new IllegalStateException("no metadata found for index " + newIndexTable.getIndex().getName());
            }
            IndexMetaData.Builder indexMetaDataBuilder = null;
            for (IndexShardRoutingTable newShardTable : newIndexTable) {
                final ShardId shardId = newShardTable.shardId();

                // update activeAllocationIds
                Set<String> activeAllocationIds = newShardTable.activeShards().stream()
                        .map(ShardRouting::allocationId)
                        .filter(Objects::nonNull)
                        .map(AllocationId::getId)
                        .collect(Collectors.toSet());
                // only update active allocation ids if there is an active shard
                if (activeAllocationIds.isEmpty() == false) {
                    // get currently stored allocation ids
                    Set<String> storedAllocationIds = oldIndexMetaData.activeAllocationIds(shardId.id());
                    if (activeAllocationIds.equals(storedAllocationIds) == false) {
                        if (indexMetaDataBuilder == null) {
                            indexMetaDataBuilder = IndexMetaData.builder(oldIndexMetaData);
                        }
                        indexMetaDataBuilder.putActiveAllocationIds(shardId.id(), activeAllocationIds);
                    }
                }

                // update primary terms
                final ShardRouting newPrimary = newShardTable.primaryShard();
                if (newPrimary == null) {
                    throw new IllegalStateException("missing primary shard for " + newShardTable.shardId());
                }
                final ShardRouting oldPrimary = oldRoutingTable.shardRoutingTable(shardId).primaryShard();
                if (oldPrimary == null) {
                    throw new IllegalStateException("missing primary shard for " + newShardTable.shardId());
                }
                // we update the primary term on initial assignment or when a replica is promoted. Most notably we do *not*
                // update them when a primary relocates
                if (newPrimary.unassigned() ||
                        newPrimary.isSameAllocation(oldPrimary) ||
                        // we do not use newPrimary.isTargetRelocationOf(oldPrimary) because that one enforces newPrimary to
                        // be initializing. However, when the target shard is activated, we still want the primary term to staty
                        // the same
                        (oldPrimary.relocating() && newPrimary.isSameAllocation(oldPrimary.getTargetRelocatingShard()))) {
                    // do nothing
                } else {
                    // incrementing the primary term
                    if (indexMetaDataBuilder == null) {
                        indexMetaDataBuilder = IndexMetaData.builder(oldIndexMetaData);
                    }
                    indexMetaDataBuilder.primaryTerm(shardId.id(), oldIndexMetaData.primaryTerm(shardId.id()) + 1);
                }
            }
            if (indexMetaDataBuilder != null) {
                if (metaDataBuilder == null) {
                    metaDataBuilder = MetaData.builder(oldMetaData);
                }
                metaDataBuilder.put(indexMetaDataBuilder);
            }
        }
        if (metaDataBuilder != null) {
            return metaDataBuilder.build();
        } else {
            return oldMetaData;
        }
    }

    public Result applyFailedShard(ClusterState clusterState, ShardRouting failedShard) {
        return applyFailedShards(clusterState, Collections.singletonList(new FailedRerouteAllocation.FailedShard(failedShard, null, null)));
    }

    /**
     * Applies the failed shards. Note, only assigned ShardRouting instances that exist in the routing table should be
     * provided as parameter and no duplicates should be contained.
     *
     * <p>
     * If the same instance of the routing table is returned, then no change has been made.</p>
     */
    public Result applyFailedShards(ClusterState clusterState, List<FailedRerouteAllocation.FailedShard> failedShards) {
        if (failedShards.isEmpty()) {
            return new Result(false, clusterState.routingTable(), clusterState.metaData());
        }
        RoutingNodes routingNodes = getMutableRoutingNodes(clusterState);
        // shuffle the unassigned nodes, just so we won't have things like poison failed shards
        routingNodes.unassigned().shuffle();
        long currentNanoTime = currentNanoTime();
        FailedRerouteAllocation allocation = new FailedRerouteAllocation(allocationDeciders, routingNodes, clusterState, failedShards,
            clusterInfoService.getClusterInfo(), currentNanoTime);

        // as failing primaries also fail associated replicas, we fail replicas first here to avoid re-resolving replica ShardRouting
        List<FailedRerouteAllocation.FailedShard> orderedFailedShards = new ArrayList<>(failedShards);
        orderedFailedShards.sort(Comparator.comparing(failedShard -> failedShard.routingEntry.primary()));

        for (FailedRerouteAllocation.FailedShard failedShardEntry : orderedFailedShards) {
            ShardRouting failedShard = failedShardEntry.routingEntry;
            final int failedAllocations = failedShard.unassignedInfo() != null ? failedShard.unassignedInfo().getNumFailedAllocations() : 0;
            UnassignedInfo unassignedInfo = new UnassignedInfo(UnassignedInfo.Reason.ALLOCATION_FAILED, failedShardEntry.message,
                failedShardEntry.failure, failedAllocations + 1, currentNanoTime, System.currentTimeMillis(), false,
                AllocationStatus.NO_ATTEMPT);
            allocation.addIgnoreShardForNode(failedShard.shardId(), failedShard.currentNodeId());
            applyFailedShard(allocation, failedShard, unassignedInfo);
        }
        gatewayAllocator.applyFailedShards(allocation);
        reroute(allocation);
        String failedShardsAsString = firstListElementsToCommaDelimitedString(failedShards, s -> s.routingEntry.shardId().toString());
        return buildResultAndLogHealthChange(allocation, "shards failed [" + failedShardsAsString + "] ...");
    }

    /**
     * Removes delay markers from unassigned shards based on current time stamp. Returns true if markers were removed.
     */
    private boolean removeDelayMarkers(RoutingAllocation allocation) {
        final RoutingNodes.UnassignedShards.UnassignedIterator unassignedIterator = allocation.routingNodes().unassigned().iterator();
        final MetaData metaData = allocation.metaData();
        boolean changed = false;
        while (unassignedIterator.hasNext()) {
            ShardRouting shardRouting = unassignedIterator.next();
            UnassignedInfo unassignedInfo = shardRouting.unassignedInfo();
            if (unassignedInfo.isDelayed()) {
                final long newComputedLeftDelayNanos = unassignedInfo.getRemainingDelay(allocation.getCurrentNanoTime(),
                    metaData.getIndexSafe(shardRouting.index()).getSettings());
                if (newComputedLeftDelayNanos == 0) {
                    changed = true;
                    unassignedIterator.updateUnassignedInfo(new UnassignedInfo(unassignedInfo.getReason(), unassignedInfo.getMessage(),
                        unassignedInfo.getFailure(), unassignedInfo.getNumFailedAllocations(), unassignedInfo.getUnassignedTimeInNanos(),
                        unassignedInfo.getUnassignedTimeInMillis(), false, unassignedInfo.getLastAllocationStatus()));
                }
            }
        }
        return changed;
    }

    /**
     * Internal helper to cap the number of elements in a potentially long list for logging.
     *
     * @param elements  The elements to log. May be any non-null list. Must not be null.
     * @param formatter A function that can convert list elements to a String. Must not be null.
     * @param <T>       The list element type.
     * @return A comma-separated string of the first few elements.
     */
    private <T> String firstListElementsToCommaDelimitedString(List<T> elements, Function<T, String> formatter) {
        final int maxNumberOfElements = 10;
        return elements
                .stream()
                .limit(maxNumberOfElements)
                .map(formatter)
                .collect(Collectors.joining(", "));
    }

    public Result reroute(ClusterState clusterState, AllocationCommands commands, boolean explain, boolean retryFailed) {
        RoutingNodes routingNodes = getMutableRoutingNodes(clusterState);
        // we don't shuffle the unassigned shards here, to try and get as close as possible to
        // a consistent result of the effect the commands have on the routing
        // this allows systems to dry run the commands, see the resulting cluster state, and act on it
        RoutingAllocation allocation = new RoutingAllocation(allocationDeciders, routingNodes, clusterState,
            clusterInfoService.getClusterInfo(), currentNanoTime(), retryFailed);
        // don't short circuit deciders, we want a full explanation
        allocation.debugDecision(true);
        // we ignore disable allocation, because commands are explicit
        allocation.ignoreDisable(true);
        RoutingExplanations explanations = commands.execute(allocation, explain);
        // we revert the ignore disable flag, since when rerouting, we want the original setting to take place
        allocation.ignoreDisable(false);
        // the assumption is that commands will move / act on shards (or fail through exceptions)
        // so, there will always be shard "movements", so no need to check on reroute
        reroute(allocation);
        return buildResultAndLogHealthChange(allocation, "reroute commands", explanations);
    }


    /**
     * Reroutes the routing table based on the live nodes.
     * <p>
     * If the same instance of the routing table is returned, then no change has been made.
     */
    public Result reroute(ClusterState clusterState, String reason) {
        return reroute(clusterState, reason, false);
    }

    /**
     * Reroutes the routing table based on the live nodes.
     * <p>
     * If the same instance of the routing table is returned, then no change has been made.
     */
    protected Result reroute(ClusterState clusterState, String reason, boolean debug) {
        RoutingNodes routingNodes = getMutableRoutingNodes(clusterState);
        // shuffle the unassigned nodes, just so we won't have things like poison failed shards
        routingNodes.unassigned().shuffle();
        RoutingAllocation allocation = new RoutingAllocation(allocationDeciders, routingNodes, clusterState,
            clusterInfoService.getClusterInfo(), currentNanoTime(), false);
        allocation.debugDecision(debug);
        if (!reroute(allocation)) {
            return new Result(false, clusterState.routingTable(), clusterState.metaData());
        }
        return buildResultAndLogHealthChange(allocation, reason);
    }

    private void logClusterHealthStateChange(ClusterStateHealth previousStateHealth, ClusterStateHealth newStateHealth, String reason) {
        ClusterHealthStatus previousHealth = previousStateHealth.getStatus();
        ClusterHealthStatus currentHealth = newStateHealth.getStatus();
        if (!previousHealth.equals(currentHealth)) {
            logger.info("Cluster health status changed from [{}] to [{}] (reason: [{}]).", previousHealth, currentHealth, reason);
        }
    }

    private boolean reroute(RoutingAllocation allocation) {
        boolean changed = false;
        // first, clear from the shards any node id they used to belong to that is now dead
        changed |= deassociateDeadNodes(allocation);

        // elect primaries *before* allocating unassigned, so backups of primaries that failed
        // will be moved to primary state and not wait for primaries to be allocated and recovered (*from gateway*)
        changed |= electPrimariesAndUnassignedDanglingReplicas(allocation);

        // now allocate all the unassigned to available nodes
        if (allocation.routingNodes().unassigned().size() > 0) {
            changed |= removeDelayMarkers(allocation);
            changed |= gatewayAllocator.allocateUnassigned(allocation);
        }

        changed |= shardsAllocator.allocate(allocation);
        assert RoutingNodes.assertShardStats(allocation.routingNodes());
        return changed;
    }

    private boolean electPrimariesAndUnassignedDanglingReplicas(RoutingAllocation allocation) {
        boolean changed = false;
        final RoutingNodes routingNodes = allocation.routingNodes();
        if (routingNodes.unassigned().getNumPrimaries() == 0) {
            // move out if we don't have unassigned primaries
            return changed;
        }
        // now, go over and elect a new primary if possible, not, from this code block on, if one is elected,
        // routingNodes.hasUnassignedPrimaries() will potentially be false
        final RoutingNodes.UnassignedShards.UnassignedIterator unassignedIterator = routingNodes.unassigned().iterator();
        while (unassignedIterator.hasNext()) {
            ShardRouting shardEntry = unassignedIterator.next();
            if (shardEntry.primary()) {
                // remove dangling replicas that are initializing for primary shards
                changed |= failReplicasForUnassignedPrimary(allocation, shardEntry);
                ShardRouting candidate = allocation.routingNodes().activeReplica(shardEntry.shardId());
                if (candidate != null) {
                    shardEntry = unassignedIterator.demotePrimaryToReplicaShard();
                    ShardRouting primarySwappedCandidate = routingNodes.promoteAssignedReplicaShardToPrimary(candidate);
                    if (primarySwappedCandidate.relocatingNodeId() != null) {
                        changed = true;
                        // its also relocating, make sure to move the other routing to primary
                        RoutingNode node = routingNodes.node(primarySwappedCandidate.relocatingNodeId());
                        if (node != null) {
                            for (ShardRouting shardRouting : node) {
                                if (shardRouting.shardId().equals(primarySwappedCandidate.shardId()) && !shardRouting.primary()) {
                                    routingNodes.promoteAssignedReplicaShardToPrimary(shardRouting);
                                    break;
                                }
                            }
                        }
                    }
                    IndexMetaData index = allocation.metaData().getIndexSafe(primarySwappedCandidate.index());
                    if (IndexMetaData.isIndexUsingShadowReplicas(index.getSettings())) {
                        routingNodes.reinitShadowPrimary(primarySwappedCandidate);
                        changed = true;
                    }
                }
            }
        }

        return changed;
    }

    private boolean deassociateDeadNodes(RoutingAllocation allocation) {
        boolean changed = false;
        for (Iterator<RoutingNode> it = allocation.routingNodes().mutableIterator(); it.hasNext(); ) {
            RoutingNode node = it.next();
            if (allocation.nodes().getDataNodes().containsKey(node.nodeId())) {
                // its a live node, continue
                continue;
            }
            changed = true;
            // now, go over all the shards routing on the node, and fail them
            for (ShardRouting shardRouting : node.copyShards()) {
                final IndexMetaData indexMetaData = allocation.metaData().getIndexSafe(shardRouting.index());
                boolean delayed = INDEX_DELAYED_NODE_LEFT_TIMEOUT_SETTING.get(indexMetaData.getSettings()).nanos() > 0;
                UnassignedInfo unassignedInfo = new UnassignedInfo(UnassignedInfo.Reason.NODE_LEFT, "node_left[" + node.nodeId() + "]",
                    null, 0, allocation.getCurrentNanoTime(), System.currentTimeMillis(), delayed, AllocationStatus.NO_ATTEMPT);
                applyFailedShard(allocation, shardRouting, unassignedInfo);
            }
            // its a dead node, remove it, note, its important to remove it *after* we apply failed shard
            // since it relies on the fact that the RoutingNode exists in the list of nodes
            it.remove();
        }
        return changed;
    }

    private boolean failReplicasForUnassignedPrimary(RoutingAllocation allocation, ShardRouting failedPrimary) {
        assert failedPrimary.primary() : "can only fail replicas for primary shard: " + failedPrimary;
        List<ShardRouting> replicas = new ArrayList<>();
        for (ShardRouting routing : allocation.routingNodes().assignedShards(failedPrimary.shardId())) {
            if (!routing.primary() && routing.initializing()) {
                replicas.add(routing);
            }
        }
        for (ShardRouting failedReplica : replicas) {
            UnassignedInfo unassignedInfo = new UnassignedInfo(UnassignedInfo.Reason.PRIMARY_FAILED,
                "primary failed while replica initializing", null, 0, allocation.getCurrentNanoTime(), System.currentTimeMillis(), false,
                AllocationStatus.NO_ATTEMPT);
            applyFailedShard(allocation, failedReplica, unassignedInfo);
        }
        return replicas.isEmpty() == false;
    }

    private void applyStartedShards(RoutingAllocation routingAllocation, List<ShardRouting> startedShardEntries) {
        assert startedShardEntries.isEmpty() == false : "non-empty list of started shard entries expected";
        RoutingNodes routingNodes = routingAllocation.routingNodes();
        for (ShardRouting startedShard : startedShardEntries) {
            assert startedShard.initializing() : "only initializing shards can be started";
            assert routingAllocation.metaData().index(startedShard.shardId().getIndex()) != null :
                "shard started for unknown index (shard entry: " + startedShard + ")";
            assert startedShard == routingNodes.getByAllocationId(startedShard.shardId(), startedShard.allocationId().getId()) :
                "shard routing to start does not exist in routing table, expected: " + startedShard + " but was: " +
                    routingNodes.getByAllocationId(startedShard.shardId(), startedShard.allocationId().getId());

            routingNodes.started(startedShard);
            logger.trace("{} marked shard as started (routing: {})", startedShard.shardId(), startedShard);

            if (startedShard.relocatingNodeId() != null) {
                // relocation target has been started, remove relocation source
                RoutingNode relocationSourceNode = routingNodes.node(startedShard.relocatingNodeId());
                ShardRouting relocationSourceShard = relocationSourceNode.getByShardId(startedShard.shardId());
                assert relocationSourceShard.isRelocationSourceOf(startedShard);
                assert relocationSourceShard.getTargetRelocatingShard() == startedShard : "relocation target mismatch, expected: "
                    + startedShard + " but was: " + relocationSourceShard.getTargetRelocatingShard();
                routingNodes.remove(relocationSourceShard);
            }
        }
    }

    /**
     * Applies the relevant logic to handle a failed shard.
     */
    private void applyFailedShard(RoutingAllocation allocation, ShardRouting failedShard, UnassignedInfo unassignedInfo) {
        RoutingNodes routingNodes = allocation.routingNodes();
        assert failedShard.assignedToNode() : "only assigned shards can be failed";
        assert allocation.metaData().index(failedShard.shardId().getIndex()) != null :
            "shard failed for unknown index (shard entry: " + failedShard + ")";
        assert routingNodes.getByAllocationId(failedShard.shardId(), failedShard.allocationId().getId()) == failedShard :
            "shard routing to fail does not exist in routing table, expected: " + failedShard + " but was: " +
                routingNodes.getByAllocationId(failedShard.shardId(), failedShard.allocationId().getId());

        logger.debug("{} failing shard {} with unassigned info ({})", failedShard.shardId(), failedShard, unassignedInfo.shortSummary());
        if (failedShard.primary()) {
            // fail replicas first otherwise we move RoutingNodes into an inconsistent state
            failReplicasForUnassignedPrimary(allocation, failedShard);
        }

        cancelShard(logger, failedShard, unassignedInfo, routingNodes);
        assert routingNodes.node(failedShard.currentNodeId()).getByShardId(failedShard.shardId()) == null : "failedShard " + failedShard +
            " was matched but wasn't removed";
    }

    public static void cancelShard(ESLogger logger, ShardRouting cancelledShard, UnassignedInfo unassignedInfo, RoutingNodes routingNodes) {
        if (cancelledShard.relocatingNodeId() == null) {
            routingNodes.moveToUnassigned(cancelledShard, unassignedInfo);
        } else {
            if (cancelledShard.initializing()) {
                // The shard is a target of a relocating shard. In that case we only
                // need to remove the target shard and cancel the source relocation.
                // No shard is left unassigned
                logger.trace("{} is a relocation target, resolving source to cancel relocation ({})", cancelledShard,
                    unassignedInfo.shortSummary());
                RoutingNode sourceNode = routingNodes.node(cancelledShard.relocatingNodeId());
                ShardRouting sourceShard = sourceNode.getByShardId(cancelledShard.shardId());
                assert sourceShard.isRelocationSourceOf(cancelledShard);
                logger.trace("{}, resolved source to [{}]. canceling relocation ... ({})", cancelledShard.shardId(), sourceShard,
                    unassignedInfo.shortSummary());
                routingNodes.cancelRelocation(sourceShard);
                routingNodes.remove(cancelledShard);
            } else {
                assert cancelledShard.relocating();
                // The cancelled shard is the main copy of the current shard routing.
                // now, find the shard that is initializing on the target node
                RoutingNode targetNode = routingNodes.node(cancelledShard.relocatingNodeId());
                ShardRouting targetShard = targetNode.getByShardId(cancelledShard.shardId());
                assert targetShard.isRelocationTargetOf(cancelledShard);
                if (cancelledShard.primary()) {
                    logger.trace("{} is removed due to the failure/cancellation of the source shard", targetShard);
                    // cancel and remove target shard
                    routingNodes.remove(targetShard);
                    routingNodes.moveToUnassigned(cancelledShard, unassignedInfo);
                } else {
                    logger.trace("{}, relocation source failed / cancelled, mark as initializing without relocation source", targetShard);
                    // promote to initializing shard without relocation source and ensure that removed relocation source
                    // is not added back as unassigned shard
                    routingNodes.removeRelocationSource(targetShard);
                    routingNodes.remove(cancelledShard);
                }
            }
        }
    }

    private RoutingNodes getMutableRoutingNodes(ClusterState clusterState) {
        RoutingNodes routingNodes = new RoutingNodes(clusterState, false); // this is a costly operation - only call this once!
        return routingNodes;
    }

    /** override this to control time based decisions during allocation */
    protected long currentNanoTime() {
        return System.nanoTime();
    }
}
