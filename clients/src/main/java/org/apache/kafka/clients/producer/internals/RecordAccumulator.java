/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.apache.kafka.clients.producer.internals;

import java.util.Iterator;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.metrics.Measurable;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Rate;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.Records;
import org.apache.kafka.common.utils.CopyOnWriteMap;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * This class acts as a queue that accumulates records into {@link org.apache.kafka.common.record.MemoryRecords}
 * instances to be sent to the server.
 * <p>
 * The accumulator uses a bounded amount of memory and append calls will block when that memory is exhausted, unless
 * this behavior is explicitly disabled.
 */
public final class RecordAccumulator {

    private static final Logger log = LoggerFactory.getLogger(RecordAccumulator.class);

    private volatile boolean closed;
    private final AtomicInteger flushesInProgress;
    private final AtomicInteger appendsInProgress;
    private final int batchSize;
    private final CompressionType compression;
    private final long lingerMs;
    private final long retryBackoffMs;
    private final BufferPool free;
    private final Time time;
    private final ConcurrentMap<TopicPartition, Deque<RecordBatch>> batches;
    private final IncompleteRecordBatches incomplete;
    // The following variables are only accessed by the sender thread, so we don't need to protect them.
    private final Set<TopicPartition> muted;
    private int drainIndex;

    /**
     * Create a new record accumulator
     * 
     * @param batchSize The size to use when allocating {@link org.apache.kafka.common.record.MemoryRecords} instances
     * @param totalSize The maximum memory the record accumulator can use.
     * @param compression The compression codec for the records
     * @param lingerMs An artificial delay time to add before declaring a records instance that isn't full ready for
     *        sending. This allows time for more records to arrive. Setting a non-zero lingerMs will trade off some
     *        latency for potentially better throughput due to more batching (and hence fewer, larger requests).
     * @param retryBackoffMs An artificial delay time to retry the produce request upon receiving an error. This avoids
     *        exhausting all retries in a short period of time.
     * @param metrics The metrics
     * @param time The time instance to use
     */
    public RecordAccumulator(int batchSize,
                             long totalSize,
                             CompressionType compression,
                             long lingerMs,
                             long retryBackoffMs,
                             Metrics metrics,
                             Time time) {
        this.drainIndex = 0;
        this.closed = false;
        this.flushesInProgress = new AtomicInteger(0);
        this.appendsInProgress = new AtomicInteger(0);
        this.batchSize = batchSize;
        this.compression = compression;
        this.lingerMs = lingerMs;
        this.retryBackoffMs = retryBackoffMs;
        // 是kafka自己设计的数据结构
        // 提升了封装批次的性能
        this.batches = new CopyOnWriteMap<>();
        String metricGrpName = "producer-metrics";
        this.free = new BufferPool(totalSize, batchSize, metrics, time, metricGrpName);
        this.incomplete = new IncompleteRecordBatches();
        this.muted = new HashSet<>();
        this.time = time;
        registerMetrics(metrics, metricGrpName);
    }

    private void registerMetrics(Metrics metrics, String metricGrpName) {
        MetricName metricName = metrics.metricName("waiting-threads", metricGrpName, "The number of user threads blocked waiting for buffer memory to enqueue their records");
        Measurable waitingThreads = new Measurable() {
            public double measure(MetricConfig config, long now) {
                return free.queued();
            }
        };
        metrics.addMetric(metricName, waitingThreads);

        metricName = metrics.metricName("buffer-total-bytes", metricGrpName, "The maximum amount of buffer memory the client can use (whether or not it is currently used).");
        Measurable totalBytes = new Measurable() {
            public double measure(MetricConfig config, long now) {
                return free.totalMemory();
            }
        };
        metrics.addMetric(metricName, totalBytes);

        metricName = metrics.metricName("buffer-available-bytes", metricGrpName, "The total amount of buffer memory that is not being used (either unallocated or in the free list).");
        Measurable availableBytes = new Measurable() {
            public double measure(MetricConfig config, long now) {
                return free.availableMemory();
            }
        };
        metrics.addMetric(metricName, availableBytes);

        Sensor bufferExhaustedRecordSensor = metrics.sensor("buffer-exhausted-records");
        metricName = metrics.metricName("buffer-exhausted-rate", metricGrpName, "The average per-second number of record sends that are dropped due to buffer exhaustion");
        bufferExhaustedRecordSensor.add(metricName, new Rate());
    }

    /**
     * Add a record to the accumulator, return the append result
     * <p>
     * The append result will contain the future metadata, and flag for whether the appended batch is full or a new batch is created
     * <p>
     *
     * @param tp The topic/partition to which this record is being sent
     * @param timestamp The timestamp of the record
     * @param key The key for the record
     * @param value The value for the record
     * @param callback The user-supplied callback to execute when the request is complete
     * @param maxTimeToBlock The maximum time in milliseconds to block for buffer memory to be available
     */
    public RecordAppendResult append(TopicPartition tp,
                                     long timestamp,
                                     byte[] key,
                                     byte[] value,
                                     Callback callback,
                                     long maxTimeToBlock) throws InterruptedException {
        // We keep track of the number of appending thread to make sure we do not miss batches in
        // abortIncompleteBatches().
        appendsInProgress.incrementAndGet();
        try {
            // check if we have an in-progress batch
            /**
             * 步骤一：
             *      先根据分区找到要插入到哪个队列
             * 如果有已经存在的队列，那么就使用存在的队列
             * 如果队列不存在，新建一个队列
             *
             * 第一次进来，新建了一个空的队列
             *
             * 对二次进来，假设还是原来那个分区
             */
            Deque<RecordBatch> dq = getOrCreateDeque(tp);
            // 分段加锁，在高并发的情况下提升性能，在高并发的情况下必须要保证线程安全
            /**
             * 假设同时有线程1，线程2，线程3进来
             */
            synchronized (dq) {
                if (closed)
                    throw new IllegalStateException("Cannot send after the producer is closed.");
                /**
                 * 步骤二：
                 *      尝试往队列里面添加数据
                 * 一开始添加数据肯定是失败的，目前是只有队列，数据是需要存储在批次对象里面（这个批次对象是需要分配内存的）
                 * 目前还没有分配内存，所以按照场景驱动方式，第一次运行到这儿其实是不成功的
                 */
                RecordAppendResult appendResult = tryAppend(timestamp, key, value, callback, dq);
                // 第一次进来appendResult为null
                if (appendResult != null)
                    return appendResult;
            }

            // we don't have an in-progress record batch try to allocate a new batch
            /**
             * 步骤三：
             *      计算一个批次的大小
             * 在消息的大小和批次的大小之间取一个最大值，用这个值作为当前批次的大小
             * 有可能一条消息的大小比设定的批次大小（默认16k）还要大
             *
             * 实际应用中，如果消息都大于16k，那么会一条一条发送，批次设计就无意义了，所以要根据实际情况适当提高批次的大小
             */
            int size = Math.max(this.batchSize, Records.LOG_OVERHEAD + Record.recordSize(key, value));
            log.trace("Allocating a new {} byte message buffer for topic {} partition {}", size, tp.topic(), tp.partition());
            /**
             * 步骤四：
             *      根据批次的大小去分配内存
             *
             * 线程1，2，3到这里都会申请内存，假设每个线程都申请了16k内存
             */
            ByteBuffer buffer = free.allocate(size, maxTimeToBlock);
            synchronized (dq) {
                // Need to check if producer is closed again after grabbing the dequeue lock.
                if (closed)
                    throw new IllegalStateException("Cannot send after the producer is closed.");
                /**
                 * 步骤五：
                 *      尝试把数据写入到批次里面
                 * 第一次执行到这里的时候，依然还是失败
                 * 目前虽然已经分配了内存，但是还没有封装成一个批次，所以想向批次写数据依然会失败
                 *
                 * 线程1进来，执行失败，appendResult返回null
                 *
                 * 线程2进来，线程1已经添加了批次，获取线程1添加的批次
                 */
                RecordAppendResult appendResult = tryAppend(timestamp, key, value, callback, dq);
                if (appendResult != null) {
                    // Somebody else found us a batch, return the one we waited for! Hopefully this doesn't happen often...
                    // 释放内存
                    // 线程2到这里，实际上它的数据已经插入到线程1申请的批次中了，所以线程2的内存就没有什么用了，就把内存释放到内存池了
                    free.deallocate(buffer);
                    return appendResult;
                }
                MemoryRecords records = MemoryRecords.emptyRecords(buffer, compression, this.batchSize);
                /**
                 * 步骤六：
                 *      根据内存大小封装批次
                 *
                 * 线程1到这里后会封装一个批次
                 */
                RecordBatch batch = new RecordBatch(tp, records, time.milliseconds());
                //  尝试往批次里面写数据，到这里的时候，写入数据会成功
                FutureRecordMetadata future = Utils.notNull(batch.tryAppend(timestamp, key, value, callback, time.milliseconds()));
                /**
                 * 步骤七：
                 *      把新创建的批次放到Deque队尾
                 *
                 * 线程1把批次添加到队尾
                 */
                dq.addLast(batch);
                incomplete.add(batch);
                return new RecordAppendResult(future, dq.size() > 1 || batch.records.isFull(), true);
            }
        } finally {
            appendsInProgress.decrementAndGet();
        }
    }

    /**
     * If `RecordBatch.tryAppend` fails (i.e. the record batch is full), close its memory records to release temporary
     * resources (like compression streams buffers).
     */
    private RecordAppendResult tryAppend(long timestamp, byte[] key, byte[] value, Callback callback, Deque<RecordBatch> deque) {
        // 首先获取到队列里面的一个批次
        RecordBatch last = deque.peekLast();
        // 第一次进来，没有批次，last为null

        // 第二次进来，往批次插入数据
        if (last != null) {
            FutureRecordMetadata future = last.tryAppend(timestamp, key, value, callback, time.milliseconds());
            if (future == null)
                last.records.close();
            else
                return new RecordAppendResult(future, deque.size() > 1 || last.records.isFull(), false);
        }
        return null;
    }

    /**
     * Abort the batches that have been sitting in RecordAccumulator for more than the configured requestTimeout
     * due to metadata being unavailable
     */
    public List<RecordBatch> abortExpiredBatches(int requestTimeout, long now) {
        List<RecordBatch> expiredBatches = new ArrayList<>();
        int count = 0;
        for (Map.Entry<TopicPartition, Deque<RecordBatch>> entry : this.batches.entrySet()) {
            // 获取到每个分区的队列里面对应的批次
            Deque<RecordBatch> dq = entry.getValue();
            TopicPartition tp = entry.getKey();
            // We only check if the batch should be expired if the partition does not have a batch in flight.
            // This is to prevent later batches from being expired while an earlier batch is still in progress.
            // Note that `muted` is only ever populated if `max.in.flight.request.per.connection=1` so this protection
            // is only active in this case. Otherwise the expiration order is not guaranteed.
            if (!muted.contains(tp)) {
                synchronized (dq) {
                    // iterate over the batches and expire them if they have been in the accumulator for more than requestTimeOut
                    RecordBatch lastBatch = dq.peekLast();
                    Iterator<RecordBatch> batchIterator = dq.iterator();
                    // 迭代的看分区里面的每个批次
                    while (batchIterator.hasNext()) {
                        RecordBatch batch = batchIterator.next();
                        boolean isFull = batch != lastBatch || batch.records.isFull();
                        // check if the batch is expired
                        // 判断一下批次是否超时
                        if (batch.maybeExpire(requestTimeout, retryBackoffMs, now, this.lingerMs, isFull)) {
                            // 增加到超时的数据结构里面
                            expiredBatches.add(batch);
                            count++;
                            // 从数据结构里面移除
                            batchIterator.remove();
                            // 释放内存
                            deallocate(batch);
                        } else {
                            // Stop at the first batch that has not expired.
                            break;
                        }
                    }
                }
            }
        }
        if (!expiredBatches.isEmpty())
            log.trace("Expired {} batches in accumulator", count);

        return expiredBatches;
    }

    /**
     * Re-enqueue the given record batch in the accumulator to retry
     */
    public void reenqueue(RecordBatch batch, long now) {
        // 累加重试次数
        batch.attempts++;
        // 设置上一次重试时间为现在
        batch.lastAttemptMs = now;
        batch.lastAppendTime = now;
        batch.setRetry();
        Deque<RecordBatch> deque = getOrCreateDeque(batch.topicPartition);
        synchronized (deque) {
            // 重新把批次放到队头
            deque.addFirst(batch);
        }
    }

    /**
     * Get a list of nodes whose partitions are ready to be sent, and the earliest time at which any non-sendable
     * partition will be ready; Also return the flag for whether there are any unknown leaders for the accumulated
     * partition batches.
     * <p>
     * A destination node is ready to send data if:
     * <ol>
     * <li>There is at least one partition that is not backing off its send
     * <li><b>and</b> those partitions are not muted (to prevent reordering if
     *   {@value org.apache.kafka.clients.producer.ProducerConfig#MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION}
     *   is set to one)</li>
     * <li><b>and <i>any</i></b> of the following are true</li>
     * <ul>
     *     <li>The record set is full</li>
     *     <li>The record set has sat in the accumulator for at least lingerMs milliseconds</li>
     *     <li>The accumulator is out of memory and threads are blocking waiting for data (in this case all partitions
     *     are immediately considered ready).</li>
     *     <li>The accumulator has been closed</li>
     * </ul>
     * </ol>
     */
    public ReadyCheckResult ready(Cluster cluster, long nowMs) {
        Set<Node> readyNodes = new HashSet<>();
        long nextReadyCheckDelayMs = Long.MAX_VALUE;
        Set<String> unknownLeaderTopics = new HashSet<>();

        // waiters里面有数据，说明我们的内存池内存不够了
        // 如果exhausted的值等于true，说明我们的内存池内存不够了
        boolean exhausted = this.free.queued() > 0;
        // 遍历所有的分区
        for (Map.Entry<TopicPartition, Deque<RecordBatch>> entry : this.batches.entrySet()) {
            // 获取到分区
            TopicPartition part = entry.getKey();
            // 获取到分区对应的队列
            Deque<RecordBatch> deque = entry.getValue();
            // 根据分区获取到leader partition所在的主机
            Node leader = cluster.leaderFor(part);
            synchronized (deque) {
                // 如果没有找到对应的主机
                if (leader == null && !deque.isEmpty()) {
                    // This is a partition for which leader is not known, but messages are available to send.
                    // Note that entries are currently not removed from batches when deque is empty.
                    unknownLeaderTopics.add(part.topic());
                } else if (!readyNodes.contains(leader) && !muted.contains(part)) {
                    // 首先从队列的队头获取到批次
                    RecordBatch batch = deque.peekFirst();
                    // 如果查询的批次不为null，判断批次是否可以发送
                    if (batch != null) {
                        /**
                         * attempts：重试的次数
                         * lastAttemptMs：上一次重试的时间
                         * retryBackoffMs：规定重试的时间间隔
                         *
                         * backingOff：支持重试，并且到达重新发送的时间
                         */
                        boolean backingOff = batch.attempts > 0 && batch.lastAttemptMs + retryBackoffMs > nowMs;
                        /**
                         * nowMs：当前时间
                         * lastAttemptMs：上一次重试的时间
                         *
                         * waitedTimeMs：这个批次已经等了多久了
                         */
                        long waitedTimeMs = nowMs - batch.lastAttemptMs;
                        /**
                         * 以场景驱动的方式分析，这里是第一次发送数据，所以之前也没有消息发送出去过，也就没有重试这一说
                         *
                         * 第一次进来：
                         * backingOff = false，timeToWaitMs = lingerMs
                         *
                         * lingerMs：默认是0，如果默认值是0的话，代表来一条消息就发送一条，所以实际应用中要设置这个参数
                         *          消息最多存多久就必须发送出去
                         */
                        long timeToWaitMs = backingOff ? retryBackoffMs : lingerMs;
                        /**
                         * timeToWaitMs：最多能等待多久
                         * waitedTimeMs：已经等待了多久
                         * timeLeftMs：还需要等待多久
                         */
                        long timeLeftMs = Math.max(timeToWaitMs - waitedTimeMs, 0);
                        /**
                         * 如果队列大于1，说明队列至少有一个批次写满了，可以发送数据
                         * 或者刚好只有一个批次，刚好这个批次写满了
                         * full：是否有写满的批次
                         */
                        boolean full = deque.size() > 1 || batch.records.isFull();
                        /**
                         * 如果expired = true，表示批次到时间了可以发送了
                         */
                        boolean expired = waitedTimeMs >= timeToWaitMs;
                        /**
                         * full：批次写满
                         * expired：到时间
                         * exhausted：内存池内存不够
                         * closed：生产者线程关闭
                         * flushInProgress：
                         */
                        boolean sendable = full || expired || exhausted || closed || flushInProgress();
                        // 可以发送消息了
                        if (sendable && !backingOff) {
                            // 把可以发送批次的partition的leader partition所在的主机加入到readyNodes中
                            readyNodes.add(leader);
                        } else {
                            // Note that this results in a conservative estimate since an un-sendable partition may have
                            // a leader that will later be found to have sendable data. However, this is good enough
                            // since we'll just wake up and then sleep again for the remaining time.
                            nextReadyCheckDelayMs = Math.min(timeLeftMs, nextReadyCheckDelayMs);
                        }
                    }
                }
            }
        }

        return new ReadyCheckResult(readyNodes, nextReadyCheckDelayMs, unknownLeaderTopics);
    }

    /**
     * @return Whether there is any unsent record in the accumulator.
     */
    public boolean hasUnsent() {
        for (Map.Entry<TopicPartition, Deque<RecordBatch>> entry : this.batches.entrySet()) {
            Deque<RecordBatch> deque = entry.getValue();
            synchronized (deque) {
                if (!deque.isEmpty())
                    return true;
            }
        }
        return false;
    }

    /**
     * Drain all the data for the given nodes and collate them into a list of batches that will fit within the specified
     * size on a per-node basis. This method attempts to avoid choosing the same topic-node over and over.
     * 
     * @param cluster The current cluster metadata
     * @param nodes The list of node to drain
     * @param maxSize The maximum number of bytes to drain
     * @param now The current unix time in milliseconds
     * @return A list of {@link RecordBatch} for each node specified with total size less than the requested maxSize.
     */
    public Map<Integer, List<RecordBatch>> drain(Cluster cluster,
                                                 Set<Node> nodes,
                                                 int maxSize,
                                                 long now) {
        /**
         * 如果网络没有建立的话，readyNodes为空，nodes就为空，下面的代码不会执行
         */
        if (nodes.isEmpty())
            return Collections.emptyMap();

        Map<Integer, List<RecordBatch>> batches = new HashMap<>();
        for (Node node : nodes) {
            int size = 0;
            List<PartitionInfo> parts = cluster.partitionsForNode(node.id());
            List<RecordBatch> ready = new ArrayList<>();
            /* to make starvation less likely this loop doesn't start at 0 */
            int start = drainIndex = drainIndex % parts.size();
            do {
                PartitionInfo part = parts.get(drainIndex);
                TopicPartition tp = new TopicPartition(part.topic(), part.partition());
                // Only proceed if the partition has no in-flight batches.
                if (!muted.contains(tp)) {
                    Deque<RecordBatch> deque = getDeque(new TopicPartition(part.topic(), part.partition()));
                    if (deque != null) {
                        synchronized (deque) {
                            RecordBatch first = deque.peekFirst();
                            if (first != null) {
                                boolean backoff = first.attempts > 0 && first.lastAttemptMs + retryBackoffMs > now;
                                // Only drain the batch if it is not during backoff period.
                                if (!backoff) {
                                    if (size + first.records.sizeInBytes() > maxSize && !ready.isEmpty()) {
                                        // there is a rare case that a single batch size is larger than the request size due
                                        // to compression; in this case we will still eventually send this batch in a single
                                        // request
                                        break;
                                    } else {
                                        RecordBatch batch = deque.pollFirst();
                                        batch.records.close();
                                        size += batch.records.sizeInBytes();
                                        ready.add(batch);
                                        batch.drainedMs = now;
                                    }
                                }
                            }
                        }
                    }
                }
                this.drainIndex = (this.drainIndex + 1) % parts.size();
            } while (start != drainIndex);
            batches.put(node.id(), ready);
        }
        return batches;
    }

    private Deque<RecordBatch> getDeque(TopicPartition tp) {
        return batches.get(tp);
    }

    /**
     * Get the deque for the given topic-partition, creating it if necessary.
     */
    private Deque<RecordBatch> getOrCreateDeque(TopicPartition tp) {
        // 直接从batches里面获取当前分区的队列
        Deque<RecordBatch> d = this.batches.get(tp);
        // 场景驱动，第一次进来batches里面没有队列，也就是d的值为null
        if (d != null)
            return d;
        // 创建新的空队列
        d = new ArrayDeque<>();
        // 把新的空队列存如入到batches里面
        Deque<RecordBatch> previous = this.batches.putIfAbsent(tp, d);
        if (previous == null)
            return d;
        else
            // 直接返回新的结果
            return previous;
    }

    /**
     * Deallocate the record batch
     */
    public void deallocate(RecordBatch batch) {
        // 从数据结构里面移除已经成功处理的批次
        incomplete.remove(batch);
        // 归还批次的内存
        free.deallocate(batch.records.buffer(), batch.records.initialCapacity());
    }
    
    /**
     * Are there any threads currently waiting on a flush?
     *
     * package private for test
     */
    boolean flushInProgress() {
        return flushesInProgress.get() > 0;
    }

    /* Visible for testing */
    Map<TopicPartition, Deque<RecordBatch>> batches() {
        return Collections.unmodifiableMap(batches);
    }
    
    /**
     * Initiate the flushing of data from the accumulator...this makes all requests immediately ready
     */
    public void beginFlush() {
        this.flushesInProgress.getAndIncrement();
    }

    /**
     * Are there any threads currently appending messages?
     */
    private boolean appendsInProgress() {
        return appendsInProgress.get() > 0;
    }

    /**
     * Mark all partitions as ready to send and block until the send is complete
     */
    public void awaitFlushCompletion() throws InterruptedException {
        try {
            for (RecordBatch batch : this.incomplete.all())
                batch.produceFuture.await();
        } finally {
            this.flushesInProgress.decrementAndGet();
        }
    }

    /**
     * This function is only called when sender is closed forcefully. It will fail all the
     * incomplete batches and return.
     */
    public void abortIncompleteBatches() {
        // We need to keep aborting the incomplete batch until no thread is trying to append to
        // 1. Avoid losing batches.
        // 2. Free up memory in case appending threads are blocked on buffer full.
        // This is a tight loop but should be able to get through very quickly.
        do {
            abortBatches();
        } while (appendsInProgress());
        // After this point, no thread will append any messages because they will see the close
        // flag set. We need to do the last abort after no thread was appending in case there was a new
        // batch appended by the last appending thread.
        abortBatches();
        this.batches.clear();
    }

    /**
     * Go through incomplete batches and abort them.
     */
    private void abortBatches() {
        for (RecordBatch batch : incomplete.all()) {
            Deque<RecordBatch> dq = getDeque(batch.topicPartition);
            // Close the batch before aborting
            synchronized (dq) {
                batch.records.close();
                dq.remove(batch);
            }
            batch.done(-1L, Record.NO_TIMESTAMP, new IllegalStateException("Producer is closed forcefully."));
            deallocate(batch);
        }
    }

    public void mutePartition(TopicPartition tp) {
        muted.add(tp);
    }

    public void unmutePartition(TopicPartition tp) {
        muted.remove(tp);
    }

    /**
     * Close this accumulator and force all the record buffers to be drained
     */
    public void close() {
        this.closed = true;
    }

    /*
     * Metadata about a record just appended to the record accumulator
     */
    public final static class RecordAppendResult {
        public final FutureRecordMetadata future;
        public final boolean batchIsFull;
        public final boolean newBatchCreated;

        public RecordAppendResult(FutureRecordMetadata future, boolean batchIsFull, boolean newBatchCreated) {
            this.future = future;
            this.batchIsFull = batchIsFull;
            this.newBatchCreated = newBatchCreated;
        }
    }

    /*
     * The set of nodes that have at least one complete record batch in the accumulator
     */
    public final static class ReadyCheckResult {
        public final Set<Node> readyNodes;
        public final long nextReadyCheckDelayMs;
        public final Set<String> unknownLeaderTopics;

        public ReadyCheckResult(Set<Node> readyNodes, long nextReadyCheckDelayMs, Set<String> unknownLeaderTopics) {
            this.readyNodes = readyNodes;
            this.nextReadyCheckDelayMs = nextReadyCheckDelayMs;
            this.unknownLeaderTopics = unknownLeaderTopics;
        }
    }
    
    /*
     * A threadsafe helper class to hold RecordBatches that haven't been ack'd yet
     */
    private final static class IncompleteRecordBatches {
        private final Set<RecordBatch> incomplete;

        public IncompleteRecordBatches() {
            this.incomplete = new HashSet<RecordBatch>();
        }
        
        public void add(RecordBatch batch) {
            synchronized (incomplete) {
                this.incomplete.add(batch);
            }
        }
        
        public void remove(RecordBatch batch) {
            synchronized (incomplete) {
                boolean removed = this.incomplete.remove(batch);
                if (!removed)
                    throw new IllegalStateException("Remove from the incomplete set failed. This should be impossible.");
            }
        }
        
        public Iterable<RecordBatch> all() {
            synchronized (incomplete) {
                return new ArrayList<>(this.incomplete);
            }
        }
    }

}
