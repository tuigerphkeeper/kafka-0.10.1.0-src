/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.apache.kafka.common.utils;

import java.util.*;
import java.util.concurrent.ConcurrentMap;

/**
 * A simple read-optimized map implementation that synchronizes only writes and does a full copy on each modification
 *
 * 1.这个数据结构在高并发的情况下是线程安全的
 * 2.采用读写分离的思想设计的数据接口
 *      每次（写数据）数据的时候都开辟新的内存空间
 *      所以在插入数据的时候，会比较耗费内存空间
 * 3.这样的数据结构适合读多写少场景
 *      读数据的适合性能很高
 *
 * 实际场景：
 * 读数据：
 *      每生产一条数据，都会从batches里面读取数据（检查topicPartition队列是否存在）
 *      如果10w条数据/s，表示每秒要读取10w次，是非常高的并发场景
 * 写数据：
 *      topic有多少个分区，就需要写入多少次数据，实际应用中，topic分区都比较少，所以写入是一个低频操作
 */
public class CopyOnWriteMap<K, V> implements ConcurrentMap<K, V> {

    /**
     * 核心的变量就是一个map
     * 这个map有个特点，他的修饰符是volatile关键字
     * 在多线程的情况下，如果这个map的值发生变化，其他线程也是可见的
     */
    private volatile Map<K, V> map;

    public CopyOnWriteMap() {
        this.map = Collections.emptyMap();
    }

    public CopyOnWriteMap(Map<K, V> map) {
        this.map = Collections.unmodifiableMap(map);
    }

    @Override
    public boolean containsKey(Object k) {
        return map.containsKey(k);
    }

    @Override
    public boolean containsValue(Object v) {
        return map.containsValue(v);
    }

    @Override
    public Set<java.util.Map.Entry<K, V>> entrySet() {
        return map.entrySet();
    }

    /**
     * 没有加锁，读取数据的时候性能很高
     * 并且是线程安全的，因为采用了读写分离的思想
     */
    @Override
    public V get(Object k) {
        return map.get(k);
    }

    @Override
    public boolean isEmpty() {
        return map.isEmpty();
    }

    @Override
    public Set<K> keySet() {
        return map.keySet();
    }

    @Override
    public int size() {
        return map.size();
    }

    @Override
    public Collection<V> values() {
        return map.values();
    }

    @Override
    public synchronized void clear() {
        this.map = Collections.emptyMap();
    }

    /**
     * 1.整个方法使用synchronized修饰，说明这个方法是线程安全的
     *   即使加了锁，这段代码的性能依然很好，因为里面都是纯内存的操作
     * 2.这种设计方式，采用的是读写分离的设计思想，读操作和写操作是相互不影响的
     *   所以度操作就是线程安全的
     * 3.最后把值赋给了map，map是用volatile关键字修饰的，说明这个map是具有可见性的，
     *   这样的话，get数据的时候，这里的值发生了变化，也是能感知到的
     */
    @Override
    public synchronized V put(K k, V v) {
        // 新的内存空间
        Map<K, V> copy = new HashMap<K, V>(this.map);
        // 插入数据
        V prev = copy.put(k, v);
        // 赋值给map
        this.map = Collections.unmodifiableMap(copy);
        return prev;
    }

    @Override
    public synchronized void putAll(Map<? extends K, ? extends V> entries) {
        Map<K, V> copy = new HashMap<K, V>(this.map);
        copy.putAll(entries);
        this.map = Collections.unmodifiableMap(copy);
    }

    @Override
    public synchronized V remove(Object key) {
        Map<K, V> copy = new HashMap<K, V>(this.map);
        V prev = copy.remove(key);
        this.map = Collections.unmodifiableMap(copy);
        return prev;
    }

    @Override
    public synchronized V putIfAbsent(K k, V v) {
        // 如果传进来的key不存在
        if (!containsKey(k))
            // 那么就调用内部的的put方法
            return put(k, v);
        else
            // 返回结果
            return get(k);
    }

    @Override
    public synchronized boolean remove(Object k, Object v) {
        if (containsKey(k) && get(k).equals(v)) {
            remove(k);
            return true;
        } else {
            return false;
        }
    }

    @Override
    public synchronized boolean replace(K k, V original, V replacement) {
        if (containsKey(k) && get(k).equals(original)) {
            put(k, replacement);
            return true;
        } else {
            return false;
        }
    }

    @Override
    public synchronized V replace(K k, V v) {
        if (containsKey(k)) {
            return put(k, v);
        } else {
            return null;
        }
    }

}
