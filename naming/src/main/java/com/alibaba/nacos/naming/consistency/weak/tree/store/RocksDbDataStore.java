/*
 * Copyright 1999-2019 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.nacos.naming.consistency.weak.tree.store;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.nacos.naming.consistency.Datum;
import com.alibaba.nacos.naming.misc.Loggers;
import com.alibaba.nacos.naming.misc.Platform;
import com.alibaba.nacos.naming.pojo.Record;
import org.apache.commons.lang3.StringUtils;
import org.rocksdb.*;
import org.rocksdb.util.SizeUnit;
import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Datum store by RocksDB.
 * Inspired by RocksDBLogStorage.java in sofa-jraft (https://github.com/sofastack/sofa-jraft)
 *
 * @author satjd
 */
@Component
public class RocksDbDataStore {

    private static final Logger LOG = Loggers.TREE;

    static {
        RocksDB.loadLibrary();
    }

    @Value("${nacos.naming.tree.dataStore.rocksdb.basePath:data/tree/}")
    private String path;

    @Value("${nacos.naming.tree.dataStore.rocksdb.isSync:false}")
    private boolean sync;
    private RocksDB db;
    private DBOptions dbOptions;
    private WriteOptions writeOptions;
    private final List<ColumnFamilyOptions> cfOptions = new ArrayList<>();
    private ColumnFamilyHandle defaultHandle;
    private ColumnFamilyHandle confHandle;
    private ReadOptions totalOrderReadOptions;

    private final ReadWriteLock lock = new ReentrantReadWriteLock(false);
    private final Lock readLock  = this.lock.readLock();
    private final Lock writeLock = this.lock.writeLock();

    private static BlockBasedTableConfig createTableConfig() {
        // use hash search(btree) for prefix scan.
        return new BlockBasedTableConfig().
            setIndexType(IndexType.kHashSearch).
            setBlockSize(4 * SizeUnit.KB).
            setFilterPolicy(new BloomFilter(16, false)).
            setCacheIndexAndFilterBlocks(true).
            setBlockCache(new LRUCache(512 * SizeUnit.MB, 8));
    }

    public static DBOptions createDBOptions() {
        // Turn based on https://github.com/facebook/rocksdb/wiki/RocksDB-Tuning-Guide
        final DBOptions opts = new DBOptions();

        // If this value is set to true, then the database will be created if it is
        // missing during {@code RocksDB.open()}.
        opts.setCreateIfMissing(true);

        // If true, missing column families will be automatically created.
        opts.setCreateMissingColumnFamilies(true);

        // Number of open files that can be used by the DB.  You may need to increase
        // this if your database has a large working set. Value -1 means files opened
        // are always kept open.
        opts.setMaxOpenFiles(-1);

        // The maximum number of concurrent background compactions. The default is 1,
        // but to fully utilize your CPU and storage you might want to increase this
        // to approximately number of cores in the system.
        opts.setMaxBackgroundCompactions(Math.min(Runtime.getRuntime().availableProcessors(), 4));

        // The maximum number of concurrent flush operations. It is usually good enough
        // to set this to 1.
        opts.setMaxBackgroundFlushes(1);

        return opts;
    }

    public static ColumnFamilyOptions createColumnFamilyOptions() {
        final BlockBasedTableConfig tConfig = createTableConfig();
        final ColumnFamilyOptions opts = new ColumnFamilyOptions();

        // Flushing options:
        // write_buffer_size sets the size of a single mem_table. Once mem_table exceeds
        // this size, it is marked immutable and a new one is created.
        opts.setWriteBufferSize(64 * SizeUnit.MB);

        // Flushing options:
        // max_write_buffer_number sets the maximum number of mem_tables, both active
        // and immutable.  If the active mem_table fills up and the total number of
        // mem_tables is larger than max_write_buffer_number we stall further writes.
        // This may happen if the flush process is slower than the write rate.
        opts.setMaxWriteBufferNumber(3);

        // Flushing options:
        // min_write_buffer_number_to_merge is the minimum number of mem_tables to be
        // merged before flushing to storage. For example, if this option is set to 2,
        // immutable mem_tables are only flushed when there are two of them - a single
        // immutable mem_table will never be flushed.  If multiple mem_tables are merged
        // together, less data may be written to storage since two updates are merged to
        // a single key. However, every Get() must traverse all immutable mem_tables
        // linearly to check if the key is there. Setting this option too high may hurt
        // read performance.
        opts.setMinWriteBufferNumberToMerge(1);

        // Level Style Compaction:
        // level0_file_num_compaction_trigger -- Once level 0 reaches this number of
        // files, L0->L1 compaction is triggered. We can therefore estimate level 0
        // size in stable state as
        // write_buffer_size * min_write_buffer_number_to_merge * level0_file_num_compaction_trigger.
        opts.setLevel0FileNumCompactionTrigger(10);

        // Soft limit on number of level-0 files. We start slowing down writes at this
        // point. A value 0 means that no writing slow down will be triggered by number
        // of files in level-0.
        opts.setLevel0SlowdownWritesTrigger(20);

        // Maximum number of level-0 files.  We stop writes at this point.
        opts.setLevel0StopWritesTrigger(40);

        // Level Style Compaction:
        // max_bytes_for_level_base and max_bytes_for_level_multiplier
        //  -- max_bytes_for_level_base is total size of level 1. As mentioned, we
        // recommend that this be around the size of level 0. Each subsequent level
        // is max_bytes_for_level_multiplier larger than previous one. The default
        // is 10 and we do not recommend changing that.
        opts.setMaxBytesForLevelBase(512 * SizeUnit.MB);

        // Level Style Compaction:
        // target_file_size_base and target_file_size_multiplier
        //  -- Files in level 1 will have target_file_size_base bytes. Each next
        // level's file size will be target_file_size_multiplier bigger than previous
        // one. However, by default target_file_size_multiplier is 1, so files in all
        // L1..LMax levels are equal. Increasing target_file_size_base will reduce total
        // number of database files, which is generally a good thing. We recommend setting
        // target_file_size_base to be max_bytes_for_level_base / 10, so that there are
        // 10 files in level 1.
        opts.setTargetFileSizeBase(64 * SizeUnit.MB);

        // If prefix_extractor is set and memtable_prefix_bloom_size_ratio is not 0,
        // create prefix bloom for memtable with the size of
        // write_buffer_size * memtable_prefix_bloom_size_ratio.
        // If it is larger than 0.25, it is santinized to 0.25.
        opts.setMemtablePrefixBloomSizeRatio(0.125);

        // Seems like the rocksDB jni for Windows doesn't come linked with any of the
        // compression type
        if (!Platform.isWindows()) {
            opts.setCompressionType(CompressionType.LZ4_COMPRESSION)
                .setCompactionStyle(CompactionStyle.LEVEL)
                .optimizeLevelStyleCompaction();
        }

        return opts.useFixedLengthPrefixExtractor(8).
            setTableFormatConfig(tConfig).
            setMergeOperator(new StringAppendOperator());
    }

    @PostConstruct
    public boolean init() {
        this.writeLock.lock();
        try {
            if (this.db != null) {
                LOG.warn("RocksDBLogStorage init() already.");
                return true;
            }
            this.dbOptions = createDBOptions();

            this.writeOptions = new WriteOptions();
            this.writeOptions.setSync(this.sync);
            this.totalOrderReadOptions = new ReadOptions();
            this.totalOrderReadOptions.setTotalOrderSeek(true);

            return initAndLoad();
        } catch (final RocksDBException e) {
            LOG.error("Fail to init RocksDBLogStorage, path={}", this.path, e);
            return false;
        } finally {
            this.writeLock.unlock();
        }
    }

    private boolean initAndLoad() throws RocksDBException {
        final List<ColumnFamilyDescriptor> columnFamilyDescriptors = new ArrayList<>();
        final ColumnFamilyOptions cfOption = createColumnFamilyOptions();
        this.cfOptions.add(cfOption);
        columnFamilyDescriptors.add(new ColumnFamilyDescriptor("Configuration".getBytes(), cfOption));
        // default column family
        columnFamilyDescriptors.add(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, cfOption));

        openDB(columnFamilyDescriptors);
        return true;
    }

    private void openDB(final List<ColumnFamilyDescriptor> columnFamilyDescriptors) throws RocksDBException {
        final List<ColumnFamilyHandle> columnFamilyHandles = new ArrayList<>();

        final File dir = new File(this.path);
        dir.mkdirs();

        if (dir.exists() && !dir.isDirectory()) {
            throw new IllegalStateException("Invalid log path, it's a regular file: " + this.path);
        }
        this.db = RocksDB.open(this.dbOptions, this.path, columnFamilyDescriptors, columnFamilyHandles);

        this.confHandle = columnFamilyHandles.get(0);
        this.defaultHandle = columnFamilyHandles.get(1);
    }

    public void shutdown() {
        this.writeLock.lock();
        try {
            // The shutdown order is matter.
            // 1. close column family handles
            closeDB();
            // 2. close column family options.
            for (final ColumnFamilyOptions opt : this.cfOptions) {
                opt.close();
            }
            // 3. close options
            this.writeOptions.close();
            this.totalOrderReadOptions.close();
            // 4. help gc.
            this.cfOptions.clear();
            this.db = null;
            this.totalOrderReadOptions = null;
            this.writeOptions = null;
            this.defaultHandle = null;
            this.confHandle = null;
        } finally {
            this.writeLock.unlock();
        }
    }

    private void closeDB() {
        this.confHandle.close();
        this.defaultHandle.close();
        this.db.close();
    }

    public boolean reset(final long nextLogIndex) {
        if (nextLogIndex <= 0) {
            throw new IllegalArgumentException("Invalid next log index.");
        }
        this.writeLock.lock();
        try (Options opt = new Options()) {
            closeDB();
            try {
                RocksDB.destroyDB(this.path, opt);
            } catch (final RocksDBException e) {
                LOG.error("Fail to reset", e);
                return false;
            }
            return true;
        } finally {
            this.writeLock.unlock();
        }
    }

    public Datum read(String key, Class<? extends Record> valueType) {
        this.readLock.lock();
        try {
            String json = new String(db.get(defaultHandle, key.getBytes()));
            if (StringUtils.isBlank(json)) {
                return null;
            }
            JSONObject jsonObject = JSON.parseObject(json);
            Datum datum = new Datum();
            datum.timestamp.set(jsonObject.getLongValue("timestamp"));
            datum.key = jsonObject.getString("key");
            datum.value = JSON.parseObject(jsonObject.getString("value"), valueType);
            return datum;
        } catch (final RocksDBException e) {
            LOG.error("Fail to write datum", e);
            return null;
        } finally {
            this.readLock.unlock();
        }
    }

    public void write(Datum datum) {
        this.readLock.lock();
        try {
            this.db.put(this.defaultHandle, datum.key.getBytes(),
                JSON.toJSONString(datum).getBytes(StandardCharsets.UTF_8));
        } catch (final RocksDBException e) {
            LOG.error("Fail to write datum", e);
        } finally {
            this.readLock.unlock();
        }
    }

    public void remove(String key) {
        this.readLock.lock();
        try {
            this.db.delete(this.defaultHandle, key.getBytes());
        } catch (final RocksDBException e) {
            LOG.error("Fail to remove datum", e);
        } finally {
            this.readLock.unlock();
        }
    }
}
