/*
 * Copyright 2020 Three Guys Labs, LLC.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package threeguys.docsdb.impl.rocksdb;

import org.rocksdb.*;

public class RocksDbShim implements AutoCloseable {

    private final RocksDB db;

    public RocksDbShim(RocksDB db) {
        this.db = db;
    }

    public ColumnFamilyHandle createColumnFamily(ColumnFamilyDescriptor columnFamilyDescriptor) throws RocksDBException {
        return db.createColumnFamily(columnFamilyDescriptor);
    }

    public void put(ColumnFamilyHandle columnFamilyHandle, byte[] key, byte[] value) throws RocksDBException {
        db.put(columnFamilyHandle, key, value);
    }

    public void delete(ColumnFamilyHandle columnFamilyHandle, byte[] key) throws RocksDBException {
        db.delete(columnFamilyHandle, key);
    }

    public byte[] get(ColumnFamilyHandle columnFamilyHandle, byte[] key) throws RocksDBException {
        return db.get(columnFamilyHandle, key);
    }

    public ColumnFamilyMetaData getColumnFamilyMetaData(ColumnFamilyHandle columnFamilyHandle) {
        return db.getColumnFamilyMetaData(columnFamilyHandle);
    }

    public ColumnFamilyHandle getDefaultColumnFamily() {
        return db.getDefaultColumnFamily();
    }

    public void close() {
        db.close();
    }

}
