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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.stubbing.Answer;
import org.rocksdb.*;
import threeguys.docsdb.Database;
import threeguys.docsdb.DatabaseException;
import threeguys.docsdb.api.Namespace;

import java.nio.file.Path;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;
import static threeguys.docsdb.Database.bytes;

public class TestRocksDbDatabaseImpl {

    @Test
    public void smokeTest(@TempDir Path tempDir) throws RocksDBException, DatabaseException {
        Options options = new Options().setCreateIfMissing(true);
        try (RocksDB db = RocksDB.open(options, tempDir.toFile().getAbsolutePath())) {
            RocksDbDatabaseImpl impl = new RocksDbDatabaseImpl(new RocksDbShim(db), new HashMap<>());

            assertEquals(Collections.emptySet(), impl.getNamespaces());
            Namespace ns = impl.getMetaData("a-new-table");
            assertEquals("ns:a-new-table", ns.getName());
            assertEquals(0, ns.getSize());

            assertEquals(new HashSet<>(Arrays.asList("ns:a-new-table")), impl.getNamespaces());
            assertNull(impl.get("a-new-table", "here's a key"));

            impl.put("a-new-table", "here's a key", bytes("this is some data"));
            assertArrayEquals(bytes("this is some data"), impl.get("a-new-table", "here's a key"));

            impl.delete("a-new-table", "here's a key");
            assertNull(impl.get("a-new-table", "here's a key"));
        }
    }

    RocksDbShim mockDb(RocksDbConsumer<RocksDbShim> setup) throws RocksDBException {
        RocksDbShim db = mock(RocksDbShim.class);
        when(db.createColumnFamily(any(ColumnFamilyDescriptor.class)))
                .thenAnswer((Answer<ColumnFamilyHandle>) invocationOnMock -> mock(ColumnFamilyHandle.class));
        setup.accept(db);
        return db;
    }

    @Test
    public void getFailure() throws RocksDBException {
        RocksDbShim db = mockDb(
                (d) -> when(d.get(any(ColumnFamilyHandle.class), any(byte[].class)))
                    .thenThrow(new RocksDBException("test-error")));

        RocksDbDatabaseImpl impl = new RocksDbDatabaseImpl(db, new HashMap<>());
        assertThrows(DatabaseException.class, () -> impl.get("test", "a-key"));
    }

    @Test
    public void putFailure() throws RocksDBException {
        RocksDbShim db = mockDb(
                (d) -> doThrow(new RocksDBException("test-error"))
                .when(d).put(any(ColumnFamilyHandle.class), any(byte[].class), any(byte[].class)));

        RocksDbDatabaseImpl impl = new RocksDbDatabaseImpl(db, new HashMap<>());
        assertThrows(DatabaseException.class,
                () -> impl.put("test", "a-key", bytes("a-value")));
    }

    @Test
    public void deleteFailure() throws RocksDBException {
        RocksDbShim db = mockDb(
                (d) -> doThrow(new RocksDBException("test-error"))
                    .when(d).delete(any(ColumnFamilyHandle.class), any(byte[].class)));

        RocksDbDatabaseImpl impl = new RocksDbDatabaseImpl(db, new HashMap<>());
        assertThrows(DatabaseException.class,
                () -> impl.delete("test", "a-key"));
    }

    @Test
    public void emptyDatabaseBootstrap(@TempDir Path tempDir) throws RocksDBException, DatabaseException {

        Set<String> expectedTables = new HashSet<>(Arrays.asList("default", "ns:test-table-1", "ns:test-table-2"));

        try (Database impl = RocksDbDatabaseImpl.openDatabase(tempDir.toString())) {
            assertEquals(new HashSet<>(Collections.singletonList("default")), impl.getNamespaces());
            impl.put("test-table-1", "tbl1-key1", bytes("value-1-1"));
            impl.put("test-table-2", "tbl2-key1", bytes("value-2-1"));
            impl.put("test-table-2", "tbl2-key2", bytes("value-2-2"));

            assertEquals(expectedTables, impl.getNamespaces());
            Namespace ns1 = impl.getMetaData("test-table-1");
            assertEquals("ns:test-table-1", ns1.getName());
            Namespace ns2 = impl.getMetaData("test-table-2");
            assertEquals("ns:test-table-2", ns2.getName());
        }

        try (Database impl = RocksDbDatabaseImpl.openDatabase(tempDir.toString())) {
            assertEquals(expectedTables, impl.getNamespaces());
            assertArrayEquals(bytes("value-1-1"), impl.get("test-table-1", "tbl1-key1"));
            assertArrayEquals(bytes("value-2-1"), impl.get("test-table-2", "tbl2-key1"));
            assertArrayEquals(bytes("value-2-2"), impl.get("test-table-2", "tbl2-key2"));
        }

    }
}
