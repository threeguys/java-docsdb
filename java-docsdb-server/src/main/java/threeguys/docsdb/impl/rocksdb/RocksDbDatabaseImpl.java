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
import org.springframework.boot.json.JsonParser;
import org.springframework.boot.json.JsonParserFactory;
import threeguys.docsdb.Database;
import threeguys.docsdb.DatabaseException;
import threeguys.docsdb.api.Namespace;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static threeguys.docsdb.Database.bytes;

public class RocksDbDatabaseImpl implements Database {

    private static final byte [] SYS_NAMESPACES = "sys/namespaces".getBytes(StandardCharsets.UTF_8);

    private static class NamespaceEntry {

        public static final String CREATING = "CREATING";
        public static final String LIVE = "LIVE";

        final String name;
        final ColumnFamilyHandle handle;

        public NamespaceEntry(String name, ColumnFamilyHandle handle) {
            this.name = name;
            this.handle = handle;
        }

    }

    private final RocksDbShim db;
    private final Map<String, NamespaceEntry> namespaces;

    public RocksDbDatabaseImpl(RocksDbShim db, Map<String, NamespaceEntry> namespaces) {
        this.db = db;
        this.namespaces = new ConcurrentHashMap<>(namespaces);
    }

    @Override
    public byte [] get(String namespace, String key) throws DatabaseException {
        try {
            return db.get(getHandle(namespace), bytes(key));
        } catch (RocksDBException e) {
            throw new DatabaseException("unable to get key", e);
        }
    }

    @Override
    public Void put(String namespace, String key, byte [] data) throws DatabaseException {
        try {
            db.put(getHandle(namespace), bytes(key), data);
            return null;
        } catch (RocksDBException e) {
            throw new DatabaseException("unable to put key", e);
        }
    }

    @Override
    public Void delete(String namespace, String key) throws DatabaseException {
        try {
            db.delete(getHandle(namespace), bytes(key));
            return null;
        } catch (RocksDBException e) {
            throw new DatabaseException("unable to delete key", e);
        }
    }

    @Override
    public void close() {
        db.close();
    }

    @Override
    public Set<String> getNamespaces() {
        return namespaces.keySet();
    }

    @Override
    public Namespace getMetaData(String namespace) throws DatabaseException {
        ColumnFamilyHandle handle = getHandle(namespace);
        ColumnFamilyMetaData md = db.getColumnFamilyMetaData(handle);
        return new Namespace(new String(md.name(), StandardCharsets.UTF_8), md.size());
    }

    private String escape(String value) {
        return value.replace("\"", "\\\"");
    }

    private byte [] toJson(Map<String, String> states) {
        return ("{" +
                states.entrySet().stream()
                .map(e -> new StringBuilder("\"")
                            .append(escape(e.getKey())).append("\":\"")
                            .append(escape(e.getValue())).append("\"")
                            .toString())
                .collect(Collectors.joining(","))
                + "}").getBytes(StandardCharsets.UTF_8);

    }

    private NamespaceEntry getNamespace(String namespace) throws DatabaseException {
        String nsKey = "ns:" + namespace;

        if (!namespaces.containsKey(nsKey)) {
            synchronized (this) {
                try {
                    if (!namespaces.containsKey(nsKey)) {
                        ColumnFamilyOptions cfOpts = new ColumnFamilyOptions().optimizeUniversalStyleCompaction();
                        ColumnFamilyDescriptor cfd = new ColumnFamilyDescriptor(bytes(nsKey), cfOpts);

                        Map<String, String> tableList = readNamespaceState(db);

                        // Write that we're creating the namespace (2pc)
                        tableList.put(nsKey, NamespaceEntry.CREATING);
                        db.put(db.getDefaultColumnFamily(), SYS_NAMESPACES, toJson(tableList));

                        ColumnFamilyHandle handle = db.createColumnFamily(cfd);

                        // Write that we created the namespace
                        tableList.put(nsKey, NamespaceEntry.LIVE);
                        db.put(db.getDefaultColumnFamily(), SYS_NAMESPACES, toJson(tableList));

                        // Add it to our list
                        namespaces.put(nsKey, new NamespaceEntry(namespace, handle));
                    }
                } catch (RocksDBException e) {
                    throw new DatabaseException("Could not get namespace", e);
                }
            }
        }

        return namespaces.get(nsKey);
    }

    private ColumnFamilyHandle getHandle(String table) throws DatabaseException {
        NamespaceEntry te = getNamespace(table);
        return te.handle;
    }

    private static Map<String, String> readNamespaceState(RocksDbShim db) throws RocksDBException {
        byte [] tablesData = db.get(db.getDefaultColumnFamily(), SYS_NAMESPACES);

        if (tablesData == null || tablesData.length == 0) {
            Map<String, String> data = new HashMap<>();
            data.put("default", NamespaceEntry.LIVE);
            return data;
        }

        JsonParser parser = JsonParserFactory.getJsonParser();
        Map<String, Object> state = parser.parseMap(new String(tablesData, StandardCharsets.UTF_8));

        return state.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, (e) -> e.getValue().toString()));
    }

    private static List<ColumnFamilyDescriptor> mapDescriptors(Map<String, String> states) {
        final ColumnFamilyOptions cfOpts = new ColumnFamilyOptions().optimizeUniversalStyleCompaction();
        return states.keySet().stream()
                .map(s -> new ColumnFamilyDescriptor(bytes(s), cfOpts))
                .collect(Collectors.toList());
    }

    private static ColumnFamilyDescriptor defaultColumnFamily() {
        final ColumnFamilyOptions cfOpts = new ColumnFamilyOptions().optimizeUniversalStyleCompaction();
        return new ColumnFamilyDescriptor(bytes("default"), cfOpts);
    }

    public static Database openDatabase(String path) throws DatabaseException {
        try {

            Map<String, String> tableState;
            List<ColumnFamilyDescriptor> descriptors;
            List<ColumnFamilyHandle> handles = new ArrayList<>();

            if (new File(path + "/CURRENT").isFile()) {
                try (final RocksDB db = RocksDB.openReadOnly(path)) {
                    tableState = readNamespaceState(new RocksDbShim(db));
                    descriptors = mapDescriptors(tableState);
                }
            } else {
                tableState = new HashMap<>();
                tableState.put("default", NamespaceEntry.LIVE);
                descriptors = Collections.singletonList(defaultColumnFamily());
            }

            final DBOptions opts = new DBOptions()
                    .setCreateIfMissing(true)
                    .setStatistics(new Statistics());

            RocksDB db = RocksDB.open(opts, path, descriptors, handles);

            Map<String, NamespaceEntry> namespaces = new HashMap<>();
            Iterator<ColumnFamilyDescriptor> descIt = descriptors.iterator();
            Iterator<ColumnFamilyHandle> handleIt = handles.iterator();
            while (descIt.hasNext() && handleIt.hasNext()) {
                ColumnFamilyDescriptor d = descIt.next();
                ColumnFamilyHandle h = handleIt.next();
                String name = new String(d.getName(), StandardCharsets.UTF_8);
                namespaces.put(name, new NamespaceEntry(name, h));
            }

            return new RocksDbDatabaseImpl(new RocksDbShim(db), namespaces);

        } catch (RocksDBException e) {
            throw new DatabaseException("Error opening database", e);
        }
    }

}
