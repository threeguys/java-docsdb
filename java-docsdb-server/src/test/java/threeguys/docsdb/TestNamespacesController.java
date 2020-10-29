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
package threeguys.docsdb;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import threeguys.docsdb.api.Namespace;

import java.util.HashSet;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;
import static threeguys.docsdb.Database.bytes;

public class TestNamespacesController {

    private Database database;
    private NamespacesController controller;

    @BeforeEach
    public void setup() {
        database = mock(Database.class);
        controller = new NamespacesController(database);
    }

    private Database verifyDb(DatabaseConsumer<Database> verifier) throws DatabaseException{
        Database db = verify(database, times(1));
        verifier.accept(db);
        verifyNoMoreInteractions(db);
        return db;
    }

    @Test
    public void testGetNamespaces() throws DatabaseException {
        when(database.getNamespaces()).thenReturn(new HashSet<>());
        assertEquals(new HashSet<>(), controller.getNamespaces());
        verifyDb(Database::getNamespaces);
    }

    @Test
    public void testGetNamespace() throws DatabaseException {
        when(database.getMetaData(anyString())).thenReturn(new Namespace("my-namespace", 42L));
        assertEquals(new Namespace("my-namespace", 42L), controller.getNamespace("my-namespace"));
        verifyDb((db) -> db.getMetaData(eq("my-namespace")));
    }

    @Test
    public void testGetKey() throws DatabaseException {
        when(database.get(anyString(), anyString())).thenReturn(bytes("what a value!"));
        assertArrayEquals(bytes("what a value!"), controller.getKey("a-test-ns", "this-is-a-key"));
        verifyDb((db) -> db.get(eq("a-test-ns"), eq("this-is-a-key")));
    }

    @Test
    public void testPutKey() throws DatabaseException {
        assertEquals(NamespacesController.OK, controller.putKey("test-ns", "test-key", bytes("my-value")));
        verifyDb((db) -> db.put(eq("test-ns"), eq("test-key"), eq(bytes("my-value"))));
    }

    @Test
    public void testDeleteKey() throws DatabaseException {
        assertEquals(NamespacesController.OK, controller.deleteKey("test-delete-ns", "target-key"));
        verifyDb((db) -> db.delete(eq("test-delete-ns"), eq("target-key")));
    }

}
