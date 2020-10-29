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

import threeguys.docsdb.api.Namespace;

import java.nio.charset.StandardCharsets;
import java.util.Set;

public interface Database extends AutoCloseable {

    Set<String> getNamespaces();
    Namespace getMetaData(String table) throws DatabaseException;
    void close();

    byte [] get(String table, String key) throws DatabaseException;
    Void put(String table, String key, byte [] data) throws DatabaseException;
    Void delete(String table, String key) throws DatabaseException;

    static byte [] bytes(String data) {
        return data.getBytes(StandardCharsets.UTF_8);
    }

}
