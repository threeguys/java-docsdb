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

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;
import threeguys.docsdb.api.Namespace;
import threeguys.docsdb.api.Result;

import java.util.Set;

@RestController
public class NamespacesController {

    public static final Result OK = new Result("ok");

    private final Database db;

    public NamespacesController(@Autowired Database db) {
        this.db = db;
    }

    @RequestMapping(path = "/namespace", method = RequestMethod.GET, produces = "application/json")
    public Set<String> getNamespaces() {
        return db.getNamespaces();
    }

    @RequestMapping(path = "/namespace/{namespace}", method = RequestMethod.GET, produces = "application/json")
    public Namespace getNamespace(@PathVariable("namespace") String namespace) throws DatabaseException {
        return db.getMetaData(namespace);
    }

    @RequestMapping(path = "/namespace/{namespace}/{key}", method = RequestMethod.GET, produces = "application/json")
    public byte [] getKey(@PathVariable("namespace") String namespace, @PathVariable("key") String key) throws DatabaseException {
        return db.get(namespace, key);
    }

    @RequestMapping(path = "/namespace/{namespace}/{key}", method = RequestMethod.PUT, produces = "application/json", consumes = "application/json")
    public Result putKey(@PathVariable("namespace") String namespace, @PathVariable("key") String key, @RequestBody byte [] body) throws DatabaseException {
        db.put(namespace, key, body);
        return OK;
    }

    @RequestMapping(path = "/namespace/{namespace}/{key}", method = RequestMethod.DELETE)
    public Result deleteKey(@PathVariable("namespace") String namespace, @PathVariable("key") String key) throws DatabaseException {
        db.delete(namespace, key);
        return OK;
    }

}
