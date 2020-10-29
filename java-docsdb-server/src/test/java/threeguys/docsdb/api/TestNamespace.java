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
package threeguys.docsdb.api;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestNamespace {

    @Test
    public void smokeTest() {
        Namespace ns = new Namespace("a-namespace", 13L);
        assertEquals("a-namespace", ns.getName());
        assertEquals(13L, ns.getSize());
        assertEquals(new Namespace("a-namespace", 13L), ns);
        assertEquals(new Namespace("a-namespace", 13L).hashCode(), ns.hashCode());
    }

}
