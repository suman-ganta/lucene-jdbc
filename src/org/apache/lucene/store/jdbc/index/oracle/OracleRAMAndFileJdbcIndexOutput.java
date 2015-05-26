/*
 * Copyright 2004-2009 the original author or authors.
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

package org.apache.lucene.store.jdbc.index.oracle;

import org.apache.lucene.store.jdbc.index.FileJdbcIndexOutput;
import org.apache.lucene.store.jdbc.index.RAMAndFileJdbcIndexOutput;
import org.apache.lucene.store.jdbc.index.RAMJdbcIndexOutput;

/**
 * A specialized Oracle version that works (through reflection) with
 * Oracle 9i/8i specific blob API for blobs bigger than 4k.
 *
 * @author kimchy
 */
public class OracleRAMAndFileJdbcIndexOutput extends RAMAndFileJdbcIndexOutput {

    private final String desc;

    /**
     * Sole constructor.  resourceDescription should be non-null, opaque string
     * describing this resource; it's returned from {@link #toString}.
     *
     * @param resourceDescription
     */
    protected OracleRAMAndFileJdbcIndexOutput(String resourceDescription) {
        super(resourceDescription);
        this.desc = resourceDescription;
    }

    protected FileJdbcIndexOutput createFileJdbcIndexOutput() {
        return new OracleFileJdbcIndexOutput(desc);
    }

    protected RAMJdbcIndexOutput createRamJdbcIndexOutput() {
        return new OracleRAMJdbcIndexOutput(desc);
    }
}
