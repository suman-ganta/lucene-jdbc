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

package org.apache.lucene.store.wrapper;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.lucene.store.*;
import org.apache.lucene.util.NamedThreadFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Wraps a Lucene {@link Directory} with
 * an in memory directory which mirrors it asynchronously.
 * <p/>
 * The original directory is read into memory when this wrapper
 * is constructed. All read realted operations are performed
 * against the in memory directory. All write related operations
 * are performed against the in memeory directory and are scheduled
 * to be performed against the original directory (using {@link ExecutorService}).
 * Locking is performed using the in memory directory.
 * <p/>
 * NOTE: This wrapper will only work in cases when either the
 * index is read only (i.e. only search operations are performed
 * against it), or when there is a single instance which updates
 * the directory.
 *
 * @author kimchy
 */
public class AsyncMemoryMirrorDirectoryWrapper extends Directory {

    private static final Log log = LogFactory.getLog(AsyncMemoryMirrorDirectoryWrapper.class);

    private Directory dir;

    private RAMDirectory ramDir;

    private ExecutorService executorService;

    private long awaitTermination;

    public AsyncMemoryMirrorDirectoryWrapper(Directory dir) throws IOException {
        this(dir, 2);
    }

    public AsyncMemoryMirrorDirectoryWrapper(Directory dir, long awaitTermination) throws IOException {
        this(dir, awaitTermination, Executors.newSingleThreadExecutor(new NamedThreadFactory("AsyncMirror[" + dir + "]")));
    }

    public AsyncMemoryMirrorDirectoryWrapper(Directory dir, long awaitTermination, ExecutorService executorService) throws IOException {
        this.dir = dir;
        this.ramDir = new RAMDirectory(dir, null/*FIXMESG*/);
        this.executorService = executorService;
        this.awaitTermination = awaitTermination;
    }

    public void deleteFile(final String name) throws IOException {
        ramDir.deleteFile(name);
        executorService.submit(new Runnable() {
            public void run() {
                try {
                    dir.deleteFile(name);
                } catch (IOException e) {
                    logAsyncErrorMessage("delete [" + name + "]");
                }
            }
        });
    }

    public long fileLength(String name) throws IOException {
        return ramDir.fileLength(name);
    }

    public String[] listAll() throws IOException {
        return ramDir.listAll();
    }

    public void renameFile(final String from, final String to) throws IOException {
        ramDir.renameFile(from, to);
        executorService.submit(new Runnable() {
            public void run() {
                try {
                    dir.renameFile(from, to);
                } catch (IOException e) {
                    logAsyncErrorMessage("rename from[" + from + "] to[" + to + "]");
                }
            }
        });
    }

    public Lock makeLock(String name) {
        return ramDir.makeLock(name);
    }

    public void close() throws IOException {
        ramDir.close();
        if (log.isDebugEnabled()) {
            log.debug("Directory [" + dir + "] shutsdown, waiting for [" + awaitTermination +
                    "] minutes for tasks to finish executing");
        }
        executorService.shutdown();
        if (!executorService.isTerminated()) {
            try {
                if (!executorService.awaitTermination(60 * awaitTermination, TimeUnit.SECONDS)) {
                    logAsyncErrorMessage("wait for async tasks to shutdown");
                }
            } catch (InterruptedException e) {
                logAsyncErrorMessage("wait for async tasks to shutdown");
            }
        }
        dir.close();
    }

    public IndexInput openInput(String name, IOContext context) throws IOException {
        return ramDir.openInput(name, context);
    }

    public IndexOutput createOutput(String name, IOContext context) throws IOException {
        return new AsyncMemoryMirrorIndexOutput(name, (RAMOutputStream) ramDir.createOutput(name, context));
    }

    @Override
    public void sync(Collection<String> names) throws IOException {

    }

    private void logAsyncErrorMessage(String message) {
        log.error("Async wrapper for [" + dir + "] failed to " + message);
    }

    public class AsyncMemoryMirrorIndexOutput extends IndexOutput {

        private String name;

        private RAMOutputStream ramIndexOutput;

        public AsyncMemoryMirrorIndexOutput(String name, RAMOutputStream ramIndexOutput) {
            super(name);
            this.name = name;
            this.ramIndexOutput = ramIndexOutput;
        }

        public void writeByte(byte b) throws IOException {
            ramIndexOutput.writeByte(b);
        }

        public void writeBytes(byte[] b, int offset, int length) throws IOException {
            ramIndexOutput.writeBytes(b, offset, length);
        }

        public long getFilePointer() {
            return ramIndexOutput.getFilePointer();
        }

        @Override
        public long getChecksum() throws IOException {
            return ramIndexOutput.getChecksum();
        }

        public void close() throws IOException {
            ramIndexOutput.close();
            executorService.submit(new Runnable() {
                public void run() {
                    try {
                        IndexOutput indexOutput = dir.createOutput(name, null/*FIXMESG*/);
                        ramIndexOutput.writeTo(indexOutput);
                        indexOutput.close();
                    } catch (IOException e) {
                        logAsyncErrorMessage("write [" + name + "]");
                    }
                }
            });
        }
    }
}
