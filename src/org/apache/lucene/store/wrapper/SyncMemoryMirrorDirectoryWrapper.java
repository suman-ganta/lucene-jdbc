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

import java.io.IOException;
import java.util.Collection;

import org.apache.lucene.store.*;

/**
 * Wraps a Lucene {@link Directory} with
 * an in memory directory which mirrors it synchronously.
 * <p/>
 * The original directory is read into memory when this wrapper
 * is constructed. All read realted operations are performed
 * against the in memory directory. All write related operations
 * are performed both against the in memeory directory and the
 * original directory. Locking is performed using the in memory
 * directory.
 * <p/>
 * NOTE: This wrapper will only work in cases when either the
 * index is read only (i.e. only search operations are performed
 * against it), or when there is a single instance which updates
 * the directory.
 *
 * @author kimchy
 */
public class SyncMemoryMirrorDirectoryWrapper extends Directory {

    private Directory dir;

    private RAMDirectory ramDir;

    public SyncMemoryMirrorDirectoryWrapper(Directory dir) throws IOException {
        this.dir = dir;
        this.ramDir = new RAMDirectory(dir, null/*FIXMESG*/);
    }

    public void deleteFile(String name) throws IOException {
        ramDir.deleteFile(name);
        dir.deleteFile(name);
    }

    public long fileLength(String name) throws IOException {
        return ramDir.fileLength(name);
    }

    public String[] listAll() throws IOException {
        return ramDir.listAll();
    }

    public void renameFile(String from, String to) throws IOException {
        ramDir.renameFile(from, to);
        dir.renameFile(from, to);
    }

    public Lock makeLock(String name) {
        return ramDir.makeLock(name);
    }

    public void close() throws IOException {
        ramDir.close();
        dir.close();
    }

    public IndexInput openInput(String name, IOContext context) throws IOException {
        return ramDir.openInput(name, context);
    }

    public IndexOutput createOutput(String name, IOContext context) throws IOException {
        return new SyncMemoryMirrorIndexOutput(name, dir.createOutput(name, context), (RAMOutputStream) ramDir.createOutput(name, context));
    }

    @Override
    public void sync(Collection<String> names) throws IOException {

    }

    public static class SyncMemoryMirrorIndexOutput extends IndexOutput {

        private IndexOutput origIndexOutput;

        private RAMOutputStream ramIndexOutput;

        public SyncMemoryMirrorIndexOutput(String desc, IndexOutput origIndexOutput, RAMOutputStream ramIndexOutput) {
            super(desc);
            this.origIndexOutput = origIndexOutput;
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
            ramIndexOutput.writeTo(origIndexOutput);
            origIndexOutput.close();
        }
    }
}
