/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.operator.join.merge;

import io.trino.spi.TrinoException;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.function.LongConsumer;

import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static java.util.Objects.requireNonNull;

/**
 * A group-local match bitmap. One block is cached so forward scans and repeated
 * passes over a large group need bounded memory and sequential file I/O.
 */
final class DiskBackedMatchBitmap
        implements AutoCloseable
{
    static final int BUFFER_SIZE = 1024;

    private final Path path;
    private final RandomAccessFile file;
    private final byte[] buffer = new byte[BUFFER_SIZE];
    private final long byteCount;
    private final LongConsumer spillBytes;
    private long blockOffset = -1;
    private int blockLength;
    private boolean dirty;
    private boolean closed;

    public DiskBackedMatchBitmap(Path path, long positions, LongConsumer spillBytes)
    {
        this.path = requireNonNull(path, "path is null");
        this.spillBytes = requireNonNull(spillBytes, "spillBytes is null");
        byteCount = (positions + 7) / 8;
        RandomAccessFile opened = null;
        boolean reserved = false;
        try {
            spillBytes.accept(byteCount);
            reserved = true;
            opened = new RandomAccessFile(path.toFile(), "rw");
            opened.setLength(byteCount);
            file = opened;
        }
        catch (IOException | RuntimeException e) {
            try {
                if (opened != null) {
                    opened.close();
                }
                Files.deleteIfExists(path);
            }
            catch (IOException cleanupFailure) {
                e.addSuppressed(cleanupFailure);
            }
            if (reserved) {
                try {
                    spillBytes.accept(-byteCount);
                }
                catch (RuntimeException cleanupFailure) {
                    e.addSuppressed(cleanupFailure);
                }
            }
            if (e instanceof RuntimeException runtimeException) {
                throw runtimeException;
            }
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Failed to initialize merge join match file", e);
        }
    }

    public void mark(long position)
    {
        long byteIndex = position >>> 3;
        loadBlock(byteIndex);
        buffer[(int) (byteIndex - blockOffset)] |= (byte) (1 << (position & 7));
        dirty = true;
    }

    public boolean isMarked(long position)
    {
        long byteIndex = position >>> 3;
        loadBlock(byteIndex);
        return (buffer[(int) (byteIndex - blockOffset)] & (1 << (position & 7))) != 0;
    }

    private void loadBlock(long byteIndex)
    {
        long offset = ((long) byteIndex / BUFFER_SIZE) * BUFFER_SIZE;
        if (offset == blockOffset) {
            return;
        }
        try {
            flush();
            blockOffset = offset;
            blockLength = (int) Math.min(BUFFER_SIZE, byteCount - offset);
            file.seek(offset);
            file.readFully(buffer, 0, blockLength);
        }
        catch (IOException e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Failed to read merge join match file", e);
        }
    }

    private void flush()
            throws IOException
    {
        if (dirty) {
            file.seek(blockOffset);
            file.write(buffer, 0, blockLength);
            dirty = false;
        }
    }

    @Override
    public void close()
    {
        if (closed) {
            return;
        }
        closed = true;
        try {
            file.close();
            Files.deleteIfExists(path);
        }
        catch (IOException e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Failed to delete merge join match file", e);
        }
        finally {
            spillBytes.accept(-byteCount);
        }
    }
}
