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

import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.spec.GCMParameterSpec;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.util.Optional;
import java.util.function.LongConsumer;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.airlift.slice.SizeOf.instanceSize;
import static io.airlift.slice.SizeOf.sizeOfByteArray;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.util.Ciphers.createRandomAesEncryptionKey;
import static java.util.Objects.requireNonNull;

/**
 * A group-local match bitmap with one cached block. Encrypted blocks use a private
 * ephemeral key and a new IV on every write, including rewrites of the same block.
 */
final class DiskBackedMatchBitmap
        implements AutoCloseable
{
    static final int BUFFER_SIZE = 1024;
    private static final int INSTANCE_SIZE = instanceSize(DiskBackedMatchBitmap.class);
    private static final int INITIALIZATION_BLOCKS_PER_CALL = 16;

    private final Path path;
    private final RandomAccessFile file;
    private final byte[] buffer = new byte[BUFFER_SIZE];
    private final long byteCount;
    private final long fileSize;
    private final LongConsumer spillBytes;
    private final Optional<Encryption> encryption;
    private long initializedBytes;
    private long blockOffset = -1;
    private int blockLength;
    private boolean dirty;
    private boolean closed;

    public DiskBackedMatchBitmap(Path path, long positions, LongConsumer spillBytes)
    {
        this(path, positions, spillBytes, false);
    }

    public DiskBackedMatchBitmap(Path path, long positions, LongConsumer spillBytes, boolean encrypted)
    {
        this.path = requireNonNull(path, "path is null");
        this.spillBytes = requireNonNull(spillBytes, "spillBytes is null");
        checkArgument(positions >= 0, "positions is negative");
        byteCount = Math.ceilDiv(positions, Byte.SIZE);
        fileSize = encrypted ? Math.multiplyExact(Math.ceilDiv(byteCount, BUFFER_SIZE), Encryption.BLOCK_SIZE) : byteCount;
        RandomAccessFile opened = null;
        boolean reserved = false;
        try {
            spillBytes.accept(fileSize);
            reserved = true;
            encryption = encrypted ? Optional.of(new Encryption()) : Optional.empty();
            opened = new RandomAccessFile(path.toFile(), "rw");
            opened.setLength(fileSize);
            file = opened;
        }
        catch (IOException | GeneralSecurityException | RuntimeException e) {
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
                    spillBytes.accept(-fileSize);
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

    public static long retainedSize(boolean encrypted)
    {
        // Include the bounded cipher/record state using a conservative estimate for JCE objects.
        return INSTANCE_SIZE + sizeOfByteArray(BUFFER_SIZE) + (encrypted ? 4096 : 0);
    }

    /**
     * Initializes encrypted zero blocks in bounded steps before any matching can modify them.
     */
    public boolean initialize()
    {
        checkState(!closed, "Match bitmap is closed");
        if (encryption.isEmpty()) {
            return true;
        }
        try {
            for (int block = 0; block < INITIALIZATION_BLOCKS_PER_CALL && initializedBytes < byteCount; block++) {
                encryption.orElseThrow().write(file, initializedBytes, buffer);
                initializedBytes += BUFFER_SIZE;
            }
            return initializedBytes >= byteCount;
        }
        catch (IOException | GeneralSecurityException e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Failed to initialize encrypted merge join match file", e);
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
        checkState(encryption.isEmpty() || initializedBytes >= byteCount, "Match bitmap initialization incomplete");
        long offset = (byteIndex / BUFFER_SIZE) * BUFFER_SIZE;
        if (offset == blockOffset) {
            return;
        }
        try {
            flush();
            blockOffset = offset;
            blockLength = (int) Math.min(BUFFER_SIZE, byteCount - offset);
            if (encryption.isPresent()) {
                encryption.orElseThrow().read(file, blockOffset, buffer);
            }
            else {
                file.seek(offset);
                file.readFully(buffer, 0, blockLength);
            }
        }
        catch (IOException | GeneralSecurityException e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Failed to read merge join match file", e);
        }
    }

    private void flush()
            throws IOException, GeneralSecurityException
    {
        if (dirty) {
            if (encryption.isPresent()) {
                encryption.orElseThrow().write(file, blockOffset, buffer);
            }
            else {
                file.seek(blockOffset);
                file.write(buffer, 0, blockLength);
            }
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
            try {
                file.close();
            }
            finally {
                Files.deleteIfExists(path);
            }
        }
        catch (IOException e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Failed to delete merge join match file", e);
        }
        finally {
            spillBytes.accept(-fileSize);
        }
    }

    private static final class Encryption
    {
        private static final int IV_SIZE = 12;
        private static final int TAG_BITS = 128;
        private static final int BLOCK_SIZE = IV_SIZE + BUFFER_SIZE + TAG_BITS / Byte.SIZE;

        private final SecretKey key;
        private final Cipher cipher;
        private final byte[] record = new byte[BLOCK_SIZE];
        private final ByteBuffer associatedData = ByteBuffer.allocate(Long.BYTES);
        private long sequence;

        private Encryption()
                throws GeneralSecurityException
        {
            key = createRandomAesEncryptionKey();
            cipher = Cipher.getInstance("AES/GCM/NoPadding");
        }

        public void read(RandomAccessFile file, long offset, byte[] buffer)
                throws IOException, GeneralSecurityException
        {
            file.seek(Math.multiplyExact(offset / BUFFER_SIZE, BLOCK_SIZE));
            file.readFully(record);
            initialize(Cipher.DECRYPT_MODE, offset);
            cipher.doFinal(record, IV_SIZE, BLOCK_SIZE - IV_SIZE, buffer, 0);
        }

        public void write(RandomAccessFile file, long offset, byte[] buffer)
                throws IOException, GeneralSecurityException
        {
            // The key belongs only to this bitmap. The counter guarantees unique IVs even
            // when repeated probes revisit and modify an earlier block. Never wrap the counter.
            sequence = Math.incrementExact(sequence);
            ByteBuffer.wrap(record).putInt(0).putLong(sequence);
            initialize(Cipher.ENCRYPT_MODE, offset);
            cipher.doFinal(buffer, 0, BUFFER_SIZE, record, IV_SIZE);
            file.seek(Math.multiplyExact(offset / BUFFER_SIZE, BLOCK_SIZE));
            file.write(record);
        }

        private void initialize(int mode, long offset)
                throws GeneralSecurityException
        {
            cipher.init(mode, key, new GCMParameterSpec(TAG_BITS, record, 0, IV_SIZE));
            // Bind ciphertext to the logical block, so moving it to another offset is rejected.
            associatedData.clear().putLong(offset).flip();
            cipher.updateAAD(associatedData);
        }
    }
}
