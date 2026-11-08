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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import javax.crypto.AEADBadTagException;

import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicLong;

import static com.google.common.primitives.Bytes.indexOf;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestDiskBackedMatchBitmap
{
    @Test
    public void testInitializationYieldsAndCanBeCancelled(@TempDir Path directory)
            throws Exception
    {
        Path path = Files.createTempFile(directory, "initializing-matches", ".bin");
        AtomicLong reserved = new AtomicLong();
        try (DiskBackedMatchBitmap bitmap = new DiskBackedMatchBitmap(path, 160000, reserved::addAndGet, true)) {
            assertThat(bitmap.initialize()).isFalse();
            assertThatThrownBy(() -> bitmap.mark(0)).hasMessage("Match bitmap initialization incomplete");
            assertThat(Files.exists(path)).isTrue();
        }
        assertThat(reserved.get()).isZero();
        assertThat(Files.exists(path)).isFalse();
    }

    @Test
    public void testZeroStateIsEncryptedBeforeMatching(@TempDir Path directory)
            throws Exception
    {
        Path path = Files.createTempFile(directory, "zero-matches", ".bin");
        try (DiskBackedMatchBitmap bitmap = new DiskBackedMatchBitmap(path, 9000, _ -> {}, true)) {
            while (!bitmap.initialize()) {
                Thread.yield();
            }
            byte[] content = Files.readAllBytes(path);
            assertThat(Arrays.equals(content, new byte[content.length])).isFalse();
            assertThat(bitmap.isMarked(0)).isFalse();
            assertThat(bitmap.isMarked(8999)).isFalse();
        }
        assertThat(Files.exists(path)).isFalse();
    }

    @Test
    public void testEncryptedPositionsBeyondIntegerLimit(@TempDir Path directory)
            throws Exception
    {
        Path path = Files.createTempFile(directory, "large-encrypted-matches", ".bin");
        long positions = (long) Integer.MAX_VALUE + 128;
        AtomicLong reserved = new AtomicLong();
        try (DiskBackedMatchBitmap bitmap = new DiskBackedMatchBitmap(path, positions, reserved::addAndGet, true)) {
            while (!bitmap.initialize()) {
                Thread.yield();
            }
            assertThat(reserved.get()).isEqualTo(Files.size(path));
            bitmap.mark(Integer.MAX_VALUE);
            bitmap.mark(positions - 1);
            assertThat(bitmap.isMarked(Integer.MAX_VALUE)).isTrue();
            assertThat(bitmap.isMarked(positions - 1)).isTrue();
            assertThat(bitmap.isMarked(Integer.MAX_VALUE - 1L)).isFalse();
        }
        assertThat(reserved.get()).isZero();
        assertThat(Files.exists(path)).isFalse();
    }

    @Test
    public void testEncryptedBlocksRewrittenWithNewIv(@TempDir Path directory)
            throws Exception
    {
        Path path = Files.createTempFile(directory, "encrypted-matches", ".bin");
        AtomicLong reserved = new AtomicLong();
        try (DiskBackedMatchBitmap bitmap = new DiskBackedMatchBitmap(path, 9000, reserved::addAndGet, true)) {
            while (!bitmap.initialize()) {
                Thread.yield();
            }
            assertThat(reserved.get()).isEqualTo(Files.size(path)).isGreaterThan(1125);
            for (int position = 0; position < 8192; position += 2) {
                bitmap.mark(position);
            }
            bitmap.mark(8192);
            byte[] first = Files.readAllBytes(path);
            byte[] plaintext = new byte[1024];
            Arrays.fill(plaintext, (byte) 0x55);
            assertThat(indexOf(first, plaintext)).isEqualTo(-1);
            bitmap.mark(1);
            bitmap.mark(8193);
            byte[] rewritten = Files.readAllBytes(path);
            assertThat(Arrays.copyOf(rewritten, 12)).isNotEqualTo(Arrays.copyOf(first, 12));
            for (int position = 0; position < 9000; position++) {
                assertThat(bitmap.isMarked(position)).as("position %s", position)
                        .isEqualTo(position == 1 || position < 8192 && position % 2 == 0 || position == 8192 || position == 8193);
            }
            assertThat(bitmap.isMarked(0)).isTrue();
        }
        assertThat(reserved.get()).isZero();
        assertThat(Files.exists(path)).isFalse();
    }

    @Test
    public void testEncryptedBlockCorruptionIsRejected(@TempDir Path directory)
            throws Exception
    {
        Path path = Files.createTempFile(directory, "encrypted-matches", ".bin");
        AtomicLong reserved = new AtomicLong();
        try (DiskBackedMatchBitmap bitmap = new DiskBackedMatchBitmap(path, 9000, reserved::addAndGet, true)) {
            while (!bitmap.initialize()) {
                Thread.yield();
            }
            bitmap.mark(0);
            bitmap.mark(8192);
            try (RandomAccessFile file = new RandomAccessFile(path.toFile(), "rw")) {
                file.seek(40);
                int value = file.readUnsignedByte();
                file.seek(40);
                file.write(value ^ 1);
            }
            assertThatThrownBy(() -> bitmap.isMarked(0))
                    .hasMessage("Failed to read merge join match file")
                    .hasRootCauseInstanceOf(AEADBadTagException.class);
        }
        assertThat(reserved.get()).isZero();
        assertThat(Files.exists(path)).isFalse();
    }

    @Test
    public void testEncryptedReservationFailureRemovesFile(@TempDir Path directory)
            throws Exception
    {
        Path path = Files.createTempFile(directory, "encrypted-matches", ".bin");
        assertThatThrownBy(() -> new DiskBackedMatchBitmap(path, 9000, _ -> {
            throw new IllegalStateException("spill limit reached");
        }, true)).isInstanceOf(IllegalStateException.class).hasMessage("spill limit reached");
        assertThat(Files.exists(path)).isFalse();
    }

    @Test
    public void testMarksAcrossBlocksAndReleasesSpillReservation(@TempDir Path directory)
            throws Exception
    {
        Path path = Files.createTempFile(directory, "matches", ".bin");
        AtomicLong reservedBytes = new AtomicLong();
        try (DiskBackedMatchBitmap bitmap = new DiskBackedMatchBitmap(path, 18000, reservedBytes::addAndGet)) {
            assertThat(reservedBytes.get()).isEqualTo(2250);
            bitmap.mark(0);
            bitmap.mark(8191);
            bitmap.mark(8192);
            bitmap.mark(17999);
            assertThat(bitmap.isMarked(0)).isTrue();
            assertThat(bitmap.isMarked(8191)).isTrue();
            assertThat(bitmap.isMarked(8192)).isTrue();
            assertThat(bitmap.isMarked(17999)).isTrue();
            assertThat(bitmap.isMarked(8190)).isFalse();
            assertThat(bitmap.isMarked(8193)).isFalse();
        }
        assertThat(reservedBytes.get()).isZero();
        assertThat(Files.exists(path)).isFalse();
    }

    @Test
    public void testReservationFailureRemovesFile(@TempDir Path directory)
            throws Exception
    {
        Path path = Files.createTempFile(directory, "matches", ".bin");
        assertThatThrownBy(() -> new DiskBackedMatchBitmap(path, 18000, _ -> {
            throw new IllegalStateException("spill limit reached");
        }))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("spill limit reached");
        assertThat(Files.exists(path)).isFalse();
    }

    @Test
    public void testPositionsBeyondIntegerLimit(@TempDir Path directory)
            throws Exception
    {
        Path path = Files.createTempFile(directory, "large-matches", ".bin");
        long positions = (long) Integer.MAX_VALUE + 128;
        long lastPosition = positions - 1;
        AtomicLong reservedBytes = new AtomicLong();
        try (DiskBackedMatchBitmap bitmap = new DiskBackedMatchBitmap(path, positions, reservedBytes::addAndGet)) {
            assertThat(reservedBytes.get()).isEqualTo((positions + 7) / 8);
            bitmap.mark(Integer.MAX_VALUE);
            bitmap.mark(lastPosition);
            assertThat(bitmap.isMarked(Integer.MAX_VALUE)).isTrue();
            assertThat(bitmap.isMarked(lastPosition)).isTrue();
            assertThat(bitmap.isMarked(Integer.MAX_VALUE - 1L)).isFalse();
        }
        assertThat(reservedBytes.get()).isZero();
        assertThat(Files.exists(path)).isFalse();
    }
}
