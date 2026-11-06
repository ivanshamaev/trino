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

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestDiskBackedMatchBitmap
{
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
