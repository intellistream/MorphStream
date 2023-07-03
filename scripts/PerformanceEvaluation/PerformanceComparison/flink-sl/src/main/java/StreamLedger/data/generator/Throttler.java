/*
 *  Copyright 2018 Data Artisans GmbH
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package StreamLedger.data.generator;

import static org.apache.flink.util.Preconditions.checkArgument;

final class Throttler {

    private final long throttleBatchSize;
    private final long nanosPerBatch;

    private long endOfNextBatchNanos;
    private int currentBatch;

    Throttler(long maxRecordsPerSecond, int numberOfParallelSubtasks) {
        checkArgument(maxRecordsPerSecond == -1 || maxRecordsPerSecond > 0,
                "maxRecordsPerSecond must be positive or -1 (infinite)");
        checkArgument(numberOfParallelSubtasks > 0, "numberOfParallelSubtasks must be greater than 0");

        if (maxRecordsPerSecond == -1) {
            // unlimited speed
            throttleBatchSize = -1;
            nanosPerBatch = 0;
            endOfNextBatchNanos = System.nanoTime() + nanosPerBatch;
            currentBatch = 0;
            return;
        }
        final float ratePerSubtask =
                (float) maxRecordsPerSecond / numberOfParallelSubtasks;

        if (ratePerSubtask >= 10000) {
            // high rates: all throttling in intervals of 2ms
            throttleBatchSize = (int) ratePerSubtask / 500;
            nanosPerBatch = 2_000_000L;
        }
        else {
            throttleBatchSize = ((int) (ratePerSubtask / 20)) + 1;
            nanosPerBatch = ((int) (1_000_000_000L / ratePerSubtask)) * throttleBatchSize;
        }
        this.endOfNextBatchNanos = System.nanoTime() + nanosPerBatch;
        this.currentBatch = 0;
    }

    void throttle() throws InterruptedException {
        if (throttleBatchSize == -1) {
            return;
        }
        if (++currentBatch != throttleBatchSize) {
            return;
        }
        currentBatch = 0;

        final long now = System.nanoTime();
        final int millisRemaining = (int) ((endOfNextBatchNanos - now) / 1_000_000);

        if (millisRemaining > 0) {
            endOfNextBatchNanos += nanosPerBatch;
            Thread.sleep(millisRemaining);
        }
        else {
            endOfNextBatchNanos = now + nanosPerBatch;
        }
    }

}
