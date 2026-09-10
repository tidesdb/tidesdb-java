/**
 *
 * Copyright (C) TidesDB
 *
 * Original Author: Alex Gaetano Padula
 *
 * Licensed under the Mozilla Public License, v. 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.mozilla.org/en-US/MPL/2.0/
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.tidesdb;

/**
 * Where writers have been made to wait, one entry per {@link StallReason},
 * returned from {@link TidesDB#getStallStats()}.
 *
 * <p>A write latency tail is answerable from this alone: compare each reason's
 * {@link StallStat#getMaxUs()} against the tail you measured, and its
 * {@link StallStat#getTotalUs()} against the others.
 */
public class StallStats {

    private final StallStat[] reasons;

    /**
     * Creates a new {@code StallStats}. Typically called by the JNI bridge
     * rather than application code.
     *
     * @param reasons the per-reason totals, indexed by {@link StallReason#getValue()};
     *        must not be {@code null} and must hold one entry per reason
     */
    public StallStats(StallStat[] reasons) {
        if (reasons == null || reasons.length != StallReason.values().length) {
            throw new IllegalArgumentException(
                "reasons must hold exactly " + StallReason.values().length + " entries");
        }
        this.reasons = reasons.clone();
    }

    /**
     * Returns the totals for one wait reason.
     *
     * @param reason the reason; must not be {@code null}
     * @return the totals for that reason, never {@code null}
     */
    public StallStat get(StallReason reason) {
        if (reason == null) {
            throw new IllegalArgumentException("Stall reason cannot be null");
        }
        return reasons[reason.getValue()];
    }

    /**
     * Returns the totals for every wait reason, indexed by
     * {@link StallReason#getValue()}.
     *
     * @return a copy of the per-reason totals
     */
    public StallStat[] getReasons() {
        return reasons.clone();
    }

    /**
     * Returns the summed wait across every reason.
     *
     * @return the total wait in microseconds
     */
    public long getTotalUs() {
        long total = 0;
        for (StallStat stat : reasons) {
            total += stat.getTotalUs();
        }
        return total;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("StallStats{");
        for (StallReason reason : StallReason.values()) {
            if (reason.getValue() > 0) {
                sb.append(", ");
            }
            sb.append(reason.name()).append('=').append(reasons[reason.getValue()]);
        }
        return sb.append('}').toString();
    }
}
