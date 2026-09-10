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
 * What each class of file asked of the device, one entry per {@link IoClass},
 * returned from {@link TidesDB#getIoStats()}.
 *
 * <p>This is the other half of {@link TidesDB#getStallStats()}: that says
 * writers waited on the log, this says whether the device was the reason.
 *
 * <p>Only handles the engine opens through its descriptor manager are counted,
 * which is every key log and every write-ahead log. The value log and the
 * manifest are not, so this measures the two classes that compete for the
 * device under load rather than every byte the database writes.
 */
public class IoStats {

    private final IoStat[] classes;

    /**
     * Creates a new {@code IoStats}. Typically called by the JNI bridge rather
     * than application code.
     *
     * @param classes the per-class totals, indexed by {@link IoClass#getValue()};
     *        must not be {@code null} and must hold one entry per class
     */
    public IoStats(IoStat[] classes) {
        if (classes == null || classes.length != IoClass.values().length) {
            throw new IllegalArgumentException(
                "classes must hold exactly " + IoClass.values().length + " entries");
        }
        this.classes = classes.clone();
    }

    /**
     * Returns the totals for one I/O class.
     *
     * @param cls the class; must not be {@code null}
     * @return the totals for that class, never {@code null}
     */
    public IoStat get(IoClass cls) {
        if (cls == null) {
            throw new IllegalArgumentException("I/O class cannot be null");
        }
        return classes[cls.getValue()];
    }

    /**
     * Returns the totals for every I/O class, indexed by
     * {@link IoClass#getValue()}.
     *
     * @return a copy of the per-class totals
     */
    public IoStat[] getClasses() {
        return classes.clone();
    }

    /**
     * Returns the bytes written across every class.
     *
     * @return the total byte count
     */
    public long getTotalBytes() {
        long total = 0;
        for (IoStat stat : classes) {
            total += stat.getBytes();
        }
        return total;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("IoStats{");
        for (IoClass cls : IoClass.values()) {
            if (cls.getValue() > 0) {
                sb.append(", ");
            }
            sb.append(cls.name()).append('=').append(classes[cls.getValue()]);
        }
        return sb.append('}').toString();
    }
}
