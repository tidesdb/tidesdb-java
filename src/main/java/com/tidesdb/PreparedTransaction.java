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
 * One transaction that was durably prepared before a restart and has no decision
 * recorded after it, returned from {@link TidesDB#recoverPrepared()}.
 *
 * <p>Resolve it with {@link Transaction#commitPrepared()} or
 * {@link Transaction#rollbackPrepared()}, then free the transaction like any
 * other. A transaction that was decided in the log is settled during open and
 * never appears here.
 */
public class PreparedTransaction {

    private final Transaction transaction;
    private final byte[] xid;

    /**
     * Creates a new {@code PreparedTransaction}. Typically called by the JNI
     * bridge rather than application code.
     *
     * @param transaction a handle in the {@link TransactionState#PREPARED} state
     * @param xid the transaction id the coordinator prepared it under
     */
    public PreparedTransaction(Transaction transaction, byte[] xid) {
        this.transaction = transaction;
        this.xid = xid == null ? new byte[0] : xid.clone();
    }

    /**
     * Returns the in-doubt transaction, in the {@link TransactionState#PREPARED}
     * state. The caller owns it and must free it once resolved.
     *
     * @return the transaction handle
     */
    public Transaction getTransaction() {
        return transaction;
    }

    /**
     * Returns the transaction id the coordinator prepared this transaction
     * under.
     *
     * @return a copy of the xid bytes
     */
    public byte[] getXid() {
        return xid.clone();
    }

    @Override
    public String toString() {
        return "PreparedTransaction{xidSize=" + xid.length + '}';
    }
}
