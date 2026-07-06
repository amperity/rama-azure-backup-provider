package com.amperity.rama.backup;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * Wraps an InputStream so that at most {@code limit} bytes can be read from it, regardless of how
 * many bytes the underlying stream actually holds. Used to feed Azure's strict-length upload()
 * exactly the declared contentLength; see the callsite in
 * {@link AzureBlobBackupProvider#putObject}.
 *
 * <p>mark/reset are overridden to keep the remaining-byte counter consistent when the Azure SDK
 * rewinds the stream to retry a request. They are not synchronized: the stream is consumed on a
 * single path (a retry resets and re-reads only after the prior attempt has fully failed), and the
 * reads that also mutate {@code remaining} are unsynchronized too, so a lock here would not make
 * the counter thread-safe anyway.
 */
final class BoundedInputStream extends FilterInputStream {
    private long remaining;
    private long markedRemaining;

    BoundedInputStream(final InputStream in, final long limit) {
        super(in);
        this.remaining = limit;
    }

    @Override
    public int read() throws IOException {
        if (remaining <= 0) {
            return -1;
        }
        int b = super.read();
        if (b != -1) {
            remaining--;
        }
        return b;
    }

    @Override
    public int read(final byte[] buf, final int off, final int len) throws IOException {
        if (remaining <= 0) {
            return -1;
        }
        int n = super.read(buf, off, (int) Math.min(len, remaining));
        if (n > 0) {
            remaining -= n;
        }
        return n;
    }

    @Override
    public void mark(final int readlimit) {
        super.mark(readlimit);
        markedRemaining = remaining;
    }

    @Override
    public void reset() throws IOException {
        super.reset();
        remaining = markedRemaining;
    }
}
