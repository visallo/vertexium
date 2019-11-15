package org.vertexium.elasticsearch7.lucene;

import java.io.IOException;
import java.io.Reader;

public final class FastCharStream implements CharStream {
    char[] buffer = null;

    int bufferLength = 0; // end of valid chars
    int bufferPosition = 0; // next char to read

    int tokenStart = 0; // offset in buffer
    int bufferStart = 0; // position in file of buffer

    Reader input; // source of chars

    /**
     * Constructs from a Reader.
     */
    public FastCharStream(Reader r) {
        input = r;
    }

    @Override
    public final char readChar() throws IOException {
        if (bufferPosition >= bufferLength) {
            refill();
        }
        return buffer[bufferPosition++];
    }

    private final void refill() throws IOException {
        int newPosition = bufferLength - tokenStart;

        if (tokenStart == 0) { // token won't fit in buffer
            if (buffer == null) { // first time: alloc buffer
                buffer = new char[2048];
            } else if (bufferLength == buffer.length) { // grow buffer
                char[] newBuffer = new char[buffer.length * 2];
                System.arraycopy(buffer, 0, newBuffer, 0, bufferLength);
                buffer = newBuffer;
            }
        } else { // shift token to front
            System.arraycopy(buffer, tokenStart, buffer, 0, newPosition);
        }

        bufferLength = newPosition; // update state
        bufferPosition = newPosition;
        bufferStart += tokenStart;
        tokenStart = 0;

        int charsRead = // fill space in buffer
            input.read(buffer, newPosition, buffer.length - newPosition);
        if (charsRead == -1) {
            throw new IOException("read past eof");
        } else {
            bufferLength += charsRead;
        }
    }

    @Override
    public final char BeginToken() throws IOException {
        tokenStart = bufferPosition;
        return readChar();
    }

    @Override
    public final void backup(int amount) {
        bufferPosition -= amount;
    }

    @Override
    public final String GetImage() {
        return new String(buffer, tokenStart, bufferPosition - tokenStart);
    }

    @Override
    public final char[] GetSuffix(int len) {
        char[] value = new char[len];
        System.arraycopy(buffer, bufferPosition - len, value, 0, len);
        return value;
    }

    @Override
    public final void Done() {
        try {
            input.close();
        } catch (IOException e) {
        }
    }

    @Override
    @Deprecated
    public final int getColumn() {
        return bufferStart + bufferPosition;
    }

    @Override
    @Deprecated
    public final int getLine() {
        return 1;
    }

    @Override
    public final int getEndColumn() {
        return bufferStart + bufferPosition;
    }

    @Override
    public final int getEndLine() {
        return 1;
    }

    @Override
    public final int getBeginColumn() {
        return bufferStart + tokenStart;
    }

    @Override
    public final int getBeginLine() {
        return 1;
    }
}
