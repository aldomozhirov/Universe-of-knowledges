package com.database.uokdb.db;

import java.io.DataInput;
import java.io.IOException;
import java.nio.ByteBuffer;

public final class CompressLZF{

    private static final int HASH_SIZE = 1 << 14;
    private static final int MAX_LITERAL = 1 << 5;
    private static final int MAX_OFF = 1 << 13;
    private static final int MAX_REF = (1 << 8) + (1 << 3);
    private int[] cachedHashTable;
    private static int first(byte[] in, int inPos) {
        return (in[inPos] << 8) | (in[inPos + 1] & 255);
    }

    private static int next(int v, byte[] in, int inPos) {
        return (v << 8) | (in[inPos + 2] & 255);
    }

    private static int hash(int h) {
        return ((h * 2777) >> 9) & (HASH_SIZE - 1);
    }

    public int compress(byte[] in, int inLen, byte[] out, int outPos) {
        int inPos = 0;
        if (cachedHashTable == null) {
            cachedHashTable = new int[HASH_SIZE];
        }
        int[] hashTab = cachedHashTable;
        int literals = 0;
        outPos++;
        int future = first(in, 0);
        while (inPos < inLen - 4) {
            byte p2 = in[inPos + 2];
            future = (future << 8) + (p2 & 255);
            int off = hash(future);
            int ref = hashTab[off];
            hashTab[off] = inPos;
       
            if (ref < inPos
                        && ref > 0
                        && (off = inPos - ref - 1) < MAX_OFF
                        && in[ref + 2] == p2
                        && in[ref + 1] == (byte) (future >> 8)
                        && in[ref] == (byte) (future >> 16)) {
                int maxLen = inLen - inPos - 2;
                if (maxLen > MAX_REF) {
                    maxLen = MAX_REF;
                }
                if (literals == 0) {
                    outPos--;
                } else {
                    out[outPos - literals - 1] = (byte) (literals - 1);
                    literals = 0;
                }
                int len = 3;
                while (len < maxLen && in[ref + len] == in[inPos + len]) {
                    len++;
                }
                len -= 2;
                if (len < 7) {
                    out[outPos++] = (byte) ((off >> 8) + (len << 5));
                } else {
                    out[outPos++] = (byte) ((off >> 8) + (7 << 5));
                    out[outPos++] = (byte) (len - 7);
                }
                out[outPos++] = (byte) off;
                outPos++;
                inPos += len;
            
                future = first(in, inPos);
                future = next(future, in, inPos);
                hashTab[hash(future)] = inPos++;
                future = next(future, in, inPos);
                hashTab[hash(future)] = inPos++;
            } else {
                out[outPos++] = in[inPos++];
                literals++;
                if (literals == MAX_LITERAL) {
                    out[outPos - literals - 1] = (byte) (literals - 1);
                    literals = 0;
                    outPos++;
                }
            }
        }
        while (inPos < inLen) {
            out[outPos++] = in[inPos++];
            literals++;
            if (literals == MAX_LITERAL) {
                out[outPos - literals - 1] = (byte) (literals - 1);
                literals = 0;
                outPos++;
            }
        }
        out[outPos - literals - 1] = (byte) (literals - 1);
        if (literals == 0) {
            outPos--;
        }
        return outPos;
    }

    public void expand(DataInput in, byte[] out, int outPos, int outLen) throws IOException {
        // if ((inPos | outPos | outLen) < 0) {
        if(CC.ASSERT && ! (outLen>=0))
            throw new AssertionError();
        do {
            int ctrl = in.readByte() & 255;
            if (ctrl < MAX_LITERAL) {
                ctrl++;
                in.readFully(out,outPos,ctrl);
                outPos += ctrl;
            } else {
                int len = ctrl >> 5;
                if (len == 7) {
                    len += in.readByte() & 255;
                }
                len += 2;

                ctrl = -((ctrl & 0x1f) << 8) - 1;

                ctrl -= in.readByte() & 255;

                ctrl += outPos;
                if (outPos + len >= out.length) {
                    throw new ArrayIndexOutOfBoundsException();
                }
                for (int i = 0; i < len; i++) {
                    out[outPos++] = out[ctrl++];
                }
            }
        } while (outPos < outLen);
    }


    public void expand(ByteBuffer in, int inPos, byte[] out, int outPos, int outLen) {
        ByteBuffer in2=null;
        if(CC.ASSERT && ! (outLen>=0))
            throw new AssertionError();
        do {
            int ctrl = in.get(inPos++) & 255;
            if (ctrl < MAX_LITERAL) {
                ctrl++;
                if(in2==null) in2 = in.duplicate();
                in2.position(inPos);
                in2.get(out,outPos,ctrl);
                outPos += ctrl;
                inPos += ctrl;
            } else {
                int len = ctrl >> 5;
                if (len == 7) {
                    len += in.get(inPos++) & 255;
                }
                len += 2;

                ctrl = -((ctrl & 0x1f) << 8) - 1;

                ctrl -= in.get(inPos++) & 255;

                ctrl += outPos;
                if (outPos + len >= out.length) {
                    throw new ArrayIndexOutOfBoundsException();
                }
                for (int i = 0; i < len; i++) {
                    out[outPos++] = out[ctrl++];
                }
            }
        } while (outPos < outLen);
    }

    public void expand(byte[] in, int inPos, byte[] out, int outPos, int outLen) {
        if (inPos < 0 || outPos < 0 || outLen < 0) {
            throw new IllegalArgumentException();
        }
        do {
            int ctrl = in[inPos++] & 255;
            if (ctrl < MAX_LITERAL) {
                ctrl++;
                System.arraycopy(in, inPos, out, outPos, ctrl);
                outPos += ctrl;
                inPos += ctrl;
            } else {
                int len = ctrl >> 5;
                if (len == 7) {
                    len += in[inPos++] & 255;
                }
                len += 2;

                ctrl = -((ctrl & 0x1f) << 8) - 1;

                ctrl -= in[inPos++] & 255;

                ctrl += outPos;
                if (outPos + len >= out.length) {
                    throw new ArrayIndexOutOfBoundsException();
                }
                for (int i = 0; i < len; i++) {
                    out[outPos++] = out[ctrl++];
                }
            }
        } while (outPos < outLen);
    }



}