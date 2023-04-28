package com.gnmathur.wingspan.refapplications;

import java.util.Arrays;

public class EchoFrame {
    // The class only has static methods. We don't want to create these objects
    private EchoFrame() {};

    public static byte[] frameThis(final byte[] message) {
        int msgLenWithStringTerm = message.length;
        byte[] out = new byte[4 + msgLenWithStringTerm];
        out[0] = (byte)((msgLenWithStringTerm & 0xFF000000) >> 24);
        out[1] = (byte)((msgLenWithStringTerm & 0x00FF0000) >> 16);
        out[2] = (byte)((msgLenWithStringTerm & 0x0000FF00) >> 8);
        out[3] = (byte)(msgLenWithStringTerm & 0x0000FF);

        for (int i = 0; i < message.length; i++) {
            out[4 + i] = message[i];
        }
        return out;
    }

    //TODO This is not frame length; See TCP convention

    /**
     * This routine converts length encoded in 4 bytes to an unsigned integer value
     * @param message
     * @return
     */
    public static int getFrameLength(final byte[] message) {
        int length = (int)(
                        (message[0] & 0XFF) << 24 |
                        (message[1] & 0xFF) << 16 |
                        (message[2] & 0xFF) << 8 |
                        (message[3] & 0xFF));
        return length;
    }

    public static void main(String[] args) {
        byte[] framedMessage = frameThis("Hello There! This might be a slightly long string".getBytes());
        System.out.println(getFrameLength(framedMessage));
    }

    public static String getStringFromFrame(byte[] msg) {
        return new String(Arrays.copyOfRange(msg, 4, msg.length));
    }
}
