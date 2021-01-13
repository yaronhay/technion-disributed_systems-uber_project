package utils;


import com.google.protobuf.ByteString;
import uber.proto.objects.ID;

import java.nio.ByteBuffer;

// Class is based on code from StackOverflow.
// By Brice Roncace answered the question
// "convert uuid to byte, that works when using UUID.nameUUIDFromBytes(b)"
// on Apr 23 '15 at 23:42
// Source: https://stackoverflow.com/questions/17893609/convert-uuid-to-byte-that-works-when-using-uuid-nameuuidfrombytesb
public class UUID {
    public static int LENGTH = 16;

    public static java.util.UUID generate() {
        return java.util.UUID.randomUUID();
    }

    public static byte[] toBytes(java.util.UUID uuid) {
        ByteBuffer buf = ByteBuffer.wrap(new byte[LENGTH]);
        buf.putLong(uuid.getMostSignificantBits());
        buf.putLong(uuid.getLeastSignificantBits());
        return buf.array();
    }

    public static ByteString toByteString(java.util.UUID uuid) {
        return ByteString.copyFrom(toBytes(uuid));
    }
    public static ID toID(java.util.UUID uuid) {
        return ID.newBuilder().setVal(toByteString(uuid)).build();
    }

    public static java.util.UUID fromBytes(byte[] bytes) {
        assert bytes.length == LENGTH;
        ByteBuffer buf = ByteBuffer.wrap(bytes);
        var mostSigBits = buf.getLong();
        var leastSigBits = buf.getLong();
        return new java.util.UUID(mostSigBits, leastSigBits);
    }

    public static java.util.UUID fromID(ID id) {
        return fromBytes(id.getVal().toByteArray());
    }
}
