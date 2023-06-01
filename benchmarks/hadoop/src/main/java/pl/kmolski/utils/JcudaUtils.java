package pl.kmolski.utils;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Objects;

public class JcudaUtils {

    private JcudaUtils() {}

    public static byte[] toNullTerminatedByteArray(InputStream inStream) throws IOException {
        Objects.requireNonNull(inStream);

        ByteArrayOutputStream outStream = new ByteArrayOutputStream();
        inStream.transferTo(outStream);
        outStream.write(0);
        return outStream.toByteArray();
    }
}
