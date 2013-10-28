/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.lemurproject.galago.utility;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import junit.framework.TestCase;
import org.lemurproject.galago.utility.compression.VByteInput;
import org.lemurproject.galago.utility.compression.VByteOutput;

/**
 *
 * @author trevor
 */
public class VByteInputTest extends TestCase {

    public VByteInputTest(String testName) {
        super(testName);
    }

    public void testReadString() throws IOException {
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        VByteOutput output = new VByteOutput(new DataOutputStream(stream));
        output.writeString("\u2297");
        stream.close();

        ByteArrayInputStream inputStream = new ByteArrayInputStream(stream.toByteArray());
        VByteInput input = new VByteInput(new DataInputStream(inputStream));
        String result = input.readString();

        assertEquals("\u2297", result);
    }

    public void testReadZero() throws Exception {
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        VByteOutput output = new VByteOutput(new DataOutputStream(stream));
        output.writeInt(0);
        stream.close();

        ByteArrayInputStream inputStream = new ByteArrayInputStream(stream.toByteArray());
        VByteInput input = new VByteInput(new DataInputStream(inputStream));
        int zero = input.readInt();

        assertEquals(0, zero);
    }
}
