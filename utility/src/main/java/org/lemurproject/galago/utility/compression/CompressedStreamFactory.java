/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.utility.compression;

import org.lemurproject.galago.utility.compression.integer.GenericLemireCompressedWriter;
import org.lemurproject.galago.utility.compression.integer.GenericLemireCompressedReader;
import org.lemurproject.galago.utility.compression.integer.CompressedLongWriter;
import org.lemurproject.galago.utility.compression.integer.VByteWriter;
import org.lemurproject.galago.utility.compression.integer.SignedVByteWriter;
import org.lemurproject.galago.utility.compression.integer.CompressedLongReader;
import org.lemurproject.galago.utility.compression.integer.SignedVByteReader;
import org.lemurproject.galago.utility.compression.integer.VByteReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import me.lemire.integercompression.BinaryPacking;
import me.lemire.integercompression.Composition;
import me.lemire.integercompression.IntegratedBinaryPacking;
import me.lemire.integercompression.IntegratedComposition;
import me.lemire.integercompression.IntegratedVariableByte;
import me.lemire.integercompression.JustCopy;
import me.lemire.integercompression.NewPFD;
import me.lemire.integercompression.NewPFDS9;
import me.lemire.integercompression.OptPFD;
import me.lemire.integercompression.OptPFDS9;
import me.lemire.integercompression.Simple9;
import me.lemire.integercompression.VariableByte;

/**
 *
 * @author sjh
 */
public class CompressedStreamFactory {

  public static CompressedLongWriter compressedLongStreamWriterInstance(String name, OutputStream stream) throws IOException {
    if (name.equals("vbyte")) {
      return new VByteWriter(stream);
    } else if (name.equals("signed-vbyte")) {
      return new SignedVByteWriter(stream);

    } else if (name.equals("null")) {
      return new GenericLemireCompressedWriter(stream, new JustCopy());
    } else if (name.equals("vbyte2")) {
      return new GenericLemireCompressedWriter(stream, new VariableByte());
    } else if (name.equals("bp-vbyte")) {
      return new GenericLemireCompressedWriter(stream, new Composition(new BinaryPacking(), new VariableByte()));
    } else if (name.equals("ibp-ivbyte")) {
      return new GenericLemireCompressedWriter(stream, new IntegratedComposition(new IntegratedBinaryPacking(), new IntegratedVariableByte()));

    } else if (name.equals("npfd-vbyte")) {
      return new GenericLemireCompressedWriter(stream, new Composition(new NewPFD(), new VariableByte()));
    } else if (name.equals("npfds9-vbyte")) {
      return new GenericLemireCompressedWriter(stream, new Composition(new NewPFDS9(), new VariableByte()));

    } else if (name.equals("optpfd-vbyte")) {
      return new GenericLemireCompressedWriter(stream, new Composition(new OptPFD(), new VariableByte()));
    } else if (name.equals("optpfds9-vbyte")) {
      return new GenericLemireCompressedWriter(stream, new Composition(new OptPFDS9(), new VariableByte()));

    } else if (name.equals("fpfd-vbyte")) {
      return new GenericLemireCompressedWriter(stream, new Composition(new NewPFD(), new VariableByte()));

    } else if (name.equals("s9")) {
      return new GenericLemireCompressedWriter(stream, new Simple9());

    } else {
      System.err.println("Can not find compressor: " + name);
      return null;
    }
  }

  public static CompressedLongReader compressedLongStreamReaderInstance(String name, InputStream stream) throws IOException {
    if (name.equals("vbyte")) {
      return new VByteReader(stream);
    } else if (name.equals("signed-vbyte")) {
      return new SignedVByteReader(stream);

    } else if (name.equals("null")) {
      return new GenericLemireCompressedReader(stream, new JustCopy());
    } else if (name.equals("vbyte2")) {
      return new GenericLemireCompressedReader(stream, new VariableByte());
    } else if (name.equals("bp-vbyte")) {
      return new GenericLemireCompressedReader(stream, new Composition(new BinaryPacking(), new VariableByte()));
    } else if (name.equals("ibp-ivbyte")) {
      return new GenericLemireCompressedReader(stream, new IntegratedComposition(new IntegratedBinaryPacking(), new IntegratedVariableByte()));

    } else if (name.equals("npfd-vbyte")) {
      return new GenericLemireCompressedReader(stream, new Composition(new NewPFD(), new VariableByte()));
    } else if (name.equals("npfds9-vbyte")) {
      return new GenericLemireCompressedReader(stream, new Composition(new NewPFDS9(), new VariableByte()));

    } else if (name.equals("optpfd-vbyte")) {
      return new GenericLemireCompressedReader(stream, new Composition(new OptPFD(), new VariableByte()));
    } else if (name.equals("optpfds9-vbyte")) {
      return new GenericLemireCompressedReader(stream, new Composition(new OptPFDS9(), new VariableByte()));

    } else if (name.equals("fpfd-vbyte")) {
      return new GenericLemireCompressedReader(stream, new Composition(new NewPFD(), new VariableByte()));

    } else if (name.equals("s9")) {
      return new GenericLemireCompressedReader(stream, new Simple9());

    } else {
      System.err.println("Can not find decompressor: " + name);
      return null;
    }
  }
}
