/**
 * Autogenerated by Thrift Compiler (0.8.0)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *  @generated
 */
package ciir.proteus;

import org.apache.thrift.scheme.IScheme;
import org.apache.thrift.scheme.SchemeFactory;
import org.apache.thrift.scheme.StandardScheme;

import org.apache.thrift.scheme.TupleScheme;
import org.apache.thrift.protocol.TTupleProtocol;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.EnumMap;
import java.util.Set;
import java.util.HashSet;
import java.util.EnumSet;
import java.util.Collections;
import java.util.BitSet;
import java.nio.ByteBuffer;
import java.util.Arrays;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PictureList implements org.apache.thrift.TBase<PictureList, PictureList._Fields>, java.io.Serializable, Cloneable {
  private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct("PictureList");

  private static final org.apache.thrift.protocol.TField PICTURES_FIELD_DESC = new org.apache.thrift.protocol.TField("pictures", org.apache.thrift.protocol.TType.LIST, (short)1);

  private static final Map<Class<? extends IScheme>, SchemeFactory> schemes = new HashMap<Class<? extends IScheme>, SchemeFactory>();
  static {
    schemes.put(StandardScheme.class, new PictureListStandardSchemeFactory());
    schemes.put(TupleScheme.class, new PictureListTupleSchemeFactory());
  }

  public List<Picture> pictures; // required

  /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    PICTURES((short)1, "pictures");

    private static final Map<String, _Fields> byName = new HashMap<String, _Fields>();

    static {
      for (_Fields field : EnumSet.allOf(_Fields.class)) {
        byName.put(field.getFieldName(), field);
      }
    }

    /**
     * Find the _Fields constant that matches fieldId, or null if its not found.
     */
    public static _Fields findByThriftId(int fieldId) {
      switch(fieldId) {
        case 1: // PICTURES
          return PICTURES;
        default:
          return null;
      }
    }

    /**
     * Find the _Fields constant that matches fieldId, throwing an exception
     * if it is not found.
     */
    public static _Fields findByThriftIdOrThrow(int fieldId) {
      _Fields fields = findByThriftId(fieldId);
      if (fields == null) throw new IllegalArgumentException("Field " + fieldId + " doesn't exist!");
      return fields;
    }

    /**
     * Find the _Fields constant that matches name, or null if its not found.
     */
    public static _Fields findByName(String name) {
      return byName.get(name);
    }

    private final short _thriftId;
    private final String _fieldName;

    _Fields(short thriftId, String fieldName) {
      _thriftId = thriftId;
      _fieldName = fieldName;
    }

    public short getThriftFieldId() {
      return _thriftId;
    }

    public String getFieldName() {
      return _fieldName;
    }
  }

  // isset id assignments
  public static final Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> metaDataMap;
  static {
    Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> tmpMap = new EnumMap<_Fields, org.apache.thrift.meta_data.FieldMetaData>(_Fields.class);
    tmpMap.put(_Fields.PICTURES, new org.apache.thrift.meta_data.FieldMetaData("pictures", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.ListMetaData(org.apache.thrift.protocol.TType.LIST, 
            new org.apache.thrift.meta_data.StructMetaData(org.apache.thrift.protocol.TType.STRUCT, Picture.class))));
    metaDataMap = Collections.unmodifiableMap(tmpMap);
    org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(PictureList.class, metaDataMap);
  }

  public PictureList() {
  }

  public PictureList(
    List<Picture> pictures)
  {
    this();
    this.pictures = pictures;
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public PictureList(PictureList other) {
    if (other.isSetPictures()) {
      List<Picture> __this__pictures = new ArrayList<Picture>();
      for (Picture other_element : other.pictures) {
        __this__pictures.add(new Picture(other_element));
      }
      this.pictures = __this__pictures;
    }
  }

  public PictureList deepCopy() {
    return new PictureList(this);
  }

  @Override
  public void clear() {
    this.pictures = null;
  }

  public int getPicturesSize() {
    return (this.pictures == null) ? 0 : this.pictures.size();
  }

  public java.util.Iterator<Picture> getPicturesIterator() {
    return (this.pictures == null) ? null : this.pictures.iterator();
  }

  public void addToPictures(Picture elem) {
    if (this.pictures == null) {
      this.pictures = new ArrayList<Picture>();
    }
    this.pictures.add(elem);
  }

  public List<Picture> getPictures() {
    return this.pictures;
  }

  public PictureList setPictures(List<Picture> pictures) {
    this.pictures = pictures;
    return this;
  }

  public void unsetPictures() {
    this.pictures = null;
  }

  /** Returns true if field pictures is set (has been assigned a value) and false otherwise */
  public boolean isSetPictures() {
    return this.pictures != null;
  }

  public void setPicturesIsSet(boolean value) {
    if (!value) {
      this.pictures = null;
    }
  }

  public void setFieldValue(_Fields field, Object value) {
    switch (field) {
    case PICTURES:
      if (value == null) {
        unsetPictures();
      } else {
        setPictures((List<Picture>)value);
      }
      break;

    }
  }

  public Object getFieldValue(_Fields field) {
    switch (field) {
    case PICTURES:
      return getPictures();

    }
    throw new IllegalStateException();
  }

  /** Returns true if field corresponding to fieldID is set (has been assigned a value) and false otherwise */
  public boolean isSet(_Fields field) {
    if (field == null) {
      throw new IllegalArgumentException();
    }

    switch (field) {
    case PICTURES:
      return isSetPictures();
    }
    throw new IllegalStateException();
  }

  @Override
  public boolean equals(Object that) {
    if (that == null)
      return false;
    if (that instanceof PictureList)
      return this.equals((PictureList)that);
    return false;
  }

  public boolean equals(PictureList that) {
    if (that == null)
      return false;

    boolean this_present_pictures = true && this.isSetPictures();
    boolean that_present_pictures = true && that.isSetPictures();
    if (this_present_pictures || that_present_pictures) {
      if (!(this_present_pictures && that_present_pictures))
        return false;
      if (!this.pictures.equals(that.pictures))
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    return 0;
  }

  public int compareTo(PictureList other) {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }

    int lastComparison = 0;
    PictureList typedOther = (PictureList)other;

    lastComparison = Boolean.valueOf(isSetPictures()).compareTo(typedOther.isSetPictures());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetPictures()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.pictures, typedOther.pictures);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    return 0;
  }

  public _Fields fieldForId(int fieldId) {
    return _Fields.findByThriftId(fieldId);
  }

  public void read(org.apache.thrift.protocol.TProtocol iprot) throws org.apache.thrift.TException {
    schemes.get(iprot.getScheme()).getScheme().read(iprot, this);
  }

  public void write(org.apache.thrift.protocol.TProtocol oprot) throws org.apache.thrift.TException {
    schemes.get(oprot.getScheme()).getScheme().write(oprot, this);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("PictureList(");
    boolean first = true;

    sb.append("pictures:");
    if (this.pictures == null) {
      sb.append("null");
    } else {
      sb.append(this.pictures);
    }
    first = false;
    sb.append(")");
    return sb.toString();
  }

  public void validate() throws org.apache.thrift.TException {
    // check for required fields
  }

  private void writeObject(java.io.ObjectOutputStream out) throws java.io.IOException {
    try {
      write(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(out)));
    } catch (org.apache.thrift.TException te) {
      throw new java.io.IOException(te);
    }
  }

  private void readObject(java.io.ObjectInputStream in) throws java.io.IOException, ClassNotFoundException {
    try {
      read(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(in)));
    } catch (org.apache.thrift.TException te) {
      throw new java.io.IOException(te);
    }
  }

  private static class PictureListStandardSchemeFactory implements SchemeFactory {
    public PictureListStandardScheme getScheme() {
      return new PictureListStandardScheme();
    }
  }

  private static class PictureListStandardScheme extends StandardScheme<PictureList> {

    public void read(org.apache.thrift.protocol.TProtocol iprot, PictureList struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TField schemeField;
      iprot.readStructBegin();
      while (true)
      {
        schemeField = iprot.readFieldBegin();
        if (schemeField.type == org.apache.thrift.protocol.TType.STOP) { 
          break;
        }
        switch (schemeField.id) {
          case 1: // PICTURES
            if (schemeField.type == org.apache.thrift.protocol.TType.LIST) {
              {
                org.apache.thrift.protocol.TList _list106 = iprot.readListBegin();
                struct.pictures = new ArrayList<Picture>(_list106.size);
                for (int _i107 = 0; _i107 < _list106.size; ++_i107)
                {
                  Picture _elem108; // optional
                  _elem108 = new Picture();
                  _elem108.read(iprot);
                  struct.pictures.add(_elem108);
                }
                iprot.readListEnd();
              }
              struct.setPicturesIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          default:
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
        }
        iprot.readFieldEnd();
      }
      iprot.readStructEnd();

      // check for required fields of primitive type, which can't be checked in the validate method
      struct.validate();
    }

    public void write(org.apache.thrift.protocol.TProtocol oprot, PictureList struct) throws org.apache.thrift.TException {
      struct.validate();

      oprot.writeStructBegin(STRUCT_DESC);
      if (struct.pictures != null) {
        oprot.writeFieldBegin(PICTURES_FIELD_DESC);
        {
          oprot.writeListBegin(new org.apache.thrift.protocol.TList(org.apache.thrift.protocol.TType.STRUCT, struct.pictures.size()));
          for (Picture _iter109 : struct.pictures)
          {
            _iter109.write(oprot);
          }
          oprot.writeListEnd();
        }
        oprot.writeFieldEnd();
      }
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

  }

  private static class PictureListTupleSchemeFactory implements SchemeFactory {
    public PictureListTupleScheme getScheme() {
      return new PictureListTupleScheme();
    }
  }

  private static class PictureListTupleScheme extends TupleScheme<PictureList> {

    @Override
    public void write(org.apache.thrift.protocol.TProtocol prot, PictureList struct) throws org.apache.thrift.TException {
      TTupleProtocol oprot = (TTupleProtocol) prot;
      BitSet optionals = new BitSet();
      if (struct.isSetPictures()) {
        optionals.set(0);
      }
      oprot.writeBitSet(optionals, 1);
      if (struct.isSetPictures()) {
        {
          oprot.writeI32(struct.pictures.size());
          for (Picture _iter110 : struct.pictures)
          {
            _iter110.write(oprot);
          }
        }
      }
    }

    @Override
    public void read(org.apache.thrift.protocol.TProtocol prot, PictureList struct) throws org.apache.thrift.TException {
      TTupleProtocol iprot = (TTupleProtocol) prot;
      BitSet incoming = iprot.readBitSet(1);
      if (incoming.get(0)) {
        {
          org.apache.thrift.protocol.TList _list111 = new org.apache.thrift.protocol.TList(org.apache.thrift.protocol.TType.STRUCT, iprot.readI32());
          struct.pictures = new ArrayList<Picture>(_list111.size);
          for (int _i112 = 0; _i112 < _list111.size; ++_i112)
          {
            Picture _elem113; // optional
            _elem113 = new Picture();
            _elem113.read(iprot);
            struct.pictures.add(_elem113);
          }
        }
        struct.setPicturesIsSet(true);
      }
    }
  }

}

