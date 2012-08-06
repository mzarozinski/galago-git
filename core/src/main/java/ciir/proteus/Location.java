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

public class Location implements org.apache.thrift.TBase<Location, Location._Fields>, java.io.Serializable, Cloneable {
  private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct("Location");

  private static final org.apache.thrift.protocol.TField FULL_NAME_FIELD_DESC = new org.apache.thrift.protocol.TField("full_name", org.apache.thrift.protocol.TType.STRING, (short)1);
  private static final org.apache.thrift.protocol.TField ALTERNATE_NAMES_FIELD_DESC = new org.apache.thrift.protocol.TField("alternate_names", org.apache.thrift.protocol.TType.LIST, (short)2);
  private static final org.apache.thrift.protocol.TField WIKI_LINK_FIELD_DESC = new org.apache.thrift.protocol.TField("wiki_link", org.apache.thrift.protocol.TType.STRING, (short)3);
  private static final org.apache.thrift.protocol.TField LONGITUDE_FIELD_DESC = new org.apache.thrift.protocol.TField("longitude", org.apache.thrift.protocol.TType.DOUBLE, (short)4);
  private static final org.apache.thrift.protocol.TField LATITUDE_FIELD_DESC = new org.apache.thrift.protocol.TField("latitude", org.apache.thrift.protocol.TType.DOUBLE, (short)5);

  private static final Map<Class<? extends IScheme>, SchemeFactory> schemes = new HashMap<Class<? extends IScheme>, SchemeFactory>();
  static {
    schemes.put(StandardScheme.class, new LocationStandardSchemeFactory());
    schemes.put(TupleScheme.class, new LocationTupleSchemeFactory());
  }

  public String full_name; // optional
  public List<String> alternate_names; // required
  public String wiki_link; // optional
  public double longitude; // optional
  public double latitude; // optional

  /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    FULL_NAME((short)1, "full_name"),
    ALTERNATE_NAMES((short)2, "alternate_names"),
    WIKI_LINK((short)3, "wiki_link"),
    LONGITUDE((short)4, "longitude"),
    LATITUDE((short)5, "latitude");

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
        case 1: // FULL_NAME
          return FULL_NAME;
        case 2: // ALTERNATE_NAMES
          return ALTERNATE_NAMES;
        case 3: // WIKI_LINK
          return WIKI_LINK;
        case 4: // LONGITUDE
          return LONGITUDE;
        case 5: // LATITUDE
          return LATITUDE;
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
  private static final int __LONGITUDE_ISSET_ID = 0;
  private static final int __LATITUDE_ISSET_ID = 1;
  private BitSet __isset_bit_vector = new BitSet(2);
  private _Fields optionals[] = {_Fields.FULL_NAME,_Fields.WIKI_LINK,_Fields.LONGITUDE,_Fields.LATITUDE};
  public static final Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> metaDataMap;
  static {
    Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> tmpMap = new EnumMap<_Fields, org.apache.thrift.meta_data.FieldMetaData>(_Fields.class);
    tmpMap.put(_Fields.FULL_NAME, new org.apache.thrift.meta_data.FieldMetaData("full_name", org.apache.thrift.TFieldRequirementType.OPTIONAL, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING)));
    tmpMap.put(_Fields.ALTERNATE_NAMES, new org.apache.thrift.meta_data.FieldMetaData("alternate_names", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.ListMetaData(org.apache.thrift.protocol.TType.LIST, 
            new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING))));
    tmpMap.put(_Fields.WIKI_LINK, new org.apache.thrift.meta_data.FieldMetaData("wiki_link", org.apache.thrift.TFieldRequirementType.OPTIONAL, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING)));
    tmpMap.put(_Fields.LONGITUDE, new org.apache.thrift.meta_data.FieldMetaData("longitude", org.apache.thrift.TFieldRequirementType.OPTIONAL, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.DOUBLE)));
    tmpMap.put(_Fields.LATITUDE, new org.apache.thrift.meta_data.FieldMetaData("latitude", org.apache.thrift.TFieldRequirementType.OPTIONAL, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.DOUBLE)));
    metaDataMap = Collections.unmodifiableMap(tmpMap);
    org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(Location.class, metaDataMap);
  }

  public Location() {
  }

  public Location(
    List<String> alternate_names)
  {
    this();
    this.alternate_names = alternate_names;
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public Location(Location other) {
    __isset_bit_vector.clear();
    __isset_bit_vector.or(other.__isset_bit_vector);
    if (other.isSetFull_name()) {
      this.full_name = other.full_name;
    }
    if (other.isSetAlternate_names()) {
      List<String> __this__alternate_names = new ArrayList<String>();
      for (String other_element : other.alternate_names) {
        __this__alternate_names.add(other_element);
      }
      this.alternate_names = __this__alternate_names;
    }
    if (other.isSetWiki_link()) {
      this.wiki_link = other.wiki_link;
    }
    this.longitude = other.longitude;
    this.latitude = other.latitude;
  }

  public Location deepCopy() {
    return new Location(this);
  }

  @Override
  public void clear() {
    this.full_name = null;
    this.alternate_names = null;
    this.wiki_link = null;
    setLongitudeIsSet(false);
    this.longitude = 0.0;
    setLatitudeIsSet(false);
    this.latitude = 0.0;
  }

  public String getFull_name() {
    return this.full_name;
  }

  public Location setFull_name(String full_name) {
    this.full_name = full_name;
    return this;
  }

  public void unsetFull_name() {
    this.full_name = null;
  }

  /** Returns true if field full_name is set (has been assigned a value) and false otherwise */
  public boolean isSetFull_name() {
    return this.full_name != null;
  }

  public void setFull_nameIsSet(boolean value) {
    if (!value) {
      this.full_name = null;
    }
  }

  public int getAlternate_namesSize() {
    return (this.alternate_names == null) ? 0 : this.alternate_names.size();
  }

  public java.util.Iterator<String> getAlternate_namesIterator() {
    return (this.alternate_names == null) ? null : this.alternate_names.iterator();
  }

  public void addToAlternate_names(String elem) {
    if (this.alternate_names == null) {
      this.alternate_names = new ArrayList<String>();
    }
    this.alternate_names.add(elem);
  }

  public List<String> getAlternate_names() {
    return this.alternate_names;
  }

  public Location setAlternate_names(List<String> alternate_names) {
    this.alternate_names = alternate_names;
    return this;
  }

  public void unsetAlternate_names() {
    this.alternate_names = null;
  }

  /** Returns true if field alternate_names is set (has been assigned a value) and false otherwise */
  public boolean isSetAlternate_names() {
    return this.alternate_names != null;
  }

  public void setAlternate_namesIsSet(boolean value) {
    if (!value) {
      this.alternate_names = null;
    }
  }

  public String getWiki_link() {
    return this.wiki_link;
  }

  public Location setWiki_link(String wiki_link) {
    this.wiki_link = wiki_link;
    return this;
  }

  public void unsetWiki_link() {
    this.wiki_link = null;
  }

  /** Returns true if field wiki_link is set (has been assigned a value) and false otherwise */
  public boolean isSetWiki_link() {
    return this.wiki_link != null;
  }

  public void setWiki_linkIsSet(boolean value) {
    if (!value) {
      this.wiki_link = null;
    }
  }

  public double getLongitude() {
    return this.longitude;
  }

  public Location setLongitude(double longitude) {
    this.longitude = longitude;
    setLongitudeIsSet(true);
    return this;
  }

  public void unsetLongitude() {
    __isset_bit_vector.clear(__LONGITUDE_ISSET_ID);
  }

  /** Returns true if field longitude is set (has been assigned a value) and false otherwise */
  public boolean isSetLongitude() {
    return __isset_bit_vector.get(__LONGITUDE_ISSET_ID);
  }

  public void setLongitudeIsSet(boolean value) {
    __isset_bit_vector.set(__LONGITUDE_ISSET_ID, value);
  }

  public double getLatitude() {
    return this.latitude;
  }

  public Location setLatitude(double latitude) {
    this.latitude = latitude;
    setLatitudeIsSet(true);
    return this;
  }

  public void unsetLatitude() {
    __isset_bit_vector.clear(__LATITUDE_ISSET_ID);
  }

  /** Returns true if field latitude is set (has been assigned a value) and false otherwise */
  public boolean isSetLatitude() {
    return __isset_bit_vector.get(__LATITUDE_ISSET_ID);
  }

  public void setLatitudeIsSet(boolean value) {
    __isset_bit_vector.set(__LATITUDE_ISSET_ID, value);
  }

  public void setFieldValue(_Fields field, Object value) {
    switch (field) {
    case FULL_NAME:
      if (value == null) {
        unsetFull_name();
      } else {
        setFull_name((String)value);
      }
      break;

    case ALTERNATE_NAMES:
      if (value == null) {
        unsetAlternate_names();
      } else {
        setAlternate_names((List<String>)value);
      }
      break;

    case WIKI_LINK:
      if (value == null) {
        unsetWiki_link();
      } else {
        setWiki_link((String)value);
      }
      break;

    case LONGITUDE:
      if (value == null) {
        unsetLongitude();
      } else {
        setLongitude((Double)value);
      }
      break;

    case LATITUDE:
      if (value == null) {
        unsetLatitude();
      } else {
        setLatitude((Double)value);
      }
      break;

    }
  }

  public Object getFieldValue(_Fields field) {
    switch (field) {
    case FULL_NAME:
      return getFull_name();

    case ALTERNATE_NAMES:
      return getAlternate_names();

    case WIKI_LINK:
      return getWiki_link();

    case LONGITUDE:
      return Double.valueOf(getLongitude());

    case LATITUDE:
      return Double.valueOf(getLatitude());

    }
    throw new IllegalStateException();
  }

  /** Returns true if field corresponding to fieldID is set (has been assigned a value) and false otherwise */
  public boolean isSet(_Fields field) {
    if (field == null) {
      throw new IllegalArgumentException();
    }

    switch (field) {
    case FULL_NAME:
      return isSetFull_name();
    case ALTERNATE_NAMES:
      return isSetAlternate_names();
    case WIKI_LINK:
      return isSetWiki_link();
    case LONGITUDE:
      return isSetLongitude();
    case LATITUDE:
      return isSetLatitude();
    }
    throw new IllegalStateException();
  }

  @Override
  public boolean equals(Object that) {
    if (that == null)
      return false;
    if (that instanceof Location)
      return this.equals((Location)that);
    return false;
  }

  public boolean equals(Location that) {
    if (that == null)
      return false;

    boolean this_present_full_name = true && this.isSetFull_name();
    boolean that_present_full_name = true && that.isSetFull_name();
    if (this_present_full_name || that_present_full_name) {
      if (!(this_present_full_name && that_present_full_name))
        return false;
      if (!this.full_name.equals(that.full_name))
        return false;
    }

    boolean this_present_alternate_names = true && this.isSetAlternate_names();
    boolean that_present_alternate_names = true && that.isSetAlternate_names();
    if (this_present_alternate_names || that_present_alternate_names) {
      if (!(this_present_alternate_names && that_present_alternate_names))
        return false;
      if (!this.alternate_names.equals(that.alternate_names))
        return false;
    }

    boolean this_present_wiki_link = true && this.isSetWiki_link();
    boolean that_present_wiki_link = true && that.isSetWiki_link();
    if (this_present_wiki_link || that_present_wiki_link) {
      if (!(this_present_wiki_link && that_present_wiki_link))
        return false;
      if (!this.wiki_link.equals(that.wiki_link))
        return false;
    }

    boolean this_present_longitude = true && this.isSetLongitude();
    boolean that_present_longitude = true && that.isSetLongitude();
    if (this_present_longitude || that_present_longitude) {
      if (!(this_present_longitude && that_present_longitude))
        return false;
      if (this.longitude != that.longitude)
        return false;
    }

    boolean this_present_latitude = true && this.isSetLatitude();
    boolean that_present_latitude = true && that.isSetLatitude();
    if (this_present_latitude || that_present_latitude) {
      if (!(this_present_latitude && that_present_latitude))
        return false;
      if (this.latitude != that.latitude)
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    return 0;
  }

  public int compareTo(Location other) {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }

    int lastComparison = 0;
    Location typedOther = (Location)other;

    lastComparison = Boolean.valueOf(isSetFull_name()).compareTo(typedOther.isSetFull_name());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetFull_name()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.full_name, typedOther.full_name);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetAlternate_names()).compareTo(typedOther.isSetAlternate_names());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetAlternate_names()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.alternate_names, typedOther.alternate_names);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetWiki_link()).compareTo(typedOther.isSetWiki_link());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetWiki_link()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.wiki_link, typedOther.wiki_link);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetLongitude()).compareTo(typedOther.isSetLongitude());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetLongitude()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.longitude, typedOther.longitude);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetLatitude()).compareTo(typedOther.isSetLatitude());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetLatitude()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.latitude, typedOther.latitude);
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
    StringBuilder sb = new StringBuilder("Location(");
    boolean first = true;

    if (isSetFull_name()) {
      sb.append("full_name:");
      if (this.full_name == null) {
        sb.append("null");
      } else {
        sb.append(this.full_name);
      }
      first = false;
    }
    if (!first) sb.append(", ");
    sb.append("alternate_names:");
    if (this.alternate_names == null) {
      sb.append("null");
    } else {
      sb.append(this.alternate_names);
    }
    first = false;
    if (isSetWiki_link()) {
      if (!first) sb.append(", ");
      sb.append("wiki_link:");
      if (this.wiki_link == null) {
        sb.append("null");
      } else {
        sb.append(this.wiki_link);
      }
      first = false;
    }
    if (isSetLongitude()) {
      if (!first) sb.append(", ");
      sb.append("longitude:");
      sb.append(this.longitude);
      first = false;
    }
    if (isSetLatitude()) {
      if (!first) sb.append(", ");
      sb.append("latitude:");
      sb.append(this.latitude);
      first = false;
    }
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
      // it doesn't seem like you should have to do this, but java serialization is wacky, and doesn't call the default constructor.
      __isset_bit_vector = new BitSet(1);
      read(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(in)));
    } catch (org.apache.thrift.TException te) {
      throw new java.io.IOException(te);
    }
  }

  private static class LocationStandardSchemeFactory implements SchemeFactory {
    public LocationStandardScheme getScheme() {
      return new LocationStandardScheme();
    }
  }

  private static class LocationStandardScheme extends StandardScheme<Location> {

    public void read(org.apache.thrift.protocol.TProtocol iprot, Location struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TField schemeField;
      iprot.readStructBegin();
      while (true)
      {
        schemeField = iprot.readFieldBegin();
        if (schemeField.type == org.apache.thrift.protocol.TType.STOP) { 
          break;
        }
        switch (schemeField.id) {
          case 1: // FULL_NAME
            if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
              struct.full_name = iprot.readString();
              struct.setFull_nameIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 2: // ALTERNATE_NAMES
            if (schemeField.type == org.apache.thrift.protocol.TType.LIST) {
              {
                org.apache.thrift.protocol.TList _list130 = iprot.readListBegin();
                struct.alternate_names = new ArrayList<String>(_list130.size);
                for (int _i131 = 0; _i131 < _list130.size; ++_i131)
                {
                  String _elem132; // optional
                  _elem132 = iprot.readString();
                  struct.alternate_names.add(_elem132);
                }
                iprot.readListEnd();
              }
              struct.setAlternate_namesIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 3: // WIKI_LINK
            if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
              struct.wiki_link = iprot.readString();
              struct.setWiki_linkIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 4: // LONGITUDE
            if (schemeField.type == org.apache.thrift.protocol.TType.DOUBLE) {
              struct.longitude = iprot.readDouble();
              struct.setLongitudeIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 5: // LATITUDE
            if (schemeField.type == org.apache.thrift.protocol.TType.DOUBLE) {
              struct.latitude = iprot.readDouble();
              struct.setLatitudeIsSet(true);
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

    public void write(org.apache.thrift.protocol.TProtocol oprot, Location struct) throws org.apache.thrift.TException {
      struct.validate();

      oprot.writeStructBegin(STRUCT_DESC);
      if (struct.full_name != null) {
        if (struct.isSetFull_name()) {
          oprot.writeFieldBegin(FULL_NAME_FIELD_DESC);
          oprot.writeString(struct.full_name);
          oprot.writeFieldEnd();
        }
      }
      if (struct.alternate_names != null) {
        oprot.writeFieldBegin(ALTERNATE_NAMES_FIELD_DESC);
        {
          oprot.writeListBegin(new org.apache.thrift.protocol.TList(org.apache.thrift.protocol.TType.STRING, struct.alternate_names.size()));
          for (String _iter133 : struct.alternate_names)
          {
            oprot.writeString(_iter133);
          }
          oprot.writeListEnd();
        }
        oprot.writeFieldEnd();
      }
      if (struct.wiki_link != null) {
        if (struct.isSetWiki_link()) {
          oprot.writeFieldBegin(WIKI_LINK_FIELD_DESC);
          oprot.writeString(struct.wiki_link);
          oprot.writeFieldEnd();
        }
      }
      if (struct.isSetLongitude()) {
        oprot.writeFieldBegin(LONGITUDE_FIELD_DESC);
        oprot.writeDouble(struct.longitude);
        oprot.writeFieldEnd();
      }
      if (struct.isSetLatitude()) {
        oprot.writeFieldBegin(LATITUDE_FIELD_DESC);
        oprot.writeDouble(struct.latitude);
        oprot.writeFieldEnd();
      }
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

  }

  private static class LocationTupleSchemeFactory implements SchemeFactory {
    public LocationTupleScheme getScheme() {
      return new LocationTupleScheme();
    }
  }

  private static class LocationTupleScheme extends TupleScheme<Location> {

    @Override
    public void write(org.apache.thrift.protocol.TProtocol prot, Location struct) throws org.apache.thrift.TException {
      TTupleProtocol oprot = (TTupleProtocol) prot;
      BitSet optionals = new BitSet();
      if (struct.isSetFull_name()) {
        optionals.set(0);
      }
      if (struct.isSetAlternate_names()) {
        optionals.set(1);
      }
      if (struct.isSetWiki_link()) {
        optionals.set(2);
      }
      if (struct.isSetLongitude()) {
        optionals.set(3);
      }
      if (struct.isSetLatitude()) {
        optionals.set(4);
      }
      oprot.writeBitSet(optionals, 5);
      if (struct.isSetFull_name()) {
        oprot.writeString(struct.full_name);
      }
      if (struct.isSetAlternate_names()) {
        {
          oprot.writeI32(struct.alternate_names.size());
          for (String _iter134 : struct.alternate_names)
          {
            oprot.writeString(_iter134);
          }
        }
      }
      if (struct.isSetWiki_link()) {
        oprot.writeString(struct.wiki_link);
      }
      if (struct.isSetLongitude()) {
        oprot.writeDouble(struct.longitude);
      }
      if (struct.isSetLatitude()) {
        oprot.writeDouble(struct.latitude);
      }
    }

    @Override
    public void read(org.apache.thrift.protocol.TProtocol prot, Location struct) throws org.apache.thrift.TException {
      TTupleProtocol iprot = (TTupleProtocol) prot;
      BitSet incoming = iprot.readBitSet(5);
      if (incoming.get(0)) {
        struct.full_name = iprot.readString();
        struct.setFull_nameIsSet(true);
      }
      if (incoming.get(1)) {
        {
          org.apache.thrift.protocol.TList _list135 = new org.apache.thrift.protocol.TList(org.apache.thrift.protocol.TType.STRING, iprot.readI32());
          struct.alternate_names = new ArrayList<String>(_list135.size);
          for (int _i136 = 0; _i136 < _list135.size; ++_i136)
          {
            String _elem137; // optional
            _elem137 = iprot.readString();
            struct.alternate_names.add(_elem137);
          }
        }
        struct.setAlternate_namesIsSet(true);
      }
      if (incoming.get(2)) {
        struct.wiki_link = iprot.readString();
        struct.setWiki_linkIsSet(true);
      }
      if (incoming.get(3)) {
        struct.longitude = iprot.readDouble();
        struct.setLongitudeIsSet(true);
      }
      if (incoming.get(4)) {
        struct.latitude = iprot.readDouble();
        struct.setLatitudeIsSet(true);
      }
    }
  }

}

