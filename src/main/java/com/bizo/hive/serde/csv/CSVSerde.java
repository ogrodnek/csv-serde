package com.bizo.hive.serde.csv;

import java.io.CharArrayReader;
import java.io.IOException;
import java.io.Reader;
import java.io.StringWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import au.com.bytecode.opencsv.CSVReader;
import au.com.bytecode.opencsv.CSVWriter;


/**
 * CSVSerde uses opencsv (http://opencsv.sourceforge.net/) to serialize/deserialize columns as CSV.
 * 
 * @author Larry Ogrodnek <ogrodnek@gmail.com>
 */
public final class CSVSerde implements SerDe {
  
  private ObjectInspector inspector;
  private String[] outputFields;
  private int numCols;
  private List<String> row;
  
  private char separatorChar;
  private char quoteChar;
  private char escapeChar;
  
    
  @Override
  public void initialize(final Configuration conf, final Properties tbl) throws SerDeException {
    final List<String> columnNames = Arrays.asList(tbl.getProperty(serdeConstants.LIST_COLUMNS).split(","));
    final List<TypeInfo> columnTypes = TypeInfoUtils.getTypeInfosFromTypeString(tbl.getProperty(serdeConstants.LIST_COLUMN_TYPES));
    
    numCols = columnNames.size();
    
    final List<ObjectInspector> columnOIs = new ArrayList<ObjectInspector>(numCols);
    
    for (int i=0; i< numCols; i++) {
      columnOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
    }
    
    this.inspector = ObjectInspectorFactory.getStandardStructObjectInspector(columnNames, columnOIs);
    this.outputFields = new String[numCols];
    row = new ArrayList<String>(numCols);
    
    for (int i=0; i< numCols; i++) {
      row.add(null);
    }
    
    separatorChar = getProperty(tbl, "separatorChar", CSVWriter.DEFAULT_SEPARATOR);
    quoteChar = getProperty(tbl, "quoteChar", CSVWriter.DEFAULT_QUOTE_CHARACTER);
    escapeChar = getProperty(tbl, "escapeChar", CSVWriter.DEFAULT_ESCAPE_CHARACTER);
  }
  
  private final char getProperty(final Properties tbl, final String property, final char def) {
    final String val = tbl.getProperty(property);
    
    if (val != null) {
      return val.charAt(0);
    }
    
    return def;
  }
  
  @Override
  public Writable serialize(Object obj, ObjectInspector objInspector) throws SerDeException {
    final StructObjectInspector outputRowOI = (StructObjectInspector) objInspector;
    final List<? extends StructField> outputFieldRefs = outputRowOI.getAllStructFieldRefs();
    
    if (outputFieldRefs.size() != numCols) {
      throw new SerDeException("Cannot serialize the object because there are "
          + outputFieldRefs.size() + " fields but the table has " + numCols + " columns.");
    }

    // Get all data out.
    for (int c = 0; c < numCols; c++) {
      final Object field = outputRowOI.getStructFieldData(obj, outputFieldRefs.get(c));
      final ObjectInspector fieldOI = outputFieldRefs.get(c).getFieldObjectInspector();
      
      // The data must be of type String
      final StringObjectInspector fieldStringOI = (StringObjectInspector) fieldOI;
      
      // Convert the field to Java class String, because objects of String type
      // can be stored in String, Text, or some other classes.
      outputFields[c] = fieldStringOI.getPrimitiveJavaObject(field);
    }
    
    final StringWriter writer = new StringWriter();
    final CSVWriter csv = newWriter(writer, separatorChar, quoteChar, escapeChar);
    
    try {
      csv.writeNext(outputFields);
      csv.close();
      
      return new Text(writer.toString());
    } catch (final IOException ioe) {
      throw new SerDeException(ioe);
    }
  }  

  @Override
  public Object deserialize(final Writable blob) throws SerDeException {
    Text rowText = (Text) blob;
    
    CSVReader csv = null;
    try {
      csv = newReader(new CharArrayReader(rowText.toString().toCharArray()), separatorChar, quoteChar, escapeChar);      
      final String[] read = csv.readNext();
      
      for (int i=0; i< numCols; i++) {
        if (read != null && i < read.length) {
          row.set(i, read[i]);
        } else {
          row.set(i, null);
        }
      }
      
      return row;
    } catch (final Exception e) {
      throw new SerDeException(e);
    } finally {
      if (csv != null) {
        try {
          csv.close();
        } catch (final Exception e) {
          // ignore
        }
      }
    }
  }
  
  private CSVReader newReader(final Reader reader, char separator, char quote, char escape) {
    // CSVReader will throw an exception if any of separator, quote, or escape is the same, but 
    // the CSV format specifies that the escape character and quote char are the same... very weird
    if (CSVWriter.DEFAULT_ESCAPE_CHARACTER == escape) {
      return new CSVReader(reader, separator, quote);
    } else {
      return new CSVReader(reader, separator, quote, escape);      
    }
  }
  
  private CSVWriter newWriter(final Writer writer, char separator, char quote, char escape) {
    if (CSVWriter.DEFAULT_ESCAPE_CHARACTER == escape) {
      return new CSVWriter(writer, separator, quote, "");
    } else {
      return new CSVWriter(writer, separator, quote, escape, "");      
    }
  }

  @Override
  public ObjectInspector getObjectInspector() throws SerDeException {
    return inspector;
  }

  @Override
  public Class<? extends Writable> getSerializedClass() {
    return Text.class;
  }
  
  public SerDeStats getSerDeStats() {
    return null;
  }
}
