package com.bizo.hive.serde.csv;

import java.io.CharArrayReader;
import java.io.IOException;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
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
  
  @Override
  public void initialize(final Configuration conf, final Properties tbl) throws SerDeException {
    final List<String> columnNames = Arrays.asList(tbl.getProperty(Constants.LIST_COLUMNS).split(","));
    
    final List<TypeInfo> columnTypes = TypeInfoUtils.getTypeInfosFromTypeString(tbl.getProperty(Constants.LIST_COLUMN_TYPES));
    
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
    final CSVWriter csv = new CSVWriter(writer, CSVWriter.DEFAULT_SEPARATOR, CSVWriter.DEFAULT_QUOTE_CHARACTER, "");
    
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
      csv = new CSVReader(new CharArrayReader(rowText.toString().toCharArray()));
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

  @Override
  public ObjectInspector getObjectInspector() throws SerDeException {
    return inspector;
  }

  @Override
  public Class<? extends Writable> getSerializedClass() {
    return Text.class;
  }
}
