package com.bizo.hive.serde.csv;

import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde.serdeConstants;
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

import com.opencsv.CSVParser;
import com.opencsv.CSVWriter;


/**
 * CSVSerde uses <a href="http://opencsv.sourceforge.net">opencsv</a>
 * to serialize/deserialize columns as CSV.
 * 
 * @author Larry Ogrodnek <ogrodnek@gmail.com>
 */
public final class CSVSerde extends AbstractSerDe {

  private ObjectInspector inspector;
  private String[] outputFields;
  private int numCols;
  private ArrayList<String> row;

  private char separatorChar;
  private char quoteChar;
  private char escapeChar;
  private CSVParser csvParser;

  @Override
  public void
      initialize(final Configuration conf, final Properties tbl) throws SerDeException {
    final List<String> columnNames =
        Arrays.asList(tbl.getProperty(serdeConstants.LIST_COLUMNS).split(","));
    final List<TypeInfo> columnTypes = TypeInfoUtils
        .getTypeInfosFromTypeString(tbl.getProperty(serdeConstants.LIST_COLUMN_TYPES));

    numCols = columnNames.size();

    final List<ObjectInspector> columnOIs = new ArrayList<ObjectInspector>(numCols);

    for (int i=0; i< numCols; i++) {
      columnOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
    }

    this.inspector =
        ObjectInspectorFactory.getStandardStructObjectInspector(columnNames, columnOIs);
    this.outputFields = new String[numCols];
    row = new ArrayList<String>();

    separatorChar = getProperty(tbl, "separatorChar", CSVWriter.DEFAULT_SEPARATOR);
    quoteChar = getProperty(tbl, "quoteChar", CSVWriter.DEFAULT_QUOTE_CHARACTER);
    escapeChar = getProperty(tbl, "escapeChar", CSVWriter.DEFAULT_ESCAPE_CHARACTER);

    if (CSVWriter.DEFAULT_ESCAPE_CHARACTER == escapeChar) {
      this.csvParser = new CSVParser(separatorChar, quoteChar);
    } else {
      this.csvParser = new CSVParser(separatorChar, quoteChar, escapeChar);
    }
  }

  private final char
      getProperty(final Properties tbl, final String property, final char def) {
    final String val = tbl.getProperty(property);

    if (val != null) {
      return val.charAt(0);
    }

    return def;
  }

  @Override
  public Writable serialize(Object obj, ObjectInspector objInspector)
      throws SerDeException {
    final StructObjectInspector outputRowOI = (StructObjectInspector) objInspector;
    final List<? extends StructField> outputFieldRefs =
        outputRowOI.getAllStructFieldRefs();

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

    try {
      final String[] strings = this.csvParser.parseLine(rowText.toString());

      for (String thisRow : strings) {
        if (strings != null) {
          row.add(thisRow.replace("\r\n", "<CRLF>")
              .replace("\r", "<CR>").replace("\n","<LF>"));
        } else {
          row.add(null);
        }
      }

      return row;
    } catch (final IOException e) {
      throw new SerDeException(e);
    }
  }

  private CSVWriter
      newWriter(final Writer writer, char separator, char quote, char escape) {
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