package com.bizo.hive.serde.csv;

import java.util.List;
import java.util.Properties;

import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.assertEquals;


public final class CSVSerdeTest {
  private final CSVSerde csv = new CSVSerde();
  final Properties props = new Properties();

  @Before
  public void setup() throws Exception {
    props.put(Constants.LIST_COLUMNS, "a,b,c");
    props.put(Constants.LIST_COLUMN_TYPES, "string,string,string");
  }

  @Test
  public void testDeserialize() throws Exception {
    csv.initialize(null, props);
    final Text in = new Text("hello,\"yes, okay\",1");

    final List<String> row = (List<String>) csv.deserialize(in);

    assertEquals("hello", row.get(0));
    assertEquals("yes, okay", row.get(1));
    assertEquals("1", row.get(2));
  }


  @Test
  public void testDeserializeCustomSeparators() throws Exception {
    props.put("separatorChar", "\t");
    props.put("quoteChar", "'");

    csv.initialize(null, props);

    final Text in = new Text("hello\t'yes\tokay'\t1");
    final List<String> row = (List<String>) csv.deserialize(in);

    assertEquals("hello", row.get(0));
    assertEquals("yes\tokay", row.get(1));
    assertEquals("1", row.get(2));
  }

  @Test
  public void testDeserializeCustomEscape() throws Exception {
    props.put("quoteChar", "'");
    props.put("escapeChar", "\\");

    csv.initialize(null, props);

    final Text in = new Text("hello,'yes\\'okay',1");
    final List<String> row = (List<String>) csv.deserialize(in);

    assertEquals("hello", row.get(0));
    assertEquals("yes'okay", row.get(1));
    assertEquals("1", row.get(2));
  }

  @Test
  public void testSerialize() throws Exception {
      props.put("quoteChar", "\"");
      props.put("escapeChar", "\\");
      props.put("encoding", "US-ASCII");
      props.put("normalize", "true");
      props.put("stripQuotes", "true");

      csv.initialize(null, props);

      final Text in = new Text("hello, test \"yes\" okay \000,1");
      final List<String> row = (List<String>) csv.deserialize(in);
      BytesWritable w = (BytesWritable) csv.serialize(row, csv.getObjectInspector());
      String t = new String(w.getBytes());
      assertEquals("\"hello\",\" test yes okay \",\"1\"", t);
  }
}
