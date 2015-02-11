package com.bizo.hive.serde.csv;

import java.util.List;
import java.util.Properties;

import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.io.Text;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.assertEquals;


public final class CSVSerdeTest {
  private final CSVSerde csv = new CSVSerde();
  final Properties props = new Properties();

  @Before
  public void setup() throws Exception {
    props.put(serdeConstants.LIST_COLUMNS, "a,b,c,d");
    props.put(serdeConstants.LIST_COLUMN_TYPES, "string,string,string,string");
  }

  @Test
  public void testDeserialize() throws Exception {
    csv.initialize(null, props);

    final Text in = new Text("hello,\"yes, okay\",1,\"new\nline\"");
    final List<String> row = (List<String>) csv.deserialize(in);

    assertEquals("hello", row.get(0));
    assertEquals("yes, okay", row.get(1));
    assertEquals("1", row.get(2));
    assertEquals("new<LF>line", row.get(3));
  }

  @Test
  public void testDeserializeCustomSeparator() throws Exception {
    props.put("separatorChar", "\t");
    props.put("quoteChar", "'");

    csv.initialize(null, props);

    final Text in = new Text("hello\t'yes\tokay'\t1\t'new\n\nline'");
    final List<String> row = (List<String>) csv.deserialize(in);

    assertEquals("hello", row.get(0));
    assertEquals("yes\tokay", row.get(1));
    assertEquals("1", row.get(2));
    assertEquals("new<LF><LF>line", row.get(3));
  }

  @Test
  public void testDeserializeCustomEscape() throws Exception {
    props.put("quoteChar", "'");
    props.put("escapeChar", "\\");

    csv.initialize(null, props);

    final Text in = new Text("hello,'yes\\'okay',1,'new\r\nline'");
    final List<String> row = (List<String>) csv.deserialize(in);

    assertEquals("hello", row.get(0));
    assertEquals("yes'okay", row.get(1));
    assertEquals("1", row.get(2));
    assertEquals("new<CRLF>line", row.get(3));
  }

  @Test
  public void testDeserializeCustomSeparatorCustomEscape() throws Exception {
    props.put("seperatorChar", ",");
    props.put("quoteChar", "\"");
    props.put("escapeChar", "\"");

    csv.initialize(null, props);

    final Text in = new Text("hello,\"yes, okay\",1,\"new\r\n\r\n\"\"line\"\"\"");
    final List<String> row = (List<String>) csv.deserialize(in);

    assertEquals("hello", row.get(0));
    assertEquals("yes, okay", row.get(1));
    assertEquals("1", row.get(2));
    assertEquals("new<CRLF><CRLF>\"line\"", row.get(3));
  }
}