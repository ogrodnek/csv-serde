package com.bizo.hive.serde.csv;

import java.util.List;
import java.util.Properties;

import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.io.Text;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.assertEquals;


public final class CSVSerdeTest {
  private final CSVSerde csv = new CSVSerde();

  final Properties new_props = new Properties();
  final Properties props = new Properties();

  @Before
  public void setup() throws Exception {
    new_props.put(Constants.LIST_COLUMNS, "a,b,c,d,e,f,g,h,i,j");
    new_props.put(Constants.LIST_COLUMN_TYPES, "string,string,int,bigint,float,double,timestamp,timestamp,void,int");
    props.put(Constants.LIST_COLUMNS, "a,b,c");
    props.put(Constants.LIST_COLUMN_TYPES, "string,string,int");
  }

  @Test
  public void testDeserialize() throws Exception {
    csv.initialize(null, new_props);
    final Text in = new Text("hello,\"yes, okay\",1,\"1409576461919\",100.05,\"1024.19002010\",\"2014-04-14 00:03:49\",\"2014-04-14 00:09:42.228000\",,");

    final List<Object> row = (List<Object>) csv.deserialize(in);

    assertEquals("hello", row.get(0));
    assertEquals("yes, okay", row.get(1));
    assertEquals(1, row.get(2));
    assertEquals(1409576461919L, row.get(3));
    assertEquals(Float.valueOf("100.05"), row.get(4));
    assertEquals((Double) 1024.19002010, row.get(5));
    assertEquals(java.sql.Timestamp.valueOf("2014-04-14 00:03:49"), row.get(6));
    assertEquals(java.sql.Timestamp.valueOf("2014-04-14 00:09:42.228"), row.get(7));
    assertEquals(null, row.get(8));
    assertEquals(0, row.get(9));
  }

  @Test
  public void testDeserializeCustomSeparators() throws Exception {
    props.put("separatorChar", "\t");
    props.put("quoteChar", "'");

    csv.initialize(null, props);

    final Text in = new Text("hello\t'yes\tokay'\t1");
    final List<Object> row = (List<Object>) csv.deserialize(in);

    assertEquals("hello", row.get(0));
    assertEquals("yes\tokay", row.get(1));
    assertEquals(1, row.get(2));
  }

  @Test
  public void testDeserializeCustomEscape() throws Exception {
    props.put("quoteChar", "'");
    props.put("escapeChar", "\\");

    csv.initialize(null, props);

    final Text in = new Text("hello,'yes\\'okay',1");
    final List<Object> row = (List<Object>) csv.deserialize(in);

    assertEquals("hello", row.get(0));
    assertEquals("yes'okay", row.get(1));
    assertEquals(1, row.get(2));
  }
}
