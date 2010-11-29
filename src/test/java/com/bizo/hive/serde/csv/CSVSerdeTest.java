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
  
  @Before
  public void setup() throws Exception {
    final Properties props = new Properties();
    props.put(Constants.LIST_COLUMNS, "a,b,c");
    props.put(Constants.LIST_COLUMN_TYPES, "string,string,string");
    
    csv.initialize(null, props);
  }
  
  @Test
  public void testDeserialize() throws Exception {
    final Text in = new Text("hello,\"yes, okay\",1");
    
    final List<String> row = (List<String>) csv.deserialize(in);
    
    assertEquals("hello", row.get(0));
    assertEquals("yes, okay", row.get(1));
    assertEquals("1", row.get(2));
  }
}
