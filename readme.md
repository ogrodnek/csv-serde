# Hive CSV Support

        add jar path/to/csv-serde.jar;

        create table my_table(a string, b string, ...)
          row format serde 'com.bizo.hive.serde.csv.CSVSerde'
          stored as textfile
        ;

see: ...