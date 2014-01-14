package com.cloudera.sa;

import java.io.IOException;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;

public class AvroStreamMapper extends Mapper<AvroKey<GenericRecord>, NullWritable, AvroKey<String>, AvroValue<GenericRecord>>{
	 public void map(AvroKey<GenericRecord> key, NullWritable ignore,
				Context context) throws IOException, InterruptedException {
		 context.write(new AvroKey<String>(key.datum().get(0).toString()), new AvroValue<GenericRecord>(key.datum()));
	 }
}
