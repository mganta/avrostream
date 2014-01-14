package com.cloudera.sa;

import java.io.IOException;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class AvroStreamReducer extends Reducer<AvroKey<String>, AvroValue<GenericRecord>, AvroKey<GenericRecord>, NullWritable>  {
	public void reduce(AvroKey<String> key, Iterable<AvroValue<GenericRecord>> values, Context context)
				throws IOException, InterruptedException {
		for (AvroValue<GenericRecord> event : values) {
			 context.write(new AvroKey<GenericRecord>(event.datum()), NullWritable.get());
		}
	}
}
