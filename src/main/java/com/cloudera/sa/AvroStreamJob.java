package com.cloudera.sa;

import org.apache.avro.Schema;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class AvroStreamJob extends Configured implements Tool 
{
	        private Configuration config = new Configuration();

	        /**
	         * Tool interface
	         */
	        public Configuration getConf()
	        {
	                return config;
	        }

	        /**
	         * Tool interface
	         */
	        public void setConf(Configuration config)
	        {
	                this.config = config;
	        }

	        public int run(String[] args) throws Exception
	        {
	    		if (args.length != 3) {
	    			System.err.printf("Usage: %s [generic options] <input> <output> <schemafile> \n",
	    					getClass().getSimpleName());
	    			ToolRunner.printGenericCommandUsage(System.err);
	    			return -1;
	    		}
	        	    
	                Job job = Job.getInstance(config);
	                
	                job.setJarByClass(AvroStreamJob.class);
	                job.setJobName("AvroStreamJob");

	        		FileInputFormat.setInputPaths(job, new Path(args[0]));
	        		FileOutputFormat.setOutputPath(job, new Path(args[1]));
	        		
	        		FileSystem fs = FileSystem.get(config);
	                Schema schema = new Schema.Parser().parse(fs.open(new Path(args[2])));
	        		
	        		job.setInputFormatClass(AvroKeyInputFormat.class);
	        		job.setOutputFormatClass(AvroKeyOutputFormat.class);
	        		
	        		AvroJob.setInputKeySchema(job, schema);
	        		AvroJob.setOutputKeySchema(job, schema);
	        		AvroJob.setMapOutputKeySchema(job, Schema.create(Schema.Type.STRING));
	        		AvroJob.setMapOutputValueSchema(job, schema);
	        		
	                job.setMapperClass(AvroStreamMapper.class);
	                job.setReducerClass(AvroStreamReducer.class);

	                AvroKeyOutputFormat.setOutputCompressorClass(job, SnappyCodec.class);
	                AvroKeyOutputFormat.setCompressOutput(job, true);

	                return (job.waitForCompletion(true) ? 0 : 1);
	        }
	        /**
	         * @param args
	         */
	        public static void main(String[] args) throws Exception
	        {
                    AvroStreamJob processor = new AvroStreamJob();
	                String[] otherArgs = new GenericOptionsParser(processor.getConf(), args).getRemainingArgs();
	                System.exit(ToolRunner.run(processor.getConf(), processor, otherArgs));
	        }

}

