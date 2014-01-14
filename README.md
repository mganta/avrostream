-------
README 
-------
This example shows how to read and write avro files in a map reduce program. 

It can be used as an identity mapper and identity reducer on avro files.

It can also be used to merge smaller avro files into larger ones by specifying the number of reducers.

It can also be used to sort the avro records using the fields enabled for sorting in the schema (fields with out "order": "ignore").


It assumes the first field in the avro record as the key for record partitioning. You can change it by changing the field sent to the write method in the mapper



Here are the steps

1. Prepare HDFS folder

		hadoop fs -mkdir /user/tom/input

		hadoop fs -mkdir /user/tom/config
	
		hadoop fs -put schema.avsc /user/tom/config
	
		hadoop fs -put table2.avro /user/tom/input

2. Run the job

		hadoop jar avrostream-0.0.1-SNAPSHOT-job.jar -Dmapred.reduce.tasks=2 /user/tom/input /user/tom/output /user/tom/config/schema.avsc

		hadoop fs -ls /user/tom/output
	
3. If curious, check the data via hive

    	a. create hive table
    
	   	hive -f create_table.sql
	   
    	b. at hive prompt:
    
       		describe mytable;  
       
    	c. Run a select
    
       		select email, name,zipcode, others['salary'] from mytable;
