package com.test.sagoku;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.SortedMap;

import org.apache.cassandra.db.IColumn;
import org.apache.cassandra.hadoop.ColumnFamilyInputFormat;
import org.apache.cassandra.hadoop.ConfigHelper;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.SliceRange;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class CassandraBulkReader extends Configured implements Tool {

    public static class Map extends Mapper<ByteBuffer,SortedMap<ByteBuffer,IColumn>,NullWritable,Text>{
	@Override
	protected void map(ByteBuffer key,SortedMap<ByteBuffer, IColumn> value,Context context)
		throws IOException, InterruptedException {
	    String mykey  = string(key); 
	    System.out.println("key"+ mykey);
	    context.write(NullWritable.get(), new Text(mykey));
	}
    }
    
    private static ByteBuffer bytes(String value){
	return ByteBuffer.wrap(value.getBytes());
    }
    
    private static String string(ByteBuffer bytes){
	return bytes.toString();
    }
    public int run(String[] args) throws Exception {
	    Configuration conf = getConf();
	    Job job = new Job(conf);
	    
	    job.setJarByClass(CassandraBulkReader.class);
	    ConfigHelper.setInputRpcPort(job.getConfiguration(), "9160");
	    ConfigHelper.setInputInitialAddress(job.getConfiguration(), "ec2-75-101-186-47.compute-1.amazonaws.com");
	    ConfigHelper.setInputPartitioner(job.getConfiguration(), "org.apache.cassandra.dht.RandomPartitioner");
	    ConfigHelper.setInputColumnFamily(job.getConfiguration(), "sequoia1", "metadata");

	    SlicePredicate predicate = new SlicePredicate();
	    
	    predicate.setSlice_range(new SliceRange(bytes(null), bytes(null), false, Integer.MAX_VALUE));
	    ConfigHelper.setInputSlicePredicate(job.getConfiguration(), predicate);

	    job.setMapperClass(Map.class);
	    job.setInputFormatClass(ColumnFamilyInputFormat.class);
	    job.setOutputFormatClass(TextOutputFormat.class);
	    String outputPath = args[0];
	    System.out.println("Output Path:"+outputPath);
	    FileOutputFormat.setOutputPath(job, new Path(outputPath) );
	    
	    return job.waitForCompletion(true)?0:1;
	}
    
    public static void main(String[] args) {
	CassandraBulkReader reader = new CassandraBulkReader();
	args = new String[1];
	int randomNum = 0 + (int)(Math.random()*20); 
	args[0] = "/tmp/output_"+randomNum;
	
	try {
	    int res = ToolRunner.run(reader, args);
	    System.out.println("res:"+res);
	} catch (Exception e) {
	    e.printStackTrace();
	}
    }

}
