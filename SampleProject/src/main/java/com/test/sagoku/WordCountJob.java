package com.test.sagoku;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;

public class WordCountJob extends Configured implements Tool{
    
public static void main(String[] args) {
    List<Option> jobOptions  = new ArrayList<Option>();
    jobOptions.add(OptionBuilder.isRequired().withArgName("inputpath").withDescription("files for exec").create("inputpath"));
    jobOptions.add(OptionBuilder.isRequired().withArgName("outputpath").withDescription("files for exec").create("outputpath"));
    

}

private Job createJob(Configuration config){
    Job job = null;
    try {
	job = new Job(config);
	
    } catch (IOException e) {
	e.printStackTrace();
    }
    
    return job;
}


public int run(String[] arg0) throws Exception {
    Configuration conf = getConf();
    return 0;
}
}
