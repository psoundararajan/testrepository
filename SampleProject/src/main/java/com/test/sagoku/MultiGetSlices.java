package com.test.sagoku;

import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.Row;
import me.prettyprint.hector.api.beans.Rows;
import me.prettyprint.hector.api.exceptions.HectorException;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.query.MultigetSliceQuery;
import me.prettyprint.hector.api.query.QueryResult;


public class MultiGetSlices {

    private static StringSerializer stringSerializer = StringSerializer.get();
    
    public static void main(String[] args) throws Exception {
        
        Cluster cluster = HFactory.getOrCreateCluster("Maple_0_1", "ec2-75-101-186-47.compute-1.amazonaws.com:9160");

        Keyspace keyspace = HFactory.createKeyspace("sequoia", cluster);
                
        try {
                
            MultigetSliceQuery<String, String, String> multigetSliceQuery = 
                    HFactory.createMultigetSliceQuery(keyspace, stringSerializer, stringSerializer, stringSerializer);
                multigetSliceQuery.setColumnFamily("dc_cf");            
                multigetSliceQuery.setKeys("10.2307/20351208", "10.2307/3548184");

            multigetSliceQuery.setRange(null, null, false, 3);
            System.out.println(multigetSliceQuery);

            
            QueryResult<Rows<String, String, String>> result = multigetSliceQuery.execute();
            
            Rows<String, String, String> orderedRows = result.get();
            
            System.out.println("Contents of rows: \n");                       
            for (Row<String, String, String> r : orderedRows) {
                System.out.println("   " + r);
            }
            
            System.out.println("The results should only have 2 entries: " + orderedRows.getCount());
            
        } catch (HectorException he) {
            he.printStackTrace();
        }
        cluster.getConnectionManager().shutdown(); 
    }
}