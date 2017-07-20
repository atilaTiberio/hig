package mx.iteso.reduce;


import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.model.*;
import com.opencsv.CSVParser;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by hiturbe on 14/06/17.
 */
public class ReduceCSV extends Reducer<Text, IntWritable, Text, NullWritable> {

    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

            int sum=0;
         for(IntWritable ite: values){
                sum+=ite.get();
            }

            String temp=key.toString();
            Text t= new Text(temp+","+sum);



            context.write(t, NullWritable.get());

    }




}
