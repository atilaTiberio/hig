package mx.iteso;


import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.model.*;
import com.opencsv.CSVParser;
import mx.iteso.mapper.MapperCSV;
import mx.iteso.reduce.ReduceCSV;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args ) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf= new Configuration();
        Job job= Job.getInstance(conf, "CSVCounter");
        job.setJarByClass(App.class);

        job.setMapperClass(MapperCSV.class);
        job.setReducerClass(ReduceCSV.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputValueClass(NullWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        boolean res=job.waitForCompletion(true);

        /*
        ¿Podria meter aqui el procedimiento para leer el output ya que termino el proceso e importar los a Dynamo?
         */
        if(res){
            try {
                Path p = new Path(args[1]);
                FileSystem fs = p.getFileSystem(conf);
                ParseFile pf = new ParseFile(args[2]);

                FileStatus[] status = fs.listStatus(p, file -> file.getName().startsWith("part"));
                Integer totalLines = 0;
                for (int i = 0; i < status.length; i++) {
                    System.out.println(status[i].getPath());
                    System.out.println("Loading to DYnamo");
                    pf.loadFile(fs.open(status[i].getPath()).getWrappedStream());

                }
            }
            catch(Exception e){
                e.printStackTrace();
                System.out.println("Fallo al cargar a dynamo");
            }
        }

        System.exit(res?0:1);

    }



}
