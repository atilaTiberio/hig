package mx.iteso;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.model.BatchWriteItemRequest;
import com.amazonaws.services.dynamodbv2.model.BatchWriteItemResult;
import com.amazonaws.services.dynamodbv2.model.PutRequest;
import com.amazonaws.services.dynamodbv2.model.WriteRequest;
import com.opencsv.CSVParser;
import com.opencsv.CSVReader;
import jdk.internal.util.xml.impl.Input;
import mx.iteso.utility.Obesidad;
import org.apache.http.impl.client.BasicCredentialsProvider;

import java.io.*;
import java.util.*;

/**
 * Created by hiturbe on 19/07/17.
 */
public class ParseFile {

    public static AmazonDynamoDB db;
    private String tableName;


    public ParseFile(String tableName){
        this.tableName=tableName;

    }

    public  void loadFile(InputStream stream) throws IOException {
        db= AmazonDynamoDBClientBuilder
                .standard()
                .withRegion(Regions.US_EAST_1)
                .build();

        CSVParser csv= new CSVParser();

        CSVReader reader= new CSVReader(new InputStreamReader(stream));
        Obesidad item=null;

        /*
        requestList size 25
         */
        List<WriteRequest> requestList= new ArrayList<WriteRequest>();

        WriteRequest request = null;

        String[] record=null;

        while((record=reader.readNext())!=null){
            item= new Obesidad(record);
            request=new WriteRequest(new PutRequest().withItem(item.getDynamoItem()));
            requestList.add(request);

            if(requestList.size()==25){
               writeBatch(this.tableName,requestList);
                requestList.clear();
                break;
            }
        }

     if(requestList.size()>0){
            writeBatch(this.tableName,requestList);
        }


    }

    private  boolean writeBatch(String dbName, List<WriteRequest> requestList){


        DynamoDBMapper mapper = new DynamoDBMapper(db);


        Map<String, List<WriteRequest>> requestItems = new HashMap<>();
        requestItems.put(dbName, requestList);

        BatchWriteItemRequest bwir = new BatchWriteItemRequest(requestItems);

        BatchWriteItemResult result  = db.batchWriteItem(bwir);

        do {
            Map<String, List<WriteRequest>> unprocessItems = result.getUnprocessedItems();
            if(unprocessItems.size() > 0) {
                System.out.println("\t"+unprocessItems.size()+" of request not processed");
                result = db.batchWriteItem(unprocessItems);
            } else {
                System.out.println("\tAll request processed");
            }
        } while (result.getUnprocessedItems().size() > 0);

        return true;
    }


}
