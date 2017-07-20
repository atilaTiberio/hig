package mx.iteso;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.model.BatchWriteItemRequest;
import com.amazonaws.services.dynamodbv2.model.BatchWriteItemResult;
import com.amazonaws.services.dynamodbv2.model.PutRequest;
import com.amazonaws.services.dynamodbv2.model.WriteRequest;
import com.opencsv.CSVParser;
import com.opencsv.CSVReader;
import mx.iteso.utility.Obesidad;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

/**
 * Created by hiturbe on 19/07/17.
 */
public class ParseFile {
    public static AmazonDynamoDB db;

    public static  void main(String[] args) throws IOException {
        db= AmazonDynamoDBClientBuilder
                .standard()
                .withRegion("..")
                .build();
        String dbName="tabla";

        CSVParser csv= new CSVParser();
        CSVReader reader= new CSVReader(new FileReader("part-r-00000"));
        Obesidad item=null;

        /*
        requestList size 25
         */
        List<WriteRequest> requestList= new ArrayList<WriteRequest>();

        WriteRequest request = null;
        requestList.add(request);


        //String[] record=reader.readNext();
        String[] record=null;

        /*do{
            item= new Obesidad(record);
            request=new WriteRequest(new PutRequest().withItem(item.getDynamoItem()));
            requestList.add(request);

            if(requestList.size()==25){
                //writeBatch(dbName,requestList);
                System.out.println(requestList.size());
                requestList.clear();

            }
        }while((record=reader.readNext())!=null); */

        while((record=reader.readNext())!=null){
            item= new Obesidad(record);
            request=new WriteRequest(new PutRequest().withItem(item.getDynamoItem()));
            requestList.add(request);

            if(requestList.size()==25){
                //writeBatch(dbName,requestList);
                System.out.println(requestList.size());


                requestList.clear();

            }
        }

        if(requestList.size()>0){
            //writeBatch(dbName,requestList);
            System.out.println(requestList.size());
        }




    }

    public static boolean writeBatch(String dbName, List<WriteRequest> requestList){

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
