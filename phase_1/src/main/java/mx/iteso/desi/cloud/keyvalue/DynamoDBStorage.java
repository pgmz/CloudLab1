package mx.iteso.desi.cloud.keyvalue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.TableWriteItems;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.QueryRequest;
import com.amazonaws.services.dynamodbv2.model.QueryResult;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.amazonaws.services.dynamodbv2.util.TableUtils;
import com.amazonaws.services.dynamodbv2.model.WriteRequest;
import com.amazonaws.services.dynamodbv2.document.BatchWriteItemOutcome;

import mx.iteso.desi.cloud.lp1.Config;


public class DynamoDBStorage extends BasicKeyValueStore {

    static AmazonDynamoDB dynamoDB;
    DynamoDB dynDB;
    Table dynamoDBTable;
    String dbName;

    // Simple autoincrement counter to make sure we have unique entries
    int inx;

    Set<String> attributesToGet = new HashSet<String>();
    HashMap<String, String> attributesToPut = new HashMap<String, String>();

    public DynamoDBStorage(String dbName) {
        this.dbName = dbName;
        inx = 0;
        init();
        createTable();
        dynamoDBTable = new DynamoDB(dynamoDB).getTable(dbName);
        dynDB = new DynamoDB(dynamoDB);
    }

    private static void init(){
        //Code taken from AmazonDynamoDBSample.java from SDK sample codes

        /*
         * The ProfileCredentialsProvider will return your [default]
         * credential profile by reading from the credentials file located at
         * (~/.aws/credentials).
         */
        ProfileCredentialsProvider credentialsProvider = new ProfileCredentialsProvider();
        try {
            credentialsProvider.getCredentials();
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }

        dynamoDB = AmazonDynamoDBClientBuilder.standard()
            .withCredentials(credentialsProvider)
            .withRegion(Config.amazonRegion)
            .build();
    }

    private void createTable(){
        try{
        CreateTableRequest createTableRequest = new CreateTableRequest().withTableName(dbName)
                .withKeySchema(new KeySchemaElement().withAttributeName("keyword").withKeyType(KeyType.HASH))
                .withAttributeDefinitions(new AttributeDefinition().withAttributeName("keyword").withAttributeType(ScalarAttributeType.S))
                .withKeySchema(new KeySchemaElement().withAttributeName("inx").withKeyType(KeyType.RANGE))
                .withAttributeDefinitions(new AttributeDefinition().withAttributeName("inx").withAttributeType(ScalarAttributeType.N))
                .withProvisionedThroughput(new ProvisionedThroughput().withReadCapacityUnits(1L).withWriteCapacityUnits(1L));

            // Create table if it does not exist yet
            TableUtils.createTableIfNotExists(dynamoDB, createTableRequest);
            // wait for the table to move into ACTIVE state
            TableUtils.waitUntilActive(dynamoDB, dbName);
        } catch(Exception e){
            e.printStackTrace();
        }
    }

    @Override
    public Set<String> get(String search) {
        Set<String> toReturn = new HashSet<String>();
        //Map to indicate the value of "keyword" we are looking for
        Map<String, AttributeValue> keyMap = new HashMap<String, AttributeValue>();
        keyMap.put(":k1", new AttributeValue().withS(search));
        //Map to fix the reserved word "value"
        Map<String, String> nameMap = new HashMap<>();
        nameMap.put("#v", "value");
        //Build Query, indicate that we are looking only for "value"
        QueryRequest queryRequest = new QueryRequest()
            .withTableName(dbName)
            .withProjectionExpression("#v")
            .withExpressionAttributeNames(nameMap)
            .withKeyConditionExpression("keyword = :k1")
            .withExpressionAttributeValues(keyMap);

        System.out.println(queryRequest.toString());

        QueryResult result = dynamoDB.query(queryRequest);
        //From the result, get the map for dbName table
        for (Map<String, AttributeValue> mapRes : result.getItems()){
            //Add results to the set we are about to return
            for (AttributeValue res: mapRes.values()){
                toReturn.add(res.getS());
            }
        }

        return toReturn;
    }

    @Override
    public boolean exists(String search) {
        //check locally, if the attribute exists
        return attributesToGet.contains(search);
    }

    @Override
    public Set<String> getPrefix(String search) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void addToSet(String keyword, String value) {
        //directly put to dynamo, and store in out set :)
        attributesToPut.put(keyword, value);
        if(attributesToPut.size() == 24){
            this.putBatch();
        }
        attributesToGet.add(keyword);
    }

    public void putBatch(){
        //try to add a new item in our table :)
        TableWriteItems tableWriteItems = new TableWriteItems(dbName);

        ArrayList<Item> itemsToPut = new ArrayList<Item>();

        try {
            for (String key : attributesToPut.keySet()){
                itemsToPut.add(
                    new Item()
                    .withPrimaryKey("keyword", key, "inx", inx++)
                    .withString("value", attributesToPut.get(key))
                );
            }
        
            tableWriteItems.withItemsToPut(itemsToPut);
            BatchWriteItemOutcome outcome = dynDB.batchWriteItem(tableWriteItems);

            do {

                Map<String, List<WriteRequest>> unprocessedItems = outcome.getUnprocessedItems();

                if (outcome.getUnprocessedItems().size() == 0) {
                    System.out.println("No unprocessed items found");
                }
                else {
                    System.out.println("Retrieving the unprocessed items");
                    outcome = dynDB.batchWriteItemUnprocessed(unprocessedItems);
                }

            } while (outcome.getUnprocessedItems().size() > 0);

            attributesToPut.clear();
        
        } catch (Exception e) {
            System.out.println("failed to put item" + " " + tableWriteItems.toString());
            System.out.println(e.getMessage());
        }
    }

    @Override
    public void put(String keyword, String value) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void close() {
        dynamoDB.shutdown();
    }
    
    @Override
    public boolean supportsPrefixes() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void sync() {
    }

    @Override
    public boolean isCompressible() {
        return false;
    }

    @Override
    public boolean supportsMoreThan256Attributes() {
        return true;
    }

}
