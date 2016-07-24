package com.company;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoIterable;
import org.bson.Document;
public class MongodbClient {
    public MongodbClient() {
        this.mongoClient = new MongoClient("localhost", 27017);
        this.db = mongoClient.getDatabase("local");;
        MongoIterable<String> names = db.listCollectionNames();
        boolean existCollect =false;
        for (String name :names){
            if (name.equals("testresult")){
                existCollect = true;
            }
        }
        if (existCollect == false){
            db.createCollection("testresult");
        }
        resultCollection = db.getCollection("testresult");
    }

    MongoClient mongoClient;
    MongoDatabase db;
    MongoCollection resultCollection;


    public static void main(String[] args) {
        MongodbClient mongoClient = new MongodbClient();
//        mongoClient.insertData();
//       mongoClient.findDocumentJson();
        mongoClient.findCount();
        mongoClient.mapreduce();
    }

    private void mapreduce() {


    }

    private void findCount() {

        BasicDBObject query = new BasicDBObject();
        query.append("result.resultId",1);
        Long count = resultCollection.count(new Document("result.resultId",1));
        System.out.println(count);
    }

    private void findDocumentJson() {

        FindIterable<Document> count =  db.getCollection("testresult").find(
                new Document("result.resultId",0)
            );
        for (Document document : count) {
           String json = document.toJson();
            System.out.println(json);
        }
    }


    private void insertData() {
        db.getCollection("testresult").insertOne(
                new Document()
                        .append("userId",2)
                        .append("name","王奕然2")
                        .append("birthday","2011-06-13")
                        .append("dept",new Document()
                        .append("id",14)
                        .append("name","1团")
                        )
                .append("result",Document.parse("{\n" +
                        "    \"resultId\":2,\n" +
                        "    \"OriginalScore\": \"120.0\",\n" +
                        "    \"StandarScore\": \"2.26\",\n" +
                        "    \"age\": 10,\n" +
                        "    \"data\": [\n" +
                        "      {\n" +
                        "        \"childDimensions\": [\n" +
                        "          {\n" +
                        "            \"id\": \"1\",\n" +
                        "            \"name\": \"感官系统\",\n" +
                        "            \"score\": \"1.3333333333333333\"\n" +
                        "          },\n" +
                        "          {\n" +
                        "            \"id\": \"2\",\n" +
                        "            \"name\": \"呼吸系统\",\n" +
                        "            \"score\": \"3.0\"\n" +
                        "          },\n" +
                        "          {\n" +
                        "            \"id\": \"3\",\n" +
                        "            \"name\": \"骨骼肌肉系统\",\n" +
                        "            \"score\": \"3.0\"\n" +
                        "          },\n" +
                        "          {\n" +
                        "            \"id\": \"4\",\n" +
                        "            \"name\": \"神经系统\",\n" +
                        "            \"score\": \"3.0\"\n" +
                        "          },\n" +
                        "          {\n" +
                        "            \"id\": \"5\",\n" +
                        "            \"name\": \"精力\",\n" +
                        "            \"score\": \"2.0\"\n" +
                        "          },\n" +
                        "          {\n" +
                        "            \"id\": \"6\",\n" +
                        "            \"name\": \"免疫系统\",\n" +
                        "            \"score\": \"2.5\"\n" +
                        "          },\n" +
                        "          {\n" +
                        "            \"id\": \"7\",\n" +
                        "            \"name\": \"睡眠\",\n" +
                        "            \"score\": \"3.0\"\n" +
                        "          },\n" +
                        "          {\n" +
                        "            \"id\": \"8\",\n" +
                        "            \"name\": \"消化系统\",\n" +
                        "            \"score\": \"2.0\"\n" +
                        "          },\n" +
                        "          {\n" +
                        "            \"id\": \"9\",\n" +
                        "            \"name\": \"心血管系统\",\n" +
                        "            \"score\": \"2.0\"\n" +
                        "          },\n" +
                        "          {\n" +
                        "            \"id\": \"10\",\n" +
                        "            \"name\": \"性\",\n" +
                        "            \"score\": \"1.0\"\n" +
                        "          },\n" +
                        "          {\n" +
                        "            \"id\": \"11\",\n" +
                        "            \"name\": \"综合\",\n" +
                        "            \"score\": \"2.0\"\n" +
                        "          }\n" +
                        "        ],\n" +
                        "        \"id\": \"1\",\n" +
                        "        \"level\": \"9\",\n" +
                        "        \"name\": \"生理\",\n" +
                        "        \"quids\": [\n" +
                        "          \"37\",\n" +
                        "          \"10\",\n" +
                        "          \"44\",\n" +
                        "          \"41\",\n" +
                        "          \"6\",\n" +
                        "          \"18\",\n" +
                        "          \"40\",\n" +
                        "          \"47\",\n" +
                        "          \"1\",\n" +
                        "          \"17\",\n" +
                        "          \"49\",\n" +
                        "          \"9\",\n" +
                        "          \"19\"\n" +
                        "        ],\n" +
                        "        \"score\": \"2.05\"\n" +
                        "      },\n" +
                        "      {\n" +
                        "        \"childDimensions\": [\n" +
                        "          {\n" +
                        "            \"id\": \"1\",\n" +
                        "            \"name\": \"抑郁情绪\",\n" +
                        "            \"score\": \"1.9\"\n" +
                        "          },\n" +
                        "          {\n" +
                        "            \"id\": \"2\",\n" +
                        "            \"name\": \"焦虑情绪\",\n" +
                        "            \"score\": \"3.0\"\n" +
                        "          },\n" +
                        "          {\n" +
                        "            \"id\": \"3\",\n" +
                        "            \"name\": \"敌意情绪\",\n" +
                        "            \"score\": \"3.0\"\n" +
                        "          },\n" +
                        "          {\n" +
                        "            \"id\": \"4\",\n" +
                        "            \"name\": \"无奈情绪\",\n" +
                        "            \"score\": \"4.0\"\n" +
                        "          },\n" +
                        "          {\n" +
                        "            \"id\": \"5\",\n" +
                        "            \"name\": \"孤独情绪\",\n" +
                        "            \"score\": \"4.0\"\n" +
                        "          },\n" +
                        "          {\n" +
                        "            \"id\": \"6\",\n" +
                        "            \"name\": \"躁动情绪\",\n" +
                        "            \"score\": \"3.0\"\n" +
                        "          }\n" +
                        "        ],\n" +
                        "        \"id\": \"2\",\n" +
                        "        \"level\": \"10\",\n" +
                        "        \"name\": \"情绪\",\n" +
                        "        \"quids\": [\n" +
                        "          \"5\",\n" +
                        "          \"11\",\n" +
                        "          \"38\",\n" +
                        "          \"39\",\n" +
                        "          \"46\",\n" +
                        "          \"48\",\n" +
                        "          \"7\",\n" +
                        "          \"13\",\n" +
                        "          \"35\",\n" +
                        "          \"50\",\n" +
                        "          \"8\",\n" +
                        "          \"53\",\n" +
                        "          \"52\",\n" +
                        "          \"43\"\n" +
                        "        ],\n" +
                        "        \"score\": \"2.50\"\n" +
                        "      },\n" +
                        "      {\n" +
                        "        \"childDimensions\": [\n" +
                        "          {\n" +
                        "            \"id\": \"1\",\n" +
                        "            \"name\": \"抱怨行为\",\n" +
                        "            \"score\": \"3.0\"\n" +
                        "          },\n" +
                        "          {\n" +
                        "            \"id\": \"2\",\n" +
                        "            \"name\": \"烦躁行为\",\n" +
                        "            \"score\": \"2.0\"\n" +
                        "          },\n" +
                        "          {\n" +
                        "            \"id\": \"3\",\n" +
                        "            \"name\": \"攻击行为\",\n" +
                        "            \"score\": \"3.0\"\n" +
                        "          },\n" +
                        "          {\n" +
                        "            \"id\": \"4\",\n" +
                        "            \"name\": \"退缩行为\",\n" +
                        "            \"score\": \"1.0\"\n" +
                        "          },\n" +
                        "          {\n" +
                        "            \"id\": \"5\",\n" +
                        "            \"name\": \"依赖行为\",\n" +
                        "            \"score\": \"3.0\"\n" +
                        "          },\n" +
                        "          {\n" +
                        "            \"id\": \"6\",\n" +
                        "            \"name\": \"指责行为\",\n" +
                        "            \"score\": \"2.0\"\n" +
                        "          },\n" +
                        "          {\n" +
                        "            \"id\": \"7\",\n" +
                        "            \"name\": \"转化性行为\",\n" +
                        "            \"score\": \"2.0\"\n" +
                        "          }\n" +
                        "        ],\n" +
                        "        \"id\": \"3\",\n" +
                        "        \"level\": \"10\",\n" +
                        "        \"name\": \"行为\",\n" +
                        "        \"quids\": [\n" +
                        "          \"16\",\n" +
                        "          \"2\",\n" +
                        "          \"36\",\n" +
                        "          \"45\",\n" +
                        "          \"51\",\n" +
                        "          \"14\",\n" +
                        "          \"3\",\n" +
                        "          \"12\"\n" +
                        "        ],\n" +
                        "        \"score\": \"2.27\"\n" +
                        "      },\n" +
                        "      {\n" +
                        "        \"childDimensions\": [\n" +
                        "          {\n" +
                        "            \"id\": \"1\",\n" +
                        "            \"name\": \"认知操作受损\",\n" +
                        "            \"score\": \"2.25\"\n" +
                        "          }\n" +
                        "        ],\n" +
                        "        \"id\": \"4\",\n" +
                        "        \"level\": \"8\",\n" +
                        "        \"name\": \"认知\",\n" +
                        "        \"quids\": [\n" +
                        "          \"4\",\n" +
                        "          \"15\",\n" +
                        "          \"42\"\n" +
                        "        ],\n" +
                        "        \"score\": \"2.25\"\n" +
                        "      }\n" +
                        "    ],\n" +
                        "    \"level\": \"10\",\n" +
                        "    \"optionHistory\": {\n" +
                        "      \"1\": \"3\",\n" +
                        "      \"2\": \"3\",\n" +
                        "      \"3\": \"3\",\n" +
                        "      \"4\": \"3\",\n" +
                        "      \"5\": \"3\",\n" +
                        "      \"6\": \"4\",\n" +
                        "      \"7\": \"4\",\n" +
                        "      \"8\": \"4\",\n" +
                        "      \"9\": \"4\",\n" +
                        "      \"10\": \"4\",\n" +
                        "      \"11\": \"4\",\n" +
                        "      \"12\": \"4\",\n" +
                        "      \"13\": \"4\",\n" +
                        "      \"14\": \"4\",\n" +
                        "      \"15\": \"4\",\n" +
                        "      \"16\": \"4\",\n" +
                        "      \"17\": \"3\",\n" +
                        "      \"18\": \"3\",\n" +
                        "      \"19\": \"3\",\n" +
                        "      \"20\": \"2\",\n" +
                        "      \"21\": \"2\",\n" +
                        "      \"22\": \"2\",\n" +
                        "      \"23\": \"2\",\n" +
                        "      \"24\": \"2\",\n" +
                        "      \"25\": \"2\",\n" +
                        "      \"26\": \"2\",\n" +
                        "      \"27\": \"2\",\n" +
                        "      \"28\": \"2\",\n" +
                        "      \"29\": \"2\",\n" +
                        "      \"30\": \"2\",\n" +
                        "      \"31\": \"2\",\n" +
                        "      \"32\": \"2\",\n" +
                        "      \"33\": \"2\",\n" +
                        "      \"34\": \"2\",\n" +
                        "      \"35\": \"3\",\n" +
                        "      \"36\": \"3\",\n" +
                        "      \"37\": \"3\",\n" +
                        "      \"38\": \"3\",\n" +
                        "      \"39\": \"3\",\n" +
                        "      \"40\": \"4\",\n" +
                        "      \"41\": \"4\",\n" +
                        "      \"42\": \"4\",\n" +
                        "      \"43\": \"4\",\n" +
                        "      \"44\": \"4\",\n" +
                        "      \"45\": \"4\",\n" +
                        "      \"46\": \"4\",\n" +
                        "      \"47\": \"4\",\n" +
                        "      \"48\": \"4\",\n" +
                        "      \"49\": \"4\",\n" +
                        "      \"50\": \"5\",\n" +
                        "      \"51\": \"5\",\n" +
                        "      \"52\": \"5\",\n" +
                        "      \"53\": \"5\",\n" +
                        "      \"54\": \"5\",\n" +
                        "      \"55\": \"5\",\n" +
                        "      \"56\": \"5\",\n" +
                        "      \"57\": \"5\",\n" +
                        "      \"58\": \"5\",\n" +
                        "      \"59\": \"5\",\n" +
                        "      \"60\": \"5\",\n" +
                        "      \"61\": \"4\",\n" +
                        "      \"62\": \"4\",\n" +
                        "      \"63\": \"3\"\n" +
                        "    }\n" +
                        "}"))
        );
    }
}

