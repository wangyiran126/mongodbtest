package com.company;

import com.mongodb.*;
import com.mongodb.client.*;
import org.bson.Document;
import org.jongo.Aggregate;
import org.jongo.Jongo;

import java.util.Arrays;
import java.util.List;

public class MongodbClient {
    public MongodbClient() {
        this.mongoClient = new MongoClient("localhost", 27017);
        this.db = mongoClient.getDatabase("testResult");;
        MongoIterable<String> names = db.listCollectionNames();
        boolean existCollect =false;
        for (String name :names){
            if (name.equals("testResult")){
                existCollect = true;
            }
        }
        if (existCollect == false){
            db.createCollection("testResult");
        }
        resultCollection = db.getCollection("testResult");
    }

    MongoClient mongoClient;
    MongoDatabase db;
    MongoCollection resultCollection;
    DB mongo;


    public static void main(String[] args) {
        MongodbClient mongoClient = new MongodbClient();
//        mongoClient.insertData();
//       mongoClient.findDocumentJson();
//       Long totalCount =  mongoClient.findCount();
//        mongoClient.computeDeptCountPercent(totalCount);
//        mongoClient.computeDeptCountPercent2(totalCount);
        mongoClient.aggregateJongo();
        //获取每个级别的人数
//        mongoClient.mapreduce();
    }

    private void aggregateJongo() {
        DB db = mongoClient.getDB("testResult");
        Jongo jongo = new Jongo(db);
        org.jongo.MongoCollection resultCollection = jongo.getCollection("testResult");
        Aggregate.ResultsIterator<DeptMap> deptMaps = resultCollection.aggregate("{$group:{_id:{deptId:\"$dept.id\",name:\"$dept.name\"},userIds:{$addToSet:\"$userId\"}}}")
                .and("{$project:{deptId:\"$_id.deptId\",deptName:\"$_id.name\",userCount:{$size:\"$userIds\"}}}")
                .as(DeptMap.class);

        for (DeptMap deptMap:deptMaps){
            System.out.println(deptMap);
        }


    }

    private void computeDeptCountPercent2(Long totalCount) {
        //不同部门人数
        AggregateIterable<Document> iterable = resultCollection.aggregate(Arrays.asList(
                new Document("$group", new Document
                                            ("_id",new Document("deptId", "$dept.id")
                                        .append("deptName", "$dept.name")
                                        ).append("userIds",new Document("$addToSet","$userId")
                                            ))
                ,new Document("$project",new Document("deptId","$_id.deptId")
                .append("userSize",new Document("$size","$userIds"))
                .append("deptName", "$_id.deptName"))
                )
        );

        for (Document document:iterable){
            Integer deptId = document.getInteger("deptId");
            Integer userSize = document.getInteger("userSize");
            String deptName = document.getString("deptName");
            System.out.println("deptId:"+deptId+",userSize:"+userSize+",deptName:"+deptName);
        }
    }

    private void computeDeptCountPercent(Long totalCount) {
        //不同部门人数
        AggregateIterable<Document> iterable = resultCollection.aggregate(Arrays.asList(
                new Document("$group", new Document("_id", "$dept.id")
                        .append("deptName",new Document("$addToSet","$dept.name"))
                        .append("userIdSet",new Document("$addToSet","$userId")
                ))
                ));

        for (Document document:iterable){
            Double deptId = document.getDouble("_id");
            List<Long> userIds = document.get("userIdSet",List.class);
            List<String> deptName = document.get("deptName",List.class);

            Double percent = Double.valueOf(userIds.size())/totalCount;
            System.out.println("deptId:"+deptId+",percent:"+percent+",deptName:"+deptName.get(0));
        }

    }

    private void mapreduce() {


    }

    private Long findCount() {

        BasicDBObject query = new BasicDBObject();
        query.append("result.resultId",1);
        Long count = resultCollection.count(new Document("result.resultId",1));
        return count;
    }

    private void findDocumentJson() {

        FindIterable<Document> count =  db.getCollection("testResult").find(
                new Document("result.resultId",0)
            );
        for (Document document : count) {
           String json = document.toJson();
            System.out.println(json);
        }
    }


    private void insertData() {
    //TODO 年龄段最好直接存age,否则要先转成date存入mongodb,要不具体string date不行
        db.getCollection("testResult").insertOne(
                new Document()
                        .append("userId",2)
                        .append("name","王奕然2")
                        .append("birthday","2011-06-13")

                        .append("dept",new Document()
                        .append("id",13)
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

