import com.mongodb.client.MongoDatabase;
import org.bson.BsonValue;
import org.bson.Document;

import java.util.ArrayList;


public class QueryBuilder {

    //How many Twitter users are in our database?
    public Integer question1(MongoDatabase db) {
        ArrayList<BsonValue> count = db.getCollection("twitter").distinct("user", BsonValue.class).
                filter(new Document("user", new Document("$ne", null))).
                into(new ArrayList<BsonValue>());
        return count.size();
    }

    //Which Twitter users link the most to other Twitter users? (Provide the top ten.)
    public String question2() {
        return "db.twitter.aggregate("
                + "{$match: {text: {$regex: /@\\S+/g}}}, "
                + "{$group: {_id: \"$user\", count:{$sum:1}}}, "
                + "{$sort: {\"count\":-1}}, {$limit: 10})";
    }

    //Who is are the most mentioned Twitter users? (Provide the top five.)
    public String question3() {
        return "db.twitter.aggregate([" +
                "         {" +
                "           $match: { " +
                "            text: new RegExp('@\\w+', 'ig'), " +
                "          }," +
                "         }, " +
                "        {           $group: { _id: '$user', tweets: { $sum: 1 } }," +
                "         }," +
                "         {           $sort: { tweets: -1 }," +
                "         },         {           $limit: 5,         }," +
                "       ],       { allowDiskUse: true })";
    }

    //Who are the most active Twitter users (top ten)?
    public String question4() {
        return "db.twitter.aggregate(\n" +
                "      [\n" +
                "        {\n" +
                "          $group: {\n" +
                "            _id: {\n" +
                "              user: '$user',\n" +
                "              tweetId: '$id',\n" +
                "            },\n" +
                "          },\n" +
                "        },\n" +
                "        {\n" +
                "          $group: {\n" +
                "            _id: '$_id.user',\n" +
                "            count: { $sum: 1 },\n" +
                "          },\n" +
                "        },\n" +
                "        {\n" +
                "          $sort: { count: -1 },\n" +
                "        },\n" +
                "        {\n" +
                "          $limit: 10,\n" +
                "        },\n" +
                "      ],\n" +
                "      { allowDiskUse: true })";

    }

    //Who are the five most grumpy (most negative tweets)?
    public String question5() {
        return "db.twitter.aggregate( [    " +
                "{     $group: {       _id: '$user'," +
                "       average_polarity: { $avg: '$polarity' }," +
                "     }," +
                "   }," +
                "   {     " +
                "$sort: { average_polarity: 1 },  " +
                " }," +
                "   {     " +
                "$limit: 5," +
                "   }," +
                "]," +
                "{ allowDiskUse: true })";
    }

    //Most happy?
    public String question6() {
        return "db.twitter.aggregate( [" +
                "    {     $group: {       _id: '$user'," +
                "       average_polarity: { $avg: '$polarity' }," +
                "     }," +
                "   }, " +
                "  {     $sort: { average_polarity: -1 }," +
                "   }," +
                "   {     $limit: 5,   }" +
                ",]" +
                ",{ allowDiskUse: true })";
    }

}
