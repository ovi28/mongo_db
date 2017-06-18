
import com.mongodb.MongoClient;
import com.mongodb.client.MongoDatabase;


import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Scanner;


public class Main {

    private static QueryBuilder query = new QueryBuilder();

    public static void main( String args[] ) {

        try{
           MongoClient mongoClient = new MongoClient( "localhost" , 27017 );

            MongoDatabase db  = mongoClient.getDatabase( "twitter" );
            System.out.println("Connect to database successfully!");
            String dbName = db.getName();

            Scanner sc = new Scanner(System.in);
            Thread.sleep(500);
            int input;
            while(true){

                showIntro();
            	input = sc.nextInt();
            	boolean shouldClose = false;
            	switch (input){
                    case 1:
                        System.out.println("Distinct tweeter users: " + query.question1(db));
                        break;
                    case 2:
                        executeQuery(query.question2());
                        break;
                    case 3:
                        executeQuery(query.question3());
                        break;
                    case 4:
                        executeQuery(query.question4());
                        break;
                    case 5:
                        executeQuery(query.question5());
                        break;
                    case 6:
                        executeQuery(query.question6());
                        break;
                    default:
                        System.out.println("Invalid input. The program will close");
                        shouldClose = true;
                        break;


                }
                if(shouldClose){
            	    break;
                }
            	Thread.sleep(3000);
            }
            
            mongoClient.close();
        }catch(Exception e){
            System.err.println( e.getClass().getName() + ": " + e.getMessage() );
        }
    }


    private static void showIntro()
    {
    	System.out.println("!!!!***********************************************!!!!!");
        System.out.println("1. How many Twitter users are in our database?");
        System.out.println("2. Which Twitter users link the most to other Twitter users?");
        System.out.println("3. Who is are the most mentioned Twitter users? ");
        System.out.println("4. Who are the most active Twitter users (top ten)?");
        System.out.println("5. Who are the five grumpiest (most negative tweets)?");
        System.out.println("6. Who are the five the happiest (most positive tweets)?");
        System.out.println("!!!!***********************************************!!!!!");
    }

    public static void executeQuery(String q) {
        // implement the logic
        Runtime rt = Runtime.getRuntime();
        try {
            Process pr = rt.exec(new String[]{"mongo", "twitter", "--eval", q});
            InputStream in = pr.getInputStream();
            BufferedReader reader = new BufferedReader(new InputStreamReader(in));
            StringBuilder out = new StringBuilder();
            String line;
            int countLines = 1;
            while((line = reader.readLine()) != null) {
                if (countLines > 2) {
                    out.append("\n" + line);
                }
                countLines++;
            }
            System.out.println(out.toString());
            reader.close();
        } catch (IOException ex) {
            System.out.println(ex);
        }
    }
}