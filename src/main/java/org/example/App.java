package org.example;



/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args )
    {
        System.out.println( "Hello World!" );
//        if(args.length == 0){
//            System.out.println("Please ...");
//            System.exit(0);
//        }
        String fileUrl = "C:\\Users\\Cool\\Downloads\\spark.txt";
        WordCount.wordCount(fileUrl);
    }
}
