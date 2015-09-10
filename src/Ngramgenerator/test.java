package Ngramgenerator;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;


public class test {
	public static void main(String[] args) throws IOException{	
		String line = "1380 Pear Avenue, Mountain45 View, CA 94043, USA 2501 S. Winchester Blvd., Campbell, CA 95008 ";
		String afterRep = line.replaceAll("[^a-zA-Z]+", " ").toLowerCase().trim();
		System.out.println(afterRep);
		String[] wordSet = afterRep.split(" ");
        //int n = 1;
        //int i = 0;
        for(int i = 1; i <= 5;i++){
        	for(int j = 0;j <= (wordSet.length - i);j++){
            	StringBuilder out = new StringBuilder();
            	int k;
            	for(k = j;k < (j + i -1);k++){
            		out.append(wordSet[k]);
            		out.append(" ");
            	}
            	out.append(wordSet[k]);
            	String word = out.toString().trim();
                System.out.println(word + j);
        	}
        }
	}
}
