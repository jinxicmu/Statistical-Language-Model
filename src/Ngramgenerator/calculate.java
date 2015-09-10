package Ngramgenerator;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;


public class calculate {
	public static void main(String[] args) throws IOException{

	BufferedReader bufferedreader = new BufferedReader(
			new InputStreamReader(System.in));
	while (true) {
		String inputLine;
		if ((inputLine = bufferedreader.readLine()) != null) {
			String[] str = inputLine.split("\t");
			if(str[0].equals("life and death"))
	        	System.out.printf(inputLine);
			}
		else break;
		}
	}
}
