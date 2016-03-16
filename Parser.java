import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

public class Parser {
	public static void main(String[] args) {
		String directory = "./offers/";
		//Input file which needs to be parsed
		String fileToParse = "offers.csv";
		BufferedReader fileReader = null;

		//Delimiter used in CSV file
		final String DELIMITER = ",";
		try
		{
			String line = "";
			//Create the file reader
			fileReader = new BufferedReader(new FileReader(directory + fileToParse));

			//Read the file line by line
			while ((line = fileReader.readLine()) != null) 
			{
				//Get all tokens available in line
				String[] tokens = line.split(DELIMITER);
				for(String token : tokens)
				{
					//Print all tokens
					System.out.println(token);
				}
			}
		} 
		catch (Exception e) {
			e.printStackTrace();
		} 
		finally
		{
			try {
				fileReader.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
}
