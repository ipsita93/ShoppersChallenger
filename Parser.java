import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement; 
import java.sql.SQLException;

public class Parser {
	public static void main(String[] args) {
		Connection c = null;
		Statement stmt = null;
		try {
			
			Class.forName("com.mysql.jdbc.Driver").newInstance();
			c = DriverManager.getConnection("jdbc:mysql://kivstudymockdb.cy4xjdg7ghgd.us-east-1.rds.amazonaws.com:3306/testdb?user=root&password=qwerty123");
			System.out.println("Opened database successfully.");
			stmt = c.createStatement();
			String sql = "CREATE TABLE Customers " +
				"(id INT PRIMARY KEY     NOT NULL," +
				"chain INT    NOT NULL, " +
				" offer INT  NOT NULL, " +
				"market INT NOT NULL, " +
				"repeattrips INT NOT NULL, "+
				"repeater BOOLEAN NOT NULL, "+
				"offerdate DATE NOT NULL)";
			stmt.executeUpdate(sql);
			stmt.close();
			c.close();
			String offers_filename = "./offers/offers.csv";
			String transactions_filename = "./transactions/transactions.csv";
			String trainHistory_filename = "./trainHistory/trainHistory.csv";
			String testHistory_filename = "./testHistory/testHistory.csv"; 

			//Input file which needs to be parsed
			String fileToParse = trainHistory_filename;
			BufferedReader fileReader = null;

			//Delimiter used in CSV file
			final String DELIMITER = ",";
			try
			{
				String line = "";
				//Create the file reader
				fileReader = new BufferedReader(new FileReader(fileToParse));

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

		} catch (Exception e) {
			System.err.println( e.getClass().getName()+": "+ e.getMessage() );
			System.exit(0);



		}
	}
}
