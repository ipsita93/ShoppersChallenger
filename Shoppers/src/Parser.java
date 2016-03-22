import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement; 
import java.sql.SQLException;

public class Parser {
	public static void main(String[] args) {

		String offers_filename = "C:\\Users\\Sony\\Google Drive\\NUS\\8 - Year 4 Sem 2\\CS4225 Massive Data Processing Techniques in Data Science\\Project\\ShoppersChallenge\\offers\\offers.csv";
		String transactions_filename = "C:\\Users\\Sony\\Google Drive\\NUS\\8 - Year 4 Sem 2\\CS4225 Massive Data Processing Techniques in Data Science\\Project\\ShoppersChallenge\\transactions\\xaa.csv";
		String trainHistory_filename = "C:\\Users\\Sony\\Google Drive\\NUS\\8 - Year 4 Sem 2\\CS4225 Massive Data Processing Techniques in Data Science\\Project\\ShoppersChallenge\\trainHistory\\trainHistory.csv";
		//String testHistory_filename = "./testHistory/testHistory.csv"; 

		populateOffersTable(offers_filename, "offers");
		populateTransactionTable(transactions_filename, "transactions");
		populateTrainHistoryTable(trainHistory_filename, "trainHistory");
		populateCustomersTable();
	}

	public static void populateCustomersTable() {
		Connection c = null;
		Statement stmt = null;
		try {

		Class.forName("com.mysql.jdbc.Driver").newInstance();
		c = DriverManager.getConnection("jdbc:mysql://kivstudymockdb.cy4xjdg7ghgd.us-east-1.rds.amazonaws.com:3306/shoppers?user=root&password=qwerty123");
		System.out.println("Opened database successfully.");
		stmt = c.createStatement();
		
		String sql = "CREATE TABLE customers AS (SELECT trainHistory.*, offers.category, offers.quantity, offers.company_id, offers.offervalue, offers.brand_id FROM   trainHistory INNER JOIN offers ON trainHistory.offer_id = offers.offer_id);";
		
		stmt.executeUpdate(sql);
		stmt.close();
		c.close();
		} 
		catch (Exception e) {
			e.printStackTrace();
		} 
	
	}
	
	public static void populateOffersTable(String fileName, String tableName){
		String fileToParse = fileName;
		BufferedReader fileReader = null;

		Connection c = null;
		Statement stmt = null;
		try {

			Class.forName("com.mysql.jdbc.Driver").newInstance();
			c = DriverManager.getConnection("jdbc:mysql://kivstudymockdb.cy4xjdg7ghgd.us-east-1.rds.amazonaws.com:3306/shoppers?user=root&password=qwerty123");
			System.out.println("Opened database successfully.");
			stmt = c.createStatement();

			//Delimiter used in CSV file
			final String DELIMITER = ",";
			try
			{
				String line = "";
				//Create the file reader
				fileReader = new BufferedReader(new FileReader(fileToParse));
				
				int numLines = 0; 
				//Read the file line by line
				while ((line = fileReader.readLine()) != null) 
				{
					if (numLines != 0) {
						//System.out.println(line);
						//Get all tokens available in line
						String[] tokens = line.split(DELIMITER);
						String sql = "Insert into " + tableName + " values (";
						for(int i=0; i<tokens.length; i++)
						{
							if (Character.isDigit(tokens[i].charAt(0)) == false) {
								System.out.println(tokens[i]);
								tokens[i] = "\'"+tokens[i]+"\'";
							}
							//Print all tokens
							sql += tokens[i];
							if(i!=tokens.length-1){
								sql += ",";
							}			
						}
						sql += ");" ;
						System.out.println(sql);
						stmt.executeUpdate(sql);
						
					}
					numLines++;
					
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

			stmt.close();
			c.close();

		} catch (Exception e) {
			System.err.println( e.getClass().getName()+": "+ e.getMessage() );
			System.exit(0);
		}

	}

	public static void populateTransactionTable(String fileName, String tableName){
		String fileToParse = fileName;
		BufferedReader fileReader = null;

		Connection c = null;
		Statement stmt = null;
		try {

			Class.forName("com.mysql.jdbc.Driver").newInstance();
			c = DriverManager.getConnection("jdbc:mysql://kivstudymockdb.cy4xjdg7ghgd.us-east-1.rds.amazonaws.com:3306/shoppers?user=root&password=qwerty123");
			System.out.println("Opened database successfully.");
			stmt = c.createStatement();

			//Delimiter used in CSV file
			final String DELIMITER = ",";
			try
			{
				String line = "";
				//Create the file reader
				fileReader = new BufferedReader(new FileReader(fileToParse));
				
				int numLines = 0; 
				//Read the file line by line
				while ((line = fileReader.readLine()) != null) 
				{
					if (numLines != 0) {
						//System.out.println(line);
						//Get all tokens available in line
						String[] tokens = line.split(DELIMITER);
						String sql = "Insert into " + tableName + " values (";
						for(int i=0; i<tokens.length; i++)
						{
							if (Character.isDigit(tokens[i].charAt(0)) == false) {
								System.out.println(tokens[i]);
								tokens[i] = "\'"+tokens[i]+"\'";
							}
							//Print all tokens
							sql += tokens[i];
							if(i!=tokens.length-1){
								sql += ",";
							}			
						}
						sql += ");" ;
						System.out.println(sql);
						stmt.executeUpdate(sql);
						
					}
					numLines++;
					
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

			stmt.close();
			c.close();

		} catch (Exception e) {
			System.err.println( e.getClass().getName()+": "+ e.getMessage() );
			System.exit(0);
		}

	}

	public static void populateTrainHistoryTable(String fileName, String tableName){
		String fileToParse = fileName;
		BufferedReader fileReader = null;

		Connection c = null;
		Statement stmt = null;
		try {

			Class.forName("com.mysql.jdbc.Driver").newInstance();
			c = DriverManager.getConnection("jdbc:mysql://kivstudymockdb.cy4xjdg7ghgd.us-east-1.rds.amazonaws.com:3306/shoppers?user=root&password=qwerty123");
			System.out.println("Opened database successfully.");
			stmt = c.createStatement();

			//Delimiter used in CSV file
			final String DELIMITER = ",";
			try
			{
				String line = "";
				//Create the file reader
				fileReader = new BufferedReader(new FileReader(fileToParse));
				
				int numLines = 0; 
				//Read the file line by line
				while ((line = fileReader.readLine()) != null) 
				{
					if (numLines != 0) {
						//System.out.println(line);
						//Get all tokens available in line
						String[] tokens = line.split(DELIMITER);
						String sql = "Insert into " + tableName + " values (";
						for(int i=0; i<tokens.length; i++)
						{
							if (Character.isDigit(tokens[i].charAt(0)) == false) {
								System.out.println(tokens[i]);
								tokens[i] = "\'"+tokens[i]+"\'";
							}
							//Print all tokens
							sql += tokens[i];
							if(i!=tokens.length-1){
								sql += ",";
							}			
						}
						sql += ");" ;
						System.out.println(sql);
						stmt.executeUpdate(sql);
						
					}
					numLines++;
					
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

			stmt.close();
			c.close();

		} catch (Exception e) {
			System.err.println( e.getClass().getName()+": "+ e.getMessage() );
			System.exit(0);
		}

	}


}
