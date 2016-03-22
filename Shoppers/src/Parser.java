import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

public class Parser {
	public static void main(String[] args) {

		String offers_filename = "C:\\Users\\Sony\\Google Drive\\NUS\\8 - Year 4 Sem 2\\CS4225 Massive Data Processing Techniques in Data Science\\Project\\ShoppersChallenge\\offers\\offers.csv";
		String transactions_filename = "C:\\Users\\Sony\\Google Drive\\NUS\\8 - Year 4 Sem 2\\CS4225 Massive Data Processing Techniques in Data Science\\Project\\ShoppersChallenge\\transactions\\xaa.csv";
		String trainHistory_filename = "C:\\Users\\Sony\\Google Drive\\NUS\\8 - Year 4 Sem 2\\CS4225 Massive Data Processing Techniques in Data Science\\Project\\ShoppersChallenge\\trainHistory\\trainHistory.csv";
		// String testHistory_filename = "./testHistory/testHistory.csv";

		// populateOffersTable(offers_filename, "offers");
		// populateTrainHistoryTable(trainHistory_filename, "trainHistory");
		// populateCustomersTable();
		populateNewFeaturesCustomersTable(transactions_filename); // takes in
																	// filename

		// populateTransactionTable(transactions_filename, "transactions");

		System.out.println("Parsing done.");
	}

	public static void populateNewFeaturesCustomersTable(String fileName) {
		String fileToParse = fileName;
		BufferedReader fileReader = null;

		Connection c = null;
		Statement stmt = null;
		try {

			Class.forName("com.mysql.jdbc.Driver").newInstance();
			c = DriverManager
					.getConnection("jdbc:mysql://kivstudymockdb.cy4xjdg7ghgd.us-east-1.rds.amazonaws.com:3306/shoppers?user=root&password=qwerty123");
			System.out.println("Opened database successfully.");
			stmt = c.createStatement();

			// Delimiter used in CSV file
			final String DELIMITER = ",";
			try {
				String line = "";
				// Create the file reader
				fileReader = new BufferedReader(new FileReader(fileToParse));

				String sql = "alter table customers add column has_bought_company_before_q int;";
				stmt.executeUpdate(sql);
				
				int numLines = 0;
				// Read the file line by line
				while ((line = fileReader.readLine()) != null) {
					if (numLines != 0) {
						// Get all tokens available in line
						String[] tokens = line.split(DELIMITER);
						// for (int i = 0; i < tokens.length; i++) {
						String query = "select c.has_bought_company_before_q from customers c"
								+ " where c.customer_id = "
								+ tokens[0]
								+ " and c.company_id = " + tokens[3] + ";";
						ResultSet rs = stmt.executeQuery(query);
						int iVal1 = 0;
						while (rs.next()) {
							iVal1 = Integer.getInteger(rs
									.getString("has_bought_company_before_q"));
						}
						iVal1++;
						sql = "update customers c "
								+ "set c.has_bought_company_before_q = " + iVal1
								+ " where c.customer_id = " + tokens[0] + ";";
						stmt.executeUpdate(sql);

						// }

					}
					numLines++;

				}
			} catch (Exception e) {
				e.printStackTrace();
			} finally {
				try {
					fileReader.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}

			stmt.close();
			c.close();

		} catch (Exception e) {
			System.err.println(e.getClass().getName() + ": " + e.getMessage());
			System.exit(0);
		}

	}

	public static void populateCustomersTable() {
		Connection c = null;
		Statement stmt = null;
		try {

			Class.forName("com.mysql.jdbc.Driver").newInstance();
			c = DriverManager
					.getConnection("jdbc:mysql://kivstudymockdb.cy4xjdg7ghgd.us-east-1.rds.amazonaws.com:3306/shoppers?user=root&password=qwerty123");
			System.out.println("Opened database successfully.");
			stmt = c.createStatement();

			String sql = "CREATE TABLE customers AS (SELECT trainHistory.*, offers.category, offers.quantity, offers.company_id, offers.offervalue, offers.brand_id FROM   trainHistory INNER JOIN offers ON trainHistory.offer_id = offers.offer_id);";

			stmt.executeUpdate(sql);
			stmt.close();
			c.close();
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	public static void populateOffersTable(String fileName, String tableName) {
		String fileToParse = fileName;
		BufferedReader fileReader = null;

		Connection c = null;
		Statement stmt = null;
		try {

			Class.forName("com.mysql.jdbc.Driver").newInstance();
			c = DriverManager
					.getConnection("jdbc:mysql://kivstudymockdb.cy4xjdg7ghgd.us-east-1.rds.amazonaws.com:3306/shoppers?user=root&password=qwerty123");
			System.out.println("Opened database successfully.");
			stmt = c.createStatement();

			// Delimiter used in CSV file
			final String DELIMITER = ",";
			try {
				String line = "";
				// Create the file reader
				fileReader = new BufferedReader(new FileReader(fileToParse));

				int numLines = 0;
				// Read the file line by line
				while ((line = fileReader.readLine()) != null) {
					if (numLines != 0) {
						// System.out.println(line);
						// Get all tokens available in line
						String[] tokens = line.split(DELIMITER);
						String sql = "Insert into " + tableName + " values (";
						for (int i = 0; i < tokens.length; i++) {
							if (Character.isDigit(tokens[i].charAt(0)) == false) {
								System.out.println(tokens[i]);
								tokens[i] = "\'" + tokens[i] + "\'";
							}
							// Print all tokens
							sql += tokens[i];
							if (i != tokens.length - 1) {
								sql += ",";
							}
						}
						sql += ");";
						System.out.println(sql);
						stmt.executeUpdate(sql);

					}
					numLines++;

				}
			} catch (Exception e) {
				e.printStackTrace();
			} finally {
				try {
					fileReader.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}

			stmt.close();
			c.close();

		} catch (Exception e) {
			System.err.println(e.getClass().getName() + ": " + e.getMessage());
			System.exit(0);
		}

	}

	public static void populateTransactionTable(String fileName,
			String tableName) {
		String fileToParse = fileName;
		BufferedReader fileReader = null;

		Connection c = null;
		Statement stmt = null;
		try {

			Class.forName("com.mysql.jdbc.Driver").newInstance();
			c = DriverManager
					.getConnection("jdbc:mysql://kivstudymockdb.cy4xjdg7ghgd.us-east-1.rds.amazonaws.com:3306/shoppers?user=root&password=qwerty123");
			System.out.println("Opened database successfully.");
			stmt = c.createStatement();

			// Delimiter used in CSV file
			final String DELIMITER = ",";
			try {
				String line = "";
				// Create the file reader
				fileReader = new BufferedReader(new FileReader(fileToParse));

				int numLines = 0;
				// Read the file line by line
				while ((line = fileReader.readLine()) != null) {
					if (numLines != 0) {
						// System.out.println(line);
						// Get all tokens available in line
						String[] tokens = line.split(DELIMITER);
						String t_sql = "Insert into " + tableName + " values (";
						String tp_sql = "Insert into " + tableName
								+ "_processsed" + " values (";

						for (int i = 0; i < tokens.length; i++) {
							// customer_id,chain_id,dept,category,company_id,brand_id,date,productsize,productmeasure,purchasequantity,purchaseamount
							if (i == 0 || i == 1 || i == 2 || i == 3 || i == 4
									|| i == 5 || i == 9) {
								// change to binary
								tp_sql += "b\'"
										+ Integer.toBinaryString(new Integer(
												tokens[i])) + "\'" + ",";
							} else if (i == 6) {
								tp_sql += "\'" + tokens[i] + "\'" + ",";
								tokens[i] = "\'" + tokens[i] + "\'";
							} else if (i == 7 || i == 10) {
								tp_sql += tokens[i] + ",";
							}

							if (i == 8) {
								tokens[i] = "\'" + tokens[i] + "\'";
							}
							// Print all tokens
							t_sql += tokens[i];
							if (i != tokens.length - 1) {
								t_sql += ",";

							}
						}
						t_sql += ");";
						tp_sql = tp_sql.substring(0, tp_sql.length() - 1); // remove
																			// last
																			// comma
						tp_sql += ");";

						System.out.println(t_sql);
						stmt.executeUpdate(t_sql);
						System.out.println(tp_sql);
						stmt.executeUpdate(tp_sql);

					}
					numLines++;

				}
			} catch (Exception e) {
				e.printStackTrace();
			} finally {
				try {
					fileReader.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}

			stmt.close();
			c.close();

		} catch (Exception e) {
			System.err.println(e.getClass().getName() + ": " + e.getMessage());
			System.exit(0);
		}

	}

	public static void populateTrainHistoryTable(String fileName,
			String tableName) {
		String fileToParse = fileName;
		BufferedReader fileReader = null;

		Connection c = null;
		Statement stmt = null;
		try {

			Class.forName("com.mysql.jdbc.Driver").newInstance();
			c = DriverManager
					.getConnection("jdbc:mysql://kivstudymockdb.cy4xjdg7ghgd.us-east-1.rds.amazonaws.com:3306/shoppers?user=root&password=qwerty123");
			System.out.println("Opened database successfully.");
			stmt = c.createStatement();

			// Delimiter used in CSV file
			final String DELIMITER = ",";
			try {
				String line = "";
				// Create the file reader
				fileReader = new BufferedReader(new FileReader(fileToParse));

				int numLines = 0;
				// Read the file line by line
				while ((line = fileReader.readLine()) != null) {
					if (numLines != 0) {
						// System.out.println(line);
						// Get all tokens available in line
						String[] tokens = line.split(DELIMITER);
						String sql = "Insert into " + tableName + " values (";
						for (int i = 0; i < tokens.length; i++) {
							if (tokens[i].equals("t")) {
								tokens[i] = "1";
							} else if (tokens[i].equals("f")) {
								tokens[i] = "0";
							} else if (Character.isDigit(tokens[i].charAt(0)) == false
									|| tokens[i].indexOf("-") > -1) {
								System.out.println(tokens[i]);
								tokens[i] = "\'" + tokens[i] + "\'";
							}
							// Print all tokens
							sql += tokens[i];
							if (i != tokens.length - 1) {
								sql += ",";
							}
						}
						sql += ");";
						System.out.println(sql);
						stmt.executeUpdate(sql);

					}
					numLines++;

				}
			} catch (Exception e) {
				e.printStackTrace();
			} finally {
				try {
					fileReader.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}

			stmt.close();
			c.close();

		} catch (Exception e) {
			System.err.println(e.getClass().getName() + ": " + e.getMessage());
			System.exit(0);
		}

	}

}
