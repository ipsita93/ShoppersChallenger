import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.Date;

public class Parser {
	public static void main(String[] args) {

		String offers_filename = "";
		String transactions_filename = "";
		String trainHistory_filename = "";
		String testHistory_filename = "";

		String env = System.getProperty("os.name");
		if (env.contains("indows")) {
			offers_filename = "C:\\Users\\Sony\\Google Drive\\NUS\\8 - Year 4 Sem 2\\CS4225 Massive Data Processing Techniques in Data Science\\Project\\ShoppersChallenge\\offers\\offers.csv";
			transactions_filename = "C:\\Users\\Sony\\Google Drive\\NUS\\8 - Year 4 Sem 2\\CS4225 Massive Data Processing Techniques in Data Science\\Project\\ShoppersChallenge\\transactions\\";
			trainHistory_filename = "C:\\Users\\Sony\\Google Drive\\NUS\\8 - Year 4 Sem 2\\CS4225 Massive Data Processing Techniques in Data Science\\Project\\ShoppersChallenge\\trainHistory\\trainHistory1.csv";
			testHistory_filename = "C:\\Users\\Sony\\Google Drive\\NUS\\8 - Year 4 Sem 2\\CS4225 Massive Data Processing Techniques in Data Science\\Project\\ShoppersChallenge\\testHistory\\testHistory.csv";
		} else {
			offers_filename = "./offers/offers.csv";
			transactions_filename = "./transactions/transactions.csv";
			trainHistory_filename = "./trainHistory/trainHistory.csv";
			testHistory_filename = "./testHistor/testHistory.csv";

		}// Step 1:
		// populateOffersTable(offers_filename, "offers");

		// Step 2:
		// populateHistoryTable(trainHistory_filename, "trainHistory");
		// Step 3:
		// populateHistoryTable(testHistory_filename, "testHistory");

		// Step 4:

		//populateCustomersTable();
		//addNewFeaturesCustomersTable("customerstrain");
		//addNewFeaturesCustomersTable("customerstest");

		// Step 5:
		/*
		 * String file = "x"; for (int i = 1; i <= 22; i++) { file += i +
		 * ".csv"; // System.out.println(transactions_filename + file);
		 * populateNewFeaturesCustomersTable(transactions_filename + file); file
		 * = "x"; }
		 */

		populateNewFeaturesCustomersTable(transactions_filename + "xaa");
		
		System.out.println("Parsing done.");
	}

	public static void addNewFeaturesCustomersTable(String tablename) {
		Connection c = null;
		Statement stmt = null;
		try {

			Class.forName("com.mysql.jdbc.Driver").newInstance();
			c = DriverManager
					.getConnection("jdbc:mysql://kivstudymockdb.cy4xjdg7ghgd.us-east-1.rds.amazonaws.com:3306/shoppers?user=root&password=qwerty123");
			System.out.println("Opened database successfully.");
			stmt = c.createStatement();

			String sql = "alter table " + tablename
					+ " add column has_bought_company_before_q int default 0;";
			stmt.executeUpdate(sql);
			sql = "alter table "
					+ tablename
					+ " add column has_bought_company_before_bool bit(1) default 0;";
			stmt.executeUpdate(sql);
			sql = "alter table "
					+ tablename
					+ " add column has_bought_company_before_a float default 0.0;";
			stmt.executeUpdate(sql);
			sql = "alter table "
					+ tablename
					+ " add column has_returned_company_before_q int default 0;";
			stmt.executeUpdate(sql);
			sql = "alter table "
					+ tablename
					+ " add column has_returned_company_before_bool bit(1) default 0;";
			stmt.executeUpdate(sql);

			sql = "alter table " + tablename
					+ " add column has_bought_brand_before_q int default 0;";
			stmt.executeUpdate(sql);
			sql = "alter table "
					+ tablename
					+ " add column has_bought_brand_before_bool bit(1) default 0;";
			stmt.executeUpdate(sql);
			sql = "alter table "
					+ tablename
					+ " add column has_bought_brand_before_a float default 0.0;";
			stmt.executeUpdate(sql);
			sql = "alter table " + tablename
					+ " add column has_returned_brand_before_q int default 0;";
			stmt.executeUpdate(sql);
			sql = "alter table "
					+ tablename
					+ " add column has_returned_brand_before_bool bit(1) default 0;";
			stmt.executeUpdate(sql);

			sql = "alter table " + tablename
					+ " add column has_bought_category_before_q int default 0;";
			stmt.executeUpdate(sql);
			sql = "alter table "
					+ tablename
					+ " add column has_bought_category_before_bool bit(1) default 0;";
			stmt.executeUpdate(sql);
			sql = "alter table "
					+ tablename
					+ " add column has_bought_category_before_a float default 0.0;";
			stmt.executeUpdate(sql);
			sql = "alter table "
					+ tablename
					+ " add column has_returned_category_before_q int default 0;";
			stmt.executeUpdate(sql);
			sql = "alter table "
					+ tablename
					+ " add column has_returned_category_before_bool bit(1) default 0;";
			stmt.executeUpdate(sql);

			sql = "alter table "
					+ tablename
					+ " add column has_bought_company_brand_category_before_q int default 0;";
			stmt.executeUpdate(sql);
			sql = "alter table "
					+ tablename
					+ " add column has_bought_company_brand_category_before_bool bit(1) default 0;";
			stmt.executeUpdate(sql);
			sql = "alter table "
					+ tablename
					+ " add column has_bought_company_brand_category_before_a float default 0.0;";
			stmt.executeUpdate(sql);
			sql = "alter table "
					+ tablename
					+ " add column has_returned_company_brand_category_before_q int default 0;";
			stmt.executeUpdate(sql);
			sql = "alter table "
					+ tablename
					+ " add column has_returned_company_brand_category_before_bool bit(1) default 0;";
			stmt.executeUpdate(sql);

			sql = "alter table "
					+ tablename
					+ " add column last_purchase_date_company_brand_category date;";
			stmt.executeUpdate(sql);
			sql = "alter table "
					+ tablename
					+ " add column num_days_since_last_purchase_transaction int default 0;";
			stmt.executeUpdate(sql);

			sql = "alter table "
					+ tablename
					+ " add column has_bought_same_category_different_company_q bit(1) default 0;";
			stmt.executeUpdate(sql);
			sql = "alter table "
					+ tablename
					+ " add column has_bought_same_company_different_category_q bit(1) default 0;";
			stmt.executeUpdate(sql);

			stmt.close();
			c.close();
		} catch (Exception e) {
			System.err.println(e.getClass().getName() + ": " + e.getMessage());
			System.exit(0);
		}

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

				int numLines = 0;
				// Read the file line by line
				while ((line = fileReader.readLine()) != null) {
					if (numLines != 0) {
						// Get all tokens available in line
						String[] tokens = line.split(DELIMITER);

						/*
						 * c.has_bought_company_before_q,
						 * has_bought_company_before_bool
						 * ,has_bought_company_before_a
						 * ,has_returned_company_before_q
						 * ,has_returned_company_before_bool
						 */
						System.out.println("Attribute 1-5");
						String sql = "select c.has_bought_company_before_q, c.has_bought_company_before_bool, c.has_bought_company_before_a, c.has_returned_company_before_q, c.has_returned_company_before_bool from customerstrain c"
								+ " where c.customer_id = "
								+ tokens[0]
								+ " and c.company_id = " + tokens[4] + ";";
						ResultSet rs = stmt.executeQuery(sql);
						int has_bought_company_before_q = 0;
						int has_bought_company_before_bool = 1;
						float has_bought_company_before_a = 0;
						int has_returned_company_before_q = 0;
						int has_returned_company_before_bool = 0;
						while (rs.next()) {
							has_bought_company_before_q = Integer.getInteger(rs
									.getString("has_bought_company_before_q")) + 1;
							has_bought_company_before_a = Integer.getInteger(rs
									.getString("has_bought_company_before_a"))
									+ Integer.getInteger(tokens[10]);
							if (Integer
									.getInteger(rs
											.getString("has_returned_company_before_q")) < 0) {
								has_returned_company_before_q = Integer
										.getInteger(rs
												.getString("has_returned_company_before_q"))
										- Integer.getInteger(tokens[9]);
								has_returned_company_before_bool = 1;
							}
						}
						sql = "update customerstrain c "
								+ "set c.has_bought_company_before_q = "
								+ has_bought_company_before_q
								+ " , c.has_bought_company_before_bool = "
								+ has_bought_company_before_bool
								+ " , c.has_bought_company_before_a = "
								+ has_bought_company_before_a
								+ " , c.has_returned_company_before_q = "
								+ has_returned_company_before_q
								+ " , has_returned_company_before_bool = "
								+ has_returned_company_before_bool
								+ " where c.customer_id = " + tokens[0]
								+ " and c.company_id = " + tokens[4] + ";";
						//System.out.println(sql);
						stmt.executeUpdate(sql);
						/*
						 * c.has_bought_brand_before_q,
						 * has_bought_brand_before_bool
						 * ,has_bought_brand_before_a
						 * ,has_returned_brand_before_q
						 * ,has_returned_brand_before_bool
						 */
						System.out.println("Attribute 6-10");
						sql = "select c.has_bought_brand_before_q, c.has_bought_brand_before_bool, c.has_bought_brand_before_a, c.has_returned_brand_before_q, c.has_returned_brand_before_bool from customerstrain c"
								+ " where c.customer_id = "
								+ tokens[0]
								+ " and c.company_id = "
								+ tokens[4]
								+ " and c.brand_id = " + tokens[5] + ";";
						rs = stmt.executeQuery(sql);
						int has_bought_brand_before_q = 0;
						int has_bought_brand_before_bool = 1;
						float has_bought_brand_before_a = 0;
						int has_returned_brand_before_q = 0;
						int has_returned_brand_before_bool = 0;
						while (rs.next()) {
							has_bought_brand_before_q = Integer.getInteger(rs
									.getString("has_bought_brand_before_q")) + 1;
							has_bought_brand_before_a = Integer.getInteger(rs
									.getString("has_bought_brand_before_a"))
									+ Integer.getInteger(tokens[10]);
							if (Integer.getInteger(rs
									.getString("has_returned_brand_before_q")) < 0) {
								has_returned_brand_before_q = Integer
										.getInteger(rs
												.getString("has_returned_brand_before_q"))
										- Integer.getInteger(tokens[14]);
								has_returned_brand_before_bool = 1;
							}
						}
						sql = "update customerstrain c "
								+ "set c.has_bought_brand_before_q = "
								+ has_bought_brand_before_q
								+ " , c.has_bought_brand_before_bool = "
								+ has_bought_brand_before_bool
								+ " , c.has_bought_brand_before_a = "
								+ has_bought_brand_before_a
								+ " , c.has_returned_brand_before_q = "
								+ has_returned_brand_before_q
								+ " , has_returned_brand_before_bool = "
								+ has_returned_brand_before_bool
								+ " where c.customer_id = " + tokens[0]
								+ " and c.company_id = " + tokens[4]
								+ " and c.brand_id = " + tokens[5] + ";";
						//System.out.println(sql);
						stmt.executeUpdate(sql);

						/*
						 * c.has_bought_category_before_q,
						 * has_bought_category_before_bool
						 * ,has_bought_category_before_a
						 * ,has_returned_category_before_q
						 * ,has_returned_category_before_bool
						 */

						System.out.println("Attribute 11-15");
						sql = "select c.has_bought_category_before_q, c.has_bought_category_before_bool, c.has_bought_category_before_a, c.has_returned_category_before_q, c.has_returned_category_before_bool from customerstrain c"
								+ " where c.customer_id = "
								+ tokens[0]
								+ " and c.category = " + tokens[3] + ";";
						rs = stmt.executeQuery(sql);
						int has_bought_category_before_q = 0;
						int has_bought_category_before_bool = 1;
						float has_bought_category_before_a = 0;
						int has_returned_category_before_q = 0;
						int has_returned_category_before_bool = 0;
						while (rs.next()) {
							has_bought_category_before_q = Integer
									.getInteger(rs
											.getString("has_bought_category_before_q")) + 1;
							has_bought_category_before_a = Integer
									.getInteger(rs
											.getString("has_bought_category_before_a"))
									+ Integer.getInteger(tokens[10]);
							if (Integer
									.getInteger(rs
											.getString("has_returned_category_before_q")) < 0) {
								has_returned_category_before_q = Integer
										.getInteger(rs
												.getString("has_returned_category_before_q"))
										- Integer.getInteger(tokens[9]);
								has_returned_category_before_bool = 1;
							}
						}
						sql = "update customerstrain c "
								+ "set c.has_bought_category_before_q = "
								+ has_bought_category_before_q
								+ " , c.has_bought_category_before_bool = "
								+ has_bought_category_before_bool
								+ " , c.has_bought_category_before_a = "
								+ has_bought_category_before_a
								+ " , c.has_returned_category_before_q = "
								+ has_returned_category_before_q
								+ " , has_returned_category_before_bool = "
								+ has_returned_category_before_bool
								+ " where c.customer_id = " + tokens[0]
								+ " and c.category = " + tokens[3] + ";";
						//System.out.println(sql);
						stmt.executeUpdate(sql);
						/*
						 * c.has_bought_company_brand_category_before_q,
						 * has_bought_company_brand_category_before_bool
						 * ,has_bought_company_brand_category_before_a
						 * ,has_returned_company_brand_category_before_q
						 * ,has_returned_company_brand_category_before_bool
						 */

						System.out.println("Attribute 16-20");
						sql = "select c.has_bought_company_brand_category_before_q, c.has_bought_company_brand_category_before_bool, c.has_bought_company_brand_category_before_a, c.has_returned_company_brand_category_before_q, c.has_returned_company_brand_category_before_bool from customerstrain c"
								+ " where c.customer_id = "
								+ tokens[0]
								+ " and c.category = "
								+ tokens[3]
								+ " and c.company_id = "
								+ tokens[4]
								+ " and c.brand_id = " + tokens[5] + ";";
						rs = stmt.executeQuery(sql);
						int has_bought_company_brand_category_before_q = 0;
						int has_bought_company_brand_category_before_bool = 1;
						float has_bought_company_brand_category_before_a = 0;
						int has_returned_company_brand_category_before_q = 0;
						int has_returned_company_brand_category_before_bool = 0;
						while (rs.next()) {
							has_bought_company_brand_category_before_q = Integer
									.getInteger(rs
											.getString("has_bought_company_brand_category_before_q")) + 1;
							has_bought_company_brand_category_before_a = Integer
									.getInteger(rs
											.getString("has_bought_company_brand_category_before_a"))
									+ Integer.getInteger(tokens[10]);
							if (Integer
									.getInteger(rs
											.getString("has_returned_company_brand_category_before_q")) < 0) {
								has_returned_company_brand_category_before_q = Integer
										.getInteger(rs
												.getString("has_returned_company_brand_category_before_q"))
										- Integer.getInteger(tokens[9]);
								has_returned_company_brand_category_before_bool = 1;
							}
						}
						sql = "update customerstrain c "
								+ "set c.has_bought_company_brand_category_before_q = "
								+ has_bought_company_brand_category_before_q
								+ " , c.has_bought_company_brand_category_before_bool = "
								+ has_bought_company_brand_category_before_bool
								+ " , c.has_bought_company_brand_category_before_a = "
								+ has_bought_company_brand_category_before_a
								+ " , c.has_returned_company_brand_category_before_q = "
								+ has_returned_company_brand_category_before_q
								+ " , has_returned_company_brand_category_before_bool = "
								+ has_returned_company_brand_category_before_bool
								+ " where c.customer_id = " + tokens[0]
								+ " and c.category = " + tokens[3]
								+ " and c.company_id = " + tokens[4]
								+ " and c.brand_id = " + tokens[5] + ";";
						//System.out.println(sql);
						stmt.executeUpdate(sql);

						/*
						 * last_purchase_date_company_brand_category,
						 * num_days_since_last_purchase_transaction
						 */
						System.out.println("Attribute 21-22");
						sql = "select c.customer_id, offerdate, last_purchase_date_company_brand_category from customerstrain c where c.customer_id = "
								+ tokens[0]
								+ " and c.company_id = "
								+ tokens[4]
								+ " and c.brand_id = "
								+ tokens[5]
								+ " and c.category = " + tokens[3] + ";";
						//System.out.println(sql);
						rs = stmt.executeQuery(sql);
						Date last_purchase_date = new Date();
						Date offer_date = new Date();
						SimpleDateFormat formatter = new SimpleDateFormat(
								"yyyy-mm-dd");
						String dateString = "";

						int customer_id = 0;
						//System.out.println(last_purchase_date);
						while (rs.next()) {
							customer_id = Integer.getInteger(rs
									.getString("customer_id"));
							dateString = rs
									.getString("last_purchase_date_company_brand_category");
							last_purchase_date = formatter.parse(dateString);
							dateString = rs.getString("offerdate");
							offer_date = formatter.parse(dateString);
						}

						Date current_date = formatter.parse(tokens[6]);
						if (customer_id != 0
								&& current_date.after(last_purchase_date)
								&& Integer.getInteger(tokens[9]) > 0) {

							long diff = offer_date.getTime()
									- current_date.getTime();
							int num_days_since_last_purchase_transaction = (int) (diff / (24 * 60 * 60 * 1000));

							// update customer
							sql = "update customer c "
									+ "set c.last_purchase_date_company_brand_category = '"
									+ tokens[6]
									+ "', "
									+ "c.num_days_since_last_purchase_transaction = "
									+ num_days_since_last_purchase_transaction
									+ " where c.customer_id = " + customer_id
									+ " and c.company_id = " + tokens[4]
									+ " and c.brand_id = " + tokens[5]
									+ " and c.category = " + tokens[3] + ";";
							//System.out.println(sql);
							stmt.executeUpdate(sql);
						}

						/* has_bought_same_category_different_company_q */
						System.out.println("Attribute 23");
						sql = "select c.customer_id, has_bought_same_category_different_company_q from customerstrain c where c.customer_id = "
								+ tokens[0]
								+ " and c.category = "
								+ tokens[3]
								+ " and c.company_id <> " + tokens[4] + ";";
						//System.out.println(sql);
						rs = stmt.executeQuery(sql);

						customer_id = 0;
						int has_bought_same_category_different_company_q = 0;
						while (rs.next()) {
							customer_id = Integer.getInteger(rs
									.getString("customer_id"));
							has_bought_same_category_different_company_q = Integer
									.getInteger(rs
											.getString("has_bought_same_category_different_company_q"));

						}
						if (customer_id != 0) {
							// update customer
							sql = "update customerstrain c "
									+ "set has_bought_same_category_different_company_q = "
									+ (has_bought_same_category_different_company_q + 1)
									+ " where c.customer_id = " + customer_id
									+ " and c.category = " + tokens[3]
									+ " and c.company_id <> " + tokens[4] + ";";
						}

						/* has_bought_same_company_different_category_q */
						System.out.println("Attribute 24");
						sql = "select c.customer_id, has_bought_same_company_different_category_q from customerstrain c where c.customer_id = "
								+ tokens[0]
								+ " and c.company_id = "
								+ tokens[4]
								+ " and c.category <> "
								+ tokens[3]
								+ ";";
						//System.out.println(sql);
						rs = stmt.executeQuery(sql);

						customer_id = 0;
						int has_bought_same_company_different_category_q = 0;
						while (rs.next()) {
							customer_id = Integer.getInteger(rs
									.getString("customer_id"));
							has_bought_same_company_different_category_q = Integer
									.getInteger(rs
											.getString("has_bought_same_company_different_category_q"));

						}
						if (customer_id != 0) {
							// update customer
							sql = "update customerstrain c "
									+ "set has_bought_same_company_different_category_q = "
									+ (has_bought_same_company_different_category_q + 1)
									+ " where c.customer_id = " + customer_id
									+ " and c.company_id = " + tokens[4]
									+ " and c.category <> " + tokens[3] + ";";
						}

						/*
						 * ######################################################
						 * CUSTOMERSTEST
						 */

						/*
						 * c.has_bought_company_before_q,
						 * has_bought_company_before_bool
						 * ,has_bought_company_before_a
						 * ,has_returned_company_before_q
						 * ,has_returned_company_before_bool
						 */
						System.out.println("Attribute 1-5");
						sql = "select c.has_bought_company_before_q, c.has_bought_company_before_bool, c.has_bought_company_before_a, c.has_returned_company_before_q, c.has_returned_company_before_bool from customerstest c"
								+ " where c.customer_id = "
								+ tokens[0]
								+ " and c.company_id = " + tokens[4] + ";";
						rs = stmt.executeQuery(sql);
						has_bought_company_before_q = 0;
						has_bought_company_before_bool = 1;
						has_bought_company_before_a = 0;
						has_returned_company_before_q = 0;
						has_returned_company_before_bool = 0;
						while (rs.next()) {
							has_bought_company_before_q = Integer.getInteger(rs
									.getString("has_bought_company_before_q")) + 1;
							has_bought_company_before_a = Integer.getInteger(rs
									.getString("has_bought_company_before_a"))
									+ Integer.getInteger(tokens[10]);
							if (Integer
									.getInteger(rs
											.getString("has_returned_company_before_q")) < 0) {
								has_returned_company_before_q = Integer
										.getInteger(rs
												.getString("has_returned_company_before_q"))
										- Integer.getInteger(tokens[9]);
								has_returned_company_before_bool = 1;
							}
						}
						sql = "update customerstest c "
								+ "set c.has_bought_company_before_q = "
								+ has_bought_company_before_q
								+ " , c.has_bought_company_before_bool = "
								+ has_bought_company_before_bool
								+ " , c.has_bought_company_before_a = "
								+ has_bought_company_before_a
								+ " , c.has_returned_company_before_q = "
								+ has_returned_company_before_q
								+ " , has_returned_company_before_bool = "
								+ has_returned_company_before_bool
								+ " where c.customer_id = " + tokens[0]
								+ " and c.company_id = " + tokens[4] + ";";
						//System.out.println(sql);
						stmt.executeUpdate(sql);
						/*
						 * c.has_bought_brand_before_q,
						 * has_bought_brand_before_bool
						 * ,has_bought_brand_before_a
						 * ,has_returned_brand_before_q
						 * ,has_returned_brand_before_bool
						 */
						System.out.println("Attribute 6-10");
						sql = "select c.has_bought_brand_before_q, c.has_bought_brand_before_bool, c.has_bought_brand_before_a, c.has_returned_brand_before_q, c.has_returned_brand_before_bool from customerstest c"
								+ " where c.customer_id = "
								+ tokens[0]
								+ " and c.company_id = "
								+ tokens[4]
								+ " and c.brand_id = " + tokens[5] + ";";
						rs = stmt.executeQuery(sql);
						has_bought_brand_before_q = 0;
						has_bought_brand_before_bool = 1;
						has_bought_brand_before_a = 0;
						has_returned_brand_before_q = 0;
						has_returned_brand_before_bool = 0;
						while (rs.next()) {
							has_bought_brand_before_q = Integer.getInteger(rs
									.getString("has_bought_brand_before_q")) + 1;
							has_bought_brand_before_a = Integer.getInteger(rs
									.getString("has_bought_brand_before_a"))
									+ Integer.getInteger(tokens[10]);
							if (Integer.getInteger(rs
									.getString("has_returned_brand_before_q")) < 0) {
								has_returned_brand_before_q = Integer
										.getInteger(rs
												.getString("has_returned_brand_before_q"))
										- Integer.getInteger(tokens[14]);
								has_returned_brand_before_bool = 1;
							}
						}
						sql = "update customerstest c "
								+ "set c.has_bought_brand_before_q = "
								+ has_bought_brand_before_q
								+ " , c.has_bought_brand_before_bool = "
								+ has_bought_brand_before_bool
								+ " , c.has_bought_brand_before_a = "
								+ has_bought_brand_before_a
								+ " , c.has_returned_brand_before_q = "
								+ has_returned_brand_before_q
								+ " , has_returned_brand_before_bool = "
								+ has_returned_brand_before_bool
								+ " where c.customer_id = " + tokens[0]
								+ " and c.company_id = " + tokens[4]
								+ " and c.brand_id = " + tokens[5] + ";";
						//System.out.println(sql);
						stmt.executeUpdate(sql);

						/*
						 * c.has_bought_category_before_q,
						 * has_bought_category_before_bool
						 * ,has_bought_category_before_a
						 * ,has_returned_category_before_q
						 * ,has_returned_category_before_bool
						 */

						System.out.println("Attribute 11-15");
						sql = "select c.has_bought_category_before_q, c.has_bought_category_before_bool, c.has_bought_category_before_a, c.has_returned_category_before_q, c.has_returned_category_before_bool from customerstest c"
								+ " where c.customer_id = "
								+ tokens[0]
								+ " and c.category = " + tokens[3] + ";";
						rs = stmt.executeQuery(sql);
						has_bought_category_before_q = 0;
						has_bought_category_before_bool = 1;
						has_bought_category_before_a = 0;
						has_returned_category_before_q = 0;
						has_returned_category_before_bool = 0;
						while (rs.next()) {
							has_bought_category_before_q = Integer
									.getInteger(rs
											.getString("has_bought_category_before_q")) + 1;
							has_bought_category_before_a = Integer
									.getInteger(rs
											.getString("has_bought_category_before_a"))
									+ Integer.getInteger(tokens[10]);
							if (Integer
									.getInteger(rs
											.getString("has_returned_category_before_q")) < 0) {
								has_returned_category_before_q = Integer
										.getInteger(rs
												.getString("has_returned_category_before_q"))
										- Integer.getInteger(tokens[9]);
								has_returned_category_before_bool = 1;
							}
						}
						sql = "update customerstest c "
								+ "set c.has_bought_category_before_q = "
								+ has_bought_category_before_q
								+ " , c.has_bought_category_before_bool = "
								+ has_bought_category_before_bool
								+ " , c.has_bought_category_before_a = "
								+ has_bought_category_before_a
								+ " , c.has_returned_category_before_q = "
								+ has_returned_category_before_q
								+ " , has_returned_category_before_bool = "
								+ has_returned_category_before_bool
								+ " where c.customer_id = " + tokens[0]
								+ " and c.category = " + tokens[3] + ";";
						//System.out.println(sql);
						stmt.executeUpdate(sql);
						/*
						 * c.has_bought_company_brand_category_before_q,
						 * has_bought_company_brand_category_before_bool
						 * ,has_bought_company_brand_category_before_a
						 * ,has_returned_company_brand_category_before_q
						 * ,has_returned_company_brand_category_before_bool
						 */

						System.out.println("Attribute 16-20");
						sql = "select c.has_bought_company_brand_category_before_q, c.has_bought_company_brand_category_before_bool, c.has_bought_company_brand_category_before_a, c.has_returned_company_brand_category_before_q, c.has_returned_company_brand_category_before_bool from customerstest c"
								+ " where c.customer_id = "
								+ tokens[0]
								+ " and c.category = "
								+ tokens[3]
								+ " and c.company_id = "
								+ tokens[4]
								+ " and c.brand_id = " + tokens[5] + ";";
						rs = stmt.executeQuery(sql);
						has_bought_company_brand_category_before_q = 0;
						has_bought_company_brand_category_before_bool = 1;
						has_bought_company_brand_category_before_a = 0;
						has_returned_company_brand_category_before_q = 0;
						has_returned_company_brand_category_before_bool = 0;
						while (rs.next()) {
							has_bought_company_brand_category_before_q = Integer
									.getInteger(rs
											.getString("has_bought_company_brand_category_before_q")) + 1;
							has_bought_company_brand_category_before_a = Integer
									.getInteger(rs
											.getString("has_bought_company_brand_category_before_a"))
									+ Integer.getInteger(tokens[10]);
							if (Integer
									.getInteger(rs
											.getString("has_returned_company_brand_category_before_q")) < 0) {
								has_returned_company_brand_category_before_q = Integer
										.getInteger(rs
												.getString("has_returned_company_brand_category_before_q"))
										- Integer.getInteger(tokens[9]);
								has_returned_company_brand_category_before_bool = 1;
							}
						}
						sql = "update customerstest c "
								+ "set c.has_bought_company_brand_category_before_q = "
								+ has_bought_company_brand_category_before_q
								+ " , c.has_bought_company_brand_category_before_bool = "
								+ has_bought_company_brand_category_before_bool
								+ " , c.has_bought_company_brand_category_before_a = "
								+ has_bought_company_brand_category_before_a
								+ " , c.has_returned_company_brand_category_before_q = "
								+ has_returned_company_brand_category_before_q
								+ " , has_returned_company_brand_category_before_bool = "
								+ has_returned_company_brand_category_before_bool
								+ " where c.customer_id = " + tokens[0]
								+ " and c.category = " + tokens[3]
								+ " and c.company_id = " + tokens[4]
								+ " and c.brand_id = " + tokens[5] + ";";
						//System.out.println(sql);
						stmt.executeUpdate(sql);

						/*
						 * last_purchase_date_company_brand_category,
						 * num_days_since_last_purchase_transaction
						 */
						System.out.println("Attribute 21-22");
						sql = "select c.customer_id, offerdate, last_purchase_date_company_brand_category from customerstest c where c.customer_id = "
								+ tokens[0]
								+ " and c.company_id = "
								+ tokens[4]
								+ " and c.brand_id = "
								+ tokens[5]
								+ " and c.category = " + tokens[3] + ";";
						//System.out.println(sql);
						rs = stmt.executeQuery(sql);
						last_purchase_date = new Date();
						offer_date = new Date();
						formatter = new SimpleDateFormat("yyyy-mm-dd");
						dateString = "";

						customer_id = 0;
						//System.out.println(last_purchase_date);
						while (rs.next()) {
							customer_id = Integer.getInteger(rs
									.getString("customer_id"));
							dateString = rs
									.getString("last_purchase_date_company_brand_category");
							last_purchase_date = formatter.parse(dateString);
							dateString = rs.getString("offerdate");
							offer_date = formatter.parse(dateString);
						}

						current_date = formatter.parse(tokens[6]);
						if (customer_id != 0
								&& current_date.after(last_purchase_date)
								&& Integer.getInteger(tokens[9]) > 0) {

							long diff = offer_date.getTime()
									- current_date.getTime();
							int num_days_since_last_purchase_transaction = (int) (diff / (24 * 60 * 60 * 1000));

							// update customer
							sql = "update customer c "
									+ "set c.last_purchase_date_company_brand_category = '"
									+ tokens[6]
									+ "', "
									+ "c.num_days_since_last_purchase_transaction = "
									+ num_days_since_last_purchase_transaction
									+ " where c.customer_id = " + customer_id
									+ " and c.company_id = " + tokens[4]
									+ " and c.brand_id = " + tokens[5]
									+ " and c.category = " + tokens[3] + ";";
							//System.out.println(sql);
							stmt.executeUpdate(sql);
						}

						/* has_bought_same_category_different_company_q */
						System.out.println("Attribute 23");
						sql = "select c.customer_id, has_bought_same_category_different_company_q from customerstest c where c.customer_id = "
								+ tokens[0]
								+ " and c.category = "
								+ tokens[3]
								+ " and c.company_id <> " + tokens[4] + ";";
						//System.out.println(sql);
						rs = stmt.executeQuery(sql);

						customer_id = 0;
						has_bought_same_category_different_company_q = 0;
						while (rs.next()) {
							customer_id = Integer.getInteger(rs
									.getString("customer_id"));
							has_bought_same_category_different_company_q = Integer
									.getInteger(rs
											.getString("has_bought_same_category_different_company_q"));

						}
						if (customer_id != 0) {
							// update customer
							sql = "update customerstest c "
									+ "set has_bought_same_category_different_company_q = "
									+ (has_bought_same_category_different_company_q + 1)
									+ " where c.customer_id = " + customer_id
									+ " and c.category = " + tokens[3]
									+ " and c.company_id <> " + tokens[4] + ";";
						}

						/* has_bought_same_company_different_category_q */
						System.out.println("Attribute 24");
						sql = "select c.customer_id, has_bought_same_company_different_category_q from customerstest c where c.customer_id = "
								+ tokens[0]
								+ " and c.company_id = "
								+ tokens[4]
								+ " and c.category <> "
								+ tokens[3]
								+ ";";
						//System.out.println(sql);
						rs = stmt.executeQuery(sql);

						customer_id = 0;
						has_bought_same_company_different_category_q = 0;
						while (rs.next()) {
							customer_id = Integer.getInteger(rs
									.getString("customer_id"));
							has_bought_same_company_different_category_q = Integer
									.getInteger(rs
											.getString("has_bought_same_company_different_category_q"));

						}
						if (customer_id != 0) {
							// update customer
							sql = "update customerstest c "
									+ "set has_bought_same_company_different_category_q = "
									+ (has_bought_same_company_different_category_q + 1)
									+ " where c.customer_id = " + customer_id
									+ " and c.company_id = " + tokens[4]
									+ " and c.category <> " + tokens[3] + ";";
						}

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

			String sql = "CREATE TABLE customerstrain AS (SELECT trainHistory.*, offers.category, offers.quantity, offers.company_id, offers.offervalue, offers.brand_id FROM   trainHistory INNER JOIN offers ON trainHistory.offer_id = offers.offer_id);";
			stmt.executeUpdate(sql);

			sql = "CREATE TABLE customerstest AS (SELECT testHistory.*, offers.category, offers.quantity, offers.company_id, offers.offervalue, offers.brand_id FROM   testHistory INNER JOIN offers ON testHistory.offer_id = offers.offer_id);";
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
						//System.out.println(sql);
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

	public static void populateHistoryTable(String fileName, String tableName) {
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
