import mysql.connector

loc_reduced_transactions = "C:\\Users\\Sony\\Google Drive\\NUS\\8 - Year 4 Sem 2\\CS4225 Massive Data Processing Techniques in Data Science\\Project\\ShoppersChallenge\\reduced3.csv"
	
def preprocess_data(loc_reduced_transactions):
	cnx = mysql.connector.connect(user='root', password='qwerty123',
                              host='kivstudymockdb.cy4xjdg7ghgd.us-east-1.rds.amazonaws.com',
                              database='shoppers')
	cursor = cnx.cursor()
  	#for numbered (enumerated) line in reduced_transactions file
  	for e, line in enumerate( open(loc_reduced_transactions) ):
  		if e == 0:
  			continue
  		tokens = line.split(",")
  		tokens[10] = tokens[10].strip()
  		print(tokens)
	  	#index:column_name
	  	#0:id
	  	#1:chain
	  	#2:dept
	  	#3:category
	  	#4:company
	  	#5:brand
	  	#6:date
	  	#7:productsize	
	  	#8:productmeasure
	  	#9:productquantity
	  	#10:purchaseamount
	  	'''
	  	c.has_bought_company_before_q,
	  	has_bought_company_before_bool
	  	,has_bought_company_before_a
	  	,has_returned_company_before_q
	  	,has_returned_company_before_bool
	  	'''
	  	print("Attribute 1-5")
		query = ("select c.has_bought_company_before_q, c.has_bought_company_before_bool, c.has_bought_company_before_a, c.has_returned_company_before_q, c.has_returned_company_before_bool from customerstrain c"
				" where c.customer_id > %s and c.company_id = %s;")
		cursor.execute(query, (tokens[0], tokens[4]))
		my_has_bought_company_before_q = 0;
		my_has_bought_company_before_bool = 1;
		my_has_bought_company_before_a = 0;
		my_has_returned_company_before_q = 0;
		my_has_returned_company_before_bool = 0;

		for (has_bought_company_before_q, has_bought_company_before_bool, has_bought_company_before_a, has_returned_company_before_q, has_returned_company_before_bool) in cursor:
			my_has_bought_company_before_q = has_bought_company_before_q + 1

			my_has_bought_company_before_a = has_bought_company_before_a + float(tokens[10])
			if (has_returned_company_before_q < 0):
				my_has_returned_company_before_q = has_returned_company_before_q - tokens[9]
				my_has_returned_company_before_bool = 1;

				query = ("update customerstrain c set c.has_bought_company_before_q = %s , c.has_bought_company_before_bool = %s , c.has_bought_company_before_a = %s , c.has_returned_company_before_q = %s , has_returned_company_before_bool = %s where c.customer_id = %s and c.company_id = %s;")
				cursor.execute(query, (my_has_bought_company_before_q, my_has_bought_company_before_bool, my_has_bought_company_before_a, my_has_returned_company_before_q, my_has_returned_company_before_bool, tokens[0], tokens[4]))

		'''
	  	c.has_bought_brand_before_q,
	  	has_bought_brand_before_bool
	  	,has_bought_brand_before_a
	  	,has_returned_brand_before_q
	  	,has_returned_brand_before_bool
	  	'''
	  	print("Attribute 6-10")
		query = ("select c.has_bought_brand_before_q, c.has_bought_brand_before_bool, c.has_bought_brand_before_a, c.has_returned_brand_before_q, c.has_returned_brand_before_bool from customerstrain c"
				 " where c.customer_id = %s and c.company_id = %s and c.brand_id = %s;")
		cursor.execute(query, (tokens[0], tokens[4], tokens[5]))
		my_has_bought_brand_before_q = 0;
		my_has_bought_brand_before_bool = 1;
		my_has_bought_brand_before_a = 0;
		my_has_returned_brand_before_q = 0;
		my_has_returned_brand_before_bool = 0;

		for (has_bought_brand_before_q, has_bought_brand_before_bool, has_bought_brand_before_a, has_returned_brand_before_q, has_returned_brand_before_bool) in cursor:
			my_has_bought_brand_before_q = has_bought_brand_before_q + 1

			my_has_bought_brand_before_a = has_bought_brand_before_a + float(tokens[10])
			if (has_returned_brand_before_q < 0):
				my_has_returned_brand_before_q = has_returned_brand_before_q - tokens[9]
				my_has_returned_brand_before_bool = 1;

				query = ("update customerstrain c set c.has_bought_brand_before_q = %s , c.has_bought_brand_before_bool = %s , c.has_bought_brand_before_a = %s , c.has_returned_brand_before_q = %s , has_returned_brand_before_bool = %s where c.customer_id = %s and c.company_id= %s and c.brand_id = %s;")
				cursor.execute(query, (my_has_bought_brand_before_q, my_has_bought_brand_before_bool, my_has_bought_brand_before_a, my_has_returned_brand_before_q, my_has_returned_brand_before_bool, tokens[0], tokens[4], tokens[5]))

		'''
	  	c.has_bought_category_before_q,
	  	has_bought_category_before_bool
	  	,has_bought_category_before_a
	  	,has_returned_category_before_q
	  	,has_returned_category_before_bool
	  	'''
	  	print("Attribute 11-15")
		query = ("select c.has_bought_category_before_q, c.has_bought_category_before_bool, c.has_bought_category_before_a, c.has_returned_category_before_q, c.has_returned_category_before_bool from customerstrain c"
				 " where c.customer_id = %s and c.category = %s;")
		cursor.execute(query, (tokens[0], tokens[3]))
		my_has_bought_category_before_q = 0;
		my_has_bought_category_before_bool = 1;
		my_has_bought_category_before_a = 0;
		my_has_returned_category_before_q = 0;
		my_has_returned_category_before_bool = 0;

		for (has_bought_category_before_q, has_bought_category_before_bool, has_bought_category_before_a, has_returned_category_before_q, has_returned_category_before_bool) in cursor:
			my_has_bought_category_before_q = has_bought_category_before_q + 1

			my_has_bought_category_before_a = has_bought_category_before_a + float(tokens[10])
			if (has_returned_category_before_q < 0):
				my_has_returned_category_before_q = has_returned_category_before_q - tokens[9]
				my_has_returned_category_before_bool = 1;

				query = ("update customerstrain c set c.has_bought_category_before_q = %s , c.has_bought_category_before_bool = %s , c.has_bought_category_before_a = %s , c.has_returned_category_before_q = %s , has_returned_category_before_bool = %s where c.customer_id = %s and c.category_id = %s;")
				cursor.execute(query, (my_has_bought_category_before_q, my_has_bought_category_before_bool, my_has_bought_category_before_a, my_has_returned_category_before_q, my_has_returned_category_before_bool, tokens[0], tokens[3], tokens[5]))


				'''
				c.has_bought_company_brand_category_before_q,
				has_bought_company_brand_category_before_bool
				,has_bought_company_brand_category_before_a
				,has_returned_company_brand_category_before_q
				,has_returned_company_brand_category_before_bool
				'''
				print("Attribute 16-20")
				query = ("select c.has_bought_company_brand_category_before_q, c.has_bought_company_brand_category_before_bool, c.has_bought_company_brand_category_before_a, c.has_returned_company_brand_category_before_q, c.has_returned_company_brand_category_before_bool from customerstrain c"
					" where c.customer_id = %s and c.category = %s and c.company_id = %s and c.brand_id = %s;")
				cursor.execute(query, (tokens[0], tokens[3], tokens[4], tokens[5]))
				my_has_bought_company_brand_category_before_bool = 1;
				my_has_bought_company_brand_category_before_a = 0;
				my_has_returned_company_brand_category_before_q = 0;
				my_has_returned_company_brand_category_before_bool = 0;

				for (has_bought_company_brand_category_before_q, has_bought_company_brand_category_before_bool, has_bought_company_brand_category_before_a, has_returned_company_brand_category_before_q, has_returned_company_brand_category_before_bool) in cursor:
					my_has_bought_company_brand_category_before_q = has_bought_company_brand_category_before_q + 1

					my_has_bought_company_brand_category_before_a = has_bought_company_brand_category_before_a + float(tokens[10])
					if (has_returned_company_brand_category_before_q < 0):
						my_has_returned_company_brand_category_before_q = has_returned_company_brand_category_before_q - tokens[9]
						my_has_returned_company_brand_category_before_bool = 1;

						query = ("update customerstrain c set c.has_bought_company_brand_category_before_q = %s , c.has_bought_company_brand_category_before_bool = %s , c.has_bought_company_brand_category_before_a = %s , c.has_returned_company_brand_category_before_q = %s , has_returned_company_brand_category_before_bool = %s where c.customer_id = %s and c.company_brand_category_id = %s;")
						cursor.execute(query, (my_has_bought_company_brand_category_before_q, my_has_bought_company_brand_category_before_bool, my_has_bought_company_brand_category_before_a, my_has_returned_company_brand_category_before_q, my_has_returned_company_brand_category_before_bool, tokens[0], tokens[3], tokens[4], tokens[5]))



				print("Attribute 21-22")
				'''
				last_purchase_date_company_brand_category
				num_days_since_last_purchase_transaction
				'''




  	print("Preprocessing done")
  	
  	cursor.close()
	cnx.close()

preprocess_data(loc_reduced_transactions)





















'''
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

					

						

						/* has_bought_same_category_different_company_q */
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

'''
