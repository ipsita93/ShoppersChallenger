import mysql.connector
import sys 
import datetime, calendar
from datetime import datetime

loc_reduced_transactions = "C:\\Users\\Sony\\Google Drive\\NUS\\8 - Year 4 Sem 2\\CS4225 Massive Data Processing Techniques in Data Science\\Project\\ShoppersChallenge\\reduced2.csv"
	
def preprocess_data(loc_reduced_transactions):
	#start a counter for progress reporting
  	start = datetime.now()
  	print("Preprocessing started")
	print start

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
  		#print(tokens)
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
		########################################## CUSTOMERTRAIN ###############################################

	  	'''
	  	c.has_bought_company_before_q,
	  	has_bought_company_before_bool
	  	,has_bought_company_before_a
	  	,has_returned_company_before_q
	  	,has_returned_company_before_bool
	  	'''
	  	#print("Attribute 1-5")
		query = ("select c.has_bought_company_before_q, c.has_bought_company_before_bool, c.has_bought_company_before_a, c.has_returned_company_before_q, c.has_returned_company_before_bool from customerstrain c"
				" where c.customer_id = %s and c.company_id = %s;")
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
	  	#print("Attribute 6-10")
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
	  	#print("Attribute 11-15")
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
		#print("Attribute 16-20")
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



		'''
		last_purchase_date_company_brand_category
		num_days_since_last_purchase_transaction
		'''
		#print("Attribute 21-22")
		query = ("select c.customer_id, c.offerdate, c.last_purchase_date_company_brand_category from customerstrain c where c.customer_id = %s and c.company_id = %s and c.brand_id = %s and c.category = %s;")
		cursor.execute(query, (tokens[0], tokens[4], tokens[5], tokens[3]))

		my_customer_id = 0
		for (customer_id, offerdate, last_purchase_date_company_brand_category) in cursor:
			my_customer_id = customer_id
			dateString = last_purchase_date_company_brand_category
			#print(offerdate)
			my_offerdate = datetime.datetime.strptime(offerdate, "%Y-%m-%d")

			my_current_date = datetime.datetime.strptime(tokens[6], "%Y-%m-%d")
			if (customer_id != 0 & current_date > last_purchase_date & tokens[9] > 0):
				delta = current_date - last_purchase_date
				num_days_since_last_purchase_transaction = delta.days
				query = ("update customer c set c.last_purchase_date_company_brand_category = '%s', c.num_days_since_last_purchase_transaction = %s where c.customer_id = %s and c.company_id = %s and c.brand_id = %s and c.category = %s;")
				cursor.execute(query, (tokens[6], num_days_since_last_purchase_transaction, my_customer_id, tokens[4], tokens[5], tokens[3]))

		'''
		has_bought_same_category_different_company_q
		'''
		#print("Attribute 23")
		query = ("select c.customer_id, has_bought_same_category_different_company_q from customerstrain c where c.customer_id = %s and c.category = %s and c.company_id <> %s;")
		cursor.execute(query, (tokens[0], tokens[3], tokens[4]))
		my_customer_id = 0;
		my_has_bought_same_category_different_company_q = 0;
		for (customer_id, has_bought_same_category_different_company_q) in cursor:
			my_customer_id = customer_id
			my_has_bought_same_category_different_company_q = has_bought_same_category_different_company_q

			if (my_customer_id != 0) :
				query = ("update customerstrain c set has_bought_same_category_different_company_q = %s where c.customer_id = %s and c.category = %s and c.company_id <> %s;")
				cursor.execute(query, ((has_bought_same_category_different_company_q + 1), my_customer_id, tokens[3], tokens[4]))


		'''
		has_bought_same_company_different_category_q
		'''
		#print("Attribute 24")
		query = ("select c.customer_id, c.has_bought_same_company_different_category_q from customerstrain c where c.customer_id = %s and c.company_id = %s and c.category <> %s;")
		cursor.execute(query, (tokens[0], tokens[4], tokens[3]))
		my_customer_id = 0;
		my_has_bought_same_company_different_category_q = 0;
		for (customer_id, has_bought_same_company_different_category_q) in cursor:
			my_has_bought_same_company_different_category_q = has_bought_same_company_different_category_q
			my_customer_id = customer_id

			if (my_customer_id != 0) :
				query = ("update customerstrain c set has_bought_same_company_different_category_q = %s where c.customer_id = %s and c.company_id = %s and c.category <> %s;")
				cursor.execute(query, ((my_has_bought_same_company_different_category_q + 1), my_customer_id, tokens[4], tokens[3]))

		
	########################################## CUSTOMERSTEST ###############################################

	  	'''
	  	c.has_bought_company_before_q,
	  	has_bought_company_before_bool
	  	,has_bought_company_before_a
	  	,has_returned_company_before_q
	  	,has_returned_company_before_bool
	  	'''
	  	#print("Attribute 1-5")
		query = ("select c.has_bought_company_before_q, c.has_bought_company_before_bool, c.has_bought_company_before_a, c.has_returned_company_before_q, c.has_returned_company_before_bool from customerstest c"
				" where c.customer_id = %s and c.company_id = %s;")
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

				query = ("update customerstest c set c.has_bought_company_before_q = %s , c.has_bought_company_before_bool = %s , c.has_bought_company_before_a = %s , c.has_returned_company_before_q = %s , has_returned_company_before_bool = %s where c.customer_id = %s and c.company_id = %s;")
				cursor.execute(query, (my_has_bought_company_before_q, my_has_bought_company_before_bool, my_has_bought_company_before_a, my_has_returned_company_before_q, my_has_returned_company_before_bool, tokens[0], tokens[4]))


		'''
	  	c.has_bought_brand_before_q,
	  	has_bought_brand_before_bool
	  	,has_bought_brand_before_a
	  	,has_returned_brand_before_q
	  	,has_returned_brand_before_bool
	  	'''
	  	#print("Attribute 6-10")
		query = ("select c.has_bought_brand_before_q, c.has_bought_brand_before_bool, c.has_bought_brand_before_a, c.has_returned_brand_before_q, c.has_returned_brand_before_bool from customerstest c"
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

				query = ("update customerstest c set c.has_bought_brand_before_q = %s , c.has_bought_brand_before_bool = %s , c.has_bought_brand_before_a = %s , c.has_returned_brand_before_q = %s , has_returned_brand_before_bool = %s where c.customer_id = %s and c.company_id= %s and c.brand_id = %s;")
				cursor.execute(query, (my_has_bought_brand_before_q, my_has_bought_brand_before_bool, my_has_bought_brand_before_a, my_has_returned_brand_before_q, my_has_returned_brand_before_bool, tokens[0], tokens[4], tokens[5]))

		'''
	  	c.has_bought_category_before_q,
	  	has_bought_category_before_bool
	  	,has_bought_category_before_a
	  	,has_returned_category_before_q
	  	,has_returned_category_before_bool
	  	'''
	  	#print("Attribute 11-15")
		query = ("select c.has_bought_category_before_q, c.has_bought_category_before_bool, c.has_bought_category_before_a, c.has_returned_category_before_q, c.has_returned_category_before_bool from customerstest c"
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

				query = ("update customerstest c set c.has_bought_category_before_q = %s , c.has_bought_category_before_bool = %s , c.has_bought_category_before_a = %s , c.has_returned_category_before_q = %s , has_returned_category_before_bool = %s where c.customer_id = %s and c.category_id = %s;")
				cursor.execute(query, (my_has_bought_category_before_q, my_has_bought_category_before_bool, my_has_bought_category_before_a, my_has_returned_category_before_q, my_has_returned_category_before_bool, tokens[0], tokens[3], tokens[5]))


		'''
		c.has_bought_company_brand_category_before_q,
		has_bought_company_brand_category_before_bool
		,has_bought_company_brand_category_before_a
		,has_returned_company_brand_category_before_q
		,has_returned_company_brand_category_before_bool
		'''
		#print("Attribute 16-20")
		query = ("select c.has_bought_company_brand_category_before_q, c.has_bought_company_brand_category_before_bool, c.has_bought_company_brand_category_before_a, c.has_returned_company_brand_category_before_q, c.has_returned_company_brand_category_before_bool from customerstest c"
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

				query = ("update customerstest c set c.has_bought_company_brand_category_before_q = %s , c.has_bought_company_brand_category_before_bool = %s , c.has_bought_company_brand_category_before_a = %s , c.has_returned_company_brand_category_before_q = %s , has_returned_company_brand_category_before_bool = %s where c.customer_id = %s and c.company_brand_category_id = %s;")
				cursor.execute(query, (my_has_bought_company_brand_category_before_q, my_has_bought_company_brand_category_before_bool, my_has_bought_company_brand_category_before_a, my_has_returned_company_brand_category_before_q, my_has_returned_company_brand_category_before_bool, tokens[0], tokens[3], tokens[4], tokens[5]))



		'''
		last_purchase_date_company_brand_category
		num_days_since_last_purchase_transaction
		'''
		#print("Attribute 21-22")
		query = ("select c.customer_id, c.offerdate, c.last_purchase_date_company_brand_category from customerstest c where c.customer_id = %s and c.company_id = %s and c.brand_id = %s and c.category = %s;")
		cursor.execute(query, (tokens[0], tokens[4], tokens[5], tokens[3]))

		my_customer_id = 0
		for (customer_id, offerdate, last_purchase_date_company_brand_category) in cursor:
			my_customer_id = customer_id
			dateString = last_purchase_date_company_brand_category
			my_offerdate = datetime.datetime.strptime(offerdate, "%Y-%m-%d")

			my_current_date = datetime.datetime.strptime(tokens[6], "%Y-%m-%d")
			if (customer_id != 0 & current_date > last_purchase_date & tokens[9] > 0):
				delta = current_date - last_purchase_date
				num_days_since_last_purchase_transaction = delta.days
				query = ("update customer c set c.last_purchase_date_company_brand_category = '%s', c.num_days_since_last_purchase_transaction = %s where c.customer_id = %s and c.company_id = %s and c.brand_id = %s and c.category = %s;")
				cursor.execute(query, (tokens[6], num_days_since_last_purchase_transaction, my_customer_id, tokens[4], tokens[5], tokens[3]))

		'''
		has_bought_same_category_different_company_q
		'''
		#print("Attribute 23")
		query = ("select c.customer_id, has_bought_same_category_different_company_q from customerstest c where c.customer_id = %s and c.category = %s and c.company_id <> %s;")
		cursor.execute(query, (tokens[0], tokens[3], tokens[4]))
		my_customer_id = 0;
		my_has_bought_same_category_different_company_q = 0;
		for (customer_id, has_bought_same_category_different_company_q) in cursor:
			my_customer_id = customer_id
			my_has_bought_same_category_different_company_q = has_bought_same_category_different_company_q

			if (my_customer_id != 0) :
				query = ("update customerstest c set has_bought_same_category_different_company_q = %s where c.customer_id = %s and c.category = %s and c.company_id <> %s;")
				cursor.execute(query, ((has_bought_same_category_different_company_q + 1), my_customer_id, tokens[3], tokens[4]))


		'''
		has_bought_same_company_different_category_q
		'''
		#print("Attribute 24")
		query = ("select c.customer_id, c.has_bought_same_company_different_category_q from customerstest c where c.customer_id = %s and c.company_id = %s and c.category <> %s;")
		cursor.execute(query, (tokens[0], tokens[4], tokens[3]))
		my_customer_id = 0;
		my_has_bought_same_company_different_category_q = 0;
		for (customer_id, has_bought_same_company_different_category_q) in cursor:
			my_has_bought_same_company_different_category_q = has_bought_same_company_different_category_q
			my_customer_id = customer_id

			if (my_customer_id != 0) :
				query = ("update customerstest c set has_bought_same_company_different_category_q = %s where c.customer_id = %s and c.company_id = %s and c.category <> %s;")
				cursor.execute(query, ((my_has_bought_same_company_different_category_q + 1), my_customer_id, tokens[4], tokens[3]))
		
		#progress report (once every 500000 lines)
      	if e % 500000 == 0:
        	print e, datetime.now() - start				
  	print("Preprocessing done")
  	
  	cnx.commit()
  	cursor.close()
	cnx.close()


preprocess_data(loc_reduced_transactions)


