#!/usr/bin/env python

##  construct_dt_and_classify_one_sample_case3.py

##  This script does the same thing as the script
##  construct_dt_and_classify_one_sample_case2.py except
##  that it uses just two of the columns of the csv file for
##  DT construction and classification.  The two features
##  used are in columns indexed 3 and 5 of the csv file.

##  Remember that column indexing is zero-based in the csv
##  file.

##  The training file `stage3cancer.csv' was taken from the
##  RPART module by Terry Therneau and Elizabeth Atkinson.
##  This module is a part of the R based statistical package
##  for classification and regression by recursive
##  partitioning of data.


import DecisionTree
import sys

training_datafile = "shoppersTrain.csv"

dt = DecisionTree.DecisionTree( training_datafile = training_datafile,
                                csv_class_column_index = 6,
                                csv_columns_for_features = [12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34],
                                entropy_threshold = 0.01,
                                max_depth_desired = 10,
                                symbolic_to_numeric_cardinality_threshold = 10,
                              )
dt.get_training_data()
dt.calculate_first_order_probabilities()
dt.calculate_class_priors()

#   UNCOMMENT THE FOLLOWING LINE if you would like to see the training
#   data that was read from the disk file:
#dt.show_training_data()

root_node = dt.construct_decision_tree_classifier()

#   UNCOMMENT THE FOLLOWING LINE if you would like to see the decision
#   tree displayed in your terminal window:
print("\n\nThe Decision Tree:\n")
root_node.display_decision_tree("     ")
																																																																																																																																																																								
#,'last_purchase_date_company_brand_category = 2010-02-02'
test_sample = ['has_bought_company_before_q = 0','has_bought_company_before_bool = 0','has_bought_company_before_a = 0','has_returned_company_before_q = 0','has_returned_company_before_bool = 0', 'has_bought_brand_before_q = 0', 'has_bought_brand_before_bool = 0','has_bought_brand_before_a = 0','has_returned_brand_before_q = 0','has_returned_brand_before_bool = 0','has_bought_category_before_q = 6','has_bought_category_before_bool = 1','has_bought_category_before_a = 19.35','has_returned_category_before_q = 0','has_returned_category_before_bool = 0','has_bought_company_brand_category_before_q = 0','has_bought_company_brand_category_before_bool = 0','has_bought_company_brand_category_before_a = 0','has_returned_company_brand_category_before_q = 0','has_returned_company_brand_category_before_bool = 0','num_days_since_last_purchase_transaction = 0','has_bought_same_category_different_company_q = 0','has_bought_same_company_different_category_q = 6']
# The rest of the script is for displaying the classification results:

classification = dt.classify(root_node, test_sample)
solution_path = classification['solution_path']
del classification['solution_path']
which_classes = list( classification.keys() )
which_classes = sorted(which_classes, key=lambda x: classification[x], reverse=True)
print("\nClassification:\n")
print("     "  + str.ljust("class name", 30) + "probability")    
print("     ----------                    -----------")
for which_class in which_classes:
    if which_class is not 'solution_path':
        print("     "  + str.ljust(which_class, 30) +  str(classification[which_class]))

print("\nSolution path in the decision tree: " + str(solution_path))
print("\nNumber of nodes created: " + str(root_node.how_many_nodes()))


eval_data = DecisionTree.EvalTrainingData(
                        training_datafile = training_datafile,
                        csv_class_column_index = 6,
                        csv_columns_for_features = [12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34],
                        entropy_threshold = 0.01,
                        max_depth_desired = 3,
                        symbolic_to_numeric_cardinality_threshold = 10,
                    )

eval_data.get_training_data()
eval_data.evaluate_training_data()

