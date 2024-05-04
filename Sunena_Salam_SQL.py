# Databricks notebook source
# MAGIC %md
# MAGIC Considering I have downloaded the zip files, i.e., clinicaltrial_2023 and pharma from the assignment brief and uploaded both of them on the definite path location. Now, in order to copy files from one location to another, lets use the databricks utilities to inetract with it: 

# COMMAND ----------

dbutils.fs.cp("/FileStore/tables/pharma.zip", "file:/tmp/")
dbutils.fs.cp("/FileStore/tables/clinicaltrial_2023.zip", "file:/tmp/")

# COMMAND ----------

# MAGIC
# MAGIC %sh
# MAGIC ls /tmp/
# MAGIC #Run a shell script so as to lists the contents of the /tmp/ directory to check the presence of the tables we just uploaded.
# MAGIC

# COMMAND ----------

# MAGIC %sh
# MAGIC unzip -d /tmp/ /tmp/pharma.zip
# MAGIC unzip -d /tmp/ /tmp/clinicaltrial_2023.zip
# MAGIC

# COMMAND ----------

# MAGIC
# MAGIC %sh
# MAGIC ls /tmp/pharma.csv
# MAGIC ls /tmp/clinicaltrial_2023.csv
# MAGIC
# MAGIC #check whether the files are present in the /tmp/ path or not
# MAGIC

# COMMAND ----------

#create a directory on the name pharma.csv

dbutils.fs.mkdirs("FileStore/tables/pharma.csv")
# "dbutils.fs.mkdirs("FileStore/tables/clinicaltrial_2023.csv") -- not using this command since there is already the file present in the path because by mistake have already stored in it at initial stage of doing the assignment. Since, can't change the file name. keeping the stored file path as it is! Hence, we dont have to create the directory."

# COMMAND ----------

# Now, we can move the file from temp folder to the needed one
dbutils.fs.mv("file:/tmp/pharma.csv", "/FileStore/tables/pharma.csv", True)

# COMMAND ----------

#check the existence of the file in the path location

dbutils.fs.ls("FileStore/tables/pharma.csv/")

# COMMAND ----------

# to list the contents of the directory

# COMMAND ----------

# MAGIC %fs ls /FileStore/tables/pharma.csv 

# COMMAND ----------

# let's name the RDD as "pharma_rdd"
pharma_rdd = sc.textFile("FileStore/tables/pharma.csv/")

# Display the first few rows of the pharma_rdd
pharma_rdd.take(5)

# COMMAND ----------

# let's name the RDD as "clinicaltrial_2023_rdd"
clinicaltrial_2023_rdd = sc.textFile("FileStore/tables/clinicaltrial_2023.csv/")

# Display the first few rows of the clinicaltrial_2023_rdd
clinicaltrial_2023_rdd.take(5)

# COMMAND ----------

# Check the first row of pharma_Rdd and print its values
column_names_pharma = pharma_rdd.first()
print("column names values - pharma_rdd :", column_names_pharma)

# COMMAND ----------

# Assuming pharma_rdd is your RDD
row_count = pharma_rdd.count()

print("Row count of pharma_with_none_notes RDD:", row_count)


# COMMAND ----------

# Read the data into the name "clinicaltrial_2023_rdd"

clinicaltrial_2023_rdd = sc.textFile("/FileStore/tables/clinicaltrial_2023.csv")

# COMMAND ----------

# Check the first row and print its values
column_names_clinicaltrial = clinicaltrial_2023_rdd.first()
print("First row values:", column_names_clinicaltrial)

# COMMAND ----------

# lets do the data preparation and cleansing on the table clinicaltrial_2023 to make it ready for the tasks

# Each line in the RDD is a string containing tab-separated values, and the lambda function splits each line into a list of values.
clinicaltrial_2023_parsed_data_rdd = clinicaltrial_2023_rdd.map(lambda line: line.split("\t"))

# define a function for extracting the variables from the first column
def parse_first_column(row):
    columns = row[0].split("|")
    return (columns,) + tuple(row[1:]) #-- this returns a tuple containing all the column values with the extracted first column along with it

# Now, in order to apply the parsing function to each row
clinicaltrial_2023_structured_data_rdd = clinicaltrial_2023_parsed_data_rdd.map(parse_first_column)

#Printing first few rows in order to check whether the pasing have been successfully done or not.
clinicaltrial_2023_structured_data_rdd.take(5)

# COMMAND ----------

# Since, there are extra unwanted commas and quotes present in our parsed dataset, we need to remove it from each row

clinicaltrial_2023_cleaned_data_rdd = clinicaltrial_2023_parsed_data_rdd.map(lambda row: [field.replace(',', '').replace('"', '') for field in row])

# view first few rows to check and confirm the data cleansing have successfully implemented
clinicaltrial_2023_cleaned_data_rdd.take(5)

# COMMAND ----------

#Now, next stage in data preparation is removing the missing values in the dataset. To check the presence of missing values in the dataset, let's define a function:

def check_missing_values(dataset):
    missing_values = dataset.filter(lambda row: '' in row)
    if missing_values.isEmpty():
        print("No missing values")
    else:
        print("Yes")
        for row in missing_values.collect():
            print(row)

# Call the function with the clinicaltrial_2023_cleaned_data_rdd in order to call out the missing values in the cleaned data RDD
check_missing_values(clinicaltrial_2023_cleaned_data_rdd)

# Now to remove these missing values with none instead: 
clinicaltrial_2023_cleaned_data_rdd_with_none = clinicaltrial_2023_cleaned_data_rdd.map(lambda row: [None if value == '' else value for value in row])

# View the first few rows in order to confirm that the missing values have been removed from the cleaned RDD
clinicaltrial_2023_cleaned_data_rdd_with_none.take(5)

# COMMAND ----------

# to double confirm that the missing values have been removed from the RDD, lets call the function again:
check_missing_values(clinicaltrial_2023_cleaned_data_rdd_with_none)

# COMMAND ----------

# Assuming your clinicaltrial_2023_cleaned_data_rdd_with_none contains the data as an RDD

# Get the count of data rows in the RDD
row_count = clinicaltrial_2023_cleaned_data_rdd_with_none.count()

# Display the count
print("Number of data rows in the clinicaltrial_2023_cleaned_data_rdd_with_none:", row_count)

# COMMAND ----------

#Now let's start the deep analysis of each row to find whether all the data values are getting in each column is as per the requirement so as to continue with the data in-depth analysis

# Extract the values of the first column from each row
Id = clinicaltrial_2023_cleaned_data_rdd_with_none.map(lambda row: row[0])

# Display the first column values
for value in Id.collect():
    print(value)

# COMMAND ----------

# to ckec whether the column contains only 11 characters, since the Id is a 11 character Alphanumeric value, viewing the header row
header_row = clinicaltrial_2023_cleaned_data_rdd_with_none.first()

# let's define a function to check if the value has 11 characters
def check_id_length(row):
    id_value = row[0]
    return len(id_value) == 11

# Check if all values in the "Id" column have 11 characters
all_ids_correct = clinicaltrial_2023_cleaned_data_rdd_with_none.filter(lambda row: row != header_row).map(check_id_length).reduce(lambda x, y: x and y)

if all_ids_correct:
    print("All values in the 'Id' column have 11 characters.")
else:
    print("Not all values in the 'Id' column have 11 characters.")


# COMMAND ----------

# Lets define a Function to check if the value has 11 characters and return the row if not
def check_id_length_and_return(row):
    id_value = row[0]
    if len(id_value) != 11:
        return row
    return None

# Check if all values in the "Id" column have 11 characters
invalid_ids = clinicaltrial_2023_cleaned_data_rdd_with_none.filter(lambda row: row != header_row).map(check_id_length_and_return).filter(lambda x: x is not None).collect()

if not invalid_ids:
    print("All values in the 'Id' column have 11 characters.")
else:
    print("Rows where 'Id' column doesn't have 11 characters:")
    for row in invalid_ids:
        print(row)

# COMMAND ----------

# Hence, we need to remove the row since there is no significance in the row without a distinct Id column in it

target_data = ['taluña|Public Health Service of Madrid|Public Health Service of Galicia|Cantabria Health Service', '97.0', 'OTHER_GOV', 'INTERVENTIONAL', 'Allocation: RANDOMIZED|Intervention Model: PARALLEL|Masking: SINGLE (OUTCOMES_ASSESSOR)|Primary Purpose: TREATMENT', '2005-01', '2010-06']

# Adding index to each row
indexed_rdd = clinicaltrial_2023_cleaned_data_rdd_with_none.zipWithIndex()

# Filter the indexed RDD to find the row containing the target data
result = indexed_rdd.filter(lambda row: row[0] == target_data).collect()

# Check if any rows match the target data
if result:
    for row in result:
        print("Row number:", row[1] + 1) 
        print("Row data:", row[0])
else:
    print("Target data not found in the RDD.")

# COMMAND ----------

# since the row without ID is in the Extract column names from the first row
column_names = clinicaltrial_2023_cleaned_data_rdd_with_none.first()

# Filter out rows where the 'Id' column doesn't have 11 characters
filtered_rdd = clinicaltrial_2023_cleaned_data_rdd_with_none.filter(lambda row: len(row[0]) == 11)

# Prepend the column attribute names to the filtered data
filtered_data_with_header = [column_names] + filtered_rdd.collect()

# Convert the filtered data with header into an RDD
filtered_rdd_with_header = sc.parallelize(filtered_data_with_header)
filtered_rdd_with_header.take(5)

# COMMAND ----------

#To confirm that we filtered the row without ID column
# Extract the header row
header_row = filtered_rdd_with_header.first()

# Function to check if the value has 11 characters and return the row if not
def check_id_length_and_return(row):
    id_value = row[0]
    if len(id_value) != 11:
        return row
    return None

# Check if all values in the "Id" column have 11 characters
invalid_ids = filtered_rdd_with_header.filter(lambda row: row != header_row).map(check_id_length_and_return).filter(lambda x: x is not None).collect()

if not invalid_ids:
    print("All values in the 'Id' column have 11 characters.")
else:
    print("Rows where 'Id' column doesn't have 11 characters:")
    for row in invalid_ids:
        print(row)

# COMMAND ----------


#just to double confirm that only required rows are coming in the dataset 
# Add index to each row
rows_with_index = filtered_rdd_with_header.zipWithIndex()

# Filter rows where 'Id' column contains 'Id' and is not None
rows_with_valid_type = rows_with_index.filter(lambda row_index: row_index[0][0] and 'Id' in row_index[0][0])

# Print the row number and rows
for row_index, row in rows_with_valid_type.collect():
    print("Row number:", row_index)
    print("Row content:", row)

# COMMAND ----------

# To get the values of the second column from each row of the final RDD filtered_rdd_with_header

study_title = filtered_rdd_with_header.map(lambda row: row[1])

# Display the first column values
for value in study_title.collect():
    print(value)

# COMMAND ----------

# To get the values of the third column from each row of the final RDD filtered_rdd_with_header

acronym = filtered_rdd_with_header.map(lambda row: row[2])

# Display the first column values
for value in acronym.collect():
    print(value)

# COMMAND ----------

# I guess, as per I looked into the dataset, the List of expected values in the fourth column are:
expected_values = {'Status', 'COMPLETED', 'ACTIVE_NOT_RECRUITING', 'RECRUITING', 'NOT_YET_RECRUITING', 'UNKNOWN', 'WITHDRAWN', 'NO_LONGER_AVAILABLE', 'ENROLLING_BY_INVITATION', 'SUSPENDED', 'AVAILABLE', 'WITHHELD', 'TEMPORARILY_NOT_AVAILABLE', 'APPROVED_FOR_MARKETING','TERMINATED'}

# Convert the set of expected values to an RDD
expected_values_rdd = sc.parallelize(expected_values)

# Extract the values of the forth column from each row
status = filtered_rdd_with_header.map(lambda row: row[3])

# Check for values other than the expected ones
other_values_rdd = status.subtract(expected_values_rdd)

# Collect the other values found
other_values = other_values_rdd.collect()

# Print any other values found - just to make sure that only these values are coming in the forth column - status
if other_values:
    print("Other values found:")
    for value in other_values:
        print(value)
else:
    print("No other values found.")

# COMMAND ----------

# To get the values of the fifth column from each row of the final RDD filtered_rdd_with_header

conditions = filtered_rdd_with_header.map(lambda row: row[4])

# Display the first column values
for value in conditions.collect():
    print(value)

# COMMAND ----------

# To get the values of the sixth column from each row of the final RDD filtered_rdd_with_header

interventions = filtered_rdd_with_header.map(lambda row: row[5])

# Display the first column values
for value in interventions.collect():
    print(value)

# COMMAND ----------

# To get the values of the seventh column from each row of the final RDD filtered_rdd_with_header

sponsor = filtered_rdd_with_header.map(lambda row: row[6])

# Display the first column values
for value in sponsor.collect():
    print(value)

# COMMAND ----------

# To get the values of the eight column from each row of the final RDD filtered_rdd_with_header

collaborators = filtered_rdd_with_header.map(lambda row: row[7])

# Display the first column values
for value in collaborators.collect():
    print(value)

# COMMAND ----------

# Extract the values of the ninth column from each row
enrollment = filtered_rdd_with_header.map(lambda row: row[8] if len(row) > 8 else None)

# Display the ninth column values
for value in enrollment.collect():
    print(value)

# COMMAND ----------

#Since the datatype of the column named "enrollment" ahouls be float since there are only float values coming in it, let's define a function to convert a value to float, handling None values
def safe_float_conversion(value):
    try:
        return float(value)
    except (ValueError, TypeError):
        return None  # Return None for non-numeric values or None

# Apply the function to convert the "enrolllment" columns to float
converted_rdd = filtered_rdd_with_header.map(lambda row: [safe_float_conversion(val) if idx in [8] else val for idx, val in enumerate(row)])

# to confirm the value is changed to float datatype
print(converted_rdd.take(5))


# COMMAND ----------

# To get the values of the column "Funder Type" from each row of the dataset filtered_rdd_with_header
funder_type = filtered_rdd_with_header.map(lambda row: row[9] if len(row) > 9 else None)

# to View the values
for value in funder_type.collect():
    print(value)

# COMMAND ----------

# to get the values of the column Type from each row of the dataset filtered_rdd_with_header
type = filtered_rdd_with_header.map(lambda row: row[10] if len(row) > 10 else None)

# Display the tenth column values
for value in type.collect():
    print(value)

# COMMAND ----------

# To check there is only expected values coming in the column type so as to smoothly get the accurate result while doing analysis:
expected_values = {'OBSERVATIONAL', 'INTERVENTIONAL','EXPANDED_ACCESS',None,'Type'}
# Assuming your verified_rdd contains the data as an RDD

# Convert the set of expected values to an RDD
expected_values_rdd = sc.parallelize(expected_values)


# Check for values other than the expected ones
other_values_rdd = type.subtract(expected_values_rdd)

# Collect the other values found
other_values = other_values_rdd.collect()

# Print any other values found
if other_values:
    print("Other values found:")
    for value in other_values:
        print(value)
else:
    print("No other values found.")

# COMMAND ----------

# To get the values of the column "Study Design" from each row of the dataset RDD filtered_rdd_with_header
study_design = filtered_rdd_with_header.map(lambda row: row[11] if len(row) > 11 else None)

# Display the ninth column values
for value in study_design.collect():
    print(value)

# COMMAND ----------

# To get the values of the "start" column from each row of the dataset filtered_rdd_with_header
start = filtered_rdd_with_header.map(lambda row: row[12] if len(row) > 12 else None)

# Display the ninth column values
for value in start.collect():
    print(value)

# COMMAND ----------

# we can notice here that none values are coming, and two data formats of date value is coming in the dataset YYYY-MM-DD and YYYY-MM-DD. Hence we need to change this non-uniform data format into the standard one of YYYY-MM-DD. For, that, Assume all the data where YYYY-MM format started with the first day of the month given

# For achieving this, let's split the RDD into two --> start_with_none --> containing only none values in it and start_without_none --> containing the two other data formats.
start_with_none = filtered_rdd_with_header.filter(lambda row: len(row) > 12 and row[12] is None)

# Filter RDD to create start_without_none containing rows without None values in the thirteenth column
start_without_none = filtered_rdd_with_header.filter(lambda row: len(row) > 12 and row[12] is not None and row[12] != 'Start')

# Print the first_part_start RDD
print("Start column containing only none values):")
for row in start_with_none.collect():
    print(row)

# Print the second_part_start RDD
print("\nStart column containing values with the format YYYY-MM-DD and YYYY-MM):")
for row in start_without_none.collect():
    print(row)


# COMMAND ----------


# Get the count of data rows in the RDD
row_count = filtered_rdd_with_header.count()

# Display the count
print("Number of data rows in the filtered_rdd_with_header:", row_count)
# Get the count of data rows in the RDD
row_count = start_with_none.count()

# Display the count
print("Number of data rows in the start_with_none:", row_count)
# Get the count of data rows in the RDD
row_count = start_without_none.count()

# Display the count
print("Number of data rows in the start_without_none:", row_count)

# COMMAND ----------

# Convert lists into tuples for start_with_none and start_without_none RDDs
start_with_none_tuples = start_with_none.map(tuple)
start_without_none_tuples = start_without_none.map(tuple)

# Create a set of all rows in start_with_none and start_without_none RDDs
start_with_none_rows = set(start_with_none_tuples.collect())
start_without_none_rows = set(start_without_none_tuples.collect())

# Convert the original RDD into tuples
filtered_rdd_tuples = filtered_rdd_with_header.map(tuple)

# Create a set of all rows in filtered_rdd_with_header RDD
all_rows = set(filtered_rdd_tuples.collect())

# Find the rows that are not included in either start_with_none or start_without_none
not_in_either = all_rows - (start_with_none_rows.union(start_without_none_rows))

# Print the rows not included in either start_with_none or start_without_none
print("Rows not included in either start_with_none or start_without_none:")
for row in not_in_either:
    print(row)


# COMMAND ----------

# Find the row where Id = NCT00146315 along with its index
matching_row_with_index = filtered_rdd_with_header.zipWithIndex() \
    .filter(lambda row_index: row_index[0][0] == 'NCT00146315') \
    .collect()

# Check if a matching row was found
if matching_row_with_index:
    # Extract the row and its index
    matching_row, index = matching_row_with_index[0]
    # Print the index and the row
    print("Index:", index)
    print("Row:", matching_row)
else:
    print("No row found with Id = NCT00146315 in filtered_rdd_with_header.")


# COMMAND ----------

# Now, let's split the start_without_none column into two such that first part start_without_none_with_day --> contains the format YYYY-MM--DD and second part start_without_none_with_month --> contains the format YYYY-MM in order to do the required data transformation. #since, the row no.293056 doesn't contain the last values, we can remove it from the dataset

start_without_none_with_day = start_without_none.filter(lambda row: len(row) > 12 and row[12] is not None and len(row[12]) == 10)

start_without_none_with_month = start_without_none.filter(lambda row: len(row) > 12 and row[12] is not None and len(row[12]) == 7)

# Print the start_without_none_with_day RDD
print("start column values (Containing date values in YYYY-MM-DD format):")
for row in start_without_none_with_day.collect():
    print(row)

# Print the start_without_none_with_month RDD
print("\nstart column values (Containing date values in YYYY-MM format):")
for row in start_without_none_with_month.collect():
    print(row)

# COMMAND ----------

# Convert each row in start_without_none RDD to a tuple
start_without_none_tuples = start_without_none.map(tuple)

# Now, let's split the start_without_none column into two such that first part 
# start_without_none_with_day contains the format YYYY-MM-DD and second part 
# start_without_none_with_month contains the format YYYY-MM in order to do the required data transformation

start_without_none_with_day = start_without_none_tuples.filter(lambda row: len(row) > 12 and row[12] is not None and len(row[12]) == 10)

start_without_none_with_month = start_without_none_tuples.filter(lambda row: len(row) > 12 and row[12] is not None and len(row[12]) == 7)

# Find the rows that don't fit in either condition
not_in_either = start_without_none_tuples.subtract(start_without_none_with_day).subtract(start_without_none_with_month)

# Print the rows that don't fit in either condition
print("Rows that don't fit in either condition:")
for row in not_in_either.collect():
    print(row)


# COMMAND ----------


# Get the count of data rows in the RDD
row_count = start_with_none.count()

# Display the count
print("Number of data rows in the start_with_none:", row_count)
# Get the count of data rows in the RDD
row_count = start_without_none_with_day.count()

# Display the count
print("Number of data rows in the start_without_none_with_day:", row_count)
# Get the count of data rows in the RDD
row_count = start_without_none_with_month.count()

# Display the count
print("Number of data rows in the start_without_none_with_month:", row_count)

# COMMAND ----------

# Now, let's initialize a variable to track if all values have the format YYYY-MM
required_format = True

# Iterate through each row in the RDD start_without_none_with_month and to check the format it
for row in start_without_none_with_month.collect():
    if len(row[12]) != 7:
        required_format = False
        break  # Exit the loop early if a value with unexpected format is found

# Print the result
if required_format:
    print("All values in the start_without_none_with_month have the format YYYY-MM")
else:
    print("Not all values in the start_without_none_with_month have the format YYYY-MM")

# COMMAND ----------

from datetime import datetime

# Now let's define a Function to convert YYYY-MM format of start_without_none_with_month to YYYY-MM-01 format as mentioned before
def convert_to_first_day_of_month(date_str):
    try:
        # Parse the input date string
        date = datetime.strptime(date_str, '%Y-%m')
        # Convert the date to YYYY-MM-01 format
        return date.strftime('%Y-%m-01')
    except ValueError:
        return date_str  # Return the original date string if not in YYYY-MM format

# To apply the function to convert the date format for each row in the RDD
start_Without_none_with_month_with_new_format = start_without_none_with_month.map(lambda row: tuple(row[:12]) + (convert_to_first_day_of_month(row[12]),) + tuple(row[13:]) if len(row[12]) == 7 else row)

# Print the RDD with the new date format
for row in start_Without_none_with_month_with_new_format.collect():
    print(row)


# COMMAND ----------

# for the final confirmation, check if any rows have the format "YYYY-MM-DD" in the thirteenth column
has_yyyy_mm_dd_format = start_Without_none_with_month_with_new_format.filter(lambda row: len(row[12]) == 10 and row[12][4] == '-' and row[12][7] == '-').count() > 0

# Print the result
if has_yyyy_mm_dd_format:
    print("Yes")
else:
    print("No")

# COMMAND ----------

# since, we got all the values in the start column to the required format we expected, we can combine the splitted RDD's into the final one
rdd_finalized = start_Without_none_with_month_with_new_format.union(start_without_none_with_day).union(start_with_none)

# Check if any row contains data other than None or the "YYYY-MM-DD" format in the start column
def check_format(row):
    if row[12] is not None and len(row[12]) != 10 and not (len(row[12]) == 7 and row[12][4] == '-'):
        return True
    return False

contains_other_format = rdd_finalized.filter(check_format).count() > 0

# Print the result
if contains_other_format:
    print("Yes, some rows contain data other than None or YYYY-MM-DD format in the start column.")
else:
    print("No, all rows have either None or YYYY-MM-DD format in the start column.")

# COMMAND ----------

#to attach header to our RDD 
rdd_with_header_finalized = sc.parallelize([column_names]).union(rdd_finalized)
rdd_with_header_finalized.take(5)

# COMMAND ----------


#just to double confirm that only required rows are coming in the dataset 
# Add index to each row
rows_with_index = rdd_with_header_finalized.zipWithIndex()

# Filter rows where 'Id' column contains 'Id' and is not None
rows_with_valid_type = rows_with_index.filter(lambda row_index: row_index[0][0] and 'Id' in row_index[0][0])

# Print the row number and rows
for row_index, row in rows_with_valid_type.collect():
    print("Row number:", row_index)
    print("Row content:", row)

# COMMAND ----------

# Assuming your clinicaltrial_2023_cleaned_data_rdd_with_none contains the data as an RDD

# Get the count of data rows in the RDD
row_count = rdd_with_header_finalized.count()

# Display the count
print("Number of data rows in the rdd_with_header_finalized:", row_count)

# COMMAND ----------

# To get the values of the completion column from each row of the dataset rdd_with_header_finalized
completion = rdd_with_header_finalized.map(lambda row: row[13] if len(row) > 13 else None)

# Display the completion column values
for value in completion.collect():
    print(value)

# COMMAND ----------

#Here as well, we could see that the formats are different like the start column, so let's do the same transformation logic that we did for the start column

# split RDD to create completion_with_none containing rows with None values in the completion column
completion_with_none = rdd_with_header_finalized.filter(lambda row: len(row) > 13 and row[13] is None)

# split RDD to create completion_without_none containing rows without None values in the completion column
completion_without_none = rdd_with_header_finalized.filter(lambda row: len(row) > 13 and row[13] is not None and row[13] != "Completion") #remove the header since the machine will detect it as column without none

# Print the completion_with_none RDD
print("completion (Containing None values in column completion):")
for row in completion_with_none.collect():
    print(row)

# Print the completion_without_none RDD
print("\ncompletion (Containing non- None values in column completion):")
for row in completion_without_none.collect():
    print(row)

# COMMAND ----------

#check we didn't ignore any rows while doing the splitting, lets re-confirm the row count
row_count_final = completion_with_none.count()

# Display the count
print("Number of data rows in the completion_with_none:", row_count_final)

#check we didn't ignore any rows while doing the splitting, lets re-confirm the row count
row_count_final = completion_without_none.count()

# Display the count
print("Number of data rows in the completion_without_none:", row_count_final)

# COMMAND ----------

#to double-confirm completion_without_none doesn't contains None values in it
contains_none = False
for row in completion_without_none.collect():
    if row[13] is None:
        contains_none = True
        break

# Print the result
if contains_none:
    print("completion_without_none contains None values in it.")
else:
    print("completion_without_none does not contain None values in it.")

# COMMAND ----------

# split RDD to create completion_without_none_with_day containing rows with date format YYYY-MM-DD in the completion column
completion_without_none_with_day = completion_without_none.filter(lambda row: len(row) > 13 and row[13] is not None and len(row[13]) == 10)

# split RDD to create completion_without_none_with_month containing rows with date format YYYY-MM in the completion column
completion_without_none_with_month = completion_without_none.filter(lambda row: len(row) > 13 and row[13] is not None and len(row[13]) == 7)

# Print the completion_without_none_with_day RDD
print("completion column (Containing date values in YYYY-MM-DD format):")
for row in completion_without_none_with_day.collect():
    print(row)

# Print the completion_without_none_with_month RDD
print("\ncompletion column (Containing date values in YYYY-MM format):")
for row in completion_without_none_with_month.collect():
    print(row)

# COMMAND ----------

#check we didn't ignore any rows while doing the splitting, lets re-confirm the row count
row_count_final = completion_without_none_with_day.count()

# Display the count
print("Number of data rows in the completion_without_none_with_day:", row_count_final)

#check we didn't ignore any rows while doing the splitting, lets re-confirm the row count
row_count_final = completion_without_none_with_month.count()

# Display the count
print("Number of data rows in the completion_without_none_with_month:", row_count_final)

# COMMAND ----------

# now let's initialize a variable to track if all values have the format YYYY-MM
all_values_yyyy_mm = True

# Iterate through each row and check the format of the forteenth column
for row in completion_without_none_with_month.collect():
    if len(row[13]) != 7:
        all_values_yyyy_mm = False
        break  # Exit the loop early if a value with unexpected format is found

# Print the result
if all_values_yyyy_mm:
    print("All values in the completion_without_none_with_month column have the format YYYY-MM")
else:
    print("Not all values in the completion_without_none_with_month column have the format YYYY-MM")

# COMMAND ----------

from datetime import datetime

# Define a function to convert YYYY-MM to YYYY-MM-01 format
def convert_to_first_day_of_month(date_str):
    try:
        # Parse the input date string
        date = datetime.strptime(date_str, '%Y-%m')
        # Convert the date to YYYY-MM-01 format
        return date.strftime('%Y-%m-01')
    except ValueError:
        return date_str  # Return the original date string if not in YYYY-MM format

# Apply the function to convert the date format for each row in the RDD
completion_without_none_with_month_with_new_format = completion_without_none_with_month.map(lambda row: tuple(row[:13]) + (convert_to_first_day_of_month(row[13]),) + tuple(row[14:]) if len(row[13]) == 7 else row)

# Print the RDD with the new date format
for row in completion_without_none_with_month_with_new_format.collect():
    print(row)


# COMMAND ----------

#check we didn't ignore any rows while doing the splitting, lets re-confirm the row count
row_count_final = completion_without_none_with_month_with_new_format.count()

# Display the count
print("Number of data rows in the completion_without_none_with_month_with_new_format:", row_count_final)

# COMMAND ----------

# Check if any rows have the format "YYYY-MM-DD" in the completion column
has_yyyy_mm_dd_format_completion = completion_without_none_with_month_with_new_format.filter(lambda row: len(row[13]) == 10 and row[13][4] == '-' and row[13][7] == '-').count() > 0

# Print the result
if has_yyyy_mm_dd_format_completion:
    print("Yes")
else:
    print("No")

# COMMAND ----------

# Now, let's Combine the three splitted RDDs of the completion column into a new RDD
rdd_final = completion_without_none_with_month_with_new_format.union(completion_without_none_with_day).union(completion_with_none)

# Check if any row contains data other than None or the "YYYY-MM-DD" format in the completion column
def check_format(row):
    if row[13] is not None and len(row[13]) != 10 and not (len(row[13]) == 7 and row[13][4] == '-'):
        return True
    return False

contains_other_format = rdd_final.filter(check_format).count() > 0

# Print the result
if contains_other_format:
    print("Yes, some rows contain data other than None or YYYY-MM-DD format in the completion column.")
else:
    print("No, all rows have either None or YYYY-MM-DD format in the completion column.")

# COMMAND ----------

#check we didn't ignore any rows while doing the splitting, lets re-confirm the row count
row_count_final = rdd_final.count()

# Display the count
print("Number of data rows in the RDD:", row_count_final)

# COMMAND ----------

from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder \
    .appName("RDD Operations") \
    .getOrCreate()

# Define the target data
target_data = ('Id', 'Study Title', 'Acronym', 'Status', 'Conditions', 'Interventions', 'Sponsor', 'Collaborators', 'Enrollment', 'Funder Type', 'Type', 'Study Design', 'Start', 'Completion')

# Add index to each row
indexed_rdd = rdd_final.zipWithIndex()

# Initialize variables to store result
found = False
row_number = None

# Loop through the indexed RDD to search for target data
for row in indexed_rdd.collect():
    if row[0] == target_data:
        found = True
        row_number = row[1]  # Get the index of the row
        break

if found:
    print("Target data found in the RDD at row number:", row_number)
else:
    print("Target data not found in the RDD.")


# COMMAND ----------

# Define the header row
header_row = ('Id', 'Study Title', 'Acronym', 'Status', 'Conditions', 'Interventions', 'Sponsor', 'Collaborators', 'Enrollment', 'Funder Type', 'Type', 'Study Design', 'Start', 'Completion')

# Convert the header row to an RDD with a single element
header_rdd = spark.sparkContext.parallelize([header_row])

# Prepend the header row to the existing RDD
final_rdd_clinicaltrial_2023 = header_rdd.union(rdd_final)
final_rdd_clinicaltrial_2023.take(5)


# COMMAND ----------

from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder \
    .appName("RDD Operations") \
    .getOrCreate()

# Define the target data
target_data = ('Id', 'Study Title', 'Acronym', 'Status', 'Conditions', 'Interventions', 'Sponsor', 'Collaborators', 'Enrollment', 'Funder Type', 'Type', 'Study Design', 'Start', 'Completion')

# Add index to each row
indexed_rdd = final_rdd_clinicaltrial_2023.zipWithIndex()

# Initialize variables to store result
found = False
row_number = None

# Loop through the indexed RDD to search for target data
for row in indexed_rdd.collect():
    if row[0] == target_data:
        found = True
        row_number = row[1]  # Get the index of the row
        break

if found:
    print("Target data found in the RDD at row number:", row_number)
else:
    print("Target data not found in the RDD.")


# COMMAND ----------

#check we didn't ignore any rows while doing the splitting, lets re-confirm the row count
row_count_final = final_rdd_clinicaltrial_2023.count()

# Display the count
print("Number of data rows in the final_rdd_clinicaltrial_2023:", row_count_final)

#check we didn't ignore any rows while doing the splitting, lets re-confirm the row count
row_count_final = rdd_final.count()

# Display the count
print("Number of data rows in the rdd_final:", row_count_final)
#check we didn't ignore any rows while doing the splitting, lets re-confirm the row count
row_count_final = completion_without_none_with_month_with_new_format.count()

# Display the count
print("Number of data rows in the completion_without_none_with_month_with_new_format:", row_count_final)
#check we didn't ignore any rows while doing the splitting, lets re-confirm the row count
row_count_final = completion_without_none_with_day.count()

# Display the count
print("Number of data rows in the completion_without_none_with_day:", row_count_final)

row_count_final = completion_with_none.count()

# Display the count
print("Number of data rows in the completion_with_none:", row_count_final)

# COMMAND ----------

#Now, let's begin the data cleansing for the dataset RDD pharma_rdd
import csv
from io import StringIO

# let's begin with defining a function to parse CSV rows
def parse_csv_row(row):
    return next(csv.reader(StringIO(row)))

# to skip the header row
header = pharma_rdd.first()
pharma_rdd_without_header = pharma_rdd.filter(lambda row: row != header)

# Parse the CSV rows and extract the column "Company"
company = pharma_rdd_without_header.map(lambda row: parse_csv_row(row)[0])

# view the value
for value in company.collect():
    print(value)

# COMMAND ----------

# let's define a function to check if any column has missing values
def is_missing(value):
    return value == ''

# Filter out missing values
missing_values = company.filter(is_missing)

# Count the missing values
num_missing_values = missing_values.count()

# Display the count of missing values
print("Number of missing values in column 'company':", num_missing_values)

# COMMAND ----------

#likewise, let's initiate values in the column "parent_company"
parent_company = pharma_rdd_without_header.map(lambda row: parse_csv_row(row)[1])

# Display the column values
for value in parent_company.collect():
    print(value)

# COMMAND ----------

# let's define a function to check if any column has missing values
def is_missing(value):
    return value == ''

# Filter out missing values
missing_values = parent_company.filter(is_missing)

# Count the missing values
num_missing_values = missing_values.count()

# Display the count of missing values
print("Number of missing values in column 'parent_company':", num_missing_values)

# COMMAND ----------

#likewise, let's initiate values in the column "Penalty_Amount"
Penalty_Amount = pharma_rdd_without_header.map(lambda row: parse_csv_row(row)[2])

# Display the column values
for value in Penalty_Amount.collect():
    print(value)

# COMMAND ----------

# let's define a function to check if any column has missing values
def is_missing(value):
    return value == ''

# Filter out missing values
missing_values = Penalty_Amount.filter(is_missing)

# Count the missing values
num_missing_values = missing_values.count()

# Display the count of missing values
print("Number of missing values in column 'Penalty_Amount':", num_missing_values)

# COMMAND ----------

#likewise, let's initiate values in the column "subtraction_from_penalty"
subtraction_from_penalty = pharma_rdd_without_header.map(lambda row: parse_csv_row(row)[3])

# Display the column values
for value in subtraction_from_penalty.collect():
    print(value)

# COMMAND ----------

# let's define a function to check if any column has missing values
def is_missing(value):
    return value == ''

# Filter out missing values
missing_values = subtraction_from_penalty.filter(is_missing)

# Count the missing values
num_missing_values = missing_values.count()

# Display the count of missing values
print("Number of missing values in column 'subtraction_from_penalty':", num_missing_values)

# COMMAND ----------

#likewise, let's initiate values in the column "penalty_amount_adjusted_for_eliminating_multiple_counting"
penalty_amount_adjusted_for_eliminating_multiple_counting = pharma_rdd_without_header.map(lambda row: parse_csv_row(row)[4])

# Display the column values
for value in penalty_amount_adjusted_for_eliminating_multiple_counting.collect():
    print(value)

# COMMAND ----------

# let's define a function to check if any column has missing values
def is_missing(value):
    return value == ''

# Filter out missing values
missing_values = penalty_amount_adjusted_for_eliminating_multiple_counting.filter(is_missing)

# Count the missing values
num_missing_values = missing_values.count()

# Display the count of missing values
print("Number of missing values in column 'penalty_amount_adjusted_for_eliminating_multiple_counting':", num_missing_values)

# COMMAND ----------

#likewise, let's initiate values in the column "penalty_year"
penalty_year = pharma_rdd_without_header.map(lambda row: parse_csv_row(row)[5])

# Display the column values
for value in penalty_year.collect():
    print(value)

# COMMAND ----------

# let's define a function to check if any column has missing values
def is_missing(value):
    return value == ''

# Filter out missing values
missing_values = penalty_year.filter(is_missing)

# Count the missing values
num_missing_values = missing_values.count()

# Display the count of missing values
print("Number of missing values in column 'penalty_year':", num_missing_values)

# COMMAND ----------

#likewise, let's initiate values in the column "penalty_date"
penalty_date = pharma_rdd_without_header.map(lambda row: parse_csv_row(row)[6])

# Display the column values
for value in penalty_date.collect():
    print(value)

# COMMAND ----------

import csv
from io import StringIO
from datetime import datetime

# Define a function to parse CSV rows
def parse_csv_row(row):
    return next(csv.reader(StringIO(row)))
# Define a function to convert the date format
def convert_date_format(date_str):
    try:
        if len(date_str) == 8:  # Check if the date has eight digits
            # Convert the date string to 'YYYY-MM-DD' format
            formatted_date = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:]}"
        elif len(date_str) == 7 and date_str.isdigit():  # Check if the date has seven digits and is all digits
            # Convert the date string to 'YYYY-MM-DD' format
            formatted_date = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:]}"
        else:
            return date_str  # Return the original date string if it doesn't match expected formats
        return formatted_date
    except ValueError:
        return date_str  # Return the original date string if not in expected formats
    
parsed_pharma_rdd = pharma_rdd.map(parse_csv_row)
# Apply the conversion function to the specified column and reconstruct the row
pharma_rdd_with_date_format = parsed_pharma_rdd.map(lambda row: row[:6] + [convert_date_format(row[6])] + row[7:] if len(row[6]) == 8 else row)

# Display the modified RDD
print(pharma_rdd_with_date_format.take(5))  # Display the first 5 rows as an example


# COMMAND ----------

# To get the values of the "penalty_date" column from each row in the dataset pharma_rdd_with_date_format
penalty_date = pharma_rdd_with_date_format.map(lambda row: row[6] if len(row) > 6 else None)

# Display the penalty_date column values
for value in penalty_date.collect():
    print(value)

# COMMAND ----------

# let's define a function to check if any column has missing values
def is_missing(value):
    return value == ''

# Filter out missing values
missing_values = penalty_date.filter(is_missing)

# Count the missing values
num_missing_values = missing_values.count()

# Display the count of missing values
print("Number of missing values in column 'penalty_date':", num_missing_values)

# COMMAND ----------

# To get the values of the "offense_group" column from each row in the dataset pharma_rdd_with_date_format
offense_group = pharma_rdd_with_date_format.map(lambda row: row[7] if len(row) > 7 else None)

# Display the offense_group column values
for value in offense_group.collect():
    print(value)

# COMMAND ----------

# let's define a function to check if any column has missing values
def is_missing(value):
    return value == ''

# Filter out missing values
missing_values = offense_group.filter(is_missing)

# Count the missing values
num_missing_values = missing_values.count()

# Display the count of missing values
print("Number of missing values in column 'offense_group':", num_missing_values)

# COMMAND ----------

# To get the values of the "primary_offence" column from each row in the dataset pharma_rdd_with_date_format
primary_offence = pharma_rdd_with_date_format.map(lambda row: row[8] if len(row) > 8 else None)

# Display the primary_offence column values
for value in primary_offence.collect():
    print(value)

# COMMAND ----------

# let's define a function to check if any column has missing values
def is_missing(value):
    return value == ''

# Filter out missing values
missing_values = primary_offence.filter(is_missing)

# Count the missing values
num_missing_values = missing_values.count()

# Display the count of missing values
print("Number of missing values in column 'primary_offence':", num_missing_values)

# COMMAND ----------

# To get the values of the "secondary_offence" column from each row in the dataset pharma_rdd_with_date_format
secondary_offence = pharma_rdd_with_date_format.map(lambda row: row[9] if len(row) > 9 else None)

# Display the secondary_offence column values
for value in secondary_offence.collect():
    print(value)

# COMMAND ----------

# let's define a function to check if any column has missing values
def is_missing(value):
    return value == ''

# Filter out missing values
missing_values = secondary_offence.filter(is_missing)

# Count the missing values
num_missing_values = missing_values.count()

# Display the count of missing values
print("Number of missing values in column 'secondary_offence':", num_missing_values)

# COMMAND ----------

# Since there is missing values in the column "secondary_offence", in order to replace it with none, let's first define a function to count missing values in the required column
def count_missing_values(row):
    # Check if the value is empty
    return 1 if row[9] == '' else 0

# To count the number of missing values
num_missing_values_before_transformation = pharma_rdd_with_date_format.filter(count_missing_values).count()

# Now, to replace the missing value columns with 'none', define another function: 
def replace_missing_with_none(row):
    # Check if the value is empty
    if row[9] == '':
        row[9] = 'None'  # Replace the empty value with 'None'
    return row

# since there are missing Replace missing values with 'None' in the parsed RDD
pharma_with_none = pharma_rdd_with_date_format.map(replace_missing_with_none)

# Count the number of missing values after replacement
num_missing_values_after_transformation = pharma_with_none.filter(lambda row: row[9] == '').count()

# Display the number of missing values before and after transformation
print("Number of missing values before replacement:", num_missing_values_before_transformation)
print("Number of missing values after replacement:", num_missing_values_after_transformation)

# COMMAND ----------

# To get the values of the "description" column from each row in the dataset pharma_with_none
description = pharma_with_none.map(lambda row: row[10] if len(row) > 10 else None)

# Display the description column values
for value in description.collect():
    print(value)

# COMMAND ----------

# let's define a function to check if any column has missing values
def is_missing(value):
    return value == ''

# Filter out missing values
missing_values = description.filter(is_missing)

# Count the missing values
num_missing_values = missing_values.count()

# Display the count of missing values
print("Number of missing values in column 'description':", num_missing_values)

# COMMAND ----------

# Since there is missing values in the column "description", in order to replace it with none, let's first define a function to count missing values in the required column
def count_missing_values(row):
    # Check if the value is empty
    return 1 if row[10] == '' else 0

# To count the number of missing values
num_missing_values_before_transformation = pharma_with_none.filter(count_missing_values).count()

# Now, to replace the missing value columns with 'none', define another function: 
def replace_missing_with_none(row):
    # Check if the value is empty
    if row[10] == '':
        row[10] = 'None'  # Replace the empty value with 'None'
    return row

# since there are missing Replace missing values with 'None' in the parsed RDD
pharma_with_none_description = pharma_with_none.map(replace_missing_with_none)

# Count the number of missing values after replacement
num_missing_values_after_transformation = pharma_with_none_description.filter(lambda row: row[10] == '').count()

# Display the number of missing values before and after transformation
print("Number of missing values before replacement:", num_missing_values_before_transformation)
print("Number of missing values after replacement:", num_missing_values_after_transformation)

# COMMAND ----------

# To get the values of the "level_of_government" column from each row in the dataset pharma_with_none_description
level_of_government = pharma_with_none_description.map(lambda row: row[11] if len(row) > 11 else None)

# Display the level_of_government column values
for value in level_of_government.collect():
    print(value)

# COMMAND ----------

# let's define a function to check if any column has missing values
def is_missing(value):
    return value == ''

# Filter out missing values
missing_values = level_of_government.filter(is_missing)

# Count the missing values
num_missing_values = missing_values.count()

# Display the count of missing values
print("Number of missing values in column 'level_of_government':", num_missing_values)

# COMMAND ----------

# To get the values of the "action_type" column from each row in the dataset pharma_with_none_description
action_type = pharma_with_none_description.map(lambda row: row[12] if len(row) > 12 else None)

# Display the action_type column values
for value in action_type.collect():
    print(value)

# COMMAND ----------

# let's define a function to check if any column has missing values
def is_missing(value):
    return value == ''

# Filter out missing values
missing_values = action_type.filter(is_missing)

# Count the missing values
num_missing_values = missing_values.count()

# Display the count of missing values
print("Number of missing values in column 'action_type':", num_missing_values)

# COMMAND ----------

# To get the values of the "agency" column from each row in the dataset pharma_with_none_description
agency = pharma_with_none_description.map(lambda row: row[13] if len(row) > 13 else None)

# Display the agency column values
for value in agency.collect():
    print(value)

# COMMAND ----------

# let's define a function to check if any column has missing values
def is_missing(value):
    return value == ''

# Filter out missing values
missing_values = agency.filter(is_missing)

# Count the missing values
num_missing_values = missing_values.count()

# Display the count of missing values
print("Number of missing values in column 'agency':", num_missing_values)

# COMMAND ----------

# Since there is missing values in the column "agency", in order to replace it with none, let's first define a function to count missing values in the required column
def count_missing_values(row):
    # Check if the value is empty
    return 1 if row[13] == '' else 0

# To count the number of missing values
num_missing_values_before_transformation = pharma_with_none_description.filter(count_missing_values).count()

# Now, to replace the missing value columns with 'none', define another function: 
def replace_missing_with_none(row):
    # Check if the value is empty
    if row[13] == '':
        row[13] = 'None'  # Replace the empty value with 'None'
    return row

# since there are missing Replace missing values with 'None' in the parsed RDD
pharma_with_none_agency = pharma_with_none_description.map(replace_missing_with_none)

# Count the number of missing values after replacement
num_missing_values_after_transformation = pharma_with_none_agency.filter(lambda row: row[13] == '').count()

# Display the number of missing values before and after transformation
print("Number of missing values before replacement:", num_missing_values_before_transformation)
print("Number of missing values after replacement:", num_missing_values_after_transformation)

# COMMAND ----------

# To get the values of the "civil_criminal" column from each row in the dataset pharma_with_none_agency
civil_criminal = pharma_with_none_agency.map(lambda row: row[14] if len(row) > 14 else None)

# Display the civil_criminal column values
for value in civil_criminal.collect():
    print(value)

# COMMAND ----------

# let's define a function to check if any column has missing values
def is_missing(value):
    return value == ''

# Filter out missing values
missing_values = civil_criminal.filter(is_missing)

# Count the missing values
num_missing_values = missing_values.count()

# Display the count of missing values
print("Number of missing values in column 'civil_criminal':", num_missing_values)

# COMMAND ----------

# To get the values of the "prosecution_agreement" column from each row in the dataset pharma_with_none_agency
prosecution_agreement = pharma_with_none_agency.map(lambda row: row[15] if len(row) > 15 else None)

# Display the prosecution_agreement column values
for value in prosecution_agreement.collect():
    print(value)

# COMMAND ----------

# let's define a function to check if any column has missing values
def is_missing(value):
    return value == ''

# Filter out missing values
missing_values = prosecution_agreement.filter(is_missing)

# Count the missing values
num_missing_values = missing_values.count()

# Display the count of missing values
print("Number of missing values in column 'prosecution_agreement':", num_missing_values)

# COMMAND ----------

# Since there is missing values in the column "prosecution_agreement", in order to replace it with none, let's first define a function to count missing values in the required column
def count_missing_values(row):
    # Check if the value is empty
    return 1 if row[15] == '' else 0

# To count the number of missing values
num_missing_values_before_transformation = pharma_with_none_agency.filter(count_missing_values).count()

# Now, to replace the missing value columns with 'none', define another function: 
def replace_missing_with_none(row):
    # Check if the value is empty
    if row[15] == '':
        row[15] = 'None'  # Replace the empty value with 'None'
    return row

# since there are missing Replace missing values with 'None' in the parsed RDD
pharma_with_none_prosecution_agreement = pharma_with_none_agency.map(replace_missing_with_none)

# Count the number of missing values after replacement
num_missing_values_after_transformation = pharma_with_none_prosecution_agreement.filter(lambda row: row[15] == '').count()

# Display the number of missing values before and after transformation
print("Number of missing values before replacement:", num_missing_values_before_transformation)
print("Number of missing values after replacement:", num_missing_values_after_transformation)

# COMMAND ----------

# To get the values of the "court" column from each row in the dataset pharma_with_none_prosecution_agreement
court = pharma_with_none_prosecution_agreement.map(lambda row: row[16] if len(row) > 16 else None)

# Display the "court" column values
for value in court.collect():
    print(value)

# COMMAND ----------

# let's define a function to check if any column has missing values
def is_missing(value):
    return value == ''

# Filter out missing values
missing_values = court.filter(is_missing)

# Count the missing values
num_missing_values = missing_values.count()

# Display the count of missing values
print("Number of missing values in column 'court':", num_missing_values)

# COMMAND ----------

# Since there is missing values in the column "court", in order to replace it with none, let's first define a function to count missing values in the required column
def count_missing_values(row):
    # Check if the value is empty
    return 1 if row[16] == '' else 0

# To count the number of missing values
num_missing_values_before_transformation = pharma_with_none_prosecution_agreement.filter(count_missing_values).count()

# Now, to replace the missing value columns with 'none', define another function: 
def replace_missing_with_none(row):
    # Check if the value is empty
    if row[16] == '':
        row[16] = 'None'  # Replace the empty value with 'None'
    return row

# since there are missing Replace missing values with 'None' in the parsed RDD
pharma_with_none_court = pharma_with_none_prosecution_agreement.map(replace_missing_with_none)

# Count the number of missing values after replacement
num_missing_values_after_transformation = pharma_with_none_court.filter(lambda row: row[16] == '').count()

# Display the number of missing values before and after transformation
print("Number of missing values before replacement:", num_missing_values_before_transformation)
print("Number of missing values after replacement:", num_missing_values_after_transformation)

# COMMAND ----------

# To get the values of the "case_ID" column from each row in the dataset pharma_with_none_court
case_ID = pharma_with_none_court.map(lambda row: row[17] if len(row) > 17 else None)

# Display the "case_ID" column values
for value in case_ID.collect():
    print(value)

# COMMAND ----------

# let's define a function to check if any column has missing values
def is_missing(value):
    return value == ''

# Filter out missing values
missing_values = case_ID.filter(is_missing)

# Count the missing values
num_missing_values = missing_values.count()

# Display the count of missing values
print("Number of missing values in column 'case_ID':", num_missing_values)

# COMMAND ----------

# Since there is missing values in the column "case_ID", in order to replace it with none, let's first define a function to count missing values in the required column
def count_missing_values(row):
    # Check if the value is empty
    return 1 if row[17] == '' else 0

# To count the number of missing values
num_missing_values_before_transformation = pharma_with_none_court.filter(count_missing_values).count()

# Now, to replace the missing value columns with 'none', define another function: 
def replace_missing_with_none(row):
    # Check if the value is empty
    if row[17] == '':
        row[17] = 'None'  # Replace the empty value with 'None'
    return row

# since there are missing Replace missing values with 'None' in the parsed RDD
pharma_with_none_case_ID = pharma_with_none_court.map(replace_missing_with_none)

# Count the number of missing values after replacement
num_missing_values_after_transformation = pharma_with_none_case_ID.filter(lambda row: row[17] == '').count()

# Display the number of missing values before and after transformation
print("Number of missing values before replacement:", num_missing_values_before_transformation)
print("Number of missing values after replacement:", num_missing_values_after_transformation)

# COMMAND ----------

# To get the values of the "private_litigation_case_title" column from each row in the dataset pharma_with_none_case_ID
private_litigation_case_title = pharma_with_none_case_ID.map(lambda row: row[18] if len(row) > 18 else None)

# Display the "private_litigation_case_title" column values
for value in private_litigation_case_title.collect():
    print(value)

# COMMAND ----------

# let's define a function to check if any column has missing values
def is_missing(value):
    return value == ''

# Filter out missing values
missing_values = private_litigation_case_title.filter(is_missing)

# Count the missing values
num_missing_values = missing_values.count()

# Display the count of missing values
print("Number of missing values in column 'private_litigation_case_title':", num_missing_values)

# COMMAND ----------

# Since there is missing values in the column "private_litigation_case_title", in order to replace it with none, let's first define a function to count missing values in the required column
def count_missing_values(row):
    # Check if the value is empty
    return 1 if row[18] == '' else 0

# To count the number of missing values
num_missing_values_before_transformation = pharma_with_none_case_ID.filter(count_missing_values).count()

# Now, to replace the missing value columns with 'none', define another function: 
def replace_missing_with_none(row):
    # Check if the value is empty
    if row[18] == '':
        row[18] = 'None'  # Replace the empty value with 'None'
    return row

# since there are missing Replace missing values with 'None' in the parsed RDD
pharma_with_none_private_litigation_case_title = pharma_with_none_case_ID.map(replace_missing_with_none)

# Count the number of missing values after replacement
num_missing_values_after_transformation = pharma_with_none_private_litigation_case_title.filter(lambda row: row[18] == '').count()

# Display the number of missing values before and after transformation
print("Number of missing values before replacement:", num_missing_values_before_transformation)
print("Number of missing values after replacement:", num_missing_values_after_transformation)

# COMMAND ----------

# To get the values of the "lawsuit_resolution" column from each row in the dataset pharma_with_none_private_litigation_case_title
lawsuit_resolution = pharma_with_none_private_litigation_case_title.map(lambda row: row[19] if len(row) > 19 else None)

# Display the "lawsuit_resolution" column values
for value in lawsuit_resolution.collect():
    print(value)

# COMMAND ----------

# let's define a function to check if any column has missing values
def is_missing(value):
    return value == ''

# Filter out missing values
missing_values = lawsuit_resolution.filter(is_missing)

# Count the missing values
num_missing_values = missing_values.count()

# Display the count of missing values
print("Number of missing values in column 'lawsuit_resolution':", num_missing_values)

# COMMAND ----------

# Since there is missing values in the column "lawsuit_resolution", in order to replace it with none, let's first define a function to count missing values in the required column
def count_missing_values(row):
    # Check if the value is empty
    return 1 if row[19] == '' else 0

# To count the number of missing values
num_missing_values_before_transformation = pharma_with_none_private_litigation_case_title.filter(count_missing_values).count()

# Now, to replace the missing value columns with 'none', define another function: 
def replace_missing_with_none(row):
    # Check if the value is empty
    if row[19] == '':
        row[19] = 'None'  # Replace the empty value with 'None'
    return row

# since there are missing Replace missing values with 'None' in the parsed RDD
pharma_with_none_lawsuit_resolution = pharma_with_none_private_litigation_case_title.map(replace_missing_with_none)

# Count the number of missing values after replacement
num_missing_values_after_transformation = pharma_with_none_lawsuit_resolution.filter(lambda row: row[19] == '').count()

# Display the number of missing values before and after transformation
print("Number of missing values before replacement:", num_missing_values_before_transformation)
print("Number of missing values after replacement:", num_missing_values_after_transformation)

# COMMAND ----------

# To get the values of the "facility_state" column from each row in the dataset pharma_with_none_lawsuit_resolution
facility_state = pharma_with_none_lawsuit_resolution.map(lambda row: row[20] if len(row) > 20 else None)

# Display the "facility_state" column values
for value in facility_state.collect():
    print(value)

# COMMAND ----------

# let's define a function to check if any column has missing values
def is_missing(value):
    return value == ''

# Filter out missing values
missing_values = facility_state.filter(is_missing)

# Count the missing values
num_missing_values = missing_values.count()

# Display the count of missing values
print("Number of missing values in column 'facility_state':", num_missing_values)

# COMMAND ----------

# Since there is missing values in the column "facility_state", in order to replace it with none, let's first define a function to count missing values in the required column
def count_missing_values(row):
    # Check if the value is empty
    return 1 if row[20] == '' else 0

# To count the number of missing values
num_missing_values_before_transformation = pharma_with_none_lawsuit_resolution.filter(count_missing_values).count()

# Now, to replace the missing value columns with 'none', define another function: 
def replace_missing_with_none(row):
    # Check if the value is empty
    if row[20] == '':
        row[20] = 'None'  # Replace the empty value with 'None'
    return row

# since there are missing Replace missing values with 'None' in the parsed RDD
pharma_with_none_facility_state = pharma_with_none_lawsuit_resolution.map(replace_missing_with_none)

# Count the number of missing values after replacement
num_missing_values_after_transformation = pharma_with_none_facility_state.filter(lambda row: row[20] == '').count()

# Display the number of missing values before and after transformation
print("Number of missing values before replacement:", num_missing_values_before_transformation)
print("Number of missing values after replacement:", num_missing_values_after_transformation)

# COMMAND ----------

# To get the values of the "city" column from each row in the dataset pharma_with_none_facility_state
city = pharma_with_none_lawsuit_resolution.map(lambda row: row[21] if len(row) > 21 else None)

# Display the "city" column values
for value in city.collect():
    print(value)

# COMMAND ----------

# let's define a function to check if any column has missing values
def is_missing(value):
    return value == ''

# Filter out missing values
missing_values = city.filter(is_missing)

# Count the missing values
num_missing_values = missing_values.count()

# Display the count of missing values
print("Number of missing values in column 'city':", num_missing_values)

# COMMAND ----------

# Since there is missing values in the column "city", in order to replace it with none, let's first define a function to count missing values in the required column
def count_missing_values(row):
    # Check if the value is empty
    return 1 if row[21] == '' else 0

# To count the number of missing values
num_missing_values_before_transformation = pharma_with_none_facility_state.filter(count_missing_values).count()

# Now, to replace the missing value columns with 'none', define another function: 
def replace_missing_with_none(row):
    # Check if the value is empty
    if row[21] == '':
        row[21] = 'None'  # Replace the empty value with 'None'
    return row

# since there are missing Replace missing values with 'None' in the parsed RDD
pharma_with_none_city = pharma_with_none_facility_state.map(replace_missing_with_none)

# Count the number of missing values after replacement
num_missing_values_after_transformation = pharma_with_none_city.filter(lambda row: row[21] == '').count()

# Display the number of missing values before and after transformation
print("Number of missing values before replacement:", num_missing_values_before_transformation)
print("Number of missing values after replacement:", num_missing_values_after_transformation)

# COMMAND ----------

# To get the values of the "address" column from each row in the dataset pharma_with_none_city
address = pharma_with_none_city.map(lambda row: row[22] if len(row) > 22 else None)

# Display the "address" column values
for value in address.collect():
    print(value)

# COMMAND ----------

# let's define a function to check if any column has missing values
def is_missing(value):
    return value == ''

# Filter out missing values
missing_values = address.filter(is_missing)

# Count the missing values
num_missing_values = missing_values.count()

# Display the count of missing values
print("Number of missing values in column 'address':", num_missing_values)

# COMMAND ----------

# Since there is missing values in the column "address", in order to replace it with none, let's first define a function to count missing values in the required column
def count_missing_values(row):
    # Check if the value is empty
    return 1 if row[22] == '' else 0

# To count the number of missing values
num_missing_values_before_transformation = pharma_with_none_city.filter(count_missing_values).count()

# Now, to replace the missing value columns with 'none', define another function: 
def replace_missing_with_none(row):
    # Check if the value is empty
    if row[22] == '':
        row[22] = 'None'  # Replace the empty value with 'None'
    return row

# since there are missing Replace missing values with 'None' in the parsed RDD
pharma_with_none_address = pharma_with_none_city.map(replace_missing_with_none)

# Count the number of missing values after replacement
num_missing_values_after_transformation = pharma_with_none_address.filter(lambda row: row[22] == '').count()

# Display the number of missing values before and after transformation
print("Number of missing values before replacement:", num_missing_values_before_transformation)
print("Number of missing values after replacement:", num_missing_values_after_transformation)

# COMMAND ----------

# To get the values of the "zip" column from each row in the dataset pharma_with_none_address
zip = pharma_with_none_address.map(lambda row: row[23] if len(row) > 23 else None)

# Display the "zip" column values
for value in zip.collect():
    print(value)

# COMMAND ----------

# let's define a function to check if any column has missing values
def is_missing(value):
    return value == ''

# Filter out missing values
missing_values = zip.filter(is_missing)

# Count the missing values
num_missing_values = missing_values.count()

# Display the count of missing values
print("Number of missing values in column 'zip':", num_missing_values)

# COMMAND ----------

# Since there is missing values in the column "zip", in order to replace it with none, let's first define a function to count missing values in the required column
def count_missing_values(row):
    # Check if the value is empty
    return 1 if row[23] == '' else 0

# To count the number of missing values
num_missing_values_before_transformation = pharma_with_none_address.filter(count_missing_values).count()

# Now, to replace the missing value columns with 'none', define another function: 
def replace_missing_with_none(row):
    # Check if the value is empty
    if row[23] == '':
        row[23] = 'None'  # Replace the empty value with 'None'
    return row

# since there are missing Replace missing values with 'None' in the parsed RDD
pharma_with_none_zip = pharma_with_none_address.map(replace_missing_with_none)

# Count the number of missing values after replacement
num_missing_values_after_transformation = pharma_with_none_zip.filter(lambda row: row[23] == '').count()

# Display the number of missing values before and after transformation
print("Number of missing values before replacement:", num_missing_values_before_transformation)
print("Number of missing values after replacement:", num_missing_values_after_transformation)

# COMMAND ----------

# To get the values of the "NAICS_code" column from each row in the dataset pharma_with_none_zip
NAICS_code = pharma_with_none_zip.map(lambda row: row[24] if len(row) > 24 else None)

# Display the "NAICS_code" column values
for value in NAICS_code.collect():
    print(value)

# COMMAND ----------

# let's define a function to check if any column has missing values
def is_missing(value):
    return value == ''

# Filter out missing values
missing_values = NAICS_code.filter(is_missing)

# Count the missing values
num_missing_values = missing_values.count()

# Display the count of missing values
print("Number of missing values in column 'NAICS_code':", num_missing_values)

# COMMAND ----------

# Since there is missing values in the column "NAICS_code", in order to replace it with none, let's first define a function to count missing values in the required column
def count_missing_values(row):
    # Check if the value is empty
    return 1 if row[24] == '' else 0

# To count the number of missing values
num_missing_values_before_transformation = pharma_with_none_zip.filter(count_missing_values).count()

# Now, to replace the missing value columns with 'none', define another function: 
def replace_missing_with_none(row):
    # Check if the value is empty
    if row[24] == '':
        row[24] = 'None'  # Replace the empty value with 'None'
    return row

# since there are missing Replace missing values with 'None' in the parsed RDD
pharma_with_none_NAICS_code = pharma_with_none_zip.map(replace_missing_with_none)

# Count the number of missing values after replacement
num_missing_values_after_transformation = pharma_with_none_NAICS_code.filter(lambda row: row[24] == '').count()

# Display the number of missing values before and after transformation
print("Number of missing values before replacement:", num_missing_values_before_transformation)
print("Number of missing values after replacement:", num_missing_values_after_transformation)

# COMMAND ----------

# To get the values of the "NAICS_translation" column from each row in the dataset pharma_with_none_NAICS_code
NAICS_translation = pharma_with_none_NAICS_code.map(lambda row: row[25] if len(row) > 25 else None)

# Display the "NAICS_translation" column values
for value in NAICS_translation.collect():
    print(value)

# COMMAND ----------

# let's define a function to check if any column has missing values
def is_missing(value):
    return value == ''

# Filter out missing values
missing_values = NAICS_translation.filter(is_missing)

# Count the missing values
num_missing_values = missing_values.count()

# Display the count of missing values
print("Number of missing values in column 'NAICS_translation':", num_missing_values)

# COMMAND ----------

# Since there is missing values in the column "NAICS_translation", in order to replace it with none, let's first define a function to count missing values in the required column
def count_missing_values(row):
    # Check if the value is empty
    return 1 if row[25] == '' else 0

# To count the number of missing values
num_missing_values_before_transformation = pharma_with_none_NAICS_code.filter(count_missing_values).count()

# Now, to replace the missing value columns with 'none', define another function: 
def replace_missing_with_none(row):
    # Check if the value is empty
    if row[25] == '':
        row[25] = 'None'  # Replace the empty value with 'None'
    return row

# since there are missing Replace missing values with 'None' in the parsed RDD
pharma_with_none_NAICS_translation = pharma_with_none_NAICS_code.map(replace_missing_with_none)

# Count the number of missing values after replacement
num_missing_values_after_transformation = pharma_with_none_NAICS_translation.filter(lambda row: row[24] == '').count()

# Display the number of missing values before and after transformation
print("Number of missing values before replacement:", num_missing_values_before_transformation)
print("Number of missing values after replacement:", num_missing_values_after_transformation)

# COMMAND ----------

# To get the values of the "HQ_country_of_parent" column from each row in the dataset pharma_with_none_NAICS_translation
HQ_country_of_parent = pharma_with_none_NAICS_translation.map(lambda row: row[26] if len(row) > 26 else None)

# Display the "HQ_country_of_parent" column values
for value in HQ_country_of_parent.collect():
    print(value)

# COMMAND ----------

# let's define a function to check if any column has missing values
def is_missing(value):
    return value == ''

# Filter out missing values
missing_values = HQ_country_of_parent.filter(is_missing)

# Count the missing values
num_missing_values = missing_values.count()

# Display the count of missing values
print("Number of missing values in column 'HQ_country_of_parent':", num_missing_values)

# COMMAND ----------

# To get the values of the "HQ_state_of_parent" column from each row in the dataset pharma_with_none_NAICS_translation
HQ_state_of_parent = pharma_with_none_NAICS_translation.map(lambda row: row[27] if len(row) > 27 else None)

# Display the "HQ_state_of_parent" column values
for value in HQ_state_of_parent.collect():
    print(value)

# COMMAND ----------

# let's define a function to check if any column has missing values
def is_missing(value):
    return value == ''

# Filter out missing values
missing_values = HQ_state_of_parent.filter(is_missing)

# Count the missing values
num_missing_values = missing_values.count()

# Display the count of missing values
print("Number of missing values in column 'HQ_state_of_parent':", num_missing_values)

# COMMAND ----------

# Since there is missing values in the column "HQ_state_of_parent", in order to replace it with none, let's first define a function to count missing values in the required column
def count_missing_values(row):
    # Check if the value is empty
    return 1 if row[27] == '' else 0

# To count the number of missing values
num_missing_values_before_transformation = pharma_with_none_NAICS_translation.filter(count_missing_values).count()

# Now, to replace the missing value columns with 'none', define another function: 
def replace_missing_with_none(row):
    # Check if the value is empty
    if row[27] == '':
        row[27] = 'None'  # Replace the empty value with 'None'
    return row

# since there are missing Replace missing values with 'None' in the parsed RDD
pharma_with_none_HQ_state_of_parent = pharma_with_none_NAICS_translation.map(replace_missing_with_none)

# Count the number of missing values after replacement
num_missing_values_after_transformation = pharma_with_none_HQ_state_of_parent.filter(lambda row: row[27] == '').count()

# Display the number of missing values before and after transformation
print("Number of missing values before replacement:", num_missing_values_before_transformation)
print("Number of missing values after replacement:", num_missing_values_after_transformation)

# COMMAND ----------

# To get the values of the "ownership_structure" column from each row in the dataset pharma_with_none_HQ_state_of_parent
ownership_structure = pharma_with_none_HQ_state_of_parent.map(lambda row: row[28] if len(row) > 28 else None)

# Display the "ownership_structure" column values
for value in ownership_structure.collect():
    print(value)

# COMMAND ----------

def is_missing(value):
    return value == ''

# Filter out missing values
missing_values = ownership_structure.filter(is_missing)

# Count the missing values
num_missing_values = missing_values.count()

# Display the count of missing values
print("Number of missing values in column 'ownership_structure':", num_missing_values)

# COMMAND ----------

# To get the values of the "parent_company_stock_ticker" column from each row in the dataset pharma_with_none_HQ_state_of_parent
parent_company_stock_ticker = pharma_with_none_HQ_state_of_parent.map(lambda row: row[29] if len(row) > 29 else None)

# Display the "parent_company_stock_ticker" column values
for value in parent_company_stock_ticker.collect():
    print(value)

# COMMAND ----------

def is_missing(value):
    return value == ''

# Filter out missing values
missing_values = parent_company_stock_ticker.filter(is_missing)

# Count the missing values
num_missing_values = missing_values.count()

# Display the count of missing values
print("Number of missing values in column 'parent_company_stock_ticker':", num_missing_values)

# COMMAND ----------

# Since there is missing values in the column "parent_company_stock_ticker", in order to replace it with none, let's first define a function to count missing values in the required column
def count_missing_values(row):
    # Check if the value is empty
    return 1 if row[29] == '' else 0

# To count the number of missing values
num_missing_values_before_transformation = pharma_with_none_HQ_state_of_parent.filter(count_missing_values).count()

# Now, to replace the missing value columns with 'none', define another function: 
def replace_missing_with_none(row):
    # Check if the value is empty
    if row[29] == '':
        row[29] = 'None'  # Replace the empty value with 'None'
    return row

# since there are missing Replace missing values with 'None' in the parsed RDD
pharma_with_none_parent_company_stock_ticker = pharma_with_none_HQ_state_of_parent.map(replace_missing_with_none)

# Count the number of missing values after replacement
num_missing_values_after_transformation = pharma_with_none_parent_company_stock_ticker.filter(lambda row: row[29] == '').count()

# Display the number of missing values before and after transformation
print("Number of missing values before replacement:", num_missing_values_before_transformation)
print("Number of missing values after replacement:", num_missing_values_after_transformation)

# COMMAND ----------

# To get the values of the "parent_company_stock_ticker" column from each row in the dataset pharma_with_none_parent_company_stock_ticker
major_industry_of_parent = pharma_with_none_parent_company_stock_ticker.map(lambda row: row[30] if len(row) > 30 else None)

# Display the "major_industry_of_parent" column values
for value in major_industry_of_parent.collect():
    print(value)

# COMMAND ----------

def is_missing(value):
    return value == ''

# Filter out missing values
missing_values = major_industry_of_parent.filter(is_missing)

# Count the missing values
num_missing_values = missing_values.count()

# Display the count of missing values
print("Number of missing values in column 'major_industry_of_parent':", num_missing_values)

# COMMAND ----------

# To get the values of the "specific_industry_of_parent" column from each row in the dataset pharma_with_none_parent_company_stock_ticker
specific_industry_of_parent = pharma_with_none_parent_company_stock_ticker.map(lambda row: row[31] if len(row) > 31 else None)

# Display the "specific_industry_of_parent" column values
for value in specific_industry_of_parent.collect():
    print(value)

# COMMAND ----------

def is_missing(value):
    return value == ''

# Filter out missing values
missing_values = specific_industry_of_parent.filter(is_missing)

# Count the missing values
num_missing_values = missing_values.count()

# Display the count of missing values
print("Number of missing values in column 'specific_industry_of_parent':", num_missing_values)

# COMMAND ----------

# To get the values of the "info_source" column from each row in the dataset pharma_with_none_parent_company_stock_ticker
info_source = pharma_with_none_parent_company_stock_ticker.map(lambda row: row[32] if len(row) > 32 else None)

# Display the "info_source" column values
for value in info_source.collect():
    print(value)

# COMMAND ----------

def is_missing(value):
    return value == ''

# Filter out missing values
missing_values = info_source.filter(is_missing)

# Count the missing values
num_missing_values = missing_values.count()

# Display the count of missing values
print("Number of missing values in column 'info_source':", num_missing_values)

# COMMAND ----------

# Since there is missing values in the column "infor_source", in order to replace it with none, let's first define a function to count missing values in the required column
def count_missing_values(row):
    # Check if the value is empty
    return 1 if row[32] == '' else 0

# To count the number of missing values
num_missing_values_before_transformation = pharma_with_none_parent_company_stock_ticker.filter(count_missing_values).count()

# Now, to replace the missing value columns with 'none', define another function: 
def replace_missing_with_none(row):
    # Check if the value is empty
    if row[32] == '':
        row[32] = 'None'  # Replace the empty value with 'None'
    return row

# since there are missing Replace missing values with 'None' in the parsed RDD
pharma_with_none_info_source = pharma_with_none_parent_company_stock_ticker.map(replace_missing_with_none)

# Count the number of missing values after replacement
num_missing_values_after_transformation = pharma_with_none_info_source.filter(lambda row: row[32] == '').count()

# Display the number of missing values before and after transformation
print("Number of missing values before replacement:", num_missing_values_before_transformation)
print("Number of missing values after replacement:", num_missing_values_after_transformation)

# COMMAND ----------

# To get the values of the "notes" column from each row in the dataset pharma_with_none_info_source
notes = pharma_with_none_info_source.map(lambda row: row[33] if len(row) > 33 else None)

# Display the "notes" column values
for value in notes.collect():
    print(value)

# COMMAND ----------

def is_missing(value):
    return value == ''

# Filter out missing values
missing_values = notes.filter(is_missing)

# Count the missing values
num_missing_values = missing_values.count()

# Display the count of missing values
print("Number of missing values in column 'notes':", num_missing_values)

# COMMAND ----------

# Since there is missing values in the column "notes", in order to replace it with none, let's first define a function to count missing values in the required column
def count_missing_values(row):
    # Check if the value is empty
    return 1 if row[33] == '' else 0

# To count the number of missing values
num_missing_values_before_transformation = pharma_with_none_info_source.filter(count_missing_values).count()

# Now, to replace the missing value columns with 'none', define another function: 
def replace_missing_with_none(row):
    # Check if the value is empty
    if row[33] == '':
        row[33] = 'None'  # Replace the empty value with 'None'
    return row

# since there are missing Replace missing values with 'None' in the parsed RDD
pharma_with_none_notes = pharma_with_none_info_source.map(replace_missing_with_none)

# Count the number of missing values after replacement
num_missing_values_after_transformation = pharma_with_none_notes.filter(lambda row: row[33] == '').count()

# Display the number of missing values before and after transformation
print("Number of missing values before replacement:", num_missing_values_before_transformation)
print("Number of missing values after replacement:", num_missing_values_after_transformation)

# COMMAND ----------

#Now, after the replacement of the missing values for the complete columns in the dataset pharma_with_none_notes, lets review the first frw rows and the row count of it

pharma_with_none_notes.take(5)

# Assuming pharma_with_none_notes is your RDD
row_count = pharma_with_none_notes.count()

print("Row count of pharma_with_none_notes RDD:", row_count)


# COMMAND ----------

#Now, let's begin doing the actual tasks given
# Completing the tasks using Resilient Distributed Dataset using pyspark
# Task 1: The number of studies in the dataset. You must ensure that you explicitly checkdistinct studies.
#since this is only depending upon the first dataset clinicaltrial_2023, lets consider that in order to do this task
#just to double confirm that only required rows are coming in the dataset 
# Add index to each row
rows_with_index = final_rdd_clinicaltrial_2023.zipWithIndex()

# Filter rows where 'Id' column contains 'Id' and is not None
rows_with_valid_type = rows_with_index.filter(lambda row_index: row_index[0][0] and 'Id' in row_index[0][0])

# Print the row number and rows
for row_index, row in rows_with_valid_type.collect():
    print("Row number:", row_index)
    print("Row content:", row)

# COMMAND ----------

#Since, we have completed all the five tasks in Resilient Distributed Dataset in pyspark using python, now, lets try the same using Dataframe in pyspark using python
#start by converting the clinicaltrial_2023_filtered into a dataframe, for that:
# Import necessary libraries
from pyspark.sql import SparkSession

# Creating a SparkSession 
spark = SparkSession.builder \
    .appName("Convert Resilient Distributed Dataset to a DataFrame") \
    .getOrCreate()

# Define the schema for the DataFrame
schema = ["Id", "study title", "acronym", "status", "conditions", "interventions",
          "sponsor", "collaborators", "Enrollment", "Funder Type", "Type",
          "Study Design", "Start", "Completion"]

# Convert the final_rdd_clinicaltrial_2023 to DataFrame
clinicaltrial_2023_dataframe = final_rdd_clinicaltrial_2023.toDF(schema)

# Show the DataFrame
clinicaltrial_2023_dataframe.show(5)

# COMMAND ----------

#Since we have completed the whole tasks by using the dataframe pyspark dataset, now let's focus on using the SQl table for doing it. Hence, needs to register the dataframe as a temporary view.
clinicaltrial_2023_dataframe.createOrReplaceTempView("clinicaltrial_2023_table")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- View the count of the table we created, to double check we didn't miss any in between
# MAGIC SELECT COUNT(*) FROM clinicaltrial_2023_table

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM clinicaltrial_2023_table; -- print the values in the SQL table

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Consider Task1:The number of studies in the dataset. You must ensure that you explicitly checkdistinct studies
# MAGIC SELECT COUNT(DISTINCT Id) AS num_studies
# MAGIC FROM clinicaltrial_2023_table
# MAGIC WHERE Id != 'Id'; -- ignoring the row containing the values as attributed

# COMMAND ----------

# MAGIC %sql
# MAGIC -- consider Task 2: You should list all the types (as contained in the Type column) of studies in thedataset along with the frequencies of each type. These should be ordered frommost frequent to least frequent.
# MAGIC SELECT Type, COUNT(*) AS frequency
# MAGIC FROM clinicaltrial_2023_table
# MAGIC WHERE Type IS NOT NULL AND Type != 'Type' -- ignoring the Type value count since its insignificant
# MAGIC GROUP BY Type
# MAGIC ORDER BY frequency DESC;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Consider Task 3: The top 5 conditions (from Conditions) with their frequencies.
# MAGIC
# MAGIC SELECT condition, COUNT(*) AS frequency
# MAGIC FROM (
# MAGIC     SELECT EXPLODE(SPLIT(conditions, "\\|")) AS condition
# MAGIC     FROM clinicaltrial_2023_table
# MAGIC     WHERE conditions IS NOT NULL
# MAGIC )
# MAGIC GROUP BY condition
# MAGIC ORDER BY frequency DESC
# MAGIC LIMIT 5;
# MAGIC

# COMMAND ----------

# consider the task 4:Find the 10 most common sponsors that are not pharmaceutical companies, alongwith the number of clinical trials they have sponsored. Hint: For a basicimplementation, you can assume that the Parent Company column contains allpossible pharmaceutical companies using Dataframe clinicaltrial_2023_dataframe
# let's start by converting the pharma_with_none_notes RDD into dataframe so as to take it for the task

# Import necessary libraries from pyspark to create the spark session
from pyspark.sql import SparkSession

# Creating the SparkSession 
spark = SparkSession.builder \
    .appName("Convert Resilient Distributed Dataset to a DataFrame") \
    .getOrCreate()

# Define the schema for the DataFrame pharma_dataframe
schema = ["Company",
  "Parent_Company",
  "Penalty_Amount",
  "Subtraction_From_Penalty",
  "Penalty_Amount_Adjusted_For_Eliminating_Multiple_Counting",
  "Penalty_Year",
  "Penalty_Date",
  "Offense_Group",
  "Primary_Offense",
  "Secondary_Offense",
  "Description",
  "Level_of_Government",
  "Action_Type",
  "Agency",
  "Civil/Criminal",
  "Prosecution_Agreement",
  "Court",
  "Case_ID",
  "Private_Litigation_Case_Title",
  "Lawsuit_Resolution",
  "Facility_State",
  "City",
  "Address",
  "Zip",
  "NAICS_Code",
  "NAICS_Translation",
  "HQ_Country_of_Parent",
  "HQ_State_of_Parent",
  "Ownership_Structure",
  "Parent_Company_Stock_Ticker",
  "Major_Industry_of_Parent",
  "Specific_Industry_of_Parent",
  "Info_Source",
  "Notes"]

# Converting the pharma_with_none_notes to DataFrame pharma_dataframe
pharma_dataframe = pharma_with_none_notes.toDF(schema)

# viewing the first 5 rows of it
pharma_dataframe.show(5)

# COMMAND ----------

# For this, we need to create the temporary view of the pharma_dataframe as well in order to create it into a SQL table for usage

pharma_dataframe.createOrReplaceTempView("pharma_table")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- View the count of the table we created, to double check we didn't miss any in between
# MAGIC SELECT COUNT(*) FROM pharma_table

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM pharma_table; -- print the values in the SQL table

# COMMAND ----------

# MAGIC %sql
# MAGIC -- In order to find the distinct Pharmaceutical companies, as mentioned in the task we can assume that all the Parent Company column contains pharmaceutical company values from the pharma_tables SQL Table.
# MAGIC SELECT DISTINCT Parent_Company
# MAGIC FROM pharma_table
# MAGIC WHERE Parent_Company IS NOT NULL;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Now in order to get the list of sponsers, lets query accordingly
# MAGIC SELECT Sponsor, COUNT(*) AS Num_Clinical_Trials
# MAGIC FROM clinicaltrial_2023_table
# MAGIC WHERE Sponsor NOT IN (SELECT DISTINCT Parent_Company FROM pharma_table WHERE Parent_Company IS NOT NULL)
# MAGIC GROUP BY Sponsor
# MAGIC ORDER BY Num_Clinical_Trials DESC
# MAGIC LIMIT 10;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Consider Task 5: Plot number of completed studies for each month in 2023. You need to include yourvisualization as well as a table of all the values you have plotted for each month.
# MAGIC
# MAGIC -- Create a temporary table to store the aggregated data
# MAGIC CREATE TEMPORARY VIEW Completed_Studies_Pr_Month AS
# MAGIC SELECT SUBSTRING(Completion, 1, 7) AS Month, COUNT(*) AS Num_Completed_Studies
# MAGIC FROM clinicaltrial_2023_table
# MAGIC WHERE Completion LIKE '2023-%'
# MAGIC GROUP BY SUBSTRING(Completion, 1, 7)
# MAGIC ORDER BY Month;
# MAGIC
# MAGIC -- Retrieve the aggregated data
# MAGIC SELECT * FROM Completed_Studies_Pr_Month;
# MAGIC

# COMMAND ----------

#visualize it using Python libraries such as Matplotlib or Pandas.
import matplotlib.pyplot as plt

# Query to retrieve data from the temporary view
data = spark.sql("SELECT * FROM Completed_Studies_Per_Month").collect()

# Extracting month and number of completed studies from the data
months = [row['Month'] for row in data]
num_completed_studies = [row['Num_Completed_Studies'] for row in data]

# Plotting the data
plt.figure(figsize=(10, 6))
plt.bar(months, num_completed_studies, color='skyblue')
plt.title('Number of Completed Studies for Each Month in 2023')
plt.xlabel('Month')
plt.ylabel('Number of Completed Studies')
plt.xticks(rotation=45)
plt.grid(axis='y', linestyle='--', alpha=0.7)
plt.tight_layout()
plt.show()

