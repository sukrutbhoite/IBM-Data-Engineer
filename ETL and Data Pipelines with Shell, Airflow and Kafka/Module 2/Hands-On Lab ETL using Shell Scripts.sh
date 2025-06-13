#! /bin/bash

# This script
# Extracts data from /etc/passwd file into a CSV file.

# The csv data file contains the user name, user id and
# home directory of each user account defined in /etc/passwd

# Transforms the text delimiter from ":" to ",".
# Loads the data from the CSV file into a table in PostgreSQL database.


# Extarct Phase

echo "Extracting Data"

# Extracting column 1(user name), 2(user id), 6(home directory path) from /etc/passwd

cut -d":" -f1,3,6 /etc/passwd > extracted_data.txt


# Transform Phase

echo "Transforming Data"

# Read the extracted data and replace colon with commas.

tr ":" "," < extracted_data.txt > transformed_data.csv


# Load phase

echo "Loading data"

# Set the PostgreSQL password environment variable.

export PGPASSWORD='hyM6AG99WuHwCuKpk97TySWQ';

# Send the instructions to connect to 'template1' and
# copy the file to the table 'users' through command pipeline.

echo "\c template1;\COPY users  FROM '/home/project/transformed_data.csv' DELIMITERS ',' CSV;" | psql --username=postgres --host=postgres


echo "SELECT * FROM users;" | psql --username=postgres --host=postgres template1