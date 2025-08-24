#!/bin/bash
set -e

DB=airflow_db
TABLE=data_audible

echo "Waiting for database $DB..."
until mysql -u root -p"$MYSQL_ROOT_PASSWORD" -e "USE $DB"; do
  sleep 1
done

echo "Importing CSV data..."
mysql --local-infile=1 -u root -p"$MYSQL_ROOT_PASSWORD" $DB <<EOF
LOAD DATA LOCAL INFILE '/docker-entrypoint-initdb.d/raw_data.csv'
INTO TABLE $TABLE
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;
EOF
echo "CSV import done."
