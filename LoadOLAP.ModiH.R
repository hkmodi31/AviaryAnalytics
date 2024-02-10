#Practicum-II (Part-II)
#Author - Modi, Harshkumar
#Term - Fall 2023

# Load the required package
library(RMySQL)
library(RSQLite)
library(dplyr)
library(lubridate)

#Connecting to local SQLite database
conn <- dbConnect(RSQLite::SQLite(), dbname = "sales.db")

#Getting data as required from the database
data_df <- dbGetQuery(conn, "SELECT productID, sales.repID, date, quantity, total, territory as region, reps.firstName, reps.lastName, commission FROM sales LEFT JOIN reps ON sales.repID=reps.repID;")

new_df <-  data_df %>%
  mutate(date = as.Date(date, format = "%m/%d/%Y")) %>%
  mutate(
    year = year(date),
    quarter = quarter(date)
  )

#Formatting sales_df to extract years & quarters. Also adding regiontotal, quarterYearTotal and unitsPerRegion (units sold)
sales_df <- data_df %>%
  mutate(date = as.Date(date, format = "%m/%d/%Y")) %>%
  mutate(
    year = year(date),
    quarter = quarter(date)
  ) %>%
  mutate(
    regionTotal = ave(total, productID, quarter, year, region, FUN = sum),
    quarterYearTotal = ave(total, productID, quarter, year, FUN = sum),
    unitsPerRegion = ave(quantity, productID, quarter, year, region, FUN = sum)
  )



#Extracting data for dimensions table
products_df <- dbGetQuery(conn, "SELECT * from products")
reps_df <- dbGetQuery(conn, "SELECT repID, territory, commission from reps")
reps_df <- reps_df[c('repID', 'territory', 'commission')]

#Formatting reps_facts data to add repTotalSold, repQuarterYearSold and repProductSold
repsFacts_df <- sales_df[c('repID', 'firstName', 'lastName', 'region', 'productID', 'quarter', 'year', 'total', 'quantity')]

repsFacts_df <- repsFacts_df %>%
  mutate(    
    repTotalSold = ave(total, repID, FUN = sum),
    repQuarterYearSold = ave(total, repID, productID, quarter, year, FUN = sum),
    repProductSold = ave(total, repID, productID, FUN = sum)
  )

repsFacts_df <- 
  unique(repsFacts_df[c('repID', 'firstName', 'lastName', 'productID','quarter', 'year', 'repTotalSold', 'repQuarterYearSold', 'repProductSold')])

#Adding primary key (unique identifier) for both facts table
repsFacts_df <- repsFacts_df %>%
  mutate(rid = row_number())
salesFacts_df <- unique(sales_df[c('productID', 'region', 'quarter', 'year','regionTotal','quarterYearTotal','unitsPerRegion')])
salesFacts_df <- salesFacts_df %>%
  mutate(saleID=row_number())

# Connecting to the MySQL database
mysql_db <- dbConnect(RMySQL::MySQL(), 
                      host='salesdatabase.ccgfbvk5hpfc.us-east-1.rds.amazonaws.com', 
                      port=3306,
                      username='admin', 
                      password='CS5200Fall23!')

#Empyting the database
dbExecute(mysql_db, "DROP DATABASE IF EXISTS salesdb")
dbIsValid(mysql_db)
dbExecute(mysql_db, "CREATE DATABASE IF NOT EXISTS salesdb;")
dbExecute(mysql_db, "USE salesdb;")



#Creating dimensions tables

create_products_sql <- "CREATE TABLE IF NOT EXISTS products_dimensions (
  productID INT PRIMARY KEY,
  product VARCHAR(255) NOT NULL
);"

create_reps_sql <- "CREATE TABLE IF NOT EXISTS reps_dimensions (
  repID INT PRIMARY KEY,
  territory VARCHAR(255) NOT NULL,
  commission NUMERIC
);"


#Creating facts tables
create_sales_facts <- "CREATE TABLE IF NOT EXISTS sales_facts (
  saleID INT PRIMARY KEY,
  productID INT NOT NULL,
  quarter INT NOT NULL,
  year INT NOT NULL,
  region VARCHAR(255),
  regionTotal INT,
  quarterYearTotal INT,
  unitsPerRegion INT,
  FOREIGN KEY(productID) REFERENCES products_dimensions(productID)
);"


create_reps_facts <- "CREATE TABLE IF NOT EXISTS reps_facts (
  rid INT PRIMARY KEY,
  repID INT,
  firstName VARCHAR(255),
  lastName VARCHAR(255),
  productID INT NOT NULL,
  quarter INT NOT NULL,
  year INT NOT NULL,
  repTotalSold INT,
  repQuarterYearSold INT,
  repProductSold INT,
  FOREIGN KEY(productID) REFERENCES products_dimensions(productID),
  FOREIGN KEY(repID) REFERENCES reps_dimensions(repID)
);"

#Creating the tables in the database
dbExecute(mysql_db, create_products_sql)
dbExecute(mysql_db, create_reps_sql)
dbExecute(mysql_db, create_reps_facts)
dbExecute(mysql_db, create_sales_facts)


#Populating the database
dbWriteTable(mysql_db, "products_dimensions", products_df, append = TRUE, row.names = FALSE)
dbWriteTable(mysql_db, "reps_dimensions", reps_df, append = TRUE, row.names = FALSE)
dbWriteTable(mysql_db, "sales_facts", salesFacts_df, append = TRUE, row.names = FALSE)
dbWriteTable(mysql_db, "reps_facts", repsFacts_df, append = TRUE, row.names = FALSE)

#Disconnecting both databases
dbDisconnect(mysql_db)
dbDisconnect(conn)

