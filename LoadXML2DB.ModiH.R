#Practicum-II (Part-I)
#Author - Modi, Harshkumar
#Term - Fall 2023

#Importing required libraries
library(XML)
library(dplyr)
library(DBI)
library(RSQLite)


# Parsing the XML data
xml_data_reps <- xmlParse("./txn-xml/pharmaReps-F23.xml")

# Extracting the 'firstName' and 'lastName'
nameNodes_df <- xmlToDataFrame(getNodeSet(xml_data_reps, "//rep/name"))
colnames(nameNodes_df) <- c('firstName', 'lastName')

# Extracting the 'territory' and 'commission'
repsData <- xmlToDataFrame(getNodeSet(xml_data_reps, "//rep"))
repsData <- repsData[c("territory", "commission")]

# Extracting the 'rID' attribute and storing it as 'repID'
repNodes <- getNodeSet(xml_data_reps, "//rep")
repIDs <- sapply(repNodes, function(node) sub("^r", "", xmlGetAttr(node, "rID")))
repIDs_df <- data.frame(repID = repIDs)

# Combining the dataframes
repsData_combined <- cbind(repIDs_df, nameNodes_df, repsData)

reps_df <- repsData_combined[c("repID","firstName", "lastName", "territory", "commission")]


# Function to extract data from each txn node
extract_txn_data <- function(txn_node, file_name) {
  data.frame(
    txnID = xmlGetAttr(txn_node, "txnID"),
    repID = xmlGetAttr(txn_node, "repID"),
    customer = xmlValue(getNodeSet(txn_node, "customer")[[1]]),
    country = xmlValue(getNodeSet(txn_node, "country")[[1]]),
    date = xmlValue(getNodeSet(txn_node, "sale/date")[[1]]),
    product = xmlValue(getNodeSet(txn_node, "sale/product")[[1]]),
    quantity = as.numeric(xmlValue(getNodeSet(txn_node, "sale/qty")[[1]])),
    total = as.numeric(xmlValue(getNodeSet(txn_node, "sale/total")[[1]])),
    fileName = file_name  # Add the filename to the data frame
  )
}

#Creating an empty dataframe
all_transactions <- data.frame()

#Setting folder path with xml-data
folder_path <- "./txn-xml/"

#Getting list of all files with the required alias
file_list <- list.files(path = folder_path, pattern = "pharmaSalesTxn.*\\.xml$", full.names = TRUE)

for (file in file_list){
  
  # Extract just the filename without the path for inclusion in the dataframe
  file_name <- basename(file)
  
  doc <- xmlParse(file)
  # Extracting all txn nodes
  txn_nodes <- getNodeSet(doc, "//txn")
  
  # Applying the function to each txn node and combining into a dataframe
  transactions_df <- do.call(rbind, lapply(txn_nodes, extract_txn_data, file_name))
  
  #Stacking them on to the empty dataframe
  all_transactions <- rbind(all_transactions, transactions_df)
}

row.names(all_transactions) <- NULL


#Adding unique identifiers to columns as per the database schema
all_transactions <- all_transactions %>%
  mutate(
    saleID = row_number(),
    productID = as.integer(factor(product, levels = unique(product))),
    customerCountryID = as.integer(factor(paste(customer, country, sep = "-")))
  )

colnames(all_transactions)[which(names(all_transactions) == 'customerCountryID')] <- 'customerID'

product_df <- unique(all_transactions[c('productID','product')])
customer_df <- unique(all_transactions[c('customerID','customer', 'country')])
sale_df <- unique(all_transactions[c('saleID','productID','customerID','repID','txnID','date','quantity','total','fileName')])

print("All dataframes created")

conn <- dbConnect(RSQLite::SQLite(), dbname = "sales.db")

#Empyting the database
dbExecute(conn, "DROP TABLE IF EXISTS sales")
dbExecute(conn, "DROP TABLE IF EXISTS customers")
dbExecute(conn, "DROP TABLE IF EXISTS reps")
dbExecute(conn, "DROP TABLE IF EXISTS products")

print("All tables dropped")

#Creating data tables as per schema
create_products_sql <- "CREATE TABLE IF NOT EXISTS products (
  productID INTEGER PRIMARY KEY,
  product TEXT NOT NULL
);"

create_customers_sql <- "CREATE TABLE IF NOT EXISTS customers (
  customerID INTEGER PRIMARY KEY,
  customer TEXT NOT NULL,
  country TEXT NOT NULL
);"

create_sales_sql <- "CREATE TABLE IF NOT EXISTS sales (
  saleID INT PRIMARY KEY,
  productID INTEGER NOT NULL,
  customerID INTEGER NOT NULL,
  repID INTEGER NOT NULL,
  txnID INTEGER NOT NULL,
  date TEXT NOT NULL,
  quantity INTEGER NOT NULL,
  total REAL NOT NULL,
  fileName TEXT NOT NULL,
  FOREIGN KEY(productID) REFERENCES products(productID),
  FOREIGN KEY(customerID) REFERENCES customers(customerID),
  FOREIGN KEY(repID) REFERENCES reps(repID)
);"

create_reps_sql <- "CREATE TABLE IF NOT EXISTS reps (
  repID INTEGER PRIMARY KEY,
  firstName TEXT NOT NULL,
  lastName TEXT NOT NULL,
  territory TEXT NOT NULL,
  commission REAL
);"

# Executing the data tables creation
dbExecute(conn, create_products_sql)
dbExecute(conn, create_reps_sql)
dbExecute(conn, create_customers_sql)
dbExecute(conn, create_sales_sql)

#Populating the tables with the required data
dbWriteTable(conn, "products", product_df, append = TRUE, row.names = FALSE)
dbWriteTable(conn, "reps", reps_df, append = TRUE, row.names = FALSE)
dbWriteTable(conn, "customers", customer_df, append = TRUE, row.names = FALSE)
dbWriteTable(conn, "sales", sale_df, append = TRUE, row.names = FALSE)

print("All tables are loaded with data")

# Close the database connection
dbDisconnect(conn)
