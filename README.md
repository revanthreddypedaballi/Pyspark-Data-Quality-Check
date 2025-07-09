# Pyspark-Data-Quality-Check
This project demonstrates how to perform data quality checks and cleansing using PySpark on three datasets namely 'orders', 'products', 'customers'. 
The pipeline includes loading data, cleaning columns, identifying invlaid records and writing clean valid data and invalid datasets to output folder in parquet form.

#Technologies Used

1) Apache Spark (PySpark)
2) Python3
3) Parquet File Format

#Datasets Used Overview 
1) orders_dirty.csv
2) products_dirty.csv
3) customers_dirty.csv

Each dataset may contain inconsistent values such as null vlaues, invalid prices, invalid emails or poorly formatted strings.

# Pipeline Steps 

1) Intializing Spark Session
2) Loading and reading all three datsets with schema inferrence and headers.
3) Data Cleaning 1) Trimming and Captiliizng 'product_name' and 'customer_name'
                 2) Calculating order_value nad rounding it to 2 decimals
                 3) Adding a new column 'loyalty_tier' based on order_value
4) Data Caching, Caching the resulting dataframe for performnce improvemnet
5) Data Quality Cchecks a) Null Checks : counting and executing null values per column
                        b) Empty Strings : Detecting empty strings in string columns
                        c) Invalid prices : detecting nad executing invalid prices such as null and negative values.
                        d) Invalid Emails : Regex pattern check
                        e) Inavlid Categories Allowing only categories such as 'electronics', 'furniture', 'stationary'.
6)Invalid Records Summary, Combining all invalid conditions into one dataframe.
7)Clean Dataset, Filtering only valid records.
8)output 1) Saves clean dta to `output/clean_dataset/` (Parquet)
         2) Saves invalid records to `output/invalid_sum/` (Parquet)
# Output Structure:
output/
├── clean_dataset/
│ └── part-.parquet
├── invalid_sum/
│ └── part-.parquet

 

