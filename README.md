# Exploring New York City Taxi Data with Azure


## Introduction 

The New York City Taxi and Limousine Commission (TLC) generously provides us with lots of data about taxi rides in the city. This data includes things like how much people paid for their rides, how long the trips took, and which part of the city they went to. In this project, we're using Azure, a cloud service, to do some cool stuff with this data. We want to clean it up, make it easier to understand, and then use it to create a cool Power BI dashboard to compare different things about taxi rides.


### Project Objectives 

1. **Data Ingestion:** We want to grab the monthly data from NYC.org, starting from 2012 until now, and save it in a special place using Azure Data Factory (ADF). 
2. **Data Storage:** Once we have the data, we'll keep it safe in something called "Azure Data Lake Storage" in a special container.
3. **Data Cleaning and Transformation:** We'll use a tool called Azure Databricks to clean and change the data, making it neater and more useful. Then, we'll store it in a different place as "Delta tables."
4. **Data Modelling:** Add new Dimension tables for Payment Mode, Rate, Borough and Zone, Vendor, and Calendar. 
5. **Data Processing and Warehousing:** Process and aggregate data, and in an Azure SQL Database. 
6. **Data Visualization:** Create a Power BI dashboard for visualizing and comparing various aspects of taxi rides. 

## Solution Architecture

![NYC solution diagram](https://github.com/Shakti93/nyc-taxi-project/assets/84408451/528b297c-fe6e-401c-b660-e8b017d2abf3)

## Data Pipeline Implementation

![NYC Taxi ADF Pipeline](https://github.com/Shakti93/nyc-taxi-project/assets/84408451/44b2257d-8a53-4e4e-893c-552b49c52338)

1. **Data Sources:** We get our data from two places, NYC.org and GitHub. They have files about taxi trips in a special format called "Parquet" from 2012 until now. Some other data, like payment methods and dates, is in simple CSV files on GitHub.
2. **Data Ingestion:** The data is extracted from NYC.org and Github Repository using HTTP Protocol Linked Service in Azure Data Factory to Azure Data Lake Storage and stored in the NYC raw container. ADF pipelines are created to fetch monthly data on 1st day of each month using the Tumbling Window Trigger. 
3. **Data Transformation:** The raw data is transformed using Azure Databricks. The transformation process includes data cleaning, data validation, and data enrichment. Exploratory data analysis (EDA) is performed to understand data patterns and anomalies. The transformed data is stored in the NYC processed container as Tables. Trip data is incrementally loaded into delta tables to ensure that only data for the current month is loaded and updated data to reduce the processing cost and time. Delta tables also provide transactional capabilities, versioning, and schema enforcement. 
5. **Data Warehousing:** Processed trip data along with DIM tables is stored in SQL Server which will be used for Data Warehousing and for future analysis and Visualization in Power BI 
5. **Data Visualization:** Power BI is used for visualizing and creating interactive dashboards. The dashboard includes charts, graphs, and maps to showcase trends, ride patterns, fare distributions, and more. 

## Power BI Dashboard 

![NYC PowerBI Report](https://github.com/Shakti93/nyc-taxi-project/assets/84408451/5418bfb7-1043-4844-aab5-fc366f370667)

The visualizations for the Power BI dashboard include- 

1. Key Performance Indicator (KPI) cards that provide insights into Total Rides, Average Fare, Average Trip Distance, and Average Trip Duration within a specific filter context.
2. A dynamic Line Chart with parameterization, enabling the display of trends in Total Rides, Average Fare, Average Trip Distance, and Average Trip Duration over the course of the past year.
3. A Bar Graph illustrating the hourly distribution of Total Rides, Average Fare, Average Trip Distance, and Average Trip Duration, facilitating a comprehensive view of time-based patterns.
4. A Horizontal Bar Graph depicting the distribution of rides by Borough, allowing for easy comparison and analysis of ride distribution across different geographic areas.
5. A Map visualization presenting the spatial density of rides by location, allowing for a visual exploration of ride hotspots.
6. A Donut Chart representing the distribution of payment modes, providing an overview of how customers choose to pay for their rides.


## Project highlights  

1. Proficiently orchestrated a comprehensive data pipeline within Azure services, encompassing data ingestion, storage, cleaning, transformation, and analysis of New York taxi data, ensuring seamless data flow and management.
2. Crafted an intuitive Power BI dashboard that empowers users to conduct in-depth comparisons of ride fares, trip lengths, timings, and boroughs, with the flexibility to explore and analyze multiple influencing factors.


## Conclusion 

We get our data from two places, NYC.org and GitHub. They have files about taxi trips in a special format called "Parquet" from 2012 until now. Some other data, like payment methods and dates, is in simple CSV files on GitHub.
