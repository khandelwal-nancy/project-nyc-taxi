Exploring New York City Taxi Data with Azure

Introduction
The New York City Taxi and Limousine Commission (TLC) generously provides us with lots of data about taxi rides in the city. This data includes things like how much people paid for their rides, how long the trips took, and which part of the city they went to. In this project, we're using Azure, a cloud service, to do some cool stuff with this data. We want to clean it up, make it easier to understand, and then use it to create a cool Power BI dashboard to compare different things about taxi rides.

Project Goals

Getting the Data: We want to grab the monthly data from NYC.org, starting from 2012 until now, and save it in a special place using Azure Data Factory (ADF).
Storing the Data: Once we have the data, we'll keep it safe in something called "Azure Data Lake Storage" in a special container.
Cleaning and Changing the Data: We'll use a tool called Azure Databricks to clean and change the data, making it neater and more useful. Then, we'll store it in a different place as "Delta tables."
Adding More Information: We'll make the data even more helpful by adding new tables that tell us stuff about payment methods, rates, areas in the city, taxi companies, and dates.
Keeping the Data: We'll store all this cleaned-up data in an Azure SQL Database for future use.
Making It Pretty: Finally, we'll use Power BI to make a nice dashboard with charts and graphs that help us understand the taxi data better.
How We Do It

Data Sources: We get our data from two places, NYC.org and GitHub. They have files about taxi trips in a special format called "Parquet" from 2012 until now. Some other data, like payment methods and dates, is in simple CSV files on GitHub.

Getting the Data: We use Azure Data Factory to fetch the data from NYC.org and GitHub, and then we put it in our storage in Azure.

Fixing the Data: In Azure Databricks, we clean and fix the data, making sure it's accurate and easy to work with. Then, we store it in our special "Delta tables."

Storing the Data: Our cleaned-up data, along with the extra information we added, finds a home in an Azure SQL Database.

Making It Look Good: With Power BI, we create a dashboard with pretty pictures that help us see things like how many rides there are, how much they cost, how long they take, and more.

What the Dashboard Shows

Important Numbers: We have cards that show big numbers like the total number of rides, average fare, average trip distance, and average trip duration, all based on what we want to see.

Trends Over Time: There's a chart that changes to show us how these numbers have been changing over the past year.

Time of Day: We have another chart that shows us when people take taxi rides during the day.

Different Parts of the City: A chart helps us see which areas of the city are busier for taxi rides.

Map View: We even have a map that shows where most of the taxi rides happen in the city.

How People Pay: Finally, we have a chart that tells us how people like to pay for their taxi rides.

Cool Things We Did

We used Azure to make the whole process of getting and using the data super easy.
The Power BI dashboard we created helps us understand the taxi data in a fun and useful way.
Conclusion
This project shows how we can use Azure to work with big sets of data like taxi information. The insights we get from looking at taxi ride details can help taxi companies and city planners make better decisions.
