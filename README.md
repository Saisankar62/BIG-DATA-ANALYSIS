# BIG-DATA-ANALYSIS
**COMPANY**: CODTECH IT SOLUTIONS
**NAME**: BALLEDA SAISANKAR
**INTERN ID**: CT08DF1488
**DOMAIN**: DATA ANALYTICS
**DURATION**: 8 WEEKS
**MENTOR**: NEELA SANTOSH

DESCRIPTION OF THE TASK
_____________________
I successfully performed an end-to-end big data analysis project using Apache Spark with PySpark on the New York City Yellow Taxi Trip Dataset (specifically the yellow_tripdata_2020-06.csv file), which was downloaded from Kaggle. This dataset contains millions of records detailing individual taxi trips in New York City, including information such as pickup and drop-off locations, timestamps, fare amounts, tip amounts, trip distances, and payment types. The main objective of this project was to clean, transform, and analyze this real-world dataset to extract actionable business insights that could be useful for transportation analysis, business planning, and city logistics.
The project began with setting up the environment for big data processing. I configured Java and ensured it was correctly added to the system’s environment path, which is a required dependency for running Spark. I then installed and used Apache Spark, PySpark, and supporting tools such as Python, Pandas, and Visual Studio Code (VS Code) as the integrated development environment. I also used the command-line interface (cmd) for configuration and running Spark jobs locally.
Next, I initialized a SparkSession, which is the entry point for all Spark functionality. Using the SparkSession object, I loaded and read a smaller, sample dataset (sample_dataset.csv) to inspect the data schema, understand the structure, and verify that the Spark setup was working as expected. This initial step helped identify the column names, data types, and overall data quality.
After understanding the schema, I moved on to data cleaning. I focused on identifying missing or null values, particularly in numeric columns like trip distance, fare amount, and tip amount. I used PySpark’s .dropna() function to remove rows with null values to ensure the analysis was not skewed by incomplete data. This basic cleaning was crucial to maintain data integrity and improve the quality of the final results.
With clean data in place, I proceeded to perform various data transformations and aggregations to extract key business insights. One of the first insights involved grouping the trips by pickup hour and calculating the total revenue generated during each hour. The analysis showed that the highest revenue was consistently recorded between 6 PM and 9 PM, indicating peak hours for taxi services in New York City.
Next, I analyzed the payment types used by passengers. By counting the number of transactions for each payment method, I found that credit card payments (payment_type=1) were the most commonly used, highlighting customer preferences and possibly the impact of digital payments in urban mobility.
I also calculated the average tip percentage across different trip distance ranges, which helped identify tipping behavior relative to the length of the ride. Furthermore, I analyzed the most popular pickup zones by counting the number of pickups per zone, which provided valuable spatial insights into high-demand locations.
Finally, I used Pandas to convert Spark DataFrames into Pandas DataFrames for easier export and visualization. The final results were safely saved to an output folder in CSV format, making them accessible for further use in reports, dashboards, or presentations.
Overall, this project effectively demonstrated the use of big data tools such as PySpark, Pandas, SparkSession, Java, VS Code, and CSV file handling to process and analyze large-scale transportation data efficiently.

#OUTPUT


revenu_by_hour:
![Image](https://github.com/user-attachments/assets/284ee32c-9d1d-47af-b418-ae5f4346ce93)

top_payment_types:
![Image](https://github.com/user-attachments/assets/09817270-8f27-485f-8374-838168e51677)
