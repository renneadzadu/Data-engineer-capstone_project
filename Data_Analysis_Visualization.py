### Import all python Liberies
import pandas as pd
import numpy as np
import requests
import matplotlib.pyplot as plt
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import pyspark.sql.functions as F
import random

print ("CDW_SAPP_CUSTOMER: CUSTOMER TABLE")

# creating sparksession DF
spark = SparkSession.Builder().appName("Loan credit card").getOrCreate()
df= spark.read.json("datasets/cdw_sapp_custmer.json")

# typecasting 
df = df.withColumn("CUST_PHONE", col("CUST_PHONE").cast(StringType()))
df = df.withColumn("SSN", col("SSN").cast(IntegerType()))
df = df.withColumn("CUST_ZIP", col("CUST_ZIP").cast(IntegerType()))
df = df.withColumn("LAST_UPDATED", col("LAST_UPDATED").cast(TimestampType()))

# worning with upper/ lower cases
f = df.withColumn("FIRST_NAME", initcap("FIRST_NAME"))
df = df.withColumn("MIDDLE_NAME", lower("MIDDLE_NAME"))
df = df.withColumn("LAST_NAME", initcap("LAST_NAME"))

#aggregating 
agg_address_df = df.withColumn("FULL_STREET_ADDRESS", concat(df["APT_NO"], lit(", "), df["STREET_NAME"]))

agg_address_df.show()

seed_rang =123
customer_df = agg_address_df.withColumn("CUST_PHONE",
                   concat(lit("("),
                          lpad((rand(seed=seed_rang) * 900 + 100).cast("int"), 3, "0"),
                          lit(") "),
                          substring(df.CUST_PHONE, 1, 3),
                          lit("-"),
                          substring(df.CUST_PHONE, 4, 3),
                          substring(df.CUST_PHONE, 7, 4)))

customer_df.show(truncate=False)

customer_df=customer_df.drop("STREET_NAME")
customer_df=customer_df.drop("APT_NO")

CDW_SAPP_CUSTOMER = customer_df.select("SSN","FIRST_NAME", "MIDDLE_NAME", "LAST_NAME","Credit_card_no","FULL_STREET_ADDRESS","CUST_CITY","CUST_STATE","CUST_COUNTRY","CUST_ZIP","CUST_PHONE","CUST_EMAIL","LAST_UPDATED")

CDW_SAPP_CUSTOMER.show()


######################################################################################################################################################################
print("CDW_SAPP_CREDIT_CARD: Credit Card Transction table")

spark = SparkSession.Builder().appName("Loan credit card").getOrCreate()
df= spark.read.json("datasets/cdw_sapp_credit.json")

df = df.withColumn("BRANCH_CODE", col("BRANCH_CODE").cast(IntegerType()))
df = df.withColumn("CUST_SSN", col("CUST_SSN").cast(IntegerType()))
df = df.withColumn("TRANSACTION_ID", col("TRANSACTION_ID").cast(IntegerType()))
df = df.withColumn("TRANSACTION_VALUE", col("TRANSACTION_VALUE").cast(DoubleType()))




print("GRAPHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHH")

monthly_totals = df.groupby('MONTH') \
                                     .agg(F.sum('TRANSACTION_VALUE').alias('TOTAL_VALUE'))

top_three_months = monthly_totals.orderBy(F.desc('TOTAL_VALUE')).limit(3).toPandas()

months = top_three_months['MONTH']
totals = top_three_months['TOTAL_VALUE']

colors = ['#' + ''.join([random.choice('0123456789ABCDEF') for _ in range(6)]) for _ in range(3)]

plt.bar(months, totals, color=colors)
plt.xlabel('Month')
plt.ylabel('Transaction Value')
plt.title('Top Three Months with Largest Transaction Data')
plt.show()



transaction_counts = df.groupby('TRANSACTION_TYPE').count()
types = transaction_counts.select('TRANSACTION_TYPE').rdd.flatMap(lambda x: x).collect()
counts = transaction_counts.select('count').rdd.flatMap(lambda x: x).collect()

plt.pie(counts, labels=types, autopct='%1.1f%%')
plt.title('Transaction Type Distribution')
plt.axis('equal')
plt.show()



df = df.withColumn("TIMEID", concat(df.YEAR.cast("string"), 
                                    lpad(df.MONTH.cast("string"), 2, "0"), 
                                    lpad(df.DAY.cast("string"), 2, "0")))

# df=df.drop("DAY")
# df=df.drop("MONTH")
# df=df.drop("YEAR")

df = df.withColumnRenamed("CREDIT_CARD_NO", "CUST_CC_NO")

CDW_SAPP_CREDIT_CARD = df.select("CUST_CC_NO","TIMEID","CUST_SSN","BRANCH_CODE","TRANSACTION_TYPE","TRANSACTION_VALUE","TRANSACTION_ID")
print(CDW_SAPP_CREDIT_CARD.dtypes)
CDW_SAPP_CREDIT_CARD.show()


print("CDW_SAPP_BRANCH: Branch code table")


spark = SparkSession.Builder().appName("Loan credit card").getOrCreate()
df= spark.read.json("datasets/cdw_sapp_branch.json")


df = df.withColumn("BRANCH_ZIP", when(col("BRANCH_ZIP").isNull(), lit("99999")).otherwise(col("BRANCH_ZIP")))


df = df.withColumn("BRANCH_ZIP", col("BRANCH_ZIP").cast(IntegerType()))
df = df.withColumn("BRANCH_CODE", col("BRANCH_CODE").cast(IntegerType()))
df = df.withColumn("LAST_UPDATED", col("LAST_UPDATED").cast(TimestampType()))

df = df.withColumn("BRANCH_PHONE",
                                     concat(lit("("),
                                            substring(df.BRANCH_PHONE, 1, 3),
                                            lit(") "),
                                            substring(df.BRANCH_PHONE, 4, 3),
                                            lit("-"),
                                            substring(df.BRANCH_PHONE, 7, 4)))
df.show(truncate=False)


CDW_SAPP_BRANCH = df.select("BRANCH_CODE","BRANCH_NAME","BRANCH_STREET","BRANCH_CITY","BRANCH_STATE","BRANCH_ZIP","BRANCH_PHONE","LAST_UPDATED")

CDW_SAPP_BRANCH.show()
print(CDW_SAPP_BRANCH.dtypes)



###########################################################################################################################################################

print("CDW_SAPP_loan_application")

park = SparkSession.builder.appName("Loan credit card").getOrCreate()

url = "https://raw.githubusercontent.com/platformps/LoanDataset/main/loan_data.json"

response = requests.get(url)
json_data = response.json()

df = spark.read.json(spark.sparkContext.parallelize([json_data]))
df.show(5)

print(f"rows: {df.count()}")
print(len(df.columns))
df.describe().show()
df.printSchema()
df.columns

status = response.status_code
print(f"Status Code: {status}")

CDW_SAPP_loan_application = df
CDW_SAPP_loan_application.show(5)



print("joining tabales")
joined_df = CDW_SAPP_BRANCH.join(CDW_SAPP_CREDIT_CARD, CDW_SAPP_BRANCH['BRANCH_CODE'] == CDW_SAPP_BRANCH['BRANCH_CODE'], 'inner')
# Display the joined DataFrame
joined_df.show()



###########################################################################################################################################################
# Join the tables
print("graph")

Customer_per_state = CDW_SAPP_CUSTOMER.groupBy("CUST_STATE").count()
Customer_per_state = Customer_per_state.orderBy(Customer_per_state["count"].desc())
state_counts_pd = Customer_per_state.toPandas()

plt.figure(figsize=(12, 6))
plt.bar(state_counts_pd["CUST_STATE"], state_counts_pd["count"])
plt.xlabel("State")
plt.ylabel("Number of Customers")
plt.title("Number of Customers by State")
plt.show()


branch_totals = CDW_SAPP_CREDIT_CARD \
    .filter(col('TRANSACTION_TYPE') == 'Healthcare') \
    .groupBy('BRANCH_CODE') \
    .agg({'TRANSACTION_VALUE': 'sum'}) \
    .withColumnRenamed('sum(TRANSACTION_VALUE)', 'total_value')

highest_branch = branch_totals \
    .orderBy(col('total_value').desc()) \
    .select('BRANCH_CODE') \
    .first()[0]

branch_totals_df = branch_totals.toPandas()

# Graph
plt.figure(figsize=(10, 6))
plt.bar(branch_totals_df['BRANCH_CODE'], branch_totals_df['total_value'], color='blue')

plt.xlabel('Branch Number')
plt.ylabel('Total Value of Healthcare Transactions')
plt.title('Total Dollar Value of Healthcare Transactions by Branch')
plt.show()


transaction_rates = CDW_SAPP_CREDIT_CARD.groupBy("TRANSACTION_TYPE").count().orderBy("count", ascending=False)
types = [row["TRANSACTION_TYPE"] for row in transaction_rates.collect()]
counts = [row["count"] for row in transaction_rates.collect()]

colors = ['#' + ''.join([random.choice('0123456789ABCDEF') for _ in range(6)]) for _ in range(3)]

plt.figure(figsize=(10, 6))
plt.barh(types, counts, color=colors)

plt.xlabel("Transaction Type")
plt.ylabel("Transaction Count")
plt.title("Transaction Rates by Transaction Type")
plt.show()


self_employed_df = df.filter(df['Self_Employed'] == 'Yes')
total_applications = self_employed_df.count()
approved_applications = self_employed_df.filter(df['Application_Status'] == 'Y').count()
percentage_approved = (approved_applications / total_applications) * 100

Self_Employed_rejected = 100 - percentage_approved

# Plot the percentage of approved and rejected applications
labels = ['Approved', 'Rejected']
sizes = [percentage_approved, Self_Employed_rejected]
colors = ['green', 'red']

plt.pie(sizes, labels=labels, colors=colors, autopct='%1.1f%%')
plt.title('Application Approved/Rejected for Self-employed Applicants')
plt.show()


married_male = df.filter((df['Married'] == 'Yes') & (df['Gender'] =='Male'))
total_applications = married_male.count()
approved_applications = married_male.filter(df['Application_Status'] == 'Y').count()
percentage_approved = (approved_applications / total_applications) * 100

married_male_rejected = 100 - percentage_approved

labels = ['Approved', 'Rejected']
sizes = [percentage_approved, married_male_rejected]
colors = ['blue', 'pink']

plt.bar(labels, sizes, color=colors)
plt.title('Application Status for Married Male Applicants')
plt.xlabel('Application Status')
plt.ylabel('Number of Applications')
plt.show()


joined_df = CDW_SAPP_CREDIT_CARD.join(CDW_SAPP_CUSTOMER, CDW_SAPP_CREDIT_CARD['CUST_SSN'] == CDW_SAPP_CUSTOMER['SSN'], 'inner')

customer_totals = joined_df.groupby("CUST_SSN", "FIRST_NAME", "LAST_NAME") \
                          .agg(F.sum("TRANSACTION_VALUE").alias("TOTAL_VALUE")) \
                          .orderBy(F.desc("TOTAL_VALUE"))

top_10_customers = customer_totals.limit(10).toPandas()
customer_names = top_10_customers["FIRST_NAME"] + " " + top_10_customers["LAST_NAME"]
transaction_values = top_10_customers["TOTAL_VALUE"]

plt.figure(figsize=(12, 6))
plt.bar(customer_names, transaction_values)
plt.xlabel("Customer")
plt.ylabel("Total Transaction Value")
plt.title("Sum of Transactions for Top 10 Customers")
plt.xticks(rotation=45)
plt.legend(customer_names, loc='upper left')
plt.show()
