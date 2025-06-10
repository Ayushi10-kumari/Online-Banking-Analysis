# Databricks notebook source
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %md
# MAGIC # LOAN DATASET #

# COMMAND ----------

df = spark.read.csv("/FileStore/Bank Analysis/loan.csv", inferSchema = True, header = True)

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df.show(5)

# COMMAND ----------

len(df.columns)

# COMMAND ----------

df.count()

# COMMAND ----------

df.distinct().count()

# COMMAND ----------

#number of loans in each category
df.groupBy("Loan Category").count().orderBy("count", ascending = False).show()

# COMMAND ----------

#number of people who have taken more than 1 lack loan
df.filter(df["Loan Amount"]>"1,00,000").count()

# COMMAND ----------

#number of people with income greater than 60000 rupees
df.filter(df["Income"]>"60000").count()

# COMMAND ----------

#number of people with 2 or more returned cheques and income less than 50000
df.filter((df[" Returned Cheque"]>"1") & (df["Income"]<"50000")).count()

# COMMAND ----------

#number of people with 2 or more returned cheques and are single
df.filter((df[" Returned Cheque"]>"1") & (df["Marital Status"]<"SINGLE")).count()

# COMMAND ----------

#number of people with expenditure over 50000 a month 
df.filter((df["Expenditure"]>"50000")).show()

# COMMAND ----------

# MAGIC %md
# MAGIC # CREDIT CARD DATASET #

# COMMAND ----------

dfc = spark.read.csv("/FileStore/Bank Analysis/credit_card.csv", inferSchema = True, header = True)

# COMMAND ----------

dfc.printSchema()

# COMMAND ----------

len(dfc.columns)

# COMMAND ----------

dfc.count()

# COMMAND ----------

dfc.distinct().count()

# COMMAND ----------

dfc.show(5)

# COMMAND ----------

#number of members who are elgible for credit card
dfc.filter(dfc["CreditScore"]>700).count()

# COMMAND ----------

#number of members who are  elgible and active in the bank
dfc.filter((dfc["IsActiveMember"]==1) & (dfc["CreditScore"]>700)).count()

# COMMAND ----------

#credit card users in Spain 
dfc.filter(dfc["Geography"]=="Spain").show()

# COMMAND ----------

dfc.filter((dfc["EstimatedSalary"]>100000) & (dfc["Exited"]==1)).count()

# COMMAND ----------

dfc.filter((dfc["EstimatedSalary"]<100000) & (dfc["NumOfProducts"]>1)).count()

# COMMAND ----------

# MAGIC %md
# MAGIC # TRANSACTION DATASET #

# COMMAND ----------

txn = spark.read.csv("/FileStore/Bank Analysis/txn.csv", inferSchema=True, header =True)

# COMMAND ----------

txn.printSchema()

# COMMAND ----------

#COUNT OF TRANSACTION ON EVERY ACCOUNT
txn.groupBy("Account No").count().show()

# COMMAND ----------

#Maximum withdrawal amount
txn.groupBy("Account No").max(" WITHDRAWAL AMT ").orderBy("max( WITHDRAWAL AMT )", ascending = False).show()

# COMMAND ----------

#MINIMUM WITHDRAWAL AMOUNT OF AN ACCOUNT
txn.groupBy("Account No").min(" WITHDRAWAL AMT ").orderBy("min( WITHDRAWAL AMT )").show()

# COMMAND ----------

#MAXIMUM DEPOSIT AMOUNT OF AN ACCOUNT
txn.groupBy("Account No").max(" DEPOSIT AMT ").orderBy("max( DEPOSIT AMT )", ascending = False).show()

# COMMAND ----------

#MINIMUM DEPOSIT AMOUNT OF AN ACCOUNT
txn.groupBy("Account No").min(" DEPOSIT AMT ").orderBy("min( DEPOSIT AMT )").show()

# COMMAND ----------

#sum of balance in every bank account
txn.groupBy("Account No").sum("BALANCE AMT").show()

# COMMAND ----------

#Number of transaction on each date
txn.groupBy("VALUE DATE").count().orderBy("count", ascending = False).show()

# COMMAND ----------

#List of customers with withdrawal amount more than 1 lakh
txn.select("Account No","TRANSACTION DETAILS"," WITHDRAWAL AMT ").filter(txn[" WITHDRAWAL AMT "]>100000).show()