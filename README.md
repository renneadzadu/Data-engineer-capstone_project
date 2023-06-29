# Data Engineer Capstone Project

## Requirements Document


### Overview: 
This Capstone Project requires learners to work with the following technologies to manage an ETL process for a Loan Application dataset and a Credit Card dataset: Python (Pandas, advanced modules, e.g., Matplotlib), SQL, Apache Spark (Spark Core, Spark SQL), and Python Visualization and Analytics libraries. Learners are expected to set up their environments and perform installations on their local machines.
<img width="347" alt="Screenshot 2023-06-27 001440" src="https://github.com/renneadzadu/Data-engineer-capstone_project/assets/101274217/ef67d053-b09f-456a-a782-01c2694a9571">

# Business Requirements - Extract Transfere Load (ETL)
## 1. Functional Requirements
<img width="336" alt="Screenshot 2023-06-27 090129" src="https://github.com/renneadzadu/Data-engineer-capstone_project/assets/101274217/48025e2a-3298-4e6b-b375-6070ea9ee1a2">

### CDW_SAPP_BRANCH
<img width="518" alt="Screenshot 2023-06-27 095008" src="https://github.com/renneadzadu/Data-engineer-capstone_project/assets/101274217/2725b0ef-d3d6-40cb-92d6-a74dde49efc5">


### CDW_SAPP_CREDIT_CARD
<img width="452" alt="Screenshot 2023-06-27 095220" src="https://github.com/renneadzadu/Data-engineer-capstone_project/assets/101274217/7c53271c-c63d-4536-84a4-48c27585ff49">

### CDW_SAPP_CUSTOMER
<img width="761" alt="Screenshot 2023-06-27 095331" src="https://github.com/renneadzadu/Data-engineer-capstone_project/assets/101274217/7a90de2e-2977-4f88-8a66-d713f1455bd9">

## 1.2 Data Loading into the Database
<img width="314" alt="Screenshot 2023-06-27 090325" src="https://github.com/renneadzadu/Data-engineer-capstone_project/assets/101274217/0672ae42-267c-436f-b386-9e1742b4271f">
<img width="952" alt="Screenshot 2023-06-27 150750" src="https://github.com/renneadzadu/Data-engineer-capstone_project/assets/101274217/5f0d945d-a527-4628-a446-10a717901c12">

# 2. Functional Requirements - Application Front-End 
Once data is loaded into the database, we need a front-end (console) to see/display data. For that, create a console-based Python program to satisfy System Requirements 2 (2.1 and 2.2). 
##  2.1 Transaction Details Module - 2.2 Customer Details Module
<img width="302" alt="Screenshot 2023-06-27 090902" src="https://github.com/renneadzadu/Data-engineer-capstone_project/assets/101274217/d7d100bf-4944-4b37-bbd6-168d7d989d5e">
<img width="307" alt="Screenshot 2023-06-27 091331" src="https://github.com/renneadzadu/Data-engineer-capstone_project/assets/101274217/675c4e18-9d49-46c7-952b-0f75f69fbf42">

## 3. Functional Requirements - Data Analysis and Visualization
After data is loaded into the database, users can make changes from the front end, and they can also view data from the front end. Now, the business analyst team wants to analyze and visualize the data. Use Python libraries for the below requirements:
<img width="306" alt="Screenshot 2023-06-27 092403" src="https://github.com/renneadzadu/Data-engineer-capstone_project/assets/101274217/76bf7525-eb86-49b3-8565-a872bdc129c6">

### 3.1  Transaction type has a high rate of transactions
![Figure_1](https://github.com/renneadzadu/Data-engineer-capstone_project/assets/101274217/a17f1611-14c7-4412-bf9b-04c4d3e860a1)
![Figure_5](https://github.com/renneadzadu/Data-engineer-capstone_project/assets/101274217/8e958f0d-9a99-443e-bb29-506a5c969fa5)
<img width="392" alt="Screenshot 2023-06-27 154002" src="https://github.com/renneadzadu/Data-engineer-capstone_project/assets/101274217/d151b4ac-4ecf-44f8-8390-23586f65cb78">


### 3.2 State has a high number of customers
![Figure_3](https://github.com/renneadzadu/Data-engineer-capstone_project/assets/101274217/93d8880b-447e-412e-8aa6-9aa57af5bf95)

### 3.3 Sum of all transactions for the top 10 customers, and which customer has the highest transaction amount.
![Figure_8](https://github.com/renneadzadu/Data-engineer-capstone_project/assets/101274217/1a81a90d-67b1-49b1-80d6-ca36b1a865a4)

## Overview of LOAN Application Data API
### 4. Functional Requirements - LOAN Application Dataset
<img width="316" alt="Screenshot 2023-06-27 092732" src="https://github.com/renneadzadu/Data-engineer-capstone_project/assets/101274217/c6a0ba00-d68d-4fba-9ab2-e5621350c640">

### CDW-SAPP_loan_application
<img width="533" alt="Screenshot 2023-06-27 101249" src="https://github.com/renneadzadu/Data-engineer-capstone_project/assets/101274217/a0f21615-5992-4ceb-8e58-16008b216b2b">

### SQL Qurey
<img width="238" alt="Screenshot 2023-06-27 101338" src="https://github.com/renneadzadu/Data-engineer-capstone_project/assets/101274217/ad8fd847-4ca6-420f-97ef-47db72334b29">

## 5. Functional Requirements - Data Analysis and Visualization for LOAN Application
<img width="323" alt="Screenshot 2023-06-27 092848" src="https://github.com/renneadzadu/Data-engineer-capstone_project/assets/101274217/631ebbb8-c880-4ec5-b2b7-3dc077c100fa">

### 5.1 Self-employed applicants
![Figure_6](https://github.com/renneadzadu/Data-engineer-capstone_project/assets/101274217/cc06e6d6-9456-4ad9-8227-368b0aee98f9)

### 5.2 Rejection for married male applicants
![Figure_7](https://github.com/renneadzadu/Data-engineer-capstone_project/assets/101274217/9ade18f8-af12-4451-b5e4-fceaefaee234)

### 5.3 Top three months with the largest transaction data
![Figure_1](https://github.com/renneadzadu/Data-engineer-capstone_project/assets/101274217/37fc4996-cbc9-483d-aa3e-020668bc74e6)

### 5.4 Branch processed the highest total dollar value of healthcare transactions 
![Figure_4](https://github.com/renneadzadu/Data-engineer-capstone_project/assets/101274217/c461846b-1fac-4c72-a055-d51120336aad)

### Transaction types
![Figure_2](https://github.com/renneadzadu/Data-engineer-capstone_project/assets/101274217/05037542-dc64-4ea0-8752-d74d248c2bdf)


## Technical Challenges 
I encountered a challenge with the mapping logic for the customer phone numbers. The desired format for the phone numbers was "(XXX)XXX-XXXX", but I was initially given the format "XXX-XXX". To resolve this, I assigned the area code digits based on the provided zip code to ensure that the phone numbers are in the correct format.
I overcome this impedimented and adapt the mapping documents to match the desired mapping logic.
### Mapping Documents
https://docs.google.com/spreadsheets/d/1t8UxBrUV6dxx0pM1VIIGZpSf4IKbzjdJ/edit#gid=1823293337

## Tecnologiest and Skillsets 
##### Apache Spark UI
##### SQL
##### Pyspark SQL
##### Python
##### Spark DataFrame
##### REST API
##### Git
##### MySQL Database
##### Numpy Library
##### Pandas library
##### Matplotlib library
