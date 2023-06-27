# Data Engineer Capstone Project

## Requirements Document


### Overview: 
This Capstone Project requires learners to work with the following technologies to manage an ETL process for a Loan Application dataset and a Credit Card dataset: Python (Pandas, advanced modules, e.g., Matplotlib), SQL, Apache Spark (Spark Core, Spark SQL), and Python Visualization and Analytics libraries. Learners are expected to set up their environments and perform installations on their local machines.

<img width="347" alt="Screenshot 2023-06-27 001440" src="https://github.com/renneadzadu/capstone_project/assets/101274217/0f92b8d1-d818-4d4f-94fb-5d33c7711a5d">

# Business Requirements - Extract Transfere Load (ETL)
## 1. Functional Requirements - Load Credit Card Database (SQL)
<img width="336" alt="Screenshot 2023-06-27 090129" src="https://github.com/renneadzadu/capstone_project/assets/101274217/5f1792ab-14fa-4c71-b207-f62e4870b6ca">
<img width="314" alt="Screenshot 2023-06-27 090325" src="https://github.com/renneadzadu/capstone_project/assets/101274217/d5d1e18e-2339-4efb-bd61-1d5d69a07f86">

### CDW_SAPP_BRANCH
<img width="518" alt="Screenshot 2023-06-27 095008" src="https://github.com/renneadzadu/capstone_project/assets/101274217/aa784a81-644a-4e3b-8711-b5cc423b5106">

### CDW_SAPP_CREDIT_CARD
<img width="452" alt="Screenshot 2023-06-27 095220" src="https://github.com/renneadzadu/capstone_project/assets/101274217/d3fa3887-6aad-4ccb-8c71-30b6661faf70">

### CDW_SAPP_CUSTOMER
<img width="761" alt="Screenshot 2023-06-27 095331" src="https://github.com/renneadzadu/capstone_project/assets/101274217/5dbdac6d-719e-4fe4-98fa-25b0e776a7bb">


# 2. Functional Requirements - Application Front-End 
Once data is loaded into the database, we need a front-end (console) to see/display data. For that, create a console-based Python program to satisfy System Requirements 2 (2.1 and 2.2). 
##  2.1 Transaction Details Module - 2.2 Customer Details Module
<img width="302" alt="Screenshot 2023-06-27 090902" src="https://github.com/renneadzadu/capstone_project/assets/101274217/fdb1b85f-272f-4ca8-91dc-c1722eedf0af">
<img width="307" alt="Screenshot 2023-06-27 091331" src="https://github.com/renneadzadu/capstone_project/assets/101274217/04b1082d-ebbf-4da4-be88-e70a899113a2">

## 3. Functional Requirements - Data Analysis and Visualization
After data is loaded into the database, users can make changes from the front end, and they can also view data from the front end. Now, the business analyst team wants to analyze and visualize the data. Use Python libraries for the below requirements:
<img width="306" alt="Screenshot 2023-06-27 092403" src="https://github.com/renneadzadu/capstone_project/assets/101274217/f19d18e1-e24d-431d-a6ad-0d35333a1a3e">

### 3.1  Transaction type has a high rate of transactions
![Graph 3.1](https://github.com/renneadzadu/capstone_project/assets/101274217/3eab1e28-7442-4780-a753-ca76efc0547e)

### 3.2 State has a high number of customers
![Figure_3](https://github.com/renneadzadu/capstone_project/assets/101274217/702a9464-680e-41c9-b22a-1ef767b548e3)

### 3.3 Sum of all transactions for the top 10 customers, and which customer has the highest transaction amount.
![Figure_8](https://github.com/renneadzadu/capstone_project/assets/101274217/43b2deb8-e796-4f8c-935a-c96d9be51c2e)

## Overview of LOAN Application Data API
### 4. Functional Requirements - LOAN Application Dataset
<img width="316" alt="Screenshot 2023-06-27 092732" src="https://github.com/renneadzadu/capstone_project/assets/101274217/630034c3-eec5-4e17-b593-a553a1da2315">

### CDW-SAPP_loan_application
<img width="533" alt="Screenshot 2023-06-27 101249" src="https://github.com/renneadzadu/capstone_project/assets/101274217/e5f8b470-e9ef-4717-8b36-1c67fb77a35a">
<img width="238" alt="Screenshot 2023-06-27 101338" src="https://github.com/renneadzadu/capstone_project/assets/101274217/d5e53123-4d19-4fa2-b6d9-fe8017f3f6ff">



## 5. Functional Requirements - Data Analysis and Visualization for LOAN Application
<img width="323" alt="Screenshot 2023-06-27 092848" src="https://github.com/renneadzadu/capstone_project/assets/101274217/0cffc270-38f9-49ed-9e92-f697beb5cc58">

### 5.1 Self-employed applicants
![Figure_6](https://github.com/renneadzadu/capstone_project/assets/101274217/b0ceed33-e4c5-4d66-b720-7c97184b5f55)

### 5.2 Rejection for married male applicants
![Figure_7](https://github.com/renneadzadu/capstone_project/assets/101274217/39dcf4e4-94d5-4af1-b3de-c5fa33eab0fe)

### 5.3 Top three months with the largest transaction data
![Figure_1](https://github.com/renneadzadu/capstone_project/assets/101274217/506d9c5f-fa50-4fa9-b1ce-f12f07d018f9)

### 5.4 Branch processed the highest total dollar value of healthcare transactions 
![Figure_4](https://github.com/renneadzadu/capstone_project/assets/101274217/6061053a-52c3-4398-b774-885281533dab)

### Transaction types
![Figure_2](https://github.com/renneadzadu/capstone_project/assets/101274217/0b7ab119-843a-401b-9fef-01852e101bd6)


## Technical Challenges 
I encountered a challenge with the mapping logic for the customer phone numbers. The desired format for the phone numbers was "(XXX)XXX-XXXX", but I was initially given the format "XXX-XXX". To resolve this, I assigned the area code digits based on the provided zip code to ensure that the phone numbers are in the correct format.
I overcome this impedimented and adapt the mapping documents to match the desired mapping logic.

### Mapping Documents
https://docs.google.com/spreadsheets/d/1t8UxBrUV6dxx0pM1VIIGZpSf4IKbzjdJ/edit#gid=1823293337


