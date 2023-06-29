import mysql.connector
import os
from dotenv import load_dotenv
load_dotenv()


conn = mysql.connector.connect(
    host=os.getenv("HOST"),
    user= os.getenv("USER"),
    password=os.getenv("PASSWORD"),
    database="creditcard_capstone"
)

# Create a cursor to execute SQL queries
cursor = conn.cursor()

##############################################################################################################################################################################

def check_account_details(ssn):
    query = """
    SELECT *
    FROM CDW_SAPP_CUSTOMER
    WHERE SSN = %s
    """
    cursor.execute(query, (ssn,))

    result = cursor.fetchone()

    if result:
        print("Account Details:")
        print(result)
    else:
        print("Account not found.")

##############################################################################################################################################################################

def modify_account_details(ssn, new_email):
    query = """
    UPDATE CDW_SAPP_CUSTOMER
    SET CUST_EMAIL = %s
    WHERE SSN = %s
    """
    cursor.execute(query, (new_email, ssn))

    conn.commit()

    print("Account details updated successfully.")

4##############################################################################################################################################################################

def generate_monthly_bill(credit_card_number, month, year):
    query = """
    SELECT *
    FROM CDW_SAPP_CREDIT_CARD
    WHERE CUST_CC_NO = %s AND MONTH = %s AND YEAR = %s
    """
    cursor.execute(query, (credit_card_number, month, year))

    results = cursor.fetchall()

    if results:
        print("Monthly Bill:")
        for row in results:
            print(row)
    else:
        print("No transactions found for the provided credit card number, month, and year.")

##############################################################################################################################################################################

def show_transactions_between_dates(ssn, start_date, end_date):
    query = """
    SELECT *
    FROM CDW_SAPP_CREDIT_CARD
    WHERE CUST_SSN = %s AND TIMEID BETWEEN %s AND %s
    """
    cursor.execute(query, (ssn, start_date, end_date))

    transactions = cursor.fetchall()
    for transaction in transactions:
        print(transaction)




# Closing the cursor and the database connection
def close_connection():
    cursor.close()
    conn.close()

# Main function
def main():
    while True:
        # Display the menu options
        print("\nMenu:")
        print("1. Check Existing Account Details of a Customer ")
        print("2. Modify Existing Account Details of a Customer ")
        print("3. Generate Monthly Bill for a Credit card number for a given month and year ")
        print("4. Display Transactions made by a customer Between Two Dates ")
        print("5. Exit Program")

        choice = input("Enter your choice (1-5): ")

        if choice == "1":
            ssn = input("Enter SSN: ")
            check_account_details(ssn)
        elif choice == "2":
            ssn = input("Enter SSN: ")
            new_email = input("Enter new email: ")
            modify_account_details(ssn, new_email)
        elif choice == "3":
            credit_card_number = input("Enter Credit Card Number: ")
            month = input("Enter Month: ")
            year = input("Enter Year: ")
            generate_monthly_bill(credit_card_number, month, year)
        elif choice == "4":
            ssn = input("Enter SSN: ")
            start_date = input("Enter Start Date (YYYYMMDD): ")
            end_date = input("Enter End Date (YYYYMMDD): ")
            show_transactions_between_dates(ssn, start_date, end_date)
        elif choice == "5":
            print("Exiting the program, BYE!!")
            break
        else:
            print("Invalid choice. Please try again.")

# Main Method run the console application
if __name__ == "__main__":
    main()


# Enter your choice (1-5): 1
# Enter SSN: 123456100
# Account Details:
# (123456100, 'Alec', 'wm', 'Hooper', '4210653310061055', '656, Main Street North', 'Natchez', 
#  'MS', 'United States', 39120, '(242) 123-7818', 'ATHooper@example.com', datetime.datetime(2018, 4, 21, 12, 49, 2))

# Enter your choice (1-5): 2
# Enter SSN: 123453023
# Enter new email: ETHolman@example.com 
# Account details updated successfully.

# Enter your choice (1-5): 3
# Enter Credit Card Number: 4210653310061055
# Enter Month: 3
# Enter Year: 2018
# Monthly Bill:
# ('4210653310061055', '20180302', 123456100, 22, 'Grocery', 58.4, 20543, 2, 3, 2018)

# Enter your choice (1-5): 4
# Enter SSN: 123451310
# Enter Start Date (YYYY-MM-DD): 20180315
# Enter End Date (YYYY-MM-DD): 20180315
# ('4210653342242023', '20180315', 123451310, 180, 'Bills', 77.79, 45069, 15, 3, 2018)