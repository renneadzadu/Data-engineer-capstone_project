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

#1)  Used to display the transactions made by customers living in a given zip code for a given month and year. Order by day in descending order.
def transactions_by_zip_month_year(zip_code, month, year):
    query = """
    SELECT *
    FROM CDW_SAPP_CUSTOMER
    INNER JOIN CDW_SAPP_CREDIT_CARD ON CDW_SAPP_CUSTOMER.SSN = CDW_SAPP_CREDIT_CARD.CUST_SSN
    WHERE CDW_SAPP_CUSTOMER.CUST_ZIP = %s AND MONTH(CDW_SAPP_CREDIT_CARD.MONTH) = %s AND YEAR(CDW_SAPP_CREDIT_CARD.YEAR) = %s
    ORDER BY DAY(CDW_SAPP_CREDIT_CARD.DAY) DESC
    """
    cursor.execute(query, (zip_code, month, year))
    transactions = cursor.fetchall()

    if transactions:
        print("Transactions:")
        for transaction in transactions:
            print(transaction)
    else:
        print("No transactions found for the provided criteria.")

    cursor.close()
    conn.close()

#2) Used to display the number and total values of transactions for a given type.
def transactions_by_type(type_of_transaction):
    query = """
    SELECT COUNT(*), SUM(TRANSACTION_VALUE)
    FROM CDW_SAPP_CREDIT_CARD
    WHERE TRANSACTION_TYPE = %s
    """
    cursor.execute(query, (type_of_transaction,))
    result = cursor.fetchone()

    if result:
        count, total_value = result
        print(f"Number of transactions is: {count}")
        print(f"Total value of transactions is: {total_value}")
    else:
        print("No transactions found for the provided type.")

    cursor.close()
    conn.close()

# Used to display the total number and total values of transactions for branches in a given state.
def total_transactions_by_state(state):
    query = """
    SELECT COUNT(*), SUM(TRANSACTION_VALUE)
    FROM CDW_SAPP_CREDIT_CARD
    INNER JOIN CDW_SAPP_BRANCH ON CDW_SAPP_CREDIT_CARD.BRANCH_CODE = CDW_SAPP_BRANCH.BRANCH_CODE
    WHERE CDW_SAPP_BRANCH.BRANCH_STATE = %s
    """
    cursor.execute(query, (state,))
    result = cursor.fetchone()

    if result:
        count, total_value = result
        print(f"Number of transactions: {count}")
        print(f"Total value of transactions: {total_value}")
    else:
        print("No transactions found for the provided state.")

    cursor.close()
    conn.close()


def main():
    while True:
        print("Transaction Details Module")
        print("1. Display transactions by zip code, month, and year")
        print("2. Display number and total values of transactions by type")
        print("3. Display total number and total values of transactions by state")
        print("4. Exit")

        choice = input("Enter your choice (1-4): ")

        if choice == "1":
            zip_code = input("Enter zip code: ")
            month = input("Enter month (1-12): ")
            year = input("Enter year: ")
            transactions_by_zip_month_year(zip_code, month, year)
        elif choice == "2":
            transaction_type = input("Enter transaction type: ")
            transactions_by_type(transaction_type)
        elif choice == "3":
            state = input("Enter state: ")
            total_transactions_by_state(state)
        elif choice == "4":
            print("Exiting the program, BYE")
            break
        else:
            print("Invalid choice. Please try again.")


if __name__ == "__main__":
    main()



# Enter your choice (1-4): 1     
# Enter zip code: 39120
# Enter month (1-12): 3
# Enter year: 2018
# No transactions found for the provided criteria.

# Enter your choice (1-4): 2
# Enter transaction type: Bills
# Number of transactions: 6861
# Total value of transactions: 351405.2800000002

# Enter your choice (1-4): 3
# Enter state:    MS
# Number of transactions: 0