📊 Online Banking Analysis

This project performs a comprehensive analysis of an online banking system using PySpark on Databricks. It explores customer data across three key datasets: loan information, credit card activity, and bank transactions.

🚀 Features

Loan Dataset Analysis:

Categorizes loan types and counts.

Filters high-value loans and high-income borrowers.

Identifies risky profiles like returned cheques with low income or single status.

Analyzes monthly expenditures.

Credit Card Dataset Analysis:

Identifies credit card eligibility based on credit score.

Combines eligibility with activity status.

Geographic filtering of credit card users.

Salary-based and product-based insights into customer churn.

Transaction Dataset Analysis:

Tracks frequency and volume of transactions per account.

Finds maximum/minimum withdrawal and deposit amounts.

Summarizes account balances.

Detects high-value withdrawals and busiest transaction dates.

🛠️ Tech Stack

Apache Spark (via PySpark)

Databricks Notebook

Python

📂 Data Sources

loan.csv — Customer loan data.

credit_card.csv — Credit card customer profiles.

txn.csv — Account transaction history.

🔍 Use Cases

Customer segmentation

Fraud detection

Financial behavior analysis

Business decision support for banking services
