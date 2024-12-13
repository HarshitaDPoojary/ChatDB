# ChatDB Application

## Overview

ChatDB is an interactive ChatGPT-like application designed to assist users in learning how to query data in database systems, including SQL and NoSQL databases. ChatDB not only suggests sample queries and interprets natural language queries but also executes these queries on database systems and displays the results. This unique functionality sets ChatDB apart, making it an invaluable tool for learning and practicing database query techniques.

## Key Features

- **Sample Query Suggestions**: Provides sample queries, including advanced constructs such as `GROUP BY` in SQL.
- **Natural Language Querying**: Understands and interprets natural language queries to generate database queries.
- **Query Execution**: Executes user-generated or suggested queries directly on the database and displays results.
- **Dataset Upload**: Allows users to upload datasets into the database system for querying.
- **Support for SQL and NoSQL Databases**: Compatible with MySQL, MongoDB, and other database systems.

## Supported Datasets

The application supports the following datasets:

1. [Coffee Shop Sales Dataset](https://www.kaggle.com/datasets/ahmedabbas757/coffee-sales?resource=download)
2. [Harry Potter Movies Dataset](https://www.kaggle.com/datasets/maricinnamon/harry-potter-movies-dataset)
3. [Target Dataset](https://www.kaggle.com/datasets/devarajv88/target-dataset?select=orders.csv)
4. [Formula 1 Status Dataset](https://www.kaggle.com/datasets/cbhavik/formula-1-ml-classifier?select=status.csv)

These datasets are stored in the database system as tables (e.g., in MySQL) or collections (e.g., in MongoDB).

## Getting Started

### Prerequisites

- Python 3.8+
- Required database systems (e.g., MySQL, MongoDB)
- Required Python libraries (listed in `requirements.txt`)

### Installation

1. Clone the repository:
   ```
   git clone https://github.com/HarshitaDPoojary/ChatDB.git
   cd ChatDB
   ```


2. Install dependencies
   ```
   pip install -r requirements.txt
   ```

3. Set up the required databases ( MySQL) and ensure the datasets are ready for upload.

### Running the Application
To run ChatDB, execute the following command:
```
python chatdb.py
```