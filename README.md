# Data Warehouse and Analytics Project

Welcome to the **Data Warehouse and Analytics Project** repository.

This project demonstrates the design and implementation of a modern data warehouse using SQL Server, including data ingestion, transformation, modelling and analytics. It is built as a hands-on portfolio project to demonstrate data engineering and analytics best practices.

---

## Project Overview

The goal of this project is to build a structured data warehouse that consolidates data from multiple business systems and enables analytical reporting.

The architecture follows a layered approach commonly used in modern data platforms.

Bronze Layer → Raw data ingestion  
Silver Layer → Data cleansing and transformation  
Gold Layer → Business-ready analytical data

The final dataset is used for SQL-based analytics and Power BI reporting.

---

## Project Requirements

### Building the Data Warehouse (Data Engineering)

#### Objective

Develop a modern data warehouse using SQL Server to consolidate sales data and support analytical reporting and business insights.

#### Specifications

- **Data Sources**: Import data from two source systems (ERP and CRM) provided as CSV files.
- **Data Quality**: Clean and resolve data quality issues before loading into the analytical model.
- **Integration**: Combine both sources into a unified and analytics-friendly data model.
- **Scope**: Focus on the latest dataset only; historisation is not required.
- **Documentation**: Provide clear architecture and data flow documentation.

---

## Data Architecture

The project implements a **Medallion Architecture**:

**Bronze Layer**
- Raw data ingestion from source systems
- Minimal transformation
- Data stored in original structure

**Silver Layer**
- Data cleansing
- Standardisation
- Derived columns
- Data enrichment

**Gold Layer**
- Business-ready datasets
- Star schema modelling
- Aggregated analytical tables

---

## Analytics & Reporting

SQL-based analytics and Power BI dashboards are used to analyse:

- Customer behaviour
- Product performance
- Sales trends
- Business insights

---

## Technologies Used

- SQL Server
- T-SQL
- Data Modelling (Star Schema)
- ETL Concepts
- Power BI
- Git / GitHub

---

## License

This project is licensed under the MIT License.

---

## About Me

Hi, I'm **Shiv Pandey**.

I work with SQL Server and data platforms and have extensive experience designing reliable data workflows, data models and reporting systems. I am currently expanding my skills in **modern data engineering technologies including Microsoft Fabric, Azure and advanced analytics platforms**.

Connect with me on LinkedIn:

https://www.linkedin.com/in/shivkumar-pandey-4000092/
