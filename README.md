# Building a Real-Time CDC Data Application Using Debezium, SQL Server, Docker, Consumer App, and AWS Athena -Part 1
Introduction
In today's fast-paced digital landscape, waiting for nightly batch jobs is no longer a viable option. Modern businesses require immediate access to data changes as they happen to power real-time analytics, dashboards, and responsive applications. This is where Change Data Capture (CDC) becomes essential.
This multi-part series will provide a hands-on guide to building a robust, real-time data pipeline. We will use Debezium to capture changes from a SQL Server database, manage our services with Docker, process the data with a custom consumer application, and perform analytics using AWS Athena. Let us get started.


# Solution Architecture
We are going to be utilizing the power of a real-time streaming pipeline to move data from an OLTP SQL Server Database to a Kafka Topic using Debezium, then deserialize the data and consume it to a storage account using Python Consumer App while trying to save cost and optimize efficiency.

![Architecture](https://github.com/kiddojazz/Debezium_SQL_Server_Docker_Consumer_App_AWS_Athena/blob/main/images/Archtecture.png)
