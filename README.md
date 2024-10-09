# flink-pocs
This POC demonstrates realtime streaming of data into Kinesis Data Stream and process thru flink program

Use Case: Find fradulant transaction activity in accounts

## Architecture

![Kinesis-Flink-POC](https://github.com/user-attachments/assets/ec241e17-51f8-4233-bd20-e25ad70271cd)

Disclaimer: The solution took inspiration from AWS Samples to implement the use case.

It is developed in PyCharm and uses flink sql kinesis connector to read and write into AWS kinesis. It is setup to run as local and in AWS Kinesis Data Analytics (AWS managed flink).

## TxnDataGenerator
Generates account data in a way that accounts have multiple transactions so logic can be applied. It sends acct/txn data into kinessis input stream. The input data must be created ahead, this can be accomplished using AWS management console. 

## TXNFraudDetector
To create kinesis source and sink connectors for flink, the right version of flink-sql-connector-kinesis jar file must be downloaded from mvn and added to flink program. The source connector must be configured to identify the Wateramark strategy whcih allows delayed transactions to part of the window. The aggregation query applies fraud detection simple logic (sum(txn_acmnounts) > 3000) to flag accounts. These accounts pushed to kinesis sink connector for further investigation. 

Note: Be sure to update application.properties file to match the settings (region etc)

## Local Testing
Configure environment variable IS_LOCAL=1 in IDE and uncomment create_print_table statement in TxnGraudDetector to see flagged accounts on console.

## Deploy in AWS and Test
There is a detailed instructions video from AWS (https://youtu.be/00JgwB5vJps) on how to deploy flink. Essentially the steps are

1. Zip the solution (TXNFraudDetector + flink-sql-connector-kinesis jar)
2. Upload the zip file into S3 bucket
3. Create Flink application in Kinesis Data Analytics
4. Attach the zip file to the flink application and IAM role with permissions
5. Configure the flink application so it knows the flink program and the jar
6. Configure flink application properties
7. Finally start the flink applicaiton
8. Check the status and open Flink Job/Task Manager

Note: The application runs in local - Ubuntu, in KDA it needs to create an Fat (UBER) Jar file.

The left screen is generating account/txn data and the right screen is desplyaing flagged accounts for fruad.
![FlinkPOC](https://github.com/user-attachments/assets/7897ec63-448a-4e84-a42a-3373c42655b4)






