# Distributed Text Analysis in the Cloud  
### Scalable Manager–Worker System Using AWS S3, SQS, and EC2

## Overview
A fully distributed cloud application for large-scale text analysis, implementing a Manager–Workers architecture on AWS EC2 with messaging via SQS and storage via S3.  
The system processes URLs of text files and performs **POS Tagging**, **Constituency Parsing**, and **Dependency Parsing** using the Stanford Parser.  
The project is structured as a multi-module Maven system: **localapp**, **manager**, **worker**, and **common**.

---

## System Architecture

### Local Application (LocalApp)
- Uploads the input file to S3  
- Ensures a Manager instance is running (creates one if needed)  
- Sends a **NEW_TASK** message to the Manager  
- Waits for a **DONE** response  
- Downloads the generated HTML summary  
- Optionally sends a **TERMINATE** command  

### Manager (EC2)
- Downloads the input file from S3  
- Splits it into discrete analysis tasks  
- Dynamically launches Worker EC2 instances based on **n** (max files per worker)  
- Aggregates all Worker results into a final HTML summary  
- Uploads the summary to S3 and notifies LocalApp  
- Shuts down Workers gracefully during termination  

### Workers (EC2)
- Continuously poll SQS for pending analysis tasks  
- Download input text files  
- Perform Stanford Parser NLP analysis  
- Upload results back to S3  
- Send **DONE** messages to the Manager  
- Recover from exceptions and continue  

---

## Features
- Distributed & parallel processing  
- Dynamic scaling of Worker instances  
- Fault-tolerant design using SQS visibility timeouts  
- Multi-client support (many LocalApps running concurrently)  
- Secure deployment via AMI and user-data scripts  
- Clean modular project structure  

---

## Project Structure
- common/ # Shared utilities & message formats
- localapp/ # Client-side application
- manager/ # Coordination logic & HTML summary generation
- worker/ # NLP processing & S3 upload

---

## Input Format
Each line is formatted as:

<analysis-type>    <URL>

Example:
POS         https://www.gutenberg.org/files/1659/1659-0.txt
DEPENDENCY  https://www.gutenberg.org/files/1342/1342-0.txt

---

## Output Format
Each line in the HTML summary follows:

<analysis-type>: <input-url> <output-s3-url or exception>

Example:
POS: <input-url> <output-s3-url>
CONSTITUENCY: <input-url> <output-s3-url>
DEPENDENCY: <input-url> <Exception: file unavailable>

---

## Running the Application
Command Line:
java -jar localapp/target/localapp-1.0-SNAPSHOT.jar <inputFile> <outputFile> <n> [terminate]