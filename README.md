# US Dollar FOREX API Query Program

## Project Description

This is a project intended to make calls to the Currencylayer's API for today's exchanges with respect to the USD. The project also provides the user the capability to make queries to answer six questions:

* What are the five strongest currencies relative to the USD (today)?
* What are the five weakest currencies relative to the USD (today)?
* Which currencies are explicitly tied to the USD?
* Out of the Central Asian states (exclusing Russia), which currencies are strongest to the USD?
* What are the exchange rates for gold (XAU), silver (XAG), and Bitcoin (BTC) today?
* For countries in the G8+5 (w/ Switzerland), which currencies are considered strongest today?

## Technologies Used

* Scala - version 2.12
* SBT- version 1.0
* Hive
* EIA's API

## Features

List of features ready:
* Incorporates a login system for users
* Permits users who are already logged in to access administrative permissions to add users
* Permits users to query existing RESTful APIs via match-case in Scala program

To-do list:
* Incorporate encryption for login process
* Provide users the ability to change their privileges from BASIC to ADMIN
* Allow users to upload their own RESTful APIs and make custom queries
* Provide admin capability to remove a user
* Write results to CSV file from Hive

## Getting Started
   
git clone https://github.com/JoshHalstedRevature/Project_1

Run SBT in terminal ("sbt") to initiate Scala SBT.

## Usage

Start maria_dev in HortonWorks (use Oracle VM).

Clone this repository, and start an SBT shell. Type in "compile", then "package". Use this command line to transfer the .jar file from local to maria_dev:

  scp -P 2222 C:path\to\project_1_files\scala_files\target\scala-2.12\scala_files_2.12-0.1.0-SNAPSHOT.jar maria_dev@sandbox-hdp.hortonworks.com:/home/maria_dev

Once the .jar file is transferred, go back to the maria_dev shell and type in the following command to start the program:

  spark-submit ./scala_files_2.12-0.1.0-SNAPSHOT.jar --class example.RunApp


## License

This project uses the following license: [<license_name>](<link>).

