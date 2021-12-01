# Energy Information Agency API Query Program

## Project Description

This is a project intended to make calls to the EIA's API to gather data about 

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
* Allow users to upload their own RESTful APIs and make custom queries

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

