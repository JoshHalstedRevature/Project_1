package example

import java.io.IOException
import example.Helpers._
import scala.util.Try
import os._
import java.nio.file.Paths
import scala.sys.process._
import scala.util.control.Breaks
// Demo1.
import java.sql.SQLException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.DriverManager;
import org.apache.hive.service.cli.HiveSQLException;
import scala.collection.mutable.ListBuffer

// scalaapi.scala

import org.apache.http.HttpEntity
import org.apache.http.HttpResponse
import org.apache.http.client.ClientProtocolException
import org.apache.http.client.HttpClient
import org.apache.http.client.methods.HttpGet
import org.apache.http.impl.client.DefaultHttpClient

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import java.io.PrintWriter;

import scala.io.Source

object RunApp{
    // Define global variables 
    var UserInfoFile = "UserPasswords.txt"
    val HiveDBName = "project_1_db"
    val PasswordTable = "passwordtable"

    def main(args: Array[String]): Unit = {
        ProvideIntro()
        HiveSetup()
        ConfirmUsername()
        ExecuteQueries()
        //println(LoginFlag) //Print LoginFlag to see if the correct flag was entered
        // if (LoginFlag == "Success") {
        //     clear()
        //     println("Provide name of CSV file containing query URLs (this should be located in home/maria_dev)")
        //     var UserProvidedCSV = scala.io.StdIn.readLine()
        //     var filename = CheckFileisCSV(UserProvidedCSV)
        //     if(filename == "N/A"){
        //         println("File is not a CSV file. Terminating program.")
        //     }
        //     else{
        //         var URLs = readFiletoList(filename)
        //         var iterator = 1
        //         var listBufferURLTables = ListBuffer[String]()
        //         for (URL <- URLs){
        //             var urlName = "urltable" + iterator
        //             var data = getRestContent(URL)
        //             print('z')
        //             var tempFileName = "URLTempFile.json"
        //             var variable_name = new PrintWriter(tempFileName)
        //             variable_name.write(data)
        //             variable_name.close()
        //             loadJSONFile2Hive(tempFileName, urlName)
        //             listBufferURLTables += urlName
        //             iterator += 1 
        //         }
        //         var listURLTables = listBufferURLTables.toList
        //         println(listURLTables)
        //         Project1Demo(listURLTables)
        //     }
        // }
    }

    def clear() : Unit = {
            "clear".!
            Thread.sleep(1000)
        }

    def ProvideIntro(): Unit = {
        clear()
        println("Welcome to the energy info API! Data is taken from the API available at the Energy Information Administration (EIA) website.")
        Thread.sleep(1000)
        println("Press any key to continue with the program.")
        scala.io.StdIn.readLine()
        clear()
    }

    def HiveSetup(): Unit = {
        println("Starting Hive Demo...")
        Connect2Hive();
        clear()
        CheckHiveDBExists();
        clear()
        CheckPasswordTableExist();
        clear()
    }

    def Connect2Hive(): Unit = {

        var con: java.sql.Connection = null;
        try {
        // For Hive2:
        var driverName = "org.apache.hive.jdbc.HiveDriver"
        val conStr = "jdbc:hive2://sandbox-hdp.hortonworks.com:10000/default";

        Class.forName(driverName);

        con = DriverManager.getConnection(conStr, "", "");
        val stmt = con.createStatement();
        stmt.executeQuery("Show databases");
        System.out.println("show database successfully");
        } catch {
            case ex => {
                ex.printStackTrace();
                throw new Exception(s"${ex.getMessage}")
            }
        } finally {
            try {
                if (con != null)
                con.close();
            } catch {
                case ex => {
                ex.printStackTrace();
                throw new Exception(s"${ex.getMessage}")
                }
            }
        }
    }

    def CheckFileisCSV(CSVFilePath: String): String = {
        if(CSVFilePath.contains(".csv")){
            return CSVFilePath
        }
        else{
            val flag: String = "N/A"
            return flag
        }
    }

    def readFiletoList(filename: String): Seq[String] = {
        val bufferedSource = scala.io.Source.fromFile(filename)
        val lines = (for (line <- bufferedSource.getLines()) yield line).toList
        bufferedSource.close
        return lines
    }

    def ObtainUsername(): String = {
        println("Provide username:")
        val username = scala.io.StdIn.readLine()
        return username
    }

    def Ask2ChangePrivileges(): Unit = {
        println("Requesting to change access privileges. Provide admin password:")
    }

    def ConfirmUsername(): Unit = {
        var instanceUser = ObtainUsername();
        var sql1 = s"SELECT username FROM $HiveDBName" + "." + s"$PasswordTable WHERE username='$instanceUser'";
        try{
            var res1 = ExecuteHiveSQL(sql1)
            if (res1.next()) {
                var ConfirmedUser = res1.getString(1)
                ConfirmPassword(ConfirmedUser)
            }else{
                println("Username is not found. Please contact an ADMIN user and restart program (1) or use different username (2):")
                var response = scala.io.StdIn.readInt()
                response match{
                    case 1 => println("Exiting")
                    case 2 => ConfirmUsername()
                    case _ => println("Please select an option, 1-3")
                }
            }
        }catch{
            case e: HiveSQLException => println("You broke the program somehow. Kudos to you")
        }
    }

    def ConfirmPassword(username: String): Unit = {
        println("Please provide password:")
        var UserProvidedPassword = scala.io.StdIn.readLine()
        var sql3 = s"SELECT password FROM $HiveDBName" + "." + s"$PasswordTable WHERE username='$username'";
        try{
            var res3 = ExecuteHiveSQL(sql3)
            if (res3.next()) {
                var ConfirmedPassword = res3.getString(1)
                if(ConfirmedPassword == UserProvidedPassword){
                    println(s"Login successful!")
                }else{
                    println("Username exists in database, but password is incorrect. Try again (1), use different username (2), or exit (3)")
                    var response = scala.io.StdIn.readInt()
                    response match{
                        case 1 => ConfirmPassword(username: String)
                        case 2 => ConfirmUsername()
                        case 3 => println("Exiting")
                        case _ => println("Please select an option, 1-3")
                    }
                }
            }
        }catch{
            case e: HiveSQLException => println("You broke the program somehow. Kudos to you")
        }
    }

    def ExecuteHiveSQL(sqlStatement: String): java.sql.ResultSet = {
        val conStr = "jdbc:hive2://sandbox-hdp.hortonworks.com:10000/default";
        var con = DriverManager.getConnection(conStr, "", "");
        val stmt = con.createStatement()
        var res = stmt.executeQuery(sqlStatement);
        return res
    }

    def CheckHiveDBExists(): Unit = {
        val conStr = "jdbc:hive2://sandbox-hdp.hortonworks.com:10000/default";
        var con = DriverManager.getConnection(conStr, "", "");
        val stmt1 = con.createStatement()
        println(s"Determining if working database $HiveDBName exists")
        var sql1 = s"CREATE DATABASE IF NOT EXISTS $HiveDBName";
        var res1 = stmt1.execute(sql1);
        val stmt2 = con.createStatement()
        var sql2 = s"USE $HiveDBName";
        var res2 = stmt2.execute(sql2);
    }

    def CheckPasswordTableExist(): Unit = {
        val conStr = "jdbc:hive2://sandbox-hdp.hortonworks.com:10000/default";
        var con = DriverManager.getConnection(conStr, "", "");
        val stmt1 = con.createStatement()
        var sql = "CREATE TABLE IF NOT EXISTS " + HiveDBName + "." + PasswordTable + " (username String, password String, privileges String)";
        var res = stmt1.execute(sql);
    }

    def GrantAdminRights(): String = {
        println("Are you an administrator?")
        val AdminFlag = scala.io.StdIn.readLine()
        if(AdminFlag == "Yes") {
            println("Provide administrative password:")
            var UserAdminInput = scala.io.StdIn.readLine()
            if(UserAdminInput == "admin"){
                println("Confirmed as administrator")
            }
            else if(UserAdminInput != "sk84trees"){
                println("Authentication failed. Please try again:")
                var UserAdminInput = scala.io.StdIn.readLine()
                if(UserAdminInput == "sk84trees"){
                    println("Confirmed as administrator")
                }
                else{
                    println("Admin login failed. Logging in as regular user.")
                }
            }
        }
        else{
            println("Request to grant user privileges denied.")
        }
        return AdminFlag
    }

    def getRestContent(url: String): String = {
        val httpClient = new DefaultHttpClient()
        val httpResponse = httpClient.execute(new HttpGet(url))
        val entity = httpResponse.getEntity()
        var content = ""
        if (entity != null) {
        val inputStream = entity.getContent()
            content = scala.io.Source.fromInputStream(inputStream).getLines.mkString
            inputStream.close
        }
        httpClient.getConnectionManager().shutdown()
        return content
    }

    // def saveData(data: String): Unit = {
    //     val fullFileName = "/tmp/data.json" 
    //     val writer = new PrintWriter(new File(fullFileName))
    //     writer.write(data)
    //     writer.close()
    //     println(s"File creation success!")
    //     var con: java.sql.Connection = null;
    //     try {
    //         var driverName = "org.apache.hive.jdbc.HiveDriver"
    //         val conStr = "jdbc:hive2://sandbox-hdp.hortonworks.com:10000/default";
    //         con = DriverManager.getConnection(conStr, "", "");
    //         val stmt = con.createStatement();
    //         stmt.execute("CREATE TABLE IF NOT EXISTS articles(json String)")
    //         stmt.execute("LOAD DATA LOCAL INPATH '" + fullFileName +"' INTO TABLE articles");
    //         stmt.executeQuery("SELECT * FROM articles");
    //     } catch {
    //         case ex : Throwable=> {
    //         ex.printStackTrace();
    //         throw new Exception (s"${ex.getMessage}")
    //         }
    //     } finally{
    //         try {
    //             if (con != null){
    //                 con.close()
    //             }
    //         } catch {
    //             case ex : Throwable => {
    //             ex.printStackTrace();
    //             throw new Exception (s"${ex.getMessage}")
    //             } 
    //         }
    //     }
    // }

    // def loadJSONFile2Hive(full_path: String, urlName: String): Unit = {
    //     val conStr = "jdbc:hive2://sandbox-hdp.hortonworks.com:10000/default";
    //     var con = DriverManager.getConnection(conStr, "", "");
    //     val stmt85 = con.createStatement()
    //     var sql975 = "DROP TABLE " + HiveDBName + s".$urlName";
    //     var res65 = stmt85.execute(sql975);
    //     var con8 = DriverManager.getConnection(conStr, "", "");
    //     val stmt87 = con8.createStatement()
    //     var sql976 = "CREATE TABLE IF NOT EXISTS " + HiveDBName + s".$urlName (str String)";
    //     var res68 = stmt87.execute(sql976);
    //     clear()
    //     println(s"Please open another session and transfer $full_path to /user/hive/ via the command:")
    //     println(s"hdfs dfs -copyFromLocal $full_path /user/hive")
    //     Thread.sleep(1000)
    //     println(s"If /user/hive/$full_path alread exists, delete it with:")
    //     println(s"hdfs dfs -rm /user/hive/$full_path")
    //     println("When complete, press ENTER")
    //     scala.io.StdIn.readLine()
    //     var con2 = DriverManager.getConnection(conStr, "", "");
    //     val stmt88 = con2.createStatement()
    //     var sql1678 = s"LOAD DATA INPATH '$full_path' INTO TABLE " + HiveDBName + s".$urlName";
    //     try {
    //         var res69 = stmt87.execute(sql1678);
    //     } catch 
    //         {
    //         case e: HiveSQLException => println("File was not found in time. Moving on to next RESTful API")
    //         case f: SQLException => println("File was moved to /user/hive/warehouse. Successful load.")
    //     }
    // }

    def ExecuteQueries(): Unit = {
        println("Login was successful. Please select from one of the eight options. Options 1-6 pertain to specific analysis questions for EIA data. Option 7 permits the user to provide own HiveSQL query. Option 8 permits user to login as ADMIN and add new users, or make other users ADMIN.")
        println("Option 1: Find state with the highest and lowest carbon emissions.")
        println("Option 2: Find states that generate the most hydroelectic power.")
        println("Option 3: ")
        println("Option 4: ")
        println("Option 5: ")
        println("Option 6: ")
        println("Option 7: Perform a customer query.")
        println("Option 8: Create new users (log in as an ADMIN).")
        println("Option 9: Exit the program.")
        var response : Int = 0
        var exit = false
        while(response != 9 && !exit){
            var response = scala.io.StdIn.readInt()
            response match {
                case 1 => println("Option 1")
                case 2 => println("Option 1")
                case 3 => println("Option 1")
                case 4 => println("Option 1")
                case 5 => println("Option 1")
                case 6 => println("Option 1")
                case 7 => println("Option 1")
                case 8 => AdminQuery()
                case 9 => exit = true
                case _ => println("Please pick a valid option")
            }
        }
        // 
    }

    def AdminQuery(): Unit = {
        println("Option 1: Change privileges from BASIC to ADMIN.")
        println("Option 2: Change another user's privileges from BASIC to ADMIN.")
        println("Option 3: Add another user with BASIC privileges.")  
        println("Option 4: Exit")      
        var response : Int = 0
        var exit = false
        while(response != 9 && !exit){
            var response = scala.io.StdIn.readInt()
            response match {
                case 1 => println("Option 1")
                case 2 => println("Option 2")
                case 3 => println("Option 3")
                case 4 => println("Option 4")
                case _ => println("Please pick a valid option")
            }
        }
    }

    def Project1Demo(tablesURL: List[String]): Unit = {
        println("*********************************************************")
        for (table <- tablesURL) {
            println(s"First query: Ask for number of articles that resulted:")
            var conStr = "jdbc:hive2://sandbox-hdp.hortonworks.com:10000/default";
            try {
                var con = DriverManager.getConnection(conStr, "", "");
                val stmt365 = con.createStatement();
                Thread.sleep(3000)
                var numArticleResults = stmt365.executeQuery("SELECT get_json_object(str, '$.totalArticles') FROM project_1_db." + s"$table")
                Thread.sleep(1000)
                while (numArticleResults.next()){
                    println(s"The number of articles written in the past week according to the API is ${numArticleResults.getString(1)}.")
                }
            } catch {
                case ex : Throwable => {
                ex.printStackTrace();
                println("This did not work")
                throw new Exception (s"${ex.getMessage}")
                }
            } finally {
                Thread.sleep(3000)
                println("Press any key to see the next query.")
                scala.io.StdIn.readLine()
                println("*********************************************************")
            }
            println(s"First query: Ask for number of articles that resulted:")
            var conStr2 = "jdbc:hive2://sandbox-hdp.hortonworks.com:10000/default";
            var con = DriverManager.getConnection(conStr2, "", "");
            try { 
                val stmt366 = con.createStatement();
                for (rank <- 0 to 2){
                    var rankedOutlet = stmt366.executeQuery("SELECT get_json_object(json, '$.articles.source.name[" + rank + "]') FROM project_1_db." + s"$table")
                    while (rankedOutlet.next()){
                        println(s"${rank + 1}: ${rankedOutlet.getString(1)}")
                    }
                }

            } catch {
                case ex : Throwable => {
                ex.printStackTrace();
                println("This did not work")
                throw new Exception (s"${ex.getMessage}")
                }
            } finally{
                    Thread.sleep(3000)
                    println("Press any key to see the next query.")
                    scala.io.StdIn.readLine()
                    println("*********************************************************")
            }
        }
    }
}