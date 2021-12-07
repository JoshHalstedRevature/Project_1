package example

import java.io.IOException
import java.io.BufferedWriter
import java.io.OutputStreamWriter
import java.io.PrintWriter;
import example.Helpers._
import scala.util.Try
import os._
import java.nio.file.Paths
import java.net.URI
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

import org.apache.spark.api.java.JavaRDD;

// scalaapi.scala

import org.apache.http.HttpEntity
import org.apache.http.HttpResponse
import org.apache.http.client.ClientProtocolException
import org.apache.http.client.HttpClient
import org.apache.http.client.methods.HttpGet
import org.apache.http.impl.client.DefaultHttpClient

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.util.Progressable
import org.apache.hadoop.fs.Path;
import java.io.PrintWriter;

import java.time.{Instant, Duration, ZoneId}
import java.time.temporal.ChronoField
import scala.io.StdIn._
import java.io.IOException
import java.sql.{SQLException, Connection, ResultSet, Statement, DriverManager}
import java.io.PrintWriter;
import scala.collection.mutable.ArrayBuffer

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
    }

    def clear() : Unit = {
            "clear".!
            Thread.sleep(1000)
        }

    def ProvideIntro(): Unit = {
        clear()
        println("Welcome to the USD forex API! Data is taken from the API available at the Currencylayer website.")
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
            clear()
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
                    case _ => println("Please select an option, 1 or 2")
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
        clear()
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
        clear()
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

    def CheckFileExists(filename: String): org.apache.hadoop.fs.FSDataOutputStream = {
        println(s"Creating file $filename ...")
        val conf = new Configuration()
        val fs = FileSystem.get(conf)
        println(s"Checking if $filename already exists...")
        val filepath = new Path( filename)
        val isExisting = fs.exists(filepath)
        if(isExisting) {
            println("Yes it does exist. Deleting it...")
            fs.delete(filepath, false)
        }
        val output = fs.create(new Path(filename))
        return output
    }

    def writeToHDFS(jsonString: String) {
        val path = "hdfs://sandbox-hdp.hortonworks.com:8020/user/maria_dev/"
        val filename = path + "tmp.csv"
        val writer = new PrintWriter(CheckFileExists(filename))
        writer.write(jsonString)
        writer.close()
        println(s"Done creating file $filename ...")
    }

    def ReadJSONtoHive(filename: String) {
        var con: java.sql.Connection = null;
        var driverName = "org.apache.hive.jdbc.HiveDriver"
        val conStr = "jdbc:hive2://sandbox-hdp.hortonworks.com:10000/default";
        con = DriverManager.getConnection(conStr, "", "");
        val stmt = con.createStatement();
        try{
            var resetEverything = ExecuteHiveSQL("DROP TABLE project_1_db.usdcompare")
        }catch{
            case e: java.sql.SQLException => println("Table does not exist, cannot delete. Continuing...")
        }finally{
            clear()
        }
        clear()
        try {
            var res1 = ExecuteHiveSQL("CREATE DATABASE project_1_db")
            clear()
        }catch{
            case e: java.sql.SQLException => println("Database already exists. Continuing...")
        }finally{
            clear()
        }
        try{
            var res2 = ExecuteHiveSQL("CREATE TABLE project_1_db.USDcompare(Currency_Code String, Value Float) ROW FORMAT DELIMITED FIELDS TERMINATED BY ','")
            clear()
        }catch{
            case e: java.sql.SQLException => println("The table already exists. Continuing...")
        }finally{
            clear()
        }
        try{
            var res9 = ExecuteHiveSQL("LOAD DATA INPATH '/user/maria_dev/tmp.csv' INTO TABLE project_1_db.usdcompare")
            clear()
        }catch{
            case e: java.sql.SQLException => println("The data already exists in the table. Continuing...")
            case f: org.apache.hadoop.security.AccessControlException => println("Hive says that there are null values in table.")
        }finally{
            clear()
        }
    }

    def USDBaseQuote(): Unit = {
        var api_key = "c09da610f2281f1a2ca3a226f26506b7"
        var URL_String = s"http://api.currencylayer.com/live?access_key=$api_key"
        var USDBaseLiveData = getRestContent(URL_String).split("\"quotes\":")(1)
        USDBaseLiveData = USDBaseLiveData.split("}}")(0) + "}"
        ConvertJSON2CSV(USDBaseLiveData)
    }

    def ConvertJSON2CSV(jsonString: String): Unit = {
        var RemoveBraces = jsonString.stripPrefix("{").stripSuffix("}").trim
        var RemoveCommas = RemoveBraces.replace(',', '\n')
        var AddCommas = RemoveCommas.replace(':', ',')
        var RemoveDoubleQuotes = AddCommas.replace('\"', ' ')
        var RemoveWhiteSpace = RemoveDoubleQuotes.replaceAll("\\s", "")
        scrapAPI2Hive(RemoveWhiteSpace)
    }

    def scrapAPI2Hive(jsonString: String): Unit = {
        writeToHDFS(jsonString)
        ReadJSONtoHive("tmp.csv")
    }

    def ExecuteQueries(): Unit = {
        USDBaseQuote()
        println("Login was successful.")
        WelcomeDialogue()
        var response : Int = 0
        var exit = false
        while(response != 9 && !exit){
            var response = scala.io.StdIn.readInt()
            response match {
                case 1 => Option1()
                case 2 => Option2()
                case 3 => Option3()
                case 4 => Option4()
                case 5 => Option5()
                case 6 => Option6()
                case 7 => AdminQuery()
                case 8 => exit = true
                case _ => println("Please pick a valid option")
            }
        }
    }

    def WelcomeDialogue(): Unit = {
        println("Please select from one of the eight options. Options 1-6 pertain to specific analysis questions for EIA data. Option 7 permits user to login as ADMIN and add new users, or make other users ADMIN.")
        println("Option 1: Find the five strongest currencies relative to the USD (today)")
        println("Option 2: Find the five weakest currencies relative to the USD (today)")
        println("Option 3: Which currencies are explicitly tied to the USD?")
        println("Option 4: Out of the Central Asian states (exclusing Russia), which currencies are strongest to the USD?")
        println("Option 5: What are the exchange rates for gold (XAU), silver (XAG), and Bitcoin (BTC) today?")
        println("Option 6: For countries in the G8+5 (w/ Switzerland), which currencies are considered strongest today?")
        println("Option 7: Create new users (only for ADMINs).")
        println("Option 8: Exit the program.")
    }

    def Option1(): Unit = {
        println("Option 1: Find the five strongest currencies relative to the USD (today)")
        try {
            var res1 = ExecuteHiveSQL("SELECT * FROM project_1_db.usdcompare WHERE (value != null) ORDER BY value DESC LIMIT 5")
            while (res1.next) {
                println(s"${res1.getString("currency_code")}\t${res1.getString("value")}")
            }
        }catch{
            case e: java.sql.SQLException => println("Query could not be executed. Please troubleshoot source code.")
        }
    }

    def Option2(): Unit = {
        println("Option 2: Find the five weakest currencies relative to the USD (today)")
        try {
            var res1 = ExecuteHiveSQL("SELECT DISTINCT currency_code, value FROM project_1_db.usdcompare WHERE (value != null) ORDER BY value ASC LIMIT 5")
            while (res1.next) {
                println(s"${res1.getString("currency_code")}\t${res1.getString("value")}")
            }
        }catch{
            case e: java.sql.SQLException => println("Query could not be executed. Please troubleshoot source code.")
        }
    }

    def Option3(): Unit = {
        println("Option 3: Which currencies are explicitly tied to the USD?")
        try {
            var res1 = ExecuteHiveSQL("SELECT DISTINCT currency_code FROM project_1_db.usdcompare WHERE (value = 1)")
            while (res1.next) {
                println(s"${res1.getString("currency_code")}\t${res1.getString("value")}")
            }
        }catch{
            case e: java.sql.SQLException => println("Query could not be executed. Please troubleshoot source code.")
        }
    }

    def Option4(): Unit = {
        println("Option 4: Out of the Central Asian states (exclusing Russia), which currencies are strongest to the USD?")
        try {
            var res1 = ExecuteHiveSQL("SELECT * FROM project_1_db.usdcompare WHERE (currency_code = 'USDAFN') OR (currency_code = 'USDAMD') OR (currency_code = 'USDAZN') OR (currency_code = 'USDGEL') OR (currency_code = 'USDKGS') OR (currency_code = 'USDMDL') OR (currency_code = 'USDMKD') OR (currency_code = 'USDMNT') OR (currency_code = 'USDTJS') OR (currency_code = 'USDTMT') OR (currency_code = 'USDUZS') ORDER BY value DESC")
            while (res1.next) {
                println(s"${res1.getString("currency_code")}\t${res1.getString("value")}")
            }
        }catch{
            case e: java.sql.SQLException => println("Query could not be executed. Please troubleshoot source code.")
        }
    }

    def Option5(): Unit = {
        println("Option 5: What are the exchange rates for gold (XAU), silver (XAG), and Bitcoin (BTC) today?")
        try {
            var res1 = ExecuteHiveSQL("SELECT * FROM project_1_db.usdcompare WHERE (currency_code = 'USDXAU') OR (currency_code = 'USDXAG') OR (currency_code = 'USDBTC') ORDER BY value DESC")
            while (res1.next) {
                println(s"${res1.getString("currency_code")}\t${res1.getString("value")}")
            }
        }catch{
            case e: java.sql.SQLException => println("Query could not be executed. Please troubleshoot source code.")
        }
    }

    def Option6(): Unit = {
        println("Option 6: For countries in the G8+5 (w/ Switzerland), which currencies are considered strongest today?")
        try {
            var res1 = ExecuteHiveSQL("SELECT * FROM project_1_db.usdcompare WHERE (currency_code = 'USDGBP') OR (currency_code = 'USDCAD') OR (currency_code = 'USDEUR') OR (currency_code = 'USDCHF') OR (currency_code = 'USDJPY') OR (currency_code = 'USDCNY') OR (currency_code = 'USDZAR') OR (currency_code = 'USDRUB') OR (currency_code = 'USDINR') OR (currency_code = 'USDMXN') OR (currency_code = 'USDBRL') ORDER BY value DESC")
            while (res1.next) {
                println(s"${res1.getString("currency_code")}\t${res1.getString("value")}")
            }
        }catch{
            case e: java.sql.SQLException => println("Query could not be executed. Please troubleshoot source code.")
        }
    }

    def AdminQuery(): Unit = {
        println("Option 1: Add another user with BASIC privileges.")  
        println("Option 2: Exit")      
        var response : Int = 0
        var exit = false
        while(response != 3 && !exit){
            var response = scala.io.StdIn.readInt()
            response match {
                case 1 => println("Option 1")
                case 2 => println("Option 2")
                case 3 => println("Option 3")
                case _ => println("Please pick a valid option")
            }
        }
    }

    def GrantAdminRights(): Unit = {
        def AddNewUser(): Unit = {
            println("Confirmed as administrator. Please specify username to add:")
            var username = scala.io.StdIn.readLine()
            println("Please specify default password for initial login:")
            var password = scala.io.StdIn.readLine()
            println("Should they have ADMIN rights? y or n")
            var AdminRights = scala.io.StdIn.readLine()
            var stmt = s"INSERT INTO table Project_1_DB.PasswordTable values('$username', '$password', 'BASIC');"
            if(AdminRights=='y'){
                stmt = s"INSERT INTO table Project_1_DB.PasswordTable values('$username', '$password', 'ADMIN');"
            }
            var res1 = ExecuteHiveSQL(stmt)       
        }
        println("Are you an administrator?")
        val AdminFlag = scala.io.StdIn.readLine()
        if(AdminFlag == "Yes") {
            println("Provide administrative password:")
            var UserAdminInput = scala.io.StdIn.readLine()
            if(UserAdminInput == "EasyPassword"){
                AddNewUser()              
            }
            else if(UserAdminInput != "EasyPassword"){
                println("Authentication failed. Please try again:")
                var UserAdminInput = scala.io.StdIn.readLine()
                if(UserAdminInput == "EasyPassword"){
                    AddNewUser()
                }
                else{
                    println("Admin login failed. Logging in as regular user.")
                }
            }
        }
        else{
            println("Moving back to query menu...")
        }
    }
}