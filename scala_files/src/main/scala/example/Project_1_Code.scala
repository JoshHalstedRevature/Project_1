package example;

import java.io.IOException

import scala.util.Try

// Demo1.
import java.sql.SQLException._;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.DriverManager;
import org.apache.hive.service.cli.HiveSQLException;

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
// import org.apache.hive;
import java.io.PrintWriter;

import scala.io.Source


object RunApp{
    // Define global variables 
    var UserInfoFile = "UserPasswords.txt"
    val HiveDBName = "Project_1_DB"
    val PasswordTable = "PasswordTable"
    def main(args: Array[String]): Unit = {
        println("Starting Hive Demo...")
        Connect2Hive()
        CheckHiveDBExists()
        CheckPasswordTableExist()
        var instanceUser = ObtainUsername()
        var instancePassword = ObtainPassword()
        ConfirmUserLogin(instanceUser, instancePassword)

        //RunBasicQuery()
        //val UserList = readFile(UserInfoFile)
        //var adminFlag = GrantAdminRights()
        //println(adminFlag)
        // println(UserList)
    }

    def Connect2Hive(): Unit = {

        var con: java.sql.Connection = null;
        try {
        // For Hive2:
        var driverName = "org.apache.hive.jdbc.HiveDriver"
        val conStr = "jdbc:hive2://sandbox-hdp.hortonworks.com:10000/default";

        // For Hive1:
        //var driverName = "org.apache.hadoop.hive.jdbc.HiveDriver"
        //val conStr = "jdbc:hive://sandbox-hdp.hortonworks.com:10000/default";

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

    def HiveSetup(args: Array[String]): Unit = {
        val conStr = "jdbc:hive2://sandbox-hdp.hortonworks.com:10000/default";
        var con = DriverManager.getConnection(conStr, "", "");
        val stmt = con.createStatement()
        val tableName = "testTable";
    } 

    def ObtainUsername(): String = {
        println("Provide username:")
        val username = scala.io.StdIn.readLine()
        return username
    }

    def ObtainPassword(): String = {
        println("Provide password:")
        val password = scala.io.StdIn.readLine()
        return password
    }

    def Ask2ChangePrivileges(): Unit = {
        println("Requesting to change access privileges. Provide admin password:")
    }

    def ConfirmUserLogin(user: String, password: String): Unit = {
        val conStr = "jdbc:hive2://sandbox-hdp.hortonworks.com:10000/default";
        var con = DriverManager.getConnection(conStr, "", "");
        val stmt1 = con.createStatement()
        var sql1 = s"SELECT username FROM $HiveDBName" + "." + s"$PasswordTable WHERE username=$user";
        try {
            var res1 = stmt1.executeQuery(sql1);
            println("User exists. Provide password:")
            var UserProvidedPassword = scala.io.StdIn.readLine()
            try {
                val stmt3 = con.createStatement()
                var sql3 = s"SELECT password FROM $HiveDBName" + "." + s"$PasswordTable WHERE password=$password";
                var res3 = stmt3.executeQuery(sql3);
                println(s"Logging in as $user")
            } catch {
                case e: HiveSQLException => println("Incorrect password provided. Try again:")
                var UserProvidedPassword = scala.io.StdIn.readLine()
                try {
                    val stmt4 = con.createStatement()
                    var sql4 = s"SELECT password FROM $HiveDBName" + "." + s"$PasswordTable WHERE password=$password";
                    var res4 = stmt4.executeQuery(sql4);
                    println(s"Logging in as $user")
                } catch {
                    case e: HiveSQLException => println("Incorrect password provided. Exiting program.")
                }
            }
        } catch {
            case e: HiveSQLException => println("This username cannot be found. Create new user with basic privileges? (y/n)")
            var UserAddInput = scala.io.StdIn.readLine()
            if(UserAddInput == "y") {
                println(s"Adding new user $user to $PasswordTable. Granting basic user privileges.")
                val stmt9 = con.createStatement()
                var sql20 = s"INSERT INTO $HiveDBName.$PasswordTable VALUES (" + "'" + user + "'" + ", " + "'" + password + "'" + ", " + "'" + "BASIC" + "'" + ")";
                println(sql20)
                var res50 = stmt9.executeUpdate(sql20);

            }else{
                println("Username not added. Closing program")
            }
        }
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

    def readFile(filename: String): Seq[String] = {
        val bufferedSource = scala.io.Source.fromFile(filename)
        val lines = (for (line <- bufferedSource.getLines()) yield line).toList
        bufferedSource.close
        return lines
    }

    def GrantAdminRights(): String = {
        println("Are you an administrator?")
        val AdminFlag = scala.io.StdIn.readLine()
        if(AdminFlag == "Yes") {
            println("Provide administrative password:")
            var UserAdminInput = scala.io.StdIn.readLine()
            if(UserAdminInput == "sk84trees"){
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
        // val currentDirectory = new java.io.File(".").getCanonicalPath
        // println(currentDirectory)
        // readFile(UserInfoFile)
        // for (line <- Source.fromFile(UserInfoFile).getLines) {
        //     println(line)
        // }
        return AdminFlag
    }


    // Test comment
    // val Login = new LoginCredentials()
    // val UserInstance = Login.LoginProcess()
    // println(UserInstance)
}






// Miscellaneous functions

// def RunBasicQuery(): Unit = {
//     val conStr = "jdbc:hive2://sandbox-hdp.hortonworks.com:10000/default";
//     var con = DriverManager.getConnection(conStr, "", "");
//     val stmt = con.createStatement()
//     val tableName = "testTable";
//     println(s"Describing table $tableName..")
//     var sql = "describe " + tableName;
//     System.out.println("Running: " + sql);
//     var res = stmt.executeQuery(sql);
//     while (res.next()) {
//         System.out.println(res.getString(1) + "\t" + res.getString(2));
//     }
// }