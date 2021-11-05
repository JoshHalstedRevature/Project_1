import java.io._
import java.math._
import java.security._
import java.text._
import java.util._
import java.util.concurrent._
import java.util.function._
import java.util.regex._
import java.util.stream._
import scala.collection.immutable._
import scala.collection.mutable._
import scala.collection.concurrent._
import scala.concurrent._
import scala.io._
import scala.math._
import scala.sys._
import scala.util.matching._
import scala.reflect._

object Result {

    /*
     * Complete the 'diagonalDifference' function below.
     *
     * The function is expected to return an INTEGER.
     * The function accepts 2D_INTEGER_ARRAY arr as parameter.
     */

    def diagonalDifference(arr: Array[Array[Int]]): Int = {
    // Write your code here
        val total_elements = arr.size
        val dimension = scala.math.sqrt(total_elements)
        var i = 0
        var j = 0
        var forward_sum = 0
        for {i <- 1 to dimension; 
            j < - 1 to dimension}{
            forward_sum += matrix[i][j];
        }
        var i = dimension
        var j = dimension
        var backward_sum = 0
        for {i <- dimension to 1  
            j <- dimension to 1 }{
            backward_sum += matrix[i][j];
        }
        val abs_difference = scala.math.abs(forward_sum-backward_sum)
        return abs_difference
    }

}

object Solution {
    def main(args: Array[String]) {
        val printWriter = new PrintWriter(sys.env("OUTPUT_PATH"))

        val n = StdIn.readLine.trim.toInt

        val arr = Array.ofDim[Int](n, n)

        for (i <- 0 until n) {
            arr(i) = StdIn.readLine.replaceAll("\\s+$", "").split(" ").map(_.trim.toInt)
        }

        val result = Result.diagonalDifference(arr)

        printWriter.println(result)

        printWriter.close()
    }
}