
package main.scala.error

import org.apache.spark.sql.SparkSession

import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.util.Utils
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.unsafe.hash.Murmur3_x86_32._

import scala.reflect.ClassTag
import scala.collection.mutable.WrappedArray
import scala.collection.immutable.HashMap
import scala.collection.mutable.ListBuffer


import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.Encoders


object ErrorAnalysis {

  def fun[T](v: T)(implicit ev: ClassTag[T]) = println(ev.toString)

   case class Error(engine: String, message: Message)

   case class Message(status_code: Long, state: String, msg: String)

   val ordinary = (('a' to 'z') ++ ('A' to 'Z') ++ Seq('.')).toSet

   def makeOrdinary(s:String) = s.filter(ordinary.contains(_))

   def isException(s: String) = s.trim.split(".").length > 1

    def main(args: Array[String]): Unit = {
        if (args.length != 1) {
          println("Usage: ErrorAnalyzer <path_to_json_file>")
          System.exit(1)
        }

        val jsonFile = args(0)

        val warehouseLocation = "file:/Users/devjyotip/Krow/data/spark-warehouse"

        val spark = SparkSession
                        .builder()
                        .appName("ErrorAnalyzer")
                        .config("spark.sql.warehouse.dir", warehouseLocation)
                        .enableHiveSupport()
                        .getOrCreate()

        import spark.implicits._

        val df = spark.read.json(jsonFile).toDF()

        val eventDataSchema = df.schema

        println("@@@@@@@@@@@@ " + eventDataSchema)

        //val err_df = df.withColumn("err", from_json(col("err_log"), eventDataSchema)).drop("err_log")

        val err_df = df

        implicit def encoder[Error](implicit ct: ClassTag[Error]) =
            org.apache.spark.sql.Encoders.kryo[Error](ct)

        println("############## " + encoder.schema)

        val fdf = err_df.map { r => {
                val engine = r.getAs[String]("EXECUTION_ENGINE")

                val status = r.getAs[Row]("status").getString(0)

                val messages = r.getAs[WrappedArray[Row]]("messages")

                var err_msg: Message = null
                if(messages != null) {
                    messages.array.foreach(m => {
                        val msg_type = m.getAs[String]("message_type")

                        if(msg_type.toLowerCase() == "error") {
                          val status_code = m.getAs[Long]("status_code")
                          val msg = m.getAs[String]("message")
                          val sql_state = m.getAs[String]("sql_state")

                          if(err_msg == null || sql_state != null) {
                             err_msg = Message(status_code, sql_state, msg)
                          }
                        }
                    })
                }

                if(status.toLowerCase() == "failed") {
                  Error(engine, err_msg)
                } else {
                  Error(engine, null)
                }
        }} (encoder)

        val ddf = fdf.rdd.map { x => x match {
          case y: Error => y.message match {
            case z: Message => {
              val tokenList: List[String] = tokenize(z.msg.split("[ \n\t]+"))
              if (!tokenList.isEmpty) {
                val exeception = tokenList.head
                val tokens = tokenList.drop(1)
                val termFrequencies = transform(tokens)
                termFrequencies
              } else {
                null
              }
            }
            case _ => null
          }
          case _ => null
        }}

        ddf.collect().foreach(println)

        /*val termDocFreq = scala.collection.mutable.HashMap[String, Int]()
        val setTF = (s: String) => termDocFreq.getOrElse(s, 0) + 1
        ddf.collect().foreach({
          termFrequencies =>
          if (termFrequencies != null) {
            termFrequencies.foreach {
                case(term, freq) => termDocFreq.put(term, setTF(term))
          }}
        })

        println("termDocFreq  =>  " + termDocFreq)*/
    }

    def tokenize(document: Iterable[String]): List[String] = {
      var tokens = new ListBuffer[String]()
      var flag = false
      var numWords = 20

      document.foreach { term =>
        val word = makeOrdinary(term)

        if(flag && word.length > 1) {
          tokens += word
          numWords -= 1

          if (numWords==0) {
            return tokens.toList
          }
        }

        if(!flag && (word contains "Exception")) {
          println("#########>> " + word)
          flag = true
          tokens += term
        }
      }
      tokens.toList
    }

    def transform(document: Iterable[_]): scala.collection.mutable.HashMap[String, Int] = {
      val termFrequencies = scala.collection.mutable.HashMap.empty[String, Int]
      val setTF = (s: String) => termFrequencies.getOrElse(s, 0) + 1

      document.foreach { term =>
        val word = term.asInstanceOf[String]
        termFrequencies.put(word, setTF(word))
      }

      termFrequencies
    }

    /*def transform(document: Iterable[_]): Vector = {
      val termFrequencies = mutable.HashMap.empty[Int, Double]
      val setTF = if (binary) (i: Int) => 1.0 else (i: Int) => termFrequencies.getOrElse(i, 0.0) + 1.0
      val hashFunc: Any => Int = murmur3Hash

      document.foreach { term =>
        val i = Utils.nonNegativeMod(hashFunc(term), numFeatures)
        termFrequencies.put(i, setTF(i))
      }

      Vectors.sparse(numFeatures, termFrequencies.toSeq)
    }

    def murmur3Hash(term: Any): Int = {
      term match {
        case null => seed
        case b: Boolean => hashInt(if (b) 1 else 0, seed)
        case b: Byte => hashInt(b, seed)
        case s: Short => hashInt(s, seed)
        case i: Int => hashInt(i, seed)
        case l: Long => hashLong(l, seed)
        case f: Float => hashInt(java.lang.Float.floatToIntBits(f), seed)
        case d: Double => hashLong(java.lang.Double.doubleToLongBits(d), seed)
        case s: String =>
          val utf8 = UTF8String.fromString(s)
          hashUnsafeBytesBlock(utf8.getMemoryBlock(), seed)
        case _ => throw new SparkException("HashingTF with murmur3 algorithm does not " +
          s"support type ${term.getClass.getCanonicalName} of input data.")
      }
    }*/
}
