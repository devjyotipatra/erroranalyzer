
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
import scala.math.log


import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.Encoders


object ErrorAnalysis {

  def fun[T](v: T)(implicit ev: ClassTag[T]) = println(ev.toString)

   case class Error(engine: String, message: Message)

   case class Message(status_code: Long, state: String, msg: String)

   case class ErrorToken(engine: String, status_code: Long, state: String,
                        exception: String, tokens: List[String],
                        termFrequencies: scala.collection.mutable.HashMap[String, Int])

   case class ErrorMessage(engine: String, status_code: Long, state: String,
                        exeception: String, msg: String)

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

        implicit def encodeError[Error](implicit ct: ClassTag[Error]) =
            org.apache.spark.sql.Encoders.kryo[Error](ct)


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
        }} (encodeError)


      //  implicit def encodeErrorTokens[ErrorToken](implicit ct: ClassTag[ErrorToken]) =
      //      org.apache.spark.sql.Encoders.kryo[ErrorToken](ct)

        val ddf = fdf.rdd.map { x => x match {
          case y: Error =>
            val engine = y.engine

            y.message match {
              case z: Message => {
                val status_code = z.status_code
                val state = z.state
                val tokenList: List[String] = tokenize(z.msg.split("[ \n\t]+"))

                if (!tokenList.isEmpty) {
                  val exception = tokenList.head
                  val tokens = tokenList.drop(1)
                  val termFrequencies = transform(tokens)

                  ErrorToken(engine, status_code, state, exception, tokens,
                              termFrequencies)
                } else {
                  null
                }
              }
              case _ => null
          }
          case _ => null
        }}



        val termDocFreq = scala.collection.mutable.HashMap[String, Int]()
        val setTF = (s: String) => termDocFreq.getOrElse(s, 0) + 1
        ddf.collect().foreach({ token => token match {
          case e: ErrorToken =>
            if (e.termFrequencies != null) {
              e.termFrequencies.foreach {
                  case(term, freq) => termDocFreq.put(term, setTF(term))
          }}
          case null => println("Null doc")
          }
        })

        println("termDocFreq  =>  " + termDocFreq)


        val errors: ListBuffer[ErrorMessage] = ListBuffer()
        val docCount = ddf.count
        ddf.collect().foreach({ token => token match {
            case e: ErrorToken =>
              if (e.termFrequencies != null) {
                val tokens: ListBuffer[String] = ListBuffer()
                e.tokens.foreach {
                    case term: String => {
                      val tfIdf = e.termFrequencies.getOrElse(term, 1) * math.log(docCount/(termDocFreq.getOrElse(term, 1) + 1.0))
                      if (tfIdf > 1) {
                        tokens += term
                      }
                    }
                 }

                 errors += ErrorMessage(e.engine, e.status_code, e.state, e.exception,
                     tokens.toList.mkString(" "))
             }
             case null => println("Null doc")
          }
        })

        errors.toList.foreach(println)
    }

// Main ends here

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
          flag = true
          tokens += term
        }
      }

      tokens.toList
    }


    def transform(document: Iterable[_]): scala.collection.mutable.HashMap[String, Int] = {
      val termFrequencies = scala.collection.mutable.HashMap.empty[String, Int]
      val setTF = (s: String) => termFrequencies.getOrElse(s, 0) + 1
      println(document)
      document.foreach { term =>
        val word = term.asInstanceOf[String]
        println(word)
        termFrequencies.put(word, setTF(word))
      }

      termFrequencies
    }

}
