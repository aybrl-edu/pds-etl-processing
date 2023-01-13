import org.apache.spark.sql.types.{DataType, StructField, StructType}

import org.apache.spark.sql.types.{BooleanType, DataType, DateType, DoubleType, FloatType, IntegerType, LongType, StringType, StructField, StructType}
import spray.json._

object Helper {

  def sparkSchemeFromJSON(): StructType = {
    // Basic type mapping map
    val stringToType = Map[String, DataType](
      "String" -> StringType,
      "Double" -> DoubleType,
      "Float" -> FloatType,
      "Int" -> IntegerType,
      "Boolean" -> BooleanType,
      "Long" -> LongType,
      "DateTime" -> DateType
    )

    // Load JSON From file
    val source = scala.io.Source.fromFile("src/confs/DataFileConfig.json")
    val lines = try source.mkString finally source.close()
    val json = lines.parseJson

    // Parse to case class
    import JsonProtocol._
    val datafileConfig = json.convertTo[DataFileConfig[ConfigElement]]

    // Convert case class to StructType
    var structSeq: Seq[StructField] = Seq()
    datafileConfig.columns.foreach(configElement => {
      structSeq = structSeq :+ StructField(configElement.name, stringToType(configElement.typeOf), nullable = false)
    })
    StructType(structSeq)
  }

}
