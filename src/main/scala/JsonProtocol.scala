import spray.json.{DefaultJsonProtocol, JsonFormat, RootJsonFormat}


object JsonProtocol extends DefaultJsonProtocol {
  implicit val configElement: RootJsonFormat[ConfigElement] = jsonFormat2(ConfigElement.apply)
  implicit def dataFileConfig[ConfigElement : JsonFormat]: RootJsonFormat[DataFileConfig[ConfigElement]] = jsonFormat1(DataFileConfig.apply[ConfigElement])

}
