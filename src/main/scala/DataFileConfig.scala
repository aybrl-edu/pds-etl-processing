case class DataFileConfig[ConfigElement](columns : List[ConfigElement])

case class ConfigElement(name : String, typeOf : String)