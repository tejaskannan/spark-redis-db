package uk.ac.cam.cl.r244

import scala.collection.immutable.List

class CacheName(_table: String, _field: String, _queryType: String, _data: List[String]) {

    private var table = _table
    private var field = _field
    private var queryType = _queryType
    private var data = _data
    private val nameFormat = "%s:%s:%s:%s"
    private val sep = ":"

    def getTable(): String = {
        table
    }

    def getField(): String = {
        field
    }

    def getQueryType(): String = {
        queryType
    }

    def getData(): List[String] = {
        data
    }

    def setTable(_table: String): Unit = {
        table = _table
    }

    def setField(_field: String): Unit = {
        field = _field
    }

    def setQueryType(_queryType: String): Unit = {
        queryType = _queryType
    }

    def setData(_data: List[String]): Unit = {
        data = _data
    }

    override def toString(): String = {
        nameFormat.format(table, field, queryType, data.mkString(sep))
    }

    override def equals(name: Any): Boolean = {
        name match {
            case name: CacheName => name.table == table &&
                                    name.field == field && name.queryType == queryType &&
                                    name.data == data
            case _ => false
        }
    }

    override def hashCode(): Int = {
        this.toString().hashCode()
    }
}
