package com.lucidworks.spark


import com.lucidworks.spark.util._
import com.lucidworks.spark.rdd.SolrRDD
import org.apache.solr.client.solrj.SolrQuery
import org.apache.solr.client.solrj.request.schema.SchemaRequest.{Update, AddField, MultiUpdate}
import org.apache.solr.common.SolrInputDocument
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.Logging

import scala.collection.{mutable, JavaConversions}
import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer
import scala.util.control.Breaks._
import scala.reflect.runtime.universe._

import com.lucidworks.spark.util.QueryConstants._
import com.lucidworks.spark.util.ConfigurationConstants._

class SolrRelation(
    val parameters: Map[String, String],
    override val sqlContext: SQLContext,
    val dataFrame: Option[DataFrame])(
  implicit
    val conf: SolrConf = new SolrConf(parameters))
  extends BaseRelation
  with TableScan
  with PrunedFilteredScan
  with InsertableRelation
  with Logging {

  def this(parameters: Map[String, String], sqlContext: SQLContext) {
    this(parameters, sqlContext, None)
  }

  checkRequiredParams()
  // Warn about unknown parameters
  val unknownParams = SolrRelation.checkUnknownParams(parameters.keySet)
  if (!unknownParams.isEmpty)
    log.warn("Unknown parameters passed to query: " + unknownParams.toString())

  val sc = sqlContext.sparkContext
  val solrRDD = {
    var rdd = new SolrRDD(conf.getZkHost.get, conf.getCollection.get, sc)

    if (conf.splits.isDefined && conf.getSplitsPerShard.isDefined) {
      rdd = rdd.doSplits().splitsPerShard(conf.getSplitsPerShard.get)
    } else if (conf.splits.isDefined) {
      rdd = rdd.doSplits()
    }

    if (conf.getSplitField.isDefined && conf.getSplitsPerShard.isDefined) {
      rdd = rdd.splitField(conf.getSplitField.get).splitsPerShard(conf.getSplitsPerShard.get)
    } else if (conf.getSplitField.isDefined) {
      rdd = rdd.splitField(conf.getSplitField.get)
    }

    rdd
  }

  val baseSchema: StructType =
    SolrSchemaUtil.getBaseSchema(
      conf.getFields.toSet,
      conf.getZkHost.get,
      conf.getCollection.get,
      conf.escapeFieldNames.getOrElse(false),
      conf.flattenMultivalued.getOrElse(true))

  val query: SolrQuery = buildQuery
  val querySchema: StructType = {
    if (dataFrame.isDefined) {
      dataFrame.get.schema
    } else {
      if (query.getFields != null) {
        SolrSchemaUtil.deriveQuerySchema(query.getFields.split(","), baseSchema)
      } else {
        baseSchema
      }
    }
  }

  override def schema: StructType = querySchema

  override def buildScan(): RDD[Row] = buildScan(Array.empty, Array.empty)

  override def buildScan(fields: Array[String], filters: Array[Filter]): RDD[Row] = {

    if (fields != null && fields.length > 0) {
      // If all the fields in the base schema are here, we probably don't need to explicitly add them to the query
      if (this.baseSchema.size == fields.length) {
        // Special case for docValues. Not needed after upgrading to Solr 5.5.0 (unless to maintain back-compat)
        if (conf.docValues.getOrElse(false)) {
          fields.zipWithIndex.foreach({ case (field, i) => fields(i) = field.replaceAll("`", "") })
          query.setFields(fields: _*)
        }
      } else {
        fields.zipWithIndex.foreach({ case (field, i) => fields(i) = field.replaceAll("`", "") })
        query.setFields(fields: _*)
      }
    }

    // We set aliasing to retrieve docValues from function queries. This can be removed after Solr version 5.5 is released
    if (query.getFields != null && query.getFields.length > 0) {
      if (conf.docValues.getOrElse(false)) {
        SolrSchemaUtil.setAliases(query.getFields.split(","), query, baseSchema)
      }
    }

    // Clear all existing filters
    if (!filters.isEmpty) {
      query.remove("fq")
      filters.foreach(filter => SolrSchemaUtil.applyFilter(filter, query, baseSchema))
    }

    if (log.isInfoEnabled) {
      log.info("Constructed SolrQuery: " + query)
    }

    try {
      val querySchema = if (!fields.isEmpty) SolrSchemaUtil.deriveQuerySchema(fields, baseSchema) else schema
      val docs = solrRDD.query(query)
      val rows = SolrSchemaUtil.toRows(querySchema, docs)
      rows
    } catch {
      case e: Throwable => throw new RuntimeException(e)
    }
  }

  def toSolrType(dataType: DataType): String = {
    dataType match {
      case bi: BinaryType => "binary"
      case b: BooleanType => "boolean"
      case dt: DateType => "tdata"
      case db: DoubleType => "double"
      case ft: FloatType => "float"
      case i: IntegerType => "int"
      case l: LongType => "long"
      case s: ShortType => "int"
      case t: TimestampType => "tdate"
      case _ => "string"
    }
  }

  def toAddFieldMap(sf: StructField): Map[String,AnyRef] = {
    val map = scala.collection.mutable.Map[String,AnyRef]()
    map += ("name" -> sf.name)
    map += ("indexed" -> "true")
    map += ("stored" -> "true")
    map += ("docValues" -> "true")
    val dataType = sf.dataType
    dataType match {
      case at: ArrayType =>
        map += ("multiValued" -> "true")
        map += ("type" -> toSolrType(at.elementType))
      case _ => map += ("type" -> toSolrType(dataType))
    }
    map.toMap
  }

  override def insert(df: DataFrame, overwrite: Boolean): Unit = {

    val zkHost = conf.getZkHost.get
    val collectionId = conf.getCollection.get
    val dfSchema = df.schema
    val solrFields : Map[String, SolrFieldMeta] =
      SolrQuerySupport.getFieldTypes(Set(), SolrSupport.getSolrBaseUrl(zkHost), collectionId)

    // build up a list of updates to send to the Solr Schema API
    val fieldsToAddToSolr = new ListBuffer[Update]()
    dfSchema.fields.foreach(f => {
      if (!solrFields.contains(f.name))
        fieldsToAddToSolr += new AddField(toAddFieldMap(f).asJava)
    })
    val addFieldsUpdateRequest = new MultiUpdate(fieldsToAddToSolr.asJava)
    addFieldsUpdateRequest.process(SolrSupport.getCachedCloudClient(zkHost), collectionId)

    val docs = df.rdd.map(row => {
      val schema: StructType = row.schema
      val doc = new SolrInputDocument
      schema.fields.foreach(field => {
        val fname = field.name
        breakable {
          if (fname.equals("_version")) break()
        }
        val fieldIndex = row.fieldIndex(fname)
        val fieldValue : Option[Any] = if (row.isNullAt(fieldIndex)) None else Some(row.get(fieldIndex))
        if (fieldValue.isDefined) {
          val value = fieldValue.get
          value match {
            //TODO: Do we need to check explicitly for ArrayBuffer and WrappedArray
            case v: Iterable[Any] =>
              val it = v.iterator
              while (it.hasNext) doc.addField(fname, it.next())
            case _ => doc.setField(fname, value)
          }
        }
      })
      doc
    })
    SolrSupport.indexDocs(solrRDD.zkHost, solrRDD.collection, 100, docs) // TODO: Make the numDocs configurable
  }

  private def buildQuery: SolrQuery = {
    val query = SolrQuerySupport.toQuery(conf.getQuery.getOrElse("*:*"))
    val fields = conf.getFields

    if (fields.nonEmpty) {
      query.setFields(fields:_*)
    } else {
      // We add all the defaults fields to retrieve docValues that are not stored. We should remove this after 5.5 release
      if (conf.docValues.getOrElse(false))
        SolrSchemaUtil.applyDefaultFields(baseSchema, query, conf.flattenMultivalued.getOrElse(true))
    }

    query.setRows(scala.Int.box(conf.getRows.getOrElse(DEFAULT_PAGE_SIZE)))
    query.add(conf.solrConfigParams)
    query.set("collection", conf.getCollection.get)
    query
  }

  private def checkRequiredParams(): Unit = {
    require(conf.getZkHost.isDefined, "Param '" + SOLR_ZK_HOST_PARAM + "' is required")
    require(conf.getCollection.isDefined, "Param '" + SOLR_COLLECTION_PARAM + "' is required")
  }

}

object SolrRelation {
  def checkUnknownParams(keySet: Set[String]): Set[String] = {
    var knownParams = Set.empty[String]
    var unknownParams = Set.empty[String]

    // Use reflection to get all the members of [ConfigurationConstants] except 'CONFIG_PREFIX'
    val rm = scala.reflect.runtime.currentMirror
    val accessors = rm.classSymbol(ConfigurationConstants.getClass).toType.members.collect {
      case m: MethodSymbol if m.isGetter && m.isPublic => m
    }
    val instanceMirror = rm.reflect(ConfigurationConstants)

    for(acc <- accessors) {
      if (acc.name.decoded != "CONFIG_PREFIX") {
        knownParams += instanceMirror.reflectMethod(acc).apply().toString
      }
    }

    // Check for any unknown options
    keySet.foreach(key => {
      if (!knownParams.contains(key)) {
        // Now check if the prefix is "solr."
        if (!key.contains(CONFIG_PREFIX)) {
          unknownParams += key
        }
      }
    })
    unknownParams
  }
}

