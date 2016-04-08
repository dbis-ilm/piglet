package dbis.pig.codegen

import dbis.pig.op._
import dbis.pig.expr._
import dbis.pig.schema._
import dbis.pig.udf._
import dbis.pig.backends.BackendManager
import dbis.pig.plan.DataflowPlan
import org.clapper.scalasti.STGroupFile
import scala.collection.mutable.ListBuffer
import scala.collection.mutable.ArrayBuffer

import java.nio.file.Path
/**
 * An exception indicating failures in C++ compiling.
 *
 * @param msg a message describing the exception.
 */
case class CompilerException(private val msg: String) extends Exception(msg)

/**
 * The load functions enumeration. The function will be retrieved as
 * string in order to compare it directly with the given load function in the query. Accordingly,
 * the target code will be rendered for this function.
 */
object LoadFuncs extends Enumeration {
  val PigStorage = Funcs("PigStorage")
  val DBStorage = Funcs("DBStorage")
  val SPARQLStorage = Funcs("SPARQLStorage")
  def Funcs(name: String): Value with Matching =
    new Val(nextId, name) with Matching

  def unapply(s: String): Option[Value] =
    values.find(s == _.toString)

  trait Matching {
    def unapply(s: String): Boolean =
      (s == toString)
  }
}
import LoadFuncs._

class CppBackendCodeGen(template: String) extends CodeGeneratorBase {

  templateFile = template

  val startupList = ListBuffer[PigOperator]()
  var groupingNode: Grouping = null

  /*------------------------------------------------------------------------------------------------- */
  /*                                  Cpp-specific code generators                                  */
  /*------------------------------------------------------------------------------------------------- */

  private val cppTypeMappingTable = Map[PigType, String](
    Types.IntType -> "int",
    Types.LongType -> "long",
    Types.FloatType -> "float",
    Types.DoubleType -> "double",
    Types.CharArrayType -> "std::string",
    Types.ByteArrayType -> "std::string")

  private def quote(str: String) = s""""$str""""

  /**
   * *
   * Construct the tuple schema typedef for each operator in the system. Each operator
   * in pipefabric should specify typedefs for its input and output. These typedefs (schemas)
   * are used during the tuple processing in this operator.
   * @param schema to build the schema typedef accordingly for the operators
   */
  def schemaToTupleStruct(schema: Option[Schema]): String = schema match {
    // TODO: works only for primitive types
    case Some(s) => s.fields.map(f => cppTypeMappingTable(f.fType)).mkString(",")
    case None    => throw SchemaException("Cannot handle unknown schema.")
  }

  /**
   * Generate C++ code for retrieving the attributes values in the pipefabric system. This is done by calling ns_types::get in the system.
   * In this function, we are focusing in getting the index of the attribute in the schema, thus,
   * ns_types::get<$idx> can be rendered directly.
   * If the field is NamedField, we should get the index by calling indexOfField via the schema class.
   * If it is PositionalField, we can get the index directly and if it is Value, we used this value as it is.
   * @param schema the schema of the operator to get the index of the field
   * @param ref to find the type of field whether NamedField, PositionalField, or Value
   * @param tuple
   * @return
   */
  private def emitRef(schema: Option[Schema], ref: Ref, tuple: String = "tp"): String = ref match {
    case nf @ NamedField(f, _) => schema match {
      case Some(s) => {
        val idx = s.indexOfField(nf)
        if (idx == -1)
          throw SchemaException(s"unknown $f in schema")
        else
          s"ns_types::get<$idx>(*$tuple)"
      }
      case None => throw SchemaException(s"unknown schema for field $f")
    }
    case PositionalField(pos) => s"ns_types::get<$pos>(*$tuple)"
    case Value(v)             => v.toString
    case DerefTuple(r1, r2)   => s"${emitRef(tupleSchema(schema, r1), r2)}"
    case _                    => ""
  }

  /**
   *
   * @param schema
   * @param ref
   * @return
   */
  def tupleSchema(schema: Option[Schema], ref: Ref): Option[Schema] = {
    val tp = ref match {
      case nf @ NamedField(f, _) => schema match {
        case Some(s) => s.field(nf).fType
        case None    => throw new SchemaException(s"unknown schema for field $f")
      }
      case PositionalField(p) => schema match {
        case Some(s) => s.field(p).fType
        case None    => None
      }
      case _ => None
    }
    if (tp == None)
      None
    else
      Some(Schema(if (tp.isInstanceOf[BagType]) tp.asInstanceOf[BagType] else BagType(tp.asInstanceOf[TupleType])))
  }

  private def getFieldInfo(schema: Option[Schema], ref: Ref): (Int, PigType) = schema match {
    case Some(s) => {
      ref match {
        case nf @ NamedField(f, _) => {
          val idx = s.indexOfField(nf)
          if (idx == -1)
            throw SchemaException(s"unknown $f in schema")
          else
            (idx, s.field(nf).fType)
        }
        case PositionalField(pos) => (pos, s.field(pos).fType)
        case DerefTuple(r1, r2)   => getFieldInfo(schema, r2)
        case _                    => (0, Types.AnyType)
      }
    }
    case None => throw SchemaException("unknown schema for the field")
  }

  /**
   * Generate C++ code for a predicate on expressions.
   *
   * @param schema the optional input schema of the operator where the expressions refer to.
   * @param predicate the actual predicate
   * @return a string representation of the generated C++ code
   */
  private def emitPredicate(schema: Option[Schema], predicate: Predicate): String = predicate match {
    case Eq(left, right)  => { s"${emitExpr(schema, left)} == ${emitExpr(schema, right)}" }
    case Neq(left, right) => { s"${emitExpr(schema, left)} != ${emitExpr(schema, right)}" }
    case Leq(left, right) => { s"${emitExpr(schema, left)} <= ${emitExpr(schema, right)}" }
    case Lt(left, right)  => { s"${emitExpr(schema, left)} < ${emitExpr(schema, right)}" }
    case Geq(left, right) => { s"${emitExpr(schema, left)} >= ${emitExpr(schema, right)}" }
    case Gt(left, right)  => { s"${emitExpr(schema, left)} > ${emitExpr(schema, right)}" }
    case And(left, right) => s"${emitPredicate(schema, left)} && ${emitPredicate(schema, right)}"
    case Or(left, right)  => s"${emitPredicate(schema, left)} || ${emitPredicate(schema, right)}"
    case Not(pred)        => s"!(${emitPredicate(schema, pred)})"
    case PPredicate(pred) => s"(${emitPredicate(schema, pred)})"
    case _                => { s"UNKNOWN PREDICATE: $predicate" }
  }

  /**
   * Generate C++ code for an arithmetic expression for some operators such as
   * filter, projection operator, etc. The function will be called recursively
   * to render the content.
   * @param schema the schema of the operator
   * @param expr a part from the complete arithmetic expression
   * @return C++ code for the arithmetic expression
   */
  private def emitExpr(schema: Option[Schema], expr: ArithmeticExpr): String = expr match {
    /*
    case CastExpr(t, e) => {
      // TODO: check for invalid type
      val targetType = cppMappingTable(t)
      s"${emitExpr(schema, e)}.to$targetType"
    }
    */
    case PExpr(e)      => s"(${emitExpr(schema, e)})"
    case MSign(e)      => s"-${emitExpr(schema, e)}"
    case Add(e1, e2)   => s"${emitExpr(schema, e1)} + ${emitExpr(schema, e2)}"
    case Minus(e1, e2) => s"${emitExpr(schema, e1)} - ${emitExpr(schema, e2)}"
    case Mult(e1, e2)  => s"${emitExpr(schema, e1)} * ${emitExpr(schema, e2)}"
    case Div(e1, e2)   => s"${emitExpr(schema, e1)} / ${emitExpr(schema, e2)}"
    case RefExpr(e)    => s"${emitRef(schema, e)}"
    /*
    case Func(f, params) => {
      val pTypes = params.map(p => { p.resultType(schema)._2 })
      UDFTable.findUDF(f, pTypes) match {
        case Some(udf) => {
          if (udf.isAggregate) s"${udf.scalaName}(${emitExpr(schema, params.head, false)}.asInstanceOf[Seq[Any]])"
          else s"${udf.scalaName}(${params.map(e => emitExpr(schema, e)).mkString(",")})"
        }
        case None => {
          // TODO: we don't know the function yet, let's assume there is a corresponding Scala function
          s"$f(${params.map(e => emitExpr(schema, e)).mkString(",")})"
        }
      }
    }*/
    /*
    case FlattenExpr(e) => emitExpr(schema, e)
    case ConstructTupleExpr(exprs) => s"PigFuncs.toTuple(${exprs.map(e => emitExpr(schema, e)).mkString(",")})"
    case ConstructBagExpr(exprs) => s"PigFuncs.toBag(${exprs.map(e => emitExpr(schema, e)).mkString(",")})"
    case ConstructMapExpr(exprs) => s"PigFuncs.toMap(${exprs.map(e => emitExpr(schema, e)).mkString(",")})"
    */
    case _             => println("unsupported expression: " + expr); ""
  }

  /**
   * Generate C++ code for source operators (i.e, load) in order to let them to start publishing
   * their tuples
   */
  private def emitStartupCode: String = {
    startupList.map(op => op match {
      case Load(out, _, _, _, _) => callST("source_start", Map("op_name" -> s"op_${out.name}"))
      case _                     => ""
    }).mkString("\n")
  }

  /**
   * Generates code for the LOAD operator with different functions. These functions
   * include PigStorage, DBStorage, and  SPARQLStorage. Depending on the function,
   * equivalent placeholder will be rendered. The PigStorage is the file source in the
   * pipefabric and needs two parameters (i.e., the file name and the separator (default = ",")). The DBStorage
   * reads tuples from a particular database. Hence, database information should be specified
   * such as the DB connection as well as a selection query to retrieve the data from a certain
   * table. The SPARQLStorage is similar to DBStorage but instead of reading tuples
   * from the DB, the tuples will be read from a RDF graph.
   *
   *
   * @param out the name of the output pipe
   * @param storage the name of the storage to be loaded. This can be a file or a selection query
   * @param loaderFunc an optional loader function (default = PigStorage)
   * @param loaderParams an optional list of parameters to a loader function (e.g., separator and DB connections)
   * @param node the load operator itself. It is used primary here for getting the schema and the output pipe name
   * @return the C++ code implementing the LOAD operator
   */

  private def emitLoader(out: String, storage: String, loaderFunc: Option[String], params: List[String], node: PigOperator): String = {
    var loaderParams = params // replace ' character with empty character
    if (loaderParams != null)
      loaderParams = params.map(_.replace("'", ""))
    
    loaderFunc.getOrElse(BackendManager.backend.defaultConnector) match {
      case LoadFuncs.PigStorage() =>
        val path = if (storage.startsWith("/")) "" else new java.io.File(".").getCanonicalPath + "/"
        callST("file_source", Map("op_name" -> s"op_${out}",
          "input_file" -> (path + storage),
          "tuple_type" -> s"${node.outPipeName}_TupleType", "ifs" -> (if (loaderParams != null) loaderParams.head else ",")))
      case LoadFuncs.DBStorage() =>
        if (loaderParams == null || loaderParams.isEmpty)
          throw CompilerException("DB connection should be specified in the parametrs")
        callST("db_source", Map("op_name" -> s"op_${out}",
          "query" -> storage,
          "tuple_type" -> s"${node.outPipeName}_TupleType",
          "prepare" -> {
            node.schema match {
              case Some(s) => s.fields.zipWithIndex.map { case (f, i) => s"r.get<${cppTypeMappingTable(f.fType)}>(${i})" }.mkString(",")
              case None    => throw CompilerException("the load operator has to have a schema")
            }
          },
          "db_connection" -> loaderParams.head))
      case LoadFuncs.SPARQLStorage() =>
        if (loaderParams == null || loaderParams.isEmpty)
          throw CompilerException("Backend and endpoint should be specified in the parametrs ")
        callST("sparql_source", Map("op_name" -> s"op_${out}",
          "query" -> storage,
          "tuple_type" -> s"${node.outPipeName}_TupleType",
          "backend" -> loaderParams(0).split(":")(0).toLowerCase(),
          "endpoint" -> loaderParams(0)))
      case _ => throw CompilerException("The load function is not supported")
    }
  }

  /**
   * Render the C++ projection expression for (foreach) operator in the template
   * @param node the operator itself
   * @param out the name of the output pipe (bag)
   * @param in the name of the input pipe (bag)
   * @param gen the foreach expression as list of generator expressions
   */
  private def emitForeach(node: PigOperator, out: String, in: String, expr: List[GeneratorExpr]): String = {
    callST("foreach", Map("op_name" -> s"op_${out}",
      "input" -> s"op_${in}",
      "in_tuple_type" -> s"${in}_TupleType",
      "out_tuple_type" -> s"${out}_TupleType",
      "expr" -> {
        emitGenerator(node.inputSchema, expr)
      }))
  }

  /**
   * Generate (currently) the projection expression  for (foreach) operator by iterating
   * on the expression part-by-part and retrieve the C++ code for this part.
   * @param schema  the schema of the operator
   * @param genExprs a list of the generator expressions to be translated to C++ Code
   */
  private def emitGenerator(schema: Option[Schema], genExprs: List[GeneratorExpr]): String = {
    s"${genExprs.map(e => emitExpr(schema, e.expr)).mkString(",")}"

  }

  /**
   * Generate C++ code for the union operator recursively if multiple relations are given in this operator.
   * Since pipefabric union operator supports only two relation as inputs and one output,the system should handle
   * each two relations separately by creating a new operator. This new operator should be an input for a
   * the other union operator, and so on .
   * For example, D = union A, B, C will be rendered as
   * A_1 = union B, C;
   * D = union A, A_1 ;
   * @param  out the final operator name
   * @param rels the name of relations
   * @param union a string container to store all unions
   */
  private def constructUnion(out: String, rels: ListBuffer[String], union: StringBuilder): Unit = {
    val index = rels.size
    if (index == 2) {
      union ++= callST("union", Map("op_name" -> s"op_${out}", "input1" -> s"op_${rels(0)}", "input2" -> s"op_${rels(1)}", "tuple_type" -> s"${out}_TupleType")) + "\n"
      return
    }
    val newOutput = s"${out}_${index - 1}"
    union ++= callST("union", Map("op_name" -> s"op_${newOutput}", "input1" -> s"op_${rels(index - 1)}", "input2" -> s"op_${rels(index - 2)}", "tuple_type" -> s"${out}_TupleType")) + "\n"
    rels.trimEnd(2)
    rels += newOutput
    constructUnion(out, rels, union)
  }

  /**
   * Generate C++ code for the join operator recursively if multiple relations are given in this operator.
   * Since pipefabric join operator supports only two relation as inputs and one output,the system should handle
   * each two relations separately by creating a new operator. This new operator should be an input for a
   * the other union operator, and so on .
   * For example, D = join A BY (x), B (y), C (z) will be rendered as
   * A_1 = join B BY (y), C BY(z);
   * D = join A (x), A_1 (yz) ;
   * Note the schema of  A_1 is the combination of y and z schemas. This will be handled by emitHelperClass function (part Join)
   * @param  out the final operator name
   * @param rels the name of relations
   * @param union a string container to store all unions
   */
  private def constructJoin(out: String, rels: ListBuffer[String], keys: List[String], predicates: List[String], join: StringBuilder): Unit = {
    val index = rels.size
    if (index == 2) { // right (0) ---> left (1)
      join ++= callST("relh_join", Map("op_name" -> s"op_${out}",
        "input2" -> s"op_${rels(0)}",
        "input1" -> s"op_${rels(1)}",
        "rtuple_type" -> s"${rels(0)}_TupleType",
        "ltuple_type" -> s"${rels(1)}_TupleType",
        "rhash_expr" -> keys(0),
        "lhash_expr" -> keys(1),
        "join_predicate" -> { predicates(0) })) + "\n"
      return
    }
    val newOutput = s"${out}_${index - 1}"
    join ++= callST("relh_join", Map("op_name" -> s"op_${newOutput}",
      "input2" -> s"op_${rels(index - 2)}",
      "input1" -> s"op_${rels(index - 1)}",
      "rtuple_type" -> s"${rels(index - 2)}_TupleType",
      "ltuple_type" -> s"${rels(index - 1)}_TupleType",
      "rhash_expr" -> keys(index - 2),
      "lhash_expr" -> keys(index - 1),
      "join_predicate" -> predicates(index - 2))) + "\n"
    rels.trimEnd(2)
    //keys.trimEnd(2)
    rels += newOutput
    constructJoin(out, rels, keys, predicates, join)
  }
  /**
   * Generate C++ code for the order expression in OrderBy operator based on the order specifications which is necessary
   * to determine the tuples order in pipefaric system
   * For example By x ASC will generate  tp1.x < tp2.x while By x ASC, y ASC
   * will generate tp1.x < tp2.x || (tp1.y < tp2.y && tp1.x == tp2.x )
   * @param schema the schema of the operator
   * @param orderSpec a list of order specification
   */
  private def emitOrderExpr(schema: Option[Schema], orderSpec: List[OrderBySpec]): String = {
    var counter: Int = 0;
    var comparePredicate = new StringBuilder()
    var positions = new ListBuffer[Ref]
    for (spec <- orderSpec) {
      positions += spec.field
      comparePredicate ++= (s" ( ${emitRef(schema, spec.field, "tp")}" + (if (spec.dir == OrderByDirection.AscendingOrder) "<" else ">") + s"${emitRef(schema, spec.field, "tp1")}")
      for (i <- 1 to counter)
        comparePredicate ++= (s" && ${emitRef(schema, positions(i - 1), "tp")} == ${emitRef(schema, positions(i - 1), "tp1")}");
      comparePredicate += ')'
      counter += 1;
      if (counter < orderSpec.size)
        comparePredicate ++= " || ";
    }
    comparePredicate += ';'
    comparePredicate.toString()
  }
  /**
   * Creates the C++ code for implementing the hashing of the join attributes.
   * @param the join operator schema
   * @param joinExpr the join expression
   */
  private def emitJoinKey(schema: Option[Schema], joinExpr: List[Ref]): String = {
    joinExpr.map(e => s"""boost::hash_combine(seed, ${emitRef(schema, e)});""").mkString("\n")
  }

  /**
   * Creates the C++ code for implementing the predicate of the join attributes.
   */
  private def emitPredicate(schemas: Tuple2[Option[Schema], Option[Schema]], predicates: List[(Ref, Ref)]): String = {
    predicates.map { case (l, r) => s"""${emitRef(schemas._1, l, "tp1")} == ${emitRef(schemas._2, r, "tp2")}""" }.mkString(" && ")
  }

  private def generateFormat(schema: Option[Schema], stringDelim: String = "','"): String = schema match {
    case Some(s) => (0 to s.fields.length - 1).toList.map { i => s"ns_types::get<$i>(*tp)"
    }.mkString(s"$stringDelim<<")
    case None => throw CompilerException("the schema should be defined to define a format")
  }

  def emitSchemaHelpers(schemas: List[Schema]): String = ""

  /**
   * Generate code for the given Pig operator. The system will go through each operator and render
   * its content in the template
   *
   * @param node the operator (an instance of PigOperator)
   * @return a string representing the code
   */
  def emitNode(node: PigOperator): String = {
    node match {
      case Load(out, storage, schema, func, params) => {
        startupList += node
        emitLoader(out.name, storage.toString, func, params, node)
      }
      case Dump(in) => callST("dump", Map("op_name" -> s"op_dump_${in.name}", "input" -> s"op_${in.name}", "tuple_type" -> s"${in.name}_TupleType"))
      case Store(in, file, func, params) => {
        val useFomrat = if (params != null && params.isDefinedAt(0)) true else false
        callST("store", Map("op_name" -> s"op_store_${in.name}", "input" -> s"op_${in.name}", "tuple_type" -> s"${in.name}_TupleType", "file" -> file.toString, "use_format" -> useFomrat, "format" -> generateFormat(node.schema, if (useFomrat) params(0) else "','")))
      }

      case Foreach(out, in, gen, _) => {
        var returnValue: String = ""
        gen match {
          case GeneratorList(expr) => {
            if (checkAggrFun(expr, node.schema) == true) {
              returnValue = generateAggregation(out.name, in.name, expr, node.inputs.head.producer.schema)
            } else returnValue = emitForeach(node, out.name, in.name, expr)
          }
        }
        groupingNode = null
        returnValue
      }

      case Filter(out, in, pred, _) => callST("filter", Map("op_name" -> s"op_${out.name}",
        "input" -> s"op_${in.name}",
        "tuple_type" -> s"${in.name}_TupleType",
        "expr" -> emitPredicate(node.schema, pred)))
      case Union(out, rels) => {
        var unionChain = new StringBuilder
        val copyRels = rels.map(_.name).to[ListBuffer]
        constructUnion(out.name, copyRels, unionChain)
        unionChain.toString()
      }
      case g @ Grouping(out, in, groupExpr, _) => {
        //node.schema = node.inputSchema
        groupingNode = g // TODO: instead we can check the previous node instead of storing it
        "" // nothing to do with grouping
      }
      case Join(out, rels, exprs, _) => {

        var joinChain = new StringBuilder
        val copyRels = rels.map(_.name).to[ListBuffer]
        val res = node.inputs.zip(exprs)
        val keys = res.map { case (i, k) => emitJoinKey(i.producer.schema, k) }
        val leftRightKeys = res.zip(res.drop(1))
        val predicates = leftRightKeys.map { case (i, k) => emitPredicate((i._1.producer.schema, k._1.producer.schema), i._2.zip(k._2)) }
        constructJoin(out.name, copyRels, keys, predicates, joinChain)
        joinChain.toString()
      }
      case Window(out, in, window, slide) => {
        callST("sliding_window", Map("op_name" -> s"op_${out.name}",
          "input" -> s"op_${in.name}",
          "tuple_type" -> s"${in.name}_TupleType",
          "win_type" -> (if (window._2.isEmpty) "RowWindow" else "RangeWindow"),
          "win_size" -> (if (window._2 == "minutes") (window._1 * 60) else window._1),
          "slide_len" -> (if (slide._2.isEmpty) throw CompilerException("Currently, row sliding is not supported")
          else (if (slide._2 == "minutes") (slide._1 * 60) else slide._1))))
      }
      case OrderBy(out, in, orderSpec, _) => callST("order_by", Map("op_name" -> s"op_${out.name}",
        "tuple_type" -> s"${in.name}_TupleType",
        "input" -> s"op_${in.name}",
        "expr" -> emitOrderExpr(node.schema, orderSpec)))
      case SocketRead(out, address, mode, _, _, _) => callST("zmq_source", Map("op_name" -> s"op_${out.name}", "endpoint" -> s"${address.protocol}${address.hostname}:${address.port}", "tuple_type" -> s"${node.outPipeName}_TupleType"))
      case SocketWrite(in, address, mode, _, _) => {
        callST("zmq_sink", Map("op_name" -> s"op_zmq_sink_${in.name}",
          "input" -> in.name,
          "endpoint" -> s"${address.protocol}${address.hostname}:${address.port}",
          "tuple_type" -> s"${in.name}_TupleType"))
      }
      case _ => ""
    }
  }

  /**
   * Generate code needed for importing packages, classes, etc.
   *
   * @return a string representing the import code
   */
  def emitImport(additionalImports: Option[String]): String = callST("init_code")

  /**
   * Generate code for the header of the script outside the main class/object,
   * e.g. defining the main object.
   *
   * @param scriptName the name of the script (e.g. used for the object)
   * @return a string representing the header code
   */
  def emitHeader1(scriptName: String): String = {
    "" // TODO: typedefs for all tuple types callST("tuple_typedef")
  }

  def emitEmbeddedCode(additionalCode: String): String = ???
  
  /**
   * Generate code for the header of the script which should be defined inside
   * the main class/object.
   *
   * @param scriptName the name of the script (e.g. used for the object)
   * @return a string representing the header code
   */
  def emitHeader2(scriptName: String, enableProfiling: Boolean): String = callST("begin_query")

  /**
   * Generate code needed for finishing the script.
   *
   * @return a string representing the end of the code.
   */
  def emitFooter(plan: DataflowPlan): String = callST("parameterize_query") + emitStartupCode + callST("end_query")

  /**
   * Generate code for any helper class/function if needed by the given operator.
   *
   * @param node the Pig operator requiring helper code
   * @return a string representing the helper code
   */
  def emitHelperClass(node: PigOperator): String = node match {
    // case SplitInto() => "" // multiple outputs
    case Dump(in)                 => "" // no output
    case Store(in, file, func, _) => "" // no output
    case Join(out, rels, exprs, _) => { // in case of multiple relations join, a new schema will be created 
      var types = new ListBuffer[String] // between each consecutive relations
      var fields = new ArrayBuffer[Field]
      val inputs = node.inputs
      for (i <- rels.length - 1 to 2 by -1) {
        fields ++= inputs(i - 1).producer.schema.get.fields ++ inputs(i).producer.schema.get.fields
        val joinSchema = Some(Schema(BagType(TupleType(fields.toArray))))
        types += callST("tuple_typedef", Map("tuple_struct" -> schemaToTupleStruct(joinSchema),
          "type_name" -> s"${node.outPipeName}_${i}_TupleType"))
      }
      types += emitOpType(node)
      types.mkString
    }
    case g @ Grouping(out, in, groupExpr, _) => {
      //node.schema = node.inputSchema
      groupingNode = g // TODO: instead we can check the previous node instead of storing it
      //emitOpType(node) // not necessary since it will get from the previous operator
      ""
    }
    case Foreach(out, in, gen, _) => {
      gen match {
        case GeneratorList(expr) => {
          if (checkAggrFun(expr, node.schema) == true) {
            var targetSchema: Option[Schema] = null
            if (groupingNode == null)
              targetSchema = node.inputs.head.producer.schema
            else
              targetSchema = groupingNode.inputs.head.producer.schema
            emitOpType(node) + generateAggrHelperCode(out.name, expr, node.inputs.head.producer.schema)
          } else emitOpType(node)
        }
      }
    }
    case _ => emitOpType(node)
  }
  
  
  def emitStageIdentifier(line: Int, lineage: String): String = ???
  
  /**
   * generate the code of the aggregation operator
   * @param node the node itself (operator)
   */
  private def generateAggregation(out: String, in: String, generators: List[GeneratorExpr], schema: Option[Schema]): String = {
    val realtime = false
    val slen = "UINT_MAX"
    //val publish = "true" // full = true, delta = false
    //val punctuation = "Punctuation::All"
    val map = Map("op_name" -> s"op_${out}",
      "out_tuple_type" -> s"${out}_TupleType",
      "realtime" -> realtime,
      "slen" -> slen,
      "iterate" -> buildIterateExpr(generators, schema),
      "finalize" -> buildFinalizeExpr(generators, schema))
    if (groupingNode == null) {
      // grouped aggregation
      callST("aggr", map ++ Map("in_tuple_type" -> s"${in}_TupleType", "input" -> s"op_${in}"))
    } else
      callST("gaggr", map ++ Map("hash" -> buildGroupingList(schema), "in_tuple_type" -> s"${groupingNode.inputs.head.name}_TupleType", "input" -> s"op_${groupingNode.inputs.head.name}"))
  }
  /**
   * Generate a helper code for the aggregation operator
   */
  def generateAggrHelperCode(out: String, generators: List[GeneratorExpr], schema: Option[Schema]): String = {

    val aggregates = buildAggregates(generators, schema)
    val initExpr = buildInitExpr(generators, schema)

    val map = Map("op_name" -> s"op_${out}",
      "aggr_fun" -> aggregates,
      "init" -> initExpr)
    if (groupingNode == null) {
      // grouped aggregation
      callST("aggr_helper", map)
    } else
      callST("gaggr_helper", map ++ Map("match" -> buildMatchExpr(schema, generators)))
  }
  private def emitOpType(node: PigOperator): String = {
    callST("tuple_typedef", Map("tuple_struct" -> schemaToTupleStruct(node.schema),
      "type_name" -> s"${node.outPipeName}_TupleType"))
  }

  /**
   * Build the C++ code for the aggregate function by analyzing the functions
   * in foreach operator.
   *
   * @param generators the list of generators from foreach operator
   * @param schema the input schema
   */
  private def buildAggregates(generators: List[GeneratorExpr], schema: Option[Schema]): String = {
    var resultType = ""
    val aggregates = generators.zipWithIndex.map {
      case (gen, i) => gen.expr match {
        case fun @ Func(f, params) => {
          val aggr = getAggrFun(fun, schema)
          if (aggr != null) {
            val inType = cppTypeMappingTable(aggr.paramTypes(0))
            resultType = cppTypeMappingTable(aggr.resultType)
            aggr.name.toLowerCase() match {
              case "count" => "AggrCount<" + inType + ", int>" + " count" + i + "_;"
              case "sum"   => "AggrSum<" + inType + "> sum" + i + "_;"
              case "min"   => "AggrMinMax<" + inType + ", std::less<" + inType + ">> min" + i + "_;"
              case "max"   => "AggrMinMax<" + inType + ", std::greater<" + inType + ">> max" + i + "_;"
              case "avg"   => "AggrAvg<" + inType + ", " + resultType + "> avg" + i + "_;"
              case _       => throw CompilerException("this aggregate funcrion is not supported")
            }
          } else {
            throw CompilerException("only aggregate funcrions is allowed here")
          }
        }
        case RefExpr(e) => {
          val resultType = getFieldInfo(schema, e)._2
          cppTypeMappingTable(resultType) + " group" + i + "_;"
        }
      }
    }
    aggregates.mkString("\n")
  }

  private def buildInitExpr(generators: List[GeneratorExpr], schema: Option[Schema]): String = {
    val initValue = generators.zipWithIndex.map {
      case (gen, i) => gen.expr match {
        case fun @ Func(f, params) => {
          val aggr = getAggrFun(fun, schema)
          if (aggr != null) {
            aggr.name.toLowerCase() + i + "_.init();"
          } else {
            throw CompilerException("only aggregate funcrions is allowed here")
          }
        }
        case RefExpr(e) => {
          val resultType = getFieldInfo(schema, e)._2
          "group" + i + "_ = " + (if (Types.isNumericType(resultType)) "0" else """""") + ";"
        }
      }
    }
    initValue.mkString("\n")
  }

  /**
   * Generate the C++ code for the iterate function in aggregation
   *
   * @param generators the list of generators in foreach operator
   * @param schema the input schema
   */
  private def buildIterateExpr(generators: List[GeneratorExpr], schema: Option[Schema]): String = {
    var resultType: Int = 0
    val iterateValue = generators.zipWithIndex.map {
      case (gen, i) => gen.expr match {
        case fun @ Func(f, params) => {
          val aggr = getAggrFun(fun, schema)
          if (aggr != null) {
            val attr = params(0).asInstanceOf[RefExpr].r
            "myState->" + aggr.name.toLowerCase() + i + "_.iterate(" + emitRef(schema, attr) + ", outdated);"
          } else {
            throw CompilerException("only aggregate funcrions is allowed here")
          }
        }
        case RefExpr(e) => {
          "myState->group" + i + "_ = " + emitRef(schema, e) + ";"
        }
      }
    }
    iterateValue.mkString("\n")

  }
  /**
   * Generate the C++ code for the final function in aggregation
   *
   * @param generators the list of generators in foreach operator
   * @param schema the input schema
   */
  private def buildFinalizeExpr(generators: List[GeneratorExpr], schema: Option[Schema]): String = {
    val finalizeExpr = generators.zipWithIndex.map {
      case (gen, i) => gen.expr match {
        case fun @ Func(f, params) => {
          val aggr = getAggrFun(fun, schema)
          if (aggr != null)
            "myState->" + aggr.name.toLowerCase() + i + "_.value()"
          else {
            throw CompilerException("only aggregate funcrions is allowed here")
          }
        }
        case RefExpr(e) => {
          "myState->group" + i + "_"
        }
      }
    }
    finalizeExpr.mkString(",")
  }

  /**
   * Generate the C++ code for the GroupMatch function in aggregation in case of grouping
   *
   * @param generators the list of generators in foreach operator
   * @param schema the input schema
   */
  private def buildMatchExpr(schema: Option[Schema], generators: List[GeneratorExpr]): String = {
    val groupIdx = generators.zipWithIndex.filter { case (gen, i) => gen.expr.isInstanceOf[RefExpr] }
    val matchExpr = groupingNode.groupExpr.keyList.zipWithIndex.map {
      case (e, i) =>
        s"group${groupIdx(i)._2}_ == ${emitRef(schema, e)}"
    }
    matchExpr.mkString("&&")
  }

  /**
   * Generate a C++ code for the hashing of the grouping attributes.
   *
   * @param schema the input schema
   */
  private def buildGroupingList(schema: Option[Schema]): String = schema match {
    case Some(s) => {
      val hash = groupingNode.groupExpr.keyList.map {
        e =>
          s"boost::hash_combine(seed, ${emitRef(schema, e)});"
      }
      hash.mkString("\n")
    }
    case None => throw SchemaException(s"unknown schema")
  }
  /**
   * check if the function is an aggregation function
   * @param schema the schema of the operator
   * @param aggr the function to be checked
   */
  private def getAggrFun(aggr: Func, schema: Option[Schema]): UDF = {
    val pTypes = aggr.params.map(p => p.resultType(schema))
    UDFTable.findUDF(aggr.f, pTypes) match {
      case Some(udf) => {
        if (udf.isAggregate) udf else null
      }
      case None => null
    }
  }
  private def checkAggrFun (generators: List[GeneratorExpr], schema: Option[Schema]): Boolean = {
    generators.map(_.expr).
                   filter(ex => ex.isInstanceOf[Func]).asInstanceOf[List[Func]].
                   exists(aggr => (getAggrFun(aggr, schema)!= null))
  }
}

class CppBackendGenerator(templateFile: String) extends CodeGenerator {
  override val codeGen = new CppBackendCodeGen(templateFile)
}
