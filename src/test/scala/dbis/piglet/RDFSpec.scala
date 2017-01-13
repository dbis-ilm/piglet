/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package dbis.piglet

import java.net.URI
import dbis.piglet.op._
import dbis.piglet.expr._
import dbis.piglet.parser.PigParser.parseScript
import dbis.piglet.plan.DataflowPlan
import dbis.piglet.plan.rewriting.internals.RDF
import dbis.piglet.schema._
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.{OptionValues, FlatSpec, Matchers}

class RDFSpec extends FlatSpec with Matchers with TableDrivenPropertyChecks with OptionValues {
  "TriplePatterns" should "be convertible to strings" in {
    val patterns = Table(
      ("patterns", "string"),
      (List(TriplePattern(NamedField("a"), Value("\"firstName\""), Value("\"Stefan\""))),
        """{ ?a "firstName" "Stefan" }"""),
      (List(TriplePattern(PositionalField(0), Value("\"firstName\""), Value("\"Stefan\""))),
        """{ $0 "firstName" "Stefan" }"""),
      (List(
        TriplePattern(PositionalField(0), Value("\"firstName\""), Value("\"Stefan\"")),
        TriplePattern(PositionalField(0), Value("\"lastName\""), Value("\"Hage\""))),
        """{ $0 "firstName" "Stefan" . $0 "lastName" "Hage" }"""
        )
    )

    forAll(patterns) {(patterns: List[TriplePattern], bgpstring: String) =>
      RDF.triplePatternsToString(patterns) shouldBe bgpstring
    }
  }

  "A BGPFilters schema" should "contain named fields for all variables if there is more than one pattern" in {
    val plan = new DataflowPlan(parseScript(
      s"""A = RDFLOAD('file.rdf');
         |B = BGP_FILTER A by { ?b "firstName" "Wieland" . ?b "lastName" "Hoffmann" .  ?b "birthDate" ?a};
         |DUMP B;
       """.stripMargin))
    val bgpfilter = plan.findOperatorForAlias("B").value
    val shouldSchema: Some[Schema] = Some(
    Schema(
      BagType(
        TupleType(
          Array(
            Field("a", Types.CharArrayType),
            Field("b", Types.CharArrayType))))))
    bgpfilter.schema shouldBe shouldSchema
  }

  it should "be the plain RDF schema if there is only one pattern and the BGPFilter reads plain data" in {
    val plan = new DataflowPlan(parseScript(
      s"""A = RDFLOAD('file.rdf');
         |B = BGP_FILTER A by { ?a "firstName" "Wieland" };
         |DUMP B;
       """.stripMargin))
    val bgpfilter = plan.findOperatorForAlias("B").value
    bgpfilter.schema shouldBe RDFLoad.plainSchema
  }

  it should "be the grouped RDF schema if there is only one pattern and the BGPFilter reads grouped data" in {
    val groupers = Table(
      ("grouping column"),
      "subject",
      "predicate",
      "object"
    )
    forAll(groupers) { g =>
      val plan = new DataflowPlan(parseScript(
      s"""A = RDFLOAD('file.rdf') grouped on $g;
        |B = BGP_FILTER A by { ?a "firstName" "Wieland" };
        |DUMP B;
      """.stripMargin))
      val bgpfilter = plan.findOperatorForAlias("B").value
      bgpfilter.schema.value shouldBe RDFLoad.groupedSchemas(g)
    }
  }

  it should "not conform to schemas that are neither plain nor grouped RDF data" in {
    val asSchemas = Table(
      ("schema"),
      "",
      "as (x: chararray, y: chararray, z: chararray)",
      "as (x: int, y: int)",
      "as (x: int, y: int, z: int, a: chararray)"
    )
    forAll(asSchemas) { asSchema =>
      val plan = new DataflowPlan(parseScript(
        s"""A = Load 'file' $asSchema;
           |B = BGP_FILTER A by { ?a "firstName" "Wieland" };
           |DUMP B;
        """.stripMargin) )
      val bgpfilter = plan.findOperatorForAlias("B").value
      bgpfilter.checkSchemaConformance shouldBe false
      }
  }

  it should "conform to plain RDF data" in {
    val plan = new DataflowPlan(parseScript(
      s"""A = Load 'file' as (subject: chararray, predicate: chararray, object: chararray);
         |B = BGP_FILTER A by { ?a "firstName" "Wieland". ?a "lastName" "Hoffmann" };
         |DUMP B;
        """.stripMargin))
    val bgpfilter = plan.findOperatorForAlias("B").value
    bgpfilter.checkSchemaConformance shouldBe true
  }

  it should "conform to grouped RDF data" in {
    val possibleGroupers = Table(("grouping column"), ("subject"), ("predicate"), ("object"))
    val patterns = Table(
      "pattern",
      List(
        TriplePattern(PositionalField(0), PositionalField(1), PositionalField(2))),
      List(
        TriplePattern(PositionalField(0), PositionalField(1), PositionalField(2)),
        TriplePattern(PositionalField(0), PositionalField(1), PositionalField(2))),
      List(
        TriplePattern(PositionalField(0), PositionalField(1), NamedField("foo")),
        TriplePattern(PositionalField(0), NamedField("bar"), PositionalField(2))),
      List(
        TriplePattern(PositionalField(0), PositionalField(1), NamedField("foo")),
        TriplePattern(PositionalField(0), PositionalField(1), NamedField("foo")))
    )
    forAll (possibleGroupers) { (g: String) =>
      forAll(patterns) { p =>
        val op1 = RDFLoad(Pipe("a"), new URI("hdfs://somewhere"), Some(g))
        val op2 = BGPFilter(Pipe("b"), Pipe("a"), p)
        val op3 = Dump(Pipe("b"))
        val plan = new DataflowPlan(List(op1, op2, op3))

        op2.inputs.map(_.producer) should have size 1

        plan.findOperatorForAlias("b").value.checkSchemaConformance shouldBe true
      }
    }
  }

  "patternsToConstraint" should "return None if no column is bound by the pattern" in {
    val pattern = TriplePattern(PositionalField(0), PositionalField(1), PositionalField(2))
    RDF.patternToConstraint(pattern) shouldBe None
  }

  it should "return a single Eq constraint if only one column is bound" in {
    val patterns = Table(
      ("pattern", "constraint"),
      (TriplePattern(Value("subject"), PositionalField(1), PositionalField(2)),
        Some(Eq(RefExpr(NamedField("subject")), RefExpr(Value("subject"))))),
      (TriplePattern(PositionalField(0), Value("predicate"), PositionalField(2)),
        Some(Eq(RefExpr(NamedField("predicate")), RefExpr(Value("predicate"))))),
      (TriplePattern(PositionalField(0), PositionalField(1), Value("object")),
        Some(Eq(RefExpr(NamedField("object")), RefExpr(Value("object")))))
    )

    forAll(patterns) ((p, c) =>
      RDF.patternToConstraint(p) shouldBe c
    )
  }

  it should "return And'ed constraints if multiple columns are bound" in {
    val patterns = Table(
      ("pattern", "constraint"),
      (TriplePattern(Value("subject"), Value("predicate"), PositionalField(2)),
        Some(
          And(
            Eq(RefExpr(NamedField("subject")), RefExpr(Value("subject"))),
            Eq(RefExpr(NamedField("predicate")), RefExpr(Value("predicate")))))
          ),
      (TriplePattern(PositionalField(0), Value("predicate"), Value("object")),
        Some(
          And(
            Eq(RefExpr(NamedField("predicate")), RefExpr(Value("predicate"))),
            Eq(RefExpr(NamedField("object")), RefExpr(Value("object")))))
        ),
      (TriplePattern(Value("subject"), PositionalField(1), Value("object")),
        Some(
          And(
            Eq(RefExpr(NamedField("subject")), RefExpr(Value("subject"))),
            Eq(RefExpr(NamedField("object")), RefExpr(Value("object")))))
        ),
      (TriplePattern(Value("subject"), Value("predicate"), Value("object")),
        Some(
          And(
            And(
              Eq(RefExpr(NamedField("subject")), RefExpr(Value("subject"))),
              Eq(RefExpr(NamedField("predicate")), RefExpr(Value("predicate")))
            ),
            Eq(RefExpr(NamedField("object")), RefExpr(Value("object")))))
        )
    )

    forAll(patterns) ((p, c) =>
      RDF.patternToConstraint(p) shouldBe c
    )
  }

  private val pathJoins = Table(
    ("pattern"),
    List(
      TriplePattern(NamedField("s"), PositionalField(1), Value("obj1")),
      TriplePattern(PositionalField(0), NamedField("s"), Value("obj2"))),
    List(
      TriplePattern(PositionalField(0), PositionalField(1),NamedField("s")),
      TriplePattern(PositionalField(0), NamedField("s"), Value("obj2"))),
    List(
      TriplePattern(PositionalField(0), PositionalField(1),NamedField("s")),
      TriplePattern(NamedField("s"), PositionalField(1), Value("obj2")))
  )

  private val starJoins = Table(
    ("patterns"),
    List(
      TriplePattern(NamedField("s"), PositionalField(1), Value("obj1")),
      TriplePattern(NamedField("s"), PositionalField(1), Value("obj2"))),
    List(
      TriplePattern(PositionalField(0), NamedField("p"), Value("obj1")),
      TriplePattern(PositionalField(0), NamedField("p"), Value("obj2"))),
    List(
      TriplePattern(PositionalField(0), Value("pred1"), NamedField("o")),
      TriplePattern(PositionalField(0), Value("pred2"), NamedField("o")))
  )

  private val noJoins = Table(
    ("patterns"),
    // Only 1 pattern, can't be a join
    List(
      TriplePattern(NamedField("s"), PositionalField(1), Value("obj1"))),
    // 2 patterns where each one contains a different variable
    List(
      TriplePattern(PositionalField(0), NamedField("p"), Value("obj1")),
      TriplePattern(PositionalField(0), NamedField("r"), Value("obj2"))),
    // 2 patterns where each one contains a different variable in a different position
    List(
      TriplePattern(PositionalField(0), NamedField("p"), Value("obj1")),
      TriplePattern(PositionalField(0), PositionalField(1), NamedField("r"))),
    // No variables at all
    List(
      TriplePattern(PositionalField(0), Value("pred1"), Value("obj1")))
  )


  "isPathJoin" should "return true for path joins" in {
    forAll(pathJoins) { p =>
      RDF.isPathJoin(p) shouldBe true
    }
  }

  it should "return false for star joins" in {
    forAll(starJoins) { p =>
      RDF.isPathJoin(p) shouldBe false
    }
  }

  it should "return false for patterns that are no joins at all" in {
    forAll(noJoins) { p =>
      RDF.isPathJoin(p) shouldBe false
    }
  }

  "isStarJoin" should "return true for star joins" in {
    forAll(starJoins) { p =>
      RDF.isStarJoin(p) shouldBe true
    }
  }

  it should "return false for path joins" in {
    forAll(pathJoins) { p =>
      RDF.isStarJoin(p) shouldBe false
    }
  }

  it should "return false for patterns that are no joins at all" in {
    forAll(noJoins) { p =>
      RDF.isStarJoin(p) shouldBe false
    }
  }
}
