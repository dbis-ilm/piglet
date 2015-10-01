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
package dbis.test.pig

import java.net.URI

import dbis.pig.op._
import dbis.pig.PigCompiler._
import dbis.pig.parser.LanguageFeature
import dbis.pig.plan.DataflowPlan
import dbis.pig.plan.rewriting.internals.RDF
import dbis.pig.schema._
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
       """.stripMargin, LanguageFeature.SparqlPig))
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
       """.stripMargin, LanguageFeature.SparqlPig))
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
      """.stripMargin, LanguageFeature.SparqlPig) )
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
        """.stripMargin, LanguageFeature.SparqlPig) )
      val bgpfilter = plan.findOperatorForAlias("B").value
      bgpfilter.checkSchemaConformance shouldBe false
      }
  }

  it should "conform to plain RDF data" in {
    val plan = new DataflowPlan(parseScript(
      s"""A = Load 'file' as (subject: chararray, predicate: chararray, object: chararray);
         |B = BGP_FILTER A by { ?a "firstName" "Wieland" };
         |DUMP B;
        """.stripMargin, LanguageFeature.SparqlPig))
    val bgpfilter = plan.findOperatorForAlias("B").value
    bgpfilter.checkSchemaConformance shouldBe true
  }

  it should "conform to grouped RDF data" in {
    val possibleGroupers = Table(("grouping column"), ("subject"), ("predicate"), ("object"))
    forAll (possibleGroupers) { (g: String) =>
      val op1 = RDFLoad(Pipe("a"), new URI("hdfs://somewhere"), Some(g))
      val op2 = BGPFilter(Pipe("b"), Pipe("a"), List.empty)
      val op3 = Dump(Pipe("b"))
      val plan = new DataflowPlan(List(op1, op2, op3))

      op2.inputs.map(_.producer) should have size 1

      plan.findOperatorForAlias("b").value.checkSchemaConformance shouldBe true
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
}
