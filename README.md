# Spark NullIntolerant Operators may cause schema mismatches in ABRiS

This repo is to demonstrate how [NullIntolerant](https://github.com/apache/spark/blob/branch-3.2/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/package.scala#L373)
expressions make a previously nullable expression non-nullable after query optimization. The nullability of the expression is only changed when the query is evaluated. This means that `printSchema`
can still show `nullable=true`, even if the column will later be non-nullable.

This is problematic for [ABRiS](https://github.com/AbsaOSS/ABRiS) because it relies on the [AvroSerializer](https://github.com/apache/spark/blob/branch-2.4/external/avro/src/main/scala/org/apache/spark/sql/avro/AvroSerializer.scala)
which relies on the nullability information, which is lazily evaluated, i.e. after query optimization.

Therefore, an innocent looking `===` fails the query in this [example](https://github.com/kevinwallimann/abris-nullability/blob/main/src/test/scala/com/github/kevinwallimann/MySparkTest.scala#L58):

```
      inputDf.filter(col("value1") === lit(42)) // causes IncompatibleSchemaException later on
```
because `===` extends the `NullIntolerant` trait and makes `value1` non-nullable after optimization. To retain the nullability, the `eqNullSafe` operator can be used.

- See the [Actions tab](https://github.com/kevinwallimann/abris-nullability/actions) for test output
- Note that this particular problem does not occur anymore in Spark 3.0, thanks to https://issues.apache.org/jira/browse/SPARK-27838)
