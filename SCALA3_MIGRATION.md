# Scala 3 with Spark: Understanding the Setup

This document explains why and how this project uses Scala 3 with Spark libraries compiled for Scala 2.13.

## TL;DR

- **Project Code**: Written in Scala 3.5.2
- **Spark Libraries**: Compiled for Scala 2.13 (`spark-sql_2.13`, `spark-core_2.13`)
- **Why**: Spark doesn't officially support Scala 3 yet, but Scala 3 is binary compatible with Scala 2.13 libraries

## Binary Compatibility

### What Does `_2.13` Mean?

In Maven/Ivy artifacts like `spark-sql_2.13`, the `_2.13` suffix indicates the Scala version used to compile the library:

```scala
// Scala 2.12 version
"org.apache.spark" % "spark-sql_2.12" % "3.5.3"

// Scala 2.13 version
"org.apache.spark" % "spark-sql_2.13" % "3.5.3"

// Scala 3 version (doesn't exist for Spark yet!)
"org.apache.spark" % "spark-sql_3" % "3.5.3"  // ❌ Not available
```

### Why Scala 3 Can Use Scala 2.13 Libraries

Scala 3 maintains **forward binary compatibility** with Scala 2.13:

✅ **What Works:**
- Scala 3 code can use Scala 2.13 compiled libraries
- Standard library features from Scala 2.13
- Java interop
- Case classes, pattern matching, collections

❌ **What Doesn't Work:**
- Scala 2.13 macros (rewritten in Scala 3)
- Scala 2 reflection APIs (replaced by metaprogramming in Scala 3)
- Some implicit resolution edge cases

## Key Differences in This Project

### 1. **Typed Datasets Don't Work**

**Scala 2.13 with Spark:**
```scala
// This works in Scala 2.13
case class Person(name: String, age: Int, city: String)

val people: Dataset[Person] = spark.read
  .option("header", "true")
  .option("inferSchema", "true")
  .csv("data.csv")
  .as[Person]  // ✅ Works with Scala 2.13
```

**Why it fails in Scala 3:**
- Spark's Dataset API relies on `scala.reflect.runtime.universe.TypeTag`
- TypeTag is part of Scala 2's reflection system
- Scala 3 replaced this with a new metaprogramming system
- Spark hasn't been updated to use Scala 3's metaprogramming

**Our Solution:**
```scala
// Use DataFrame with explicit schema
case class Person(name: String, age: Int, city: String)

object Person {
  val schema: StructType = StructType(Array(
    StructField("name", StringType, nullable = false),
    StructField("age", IntegerType, nullable = false),
    StructField("city", StringType, nullable = false)
  ))

  def fromRow(row: Row): Person =
    Person(row.getString(0), row.getInt(1), row.getString(2))
}

val peopleDF: DataFrame = spark.read
  .schema(Person.schema)  // Explicit schema instead of inference
  .csv("data.csv")

// Convert to typed collection when needed
val people: Seq[Person] = peopleDF.collect().map(Person.fromRow).toSeq
```

### 2. **Syntax Improvements in Scala 3**

Our project uses several Scala 3 features:

**Cleaner Syntax:**
```scala
// Scala 2.13
def fromRow(row: Row): Person = {
  Person(row.getString(0), row.getInt(1), row.getString(2))
}

// Scala 3 - Optional braces
def fromRow(row: Row): Person =
  Person(row.getString(0), row.getInt(1), row.getString(2))
```

**Improved Type Inference:**
```scala
// Scala 3 infers types better
val people = peopleDF.collect().map(Person.fromRow).toSeq
// No need to explicitly specify types as often
```

**Quieter Syntax:**
```scala
// Both work, but Scala 3 doesn't require parentheses for parameterless methods
spark.implicits._   // Scala 2.13 and 3
```

### 3. **Implicits → Given/Using**

While this project uses Scala 2.13-style implicits (for Spark compatibility), Scala 3 introduces `given`/`using`:

**Scala 2.13 Style (what we use):**
```scala
import spark.implicits._
```

**Scala 3 Style (not yet compatible with Spark):**
```scala
// Scala 3's new syntax (would be used when Spark supports Scala 3)
given SparkSession = spark
```

### 4. **Pattern Matching Enhancements**

Scala 3 offers better pattern matching, but we stick to Scala 2.13 compatible patterns:

```scala
// Works in both
people.foreach { p =>
  println(s"${p.name} is ${p.age} years old")
}

// Scala 3 exclusive features (avoided for now)
// - Pattern matching on types without parentheses
// - More powerful match types
```

## Performance Considerations

### No Performance Difference
- Compiled bytecode is nearly identical
- Spark execution is the same
- JVM runtime behavior is identical

### Compile Time
- Scala 3 compiler is generally faster
- Better error messages
- Improved incremental compilation

## Migration Gotchas

### What to Watch Out For

1. **Encoder Issues**
   - Always use explicit schemas with DataFrame
   - Convert to case classes manually when type safety is needed
   - Don't rely on `as[CaseClass]` conversions

2. **Implicit Imports**
   - `import spark.implicits._` still required for `$"column"` syntax
   - Some implicit conversions may behave differently

3. **Reflection-Based Features**
   - Avoid Spark features that rely on runtime reflection
   - Stick to schema-based operations

## When Will Spark Support Scala 3 Natively?

**Current Status (as of Spark 3.5.3):**
- ❌ No official Scala 3 support
- ⏳ Scala 3 support is being discussed in SPARK-32981
- 🔮 Likely in Spark 4.x or later

**Why It Takes Time:**
- Spark's codebase is large and complex
- Heavy reliance on Scala 2 macros and reflection
- Needs rewrite of encoder generation
- Breaking change for ecosystem

## Recommendations

### ✅ Use Scala 3 When:
- You want modern language features
- Better type inference is important
- You prefer cleaner syntax
- You don't need Spark's typed Dataset API

### ⚠️ Consider Scala 2.13 When:
- You heavily rely on typed Datasets
- You need full Spark API compatibility
- You're using other libraries without Scala 3 support
- You want to avoid any compatibility risks

## Our Approach: Best of Both Worlds

This project demonstrates the **pragmatic middle ground**:

1. ✅ **Write code in Scala 3** - modern syntax, better tooling
2. ✅ **Use DataFrames with explicit schemas** - type-safe, compatible
3. ✅ **Convert to case classes when needed** - type safety where it matters
4. ✅ **Avoid reflection-based features** - stick to schema-based operations

## Example: Full Comparison

### Scala 2.13 + Spark (Fully Native)
```scala
case class Person(name: String, age: Int, city: String)

val people: Dataset[Person] = spark.read
  .option("header", "true")
  .option("inferSchema", "true")
  .csv("data.csv")
  .as[Person]

people.filter(_.age >= 18).show()
```

### Scala 3 + Spark (Our Approach)
```scala
case class Person(name: String, age: Int, city: String)

object Person:
  val schema: StructType = StructType(Array(
    StructField("name", StringType, nullable = false),
    StructField("age", IntegerType, nullable = false),
    StructField("city", StringType, nullable = false)
  ))

  def fromRow(row: Row): Person =
    Person(row.getString(0), row.getInt(1), row.getString(2))

val people: DataFrame = spark.read
  .schema(Person.schema)
  .csv("data.csv")

people.filter($"age" >= 18).show()

// Type-safe operations when needed
val typedPeople: Seq[Person] = people.collect().map(Person.fromRow).toSeq
typedPeople.filter(_.age >= 18).foreach(println)
```

## References

- [Scala 3 Migration Guide](https://docs.scala-lang.org/scala3/guides/migration/compatibility-intro.html)
- [Spark Scala 3 Support Tracking](https://issues.apache.org/jira/browse/SPARK-32981)
- [Scala 3 Binary Compatibility](https://docs.scala-lang.org/scala3/guides/migration/compatibility-classpath.html)

## Conclusion

Using Scala 3 with Spark is **viable and practical**, but requires understanding the limitations:

- ✅ Modern language features
- ✅ Better developer experience
- ✅ Binary compatibility with Scala 2.13 libraries
- ⚠️ No typed Datasets (use DataFrames with explicit schemas)
- ⚠️ Avoid reflection-based APIs

This project shows how to bridge the gap until Spark provides native Scala 3 support.