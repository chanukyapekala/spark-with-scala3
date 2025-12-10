package preprocessing.processors

import preprocessing.models.*
import preprocessing.models.types.*
import cats.effect.IO
import fs2.{Stream, Pipe}
import io.circe.parser.*
import io.circe.syntax.*
import com.github.mjakubowski84.parquet4s.{ParquetWriter, ParquetReader}
import shared.schemas.PersonSchema
import org.apache.logging.log4j.LogManager

import java.nio.file.{Path, Paths}

// ============================================
// FEATURE #7: GIVEN/USING (Type Classes)
// ============================================

trait DataProcessor[A]:
  def parse(line: String): Validated[A]
  def validate(item: A): Validated[A]
  def transform(item: A): A

// ============================================
// FEATURE #8: GIVEN INSTANCES
// ============================================

object DataProcessor:
  private val logger = LogManager.getLogger(getClass)

  // Given instance for Person processing
  given personProcessor: DataProcessor[Person] with

    def parse(line: String): Validated[Person] =
      decode[Person](line).left.map(e => s"Parse error: ${e.getMessage}")

    def validate(person: Person): Validated[Person] =
      if person.name.trim.isEmpty then
        Left(s"Empty name for person ${person.id}")
      else if person.age.value < 0 then
        Left(s"Negative age for ${person.name}")
      else
        Right(person)

    def transform(person: Person): Person =
      person.copy(
        name = person.name.trim,
        city = person.city.normalize
      )

// ============================================
// FEATURE #9: CONTEXT FUNCTIONS (using clause)
// ============================================

class ProcessingPipeline[A](using processor: DataProcessor[A]):
  private val logger = LogManager.getLogger(getClass)

  def processLines(lines: Stream[IO, String]): Stream[IO, Either[String, A]] =
    lines
      .map { line =>
        processor.parse(line)
          .flatMap(processor.validate)
          .map(processor.transform)
      }

  def filterValid(results: Stream[IO, Either[String, A]]): Stream[IO, A] =
    results.collect { case Right(item) => item }

  def logErrors(results: Stream[IO, Either[String, A]]): Stream[IO, A] =
    results.evalTap {
      case Left(error) => IO(logger.error(s"Processing error: $error"))
      case Right(_) => IO.unit
    }.collect { case Right(item) => item }

case class ProcessingStats(
  total: Int,
  successful: Int,
  failed: Int,
  errors: Seq[String]
):
  def successRate: Double =
    if total == 0 then 0.0
    else successful.toDouble / total * 100

  override def toString: String =
    s"""Processing Statistics:
       |  Total Records: $total
       |  Successful: $successful (${successRate.formatted("%.2f")}%)
       |  Failed: $failed
       |  Sample Errors: ${errors.take(3).mkString(", ")}${if errors.size > 3 then "..." else ""}
       |""".stripMargin

// ============================================
// FEATURE #10: INLINE FUNCTIONS (Zero-cost)
// ============================================

inline def measure[A](name: String)(inline block: => A): A =
  val logger = LogManager.getLogger("performance")
  val start = System.nanoTime()
  val result = block
  val duration = (System.nanoTime() - start) / 1_000_000.0
  logger.info(s"$name completed in ${duration.formatted("%.2f")} ms")
  result

// ============================================
// FILE I/O OPERATIONS
// ============================================

class FileOperations:
  private val logger = LogManager.getLogger(getClass)

  def readJsonLines(path: Path): Stream[IO, String] =
    fs2.io.file.Files[IO]
      .readAll(fs2.io.file.Path.fromNioPath(path))
      .through(fs2.text.utf8.decode)
      .through(fs2.text.lines)
      .filter(_.trim.nonEmpty)

  def writeJsonLines(path: Path): Pipe[IO, String, Nothing] =
    lines =>
      lines
        .intersperse("\n")
        .through(fs2.text.utf8.encode)
        .through(
          fs2.io.file.Files[IO].writeAll(
            fs2.io.file.Path.fromNioPath(path)
          )
        )

  def writeParquet(path: Path, people: Seq[Person]): IO[Unit] =
    IO {
      logger.info(s"Writing ${people.size} records to Parquet: $path")
      val records = people.map(Person.toSchemaRecord)

      // Use parquet4s API correctly
      import com.github.mjakubowski84.parquet4s.{ParquetWriter, Path as PPath}
      val ppath = PPath(path.toString)
      ParquetWriter.of[PersonSchema.PersonRecord].writeAndClose(ppath, records)

      logger.info(s"Successfully wrote ${people.size} records")
    }

  def readParquet(path: Path): IO[Seq[Person]] =
    IO {
      logger.info(s"Reading Parquet file: $path")

      // Use parquet4s API correctly
      import com.github.mjakubowski84.parquet4s.{ParquetReader, Path as PPath}
      val ppath = PPath(path.toString)
      val records = ParquetReader.as[PersonSchema.PersonRecord].read(ppath).toSeq

      logger.info(s"Read ${records.size} records, converting to Person")
      val people = records.flatMap { record =>
        Person.fromSchemaRecord(record) match
          case Right(person) => Some(person)
          case Left(error) =>
            logger.error(s"Conversion error: $error")
            None
      }
      logger.info(s"Successfully converted ${people.size} records")
      people
    }

// ============================================
// MAIN PROCESSOR OBJECT
// ============================================

object Processor:
  private val logger = LogManager.getLogger(getClass)

  def processJsonToParquet[A](
    inputPath: Path,
    outputPath: Path
  )(using processor: DataProcessor[A]): IO[ProcessingStats] =

    val fileOps = FileOperations()
    val pipeline = ProcessingPipeline[A]

    measure("JSON to Parquet conversion") {
      fileOps.readJsonLines(inputPath)
        .through(pipeline.processLines)
        .compile
        .toList
        .flatMap { results =>
          val stats = ProcessingStats(
            total = results.size,
            successful = results.count(_.isRight),
            failed = results.count(_.isLeft),
            errors = results.collect { case Left(err) => err }
          )

          logger.info(s"Processing complete: ${stats.toString}")
          IO.pure(stats)
        }
    }

  def processJsonToParquetWithWrite(
    inputPath: Path,
    outputPath: Path
  )(using processor: DataProcessor[Person]): IO[ProcessingStats] =

    val fileOps = FileOperations()
    val pipeline = ProcessingPipeline[Person]

    measure("JSON to Parquet conversion with write") {
      fileOps.readJsonLines(inputPath)
        .through(pipeline.processLines)
        .through(pipeline.logErrors)
        .compile
        .toList
        .flatMap { people =>
          val stats = ProcessingStats(
            total = people.size,
            successful = people.size,
            failed = 0,
            errors = Seq.empty
          )

          fileOps.writeParquet(outputPath, people)
            .as(stats)
        }
    }