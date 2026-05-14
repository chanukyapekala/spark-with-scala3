package preprocessing.models

import java.time.Instant
import java.util.UUID
import io.circe.{Decoder, Encoder}
import io.circe.generic.semiauto.*
import shared.schemas.PersonSchema

// ============================================
// SCALA 3 LEARNING MODULE
// This file demonstrates key Scala 3 features
// ============================================

// ============================================
// FEATURE #1: ENUMS (Native to Scala 3)
// ============================================

enum PersonStatus:
  case Active
  case Inactive
  case Pending
  case Suspended(reason: String, until: Instant)

  def isAvailable: Boolean = this match
    case Active | Pending => true
    case _ => false

  def toStorageString: String = this match
    case Active => PersonSchema.Status.ACTIVE
    case Inactive => PersonSchema.Status.INACTIVE
    case Pending => PersonSchema.Status.PENDING
    case Suspended(reason, _) => s"${PersonSchema.Status.SUSPENDED}:$reason"

object PersonStatus:
  def fromString(s: String): Either[String, PersonStatus] = s match
    case PersonSchema.Status.ACTIVE => Right(Active)
    case PersonSchema.Status.INACTIVE => Right(Inactive)
    case PersonSchema.Status.PENDING => Right(Pending)
    case s if s.startsWith(PersonSchema.Status.SUSPENDED) =>
      val parts = s.split(":")
      if parts.length == 2 then
        Right(Suspended(parts(1), Instant.now().plusSeconds(86400 * 30)))
      else
        Left(s"Invalid suspended format: $s")
    case _ => Left(s"Unknown status: $s")

// ============================================
// FEATURE #2: UNION TYPES
// ============================================

type ValidationError = String
type Validated[A] = Either[ValidationError, A]

// ============================================
// FEATURE #3: OPAQUE TYPES (Zero-cost wrappers)
// ============================================

object types:

  opaque type Email = String
  object Email:
    def apply(value: String): Validated[Email] =
      val emailRegex = "^[A-Za-z0-9+_.-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$"
      if value.matches(emailRegex) then Right(value)
      else Left(s"Invalid email format: $value")

    extension (e: Email)
      def value: String = e
      def domain: String = e.split("@")(1)
      def username: String = e.split("@")(0)

  opaque type Age = Int
  object Age:
    def apply(value: Int): Validated[Age] =
      if value >= PersonSchema.Validation.MIN_AGE && value < PersonSchema.Validation.MAX_AGE then
        Right(value)
      else
        Left(s"Invalid age: $value (must be between ${PersonSchema.Validation.MIN_AGE} and ${PersonSchema.Validation.MAX_AGE})")

    extension (a: Age)
      def value: Int = a
      def isAdult: Boolean = a >= 18
      def isMinor: Boolean = !isAdult
      def category: String = a match
        case a if a < 13 => "child"
        case a if a < 20 => "teen"
        case a if a < 65 => "adult"
        case _ => "senior"

  opaque type City = String
  object City:
    def apply(value: String): Validated[City] =
      val trimmed = value.trim
      if trimmed.nonEmpty && trimmed.length <= 100 then Right(trimmed)
      else Left(s"Invalid city: '$value'")

    extension (c: City)
      def value: String = c
      def normalize: City = c.trim.toLowerCase.capitalize

// ============================================
// FEATURE #4: DERIVES CLAUSE (Auto-derivation)
// ============================================

case class Person(
  id: UUID,
  name: String,
  email: types.Email,
  age: types.Age,
  city: types.City,
  status: PersonStatus,
  createdAt: Instant
)

// Custom codecs for opaque types and enums
object Person:
  import types.*

  // JSON encoders/decoders
  given Encoder[Email] = Encoder.encodeString.contramap(_.value)
  given Decoder[Email] = Decoder.decodeString.emap(Email(_).left.map(_.toString))

  given Encoder[Age] = Encoder.encodeInt.contramap(_.value)
  given Decoder[Age] = Decoder.decodeInt.emap(Age(_).left.map(_.toString))

  given Encoder[City] = Encoder.encodeString.contramap(_.value)
  given Decoder[City] = Decoder.decodeString.emap(City(_).left.map(_.toString))

  given Encoder[PersonStatus] = Encoder.encodeString.contramap(_.toStorageString)
  given Decoder[PersonStatus] = Decoder.decodeString.emap(PersonStatus.fromString)

  given Encoder[Person] = deriveEncoder[Person]
  given Decoder[Person] = deriveDecoder[Person]

  // Smart constructor with validation
  def create(
    name: String,
    emailStr: String,
    ageInt: Int,
    cityStr: String
  ): Validated[Person] =
    for
      email <- Email(emailStr)
      age <- Age(ageInt)
      city <- City(cityStr)
    yield Person(
      id = UUID.randomUUID(),
      name = name.trim,
      email = email,
      age = age,
      city = city,
      status = PersonStatus.Pending,
      createdAt = Instant.now()
    )

  // Create from schema record
  def fromSchemaRecord(record: PersonSchema.PersonRecord): Validated[Person] =
    for
      id <- try Right(UUID.fromString(record.id))
            catch case _: IllegalArgumentException => Left(s"Invalid UUID: ${record.id}")
      email <- Email(record.email)
      age <- Age(record.age)
      city <- City(record.city)
      status <- PersonStatus.fromString(record.status)
      createdAt <- try Right(Instant.parse(record.createdAt))
                   catch case _: Exception => Left(s"Invalid timestamp: ${record.createdAt}")
    yield Person(id, record.name, email, age, city, status, createdAt)

  // Convert to schema record for storage
  def toSchemaRecord(person: Person): PersonSchema.PersonRecord =
    PersonSchema.PersonRecord(
      id = person.id.toString,
      name = person.name,
      email = person.email.value,
      age = person.age.value,
      city = person.city.value,
      status = person.status.toStorageString,
      createdAt = person.createdAt.toString
    )

// ============================================
// FEATURE #5: EXTENSION METHODS (Cleaner than implicits)
// ============================================

extension (p: Person)

  def isActive: Boolean = p.status.isAvailable

  def isAdult: Boolean = p.age.isAdult

  def withStatus(newStatus: PersonStatus): Person =
    p.copy(status = newStatus)

  def anonymize: Person =
    p.copy(
      name = s"User-${p.id.toString.take(8)}",
      email = types.Email("anonymous@example.com").getOrElse(p.email)
    )

  def toSummary: String =
    s"${p.name} (${p.age.value}yo, ${p.city.value}) - ${p.status}"

  def emailDomain: String = p.email.domain

  def ageCategory: String = p.age.category

// ============================================
// FEATURE #6: TOP-LEVEL DEFINITIONS
// ============================================

def categorize(person: Person): String = person match
  case p if p.age.isMinor => "Minor"
  case p if p.isAdult && p.status == PersonStatus.Active => "Active Adult"
  case Person(_, _, _, _, _, PersonStatus.Suspended(reason, _), _) =>
    s"Suspended: $reason"
  case _ => "Other"