package shared.schemas

/**
 * Shared schema definitions (Scala 2.13)
 * These can be used by both preprocessing and ETL modules
 *
 * Note: This is Scala 2.13 code, so both modules can read it:
 * - Scala 3 can read Scala 2.13 bytecode ✓
 * - Scala 2.13 can read Scala 2.13 bytecode ✓
 */
object I PersonSchema {

  /**
   * Field names as constants
   * Prevents typos and makes refactoring easier
   */
  object Fields {
    val ID = "id"
    val NAME = "name"
    val EMAIL = "email"
    val AGE = "age"
    val CITY = "city"
    val STATUS = "status"
    val CREATED_AT = "createdAt"
  }

  /**
   * Simple case class for Person (Scala 2.13 compatible)
   * Used as a reference schema - not for runtime type conversion
   */
  case class PersonRecord(
    id: String,
    name: String,
    email: String,
    age: Int,
    city: String,
    status: String,
    createdAt: String
  )

  /**
   * Status values
   */
  object Status {
    val ACTIVE = "Active"
    val INACTIVE = "Inactive"
    val PENDING = "Pending"
    val SUSPENDED = "Suspended"
  }

  /**
   * Validation constants
   */
  object Validation {
    val MIN_AGE = 0
    val MAX_AGE = 150
    val MIN_NAME_LENGTH = 1
    val MAX_NAME_LENGTH = 100
  }
}