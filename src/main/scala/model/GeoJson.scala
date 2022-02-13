package model

import doobie.{Read, Write}

case class Properties(id: Option[String],
                      hash: Option[String],
                      number: Option[String],
                      street: Option[String],
                      unit: Option[String],
                      city: Option[String],
                      district: Option[String],
                      region: Option[String],
                      postcode: Option[String])

//object Properties {
//
//  // These are not working as expected - wip
//  implicit val propertiesDbRead: Read[Properties] =
//    Read[
//      (Option[String],
//       Option[String],
//       Option[String],
//       Option[String],
//       Option[String],
//       Option[String],
//       Option[String],
//       Option[String],
//       Option[String])
//    ].map {
//      case (id, hash, number, street, unit, city, district, region, postcode) =>
//        Properties(
//          id = id,
//          hash = hash,
//          number = number,
//          street = street,
//          unit = unit,
//          city = city,
//          district = district,
//          region = region,
//          postcode = postcode
//        )
//    }
//
//  implicit val propertiesDbWrite: Write[Properties] =
//    Write[
//      (Option[String],
//       Option[String],
//       Option[String],
//       Option[String],
//       Option[String],
//       Option[String],
//       Option[String],
//       Option[String],
//       Option[String])
//    ].contramap { p =>
//      (
//        p.id,
//        p.hash,
//        p.number,
//        p.street,
//        p.unit,
//        p.city,
//        p.district,
//        p.region,
//        p.postcode
//      )
//    }
//}

case class SqlProperties(id: String,
                         hash: String,
                         number: String,
                         street: String,
                         unit: String,
                         city: String,
                         district: String,
                         region: String,
                         postcode: String)
object SqlProperties {
  def apply(p: Properties): SqlProperties = {
    new SqlProperties(
      p.id.map(v => s"'${escape(v)}'").orNull,
      p.hash.map(v => s"'${escape(v)}'").orNull,
      p.number.map(v => s"'${escape(v)}'").orNull,
      p.street.map(v => s"'${escape(v)}'").orNull,
      p.unit.map(v => s"'${escape(v)}'").orNull,
      p.city.map(v => s"'${escape(v)}'").orNull,
      p.district.map(v => s"'${escape(v)}'").orNull,
      p.region.map(v => s"'${escape(v)}'").orNull,
      p.postcode.map(v => s"'${escape(v)}'").orNull
    )
  }

  private def escape(s: String): String = s.replaceAll("'", "''")
}

case class GeoJson(`type`: String, properties: Properties)
