package com.randomstatistic.alo

import java.util.UUID
import java.util.regex.Pattern

case class MaskedUUIDGenerator(maskLen: Int) {
  require(maskLen <= 3) // let's not get out of control here, 3 means 3360 masks

  type IdGenerator = () => UUID

  def maskedIdGenerator(mask: String): IdGenerator = {
    require(mask.forall(c => c.toString.matches("[a-fA-F0-9]")))
    require(mask.length == maskLen)
    val re = "^" + "." * mask.length
    val reC = Pattern.compile(re)

    () => {
      val matcher = reC.matcher(UUID.randomUUID().toString)
      val masked = matcher.replaceFirst(mask)
      UUID.fromString(masked)
    }
  }

  def getMask(uuid: UUID) = {
    uuid.toString.substring(0, maskLen)
  }

  val masks: List[String] = {
    val charRange = (('0' to '9') ++ ('a' to 'f')).map(_.toString).toList
    val combinations =
      charRange.combinations(maskLen).map(_.permutations).flatten.map(_.mkString)
    combinations.toList
  }

  val masksItr = masks.toIterator

  val generators = {
    masksItr.map( mask => (mask, maskedIdGenerator(mask)) )
  }

}

