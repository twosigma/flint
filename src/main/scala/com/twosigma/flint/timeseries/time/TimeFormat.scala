/*
 *  Copyright 2017-2018 TWO SIGMA OPEN SOURCE, LLC
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.twosigma.flint.timeseries.time

import java.util.concurrent.TimeUnit

import org.joda.time.format.{ DateTimeFormat, DateTimeFormatter, ISODateTimeFormat }
import org.joda.time.{ DateTime, DateTimeZone }

import scala.concurrent.duration.TimeUnit
import scala.util.Try

object TimeFormat {

  /**
   * Parses a date time from the given text.
   * <p>
   * It will try all possible known formatters with the given time zone (default UTC) as the default time Zone and
   * ISO chronology to parse, respectively. It will return the first parsable date time.
   * <p>
   * If the text contains a time zone string then that will be taken into account. However, the underneath MILLISECONDS
   * from 1970-01-01T00:00:00Z won't be changed.
   *
   * @param text the text to parse, not null
   * @return a parsed [[org.joda.time.DateTime]].
   */
  @throws(classOf[IllegalArgumentException])
  protected[flint] def parseDateTime(text: String, timeZone: DateTimeZone = DateTimeZone.UTC): DateTime = {
    val parsedOption = formatters.view.flatMap { formatter =>
      Try(formatter.withZone(timeZone).parseDateTime(text.trim)).toOption
    }.headOption

    parsedOption.getOrElse(
      throw new IllegalArgumentException(s"Can't parse the given text $text as date time.")
    )
  }

  /**
   * Parses a date-time from the given text and returning it in terms of `timeUnit` since the epoch,
   * 1970-01-01T00:00:00Z.
   * <p>
   * It will try all possible known formatters with the given time zone (default: UTC) and
   * ISO chronology to parse, respectively. It will return the first parsable one.
   * <p>
   * If the text contains a time zone string then that will be taken into account. However, the underneath MILLISECONDS
   * from 1970-01-01T00:00:00Z won't be changed.
   *
   * @param text the text to parse, not null
   * @return an parsed NANOSECONDS since the epoch 1970-01-01T00:00:00Z.
   */
  protected[flint] def parse(
    text: String,
    timeZone: DateTimeZone = DateTimeZone.UTC,
    timeUnit: TimeUnit = TimeUnit.NANOSECONDS
  ): Long =
    timeUnit.convert(parseDateTime(text, timeZone).getMillis, TimeUnit.MILLISECONDS)

  /**
   * Parses a date-time from the given text and returning the number of NANOSECONDS since the epoch,
   * 1970-01-01T00:00:00Z.
   *
   * @see [[parse(String, DateTimeZone, TimeUnit)]] for parsing rules.
   *
   * @param text the text to parse, not null
   * @return an parsed NANOSECONDS since the epoch 1970-01-01T00:00:00Z.
   */
  protected[flint] def parseNano(text: String, timeZone: DateTimeZone = DateTimeZone.UTC): Long =
    parse(text, timeZone, timeUnit = TimeUnit.NANOSECONDS)

  private val formatters: List[DateTimeFormatter] = List(
    // Double `HH` formatter
    DateTimeFormat.forPattern("yyyyMMdd HH:mm:ss.SSS Z"),
    DateTimeFormat.forPattern("yyyyMMdd HH:mm:ss Z"),
    DateTimeFormat.forPattern("yyyyMMdd HH:mm Z"),
    DateTimeFormat.forPattern("yyyyMMdd HH:mm:ss.SSS"),
    DateTimeFormat.forPattern("yyyyMMdd HH:mm:ss"),
    DateTimeFormat.forPattern("yyyyMMdd HH:mm"),
    DateTimeFormat.forPattern("yyyyMMdd"),
    DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSS Z"),
    DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss Z"),
    DateTimeFormat.forPattern("yyyy-MM-dd HH:mm Z"),
    DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSS"),
    DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss"),
    DateTimeFormat.forPattern("yyyy-MM-dd HH:mm"),
    DateTimeFormat.forPattern("yyyy-MM-dd"),
    // Single `H` formatter
    DateTimeFormat.forPattern("yyyyMMdd H:mm:ss.SSS"),
    DateTimeFormat.forPattern("yyyyMMdd H:mm:ss.SSS Z"),
    DateTimeFormat.forPattern("yyyy-MM-dd H:mm:ss.SSS"),
    DateTimeFormat.forPattern("yyyy-MM-dd H:mm:ss.SSS Z"),

    // ISO DateTime
    ISODateTimeFormat.dateTimeParser()
  )
}
