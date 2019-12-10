package io.kafka4s.effect.config

import scala.util.control.NoStackTrace

final case class GetterException(key: String, ex: Throwable)
    extends Throwable(s"""Error parsing "$key": ${ex.getMessage}""")
    with NoStackTrace
