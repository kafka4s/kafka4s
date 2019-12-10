package io.kafka4s.effect.admin

import cats.effect.Sync
import org.apache.kafka.clients.admin.AdminClient

class AdminEffect[F[_]] private (admin: AdminClient)(implicit F: Sync[F]) {}
