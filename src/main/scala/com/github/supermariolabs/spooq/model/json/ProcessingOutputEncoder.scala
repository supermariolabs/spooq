package com.github.supermariolabs.spooq.model.json

import com.github.supermariolabs.spooq.model.ProcessingOutput
import io.circe.{Encoder, Json}

class ProcessingOutputEncoder extends Encoder[ProcessingOutput] {
    final def apply(po: ProcessingOutput): Json = Json.obj(
      ("step", Json.obj(
        ("id",Json.fromString(po.step.id)),
      )),
      ("exit", Json.fromBoolean(po.exit)),
    )
}
