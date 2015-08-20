package nl.knaw.dans.easy.ingest_flow

import java.io.File

import nl.knaw.dans.easy.stage.EasyStageDataset
import org.slf4j.LoggerFactory

object EasyIngestFlow {
  val log = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]) {

    implicit val stageSettings = nl.knaw.dans.easy.stage.Settings(
      ownerId = "ownerId",
      bagStorageLocation = "http://localhost/bags",
      bagitDir = new File("test-resources/example-bag"),
      sdoSetDir = new File("out/sdoSetDir"),
      URN = "urn:nbn:nl:ui:13-1337-13",
      DOI = "10.1000/xyz123")

    for {
      _ <- EasyStageDataset.run()
    } yield log.info("hello, friend")

  }
}
