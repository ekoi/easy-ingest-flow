package nl.knaw.dans.easy.ingest_flow

import java.io.File

import nl.knaw.dans.easy.ingest_flow.EasyIngestFlow.Settings
import org.apache.commons.configuration.PropertiesConfiguration

import scala.util.Try

object DepositState {
  case class State(state: String, description: String, timeStamp: String)

  def setDepositState(state: String, description: String)(implicit s: Settings): Try[Unit] = Try {
    val stateFile = new PropertiesConfiguration(new File(s.depositDir, "deposit.properties"))
    stateFile.setProperty("state.label", state)
    stateFile.setProperty("state.description", description)
    stateFile.save()
  }
}
