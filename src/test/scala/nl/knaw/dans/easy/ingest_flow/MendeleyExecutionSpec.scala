package nl.knaw.dans.easy.ingest_flow

import org.scalatest.{Matchers, FlatSpec}

import scala.util.{Failure, Success}

import MendeleyExecution._

/**
  * NOTE: the XML documents are all INVALID DDM-documents for other reasons than the focus of the test. Since the
  * functions tested do not perform an overall validation of the document, this is not a problem. On the other hand,
  * valid documents would take up too much space and obscure the meaning of these very limited tests.
  */
class MendeleyExecutionSpec extends FlatSpec with Matchers {

  "getDoiFromDdm" should "succeed if there is one DOI-identifier with correct type-attribute present" in {
    getDoiFromDdm(
      <ddm:DDM xmlns:dcterms="http://purl.org/dc/terms/"
               xmlns:id-type="http://easy.dans.knaw.nl/schemas/vocab/identifier-type/"
               xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
        <ddm:dcmiMetadata>
          <dcterms:identifier xsi:type="id-type:DOI">THE-VALID-DOI</dcterms:identifier>
          <dcterms:identifier>SOME-OTHER-ID</dcterms:identifier>
        </ddm:dcmiMetadata>
      </ddm:DDM>) shouldBe Success("THE-VALID-DOI")
  }

  it should "fail if type attribute is not from Schema-Instance namespace" in {
    getDoiFromDdm(
      <ddm:DDM xmlns:dcterms="http://purl.org/dc/terms/"
               xmlns:id-type="http://easy.dans.knaw.nl/schemas/vocab/identifier-type/"
               xmlns:xsi="**NOT** http://www.w3.org/2001/XMLSchema-instance">
        <ddm:dcmiMetadata>
          <dcterms:identifier xsi:type="id-type:DOI">THE-VALID-DOI</dcterms:identifier>
        </ddm:dcmiMetadata>
      </ddm:DDM>) shouldBe a[Failure[_]]
  }

  it should "fail if type attribute value is not from id-type namespace" in {
    getDoiFromDdm(
      <ddm:DDM xmlns:dcterms="http://purl.org/dc/terms/"
               xmlns:id-type="*** NOT *** http://easy.dans.knaw.nl/schemas/vocab/identifier-type/"
               xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
        <ddm:dcmiMetadata>
          <dcterms:identifier xsi:type="id-type:DOI">THE-VALID-DOI</dcterms:identifier>
        </ddm:dcmiMetadata>
      </ddm:DDM>) shouldBe a[Failure[_]]
  }


  it should "fail if there are multiple DOI-identifiers with correct type-attribute present" in {
    getDoiFromDdm(
      <ddm:DDM xmlns:dcterms="http://purl.org/dc/terms/"
               xmlns:id-type="http://easy.dans.knaw.nl/schemas/vocab/identifier-type/"
               xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
        <ddm:dcmiMetadata>
          <dcterms:identifier xsi:type="id-type:DOI">THE-VALID-DOI</dcterms:identifier>
          <dcterms:identifier xsi:type="id-type:DOI">THE-OTHER-VALID-DOI</dcterms:identifier>
          <dcterms:identifier>SOME-OTHER-ID</dcterms:identifier>
        </ddm:dcmiMetadata>
      </ddm:DDM>) shouldBe a[Failure[_]]
  }

  "assertNoAccessSet" should "succeed if one accessRights element present, set to NO_ACCESS" in {
    assertNoAccessSet(
      <ddm:DDM>
        <ddm:profile>
          <ddm:accessRights>NO_ACCESS</ddm:accessRights>
        </ddm:profile>
      </ddm:DDM>) shouldBe a[Success[_]]
  }

  it should "fail if no accessRights present" in {
    assertNoAccessSet(
      <ddm:DDM>
        <ddm:profile>
          <dc:title>NO ACCESSRIGHTS SET</dc:title>
        </ddm:profile>
        <ddm:dcmiMetadata />
      </ddm:DDM>) shouldBe a[Failure[_]]
  }

  it should "fail if multiple accessRights present" in {
    assertNoAccessSet(
      <ddm:DDM>
        <ddm:profile>
          <ddm:accessRights>NO_ACCESS</ddm:accessRights>
          <ddm:accessRights>OPEN_ACCESS</ddm:accessRights>
        </ddm:profile>
        <ddm:dcmiMetadata />
      </ddm:DDM>) shouldBe a[Failure[_]]
  }
}
