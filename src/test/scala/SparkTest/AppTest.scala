package SparkTest

import org.junit._
import Assert._
import SparkDemo._

@Test
class AppTest {

  @Test
  def SiLafonctionPrincipaleOk(): Unit = {
    assertEquals(defPerson.moi.nom,"Lutula")
    assertEquals(defPerson.moi.prenom,"okito")
    assertEquals(defPerson.moi.age,32)



  }



}


