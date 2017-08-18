package SparkDemo

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object Banque {
  def main (arg:Array[String]): Unit = {

    val conf = new SparkConf().setAppName("Banque").setMaster("local[4]")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._

      def Client(dataPath1: String) = sqlContext.read.format("com.databricks.spark.csv")
        .option("header","true")
        .option("inferSchema", "true")
        .load(dataPath1)

      def Commercial(dataPath1: String) = sqlContext.read.format("com.databricks.spark.csv")
        .option("header","true")
        .option("inferSchema", "true")
        .load(dataPath1)

      def Compte(dataPath1: String) = sqlContext.read.format("com.databricks.spark.csv")
        .option("header","true")
        .option("inferSchema", "true")
        .load(dataPath1)

      def Portefeuille(dataPath1: String) = sqlContext.read.format("com.databricks.spark.csv")
        .option("header","true")
        .option("inferSchema", "true")
        .load(dataPath1)

    Client("Data/Entrer/jour1/client.csv").registerTempTable("client")
    val client = Client("Data/Entrer/jour1/client.csv")
    Commercial("Data/Entrer/jour1/commercial.csv").registerTempTable("commercial")
    Compte("Data/Entrer/jour1/compte.csv").registerTempTable("compte")
    Portefeuille("Data/Entrer/jour1/portefeuille.csv").registerTempTable("portefeuille")
    val portefeuille = Portefeuille("Data/Entrer/jour1/portefeuille.csv")

    //Nom et mail de tous les clients
    /*val clientNomMail = sqlContext.sql("SELECT nom, email FROM client")
    clientNomMail.show()
    client.select("nom","email").show()
    */

    /* Date d'attribution sans doublon5555555555
    val datetriSansDoublon = sqlContext.sql("SELECT DISTINCT date_attribution FROM portefeuille")
    datetriSansDoublon.show()
    portefeuille.select("date_attribution").distinct().show()
    */


    /* Longueur du email des clients (fonction chaine)
    val longEmail = sqlContext.sql("SELECT email, length(email) FROM client")
    longEmail.show()
    */

    /* Nom des clients suivi de => email
    val clientEtEmail = sqlContext.sql("SELECT concat_ws(' => ', nom, email) AS client FROM client")
    clientEtEmail.show()
    */

    /* -----------------------------------------------------------------
    Jointures :
    - naturelles
    - requetes imbriquees
    ----------------------------------------------------------------- */
    /* Comptes avec n°, nom du client et solde */

  }
  println(defPerson.funPersonne(9).age)

}
