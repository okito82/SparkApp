package SparkDemo


object defPerson {

  class Personne(val nom: String, val prenom: String, val age: Int)
  val moi = new Personne("Lutula", "okito", 32)
  def funPersonne(age2: Int): Personne = {
    val age3: Int =  1 + age2
    val personne2 = new Personne("lutula","sarah",age3)
    return personne2
  }

}






