object student {
  def main(args: Array[String]): Unit = {
    // read TextFile(.csv)
    val df = spark.read.format("csv").option("header","true").option("sep", ";").load("/home/hadoop/student/student-por.csv").rdd
    // RDD[row] to RDD[String]
    val rddS = df.map(row => {
      row.mkString(",")
    })
    // RDD[String] to RDD[ArrayBuffer]
    val rddA = rddS.map(line => {
      val arr = line.split(",")
      val buf = collection.mutable.ArrayBuffer(arr: _*)
      buf
    })

    // transformer(String -> Boolean)
    def strToBool(str:String):Boolean = {
      if(str == "yes"){
        true
      }else{
        false
      }
    }

    //Compare Mean Final Grade of Two Schools
    val averSch = rddA.map(line => {
      val g1 = line(30).toDouble
      val g2 = line(31).toDouble
      val g3 = line(32).toDouble
      val a = (g1 + g2 + g3) / 3
      (line(0), (a , 1))
    }).reduceByKey((x,y) => {
      (x._1 + y._1, x._2 + y._2)
    }).map(t => {
      (t._1, t._2._1 / t._2._2)
    }).collect().foreach(println)

    //Compare the Final Grade of Male Versus Female
    val averGen = rddA.map(line => {
      val g1 = line(30).toDouble
      val g2 = line(31).toDouble
      val g3 = line(32).toDouble
      val a = (g1 + g2 + g3) / 3
      (line(1), (a , 1))
    }).reduceByKey((x,y) => {
      (x._1 + y._1, x._2 + y._2)
    }).map(t => {
      (t._1, t._2._1 / t._2._2)
    })
    averGen.collect().foreach(println)


    /*
    Add the Following Extra Features:
    33 allSup: schoolsup & famsup
    34 pairEdu: Medu & Fedu
    35 moreHigh: higher & (schoolsup | paid)
    36 allAlc: Walc + Dalc 
    37 dalcPerWeek: Dalc / (Walc + Dalc)
    38 studytimeRatio: studytime / (traveltime + studytime + freetime)

    Drop the Following Previous Features
    14 studytime
    27 Dalc
    */

    //Create New DataSet by Spark
    val rr = rddA.map(line => {
      val allSup = strToBool(line(15)) && strToBool(line(16)) // allSup = schoolsup & famsup
      val pairEdu = strToBool(line(6)) && strToBool(line(7)) // pairEdu = Medu & Fedu
      val moreHigh = strToBool(line(20)) && (strToBool(line(15)) || strToBool(line(17))) // moreHigh = higher & (schoolsup | paid)
      val allAlc = line(26).toInt + line(27).toInt // allAlc = Dalc + Walc 
      val dalcPerWeek = line(26).toDouble / allAlc // dalcPerWeek = Dalc / allAlc
      val studytimeRatio = line(13).toDouble / (line(12).toInt + line(13).toInt + line(24).toInt) // studytimeRatio = studytime / (traveltime + studytime + freetime)
      val allSup1 = String.valueOf(allSup)
      val pairEdu1 = String.valueOf(pairEdu)
      val moreHigh1 = String.valueOf(moreHigh)
      val allAlc1 = String.valueOf(allAlc)
      val dalcPerWeek1 = String.valueOf(dalcPerWeek)
      val studytimeRatio1 = String.valueOf(studytimeRatio)
      line ++= Array(allSup1, pairEdu1, moreHigh1, allAlc1, dalcPerWeek1, studytimeRatio1)
      line.remove(26) // drop Dalc
      line.remove(13) // drop studytime
      line.mkString(",")
    }).coalesce(1,true).saveAsTextFile("/home/hadoop/student/output")

    println("Analysis complete")
  }



    