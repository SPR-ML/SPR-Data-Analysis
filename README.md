# SPR-DataAnalysis

## 1. Overview

```scala
/*
By: AlvaYoung

Envirornment:
    Jdk: 1.8.0.241
    Hadoop: 2.7.7
    Scala: 2.11.11
    Spark: 2.4.5

Data Source: 
1. https://raw.githubusercontent.com/arunk13/MSDA-Assignments/master/IS607Fall2015/Assignment3/student-por.csv
2. https://raw.githubusercontent.com/arunk13/MSDA-Assignments/master/IS607Fall2015/Assignment3/student-mat.csv

Data Attributes(variables : 33):
Attributes for both student-mat.csv (Math course) and student-por.csv (Portuguese language course) datasets:

1 school - student’s school (binary: ‘GP’ - Gabriel Pereira or ‘MS’ - Mousinho da Silveira)
2 sex - student’s sex (binary: ‘F’ - female or ‘M’ - male)
3 age - student’s age (numeric: from 15 to 22)
4 address - student’s home address type (binary: ‘U’ - urban or ‘R’ - rural)
5 famsize - family size (binary: ‘LE3’ - less or equal to 3 or ‘GT3’ - greater than 3)
6 Pstatus - parent’s cohabitation status (binary: ‘T’ - living together or ‘A’ - apart)
7 Medu - mother’s education (numeric: 0 - none, 1 - primary education (4th grade), 2 - “ 5th to 9th grade, 3 - “ secondary education or 4 - “ higher education)
8 Fedu - father’s education (numeric: 0 - none, 1 - primary education (4th grade), 2 - “ 5th to 9th grade, 3 - “ secondary education or 4 - “ higher education)
9 Mjob - mother’s job (nominal: ‘teacher’, ‘health’ care related, civil ‘services’ (e.g. administrative or police), ‘at_home’ or ‘other’)
10 Fjob - father’s job (nominal: ‘teacher’, ‘health’ care related, civil ‘services’ (e.g. administrative or police), ‘at_home’ or ‘other’)
11 reason - reason to choose this school (nominal: close to ‘home’, school ‘reputation’, ‘course’ preference or ‘other’)
12 guardian - student’s guardian (nominal: ‘mother’, ‘father’ or ‘other’)
13 traveltime - home to school travel time (numeric: 1 - <15 min., 2 - 15 to 30 min., 3 - 30 min. to 1 hour, or 4 - >1 hour)
14 studytime - weekly study time (numeric: 1 - <2 hours, 2 - 2 to 5 hours, 3 - 5 to 10 hours, or 4 - >10 hours)
15 failures - number of past class failures (numeric: n if 1<=n<3, else 4)
16 schoolsup - extra educational support (binary: yes or no)
17 famsup - family educational support (binary: yes or no)
18 paid - extra paid classes within the course subject (Math or Portuguese) (binary: yes or no)
19 activities - extra-curricular activities (binary: yes or no)
20 nursery - attended nursery school (binary: yes or no)
21 higher - wants to take higher education (binary: yes or no)
22 internet - Internet access at home (binary: yes or no)
23 romantic - with a romantic relationship (binary: yes or no)
24 famrel - quality of family relationships (numeric: from 1 - very bad to 5 - excellent)
25 freetime - free time after school (numeric: from 1 - very low to 5 - very high)
26 goout - going out with friends (numeric: from 1 - very low to 5 - very high)
27 Dalc - workday alcohol consumption (numeric: from 1 - very low to 5 - very high)
28 Walc - weekend alcohol consumption (numeric: from 1 - very low to 5 - very high)
29 health - current health status (numeric: from 1 - very bad to 5 - very good)
30 absences - number of school absences (numeric: from 0 to 93)

These grades are related with the course subject, Math or Portuguese:
31 G1 - first period grade (numeric: from 0 to 20)
31 G2 - second period grade (numeric: from 0 to 20)
32 G3 - final grade (numeric: from 0 to 20, output target)

*/

```

## 2. Load Data File From HDFS

```scala
//Read TextFile(.csv)
    val df = spark.read.format("csv").option("header","true").option("sep", ";").load("hdfs://test:9000/user/hadoop/student.csv").rdd

//Get Data Count
	df.count()

/*
Long = 649
*/
```



## 3. Transformation for RDD Type

```scala
//RDD[row] to RDD[String]
    val rddS = df.map(row => {
      row.mkString(",")
    })

//RDD[String] to RDD[ArrayBuffer]
    val rddA = rddS.map(line => {
      val arr = line.split(",")
      val buf = collection.mutable.ArrayBuffer(arr: _*)
      buf
    })
```



## 4. Compare Mean Final Grade of Two Schools

```scala
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
    
    /*
    OUTPUT:
    (GP,12.235618597320721)
	(MS,10.482300884955754)
    */

```

+ *"GP" is a little Better than "MS"*

## 5. Compare the Final Grade of Male Versus Female.

```scala
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
    }).collect().foreach(println)

    /*
    OUTPUT:
	(M,11.223057644110266)
	(F,11.904264577893827)
    */

```

* *There seems to be a weak  correlation between Male Versus Female*
* *Finally, we decided to use all the attributes*

## 6. Create Extra Features

```scala
/*
Add the Following Extra Features:
33 allSup: schoolsup & famsup
34 pairEdu: Medu & Fedu
35 moreHigh: higher & (schoolsup | paid)
36 allAlc: Walc + Dalc 
37 dalcPerWeek: Dalc / (Walc + Dalc)
38 studytimeRatio: studytime / (traveltime + studytime + freetime)

Drop the Following Previous Features
14 Dalc
26 
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
    }).coalesce(1,true).saveAsTextFile("hdfs://test:9000/user/hadoop/output_Data")

```



## 7. Random Data Generator

```scala
/*
Data Generator 
version: 1.0
*/

import java.io.FileWriter
import java.io.File

object RandomData{
    def main(args:Array[String]) {
        val size:Int = 100000
        val arr1 = Array("GP","MS")
        val arr2 = Array("F","M")
        val arr4 = Array("U","R")
        val arr5 = Array("LE3","GT3")
        val arr6 = Array("T","A")
        val arr9 = Array("teache", "health", "services", "at_home", "other")
        val arr10 = Array("teache", "health", "services", "at_home", "other")
        val arr11 = Array("home", "reputation", "course", "other")
        val arr12 = Array("mother", "father", "other")
        val arr16 = Array("yes", "no")
        val arr17 = Array("yes", "no")
        val arr18 = Array("yes", "no")
        val arr19 = Array("yes", "no")
        val arr20 = Array("yes", "no")
        val arr21 = Array("yes", "no")
        val arr22 = Array("yes", "no")
        val arr23 = Array("yes", "no")   
        val fw = new FileWriter("hdfs://test:9000/user/hadoop/randomData1.csv", false)
        val rd = new util.Random
        for(i <- 0 until size){
            val r1 = rd.nextInt(2)
            val r2 = rd.nextInt(2)
            val r3 = rd.nextInt(8) + 15
            val r4 = rd.nextInt(2)
            val r5 = rd.nextInt(2)
            val r6 = rd.nextInt(2)
            val r7 = rd.nextInt(4) + 1
            val r8 = rd.nextInt(4) + 1
            val r9 = rd.nextInt(5)
            val r10 = rd.nextInt(5)
            val r11 = rd.nextInt(4)
            val r12 = rd.nextInt(3)
            val r13 = rd.nextInt(4) + 1
            val r14 = rd.nextInt(4) + 1
            val r15 = rd.nextInt(4) + 1
            val r16 = rd.nextInt(2)
            val r17 = rd.nextInt(2)
            val r18 = rd.nextInt(2)
            val r19 = rd.nextInt(2)
            val r20 = rd.nextInt(2)
            val r21 = rd.nextInt(2)
            val r22 = rd.nextInt(2)
            val r23 = rd.nextInt(2)
            val r24 = rd.nextInt(5) + 1
            val r25 = rd.nextInt(5) + 1
            val r26 = rd.nextInt(5) + 1
            val r27 = rd.nextInt(5) + 1
            val r28 = rd.nextInt(5) + 1
            val r29 = rd.nextInt(5) + 1
            val r30 = rd.nextInt(94)
            val temp = Array(
                arr1(r1), 
                arr2(r2), 
                String.valueOf(r3), 
                arr4(r4), 
                arr5(r5), 
                arr6(r6), 
                String.valueOf(r7), 
                String.valueOf(r8), 
                arr9(r9), 
                arr10(r10), 
                arr11(r11), 
                arr12(r12), 
                String.valueOf(r13), 
                String.valueOf(r14),
                String.valueOf(r15), 
                arr16(r16),
                arr17(r17), 
                arr18(r18), 
                arr19(r19),
                arr20(r20),
                arr21(r21),
                arr22(r22),
                arr23(r23),
                String.valueOf(r24), 
                String.valueOf(r25),
                String.valueOf(r26), 
                String.valueOf(r27), 
                String.valueOf(r28),
                String.valueOf(r29),
                String.valueOf(r30)
                )
            val temp1 = temp.mkString(",")
            fw.write(temp1 + "\n")
        }
        fw.close()
    }
}
```



