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