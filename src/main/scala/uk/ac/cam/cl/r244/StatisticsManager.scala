package uk.ac.cam.cl.r244

/**
 * @author ${user.name}
 */

import scala.collection.mutable.Map

class StatisticsManager {

    private val prefixFreq: Array[Double] = Array(0.0687, 0.0498, 0.0867, 0.0506, 0.0384, 0.0321, 
                                                  0.0296, 0.0371, 0.0357, 0.0077, 0.0107, 0.027,
                                                  0.0535, 0.0364, 0.0343, 0.0942, 0.0048, 0.0453,
                                                  0.1047, 0.0508, 0.0615, 0.0144, 0.0177, 0.0014,
                                                  0.0031, 0.0038)

    private val suffixFreq: Array[Double] = Array(0.0442, 0.0017, 0.037, 0.0835, 0.1539, 0.0032,
                                                  0.0527, 0.0164, 0.0091, 0.0001, 0.0096, 0.0495,
                                                  0.0281, 0.0693, 0.0097, 0.0074, 0.0001, 0.0561,
                                                  0.2044, 0.0561, 0.0022, 0.0004, 0.0026, 0.0028,
                                                  0.0994, 0.0007)

    private val middleFreq: Array[Double] = Array(0.089, 0.0187, 0.0423, 0.0262, 0.0971, 0.0103, 
                                                  0.0216, 0.0296, 0.0973, 0.0011, 0.0083, 0.0605,
                                                  0.0305, 0.0744, 0.0781, 0.0303, 0.0018, 0.0778,
                                                  0.0506, 0.0695, 0.0431, 0.0118, 0.0065, 0.004,
                                                  0.0138, 0.0055)



    def getPrefixFreq(c: Char): Double = {
        prefixFreq(charToIndex(c))
    }

    def getSuffixFreq(c: Char): Double = {
        suffixFreq(charToIndex(c))
    }

    def getMiddleFreq(c: Char): Double = {
        middleFreq(charToIndex(c))
    }

    // Only works with lowercase letters for now
    private def charToIndex(c: Char): Int = {
        c.toInt - 97
    }

}
