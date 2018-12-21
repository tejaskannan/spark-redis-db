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

    private val containsFreq: Array[Double] = Array(0.0795, 0.0214, 0.0465, 0.0362, 0.0915, 0.0127,
                                                    0.0271, 0.03, 0.0817, 0.0019, 0.0092, 0.057, 
                                                    0.0339, 0.0682, 0.067, 0.0357, 0.0021, 0.0719, 
                                                    0.0668, 0.0661, 0.0414, 0.0115, 0.0077, 0.0038,
                                                    0.0241, 0.0051)



    def getPrefixFreq(c: Char): Double = {
        prefixFreq(charToIndex(c))
    }

    def getSuffixFreq(c: Char): Double = {
        suffixFreq(charToIndex(c))
    }

    def getContainsFreq(c: Char): Double = {
        containsFreq(charToIndex(c))
    }

    // Only works with lowercase letters for now
    private def charToIndex(c: Char): Int = {
        c.toInt - 97
    }

}
