package uk.ac.cam.cl.r244

object Utils {

	def isLetter(c: Char): Boolean = {
		(c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')
	}

	def getMaxFreqLetter(str: String, stats: StatisticsManager): Char = {
		val start: Int = if (str(0) != '^') 0 else 1
		val end: Int = if (str(str.length - 1) != '$') str.length else (str.length - 1)

		var maxFreq: Double = 0.0
		var maxChar: Char = '\0'
		for (i <- start until end) {
			if (isLetter(str(i)) && stats.getContainsFreq(str(i)) > maxFreq) {
				maxFreq = stats.getContainsFreq(str(i))
				maxChar = str(i)
			}
		}
		maxChar
	}


	def editDistance(s1: String, s2: String, limit: Int): Boolean = {
		val mat: Array[Array[Int]] = Array.ofDim[Int](s1.length + 1, s2.length + 1)

		for (i <- 0 until s1.length) {
			mat(i)(0) = i
		}

		for (j <- 0 until s2.length) {
			mat(0)(j) = j
		}

		for (i <- 1 to s1.length) {
			for (j <- 1 to s2.length) {
				if (s1(i-1) == s2(j-1)) {
					mat(i)(j) = mat(i-1)(j-1)
				} else {
					mat(i)(j) = min3(mat(i-1)(j) + 1, mat(i)(j-1) + 1,
									 mat(i-1)(j-1) + 1)
				}
			}
		}
		return mat(s1.length)(s2.length) <= limit
	}

	private def min3(a: Int, b: Int, c: Int): Int = {
		if (a < b && a < c) {
			a
		} else if (b < a && b < c) {
			b
		} else {
			c
		}
	}

}
