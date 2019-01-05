package uk.ac.cam.cl.r244

object Utils {

    def isLetter(c: Char): Boolean = {
        (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')
    }

    def getMaxFreqLetter(str: String, stats: StatisticsManager, cutoff: Int): Char = {
        val start: Int = if (str(0) != '^') 0 else 1
        val end: Int = if (str(str.length - 1) != '$') str.length else (str.length - 1)

        var maxFreq: Double = 0.0
        var maxChar: Char = '\0'
        for (i <- start until min(end, cutoff)) {
            if (isLetter(str(i)) && stats.getContainsFreq(str(i)) > maxFreq) {
                maxFreq = stats.getContainsFreq(str(i))
                maxChar = str(i)
            }
        }
        maxChar
    }

    // Optimized version of edit distance algorithm
    // Implemetation from: https://www.codeproject.com/Articles/13525/%2FArticles%2F13525%2FFast-memory-efficient-Levenshtein-algorithm-2
    def editDistance(s1: String, s2: String, limit: Int): Boolean = {
        // We define the starting index by omitting comparisons between
        // common prefixes
        var start = 0
        while (start < s1.length && start < s2.length && s1(start) == s2(start)) {
            start += 1
        }

        if (start == s1.length) {
            (s2.length - s1.length) <= limit
        } else if (start == s2.length) {
            (s1.length - s2.length) <= limit
        } else {
            var str1 = s1
            var str2 = s2
            // We keep an array of size s2.length, so we reduce memory here if possible
            if (s2.length < s1.length) {
                str1 = s2
                str2 = s1
            }

            var a0: Array[Int] = (0 to str2.length - start).toArray
            var a1: Array[Int] = Array.fill[Int](str2.length + 1 - start)(0)
            for (i <- 1 to str1.length - start) {
                a1(0) = i
                val c = str1(i + start - 1)
                var dist = str1.length + 1
                for (j <- 1 to str2.length - start) {
                    val cost = if (c == str2(j + start - 1)) 0 else 1
                    a1(j) = min3(a0(j) + 1, a1(j-1) + 1, a0(j-1) + cost)
                    if (a1(j) < dist) {
                        dist = a1(j)
                    }
                }

                // We can stop early if the minimum value seen in this column
                // is greater than the limit as edit distance cannot decrease
                if (dist > limit) {
                    return false
                }

                val tmp: Array[Int] = a0
                a0 = a1
                a1 = tmp
            }
            a0(a0.length - 1) <= limit
        }
    }

    def smithWatermanLinear(s1: String, s2: String, minScore: Int): Boolean = {
        var str1 = s1
        var str2 = s2
        // We keep an array of size O(s2.length), so we reduce memory here if possible
        if (s2.length < s1.length) {
            str1 = s2
            str2 = s1
        }

        val gapPenalty = 1

        var a0: Array[Int] = Array.fill[Int](str2.length + 1)(0)
        var a1: Array[Int] = Array.fill[Int](str2.length + 1)(0)
        for (i <- 1 to str1.length) {
            a1(0) = 0
            val c = str1(i-1)
            for (j <- 1 to str2.length) {
                val s = if (c == str2(j-1)) 1 else -1
                a1(j) = max4(a0(j) - gapPenalty, a1(j-1) - gapPenalty, a0(j-1) + s, 0)
                if (a1(j) >= minScore) {
                    return true
                }
            }

            val tmp: Array[Int] = a0
            a0 = a1
            a1 = tmp
        }
        false
    }

    def getLongestCharSubstring(regex: String): String = {
        var index = 0
        while (index < regex.length && !isLetter(regex(index))) {
            index += 1
        }
        if (index == regex.length) {
            regex
        } else {
            var start = index
            index += 1

            var bestStart = start
            var bestEnd = start + 1
            while (index < regex.length) {
                if (!isLetter(regex(index))) {
                    if (index - start > bestEnd - bestStart) {
                        bestStart = start
                        bestEnd = index
                    }
                    start = index + 1
                }
                index += 1
            }
            if ((index - start) > bestEnd - bestStart) {
                regex.substring(start, index)
            } else {
                regex.substring(bestStart, bestEnd)
            }
        }
    }

    def max(a: Int, b: Int): Int = {
        if (a > b) {
            a
        } else {
            b
        }
    }

    private def min3(a: Int, b: Int, c: Int): Int = {
        min(min(a, b), c)
    }

    private def max4(a: Int, b: Int, c: Int, d: Int): Int = {
        max(max(a, b), max(c, d))
    }

    private def min(a: Int, b: Int): Int = {
        if (a < b) {
            a
        } else {
            b
        }
    }

}
