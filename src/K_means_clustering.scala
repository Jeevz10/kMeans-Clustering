import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/** A raw posting, either a question or an answer */
case class Posting(postingType: Int, id: Int, parentId: Option[Int], score: Int, tags: Option[String]) extends Serializable

/** The main class */
object Assignment2 extends Assignment2 {

  @transient lazy val conf: SparkConf = new SparkConf().setMaster("local").setAppName("Assignment2")
  @transient lazy val sc: SparkContext = new SparkContext(conf)

  //sc.setLogLevel("WARN")

  /** Main function */
  def main(args: Array[String]): Unit = {

    val lines   = sc.textFile(args(0))
    val raw     = rawPostings(lines)
    val grouped = groupedPostings(raw)
    val scored  = scoredPostings(grouped)
    val vectors = vectorPostings(scored)

    val means = kmeans(vectors)
    val results = clusterResults(means, vectors)
    printResults(results)
  }
}


/** The parsing and kmeans methods */
class Assignment2 extends Serializable {

  /** Languages */
  val Domains =
    List(
      "Machine-Learning", "Compute-Science", "Algorithm", "Big-Data", "Data-Analysis", "Security", "Silicon Valley", "Computer-Systems",
      "Deep-learning", "Internet-Service-Providers", "Programming-Language", "Cloud-services", "Software-Engineering", "Embedded-System", "Architecture")


  /** K-means parameter: How "far apart" languages should be for the kmeans algorithm? */
  def DomainSpread = 50000
  assert(DomainSpread > 0)

  /** K-means parameter: Number of clusters */
  def kmeansKernels = 45

  /** K-means parameter: Convergence criteria, if changes of all centriods < kmeansEta, stop*/
  def kmeansEta: Double = 20.0D

  /** K-means parameter: Maximum iterations */
  def kmeansMaxIterations = 120


  //
  //
  // Parsing utilities:
  //
  //

  /** Load postings from the given file */
  def rawPostings(lines: RDD[String]): RDD[Posting] =
    lines.map(line => {
      val arr = line.split(",")
      Posting(postingType =    arr(0).toInt,
        id =             arr(1).toInt,
        parentId =       if (arr(2) == "") None else Some(arr(2).toInt),
        score =          arr(3).toInt,
        tags =           if (arr.length >= 5) Some(arr(4).intern()) else None)
    })

  // how to print: https://www.edureka.co/community/9005/how-to-print-the-contents-of-rdd-in-apache-spark
  // how to differentiate between RDD[unit] and RDD[any]: https://stackoverflow.com/questions/14205367/found-unit-required-int-why-is-the-error-not-obvious
  /**
   * Group the questions and answers together. ID / ParentID are key, and an iterable of Postings as questions and answers
   * */
  def groupedPostings(postings: RDD[Posting]): RDD[(Option[Int], Iterable[Posting])]  = {
    // Filter the questions and answers separately
    // Prepare them for a join operation by extracting the QID value in the first element of a tuple.
    val requiredPostings: RDD[(Option[Int], Posting)] = postings.map(posting => {
      if (posting.postingType == 1) {
        (Option(posting.id), posting)
      }
      else {
        (posting.parentId, posting)
      }
    })
    // requiredPostings.collect().foreach(println)
    val groupedPostings = requiredPostings.groupByKey()
    // groupedPostings.collect().foreach(println)
    groupedPostings
  }

  /** Compute the maximum score for each posting */
  def scoredPostings(postings: RDD[(Option[Int], Iterable[Posting])]): RDD[(Int, Iterable[String])]  = {
    val maxScorePostingKVPair: RDD[(Int, Iterable[String])] = postings.map(posting => {

      // Max Score
      val maxScorePosting = posting._2.maxBy(each => each.score)
      val maxScore = maxScorePosting.score

      // Domains
      val domain = posting._2.flatMap(each => each.tags)

      (maxScore, domain)

    })
    // maxScorePostingKVPair.collect().foreach(println)
    maxScorePostingKVPair
  }


  /** Compute the vectors for the kmeans
   *  Me: When designing vectors, you need the domain index (take from domain) and the highest score from all its answers.
   * */
  def vectorPostings(maxScorePostingKVPair: RDD[(Int, Iterable[String])]): RDD[(Int,Int)] = {
    val vector: RDD[(Int, Int)] = maxScorePostingKVPair.map(pair => {
      // Get Max Score
      val maxScore = pair._1

      // Get Domain
      val domain = pair._2.head
      val domainIndex = Domains.indexOf(domain)

      // Compute Domain multiplied with spread
      val domainMultiplyWithSpread = domainIndex * DomainSpread

      (domainMultiplyWithSpread, maxScore)
    })

    // vector.collect().foreach(println)
    vector
  }

  /**
   * Main kmeans computation
   * */

  final def kmeans(vectors: RDD[(Int, Int)]): Array[(Int,Int)]= {

    // Start with kmeansKernels (45) random points as centroids
    var kPoints = vectors.takeSample(false, kmeansKernels)

    // Loop at most kmeansMaxIterations (125) items with the overbearing condition of distance in mind

    var tempDist = Double.PositiveInfinity

    for(i <- 1 to kmeansMaxIterations
        if tempDist > kmeansEta) {

      // find the closest k cluster for each point (vector)
      val findingPointsClosestToK = vectors.map(vector => (findClosest(vector, kPoints), vector))


      val groupedPointsClosestToK = findingPointsClosestToK.groupByKey()

      // find the average point for each cluster
      val averagePointOfEachK = groupedPointsClosestToK.map(pair => {
        val arrayOfPoints = pair._2
        val averagePoint = averageVectors(arrayOfPoints)
        (pair._1, averagePoint)
      })

      // Calculate the average distance between original k point and newly computed average point
      tempDist = 0.0

      val tempDistRDD = averagePointOfEachK.map(point => {
        val kCluster = point._1

        val currentKPoint = kPoints(kCluster)
        val averagePoint = point._2
        val distance = euclideanDistance(currentKPoint, averagePoint)
        distance
      })

      tempDist = tempDistRDD.reduce((a,b) => a+b)

      println("Iteration :" + i + " Distance is " + tempDist)
      // Assign back the points

      val kPointsRDD = averagePointOfEachK.map(point => {
        point._2
      })

      kPoints = kPointsRDD.collect()
    }
    kPoints
  }

  //  Kmeans utilities (Just some cases, you can implement your own utilities.)

  /**
   * Remove if not used
   */

  def addPoints(p1: (Int, Int), p2: (Int, Int)) ={
    (p1._1 + p2._1, p1._2 + p2._2)
  }
  /** Decide whether the kmeans clustering converged */
  def converged(distance: Double) = distance < kmeansEta


  /** Return the euclidean distance between two points */
  def euclideanDistance(v1: (Int, Int), v2: (Int, Int)): Double = {
    val part1 = (v1._1 - v2._1).toDouble * (v1._1 - v2._1)
    val part2 = (v1._2 - v2._2).toDouble * (v1._2 - v2._2)
    part1 + part2
  }

  /** Return the euclidean distance between two points */
  def euclideanDistance(a1: Array[(Int, Int)], a2: Array[(Int, Int)]): Double = {
    assert(a1.length == a2.length)
    var sum = 0d
    var idx = 0
    while(idx < a1.length) {
      sum += euclideanDistance(a1(idx), a2(idx))
      idx += 1
    }
    sum
  }

  /** Return the closest point */
  def findClosest(p: (Int, Int), centers: Array[(Int, Int)]): Int = {
    var bestIndex = 0
    var closest = Double.PositiveInfinity
    for (i <- 0 until centers.length) {
      val tempDist = euclideanDistance(p, centers(i))
      if (tempDist < closest) {
        closest = tempDist
        bestIndex = i
      }
    }
    bestIndex
  }


  /** Average the vectors */
  def averageVectors(ps: Iterable[(Int, Int)]): (Int, Int) = {
    val iter = ps.iterator
    var count = 0
    var comp1: Long = 0
    var comp2: Long = 0
    while (iter.hasNext) {
      val item = iter.next
      comp1 += item._1
      comp2 += item._2
      count += 1
    }
    ((comp1 / count).toInt, (comp2 / count).toInt)
  }


  def computeMedian(a: Iterable[(Int, Int)]) = {
    val s = a.map(x => x._2).toArray
    val length = s.length
    val (lower, upper) = s.sortWith(_<_).splitAt(length / 2)
    if (length % 2 == 0) (lower.last + upper.head) / 2 else upper.head
  }

  def clusterResults(means: Array[(Int, Int)], vectors: RDD[(Int, Int)]): Array[(String, (Int, Int), Int, Int, Int)] = {

    val groupingVectorsToCluster = vectors.map(vector => (findClosest(vector, means), vector))
    val groupedVectorsToEachCluster = groupingVectorsToCluster.groupByKey()

    val answerPerClusterRDD = groupedVectorsToEachCluster.map(row => {
      val median = computeMedian(row._2)
      val size = row._2.size
      val centroid = means(row._1)

      val averagePair = averageVectors(row._2)
      val average = averagePair._2

      val domainIndex = averagePair._1 / DomainSpread
      val domain = Domains(domainIndex)

      (domain, centroid, size, median, average)
    })

    val answerArray = answerPerClusterRDD.collect()
    answerArray
  }
  //  Displaying results:

  def printResults(groupedVectors: Array[(String, (Int, Int), Int, Int, Int)]): Unit  = {
    println("Pre Defined Parameters are below")
    println("Domain Spread: " + DomainSpread)
    println("K Means Kernels: " + kmeansKernels)
    println("K Means ETA: " + kmeansEta)
    println("K Means Max Iterations: " + kmeansMaxIterations)
    println("=================================")
    println("Resulting Clusters:")
    println(" Number Domain Centroid Size Median Average")
    println("===========================================")
    for (i <- 0 until groupedVectors.size) {
      val row = groupedVectors(i)
      val domain = row._1
      val centroid = row._2
      val size = row._3
      val median = row._4
      val average = row._5

      println(i + " " + domain + " " + centroid + " " + size + " " + median + " " + average)
    }
  }
}
