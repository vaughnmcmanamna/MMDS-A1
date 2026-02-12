package similarity

import scala.io.Source

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.log4j.{Level, Logger}

object Main extends App {

  // Reduce level of messages from Spark while running

  val rootLogger = Logger.getRootLogger()
  rootLogger.setLevel(Level.ERROR)
  Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
  Logger.getLogger("org.spark-project").setLevel(Level.ERROR)

  val conf: SparkConf = new SparkConf().setMaster("local[4]").setAppName("MinHash")
  val sc: SparkContext = new SparkContext(conf)
  sc.setLogLevel("ERROR") // avoid all those messages going on

  // process arguments

  if (args.size != 9) {
    val sep = " "
    System.err.println(s"Arguments missing <filename> <delimiter> <minimumSimilarity> <shingleSize> <hashCount> <bandSize> <doJaccard> <doAllMinHashes> <printHashCoefficients> <outputHashFunctions>. Only provided ${args.size} parameters\n   Provided ${args.mkString(sep)}")
    throw new IllegalArgumentException(s"Program terminated...")

  }
  val filename = args(0)
  val SEPARATOR = args(1)
  val MIN_SIM = args(2).toDouble
  val SHINGLE_SIZE = args(3).toInt
  val HASH_COUNT = args(4).toInt
  val BAND_SIZE = args(5).toInt
  val DO_JACCARD = args(6).toBoolean
  val DO_ALL_MIN_HASHES = args(7).toBoolean
  val PRINT_HASH_COEFS = args(8).toBoolean

  println("Computing similarity with parameters: ")
  List(("Filename",filename), ("Separator", "[" + SEPARATOR + "]"), ("Minimum similarity", MIN_SIM),
    ("Shingle size", SHINGLE_SIZE), ("Hash count", HASH_COUNT), ("Band size", BAND_SIZE),
    ("Compute Jaccard similarity", DO_JACCARD),
    ("Compute all minHashes similarity", DO_ALL_MIN_HASHES),
    ("Print hash coefficients", PRINT_HASH_COEFS),
  ).map{
    case (st, v)=>println( s"    ${st}: $v")
  }
  println()

//-------------------------------
// starting...

  val lines = sc.textFile(filename)

  // create hash functions needed
  val aHashCoefs = utils.generate_random_coefficients(HASH_COUNT)
  val bHashCoefs = utils.generate_random_coefficients(HASH_COUNT)

  if (PRINT_HASH_COEFS) {
    println("Hash coefficients:")
    println(aHashCoefs)
    println(bHashCoefs)
  }

  def hashFunctions : List[Hash_Func] = utils.create_hash_functions(aHashCoefs, bHashCoefs)

  System.err.println("Shingling records...")

  val docs = lines.
    filter(_.contains(SEPARATOR)).
    map{line =>
      val tokens = line.split(SEPARATOR).filter(_ != "").toList
      minhash.shingle_line(tokens, SHINGLE_SIZE)
    }.
    filter(_.shingles.isDefined).persist()

  System.err.println("Minhashing records...")

  val minHashes = docs.
    map(r=> minhash.minhash_record(r, hashFunctions)).persist()

  // do lhs now
  val jac: Matches =
    if (DO_JACCARD) {
      System.err.println("Doing Jaccard comparison... this might be slow...")
      minhash.find_jaccard_matches(docs, MIN_SIM)
    } else
        Array()

  val minHashCandidates: Candidates =
    if (DO_ALL_MIN_HASHES) {
      System.err.println("Doing all min hashes comparison...")
      minhash.find_minhash_candidates(minHashes, MIN_SIM)
    } else Array()

  System.err.println("Doing LSH comparisons...")

  // Generate hash functions for LSH bands (one per band)
  // Each band uses a different hash function by incorporating the band index
  val numBands = math.ceil(HASH_COUNT.toDouble / BAND_SIZE).toInt
  val bandHashFuncs = (0 until numBands).map { bandIdx =>
    (band: List[Int]) => utils.intList_to_hash(bandIdx :: band)  // Prepend bandIdx for uniqueness
  }.toList

  // Step 1: Create LSH buckets (student implements)
  val buckets = minhash.find_lsh_bucket_sets(minHashes, BAND_SIZE, bandHashFuncs)

  // Step 2: Generate and deduplicate candidate pairs (we provide)
  val candidatePairs = buckets
    .flatMap { bucket =>
      val records = bucket.toList
      for {
        i <- records.indices
        j <- (i+1) until records.size
      } yield {
        // Ensure IDs are in sorted order for consistency
        val (first, second) = if (records(i).id < records(j).id)
          (records(i), records(j))
        else
          (records(j), records(i))
        (first, second)
      }
    }
    .keyBy { case (a, b) => (a.id, b.id) }
    .reduceByKey((pair, _) => pair)  // Deduplicate pairs
    .values

  // Step 3: Compute similarities for candidates (student implements)
  val lshCandidates = minhash.compute_lsh_candidates_similarity(candidatePairs, MIN_SIM)

  System.err.println("Doing report.")
  utils.do_report(jac, minHashCandidates, lshCandidates)
  System.err.println("Finished.")

  sc.stop()


}
