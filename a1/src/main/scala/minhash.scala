package similarity


import org.apache.spark.rdd.RDD

object minhash {


  def shingle_line(stList: List[String], shingleSize:Int):Shingled_Record = {
    if (stList.isEmpty) throw new IllegalArgumentException("...")
    
    val id = stList.head
    val words = stList.tail
    val nWords = words.size

    val shingles =
      if (nWords < shingleSize) None
      else Some(
        words
          .sliding(shingleSize)
          .map(utils.stringList_to_hash)
          .toSet
      )
    Shingled_Record(id, nWords, shingles)
  }

  def minhash_record(r: Shingled_Record, hashFuncs: List[Hash_Func]): Min_Hash_Record = ???

  def compute_jaccard_pair(a:Shingled_Record, b: Shingled_Record): Similarity = {
    if (a.shingles.isEmpty || b.shingles.isEmpty) throw new IllegalArgumentException("...")

    val setA = a.shingles.get
    val setB = b.shingles.get

    val intersection = setA.intersect(setB)
    val union = setA.union(setB)

    val sim = intersection.size.toDouble / union.size.toDouble

    Similarity(a.id, b.id, sim)
  }



  def find_jaccard_matches(records: RDD[Shingled_Record], minSimilarity:Double): Matches = ???

  def compute_minhash_pair(a: Min_Hash_Record, b: Min_Hash_Record): Similarity = ???

  def find_minhash_candidates(records:RDD[Min_Hash_Record], minSimilarity:Double): Candidates = ???

  def find_lsh_bucket_sets(minHashes: RDD[Min_Hash_Record],
                           bandSize: Int,
                           bandHashFuncs: List[List[Int] => Int]): RDD[Iterable[Min_Hash_Record]] = ???

  def compute_lsh_candidates_similarity(candidatePairs: RDD[(Min_Hash_Record, Min_Hash_Record)],
                                        minSimilarity: Double): Candidates = ???
}



