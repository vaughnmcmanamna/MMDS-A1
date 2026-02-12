package similarity


import org.apache.spark.rdd.RDD

object minhash {


  def shingle_line(stList: List[String], shingleSize:Int):Shingled_Record = ???

  def minhash_record(r: Shingled_Record, hashFuncs: List[Hash_Func]): Min_Hash_Record = ???


  def compute_jaccard_pair(a:Shingled_Record, b: Shingled_Record): Similarity = ???

  def find_jaccard_matches(records: RDD[Shingled_Record], minSimilarity:Double): Matches = ???

  def compute_minhash_pair(a: Min_Hash_Record, b: Min_Hash_Record): Similarity = ???

  def find_minhash_candidates(records:RDD[Min_Hash_Record], minSimilarity:Double): Candidates = ???

  def find_lsh_bucket_sets(minHashes: RDD[Min_Hash_Record],
                           bandSize: Int,
                           bandHashFuncs: List[List[Int] => Int]): RDD[Iterable[Min_Hash_Record]] = ???

  def compute_lsh_candidates_similarity(candidatePairs: RDD[(Min_Hash_Record, Min_Hash_Record)],
                                        minSimilarity: Double): Candidates = ???
}



