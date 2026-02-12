package similarity

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * Unit tests for MinHash/LSH algorithms
 *
 * These tests validate individual functions without requiring Spark,
 * making them fast and focused for development and debugging.
 */
class MinHashSpec extends AnyFunSuite with Matchers {

  // ========================================================================
  // Code quality checks
  // ========================================================================

  test("Code quality: minhash.scala should not contain mutable var bindings") {
    val sourceFile = "src/main/scala/minhash.scala"
    val lines = scala.io.Source.fromFile(sourceFile).getLines().toList

    // Find lines with var declarations (being smart about comments and context)
    val varLines = lines.filter { line =>
      val trimmed = line.trim

      // Skip comment lines
      if (trimmed.startsWith("//") || trimmed.startsWith("/*") || trimmed.startsWith("*")) {
        false
      } else {
        // Remove string literals to avoid false positives
        val withoutStrings = trimmed.replaceAll("\"[^\"]*\"", "")

        // Check for 'var ' or 'var\t' (word boundary) but not as part of another word
        val pattern = "\\bvar\\s+".r
        pattern.findFirstIn(withoutStrings).isDefined
      }
    }

    if (varLines.nonEmpty) {
      val errorMessage = varLines.mkString(
        "Found mutable var bindings in minhash.scala (use val instead):\n  ",
        "\n  ",
        ""
      )
      fail(errorMessage)
    }
  }

  // ========================================================================
  // Tests for shingle_line
  // ========================================================================

  test("shingle_line: creates correct shingles for valid input") {
    val input = List("id1", "the", "cat", "sat", "on", "mat")
    val result = minhash.shingle_line(input, 2)

    assert(result.id == "id1")
    assert(result.nWords == 5)
    assert(result.shingles.isDefined)

    // Should have 4 shingles: (the,cat), (cat,sat), (sat,on), (on,mat)
    result.shingles.get.size should be (4)
  }

  test("shingle_line: returns None for insufficient tokens") {
    val input = List("id1", "cat")
    val result = minhash.shingle_line(input, 3)

    assert(result.id == "id1")
    assert(result.nWords == 1)
    assert(result.shingles.isEmpty)
  }

  test("shingle_line: handles exact shingle size") {
    val input = List("id1", "the", "cat", "sat")
    val result = minhash.shingle_line(input, 3)

    assert(result.shingles.isDefined)
    result.shingles.get.size should be (1) // Only one shingle: (the,cat,sat)
  }

  test("shingle_line: throws exception for empty input") {
    val input = List.empty[String]
    assertThrows[IllegalArgumentException] {
      minhash.shingle_line(input, 2)
    }
  }

  test("shingle_line: shingle size 1 creates individual word shingles") {
    val input = List("id1", "the", "cat", "sat")
    val result = minhash.shingle_line(input, 1)

    assert(result.shingles.isDefined)
    result.shingles.get.size should be (3) // Three words
  }

  // ========================================================================
  // Tests for compute_jaccard_pair
  // ========================================================================

  test("Jaccard: identical sets have similarity 1.0") {
    val a = Shingled_Record("a", 3, Some(Set(1, 2, 3)))
    val b = Shingled_Record("b", 3, Some(Set(1, 2, 3)))
    val sim = minhash.compute_jaccard_pair(a, b)

    assert(sim.idA == "a")
    assert(sim.idB == "b")
    assert(sim.sim === 1.0)
  }

  test("Jaccard: disjoint sets have similarity 0.0") {
    val a = Shingled_Record("a", 3, Some(Set(1, 2, 3)))
    val b = Shingled_Record("b", 3, Some(Set(4, 5, 6)))
    val sim = minhash.compute_jaccard_pair(a, b)

    assert(sim.sim === 0.0)
  }

  test("Jaccard: is symmetric") {
    val a = Shingled_Record("a", 3, Some(Set(1, 2, 3)))
    val b = Shingled_Record("b", 3, Some(Set(2, 3, 4)))

    val simAB = minhash.compute_jaccard_pair(a, b)
    val simBA = minhash.compute_jaccard_pair(b, a)

    assert(simAB.sim === simBA.sim)
  }

  test("Jaccard: correctly computes for partial overlap") {
    val a = Shingled_Record("a", 4, Some(Set(1, 2, 3, 4)))
    val b = Shingled_Record("b", 3, Some(Set(3, 4, 5)))
    val sim = minhash.compute_jaccard_pair(a, b)

    // Intersection: {3, 4} = 2 elements
    // Union: {1, 2, 3, 4, 5} = 5 elements
    // Jaccard = 2/5 = 0.4
    assert(sim.sim === 0.4)
  }

  test("Jaccard: throws exception for empty shingles") {
    val a = Shingled_Record("a", 0, None)
    val b = Shingled_Record("b", 3, Some(Set(1, 2, 3)))

    assertThrows[IllegalArgumentException] {
      minhash.compute_jaccard_pair(a, b)
    }
  }

  // ========================================================================
  // Tests for minhash_record
  // ========================================================================

  test("MinHash: produces correct number of hash values") {
    val record = Shingled_Record("id1", 3, Some(Set(1, 2, 3, 4, 5)))
    val hashFuncs = List[Hash_Func](
      (x: Int) => x * 2,
      (x: Int) => x * 3,
      (x: Int) => x * 5
    )

    val result = minhash.minhash_record(record, hashFuncs)

    assert(result.id == "id1")
    assert(result.minHashes.size == 3)
  }

  test("MinHash: computes minimum hash for each function") {
    val record = Shingled_Record("id1", 3, Some(Set(10, 20, 30)))
    val hashFuncs = List[Hash_Func](
      (x: Int) => x,      // Identity: min should be 10
      (x: Int) => -x,     // Negation: min should be -30
      (x: Int) => x * 2   // Double: min should be 20
    )

    val result = minhash.minhash_record(record, hashFuncs)

    assert(result.minHashes(0) == 10)
    assert(result.minHashes(1) == -30)
    assert(result.minHashes(2) == 20)
  }

  test("MinHash: throws exception for empty shingles") {
    val record = Shingled_Record("id1", 0, None)
    val hashFuncs = List[Hash_Func]((x: Int) => x)

    assertThrows[IllegalArgumentException] {
      minhash.minhash_record(record, hashFuncs)
    }
  }

  // ========================================================================
  // Tests for compute_minhash_pair
  // ========================================================================

  test("MinHash similarity: identical hashes have similarity 1.0") {
    val a = Min_Hash_Record("a", Vector(1, 2, 3, 4, 5))
    val b = Min_Hash_Record("b", Vector(1, 2, 3, 4, 5))

    val sim = minhash.compute_minhash_pair(a, b)

    assert(sim.sim === 1.0)
  }

  test("MinHash similarity: no matching hashes have similarity 0.0") {
    val a = Min_Hash_Record("a", Vector(1, 2, 3, 4, 5))
    val b = Min_Hash_Record("b", Vector(6, 7, 8, 9, 10))

    val sim = minhash.compute_minhash_pair(a, b)

    assert(sim.sim === 0.0)
  }

  test("MinHash similarity: correctly computes partial matches") {
    val a = Min_Hash_Record("a", Vector(1, 2, 3, 4, 5))
    val b = Min_Hash_Record("b", Vector(1, 2, 8, 9, 10))

    val sim = minhash.compute_minhash_pair(a, b)

    // 2 matches out of 5 = 0.4
    assert(sim.sim === 0.4)
  }

  // ========================================================================
  // Property-based tests
  // ========================================================================

  test("Property: Jaccard similarity is always between 0 and 1") {
    val testCases = List(
      (Set(1, 2, 3), Set(4, 5, 6)),       // Disjoint
      (Set(1, 2, 3), Set(1, 2, 3)),       // Identical
      (Set(1, 2, 3), Set(2, 3, 4)),       // Partial overlap
      (Set(1), Set(1, 2, 3, 4, 5)),       // Subset
      (Set(1, 2, 3, 4, 5), Set(3))        // Superset
    )

    testCases.foreach { case (setA, setB) =>
      val a = Shingled_Record("a", setA.size, Some(setA))
      val b = Shingled_Record("b", setB.size, Some(setB))
      val sim = minhash.compute_jaccard_pair(a, b)

      assert(sim.sim >= 0.0 && sim.sim <= 1.0,
        s"Jaccard similarity ${sim.sim} not in [0,1] for sets $setA and $setB")
    }
  }

  test("Property: MinHash similarity is always between 0 and 1") {
    val testCases = List(
      (Vector(1, 2, 3, 4, 5), Vector(6, 7, 8, 9, 10)),     // No matches
      (Vector(1, 2, 3, 4, 5), Vector(1, 2, 3, 4, 5)),      // All match
      (Vector(1, 2, 3, 4, 5), Vector(1, 7, 8, 9, 10))      // One match
    )

    testCases.foreach { case (hashesA, hashesB) =>
      val a = Min_Hash_Record("a", hashesA)
      val b = Min_Hash_Record("b", hashesB)
      val sim = minhash.compute_minhash_pair(a, b)

      assert(sim.sim >= 0.0 && sim.sim <= 1.0,
        s"MinHash similarity ${sim.sim} not in [0,1]")
    }
  }

  test("Property: Shingle count decreases as shingle size increases") {
    val tokens = List("id", "the", "quick", "brown", "fox", "jumps")
    val numWords = tokens.size - 1 // Exclude ID

    for (shingleSize <- 1 to numWords) {
      val result = minhash.shingle_line(tokens, shingleSize)
      if (result.shingles.isDefined) {
        val expectedCount = numWords - shingleSize + 1
        assert(result.shingles.get.size == expectedCount,
          s"Expected $expectedCount shingles for size $shingleSize, got ${result.shingles.get.size}")
      }
    }
  }

  // Note: LSH functions (find_lsh_bucket_sets, compute_candidates_similarity)
  // are tested through integration tests as they require full Spark RDD operations
}
