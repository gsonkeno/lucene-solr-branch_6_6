/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.lucene.search;


import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

import org.apache.lucene.index.LeafReaderContext;

/** A {@link Rescorer} that uses a provided Query to assign
 *  scores to the first-pass hits.
 *
 * @lucene.experimental */
public abstract class QueryRescorer extends Rescorer {
  /**第二轮的排序query**/
  private final Query query;

  /** Sole constructor, passing the 2nd pass query to
   *  assign scores to the 1st pass hits.  */
  public QueryRescorer(Query query) {
    this.query = query;
  }

  /**
   * Implement this in a subclass to combine the first pass and
   * second pass scores.  If secondPassMatches is false then
   * the second pass query failed to match a hit from the
   * first pass query, and you should ignore the
   * secondPassScore.
   */
  protected abstract float combine(float firstPassScore, boolean secondPassMatches, float secondPassScore);

  @Override
  public TopDocs rescore(IndexSearcher searcher, TopDocs firstPassTopDocs, int topN) throws IOException {
    ScoreDoc[] hits = firstPassTopDocs.scoreDocs.clone();//第一阶段查询文档
    Arrays.sort(hits,
                new Comparator<ScoreDoc>() {
                  @Override
                  public int compare(ScoreDoc a, ScoreDoc b) {
                    return a.doc - b.doc;
                  }//对第一阶段查询先按docid升序排序
                });

    List<LeafReaderContext> leaves = searcher.getIndexReader().leaves();
    // 使用第二轮排序的query进行重排
    Weight weight = searcher.createNormalizedWeight(query, true);

    // Now merge sort docIDs from hits, with reader's leaves:
    int hitUpto = 0;
    int readerUpto = -1;
    int endDoc = 0;
    int docBase = 0;
    Scorer scorer = null;

    while (hitUpto < hits.length) {//循环所有的第一轮收集到的doc
      ScoreDoc hit = hits[hitUpto];
      int docID = hit.doc;
      LeafReaderContext readerContext = null;
      while (docID >= endDoc) {
        readerUpto++;
        readerContext = leaves.get(readerUpto);
        endDoc = readerContext.docBase + readerContext.reader().maxDoc();//endDoc表示当前正在查找的段的最大的docid。
      }

      if (readerContext != null) {
        // We advanced to another segment:
        docBase = readerContext.docBase;
        scorer = weight.scorer(readerContext);
      }

      if(scorer != null) {
        int targetDoc = docID - docBase;
        int actualDoc = scorer.docID();
        if (actualDoc < targetDoc) {
          actualDoc = scorer.iterator().advance(targetDoc);
        }

        if (actualDoc == targetDoc) {//如果相等，表示第二次查询也命中了这个doc
          // Query did match this doc:
          hit.score = combine(hit.score, true, scorer.score());
        } else {
          // Query did not match this doc:
          assert actualDoc > targetDoc;
          hit.score = combine(hit.score, false, 0.0f);
        }
      } else {//如果在这个段中第二轮的qurry没有命中任何的结果，则一定不会命中这个doc了
        // Query did not match this doc:
        hit.score = combine(hit.score, false, 0.0f);
      }

      hitUpto++;
    }

    // TODO: we should do a partial sort (of only topN)
    // instead, but typically the number of hits is
    // smallish:
    Arrays.sort(hits,
                new Comparator<ScoreDoc>() {
                  @Override
                  public int compare(ScoreDoc a, ScoreDoc b) {
                    // Sort by score descending, then docID ascending:
                    if (a.score > b.score) {
                      return -1;
                    } else if (a.score < b.score) {
                      return 1;
                    } else {
                      // This subtraction can't overflow int
                      // because docIDs are >= 0:
                      return a.doc - b.doc;
                    }
                  }
                });

    if (topN < hits.length) {//这个的意思是如果最后返回的额topN个（也就是start+rows）小于收集到的doc，则只取topN个。比如第一阶段收集了100个，但是分页显示每一页10个，只要第二页的10个，则topN是20，只要前面的20个即可。
      ScoreDoc[] subset = new ScoreDoc[topN];
      System.arraycopy(hits, 0, subset, 0, topN);
      hits = subset;
    }

    return new TopDocs(firstPassTopDocs.totalHits, hits, hits[0].score);
  }

  @Override
  public Explanation explain(IndexSearcher searcher, Explanation firstPassExplanation, int docID) throws IOException {
    Explanation secondPassExplanation = searcher.explain(query, docID);

    Float secondPassScore = secondPassExplanation.isMatch() ? secondPassExplanation.getValue() : null;

    float score;
    if (secondPassScore == null) {
      score = combine(firstPassExplanation.getValue(), false, 0.0f);
    } else {
      score = combine(firstPassExplanation.getValue(), true,  secondPassScore.floatValue());
    }

    Explanation first = Explanation.match(firstPassExplanation.getValue(), "first pass score", firstPassExplanation);

    Explanation second;
    if (secondPassScore == null) {
      second = Explanation.noMatch("no second pass score");
    } else {
      second = Explanation.match(secondPassScore, "second pass score", secondPassExplanation);
    }

    return Explanation.match(score, "combined first and second pass score using " + getClass(), first, second);
  }

  /** Sugar API, calling {#rescore} using a simple linear
   *  combination of firstPassScore + weight * secondPassScore */
  public static TopDocs rescore(IndexSearcher searcher, TopDocs topDocs, Query query, final double weight, int topN) throws IOException {
    return new QueryRescorer(query) {
      @Override
      protected float combine(float firstPassScore, boolean secondPassMatches, float secondPassScore) {
        float score = firstPassScore;
        if (secondPassMatches) {
          score += weight * secondPassScore;
        }
        return score;
      }
    }.rescore(searcher, topDocs, topN);
  }
}
