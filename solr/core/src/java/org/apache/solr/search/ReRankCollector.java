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
package org.apache.solr.search;

import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Map;
import com.carrotsearch.hppc.IntFloatHashMap;
import com.carrotsearch.hppc.IntIntHashMap;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Rescorer;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopDocsCollector;
import org.apache.lucene.search.TopFieldCollector;
import org.apache.lucene.search.TopScoreDocCollector;
import org.apache.lucene.util.BytesRef;
import org.apache.solr.common.SolrException;
import org.apache.solr.handler.component.QueryElevationComponent;
import org.apache.solr.request.SolrRequestInfo;

/* A TopDocsCollector used by reranking queries. */
public class ReRankCollector extends TopDocsCollector {

  final private TopDocsCollector  mainCollector;
  final private IndexSearcher searcher;
  final private int reRankDocs;
  final private int length;
  final private Map<BytesRef, Integer> boostedPriority;
  final private Rescorer reRankQueryRescorer;


  public ReRankCollector(int reRankDocs,
      int length,
      Rescorer reRankQueryRescorer,
      QueryCommand cmd,
      IndexSearcher searcher,
      Map<BytesRef, Integer> boostedPriority) throws IOException {
    super(null);
    this.reRankDocs = reRankDocs;
    this.length = length;
    this.boostedPriority = boostedPriority;
    Sort sort = cmd.getSort();
    if(sort == null) {
      this.mainCollector = TopScoreDocCollector.create(Math.max(this.reRankDocs, length));
    } else {//如果有sort ,也是第一轮查询时有用
      sort = sort.rewrite(searcher);
      this.mainCollector = TopFieldCollector.create(sort, Math.max(this.reRankDocs, length), false, true, true);
    }
    this.searcher = searcher;
    this.reRankQueryRescorer = reRankQueryRescorer;
  }

  public int getTotalHits() {
    return mainCollector.getTotalHits();
  }

  @Override
  public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
    return mainCollector.getLeafCollector(context);
  }

  @Override
  public boolean needsScores() {
    return true;
  }
 //第一个参数表示分页查找时的偏移量，也就是从第几个开始返回，第二个参数表示返回多少个。第一个参数往往是0
  public TopDocs topDocs(int start, int howMany) {

    try {
      //默认情况下，howMany=length=10,而reRankDocs是url参数中指定的
      TopDocs mainDocs = mainCollector.topDocs(0,  Math.max(reRankDocs, length));
      //第一轮搜索结果为0的话，直接返回
      if(mainDocs.totalHits == 0 || mainDocs.scoreDocs.length == 0) {
        return mainDocs;
      }

      ScoreDoc[] mainScoreDocs = mainDocs.scoreDocs;
      ScoreDoc[] reRankScoreDocs = new ScoreDoc[Math.min(mainScoreDocs.length, reRankDocs)];//粗查的文档数量<重排的文档数量时，要取最小值
      System.arraycopy(mainScoreDocs, 0, reRankScoreDocs, 0, reRankScoreDocs.length);

      mainDocs.scoreDocs = reRankScoreDocs;

      TopDocs rescoredDocs = reRankQueryRescorer
          .rescore(searcher, mainDocs, mainDocs.scoreDocs.length);

      //Lower howMany to return if we've collected fewer documents.
      howMany = Math.min(howMany, mainScoreDocs.length);

      if(boostedPriority != null) {
        SolrRequestInfo info = SolrRequestInfo.getRequestInfo();
        Map requestContext = null;
        if(info != null) {
          requestContext = info.getReq().getContext();
        }

        IntIntHashMap boostedDocs = QueryElevationComponent.getBoostDocs((SolrIndexSearcher)searcher, boostedPriority, requestContext);

        Arrays.sort(rescoredDocs.scoreDocs, new BoostedComp(boostedDocs, mainDocs.scoreDocs, rescoredDocs.getMaxScore()));
      }

      if(howMany == rescoredDocs.scoreDocs.length) {
        return rescoredDocs; // Just return the rescoredDocs
      } else if(howMany > rescoredDocs.scoreDocs.length) {
        //We need to return more then we've reRanked, so create the combined page.
        ScoreDoc[] scoreDocs = new ScoreDoc[howMany];
        System.arraycopy(mainScoreDocs, 0, scoreDocs, 0, scoreDocs.length); //lay down the initial docs
        System.arraycopy(rescoredDocs.scoreDocs, 0, scoreDocs, 0, rescoredDocs.scoreDocs.length);//overlay the re-ranked docs.
        rescoredDocs.scoreDocs = scoreDocs;
        return rescoredDocs;
      } else {
        //We've rescored more then we need to return.
        ScoreDoc[] scoreDocs = new ScoreDoc[howMany];
        System.arraycopy(rescoredDocs.scoreDocs, 0, scoreDocs, 0, howMany);
        rescoredDocs.scoreDocs = scoreDocs;
        return rescoredDocs; //返回重排后的文档
      }
    } catch (Exception e) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, e);
    }
  }

  public static class BoostedComp implements Comparator {
    IntFloatHashMap boostedMap;

    public BoostedComp(IntIntHashMap boostedDocs, ScoreDoc[] scoreDocs, float maxScore) {
      this.boostedMap = new IntFloatHashMap(boostedDocs.size()*2);

      for(int i=0; i<scoreDocs.length; i++) {
        final int idx;
        if((idx = boostedDocs.indexOf(scoreDocs[i].doc)) >= 0) {
          boostedMap.put(scoreDocs[i].doc, maxScore+boostedDocs.indexGet(idx));
        } else {
          break;
        }
      }
    }

    public int compare(Object o1, Object o2) {
      ScoreDoc doc1 = (ScoreDoc) o1;
      ScoreDoc doc2 = (ScoreDoc) o2;
      float score1 = doc1.score;
      float score2 = doc2.score;
      int idx;
      if((idx = boostedMap.indexOf(doc1.doc)) >= 0) {
        score1 = boostedMap.indexGet(idx);
      }

      if((idx = boostedMap.indexOf(doc2.doc)) >= 0) {
        score2 = boostedMap.indexGet(idx);
      }

      return -Float.compare(score1, score2);
    }
  }
}
