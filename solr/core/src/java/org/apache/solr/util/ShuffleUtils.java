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

package org.apache.solr.util;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.stream.Collectors;

import org.apache.solr.handler.component.ShardDoc;

/**
 * @author gaosong
 * @since 2019/1/22
 */
public class ShuffleUtils {

    /**
     * @param shardDocList   待打散的商品数
     * @param limitInSegment 每组限制同一个档口不能超过的商品数
     * @param segmentSize    每组的商品数
     * @param segments       对多少组商品进行打散
     */
    public static void shuffle(List<ShardDoc> shardDocList, int limitInSegment, int segmentSize, int segments) {
        for (int i = 0; i < segments; i++) {
            //按分数倒序排列
            shardDocList.sort((o1, o2) -> ((Comparable) o2.score).compareTo(o1.score));
            int begin = i * segmentSize;
            int end = (i + 1) * segmentSize;

            Map<Object, Long> groupCountMap = shardDocList.subList(begin, end).stream().collect(
                Collectors.groupingBy(o -> getShopId(o), Collectors.counting()));

            Map<Object, List<ShardDoc>> groupItemsMap = shardDocList.subList(begin, end).
                stream().collect(Collectors.groupingBy(o -> getShopId(o)));

            // 统计分组名下元素个数大于边界的分组名,是档口列表
            List<Object> badCases = groupCountMap.entrySet().stream().filter(o -> o.getValue() > limitInSegment).map(o -> o.getKey()).collect(Collectors.toList());

            //下个分组的第一个文档
            int nextSegmentPos = end;
            //当前分组的最后一个文档分数
            float minScoreInFirstStage = shardDocList.get(nextSegmentPos - 1).score;
            //下个分组的第一个文档分数
            float maxScoreInSecondStatge = shardDocList.get(nextSegmentPos).score;
            //取两者之间的平均值
            float midScoreInStage = (minScoreInFirstStage + maxScoreInSecondStatge) / 2;


            //要进行比对的文档位置
            int nextComparePos = end;
            //进行
            int reduceTimes = 0;
            int riseTimes = 0;
            for (Object badCase : badCases) {
                List<ShardDoc> badItemsInCase = groupItemsMap.get(badCase);
                for (int j = limitInSegment; j < badItemsInCase.size(); j++) {
                    String shopId2 = getShopId(shardDocList.get(nextComparePos));

                    while (groupCountMap.getOrDefault(shopId2, 0L) >= limitInSegment) {
                        nextComparePos++;
                        shopId2 = getShopId(shardDocList.get(nextComparePos));
                    }

                    riseTimes++;
                    shardDocList.get(nextComparePos).score = midScoreInStage + 0.00000001f * (100 - riseTimes);

                    //重要，因为分数的提升，统计 档口->商品数 map时，要加上
                    groupCountMap.merge(shopId2, 1L, (x, y) -> x + y);

                    reduceTimes++;
                    ShardDoc shardDoc1 = badItemsInCase.get(j);
                    shardDoc1.score = midScoreInStage - 0.00000001f * reduceTimes;

                    nextComparePos++;
                }
            }
        }
        shardDocList.sort((o1, o2) -> ((Comparable) o2.score).compareTo(o1.score));
    }

    private static String getShopId(ShardDoc doc) {
        Object id = doc.id;
        return ((String) id).split("-")[1];
    }
}



