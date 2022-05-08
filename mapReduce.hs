import Data.Map (Map, lookup, fromList, empty,insertWith,mapWithKey,filterWithKey,toList)

mapPerKey :: Ord k2 =>  (k1 -> v1 -> [(k2,v2)]) -> Map k1 v1 -> [(k2,v2)]
mapPerKey mAP = concat . map (uncurry mAP) . toList 

groupByKey :: Ord k2 => [(k2,v2)] -> Map k2 [v2]
groupByKey = foldl insert empty where insert dict (k2,v2) = insertWith (++) k2 [v2] dict



reducePerKey :: Ord k2 => (k2 -> [v2] -> Maybe v3) -> Map k2 [v2] -> Map k2 v3
reducePerKey f = mapWithKey unJust . filterWithKey isJust . (mapWithKey f) where  
isJust k (Just v) = True 
isJust k Nothing = False 
unJust k (Just v) = v

mapReduce :: Ord k2 => (k1 -> v1 -> [(k2,v2)]) -> (k2 -> [v2] -> Maybe v3) -> Map k1 v1 -> Map k2 v3 
mapReduce mAP rEDUCE = (reducePerKey rEDUCE) . groupByKey . (mapPerKey mAP)


-- Page Rank
pageRankMap :: (Integer, [Integer]) -> Maybe Double -> [(Integer, Double)]
pageRankMap outgoing (Just value) = map (\x -> (x, value/d)) (snd outgoing)
        where d = fromIntegral $ length (snd outgoing)
pageRankMap outgoing Nothing = []

pageRankReduce :: Integer -> [Double] -> Maybe Double
pageRankReduce node values = Just $ sum values

graph = [(1,[2,3]),  (2,[3,4]), (3,[4]), (4,[1])]

initialResult = fromList $ map (\x -> (x, 0.25)) [1..4]

toGraph graph result = fromList $ map (\x -> (x, Data.Map.lookup (fst x) result)) graph

pageRank = mapReduce pageRankMap pageRankReduce

pgRankN graph steps = iterate (toGraph graph . pageRank) (toGraph graph initialResult) !! steps