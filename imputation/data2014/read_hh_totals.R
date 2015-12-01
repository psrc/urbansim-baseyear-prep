# Must create an object hhtots which is a data table with columns:
# county_id, tract, block_group, HH
hhs14 <- data.table(read.table(file.path(data.dir, "PSRC_SAEP_POST_BLK2010.csv"), sep=',', header=TRUE))
geoattr <- hhs14 %>% use_series(geoid)  %>% as.character 
hhs14[,'county_id'] <- geoattr %>% substr(4, 5) %>% as.integer
hhs14[,'tract'] <- geoattr %>% substr(6, 11)  %>% as.integer
hhs14[,'block_group'] <- geoattr %>% substr(12, 12)  %>% as.integer
# aggregate over blocks
hhtots <- hhs14[, list(HH=sum(hh14)), by=.(county_id, tract, block_group)]
