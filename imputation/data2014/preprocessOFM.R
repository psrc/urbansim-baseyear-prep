# OFM data from December 2015. Cannot be distributed on the block level -> aggregated to block group in order to put it on GitHub
# Creates a file that is read by read_hh_totals.R which is called from match_hhs_to_census.R

library(data.table)
library(magrittr)
hhs14 <- data.table(read.table(file.path(data.dir, "OFM_SAEP_POST_BLK2010_2014_15.csv"), sep=',', header=TRUE)) 
geoattr <- hhs14 %>% use_series(GEOID10)  %>% as.character 
hhs14[,'county_id'] <- geoattr %>% substr(4, 5) %>% as.integer
hhs14[,'tract'] <- geoattr %>% substr(6, 11)  %>% as.integer
hhs14[,'block_group'] <- geoattr %>% substr(12, 12)  %>% as.integer
# aggregate over blocks
hhtots <- as.data.frame(hhs14[, list(TOTPOP14=sum(TOTPOP14), HHPOP14=sum(HHPOP14), GQPOP14=sum(GQPOP14), HHS14=sum(HHS14), HU14=sum(HU14), 
									TOTPOP15=sum(TOTPOP15), HHPOP15=sum(HHPOP15), GQPOP15=sum(GQPOP15), HHS15=sum(HHS15), HU15=sum(HU15)), 
								by=.(county_id, tract, block_group)]) 
write.table(hhtots, file="OFMPopHHblockgroups.csv", sep=",", row.names=FALSE, quote=FALSE)
