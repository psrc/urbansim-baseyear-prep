# Must create an object hhtots which is a data table with columns:
# county_id, tract, block_group, HH

library(data.table)
hhtots <- read.table(file.path(data.dir, "OFMPopHHblockgroups.csv"), sep=",", header=TRUE)[, c("county_id", "tract", "block_group", "HU14")]
colnames(hhtots)[4] <- "HH"
hhtots <- data.table(hhtots)
