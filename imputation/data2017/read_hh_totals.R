# Must create an object hhtots which is a data table with columns:
# county_id, tract, block_group, HH

library(data.table)
data.dir <- "C:/Users/CLam/Desktop/urbansim-baseyear-prep/imputation/data2017"
hhtots <- read.table(file.path(data.dir, "OFMPopHHblocks.csv"), sep=",", header=TRUE)[, c("COUNTYFP10", "GEOID10", "HU2017")]
colnames(hhtots)[3] <- "HH"
hhtots <- data.table(hhtots)
