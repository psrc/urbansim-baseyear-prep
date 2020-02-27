# Must create an object hhtots which is a data table with columns:
# county_id, tract, block_group, HH

library(data.table)
library(stringr)

# data.dir <- "C:/Users/CLam/Desktop/urbansim-baseyear-prep/imputation/data2017" # remove when committing
hhtots <- read.table(file.path(data.dir, "OFMPopHHblocks.csv"), sep=",", header=TRUE)[, c("COUNTYFP10", "GEOID10", "HU2018", "GQ2018")]
colnames(hhtots)[3:4] <- c("HH", "GQ")
hhtots <- data.table(hhtots)

hhtots[, GEOID10 := as.character(GEOID10)
       ][, `:=`(county_id = COUNTYFP10, 
                tract = str_sub(GEOID10, start = 6, end = 11), 
                block_group = str_sub(GEOID10, start = 12, end = 12), 
                block = str_sub(GEOID10, start = 12, end = 15))
         ][, COUNTYFP10 := NULL]
block.lu <- fread(file.path(data.dir, "census_blocks.csv")) %>% as.data.table()
block.lu <- block.lu[, GEOID10 := as.character(census_2010_block_id)][, 
                                      .(GEOID10, census_block_id, census_block_group_id, census_tract_id)]

df <- merge(hhtots, block.lu, by = "GEOID10", all.x = TRUE)

hhtots <- df
rm(df)

