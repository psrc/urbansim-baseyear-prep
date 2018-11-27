# Must create an object hhtots which is a data table with columns:
# county_id, tract, block_group, HH

library(data.table)
library(stringr)

# data.dir <- "C:/Users/CLam/Desktop/urbansim-baseyear-prep/imputation/data2017" # remove when committing
hhtots <- read.table(file.path(data.dir, "OFMPopHHblocks.csv"), sep=",", header=TRUE)[, c("COUNTYFP10", "GEOID10", "HU2017", "GQ2017")]
colnames(hhtots)[3:4] <- c("HH", "GQ")
hhtots <- data.table(hhtots)

hhtots[, GEOID10 := as.character(GEOID10)
       ][, `:=`(county_id = COUNTYFP10, 
                tract = str_sub(GEOID10, start = 6, end = 11), 
                block_group = str_sub(GEOID10, start = 12, end = 12), 
                block = str_sub(GEOID10, start = 12, end = 15))
         ][, COUNTYFP10 := NULL]
block.lu <- fread(file.path(data.dir, "census_blocks.csv")) %>% as.data.table()
block.lu <- block.lu[, GEOID10 := as.character(census_2010_block_id)][, .(GEOID10, census_block_id)]

df <- merge(hhtots, block.lu, by = "GEOID10", all.x = TRUE)

blu <- fread(file.path(data.dir, "all_blocks_usim_id.csv"), quote = "") %>% as.data.table()
blu <- blu[, GEOID10 := str_extract(geoid, "\\d+")][, geoid := NULL][, .(GEOID10, census_block_id2 = census_block_id)]

df[blu, on = c("GEOID10"), census_block_id2 := i.census_block_id2]
df[is.na(census_block_id), census_block_id := census_block_id2][, census_block_id2 := NULL]

hhtots <- df
rm(df)

