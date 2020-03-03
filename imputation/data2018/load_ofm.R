# Reads OFM data

library(data.table)
library(stringr)

ofm <- fread(file.path(data.dir, "OFMPopHHblocks.csv"))[, .(COUNTYFP10, GEOID10, HU2018, GQ2018)]
setnames(ofm, "HU2018", "HU")
setnames(ofm, "GQ2018", "GQ")

ofm[, GEOID10 := as.character(GEOID10)
       ][, `:=`(county_id = COUNTYFP10, 
                tract = str_sub(GEOID10, start = 6, end = 11), 
                block_group = str_sub(GEOID10, start = 12, end = 12), 
                block = str_sub(GEOID10, start = 12, end = 15))
         ][, COUNTYFP10 := NULL]

# merge with urbansim census blocks ids
block.lu <- fread(file.path(data.dir, "census_blocks.csv"))
block.lu <- block.lu[, GEOID10 := as.character(census_2010_block_id)][, 
                        .(GEOID10, census_block_id, census_block_group_id, census_tract_id)]

ofm <- merge(ofm, block.lu, by = "GEOID10", all.x = TRUE)


