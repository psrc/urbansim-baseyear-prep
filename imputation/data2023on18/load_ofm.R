# Reads OFM data

library(data.table)
library(stringr)

ofm <- fread(file.path(data.dir, "saep_block20.csv"))[COUNTYFP %in% c(33, 35, 53, 61)]
colnames(ofm) <- tolower(colnames(ofm))
setnames(ofm, "countyfp", "county_id")
ofm <- ofm[, .(county_id, geoid20, hu2023, gq2023, ohu2023)]

setnames(ofm, "hu2023", "HU")
setnames(ofm, "ohu2023", "HH")
setnames(ofm, "gq2023", "GQ")

ofm[, geoid20 := as.character(geoid20)
       ][, `:=`(tract = str_sub(geoid20, start = 6, end = 11), 
                block_group = str_sub(geoid20, start = 12, end = 12), 
                block = str_sub(geoid20, start = 12, end = 15))
         ]

# merge with urbansim census blocks ids
block.lu <- fread(file.path(data.dir, "census_2020_blocks.csv"))
block.lu <- block.lu[, geoid20 := as.character(census_2020_block_geoid)][, census_2020_block_geoid := NULL]

# merge with urbansim census tract ids
bg.lu <- fread(file.path(data.dir, "census_2020_block_groups.csv"))
tracts.lu <- fread(file.path(data.dir, "census_2020_tracts.csv"))
block.lu <- merge(merge(block.lu, bg.lu, by = "census_2020_block_group_id"),
                  tracts.lu, by = "census_2020_tract_id")

ofm <- merge(ofm, block.lu, by = c("geoid20", "county_id"), all.x = TRUE)


