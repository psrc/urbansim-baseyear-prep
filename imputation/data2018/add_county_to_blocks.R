library(data.table)

cb <- fread("census_blocks.csv")
cb[, county_id := as.integer(substr(census_2010_block_id, 4,5))]
fwrite(cb, file = "census_blocks.csv")
