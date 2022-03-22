# Creates households and buildings tables needed for estimating the HLCM.
# Hana Sevcikova, PSRC
# Updated 02/15/2022
# TODO: update the script to work with column names without info type

library(data.table)
setwd("~/psrc/urbansim-baseyear-prep/hhs_persons/2018/estimation")

# TODO replace large year_built with 0 (in the official US buildings table)

inp.dir <- "urbansimBYinputs"
out.dir <- "urbansimBYestimation"

blds <- fread(file.path(inp.dir, "buildings.csv"))
hhs <- fread(file.path(inp.dir, "households.csv"))
hhest <- fread(file.path("output", "households_for_estimation_raw.csv"))

blds[`year_built:i4` > 2100, `year_built:i4` := 0] # those records should not be there (clean it in the parent table)
nblds <- nrow(blds)

blds <- blds[`year_built:i4` <= 2012 | `building_id:i8` %in% hhest[, previous_building_id]] 
removed <- nblds - nrow(blds) # 61789 buildings removed

# add the "move" and "is_inmigrant" attributes to households
hhs[hhest, `:=`(`move:i4` = i.move, `is_inmigrant:i4` = i.is_inmigrant), on = c(`household_id:i8` = "household_id")]
hhs[is.na(`move:i4`), `move:i4` := 0][is.na(`is_inmigrant:i4`), `is_inmigrant:i4` := 0]

hhsprev <- copy(hhs)
unplace <- nrow(hhs[! `building_id:i4` %in% blds[, `building_id:i8`],]) # 108932
hhsprev[! `building_id:i4` %in% blds[, `building_id:i8`], `building_id:i4` := -1 ]

hhestprev <- hhest[previous_building_id > 0]
hhestprev[, building_id := previous_building_id][, `:=`(previous_building_id = NULL, parcel_id = NULL, previous_parcel_id = NULL,
                                                        census_block_group_id = NULL, hhid = NULL, puma = NULL)]
hhsprev[hhestprev, `building_id:i4`:= i.building_id, on = c(`household_id:i8` = "household_id")]
    
hhest[is.na(previous_building_id), previous_building_id := -1][, `:=`(hhid = NULL, puma = NULL, previous_parcel_id = NULL, 
                                                                      census_block_group_id = NULL)]


# export results
fwrite(hhestprev, file = file.path(out.dir, "households_2013.csv"))
fwrite(hhest, file = file.path(out.dir, "households_for_estimation.csv"))
fwrite(hhsprev, file = file.path(out.dir, "households_2012.csv"))
fwrite(hhs, file = file.path(out.dir, "households.csv"))
fwrite(blds, file = file.path(out.dir, "buildings_2012.csv"))
