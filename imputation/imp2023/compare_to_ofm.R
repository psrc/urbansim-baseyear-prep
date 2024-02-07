library(data.table)

data.year <- 2023
date <- "20240205"
data.dir <- file.path("..", paste0("data", data.year))
parcels18 <- readRDS("/Users/hana/psrc/R/shinyserver/baseyear2018explorer/data/parcels.rds")
bldgs <- fread(file.path(data.dir, paste0("buildings_imputed_phase1_", date, ".csv")))
bgs <- fread(file.path(data.dir, "census_2020_blocks.csv"))

ofm <- fread(file.path(data.dir, "saep_block20.csv"))[COUNTYFP %in% c(33, 35, 53, 61)]
colnames(ofm) <- tolower(colnames(ofm))
setnames(ofm, "countyfp", "county_id")

bldgs[parcels18, `:=`(census_2020_block_id = i.census_2020_block_id, county_id = i.county_id), on = "parcel_id"]

DUblock <- bldgs[, .(residential_units = sum(residential_units)), by = .(county_id, census_2020_block_id)]
DUblock[bgs, geoid20 := i.census_2020_block_geoid, on = "census_2020_block_id"]

DUwOFM <- merge(DUblock, ofm[, .(county_id, geoid20, ofm = round(hu2023))], by = c("county_id", "geoid20"), 
                all = TRUE)
DUwOFM[is.na(residential_units), residential_units := 0]
DUwOFM[, dif := residential_units - ofm]
DUwOFM <- DUwOFM[residential_units > 0 & ofm > 0]
DUwOFMd <- DUwOFM[abs(dif) > 0]
DUwOFMd[order(-abs(dif))] 

hist(DUwOFM[, dif], breaks = 100)
hist(DUwOFMd[, dif], breaks = 100)

plot(DUwOFMd[, residential_units], DUwOFMd[, ofm])
abline(0,1)     
# parcels for exploring in base year explorer
# paste(parcels18[census_2020_block_id == 32067, parcel_id], collapse = ",")
bldgs[parcel_id == 1016012 & residential_units > 0, ]
