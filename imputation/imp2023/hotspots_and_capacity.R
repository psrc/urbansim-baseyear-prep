# This script adds preliminary capacity to the buildings table and 
# processes hotspot changes
# Hana Sevcikova, 2024/10/22

library(data.table)

setwd("~/psrc/urbansim-baseyear-prep/imputation/imp2023")

save.into.mysql <- FALSE
save.as.csv <- FALSE

process.hotspots <- TRUE
process.capacity <- TRUE

bld.file.name <- "buildings_imputed_phase3_lodes_20240904.csv" # latest buildings table
bld.outname <- paste0("buildings_imputed_phase4_capacity_", format(Sys.Date(), '%Y%m%d'))
bld.outname.clean <- "buildings_with_prelim_capacity"
  
# The following datasets are only used if process.capacity is TRUE.
# That section of the code can be run only after the lodes data are unrolled, i.e. pre-parcelized
jobs.file.name <- "../../lodes/jobs_preparcelized_20241104.csv" # use the full path
bspj.name <- "building_sqft_per_job.csv"
parcels.file.name <- "parcels.csv" # we only need zone_id from it

data.year <- 2023 # various datasets will be taken from "../data{data.year}"; the output goes there as well
data.dir <- file.path("..", paste0("data", data.year))

# read buildings
bld <- fread(file.path(data.dir, bld.file.name))

if(process.hotspots) {
    # UW
    parcel <- c(630040)
    id <- bld[parcel_id %in% parcel, building_id] # 570840
    if(! all(id == c(570840))) stop("Check parcel_id or building_id for UW changes")
    bld[building_id %in% id, `:=`(building_type_id = 3)]
  
    # Boeing changes: 2 buildings in 2 main Boeing parcels get a boost of non-res sqft 
    # and building type is changed to industrial
    parcel <- c(1283051, 1283106)
    id <- bld[parcel_id %in% parcel, building_id] # 1213111, 1213151
    if(! all(id == c(1213111, 1213151))) stop("Check parcel_id or building_id for Boeing changes")
    bld[building_id %in% id, `:=`(building_type_id = 8, non_residential_sqft = 4200000)]
    
    # public hospitals - reclassify as government
    parcel <- c(256443,
                630036,
                558613,
                127807,
                239592,
                239596,
                580067,
                7488,
                15373,
                684983,
                580072)
    id <- bld[parcel_id %in% parcel & building_type_id == 7, building_id]
    bld[building_id %in% id, building_type_id := 5]

}

if(process.capacity) {
    # read jobs
    jobs <- fread(jobs.file.name)
    if(any(grepl(":", colnames(jobs)))) # remove ":i4" and ":i1" suffixes if present
        colnames(jobs) <- gsub(":i4|:i1", "", colnames(jobs))
    # summarize by block group
    njobs <- jobs[home_based_status == 0 & ! sector_id %in% c(12, 13), .N, by = "census_block_group_id"]
    # read sqft per job
    bspj <- fread(file.path(data.dir, bspj.name))
    # read parcels (needed for assigning zone_id to buildings)
    pcl <- fread(file.path(data.dir, parcels.file.name))
    # add zone_id column to buildings
    bld[pcl, zone_id := i.zone_id, on = "parcel_id"]
    # determine sqft per job for each building
    bld[bspj, bsqft_per_job := i.building_sqft_per_job, on = c("building_type_id", "zone_id")]
    # bld[!is.na(bsqft_per_job), .N, by = .(building_type_id)][order(building_type_id)] # for checking purposes

    # set mixed-use (use the value for commercial BT)
    bld[bspj[building_type_id == 3], bsqft_per_job := ifelse(building_type_id == 10 & is.na(bsqft_per_job), 
                                                             i.building_sqft_per_job, bsqft_per_job),
        on = "zone_id"]
    
    # set hospitals (use the value for office BT)
    bld[bspj[building_type_id == 13], bsqft_per_job := ifelse(building_type_id == 7 & is.na(bsqft_per_job), 
                                                             i.building_sqft_per_job, bsqft_per_job),
        on = "zone_id"]
    
    # get the max value for each zone
    bspj.max <- bspj[, .(max_value = max(building_sqft_per_job)), by = "zone_id"]
    # set building types 1, 17, and 23 to the max value
    bld[bspj.max, bsqft_per_job := ifelse(building_type_id %in% c(1, 17, 23) & is.na(bsqft_per_job), 
                                          i.max_value, bsqft_per_job), on = "zone_id"]
    
    # compute initial capacity
    bld[, capacity_ini := non_residential_sqft/bsqft_per_job]
    # set the "remaining building types"ignore" records to 0
    bld[is.na(capacity_ini), capacity_ini := 0]
    
    # sum over BG
    bld.cap.bg <- bld[, .(capacity_ini_bg = sum(capacity_ini)), by = "census_block_group_id"]
    
    # join with lodes jobs
    bld.cap.bg[njobs, jobs_bg := i.N, on = "census_block_group_id"]
    
    # compute expansion factor
    bld.cap.bg[, cap_factor := pmax(1, jobs_bg/capacity_ini_bg + 0.05)]
    # summary(bld.cap.bg[is.finite(cap_factor), cap_factor]) # check 
    
    # adjust capacity
    bld[bld.cap.bg[is.finite(cap_factor)], capacity_prelim := round(capacity_ini * i.cap_factor), on = "census_block_group_id"]
    bld[is.na(capacity_prelim), capacity_prelim := 0]
}

bld.clean <- bld[, .(building_id, parcel_id, gross_sqft, non_residential_sqft, sqft_per_unit, 
                     year_built, residential_units, improvement_value, building_type_id, 
                     stories, land_area, capacity_prelim)]

# export buildings table
if(save.into.mysql) {
  source("../../collect_parcels_buildings/BY2023/mysql_connection.R")
  db <- "psrc_2023_parcel_baseyear"
  connection <- mysql.connection(db)
  dbWriteTable(connection, bld.outname, bld, overwrite = TRUE, row.names = FALSE)
  dbWriteTable(connection, bld.outname.clean, bld.clean, overwrite = TRUE, row.names = FALSE)
  DBI::dbDisconnect(connection)
}
if(save.as.csv) {
  fwrite(bld, file.path(data.dir, paste0(bld.outname, ".csv")))
}


