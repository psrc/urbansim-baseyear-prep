library(data.table)

setwd("~/psrc/urbansim-baseyear-prep/imputation/imp2023")

save.into.mysql <- TRUE
save.as.csv <- TRUE
bld.outname <- "buildings_no_capacity"

bld.file.name <- "buildings_imputed_phase3_lodes_20240904.csv" # latest buildings table
data.year <- 2023 # data files will be taken from "../data{data.year}"
data.dir <- file.path("..", paste0("data", data.year))
bld <- fread(file.path(data.dir, bld.file.name))

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

bld.clean <- bld[, .(building_id, parcel_id, gross_sqft, non_residential_sqft, sqft_per_unit, 
                     year_built, residential_units, improvement_value, building_type_id, 
                     stories, land_area)]

# export buildings table
if(save.into.mysql) {
  source("../../collect_parcels_buildings/BY2023/mysql_connection.R")
  db <- "psrc_2023_parcel_baseyear"
  connection <- mysql.connection(db)
  dbWriteTable(connection, bld.outname, bld.clean, overwrite = TRUE, row.names = FALSE)
  DBI::dbDisconnect(connection)
}
if(save.as.csv) {
  fwrite(bld.clean, file.path(data.dir, paste0(bld.outname, ".csv")))
}


