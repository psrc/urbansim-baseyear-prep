# Script to create parcels and buildings tables from King assessor data
# for the use in urbansim 
#
# Hana Sevcikova, last update 10/30/2023
#

library(data.table)

county <- "King"

data.dir <- file.path("~/e$/Assessor23", county) # path to the Assessor text files
misc.data.dir <- "data" # path to the BY2023/data folder
write.result.to.mysql <- TRUE # it will overwrite the existing tables urbansim_parcels & urbansim_buildings

if(write.result.to.mysql) source("mysql_connection.R")

###############
# Load all data
###############

# parcels & tax accounts
parcels.23to18 <- fread(file.path(data.dir, "parcels23_kin_to_2018_parcels.csv"))
parcel <- fread(file.path(data.dir, "extr_parcel.csv"))

rpacct <- fread(file.path(data.dir, "EXTR_RPAcct_NoName.csv"))

# reclass tables
lu_reclass <- fread(file.path(misc.data.dir, "land_use_generic_reclass_2023.csv"))
bt_reclass <- fread(file.path(misc.data.dir, "building_use_generic_reclass_2023.csv"))


###################
# Process parcels
###################

# remove duplicates and zero urbansim parcel id (pin_1)
parcels.nodupl <- parcels.23to18[!duplicated(pin) | pin_1 == 0]
cat("\nNumber of records removed from parcels (either pin_1 = 0 or duplicates): ", 
    nrow(parcels.23to18) - nrow(parcels.nodupl), "\n")

# make column names lowercase
colnames(rpacct) <- tolower(colnames(rpacct))
rpacct[, acctnbr := as.character(acctnbr)]

colnames(parcel) <- tolower(colnames(parcel))

# It looks like all duplicates in rpacct have zero land and improvement values, 
# thus we can just simply remove the duplicates
# checked with the following which returned zero rows: 
# rpacct[acctnbr %in% rpacct[duplicated(acctnbr), acctnbr]][order(acctnbr)][apprimpsval > 0 | apprlandval > 0]
rpacct.nodupl <- rpacct[!duplicated(acctnbr)]

# construct a column (acctnbr2) that will be used for joining with parcels
rpacct.nodupl[, `:=`(acctnbr2 = substr(acctnbr, 1, nchar(acctnbr) - 2), # remove last two digits
                     exempt = taxstat != "T")][,
                        add0 := 10 - nchar(acctnbr2) # how many leading zeros to add
                ]
zero_templ <- sapply(0:9, function(x) paste0(rep(0, x), collapse = ""))
rpacct.nodupl[, acctnbr2 := paste0(zero_templ[add0+1], acctnbr2)] # add leading zeros

# group records with the same acctnbr2
rpacct_grouped <- rpacct.nodupl[, .(apprlandval = sum(apprlandval), apprimpsval = sum(apprimpsval),
                                    exempt = sum(exempt)), by = "acctnbr2"]

# construct column "pin" in parcel table
parcel[, `:=`(major.char = as.character(major), minor.char = as.character(minor))][
    , `:=`(add0.major = 6 - nchar(major.char), add0.minor = 4 - nchar(minor.char))][
        , `:=`(major.char = paste0(zero_templ[add0.major + 1], major.char),
               minor.char = paste0(zero_templ[add0.minor + 1], minor.char))][
                   , pin := paste0(major.char, minor.char)]


# join with parcels
prep_parcels <- merge(
    merge(parcels.nodupl, rpacct_grouped, by.x = "pin", by.y = "acctnbr2", all.x = TRUE),
    parcel[, .(pin, use_code = as.character(presentuse), propname, proptype, currentzoning, sqftlot, nbrbldgsites)], 
    by = "pin", all.x = TRUE)

cat("\n", nrow(rpacct_grouped[!acctnbr2 %in% prep_parcels[, pin]]), "records from rpacct were not matched with parcels.\n",
    nrow(prep_parcels[is.na(apprlandval)]), "parcel records did not have a record in rpacct.")
cat("\n", nrow(parcel[!pin %in% prep_parcels[, pin]]), "records from extr_parcel were not matched with parcels.\n",
    nrow(prep_parcels[is.na(use_code)]), "parcel records did not have a record in extr_parcel.")


# join with reclass table
prep_parcels[lu_reclass[county_id == 33], land_use_type_id := i.land_use_type_id, 
                    on = c(use_code = "county_land_use_code")]
       
cat("\nMatched", nrow(prep_parcels[!is.na(land_use_type_id)]), "records with land use reclass table")
cat("\nUnmatched: ", nrow(prep_parcels[is.na(land_use_type_id)]), "records.")
cat("\nThe following codes were not found:\n")
print(prep_parcels[is.na(land_use_type_id) & !is.na(use_code), .N, by = "use_code"][order(use_code)])

# construct final parcels                
parcels_final <- prep_parcels[, .(
    parcel_id_fips = pin, land_use_type_id,
    land_value = apprlandval, improvement_value = apprimpsval, exemption = exempt > 0,
    parcel_sqft = poly_area, y_coord_sp = point_y, x_coord_sp = point_x
    )]


###################
# Process buildings
###################

###############
# write results
################
fwrite(parcels_final, file = "urbansim_parcels.csv")
#fwrite(buildings_final, file = "urbansim_buildings.csv")

if(write.result.to.mysql){
    db <- paste0(tolower(county), "_2023_parcel_baseyear")
    connection <- mysql.connection(db)
    dbWriteTable(connection, "urbansim_parcels", parcels_final, overwrite = TRUE, row.names = FALSE)
    #dbWriteTable(connection, "urbansim_buildings", buildings_final, overwrite = TRUE, row.names = FALSE)
    DBI::dbDisconnect(connection)
}
