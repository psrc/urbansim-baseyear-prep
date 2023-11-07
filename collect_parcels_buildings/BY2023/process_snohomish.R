# Script to create parcels and buildings tables from Snohomish assessor data
# for the use in urbansim 
#
# Hana Sevcikova, last update 11/07/2023
#

library(data.table)

county <- "Snohomish"

data.dir <- file.path("~/e$/Assessor23", county) # path to the Assessor text files
#data.dir <- "Snohomish_data" # Hana's local path
misc.data.dir <- "data" # path to the BY2023/data folder
write.result.to.mysql <- TRUE # it will overwrite the existing tables urbansim_parcels & urbansim_buildings

if(write.result.to.mysql) source("mysql_connection.R")

###############
# Load all data
###############

# parcels & tax accounts
#parcels.23to18 <- fread(file.path(data.dir, "allparcels23_snoh_to_2018_parcels.txt"))
parcels.23to18 <- fread(file.path(data.dir, "parcels23_snoh_to_2018_parcels_short.txt"))

# main data (do we need it?)
#main_data <- fread(file.path(data.dir, "MainData.txt"))

# exemptions
exemptions <- fread(file.path(data.dir, "Exemptions.txt"))

# reclass tables
lu_reclass <- fread(file.path(misc.data.dir, "land_use_generic_reclass_2023.csv"))
bt_reclass <- fread(file.path(misc.data.dir, "building_use_generic_reclass_2023.csv"))

# buildings
raw_buildings <- fread(file.path(data.dir, "improvement.txt"))

###################
# Process parcels
###################
# make column names lowercase
colnames(parcels.23to18) <- tolower(colnames(parcels.23to18))
#colnames(main_data) <- tolower(colnames(main_data))

# remove duplicates (if any) and records with parcel_id = NA
parcels.nodupl <- parcels.23to18[!is.na(parcel_id)][!duplicated(parcel_id)]
cat("\nNumber of records removed from parcels (either NA or duplicates): ", 
    nrow(parcels.23to18) - nrow(parcels.nodupl), "\n")

prep_parcels <- parcels.nodupl[, .(parcel_id = pin, parcel_number = parcel_id, 
                                   parcel_lrsn = lrsn,
                                   land_value = mklnd, improvement_value = mkimp,
                                   total_value = mkttl,
                                   gross_sqft = round(poly_area),
                                   x_coord_sp = point_x, y_coord_sp = point_y,
                                   usecode, 
                                   exemption = as.integer(parcel_id %in% exemptions[, parcel_number]))]

# join with reclass table
prep_parcels[lu_reclass[county_id == 61], land_use_type_id := i.land_use_type_id, 
              on = c(usecode = "county_land_use_description")]

cat("\nMatched", nrow(prep_parcels[!is.na(land_use_type_id)]), "records with land use reclass table")
cat("\nUnmatched: ", nrow(prep_parcels[is.na(land_use_type_id)]), "records.")
cat("\nThe following codes were not found:\n")
print(prep_parcels[is.na(land_use_type_id) & !is.na(usecode), .N, by = "usecode"][order(usecode)])

# construct final parcels 
# (if no additional columns or other cleaning needed then it's just a copy of prep_parcels)
parcels_final <- copy(prep_parcels)

###################
# Process buildings
###################
# make column names lowercase
colnames(raw_buildings) <- tolower(colnames(raw_buildings))

prep_buildings <- raw_buildings[, .(building_id = 1:nrow(raw_buildings),
                                    parcel_number = pin, lrsn,
                                    imprtype, usecode, usedesc,
                                    bldgtype, stories, yrbuilt, finsize,
                                    numberrooms, numbedrms, propext)]


# join with building reclass table
prep_buildings[bt_reclass[county_id == 61], building_type_id := i.building_type_id, 
               on = c(usecode = "county_building_use_code")]

# Calculate improvement value proportionally to the sqft
prep_buildings[prep_parcels, `:=`(total_improvement_value = i.improvement_value, 
                                  parcel_id = i.parcel_id),
               on = "parcel_number"]
prep_buildings[, `:=`(sqft_tmp = pmax(1, finsize, na.rm = TRUE))]
prep_buildings[, `:=`(total_sqft = sum(sqft_tmp), count = .N), by = "parcel_number"]
prep_buildings[, `:=`(improvement_value = round(sqft_tmp/total_sqft * total_improvement_value))]

# impute residential units
prep_buildings[, residential_units := 0]
prep_buildings[building_type_id == 19, residential_units := 1] # SF
prep_buildings[usecode == 2, residential_units := 2]
prep_buildings[usecode == 3, residential_units := 3]
# for usecode 4 set it between 4 and 6 depending on sqft, using 800sf/DU
prep_buildings[usecode == 4,
               residential_units := pmax(4, pmin(round(sqft_tmp/800), 6))]

# usecode == 5 looks like each record is an individual unit
# use NumberRooms for units for single-record Apartment buildings
prep_buildings[usecode == "APART" & numberrooms > 0, 
               `:=`(residential_units = numberrooms, building_type_id = 12)]

# for the types above, set building type MF if not already set otherwise
prep_buildings[usecode %in% c(2, 3, 4, 5, "APART") & is.na(building_type_id),
               building_type_id := 12]

# Assert 1 unit per each Condo unit and set building type as Condo if not already set otherwise
prep_buildings[usecode %in% c(51, 52, 53, 61, 62), 
               `:=`(residential_units = 1, 
                    building_type_id = ifelse(is.na(building_type_id), 4, building_type_id))]

# use 800sf/DU for the remaining MF
prep_buildings[residential_units == 0 & usecode %in% c(44, 63, 70, 'APART'),
               `:=`(residential_units = pmax(1, round(sqft_tmp/800)))]
# for these use codes, set building type MF if not already set otherwise
prep_buildings[usecode %in% c(44, 63, 70, 'APART')  & is.na(building_type_id),
                    building_type_id := 12]

cat("\nMatched", nrow(prep_buildings[!is.na(building_type_id)]), "records with building reclass table")
cat("\nUnmatched: ", nrow(prep_buildings[is.na(building_type_id)]), "records.")
cat("\nThe following building codes were not found:\n")
print(prep_buildings[is.na(building_type_id), .N, by = "usecode"][order(-N)])

# TODO: there are no mobile homes in the reclass table

# assemble columns for final buildings table by joining residential and non-res part
# TODO: for non-res buildings is gross_sqft the same as non_residential_sqft?
buildings_final <- rbind(
    prep_buildings[building_type_id %in% c(4, 12, 19)
    , .(building_id, parcel_number, building_type_id, gross_sqft = finsize, 
        sqft_per_unit = round(finsize/residential_units),
        year_built = yrbuilt, residential_units, non_residential_sqft = 0,
        improvement_value, use_code = usecode, stories, parcel_lrsn = lrsn
        )],
    prep_buildings[! building_type_id %in% c(4, 12, 19)
    , .(building_id, parcel_number, building_type_id, gross_sqft = finsize, 
        sqft_per_unit = 1, year_built = yrbuilt, residential_units = 0,
        non_residential_sqft = finsize, improvement_value, use_code = usecode, 
        stories, parcel_lrsn = lrsn
        )])
# add urbansim parcel_id
buildings_final[parcels_final, parcel_id := i.parcel_id, on = "parcel_number"]

# remove buildings that cannot be assigned to parcels
nbld <- nrow(buildings_final)
buildings_final <- buildings_final[!is.na(parcel_id)]
cat("\nDropped ", nbld - nrow(buildings_final), " buildings due to missing parcels.\n",
    "Total: ", nrow(buildings_final), "buildings")

# rename parcel_number column
setnames(buildings_final, "parcel_number", "parcel_id_fips") 
# column order
setcolorder(buildings_final, c("building_id", "parcel_id", "parcel_id_fips", "parcel_lrsn"))

# remove columns from parcels_final and rename parcel_number
parcels_final[, `:=`(improvement_value = NULL, total_value = NULL)]
setnames(parcels_final, "parcel_number", "parcel_id_fips") 


# write results
fwrite(parcels_final, file = "urbansim_parcels.csv")
fwrite(buildings_final, file = "urbansim_buildings.csv")

if(write.result.to.mysql){
    db <- paste0(tolower(county), "_2023_parcel_baseyear")
    connection <- mysql.connection(db)
    dbWriteTable(connection, "urbansim_parcels", parcels_final, overwrite = TRUE, row.names = FALSE)
    dbWriteTable(connection, "urbansim_buildings", buildings_final, overwrite = TRUE, row.names = FALSE)
    DBI::dbDisconnect(connection)
}
