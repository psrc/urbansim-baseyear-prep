# Script to create parcels and buildings tables from Snohomish assessor data
# for the use in urbansim 
# It generates 4 tables: 
#    urbansim_parcels, urbansim_buildings: contain records that are found in BY2018
#.   urbansim_parcels_all, urbansim_buildings_all: all records regardless if they are found in BY2018
#
# Hana Sevcikova, last update 01/22/2024
#

library(data.table)

county <- "Snohomish"
county.id <- 61

#data.dir <- file.path("~/e$/Assessor23", county) # path to the Assessor text files
data.dir <- "Snohomish_data" # Hana's local path
misc.data.dir <- "data" # path to the BY2023/data folder
write.result <- TRUE # it will overwrite the existing tables urbansim_parcels & urbansim_buildings

if(write.result) source("mysql_connection.R")

###############
# Load all data
###############

# parcels & tax accounts
parcels.23to18 <- fread(file.path(data.dir, "parcels23_snoh_to_2018_parcels.csv")) # contains assignments of stacked parcels
full.parcels <- fread(file.path(data.dir, "parcels23_snoh_to_2018_parcels_short.txt")) # contains all parcels attributes

# main data (do we need it?)
#main_data <- fread(file.path(data.dir, "MainData.txt"))

# exemptions
exemptions <- fread(file.path(data.dir, "Exemptions.txt"))

# reclass tables
lu_reclass <- fread(file.path(misc.data.dir, "land_use_generic_reclass_2023.csv"))
bt_reclass <- fread(file.path(misc.data.dir, "building_use_generic_reclass_2023.csv"))

# buildings
raw_buildings <- fread(file.path(data.dir, "improvement.txt"))

# RVs to remove
rvs <- fread(file.path(data.dir, "snoh_rv_units.csv"))

###################
# Process parcels
###################

# Snohomish ID: 
#  - pin23 (base parcel), pin23_orig (all parcels)
#  - full.parcels: parcel_id (all parcels)
# urbansim ID: pin

cat("\nProcessing Snohomish parcels\n=========================\n")

# make column names lowercase
colnames(parcels.23to18) <- tolower(colnames(parcels.23to18))
colnames(full.parcels) <- tolower(colnames(full.parcels))
#colnames(main_data) <- tolower(colnames(main_data))

parcels.23to18[, `:=`(new_parcel_id = as.character(pin23), parcel_id = as.character(pin23_orig))]
full.parcels[, parcel_id := as.character(parcel_id)]
exemptions[, parcel_id := as.character(parcel_number)]

# remove records that are not in full.parcels (should be zero!)
if(nrow((miss <- parcels.23to18[! parcel_id %in% full.parcels[, parcel_id]])) > 0){
    warning("The following parcels are not contained in the full set of parcels: ", 
            paste(miss[, parcel_id], collapse = ", "))
    parcels.23to18 <- parcels.23to18[!parcel_id %in% miss[, parcel_id]]
}

# set base_pin_flag for all records
parcels.23to18[, `:=`(Npcl = .N, Nbase = sum(base_pin_flag)), by = "new_parcel_id"]
parcels.23to18[Npcl == 1 & Nbase == 0, `:=`(base_pin_flag = 1, Nbase = 1)]
parcels.23to18[Nbase == 0 & new_parcel_id > 0, 
               base_pin_flag := c(1, base_pin_flag[-1]), by = "new_parcel_id"] # set the flag to one for each first row in the group

full.parcels[parcels.23to18, `:=`(new_parcel_id = i.new_parcel_id), on = "parcel_id"]
exemptions[parcels.23to18, new_parcel_id := i.new_parcel_id, on = "parcel_id"]
full.parcels[, exemption := parcel_id %in% exemptions[, parcel_id]]

# join with reclass table
full.parcels[lu_reclass[county_id == county.id], land_use_type_id := i.land_use_type_id, 
             on = c(usecode = "county_land_use_description")]

cat("\nMatched", nrow(full.parcels[!is.na(land_use_type_id)]), "records with land use reclass table")
cat("\nUnmatched: ", nrow(full.parcels[is.na(land_use_type_id)]), "records.")
if(nrow(misslu <- full.parcels[is.na(land_use_type_id) & !is.na(usecode), .N, by = "usecode"]) > 0){
    cat("\nThe following land use codes were not found:\n")
    print(misslu[order(usecode)])
} else cat("\nAll land use codes matched.")

# assign land use type of the base parcel
full.parcels[is.na(land_use_type_id), land_use_type_id := 0]
parcels.23to18[full.parcels, land_use_type_id := i.land_use_type_id, on = "parcel_id"]
full.parcels[parcels.23to18[base_pin_flag == 1], `:=`(base_land_use_type_id = i.land_use_type_id), 
             on = "new_parcel_id"]
full.parcels[is.na(base_land_use_type_id), base_land_use_type_id := ifelse(is.na(land_use_type_id), 0, land_use_type_id)]
full.parcels[, `:=`(N = .N, has_valid_base_land_use_type = any(base_land_use_type_id > 0)), by = "new_parcel_id"]
full.parcels[base_land_use_type_id == 0 & N > 1, base_land_use_type_id := ifelse(has_valid_base_land_use_type, NA, land_use_type_id)]


# aggregate land value
prep_parcels <- full.parcels[!is.na(parcel_id), .(land_value = sum(mklnd), 
                                                    improvement_value = sum(mkimp),
                                                    total_value = sum(mkttl),
                                                    exemption = as.integer(any(exemption)),
                                                    land_use_type_id = max(base_land_use_type_id, na.rm = TRUE),
                                                    use_code_dif = any(land_use_type_id != max(base_land_use_type_id, na.rm = TRUE)),
                                                    N = .N
                                                    ), by = "new_parcel_id"] 
prep_parcels[is.na(land_use_type_id), land_use_type_id := 0]
# for now we use the land use type of the base parcel
# but these parcels have differences in land use types:
prep_parcels[!is.na(new_parcel_id) & new_parcel_id > 0 & N > 1 & use_code_dif == TRUE][order(-N)]

# get sqft and x & y coordinates
prep_parcels[parcels.23to18[base_pin_flag == 1], `:=`(pin = i.pin, 
        gross_sqft = i.gis_sqft, x_coord_sp = i.point_x_int, y_coord_sp = i.point_y_int),
             on = "new_parcel_id"]

# prep_parcels <- parcels.nodupl[, .(parcel_id = pin, parcel_number = parcel_id, 
#                                    parcel_lrsn = lrsn,
#                                    land_value = mklnd, improvement_value = mkimp,
#                                    total_value = mkttl,
#                                    gross_sqft = round(gis_sq_ft),
#                                    x_coord_sp = point_x, y_coord_sp = point_y,
#                                    usecode, 
#                                    exemption = as.integer(parcel_id %in% exemptions[, parcel_number]))]



# construct final parcels 
# (if no additional columns or other cleaning needed then it's just a copy of prep_parcels)
parcels_final <- prep_parcels[, .(parcel_id = pin, parcel_number = new_parcel_id, 
                                  land_value, improvement_value, total_value, gross_sqft,
                                  x_coord_sp, y_coord_sp, land_use_type_id, exemption,
                                  county_id = county.id)]

cat("\nTotal all:", nrow(parcels_final), "parcels")
cat("\nAssigned to 2018:", nrow(parcels_final[!is.na(parcel_id) & parcel_id != 0]), "parcels")
cat("\nDifference:", nrow(parcels_final) - nrow(parcels_final[!is.na(parcel_id) & parcel_id != 0]))

###################
# Process buildings
###################

cat("\n\nProcessing Snohomish buildings\n=========================\n")

# make column names lowercase
colnames(raw_buildings) <- tolower(colnames(raw_buildings))

prep_buildings <- raw_buildings[, .(building_id = 1:nrow(raw_buildings),
                                    parcel_number_orig = as.character(pin), 
                                    imprtype, usecode, usedesc,
                                    bldgtype, stories, yrbuilt, finsize,
                                    numberrooms, numbedrms, propext)]

# assign new_parcel_id
prep_buildings[parcels.23to18, `:=`(new_parcel_id = i.new_parcel_id, 
                                    land_use_type_id = i.land_use_type_id),
                                    on = c(parcel_number_orig = "parcel_id")]

# join with building reclass table
prep_buildings[bt_reclass[county_id == county.id], building_type_id := i.building_type_id, 
               on = c(usecode = "county_building_use_code")]


# Calculate improvement value proportionally to the sqft (using the original parcels)
prep_buildings[full.parcels, `:=`(total_improvement_value = i.mkimp, 
                                  pin = i.pin),
               on = c(parcel_number_orig = "parcel_id")]
prep_buildings[, `:=`(sqft_tmp = pmax(1, finsize, na.rm = TRUE))]
prep_buildings[, `:=`(total_sqft = sum(sqft_tmp), count = .N), by = "parcel_number_orig"]
prep_buildings[, `:=`(improvement_value = round(sqft_tmp/total_sqft * total_improvement_value))]

# remove RVs in three areas: Port Susan, Gold Bar, Lake Connor
nbld <- nrow(prep_buildings)
prep_buildings <- prep_buildings[! parcel_number_orig %in% rvs[, as.character(pin23_orig)]]
cat("\n", nbld - nrow(prep_buildings), " RVs removed.\n")
               
# As there are no mobile homes in the reclass table,
# set building type to mobile home if on mobile home land use type and if residential use code
prep_buildings[land_use_type_id == 13 & 
                   usecode %in% c("1", "", "5", "2", "3", "APART", "0", "70", "11"), 
               building_type_id := 11]

# impute residential units
prep_buildings[, residential_units := 0]
prep_buildings[building_type_id %in% c(19, 11), residential_units := 1] # SF, MH
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
if(nrow(missbt <- prep_buildings[is.na(building_type_id), .N, by = c("usecode", "usedesc")]) > 0){
    cat("\nThe following building codes were not found:\n")
    print(missbt[order(-N)])
} else cat("\nAll building use codes matched.")

# aggregate DUs for Condo buildings on the same parcel
prep_buildings_condo <- NULL
for(bt in c(4)){
    prep_buildings_condo <- rbind(prep_buildings_condo,
                               prep_buildings[building_type_id == bt,
                                    .(building_id = building_id[1], building_type_id = bt, gross_sqft = sum(finsize),
                                      sqft_per_unit = round(sum(finsize)/sum(residential_units)),
                                      year_built = round(mean(yrbuilt)),
                                      residential_units = sum(residential_units),
                                      non_residential_sqft = 0,
                                      improvement_value = sum(improvement_value), use_code = usecode[1],
                                      stories = NA), by = "new_parcel_id"]
                        )
}
setnames(prep_buildings_condo, "new_parcel_id", "parcel_number")

# assemble columns for final buildings table by joining residential and non-res part
# TODO: for non-res buildings is gross_sqft the same as non_residential_sqft?
buildings_final <- rbind(
    prep_buildings[building_type_id %in% c(12, 19, 11)
    , .(building_id, parcel_number = new_parcel_id, building_type_id, gross_sqft = finsize, 
        sqft_per_unit = round(finsize/residential_units),
        year_built = yrbuilt, residential_units, non_residential_sqft = 0,
        improvement_value, use_code = usecode, stories
        )],
    prep_buildings_condo,
    prep_buildings[! building_type_id %in% c(4, 12, 19, 11)
    , .(building_id, parcel_number = new_parcel_id, building_type_id, gross_sqft = finsize, 
        sqft_per_unit = 1, year_built = yrbuilt, residential_units = 0,
        non_residential_sqft = finsize, improvement_value, use_code = usecode, 
        stories
        )])
# add urbansim parcel_id
buildings_final[parcels_final, parcel_id := i.parcel_id, on = "parcel_number"]

# remove buildings that cannot be assigned to parcels
nbld <- nrow(buildings_final)
buildings_final <- buildings_final[parcel_number %in% parcels_final[, parcel_number]]
cat("\nDropped ", nbld - nrow(buildings_final), " buildings due to missing parcels.")

# rename parcel_number column
setnames(buildings_final, "parcel_number", "parcel_id_fips") 
# column order
setcolorder(buildings_final, c("building_id", "parcel_id", "parcel_id_fips"))

# remove columns from parcels_final and rename parcel_number
parcels_final[, `:=`(improvement_value = NULL, total_value = NULL)]
setnames(parcels_final, "parcel_number", "parcel_id_fips") 

# replace NAs with zeros
for(col in c("gross_sqft", "year_built", "non_residential_sqft")){
    buildings_final[[col]][is.na(buildings_final[[col]])] <- 0
}

# clean stories column
# check with buildings_final[, .N, by = "stories"]
buildings_final[stories %in% c("5353", "53", "053", "2022", "", "\x80P@"), stories := NA]
buildings_final[stories %in% c("2.,0", "2+", "2..0", "2.-0", "2,0"), stories := "2"]
buildings_final[stories == "1,5", stories := "1.5"]
buildings_final[, stories := as.numeric(stories)]

cat("\nTotal all: ", nrow(buildings_final), "buildings")
cat("\nAssigned to 2018:", nrow(buildings_final[!is.na(parcel_id) & parcel_id != 0]), "buildings")
cat("\nDifference:", nrow(buildings_final) - nrow(buildings_final[!is.na(parcel_id) & parcel_id != 0]), "\n")


if(write.result){
    # write results
    fwrite(parcels_final, file = "urbansim_parcels_all_snohomish.csv")
    fwrite(buildings_final, file = "urbansim_buildings_all_snohomish.csv")
    fwrite(parcels_final[!is.na(parcel_id) & parcel_id != 0], file = "urbansim_parcels_snohomish.csv")
    fwrite(buildings_final[!is.na(parcel_id) & parcel_id != 0], file = "urbansim_buildings_snohomish.csv")
    db <- paste0(tolower(county), "_2023_parcel_baseyear")
    connection <- mysql.connection(db)
    dbWriteTable(connection, "urbansim_parcels", parcels_final[!is.na(parcel_id) & parcel_id != 0], overwrite = TRUE, row.names = FALSE)
    dbWriteTable(connection, "urbansim_buildings", buildings_final[!is.na(parcel_id) & parcel_id != 0], overwrite = TRUE, row.names = FALSE)
    dbWriteTable(connection, "urbansim_parcels_all", parcels_final, overwrite = TRUE, row.names = FALSE)
    dbWriteTable(connection, "urbansim_buildings_all", buildings_final, overwrite = TRUE, row.names = FALSE)
    DBI::dbDisconnect(connection)
}
