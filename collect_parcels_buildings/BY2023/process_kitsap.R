# Script to create parcels and buildings tables from Kitsap assessor data
# for the use in urbansim 
#
# Hana Sevcikova, last update 10/17/2023
#

library(data.table)

county <- "Kitsap"

data.dir <- file.path("~/e$/Assessor23", county) # path to the Assessor text files
misc.data.dir <- "data" # path to the BY2023/data folder
write.result.to.mysql <- TRUE # it will overwrite the existing tables urbansim_parcels & urbansim_buildings

if(write.result.to.mysql) source("mysql_connection.R")

###############
# Load all data
###############

# parcels tables
parcels <- fread(file.path(data.dir, "parcels.txt"))
parcels.23to18 <- fread(file.path(data.dir, "parcels23_kit_to_2018_parcels.csv"))

# tables flatats, land & main
tax <- fread(file.path(data.dir, "flatats.txt")) # Combined Tax Account Data
land <- fread(file.path(data.dir, "land.txt")) # Land data
main <- fread(file.path(data.dir, "main.txt")) # Real Property Information

# land use and building reclass tables
lu_reclass <- fread(file.path(misc.data.dir, "land_use_generic_reclass_kitsap.csv"))
bt_reclass <- fread(file.path(misc.data.dir, "building_use_generic_reclass_kitsap.csv"))

# buildings
raw_buildings <- fread(file.path(data.dir, "building.txt"))

# mobile homes
mobile_homes <- fread(file.path(data.dir, "mh_fixed.txt"))[, V15 := NULL] # remove extra column

# Valuation
valuation <- fread(file.path(data.dir, "Valuations.txt"))

###################
# Process parcels
###################

# remove duplicates (just take the first record of each duplicate)
parcels.nodupl <- parcels[!duplicated(rp_acct_id)]
parcels.23to18.nodupl <- parcels.23to18[!duplicated(rp_acct_id)]
cat("\nNumber of duplicates removed from parcels: ", nrow(parcels) - nrow(parcels.nodupl), "\n")

# merge parcels
parcels_allinfo <- merge(parcels.nodupl, 
                        parcels.23to18.nodupl[, .(rp_acct_id, pin, point_x, point_y, shape_area)],
                        by = "rp_acct_id")

# remove any duplicates from the three assessors files
tax <- unique(tax)
land <- unique(land)
main <- unique(main)

# extract what is needed and merge together with parcels
# flatats dataset
prep_parcels <- merge(parcels_allinfo,
                      tax[, .(rp_acct_id, levy_code, jurisdict, acres_tax = acres, bldg_value, 
                              land_value, assd_value)],
                      by = "rp_acct_id", all.x = TRUE)
# There are more than 42K parcels in the "tax" dataset that are not in "parcels",
# but only 324 of them have acres > 0. We will ignore them for now.

# land dataset
prep_parcels <- merge(prep_parcels,
                      land[, .(rp_acct_id, acres_land = acres, nbrhd_cd, prop_class, 
                               zone_code, num_dwell, num_other, tot_improv)],
                      by = "rp_acct_id", all.x = TRUE)
cat("\nNumber of records in land that are not available in parcels: ",
    nrow(prep_parcels[!rp_acct_id %in% land[, rp_acct_id]]), "\n")

# main dataset
prep_parcels <- merge(prep_parcels,
                      main[, .(rp_acct_id, acct_stat, tax_status, tax_year)],
                      by = "rp_acct_id", all.x = TRUE)
# There are 427 records in parcels that are not present in land and main.
cat("\nNumber of records in main that are not available in parcels: ",
    nrow(prep_parcels[!rp_acct_id %in% main[, rp_acct_id]]), "\n")

# check for duplicates
cat("\nNumber of duplicates: ", nrow(prep_parcels[, .N, by = "rp_acct_id"][N > 1]), "\n")

parcels_final <- prep_parcels[, .(parcel_id = pin, rp_acct_id, 
                                  land_value, num_dwell,
                                  use_code = prop_class,
                                  exemption = tax_status != "T", 
                                  gross_sqft = round(shape_area), 
                                  y_coord_sp = point_y, x_coord_sp = point_x
                                  )]
# TODO: - Do we need the other columns of prep_parcels? 

# join with land use reclass table
parcels_final[lu_reclass, land_use_type_id := i.land_use_type_id, on = "use_code"]
cat("\nThe following codes were not found:\n")
print(parcels_final[is.na(land_use_type_id) & !is.na(use_code), .N, by = "use_code"][order(use_code)])


###################
# Process buildings
###################

# extract info from the assessors buildings table
# ------------------------
prep_buildings <- raw_buildings[, .(id = 1:nrow(raw_buildings),
                                    improv_typ, 
                                    rp_acct_id, total_sqft = flr_tot_sf,
                                    year_built, stories, bedrooms,
                                    bldg_typ, use_desc
                                    )]

# add mobile homes info
# ------------------
# remove duplicates
mobile_homes.nodupl <- mobile_homes[!duplicated(mobile_homes[, c("rp_acct_id", "yr_blt", "mh_make", "mh_size"), with = FALSE])]
cat("\nNumber of duplicates removed from mobile homes: ", nrow(mobile_homes) - nrow(mobile_homes.nodupl), "\n")

# identify building records for these mobile homes
bldg_mh <- merge(mobile_homes.nodupl, prep_buildings[, .(id_bld = id, rp_acct_id, yr_blt = year_built)], 
                 by = c("rp_acct_id", "yr_blt"), all.x = TRUE)
# if year didn't match try without it, but blg_type must be MHOME
rp <- unique(bldg_mh[is.na(id_bld) & rp_acct_id %in% prep_buildings[bldg_typ == "MHOME", rp_acct_id], rp_acct_id])
bldg_mh[prep_buildings[bldg_typ == "MHOME" & rp_acct_id %in% rp], 
        `:=`(id_bld = i.id, yr_blt = pmax(i.year_built, yr_blt)), on = "rp_acct_id"] # take the max of year built

# For now we will ignore the 14 remaining mobile homes where there is no match to the buildings
cat("\nIgnoring ", nrow(bldg_mh[is.na(id_bld)]), " mobile homes with no match to the buildings dataset.")

# update total sqft and year built for mobile homes
prep_buildings[bldg_mh, `:=`(total_sqft = i.mh_size, year_built = i.yr_blt), on = c(id = "id_bld")]


# drop buildings that do not have spatial representation in parcels
nbld <- nrow(prep_buildings)
prep_buildings_in_pcl <- prep_buildings[rp_acct_id %in% parcels_final[, rp_acct_id]]
cat("\nDropped ", nbld - nrow(prep_buildings_in_pcl), " buildings due to missing parcels.")


# Calculate improvement value from valuation
# ----------------------------
# It will be distributed to individual buildings proportionally to the sqft

year_valuation <- valuation[tax_yr == 2023]
# if property doesn't have valuation for 2023, take the 2024 value
year_valuation <- rbind(year_valuation, 
                        valuation[!rp_acct_id %in% year_valuation[, rp_acct_id] & tax_yr == 2024])
cat("\nNumber of missed records: ", nrow(year_valuation[!rp_acct_id %in% valuation[, rp_acct_id]]))
cat("\nNumber of duplicates: ", nrow(year_valuation[duplicated(rp_acct_id)]))

# calculate proportions of sqft. Set minimum of 1 for zero sqft
prep_buildings_in_pcl[, `:=`(total_sqft_tmp = pmax(1, total_sqft))]
# sum total_sqft per rp_acct_id
prep_buildings_in_pcl[, `:=`(total_sqft_per_account = sum(total_sqft_tmp)), by = "rp_acct_id"]
# calculate proportions and resulting improvement value
prep_buildings_in_pcl[, proportion := total_sqft_tmp / total_sqft_per_account
               ][year_valuation, improvement_value := round(proportion * i.impr_av), 
                 on = "rp_acct_id"]


# Impute building_type_id
# ------------------------
# remove leading and trailing spaces in some of the columns in the building reclass table
# (should not be needed anymore)
#cols_to_trim <- colnames(bt_reclass)[sapply(colnames(bt_reclass), function(x) is.character(bt_reclass[[x]]))]
#bt_reclass[, (cols_to_trim) := lapply(.SD, trimws), .SDcols = cols_to_trim]

# Assign building type by joining with the building reclass table
prep_buildings_in_pcl[bt_reclass, 
                      building_type_id := ifelse(is.na(building_type_id), i.building_type_id, 
                                                 building_type_id), 
                      on = c("bldg_typ", "improv_typ", "use_desc")]

cat("\nMatched", nrow(prep_buildings_in_pcl[!is.na(building_type_id)]), "records with building reclass table")
cat("\nUnmatched: ", nrow(prep_buildings_in_pcl[is.na(building_type_id)]), "records.")
cat("\nThe following building codes were not found:\n")
print(prep_buildings_in_pcl[is.na(building_type_id), .N, by = c("bldg_typ", "improv_typ", "use_desc")][order(-N)])


# impute DUs
# ------------
# join buildings with parcel info 
prep_buildings_in_pcl <- merge(prep_buildings_in_pcl, 
                               parcels_final[, .(rp_acct_id, parcel_id, num_dwell, land_use_type_id, use_code)],
                               by = "rp_acct_id", all.x = TRUE)
prep_buildings_in_pcl[is.na(num_dwell), num_dwell := 0]

# first set all residential records to one unit and non-res to zero 
index_residential <- with(prep_buildings_in_pcl, building_type_id %in% c(4, 11, 12, 19))
prep_buildings_in_pcl[index_residential, residential_units := 1][, residential_units_orig := 1]
prep_buildings_in_pcl[!index_residential, `:=`(residential_units = 0, residential_units_orig = 0)]
# sum units per parcel
prep_buildings_in_pcl[index_residential, `:=`(sum_du = sum(residential_units)), by = "parcel_id"]

# Correct units for multiplexes. 
# Set codes and the corresponding number of units. 
# If it is a range, it's in the format (minimum, maximum, default).
codes_units <- list(`121` = 2,   # Duplex
                    `122` = 3,   # Triplex
                    `123` = 4,   # Fourplex
                    `131` = c(5, 9, 7),   # 5-9 units (min, max, default)
                    `132` = c(10, 14, 12),  # 10-14 units
                    `133` = c(15, 19, 17),  # 15-19 units
                    `134` = c(20, 29, 25),  # 20-29 units
                    `135` = c(30, 39, 35),  # 30-39 units
                    `136` = c(40, 49, 45),  # 40-49 units
                    `137` = c(50, 100, 60), # 50+. (min, max, default)
                    `141` = c(4, 100, 20)  # Condo (min, max, default)
                    )
# first process parcels that have only 1 unit and is not mobile home
for(code in names(codes_units)){
    index <- with(prep_buildings_in_pcl, index_residential & sum_du == 1 & building_type_id != 11 & 
                      !is.na(use_code) & use_code == code)
    if(sum(index) == 0) next # if no buildings for this code, go to the next code
    if(length(codes_units[[code]]) > 1){ # number of units is given as a range; compute from sqft
        units <- prep_buildings_in_pcl[index, round(total_sqft/1000)] # using 1000sf per unit 
        units <- pmax(codes_units[[code]][1], pmin(units, codes_units[[code]][2])) # keep it in the given range
    } else units <- codes_units[[code]]
    prep_buildings_in_pcl[index,  `:=`(residential_units = units)]
}

# now process parcels with multiple buildings
# here correct units for duplexes 
for(code in names(codes_units)) {
    prep_buildings_in_pcl[index_residential & sum_du > 1 & sum_du < codes_units[[code]][1] & 
                              !is.na(use_code) & use_code == code & use_desc == "Duplex", 
                          `:=`(residential_units = 2)]
}
# now we impute units into Multi-family records 
# recompute the number of DUs per parcel and get the number of Multi-family records
prep_buildings_in_pcl[, `:=`(sum_du2 = sum(residential_units), nmf = sum(building_type_id %in% c(4, 12))), 
                      by = "parcel_id"]
for(code in names(codes_units)) {
    # only MF records where the new sum of DUs hasn't reached what we expect
    index <- with(prep_buildings_in_pcl, index_residential & sum_du2 > 1 & sum_du2 < codes_units[[code]][1] & 
                      !is.na(use_code) & use_code == code & nmf > 0 & building_type_id %in% c(4, 12))
    if(sum(index) == 0) next # if no buildings for this code, go to the next code
    target <- if(length(codes_units[[code]]) > 1) codes_units[[code]][3] else codes_units[[code]] # number of DUs we expect according to the use code
    # add the remainder of the expectation to the MF records 
    prep_buildings_in_pcl[index, `:=`(residential_units = residential_units + round((target - sum_du2) / nmf))]
}

cat("\nImputed", prep_buildings_in_pcl[, sum(residential_units) - sum(residential_units_orig)], 
    "residential units. Total number of units is", prep_buildings_in_pcl[, sum(residential_units)],
    ".")


# collect attributes for the final table (here we could pre-populate other columns if needed)
buildings_final <- prep_buildings_in_pcl[, .(building_id = id, gross_sqft = total_sqft, year_built,
                                             parcel_id, rp_acct_id, residential_units, 
                                             building_type_id, improvement_value, stories, land_area = 0, 
                                             non_residential_sqft = ifelse(residential_units > 0, 0, total_sqft),
                                             use_code, use_desc)]

# remove unnecessary columns and pre-populate other columns, whatever is needed
parcels_final[, `:=`(num_dwell = NULL, county_id = 35)]

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
