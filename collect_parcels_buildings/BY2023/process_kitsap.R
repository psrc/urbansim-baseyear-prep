# Script to create parcels and buildings tables from Kitsap assessor data
# for the use in urbansim.
# It generates 3 tables: 
#    urbansim_parcels, urbansim_buildings, building_type_crosstab
#
# Hana Sevcikova, last update 05/28/2024
#

library(data.table)

county <- "Kitsap"

# path to the Assessor text files
#data.dir <- "\\aws-model10\UrbanSIM Data\Projects\2023_Baseyear\Assessor\Extracts\Kitsap\Urbansim_Processing" 
data.dir <- "Kitsap_data" # Hana's local path
misc.data.dir <- "data" # path to the BY2023/data folder
# write into mysql as well as csv; 
# it will overwrite the existing mysql tables 
# urbansim_parcels, urbansim_buildings, building_type_crosstab
write.result <- TRUE

if(write.result) source("mysql_connection.R")

###############
# Load all data
###############

# parcels table
parcels.all <- fread(file.path(data.dir, "parcels23_kit_base.csv"))
parcels.units <- fread(file.path(data.dir, "KITSAP_COUNTY_PARCELS_01172024.csv")) # from Grant (got it from Kitsap staff)
                                                                    # in \\file\datateam\Projects\Assessor\assessor_permit\kitsap\data\2023\extracts

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

# Info about fake parcels and buildings (prepared by Mark)
fake_parcels <- fread(file.path(data.dir, "kitsap_fake_parcel_changes.csv"))
fake_buildings <- fread(file.path(data.dir, "kitsap_fake_building_changes.csv"))

###################
# Process parcels
###################

cat("\nProcessing Kitsap parcels\n=========================\n")
# remove duplicates (just take the first record of each duplicate); remove rp_acct_id = 1
parcels.all.nodupl <- parcels.all[!duplicated(rp_acct_id)][rp_acct_id > 1]
cat("\nNumber of duplicates removed from parcels dataset: ", 
    nrow(parcels.all) - nrow(parcels.all.nodupl))

# remove any duplicates from the three assessors files
tax <- unique(tax)
land <- unique(land)
main <- unique(main)

# extract what is needed and merge together
# with the flatats dataset
prep_parcels <- merge(parcels.all.nodupl,
                      tax[, .(rp_acct_id, land_value)],
                      by = "rp_acct_id", all.x = TRUE)

# There are more than 50K parcels in the "tax" dataset that are not in "parcels",
# but only 709 of them have acres > 0. We will ignore them for now.
cat("\nNumber of records in parcels that are not available in flatats (records dropped from parcels): ",
    nrow(parcels.all.nodupl[!rp_acct_id %in% tax[, rp_acct_id]]))
cat("\nNumber of records in flatats that are not available in updated parcels (records ignored): ",
    nrow(tax[!rp_acct_id %in% prep_parcels[, rp_acct_id]]))
cat("\n\t\t\t",
    nrow(tax[!rp_acct_id %in% prep_parcels[, rp_acct_id] & acres > 0]), "of those have acres > 0, summing to", 
    tax[!rp_acct_id %in% parcels.all.nodupl[, rp_acct_id], sum(acres)], "acres.")

# land dataset
prep_parcels <- merge(prep_parcels, land[, .(rp_acct_id, prop_class, num_dwell)],
                      by = "rp_acct_id", all.x = TRUE)
cat("\nNumber of records in land that are not available in parcels (records ignored): ",
    nrow(land[!rp_acct_id %in% prep_parcels[, rp_acct_id]]))
cat("\nNumber of records in parcels that are not available in land: ",
    nrow(prep_parcels[!rp_acct_id %in% land[, rp_acct_id]]))

# main dataset
prep_parcels <- merge(prep_parcels, main[, .(rp_acct_id, tax_status)],
                      by = "rp_acct_id", all.x = TRUE)
# There are 404 records in parcels that are not present in land and main.
cat("\nNumber of records in main that are not available in parcels (records ignored): ",
    nrow(main[!rp_acct_id %in% prep_parcels[, rp_acct_id]]))
cat("\nNumber of records in parcels that are not available in main: ",
    nrow(prep_parcels[!rp_acct_id %in% main[, rp_acct_id]]))

# join fake parcels with the three attribute files
fake_parcels[tax, `:=`(land_value = i.land_value * land_proportion), 
             on = c(orig_rp_acct_id = "rp_acct_id")]
fake_parcels[land, `:=`(old_prop_class = i.prop_class, num_dwell = i.num_dwell), 
             on = c(orig_rp_acct_id = "rp_acct_id")]
fake_parcels[main, `:=`(tax_status = i.tax_status), on = c(orig_rp_acct_id = "rp_acct_id")]

# add info from fake parcels to the set of all parcels
# (expects that new parcels are included in the set of base parcels)
prep_parcels <- prep_parcels[! rp_acct_id %in% unique(fake_parcels[, orig_rp_acct_id])]
prep_parcels[fake_parcels, `:=`(prop_class = i.new_prop_class, land_value = i.land_value, 
                                num_dwell = i.num_dwell, tax_status = i.tax_status),
             on = c(rp_acct_id = "new_rp_acct_id")]
cat("\nIncorporated info for ", nrow(fake_parcels[new_rp_acct_id %in% prep_parcels[, rp_acct_id]]),
    "fake parcels")

# compose final parcels dataset
parcels_final <- prep_parcels[, .(parcel_id = rp_acct_id,
                                  rp_acct_id, 
                                  land_value, num_dwell,
                                  use_code = prop_class,
                                  exemption = as.integer(tax_status != "T"), 
                                  gross_sqft = round(gis_sqft), 
                                  x_coord_sp = point_x, y_coord_sp = point_y
                                  )]

# join with land use reclass table
parcels_final[lu_reclass, land_use_type_id := i.land_use_type_id, on = "use_code"]
if(nrow(notfoundlu <- parcels_final[is.na(land_use_type_id) & !is.na(use_code), .N, by = "use_code"]) > 0){
    cat("\nThe following land use codes were not found:\n")
    print(notfoundlu[order(use_code)])
} else cat("\nAll land use codes matched.")

cat("\nTotal:", nrow(parcels_final), "parcels")

###################
# Process buildings
###################

cat("\n\nProcessing Kitsap buildings\n=========================\n")

# extract info from the assessors buildings table
# ------------------------
prep_buildings <- raw_buildings[, .(id = 1:nrow(raw_buildings),
                                    improv_typ, 
                                    rp_acct_id, total_sqft = flr_tot_sf,
                                    year_built, stories, 
                                    bldg_typ, use_desc
                                    )]

# drop buildings that do not have spatial representation in parcels
nbld <- nrow(prep_buildings)
prep_buildings_in_pcl <- prep_buildings[rp_acct_id %in% parcels_final[, rp_acct_id]]
cat("\nDropped ", nbld - nrow(prep_buildings_in_pcl), " buildings due to missing parcels.")

# add fake buildings
new_fake_buildings <- fake_buildings[is.na(orig_rp_acct_id), 
                                     .(rp_acct_id = new_rp_acct_id, bldg_typ = new_bld_typ,
                                      improv_typ = new_improv_type, use_desc = new_use_desc,
                                      total_sqft = new_flr_tot_sf, year_built = NA, stories = NA)]
new_fake_buildings[, id := seq(max(prep_buildings_in_pcl[, id]) + 1, length = nrow(new_fake_buildings))]

nbld <- nrow(prep_buildings_in_pcl)
prep_buildings_in_pcl <- rbind(prep_buildings_in_pcl, new_fake_buildings)
cat("\nAdded ", nrow(prep_buildings_in_pcl) - nbld, " fake buidlings.")

if(nrow((mod_fake_buildings <- fake_buildings[!is.na(orig_rp_acct_id)])) > 0){
  # move buildings into different parcels
  # (this code is untested as there is currently no such case)
  prep_buildings_in_pcl[mod_fake_buildings, `:=`(rp_acct_id = i.new_rp_acct_id, bld_typ = i.new_bld_typ,
                                                 improv_typ = i.new_improv_type, use_desc = i.new_use_desc,
                                                 total_sqft = i.new_flr_tot_sf),
                        on = c(rp_acct_id = "orig_rp_acct_id", bld_typ = "orig_bld_typ", 
                               improv_typ = "orig_improv_type")]
  cat("\nModified ", nrow(mod_fake_buildings), " fake buildings.")
}


# add mobile homes info
# ------------------
# remove duplicates
mobile_homes.nodupl <- mobile_homes[!duplicated(mobile_homes[, c("rp_acct_id", "yr_blt", "mh_make", "mh_size"), with = FALSE])]
cat("\nNumber of duplicates removed from mobile homes: ", nrow(mobile_homes) - nrow(mobile_homes.nodupl))

# match mobile homes to buildings;
# as there might be duplicates on both sides (MHs and buildings), loop over unique records and tag processed rows
mobile_homes.nodupl[, processed := FALSE]
prep_buildings_in_pcl[, processed := FALSE]
while(nrow(mobile_homes.nodupl[processed == FALSE]) > 0 && 
      nrow(prep_buildings_in_pcl[rp_acct_id %in% mobile_homes.nodupl[processed == FALSE, rp_acct_id] & 
                          processed == FALSE]) > 0){
    tmh <- mobile_homes.nodupl[processed == FALSE]
    tmh <- tmh[!duplicated(tmh[,.(rp_acct_id, yr_blt)])]
    if("id_bld" %in% colnames(tmh)) tmh[, id_bld := NULL]
    tbld_mh <- prep_buildings_in_pcl[rp_acct_id %in% tmh[, rp_acct_id] & processed == FALSE & bldg_typ == "MHOME"]
    utbld_mh <- tbld_mh[!duplicated(tbld_mh[, .(rp_acct_id, year_built)])]
    bldg_mh <- merge(tmh, utbld_mh[,.(id_bld = id, rp_acct_id, yr_blt = year_built)], 
                     by = c("rp_acct_id", "yr_blt"), all.x = TRUE)
    prep_buildings_in_pcl[id %in% bldg_mh[, id_bld] & processed == FALSE, processed := TRUE]
    # if there was no match, try it without MHOME; year must match
    tbld_mh <- prep_buildings_in_pcl[rp_acct_id %in% bldg_mh[is.na(id_bld), rp_acct_id] & processed == FALSE]
    utbld_mh <- tbld_mh[!duplicated(tbld_mh[, .(rp_acct_id, year_built)])]
    bldg_mh[utbld_mh[,.(id_bld = id, rp_acct_id, yr_blt = year_built)], 
            `:=`(id_bld = ifelse(is.na(id_bld), i.id_bld, id_bld)), on = c("rp_acct_id", "yr_blt")]
    prep_buildings_in_pcl[id %in% bldg_mh[, id_bld] & processed == FALSE, processed := TRUE]
    # if year didn't match try without it, but blg_type must be MHOME
    tbld_mh <- prep_buildings_in_pcl[rp_acct_id %in% bldg_mh[is.na(id_bld), rp_acct_id] & bldg_typ == "MHOME" & processed == FALSE]
    bldg_mh[tbld_mh[,.(id_bld = id, rp_acct_id, yr_blt = year_built)], 
            `:=`(id_bld = ifelse(is.na(id_bld), i.id_bld, id_bld), 
                 yr_blt = pmax(i.yr_blt, yr_blt)), on = "rp_acct_id"] # take the max of year built
    prep_buildings_in_pcl[id %in% bldg_mh[, id_bld] & processed == FALSE, processed := TRUE]
    mobile_homes.nodupl[bldg_mh[!is.na(id_bld)], id_bld := i.id_bld, on = "id"]
    nprocessed <- nrow(mobile_homes.nodupl[id %in% bldg_mh[!is.na(id_bld), id]  & processed == FALSE])
    if(nprocessed == 0) break
    mobile_homes.nodupl[id %in% bldg_mh[!is.na(id_bld), id]  & processed == FALSE, processed := TRUE]
}

# For now we will ignore the 2575 remaining mobile homes where there is no match to the buildings
cat("\nIgnoring", nrow(mobile_homes.nodupl[processed == FALSE]), "(out of",  nrow(mobile_homes.nodupl),
        ") mobile homes with no match to the buildings dataset.")

# update total sqft and year built for mobile homes
prep_buildings_in_pcl[mobile_homes.nodupl[processed == TRUE], `:=`(total_sqft = i.mh_size, year_built = i.yr_blt), on = c(id = "id_bld")]


# Calculate improvement value from valuation
# ----------------------------
# It will be distributed to individual buildings proportionally to the sqft

year_valuation <- valuation[tax_yr == 2023]
# if property doesn't have valuation for 2023, take the 2024 value
year_valuation <- rbind(year_valuation, 
                        valuation[!rp_acct_id %in% year_valuation[, rp_acct_id] & tax_yr == 2024])
cat("\nNumber of missed records in valuation: ", nrow(year_valuation[!rp_acct_id %in% valuation[, rp_acct_id]]))
cat("\nNumber of duplicates in valuation: ", nrow(year_valuation[duplicated(rp_acct_id)]))

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
# Assign building type by joining with the building reclass table
prep_buildings_in_pcl[bt_reclass, building_type_id := i.building_type_id, 
                      on = c("bldg_typ", "improv_typ", "use_desc")]

cat("\nMatched", nrow(prep_buildings_in_pcl[!is.na(building_type_id)]), "records with building reclass table")
cat("\nUnmatched: ", nrow(prep_buildings_in_pcl[is.na(building_type_id)]), "records.")
if(nrow(missbt <- prep_buildings_in_pcl[is.na(building_type_id), .N, by = c("bldg_typ", "improv_typ", "use_desc")]) > 0){
    cat("\nThe following building codes were not found:\n")
    print(missbt[order(-N)])
} else cat("\nAll building use codes matched.")

# handle special cases in parcels.units (most likely errors in the units file)
parcels.units[rp_acct_id %in% c(2561223, 1490747, 2647287, 2680080, 2687820), total_unit := NA]
parcels.units[rp_acct_id == 2414415, total_unit := 7] # from the Townhouse records

# impute DUs
# ------------
# join buildings with parcel info 
# first an external dataset with # units
# (alternatively, the column num_dwell could serve the same purpose but seems to underestimate the number of units)
prep_buildings_in_pcl[parcels.units, du_in_pcl := i.total_unit, on = "rp_acct_id"] 
# now other attributes
prep_buildings_in_pcl <- merge(prep_buildings_in_pcl, 
                               parcels_final[, .(parcel_id, rp_acct_id, num_dwell, land_use_type_id, use_code)],
                               by = "rp_acct_id", all.x = TRUE)
prep_buildings_in_pcl[is.na(num_dwell), num_dwell := 0]

# first, set all residential records to one unit and non-res to zero 
index_residential <- with(prep_buildings_in_pcl, building_type_id %in% c(4, 11, 12, 19))
# note that there are 24 mix-use records, none of which looks like there should be DUs in it
prep_buildings_in_pcl[index_residential, residential_units := 1][, residential_units_orig := 1]
prep_buildings_in_pcl[!index_residential, `:=`(residential_units = 0, residential_units_orig = 0)]
# sum units per parcel
prep_buildings_in_pcl[index_residential, `:=`(sum_du = sum(residential_units)), by = "rp_acct_id"]

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


# consider parcels with one unit that match the external info as done
prep_buildings_in_pcl[, processed := FALSE]
prep_buildings_in_pcl[index_residential & sum_du == du_in_pcl, processed := TRUE]

# first process parcels that have only 1 unit and is not mobile home
for(code in names(codes_units)){
    index <- with(prep_buildings_in_pcl, index_residential & !processed & sum_du == 1 & building_type_id != 11 & 
                      !is.na(use_code) & use_code == code)
    if(sum(index) == 0) next # if no buildings for this code, go to the next code
    if(length(codes_units[[code]]) > 1){ # number of units is given as a range; compute from sqft
        units <- prep_buildings_in_pcl[index, round(total_sqft/1000)] # using 1000sf per unit 
        units <- pmax(codes_units[[code]][1], pmin(units, codes_units[[code]][2])) # keep it in the given range
    } else units <- codes_units[[code]]
    prep_buildings_in_pcl[index,  `:=`(residential_units = ifelse(is.na(du_in_pcl), units, du_in_pcl),
                                       processed = TRUE)]
}

# now process parcels with multiple buildings
# here correct units for duplexes 
for(code in names(codes_units)) {
    prep_buildings_in_pcl[index_residential & !processed & sum_du > 1 & sum_du < codes_units[[code]][1] & 
                              !is.na(use_code) & use_code == code & use_desc == "Duplex", 
                          `:=`(residential_units = 2, processed = TRUE)]
}
# now we impute units into Multi-family records 
# recompute the number of DUs per parcel and get the number of Multi-family records
prep_buildings_in_pcl[, `:=`(sum_du2 = sum(residential_units), nmf = sum(building_type_id %in% c(4, 12))), 
                      by = "rp_acct_id"]
for(code in names(codes_units)) {
    # only MF records where the new sum of DUs hasn't reached what we expect
    index <- with(prep_buildings_in_pcl, index_residential & !processed & sum_du2 > 1 & 
                      sum_du2 < ifelse(is.na(du_in_pcl), codes_units[[code]][1], du_in_pcl) & 
                      !is.na(use_code) & use_code == code & nmf > 0 & building_type_id %in% c(4, 12))
    if(sum(index) == 0) next # if no buildings for this code, go to the next code
    target <- if(length(codes_units[[code]]) > 1) codes_units[[code]][3] else codes_units[[code]] # number of DUs we expect according to the use code
    targets <- prep_buildings_in_pcl[index, ifelse(is.na(du_in_pcl), target, du_in_pcl)]
    # add the remainder of the expectation to the MF records 
    prep_buildings_in_pcl[index, `:=`(residential_units = residential_units + round((targets - sum_du2) / nmf), processed = TRUE)]
}

# process the rest - distribute the remaining units into non-processed buildings
prep_buildings_in_pcl[, `:=`(sum_du2 = sum(residential_units)), by = "rp_acct_id"]
index <- with(prep_buildings_in_pcl, index_residential & !processed  & du_in_pcl > sum_du2)

tmpbld <- prep_buildings_in_pcl[index, .(nres = .N), by = "rp_acct_id"]
prep_buildings_in_pcl[index, process_this_step := TRUE]
prep_buildings_in_pcl[tmpbld, `:=`(residential_units = ifelse(process_this_step == TRUE, residential_units + round((du_in_pcl - sum_du2) / i.nres), 
    residential_units)), on = "rp_acct_id"]
prep_buildings_in_pcl[index, processed := TRUE]

## for checking purposes
# bldpcl <- prep_buildings_in_pcl[, .(sum_du = sum(residential_units)), by = "rp_acct_id"]
# bldpcl[parcels.units, should_be := i.total_unit, on = "rp_acct_id"]
# bldpcl[, .(sum(sum_du, na.rm = TRUE), sum(should_be, na.rm = TRUE))]
# prep_buildings_in_pcl[rp_acct_id %in%  bldpcl[should_be > 2*sum_du, rp_acct_id], .N, by = "use_desc"][order(-N)]

prep_buildings_in_pcl[is.na(residential_units), residential_units := 0]

cat("\nImputed", prep_buildings_in_pcl[, sum(residential_units) - sum(residential_units_orig)], 
    "residential units. Total number of units is", prep_buildings_in_pcl[, sum(residential_units)],
    ".")

# collect attributes for the final table (here we could pre-populate other columns if needed)
buildings_final <- prep_buildings_in_pcl[, .(building_id = id, parcel_id,
                                             gross_sqft = total_sqft, year_built,
                                             parcel_id_fips = rp_acct_id, residential_units, 
                                             building_type_id, improvement_value, stories, land_area = 0, 
                                             non_residential_sqft = ifelse(residential_units > 0, 0, total_sqft),
                                             use_code = paste(bldg_typ, improv_typ, sep = ";"), 
                                             use_desc)]

cat("\nTotal all: ", nrow(buildings_final), "buildings")

# remove unnecessary columns and pre-populate other columns, whatever is needed
parcels_final[, `:=`(num_dwell = NULL, county_id = 35)]
setnames(parcels_final, "rp_acct_id", "parcel_id_fips") # rename rp_acct_id column

# generate building type crosstabs
bt.tab <- merge(buildings_final,
                prep_buildings_in_pcl[, .(building_id = id, bldg_typ, improv_typ)], by = "building_id", 
                all.x = TRUE, all.y = FALSE)[, .(N = .N, DU = sum(residential_units), 
                                                nonres_sqft = sum(non_residential_sqft)), 
                          by = .(bldg_typ, improv_typ, use_desc, building_type_id)]
bt.tab <- merge(bt.tab, bt_reclass[, .(bldg_typ, improv_typ, use_desc, building_type_id, building_type_name)],
                by = c("bldg_typ", "improv_typ", "use_desc", "building_type_id"), all.x = TRUE)
setcolorder(bt.tab, c("bldg_typ", "improv_typ", "use_desc", "building_type_id", "building_type_name"))


if(write.result){
    # write results
    fwrite(parcels_final, file = "urbansim_parcels_kitsap.csv")
    fwrite(buildings_final, file = "urbansim_buildings_kitsap.csv")
    db <- paste0(tolower(county), "_2023_parcel_baseyear")
    connection <- mysql.connection(db)
    dbWriteTable(connection, "urbansim_parcels", parcels_final, overwrite = TRUE, row.names = FALSE)
    dbWriteTable(connection, "urbansim_buildings", buildings_final, overwrite = TRUE, row.names = FALSE)
    dbWriteTable(connection, "building_type_crosstab", bt.tab, overwrite = TRUE, row.names = FALSE)
    DBI::dbDisconnect(connection)
}
