# Script to create parcels and buildings tables from Pierce assessor data
# for the use in urbansim 
# It generates 4 tables: 
#    urbansim_parcels, urbansim_buildings: contain records that are found in BY2018
#    urbansim_parcels_all, urbansim_buildings_all: all records regardless if they are found in BY2018
#
# Hana Sevcikova, last update 01/09/2024
#

library(data.table)

county <- "Pierce"
county.id <- 53

#data.dir <- file.path("~/e$/Assessor23", county) # path to the Assessor text files
#data.dir <- "~/J/UrbanSIM\\ Data/Projects/2023_Baseyear/Assessor/Extracts/Pierce/Urbansim_Processing"
data.dir <- "Pierce_data" # Hana's local path
misc.data.dir <- "data" # path to the BY2023/data folder
# write into mysql as well as csv; 
# it will overwrite the existing mysql tables 
# urbansim_parcels, urbansim_buildings urbansim_parcels_all, urbansim_buildings_all
write.result <- TRUE # it will overwrite the existing tables urbansim_parcels & urbansim_buildings

if(write.result) source("mysql_connection.R")

###############
# Load all data
###############

# parcels & tax accounts
parcels.23to18 <- fread(file.path(data.dir, "parcels23_pie_to_2018_parcels.csv"))
full.parcels <- fread(file.path(data.dir, "parcels23_pie_to_2018_parcels_short.txt")) # file with all attributes
dissolve <- fread(file.path(data.dir, "parcels23_pierce_condo_dissolve_pt_to_base_dissolve_overlay.csv"))
tax <- fread(file.path(data.dir, "tax_account.txt"), sep = "|", quote = "") # 

# reclass tables
lu_reclass <- fread(file.path(misc.data.dir, "land_use_generic_reclass_2023.csv"))
bt_reclass <- fread(file.path(misc.data.dir, "building_use_generic_reclass_2023.csv"))

# buildings
raw_buildings <- fread(file.path(data.dir, "improvement_builtas.txt"))
improvement <- fread(file.path(data.dir, "improvement.txt"))
#improvement_detail <- fread(file.path(data.dir, "improvement_detail.txt"))

###################
# Process parcels
###################
# Pierce ID: taxparceln, parcel_number (they should be the same)
# urbansim ID: pin

cat("\nProcessing Pierce parcels\n=========================\n")

# make column names lowercase
colnames(parcels.23to18) <- tolower(colnames(parcels.23to18))
colnames(full.parcels) <- tolower(colnames(full.parcels))

# remove duplicates (just take the first record of each duplicate)
parcels.23to18[, `:=`(taxparceln = as.double(taxparceln), 
                      taxparceln_orig = as.double(taxparceln_orig))]
full.parcels[, `:=`(taxparceln = as.double(taxparceln))]
dissolve[, `:=`(taxparce_1 = as.double(taxparce_1),
                taxparceln = as.double(taxparceln))]

parcels.nodupl <- parcels.23to18[!duplicated(taxparceln)]
full.parcels <- full.parcels[!duplicated(taxparceln)]
cat("\nNumber of duplicates removed from 23to18 parcels: ", nrow(parcels.23to18) - nrow(parcels.nodupl))

# aggregate attributes in full.parcels for stacked parcels
full.parcels[dissolve, base_parcel := i.taxparce_1, on = "taxparceln"]
full.parcels[is.na(base_parcel), base_parcel := taxparceln]
prep_parcels <- full.parcels[!duplicated(taxparceln)]

tax[, taxparceln := as.double(parcel_number)]
tax.nodupl <- tax[!duplicated(taxparceln)]
cat("\nNumber of duplicates removed from Pierce parcels: ", nrow(tax) - nrow(tax.nodupl))

# if land value is zero, try to get the current year info in the tax dataset 
zero_lv <- nrow(prep_parcels[land_value== 0])
prep_parcels[tax.nodupl, land_value := ifelse(land_value == 0 & !is.na(i.land_value_current_year) & i.land_value_current_year > 0, 
                                              i.land_value_current_year, land_value),
             on = "taxparceln"]
cat("\nImputed land value into", zero_lv - nrow(prep_parcels[land_value== 0]), "records.")

# the same for improvement value
zero_imp <- nrow(prep_parcels[improvemen == 0])
prep_parcels[tax.nodupl, improvemen := ifelse(improvemen == 0 & !is.na(i.improvement_value_current_year) & i.improvement_value_current_year > 0, 
                                              i.improvement_value_current_year, improvemen),
             on = "taxparceln"]
cat("\nImputed improvement value into", zero_imp - nrow(prep_parcels[improvemen == 0]), "records.")

# Now if land value is zero, try to get the prior year in the tax dataset and add 10% inflation
zero_lv <- nrow(prep_parcels[land_value== 0])
prep_parcels[tax.nodupl, land_value := ifelse(
    land_value == 0 & !is.na(i.land_value_prior_year) & i.land_value_prior_year > 0, 
    i.land_value_prior_year * 1.1, land_value),
    on = "taxparceln"]
cat("\nImputed land value into additional", zero_lv - nrow(prep_parcels[land_value== 0]), "records using prior year info.")

# do the same for improvement value
zero_imp <- nrow(prep_parcels[improvemen == 0])
prep_parcels[tax.nodupl, improvemen := ifelse(
    improvemen == 0 & !is.na(i.improvement_value_prior_year) & i.improvement_value_prior_year > 0, 
    i.improvement_value_prior_year * 1.1, improvemen),
    on = "taxparceln"]
cat("\nImputed improvement value into additional", zero_imp - nrow(prep_parcels[improvemen == 0]), "records using prior year info.")

# assign the use code of the base parcel
prep_parcels[prep_parcels[taxparcelt == "Base Parcel"], use_code_base_parcel := i.use_code, on = "base_parcel"]
prep_parcels[is.na(use_code_base_parcel), use_code_base_parcel := ifelse(is.na(use_code), 0, use_code)]
prep_parcels[, `:=`(N = .N, has_valid_use_code = any(use_code_base_parcel > 0)), by = "base_parcel"]
prep_parcels[use_code_base_parcel == 0 & N > 1, use_code_base_parcel := ifelse(has_valid_use_code, NA, use_code)]

aggr_prep_parcels <- prep_parcels[
    , .(land_value = sum(land_value), improvement_value = sum(improvemen),
        exemption_ = all(ifelse(exemption_ == "", 0, 1)==0),
        use_code = max(use_code_base_parcel, na.rm = TRUE), 
        use_code_dif = any(use_code_base_parcel != max(use_code_base_parcel, na.rm = TRUE)),
        N = .N), by = "base_parcel"]
# TODO: set the use_code to a predominant use 
aggr_prep_parcels[N > 1 & use_code_dif, use_code := NA]

cat("\nSet use_code to NA for", nrow(aggr_prep_parcels[N > 1 & use_code_dif]), "base parcels.")

# extract a subset of columns from the final set of parcels
prep_parcels2 <- parcels.nodupl[, .(taxparceln, taxparceln_orig, pin, point_x, point_y, gis_sqft)]

# join with aggregated full parcels
prep_parcels2[aggr_prep_parcels,
             `:=`(use_code = i.use_code, exemption_ = i.exemption_, land_value = i.land_value,
                  improvemen = i.improvement_value),
             on = c(taxparceln = "base_parcel")]

# assemble columns for final parcels table
parcels_final <- prep_parcels2[, .(
    parcel_id = pin, parcel_id_fips = as.character(taxparceln), land_value, use_code = as.character(use_code), 
    gross_sqft = round(gis_sqft),  x_coord_sp = point_x, y_coord_sp = point_y, 
    exemption = as.integer(exemption_), county_id = county.id)]
    
# join with reclass table
parcels_final[lu_reclass[county_id == county.id], land_use_type_id := i.land_use_type_id, 
              on = c(use_code = "county_land_use_code")]
if(nrow(misslu <- parcels_final[is.na(land_use_type_id) & !is.na(use_code), .N, by = "use_code"]) > 0){
    cat("\nThe following land use codes were not found:\n")
    print(misslu[order(use_code)])
} else cat("\nAll land use codes matched.")

#parcels_all <- merge(tax[, .(parcel_id_fips = taxparceln)], parcels_final, all = TRUE, by = "parcel_id_fips")

cat("\nTotal all:", nrow(parcels_final), "parcels")
cat("\nAssigned to 2018:", nrow(parcels_final[!is.na(parcel_id) & parcel_id != 0]), "parcels")
cat("\nDifference:", nrow(parcels_final) - nrow(parcels_final[!is.na(parcel_id) & parcel_id != 0]))

###################
# Process buildings
###################

cat("\n\nProcessing Pierce buildings\n=========================\n")

# join with improvement table
buildings_tmp <- merge(improvement, raw_buildings, 
                       by = c("building_id", "parcel_number"),
                       all = TRUE)
setnames(buildings_tmp, "parcel_number", "parcel_number_orig")

# load lookup table
# TODO: this Xwalk table should be taken directly from parcels
impXwalk <- fread(file.path(data.dir, "improvement_builtas_pin_lookup.csv"))
impXwalk <- unique(impXwalk)

# join with dissolved parcels
buildings_tmp <- merge(buildings_tmp, impXwalk, by = "parcel_number_orig", all.x = TRUE)
                       

# group buildings since there were duplicate building_id values per parcel
buildings_all <- buildings_tmp[, .(
    sqft = sum(built_as_square_feet, na.rm = TRUE), units = sum(units, na.rm = TRUE), 
    bedrooms = sum(bedrooms, na.rm = TRUE),
    year_built = min(year_built), square_feet = sum(square_feet, na.rm = TRUE),
    net_square_feet = sum(net_square_feet, na.rm = TRUE)), 
    by = .(primary_occupancy_code, primary_occupancy_description, built_as_id, built_as_description, parcel_number)]
buildings_all[, count:= .N, by = "parcel_number"]

# preliminary join with building reclass table
buildings_all[, `:=`(primary_occupancy_code = as.character(primary_occupancy_code), 
                      built_as_id = as.character(built_as_id))]
buildings_all[bt_reclass[county_id == county.id], building_type_id := i.building_type_id, 
               on = c(primary_occupancy_code = "county_building_use_code")]
buildings_all[bt_reclass[county_id == county.id], built_as_building_type_id := i.building_type_id, 
               on = c(built_as_id = "county_building_use_code")]

# reclass some non-res types that appear in residential buildings
# check types via 
# prep_buildings[count > 1 & building_type_id %in% c(4, 12, 19, 11) & ! is.na(built_as_building_type_id) & ! built_as_building_type_id %in% c(4, 12, 19, 11), .N, by = c("built_as_id", "built_as_description")][order(built_as_description)]
buildings_all[count > 1 & building_type_id %in% c(4, 12, 19, 11) & 
                   ! is.na(built_as_building_type_id) & ! built_as_building_type_id %in% c(4, 12, 19, 11) & !built_as_id == 124,
               `:=`(primary_occupancy_code = built_as_id, units = 0)]

# rerun the join on reclass table as the occupancy code might have changed
buildings_all[bt_reclass[county_id == county.id], building_type_id := i.building_type_id, 
              on = c(primary_occupancy_code = "county_building_use_code")]

# join with improvement table
#buildings_all <- merge(improvement, buildings_grouped, 
#                        by = c("building_id", "parcel_number"),
#                        all = TRUE)

prep_buildings <- buildings_all[parcel_number %in% parcels_final[, parcel_id_fips]]
cat("\nDropped", nrow(buildings_all) - nrow(prep_buildings), "buildings due to missing representation in the parcels dataset.")

# Calculate improvement value proportionally to the sqft
prep_buildings[prep_parcels, `:=`(total_improvement_value = i.improvemen, 
                                  parcel_id = i.pin),
               on = c(parcel_number = "taxparceln")]
prep_buildings[, `:=`(sqft_tmp = pmax(1, sqft, na.rm = TRUE))]
prep_buildings[, `:=`(total_sqft = sum(sqft_tmp), count = .N), by = "parcel_number"]
prep_buildings[, `:=`(improvement_value = round(sqft_tmp/total_sqft * total_improvement_value))][, sqft_tmp := NULL]

# join with building reclass table
prep_buildings[, primary_occupancy_code := as.character(primary_occupancy_code)]
prep_buildings[bt_reclass[county_id == county.id], building_type_id := i.building_type_id, 
               on = c(primary_occupancy_code = "county_building_use_code")]
cat("\nMatched", nrow(prep_buildings[!is.na(building_type_id)]), "records with building reclass table")
cat("\nUnmatched: ", nrow(prep_buildings[is.na(building_type_id)]), "records.")
if(nrow(missbt <- prep_buildings[is.na(building_type_id), .N, by = c("primary_occupancy_code", "primary_occupancy_description")]) > 0){
    cat("\nThe following building codes were not found:\n")
    print(missbt[order(-N)])
} else cat("\nAll building use codes matched.")

# Clean up the units column
prep_buildings[is.na(units), units := 0]
# set units to zero for non-residential records
index_residential <- with(prep_buildings, building_type_id %in% c(4, 11, 12, 19))
prep_buildings[!index_residential, units := 0]

# set non_residential_sqft
prep_buildings[, non_residential_sqft := square_feet][index_residential, non_residential_sqft := 0]

# Impute units where residential & units is 0
# SF & mobile homes
prep_buildings[building_type_id %in% c(11, 19) & units == 0, units := 1]
# condos & separate units
prep_buildings[building_type_id %in% c(4, 12) & units == 0 &
                   primary_occupancy_description %in% c(
                       'Condo - Separate Unit', 'Townhouse', 'Townhouse/Condo'),
               units := 1]
# Duplex
prep_buildings[building_type_id == 12 & units == 0  &
                   primary_occupancy_description %in% c('Duplex Conv', 'Duplex'),
               units := 2]

# Triplex (I don't think this affects any records)
prep_buildings[building_type_id == 12 & units == 0  &
                   primary_occupancy_description %in% c('Triplex'),
               units := 3]

# For Apartment High Rise & Apartment w/4-8 Units use sqft 
prep_buildings[index_residential & units == 0 & 
                   primary_occupancy_description %in% c(
                       "Apartment High Rise", "Apartment w/4-8 Units"),
               units := pmax(1, round(sqft/830))]

# get sum of DU on parcels
prep_buildings[index_residential, `:=`(sum_du = sum(units),
                                       has_zero_units = any(units == 0),
                                       has_nonzero_units = any(units > 0),
                                       nbuildings = .N), 
               by = "parcel_number"]

# For Apt Low Rise 100 Units Plus impute by using sqft, but only for those records 
# where the sum of DU on the parcel is < 50, otherwise put just one unit there
prep_buildings[index_residential & units == 0  & 
                   primary_occupancy_description %in% c("Apt Low Rise 100 Units Plus"),
               units := ifelse(sum_du < 50, pmax(1, round(sqft/830)), 1)]

cat("\nDue to missing sqft units were not imputed into building with the following occupancy description:\n")
print(prep_buildings[index_residential & units == 0, .N, by = "primary_occupancy_description"])

# assemble columns for final buildings table
buildings_final <- prep_buildings[, .(
    building_id = 1:nrow(prep_buildings), 
    parcel_id, parcel_id_fips = as.character(parcel_number), gross_sqft = sqft, sqft_per_unit = ceiling(sqft/units),
    year_built, residential_units = units, non_residential_sqft, improvement_value,
    use_code = primary_occupancy_code, building_type_id
)]

cat("\nTotal all: ", nrow(buildings_final), "buildings")
cat("\nAssigned to 2018:", nrow(buildings_final[!is.na(parcel_id) & parcel_id != 0]), "buildings")
cat("\nDifference:", nrow(buildings_final) - nrow(buildings_final[!is.na(parcel_id) & parcel_id != 0]), "\n")


if(write.result){
    fwrite(parcels_final[!is.na(parcel_id) & parcel_id != 0], file = "urbansim_parcels_pierce.csv")
    fwrite(buildings_final[!is.na(parcel_id) & parcel_id != 0], file = "urbansim_buildings_pierce.csv")
    fwrite(parcels_final, file = "urbansim_parcels_all_pierce.csv")
    fwrite(buildings_final, file = "urbansim_buildings_all_pierce.csv")
    db <- paste0(tolower(county), "_2023_parcel_baseyear")
    connection <- mysql.connection(db)
    dbWriteTable(connection, "urbansim_parcels", parcels_final[!is.na(parcel_id) & parcel_id != 0], overwrite = TRUE, row.names = FALSE)
    dbWriteTable(connection, "urbansim_buildings", buildings_final[!is.na(parcel_id) & parcel_id != 0], overwrite = TRUE, row.names = FALSE)
    dbWriteTable(connection, "urbansim_parcels_all", parcels_final, overwrite = TRUE, row.names = FALSE)
    dbWriteTable(connection, "urbansim_buildings_all", buildings_final, overwrite = TRUE, row.names = FALSE)
    DBI::dbDisconnect(connection)
}
