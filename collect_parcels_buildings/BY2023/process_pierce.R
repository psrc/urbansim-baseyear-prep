# Script to create parcels and buildings tables from Pierce assessor data
# for the use in urbansim 
#
# Hana Sevcikova, last update 10/16/2023
#

library(data.table)
#library(bit64)

county <- "Pierce"

data.dir <- file.path("~/e$/Assessor23", county) # path to the Assessor text files
misc.data.dir <- "data" # path to the BY2023/data folder
write.result.to.mysql <- TRUE # it will overwrite the existing tables urbansim_parcels & urbansim_buildings

if(write.result.to.mysql) source("mysql_connection.R")

###############
# Load all data
###############

# parcels & tax accounts
parcels.23to18 <- fread(file.path(data.dir, "parcels23_pie_to_2018_parcels_short.txt"))
tax <- fread(file.path(data.dir, "tax_account.txt"), sep = "|", quote = "") # 

# reclass tables
lu_reclass <- fread(file.path(misc.data.dir, "land_use_generic_reclass_2023.csv"))
bt_reclass <- fread(file.path(misc.data.dir, "building_use_generic_reclass_2023.csv"))

# buildings
raw_buildings <- fread(file.path(data.dir, "improvement_builtas.txt"))
improvement <- fread(file.path(data.dir, "improvement.txt"))
improvement_detail <- fread(file.path(data.dir, "improvement_detail.txt"))

###################
# Process parcels
###################
# make column names lowercase
colnames(parcels.23to18) <- tolower(colnames(parcels.23to18))

# remove duplicates (just take the first record of each duplicate)
parcels.23to18[, taxparceln := as.double(taxparceln)]
parcels.nodupl <- parcels.23to18[!duplicated(taxparceln)]
cat("\nNumber of duplicates removed from parcels: ", nrow(parcels.23to18) - nrow(parcels.nodupl), "\n")

# extract a subset of columns (not all of these are actually needed)
prep_parcels <- parcels.nodupl[, .(taxparceln, taxparcelt, taxparcell, taxparcelu,
                                   pin, taxable_va, point_x, point_y, poly_area,
                                   site_addre, zipcode, land_acres, land_value,
                                   improvemen, use_code, landuse_de, exemption_)
                                ]
# Q: should the records be filtered for taxparcelt %in% c('Base Parcel', 'Tax Purpose Only') ?

tax[, taxparceln := as.double(parcel_number)]
tax.nodupl <- tax[!duplicated(taxparceln)]

# if land value is zero, try to get the current year info in the tax dataset 
zero_lv <- nrow(prep_parcels[land_value== 0])
prep_parcels[tax.nodupl, land_value := ifelse(land_value == 0 & !is.na(i.land_value_current_year) & i.land_value_current_year > 0, 
                                  i.land_value_current_year, land_value),
             on = "taxparceln"]
cat("\nImputed land value into", zero_lv - nrow(prep_parcels[land_value== 0]), "records.\n")

# the same for improvement value
zero_imp <- nrow(prep_parcels[improvemen == 0])
prep_parcels[tax.nodupl, improvemen := ifelse(improvemen == 0 & !is.na(i.improvement_value_current_year) & i.improvement_value_current_year > 0, 
                                       i.improvement_value_current_year, improvemen),
             on = "taxparceln"]
cat("\nImputed improvement value into", zero_imp - nrow(prep_parcels[improvemen == 0]), "records.\n")

# Now if land value is zero, try to get the prior year in the tax dataset and add 10% inflation
zero_lv <- nrow(prep_parcels[land_value== 0])
prep_parcels[tax.nodupl, land_value := ifelse(
    land_value == 0 & !is.na(i.land_value_prior_year) & i.land_value_prior_year > 0, 
    i.land_value_prior_year * 1.1, land_value),
             on = "taxparceln"]
cat("\nImputed land value into additional", zero_lv - nrow(prep_parcels[land_value== 0]), "records using prior year info.\n")

# do the same for improvement value
zero_imp <- nrow(prep_parcels[improvemen == 0])
prep_parcels[tax.nodupl, improvemen := ifelse(
    improvemen == 0 & !is.na(i.improvement_value_prior_year) & i.improvement_value_prior_year > 0, 
    i.improvement_value_prior_year * 1.1, improvemen),
             on = "taxparceln"]
cat("\nImputed improvement value into additional", zero_imp - nrow(prep_parcels[improvemen == 0]), "records using prior year info.\n")

parcels_final <- prep_parcels[, .(
    parcel_id = pin, parcel_id_fips = taxparceln, land_value, use_code = as.character(use_code), 
    gross_sqft = round(poly_area),  x_coord_sp = point_x, y_coord_sp = point_y, address = site_addre,
    zip_id = zipcode, county_id = 53)][, exemption := ifelse(exemption_ == "", 0, 1)]
    
# join with reclass table
parcels_final[lu_reclass[county_id == 53], land_use_type_id := i.land_use_type_id, 
              on = c(use_code = "county_land_use_code")]
cat("\nThe following codes were not found:\n")
print(parcels_final[is.na(land_use_type_id) & !is.na(use_code), .N, by = "use_code"][order(use_code)])

###################
# Process buildings
###################

# group buildings since there were duplicate building_id values per parcel
buildings_grouped <- raw_buildings[, .(
    sqft = sum(built_as_square_feet), units = sum(units), bedrooms = sum(bedrooms),
    year_built = min(year_built), count = .N), 
    by = .(building_id, parcel_number)]

# join with improvement table
prep_buildings <- merge(improvement, buildings_grouped, 
                        by = c("building_id", "parcel_number"),
                        all = TRUE)

# Calculate improvement value proportionally to the sqft
prep_buildings[prep_parcels, total_improvement_value := i.improvemen, 
               on = c(parcel_number = "taxparceln")]
prep_buildings[, `:=`(total_sqft = sum(sqft), count = .N), by = "parcel_number"]
prep_buildings[, `:=`(improvement_value = round(sqft/total_sqft * total_improvement_value))]

# join with building reclass table
prep_buildings[, primary_occupancy_code := as.character(primary_occupancy_code)]
prep_buildings[bt_reclass, building_type_id := i.building_type_id, 
               on = c(primary_occupancy_code = "county_building_use_code")]
cat("\nMatched", nrow(prep_buildings[!is.na(building_type_id)]), "records with building reclass table")
cat("\nUnmatched: ", nrow(prep_buildings[is.na(building_type_id)]), "records.")
cat("\nThe following building codes were not found:\n")
print(prep_buildings[is.na(building_type_id), .N, by = "primary_occupancy_code"][order(-N)])

# Clean up the units column
prep_buildings[is.na(units), units := 0]
# set units to zero for non-residential records
index_residential <- with(prep_buildings, building_type_id %in% c(4, 11, 12, 19))
prep_buildings[!index_residential, units := 0]

# impute units where residential & units is 0
# TODO: check the total number of units on parcels as the units can be assigned to 
# other building on that parcel
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
# Triplex
prep_buildings[building_type_id == 12 & units == 0  &
                   primary_occupancy_description %in% c('Triplex'),
               units := 3]

prep_buildings[index_residential & units == 0 & 
                   primary_occupancy_description %in% c(
                       "Apartment High Rise", "Apartment w/4-8 Units"),
               units := pmax(1, round(sqft/830))]

buildings_final <- prep_buildings[parcels_final, .(
    building_id = 1:nrow(prep_buildings), building_id_orig = building_id,
    parcel_id = i.parcel_id, gross_sqft = sqft, sqft_per_unit = sqft/units,
    year_built, residential_units = units, improvement_value,
    use_code = primary_occupancy_code, building_type_id
                                      ), 
    on = c(parcel_number = "taxparceln")]

# TODO: Question: The mysql script has a segment processing stacked parcels.
#                   Do we need it?

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
