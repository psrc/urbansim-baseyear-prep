# Script to create parcels and buildings tables from Pierce assessor data
# for the use in urbansim 
# It generates 3 tables: 
#    urbansim_parcels, urbansim_buildings, building_type_crosstab
#
# Hana Sevcikova, last update 06/10/2024
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

############
# Functions
############
fill.zeros <- function(values, l = 10){
    prefix.length <- l - nchar(values)
    prefix <- sapply(prefix.length, function(x) if(x == 0) "" else paste(rep(0, x), collapse = ""))
    return(paste0(prefix, values))
}


###############
# Load all data
###############

# parcels & tax accounts
parcels.base <- fread(file.path(data.dir, "parcels23_pie_base.csv"), 
                      colClasses = c(parcel_id = "character", taxparceln = "character"))
parcels.full <- fread(file.path(data.dir, "tax_parcels.csv"),          
                      colClasses = c(TaxParcelN = "character")) # file with all attributes
stacked <- fread(file.path(data.dir, "parcels23_stacked_pins.csv"),
                 colClasses = c(new_parcel_id = "character", taxparceln = "character"))
tax <- fread(file.path(data.dir, "tax_account.txt"), sep = "|", quote = "") # 

# reclass tables
lu_reclass <- fread(file.path(misc.data.dir, "land_use_generic_reclass_2023.csv"))
bt_reclass <- fread(file.path(misc.data.dir, "building_use_generic_reclass_2023.csv"))

# buildings
raw_buildings <- fread(file.path(data.dir, "improvement_builtas.txt"))
improvement <- fread(file.path(data.dir, "improvement.txt"))

# Info about fake parcels and buildings (prepared by Mark)
fake_parcels <- fread(file.path(data.dir, "pierce_fake_parcel_changes.csv"), 
                      colClasses = c(Orig_TaxParcelN = "character", New_TaxParcelN = "character"))
fake_buildings <- fread(file.path(data.dir, "pierce_fake_building_changes.csv"),
                        colClasses = c(orig_parcel_number = "character", new_parcel_number = "character"))

###################
# Process parcels
###################
# Pierce ID: taxparceln, parcel_id (they should be the same)

cat("\nProcessing Pierce parcels\n=========================\n")

# make column names lowercase
colnames(parcels.full) <- tolower(colnames(parcels.full))
colnames(fake_parcels) <- tolower(colnames(fake_parcels))

# type change & re-assignment to new parcel ids due to stacked parcels 
#parcels.base[, `:=`(taxparceln = as.character(taxparceln))]
#parcels.full[, `:=`(taxparceln = as.character(taxparceln), taxparceln_old = as.character(taxparceln))]
parcels.full[, `:=`(taxparceln_old = taxparceln)]
#stacked[, `:=`(taxparceln = as.character(taxparceln), new_parcel_id = as.character(new_parcel_id))]
parcels.full[stacked, taxparceln := i.new_parcel_id, on = c(taxparceln_old = "taxparceln")]
#fake_parcels[, `:=`(orig_taxparceln = as.character(orig_taxparceln),
#                    new_taxparceln = as.character(new_taxparceln))]

# join fake parcels with the full attribute dataset
if(nrow((old.fakes <- fake_parcels[!is.na(orig_taxparceln) & orig_taxparceln != ""])) > 0){ # this section is untested since there are no such parcels
    fake_parcels[parcels.full[taxparceln %in% old.fakes[, orig_taxparceln]], 
                 `:=`(land_value = i.land_value * land_proportion,
                      improvemen = i.improvemen * land_proportion,
                      exemption_ = i.exemption_
                      ), 
             on = c(orig_taxparceln = "taxparceln")]
    parcels.full <- parcels.full[! taxparceln %in% unique(old.fakes[, orig_taxparceln])]
} else {
    fake_parcels[, `:=`(land_value = 0, improvemen = 0, exemption_ = "")]
}
new.fakes.add <- fake_parcels[!(new_taxparceln %in% parcels.full[, taxparceln])] # parcels to add
new.fakes.mod <- fake_parcels[(new_taxparceln %in% parcels.full[, taxparceln])] # parcels to modify
if(nrow(new.fakes.add) > 0){
    # fake parcels to add
    parcels.full <- rbind(parcels.full, new.fakes.add[
        , .(taxparceln = new_taxparceln, use_code = new_use_code, 
            landuse_de = new_landuse_de,
            land_value, improvemen, exemption_
            )],
        fill = TRUE)
}
if(nrow(new.fakes.mod) > 0){
    # fake parcels to modify (untested; currently no such case)
    parcels.full[new.fakes.mod, `:=`(use_code = i.new_use_code, landuse_de = i.new_landuse_de,
                                     land_value = i.land_value, improvemen = i.improvemen, 
                                     exemption_ = i.exemption_
                                     ), on = c(taxparceln = "new_taxparceln")]
}
cat("\nIncorporated info for ", nrow(fake_parcels[new_taxparceln %in% parcels.full[, taxparceln]]),
    "fake parcels")

# aggregate attributes in parcels.full
parcels.aggr <- parcels.full[, .(land_value = sum(land_value), 
                                 improvement_value = sum(improvemen),
                                 exemption = !all(ifelse(exemption_ == "", 0, 1)==0)), 
                             by = "taxparceln"]

# this is for cases that were re-assigned to a new id, but the old id still exists in base parcels (there are just two such parcels)
parcels.aggr.by.old.id <- parcels.full[, .(land_value = sum(land_value), 
                                 improvement_value = sum(improvemen),
                                 exemption = !all(ifelse(exemption_ == "", 0, 1)==0)), 
                             by = c("taxparceln", "taxparceln_old")]

# for land use code, use the base parcel code
parcels.full[, is_base := taxparcelt == "Base Parcel" & taxparceln == taxparceln_old][, nbase := sum(is_base), by = "taxparceln"]
parcels.full[nbase == 0, is_base := taxparceln == taxparceln_old]
parcels.full[parcels.full[is_base == TRUE, .(use_code = use_code[1]), by = "taxparceln"], use_code_base := i.use_code, on = "taxparceln"]

# merge base parcels with the aggregations
prep_parcels <- merge(parcels.base, 
                      rbind(parcels.aggr[, .(taxparceln, land_value, improvement_value, exemption)], 
                            parcels.aggr.by.old.id[! taxparceln_old %in% parcels.aggr[, taxparceln] & 
                                                       taxparceln_old %in% parcels.base[, taxparceln], 
                                                   .(taxparceln = taxparceln_old, land_value, improvement_value, exemption)]
                            ),
                      by = "taxparceln", all.x = TRUE)

tax[, taxparceln := as.character(parcel_number)]
tax.nodupl <- tax[!duplicated(taxparceln)] # if there would be duplicates, one would probably need to sum the land value 
                                           # instead of removing records (not sure)
cat("\nNumber of duplicates removed from Pierce parcels: ", nrow(tax) - nrow(tax.nodupl))

# if land value is zero, try to get the current year info in the tax dataset 
zero_lv <- nrow(prep_parcels[land_value== 0])
prep_parcels[tax.nodupl, land_value := ifelse(land_value == 0 & !is.na(i.land_value_current_year) & i.land_value_current_year > 0, 
                                              i.land_value_current_year, land_value),
             on = "taxparceln"]
cat("\nImputed land value into", zero_lv - nrow(prep_parcels[land_value== 0]), "records.")

# the same for improvement value
zero_imp <- nrow(prep_parcels[improvement_value == 0])
prep_parcels[tax.nodupl, improvement_value := ifelse(improvement_value == 0 & !is.na(i.improvement_value_current_year) & i.improvement_value_current_year > 0, 
                                              i.improvement_value_current_year, improvement_value),
             on = "taxparceln"]
cat("\nImputed improvement value into", zero_imp - nrow(prep_parcels[improvement_value == 0]), "records.")

# Now if land value is zero, try to get the prior year in the tax dataset and add 10% inflation
zero_lv <- nrow(prep_parcels[land_value== 0])
prep_parcels[tax.nodupl, land_value := ifelse(
    land_value == 0 & !is.na(i.land_value_prior_year) & i.land_value_prior_year > 0, 
    i.land_value_prior_year * 1.1, land_value),
    on = "taxparceln"]
cat("\nImputed land value into additional", zero_lv - nrow(prep_parcels[land_value== 0]), "records using prior year info.")

# do the same for improvement value
zero_imp <- nrow(prep_parcels[improvement_value == 0])
prep_parcels[tax.nodupl, improvement_value := ifelse(
    improvement_value == 0 & !is.na(i.improvement_value_prior_year) & i.improvement_value_prior_year > 0, 
    i.improvement_value_prior_year * 1.1, improvement_value),
    on = "taxparceln"]
cat("\nImputed improvement value into additional", zero_imp - nrow(prep_parcels[improvement_value == 0]), "records using prior year info.")

# assign use codes
prep_parcels[unique(parcels.full[, .(taxparceln, use_code)]), use_code := i.use_code, on = "taxparceln"]
cat("\nuse_code is NA for", nrow(prep_parcels[is.na(use_code)]), "parcels.")

# assemble columns for final parcels table
parcels_final <- prep_parcels[, .(
    parcel_id = 1:nrow(prep_parcels), parcel_id_fips = as.character(taxparceln), land_value, use_code = as.character(use_code), 
    gross_sqft = round(gis_sqft),  x_coord_sp = point_x, y_coord_sp = point_y, 
    exemption, county_id = county.id)]
    
# join with reclass table
parcels_final[lu_reclass[county_id == county.id], land_use_type_id := i.land_use_type_id, 
              on = c(use_code = "county_land_use_code")]
if(nrow(misslu <- parcels_final[is.na(land_use_type_id) & !is.na(use_code), .N, by = "use_code"]) > 0){
    cat("\nThe following land use codes were not found:\n")
    print(misslu[order(use_code)])
} else cat("\nAll land use codes matched.")

cat("\nTotal all:", nrow(parcels_final), "parcels")

###################
# Process buildings
###################

cat("\n\nProcessing Pierce buildings\n=========================\n")

# join with improvement table
buildings_tmp <- merge(improvement, raw_buildings, 
                       by = c("building_id", "parcel_number"),
                       all = TRUE)
buildings_tmp[, `:=`(parcel_number_orig = as.character(parcel_number))][, parcel_number := NULL]
buildings_tmp[, `:=`(parcel_number_orig = fill.zeros(buildings_tmp$parcel_number_orig))]

# join with parcels and assign the right taxparceln
buildings_tmp <- merge(buildings_tmp, stacked, by.x = "parcel_number_orig", by.y = "taxparceln", all.x = TRUE)
buildings_tmp[, taxparceln := ifelse(is.na(new_parcel_id), parcel_number_orig, new_parcel_id)]

# check duplicates 
# buildings_tmp[duplicated(buildings_tmp, by = c("building_id", "taxparceln"))]

# group buildings since there were duplicate building_id values per parcel
buildings_all <- buildings_tmp[, .(
    sqft = sum(built_as_square_feet, na.rm = TRUE), units = sum(units, na.rm = TRUE), 
    bedrooms = sum(bedrooms, na.rm = TRUE), stories = max(stories),
    year_built = min(year_built), square_feet = sum(square_feet, na.rm = TRUE),
    net_square_feet = sum(net_square_feet, na.rm = TRUE)), 
    by = .(primary_occupancy_code, primary_occupancy_description, built_as_id, built_as_description, taxparceln)]

# add fake buildings
new_fake_buildings <- fake_buildings[is.na(orig_parcel_number) | orig_parcel_number == "", 
                                     .(taxparceln = fill.zeros(fake_buildings$new_parcel_number), 
                                       primary_occupancy_code = new_primary_occupancy_code,
                                       primary_occupancy_description = new_primary_occupancy_description,
                                       built_as_id = new_primary_occupancy_code,
                                       built_as_description = new_primary_occupancy_description,
                                       square_feet = new_square_feet,
                                        year_built = NA)]

nbld <- nrow(buildings_all)
buildings_all <- rbind(buildings_all, new_fake_buildings, fill = TRUE)
cat("\nAdded ", nrow(buildings_all) - nbld, " fake buidlings.")

buildings_all[, count:= .N, by = "taxparceln"]

# preliminary join with building reclass table
buildings_all[, `:=`(primary_occupancy_code = as.character(primary_occupancy_code), 
                      built_as_id = as.character(built_as_id))]
buildings_all[bt_reclass[county_id == county.id], building_type_id := i.building_type_id, 
               on = c(primary_occupancy_code = "county_building_use_code")]
buildings_all[bt_reclass[county_id == county.id], built_as_building_type_id := i.building_type_id, 
               on = c(built_as_id = "county_building_use_code")]

# remove detached garages
nbld <- nrow(buildings_all)
buildings_all <- buildings_all[!((!is.na(built_as_id) & built_as_id == 99) | (!is.na(primary_occupancy_code) & primary_occupancy_code == 99))]
cat("\nDropped", nbld - nrow(buildings_all), "detached garages.")
    
# reclass some non-res types that appear in residential buildings
# check types via 
# buildings_all[count > 1 & building_type_id %in% c(4, 12, 19, 11) & ! is.na(built_as_building_type_id) & ! built_as_building_type_id %in% c(4, 12, 19, 11), .N, by = c("built_as_id", "built_as_description")][order(built_as_description)]
buildings_all[count > 1 & building_type_id %in% c(4, 12, 19, 11) & 
                   ! is.na(built_as_building_type_id) & ! built_as_building_type_id %in% c(4, 12, 19, 11) & 
                  !built_as_id == 124,
               `:=`(primary_occupancy_code = built_as_id, units = 0)]

# for warehouses, take the alternative type 
buildings_all[count > 1 & building_type_id == 21 & 
                  ! is.na(built_as_building_type_id) & built_as_building_type_id != 21,
               `:=`(primary_occupancy_code = built_as_id, 
                    units = ifelse(built_as_building_type_id %in% c(4, 12, 19, 11), units, 0))]
              
# remove laundromats & clubhouses in res buildings
nbld <- nrow(buildings_all)
buildings_all <- buildings_all[!((!is.na(built_as_id) & built_as_id == 99) | (!is.na(primary_occupancy_code) & primary_occupancy_code == 99))]
buildings_all <- buildings_all[!(building_type_id %in% c(4, 11, 12, 19) & built_as_id %in% c(336, 311) & units == 0)]
    cat("\nDropped", nbld - nrow(buildings_all), "laundromats and clubhouses in residential buildings.")
    
# rerun the join on reclass table as the occupancy code might have changed
buildings_all[bt_reclass[county_id == county.id], building_type_id := i.building_type_id, 
              on = c(primary_occupancy_code = "county_building_use_code")]

prep_buildings <- buildings_all[taxparceln %in% parcels_final[, parcel_id_fips]]
cat("\nDropped", nrow(buildings_all) - nrow(prep_buildings), "buildings due to missing representation in the parcels dataset.")

# Calculate improvement value proportionally to the sqft
prep_buildings[prep_parcels, `:=`(total_improvement_value = i.improvement_value), 
               on = "taxparceln"]
prep_buildings[, `:=`(sqft_tmp = pmax(1, sqft, na.rm = TRUE))]
prep_buildings[, `:=`(total_sqft = sum(sqft_tmp), count = .N), by = "taxparceln"]
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
                       "Apartment High Rise", "Apartment w/4-8 Units", "Apt Low Rise up to 19 Units"),
               units := pmax(1, round(sqft/830))]

# get sum of DU on parcels
prep_buildings[index_residential, `:=`(sum_du = sum(units),
                                       has_zero_units = any(units == 0),
                                       has_nonzero_units = any(units > 0),
                                       nbuildings = .N), 
               by = "taxparceln"]

# For Apt Low Rise 20-99 and 100 Units Plus impute by using sqft, but only for those records 
# where the sum of DU on the parcel is < 50, otherwise put just one unit there
prep_buildings[index_residential & units == 0  & 
                   primary_occupancy_description %in% c("Apt Low Rise 100 Units Plus", "Apt Low Rise 20 to 99 Units"),
               units := ifelse(sum_du < 50, pmax(1, round(sqft/830)), 1)]

# For condos, impute only if the sum of units on the parcel is smaller than the median 
# check which make sense with 
# prep_buildings[index_residential & units == 0, .N, by = "primary_occupancy_description"]
for (descr in c("Condo Low Rise", "Condo High Rise", "Condo Apartment Low Rise")){
    med.value <- prep_buildings[primary_occupancy_description == descr & units > 0, median(units)]
    prep_buildings[index_residential & units == 0 & sum_du < med.value & primary_occupancy_description == descr, units := pmax(1, round(sqft/830))]
}

cat("\nDue to missing sqft units were not imputed into building with the following occupancy description:\n")
print(prep_buildings[index_residential & units == 0, .N, by = "primary_occupancy_description"])

prep_buildings[parcels_final, parcel_id := i.parcel_id, on = c(taxparceln = "parcel_id_fips")]

# assemble columns for final buildings table
buildings_final <- prep_buildings[, .(
    building_id = 1:nrow(prep_buildings), 
    parcel_id, parcel_id_fips = taxparceln, gross_sqft = sqft, sqft_per_unit = ceiling(sqft/units),
    year_built, residential_units = units, non_residential_sqft, stories, improvement_value, 
    land_area = round(sqft/stories),
    use_code = primary_occupancy_code, building_type_id
)]

cat("\nTotal all: ", nrow(buildings_final), "buildings")

# generate building type crosstabs
bt.tab <- buildings_final[, .(N = .N, DU = sum(residential_units), 
                             nonres_sqft = sum(non_residential_sqft)), 
                          by = .(use_code, building_type_id)]
bt.tab <- merge(bt.tab, bt_reclass[county_id == county.id, .(county_building_use_code, county_building_use_description, building_type_id, building_type_name)],
                by.x = c("use_code", "building_type_id"), by.y = c("county_building_use_code", "building_type_id"),
                all.x = TRUE)
setcolorder(bt.tab, c("use_code", "county_building_use_description", "building_type_id", "building_type_name"))

if(write.result){
    fwrite(parcels_final, file = "urbansim_parcels_pierce.csv")
    fwrite(buildings_final, file = "urbansim_buildings_pierce.csv")
    db <- paste0(tolower(county), "_2023_parcel_baseyear")
    connection <- mysql.connection(db)
    dbWriteTable(connection, "urbansim_parcels", parcels_final, overwrite = TRUE, row.names = FALSE)
    dbWriteTable(connection, "urbansim_buildings", buildings_final, overwrite = TRUE, row.names = FALSE)
    dbWriteTable(connection, "building_type_crosstab", bt.tab, overwrite = TRUE, row.names = FALSE)
    DBI::dbDisconnect(connection)
}

## Output on 2024/6/17
##############################
# Processing Pierce parcels
# =========================
#     
#     Incorporated info for  12 fake parcels
# Number of duplicates removed from Pierce parcels:  0
# Imputed land value into 0 records.
# Imputed improvement value into 0 records.
# Imputed land value into additional 72 records using prior year info.
# Imputed improvement value into additional 91 records using prior year info.
# use_code is NA for 32 parcels.
# All land use codes matched.
# Total all: 309233 parcels
# 
# Processing Pierce buildings
# =========================
#     
#     Added  16  fake buidlings.
# Dropped 34394 detached garages.
# Dropped 115 laundromats and clubhouses in residential buildings.
# Dropped 13167 buildings due to missing representation in the parcels dataset.
# Matched 285739 records with building reclass table
# Unmatched:  0 records.
# All building use codes matched.
# Due to missing sqft units were not imputed into building with the following occupancy description:
#     primary_occupancy_description N
# 1:      Gen Warehouse up to 19,999 SF 1
# 2: Gen Warehouse 20,000 to 199,999 SF 1
# 3:                     Condo Low Rise 1
# 4:                    Condo High Rise 2
# 5:           Condo Apartment Low Rise 1
# 
# Total all:  285739 buildings
