# Script to create parcels and buildings tables from King assessor data
# for the use in urbansim 
#
# Hana Sevcikova, last update 11/06/2023
#

library(data.table)

county <- "King"

data.dir <- file.path("~/e$/Assessor23", county) # path to the Assessor text files
data.dir <- "King_data"
misc.data.dir <- "data" # path to the BY2023/data folder
write.result.to.mysql <- FALSE # it will overwrite the existing tables urbansim_parcels & urbansim_buildings

if(write.result.to.mysql) source("mysql_connection.R")

############
# Functions
############

construct_pin_from_major_minor <- function(dt){
    # construct column "pin" by concatenating the major & minor columns while adding leading zeros
    zero_templ <- sapply(0:9, function(x) paste0(rep(0, x), collapse = ""))
    dt[, `:=`(major.char = as.character(major), minor.char = as.character(minor))][      # convert major & minor to character
        , `:=`(add0.major = 6 - nchar(major.char), add0.minor = 4 - nchar(minor.char))][ # how many zeros should be added
            , `:=`(major.char = paste0(zero_templ[add0.major + 1], major.char),          # add zeros
                minor.char = paste0(zero_templ[add0.minor + 1], minor.char))][
                    , pin := paste0(major.char, minor.char)][,   # concatenate into one column
                        `:=`(major.char = NULL, minor.char = NULL, add0.major = NULL, add0.minor = NULL) # cleanup
                    ]
    dt
}

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

# buildings tables
aptcomplex <- fread(file.path(data.dir, "EXTR_AptComplex.csv"))
commbldg <- fread(file.path(data.dir, "EXTR_CommBldg2.csv"))
condocomplex <- fread(file.path(data.dir, "EXTR_CondoComplex.csv"))
condounit <- fread(file.path(data.dir, "EXTR_CondoUnit2.csv"))
resbldg <- fread(file.path(data.dir, "EXTR_ResBldg.csv"))

###################
# Process parcels
###################

# remove duplicates and zero urbansim parcel id (pin_1)
parcels.nodupl <- parcels.23to18[!(duplicated(pin) | pin_1 == 0)]
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

# # construct a column (acctnbr2) that will be used for joining with parcels
# rpacct.nodupl[, `:=`(acctnbr2 = substr(acctnbr, 1, nchar(acctnbr) - 2), # remove last two digits
#                      exempt = taxstat != "T")][,
#                         add0 := 10 - nchar(acctnbr2) # how many leading zeros to add
#                 ]
# zero_templ <- sapply(0:9, function(x) paste0(rep(0, x), collapse = ""))
# rpacct.nodupl[, acctnbr2 := paste0(zero_templ[add0+1], acctnbr2)] # add leading zeros
# 

# construct column "pin" in the rpacct table
rpacct.nodupl <- construct_pin_from_major_minor(rpacct.nodupl)

# group records with the same acctnbr2
rpacct_grouped <- rpacct.nodupl[, .(apprlandval = sum(apprlandval), apprimpsval = sum(apprimpsval),
                                    exempt = sum(taxstat != "T")), by = "pin"]

# construct column "pin" in parcel table
parcel <- construct_pin_from_major_minor(parcel)


# join with parcels
prep_parcels <- merge(
    merge(parcels.nodupl, rpacct_grouped, by = "pin", all.x = TRUE),
    parcel[, .(pin, use_code = as.character(presentuse), propname, proptype, currentzoning, sqftlot, nbrbldgsites)], 
    by = "pin", all.x = TRUE)

cat("\n", nrow(rpacct_grouped[!pin %in% prep_parcels[, pin]]), "records from rpacct were not matched with parcels.\n",
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
# set column names to lower case
colnames(aptcomplex) <- tolower(colnames(aptcomplex))
colnames(commbldg) <- tolower(colnames(commbldg))
colnames(condocomplex) <- tolower(colnames(condocomplex))
colnames(condounit) <- tolower(colnames(condounit))
colnames(resbldg) <- tolower(colnames(resbldg))

prep_buildings_apt <- construct_pin_from_major_minor(aptcomplex)[
    , .(pin, stories = nbrstories, units = nbrunits, sqft_per_unit = avgunitsize,
        building_quality = bldgquality, year_built = yrbuilt)]

cat("\nNumber of apartments:", nrow(prep_buildings_apt))

prep_buildings_condo <- construct_pin_from_major_minor(condocomplex[, minor := "0000"])[
    , .(pin, major, stories = nbrstories, units = nbrunits, sqft_per_unit = avgunitsize,
        building_quality = bldgquality, year_built = yrbuilt)]

cat("\nNumber of condo complexes:", nrow(prep_buildings_condo))

prep_buildings_com <- construct_pin_from_major_minor(commbldg)[
    , .(pin, stories = nbrstories, building_number = bldgnbr,
        use_code = predominantuse, building_quality = bldgquality, 
        bldg_gross_sqft = bldggrosssqft, bldg_net_sqft = bldgnetsqft,
        year_built = yrbuilt, residential_units = 0, sqft_per_unit = 1)]

cat("\nNumber of commercial buildings:", nrow(prep_buildings_com))

# sum land and improvement values over condos in condo complexes
condo_value <- merge(construct_pin_from_major_minor(condounit), rpacct_grouped, by = "pin")
condo_land_imp_value <- condo_value[, .(land_value = sum(apprlandval), improvement_value = sum(apprimpsval)),
                                    by = "major"]
prep_buildings_condo <- merge(prep_buildings_condo, condo_land_imp_value, by = "major")

# cases where a building is in both tables, apartment and commercial
aptcom_combo <- merge(prep_buildings_apt[, .(pin, units, sqft_per_unit_apt = sqft_per_unit)], 
                      prep_buildings_com, by = "pin")
# set residential units for each commercial records using sqft proportions 
aptcom_combo[, `:=`(count = .N, gross_sqft = sum(bldg_gross_sqft)), by = "pin"][
    , proportion := bldg_gross_sqft/gross_sqft][, residential_units := units * proportion]
cat("\nNumber of buildings in both tables, apartment and commercial:", nrow(aptcom_combo))

# insert the derived residential units into the commercial buildings
prep_buildings_com[aptcom_combo, residential_units := i.residential_units, on = c("pin", "building_number")]

# cases when a record is in both, condo and commercial (adjust the same way as above)
condocom_combo <- merge(prep_buildings_condo[, .(pin, units, sqft_per_unit_apt = sqft_per_unit)], 
                      prep_buildings_com, by = "pin")[!pin %in% aptcom_combo[, pin]] # only if it is not in the appartment table
# set residential units for each commercial records using sqft proportions 
condocom_combo[, `:=`(count = .N, gross_sqft = sum(bldg_gross_sqft)), by = "pin"][
    , proportion := bldg_gross_sqft/gross_sqft][, residential_units := units * proportion]
cat("\nNumber of buildings in both tables, condo and commercial:", nrow(aptcom_combo))

# insert the derived residential units into the commercial buildings
prep_buildings_com[condocom_combo, residential_units := i.residential_units, on = c("pin", "building_number")]

# construct remaining residential buildings
prep_buildings_res <- construct_pin_from_major_minor(resbldg[
    , .(major, minor, bldgnbr, nbrlivingunits, address, stories, sqfttotliving,
        yrbuilt, building_quality_id = condition)])

# set use_code depending on number of units
for (units_code in c(1:6, 9, 20))
    prep_buildings_res[nbrlivingunits == units_code, use_code := units_code]

# put everything together
prep_buildings <- rbind(
    prep_buildings_res[
        , .(pin, stories, residential_units = nbrlivingunits, year_built = yrbuilt,
            gross_sqft = sqfttotliving, sqft_per_unit = ceiling(sqfttotliving/nbrlivingunits), 
            building_quality_id, use_code, non_residential_sqft = 0)],
    prep_buildings_condo[
        ! pin %in% condocom_combo[, pin],
        .(pin, stories, residential_units = units, year_built, sqft_per_unit, 
          building_quality_id = building_quality, use_code = 800, non_residential_sqft = 0)
                         ],
    prep_buildings_apt[
        ! pin %in% aptcom_combo[, pin],
        .(pin, stories, residential_units = units, year_built, sqft_per_unit, 
          building_quality_id = building_quality, use_code = 300, non_residential_sqft = 0)
                        ], 
    prep_buildings_com[
        , .(pin, stories, residential_units, year_built,
            gross_sqft = bldg_gross_sqft, non_residential_sqft = bldg_net_sqft,
            sqft_per_unit, building_quality_id = building_quality, use_code)],
    fill = TRUE
    )

# calculate improvement value
prep_buildings[parcels_final, `:=`(total_improvement_value = i.improvement_value), 
               on = c(pin = "parcel_id_fips")]
prep_buildings[, total_sqft := sum(gross_sqft), by = "pin"]
               

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
