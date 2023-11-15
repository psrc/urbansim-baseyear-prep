# Script to create parcels and buildings tables from King assessor data
# for the use in urbansim 
# It generates 4 tables: 
#    urbansim_parcels, urbansim_buildings: contain records that are found in BY2018
#    urbansim_parcels_all, urbansim_buildings_all: all records regardless if they are found in BY2018
#
# Hana Sevcikova, last update 11/15/2023
#

library(data.table)

county <- "King"
county.id <- 33

data.dir <- file.path("~/e$/Assessor23", county) # path to the Assessor text files
#data.dir <- "King_data"
misc.data.dir <- "data" # path to the BY2023/data folder
write.result <- TRUE # it will overwrite the existing tables urbansim_parcels & urbansim_buildings

if(write.result) source("mysql_connection.R")

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
# King ID: pin = major + minor
# urbansim ID: pin_1

cat("\nProcessing King parcels\n=========================\n")
# remove duplicates
parcels.nodupl <- parcels.23to18[!duplicated(pin)]
cat("\nNumber of duplicates removed from 23to18 parcels: ", nrow(parcels.23to18) - nrow(parcels.nodupl))

# make column names lowercase
colnames(rpacct) <- tolower(colnames(rpacct))
rpacct[, acctnbr := as.character(acctnbr)]

colnames(parcel) <- tolower(colnames(parcel))

# It looks like all duplicates in rpacct have zero land and improvement values, 
# thus we can just simply remove the duplicates
# checked with the following which returned zero rows: 
# rpacct[acctnbr %in% rpacct[duplicated(acctnbr), acctnbr]][order(acctnbr)][apprimpsval > 0 | apprlandval > 0]
rpacct.nodupl <- rpacct[!duplicated(acctnbr)]

# construct column "pin" in the rpacct table
rpacct.nodupl <- construct_pin_from_major_minor(rpacct.nodupl)

# group records with the same acctnbr2
rpacct_grouped <- rpacct.nodupl[, .(apprlandval = sum(apprlandval), apprimpsval = sum(apprimpsval),
                                    exempt = sum(taxstat != "T")), by = "pin"]

# construct column "pin" in parcel table
parcel <- construct_pin_from_major_minor(parcel)

# join with parcels
prep_parcels <- merge(
    merge(parcels.nodupl, rpacct_grouped, by = "pin", all = TRUE),
    parcel[, .(pin, use_code = as.character(presentuse), propname, proptype, currentzoning, sqftlot, nbrbldgsites)], 
    by = "pin", all = TRUE)

cat("\n", nrow(rpacct_grouped[!pin %in% prep_parcels[, pin]]), "records from rpacct were not matched with parcels.\n",
    nrow(prep_parcels[is.na(apprlandval)]), "parcel records did not have a record in rpacct.")
cat("\n", nrow(parcel[!pin %in% prep_parcels[, pin]]), "records from extr_parcel were not matched with 23to18 parcels.\n",
    nrow(prep_parcels[is.na(use_code)]), " 23to18 parcel records did not have a record in extr_parcel.")

# join with reclass table
prep_parcels[lu_reclass[county_id == county.id], land_use_type_id := i.land_use_type_id, 
                    on = c(use_code = "county_land_use_code")]
       
cat("\nMatched", nrow(prep_parcels[!is.na(land_use_type_id)]), "records with land use reclass table")
cat("\nUnmatched: ", nrow(prep_parcels[is.na(land_use_type_id) & !is.na(use_code)]), "records.")
if(nrow(notfoundlu <- prep_parcels[is.na(land_use_type_id) & !is.na(use_code), .N, by = "use_code"]) > 0){
    cat("\nThe following land use codes were not found:\n")
    print(notfoundlu[order(use_code)])
} else cat("\nAll land use codes matched.")

# construct final parcels                
parcels_final <- prep_parcels[, .(
    parcel_id = pin_1, parcel_id_fips = pin, land_use_type_id,
    land_value = apprlandval, improvement_value = apprimpsval, exemption = as.integer(exempt > 0),
    parcel_sqft = poly_area, y_coord_sp = point_y, x_coord_sp = point_x
    )]

cat("\nTotal all:", nrow(parcels_final), "parcels")
cat("\nAssigned to 2018:", nrow(parcels_final[!is.na(parcel_id) & parcel_id != 0]), "parcels")
cat("\nDifference:", nrow(parcels_final) - nrow(parcels_final[!is.na(parcel_id) & parcel_id != 0]))


###################
# Process buildings
###################

cat("\n\nProcessing King buildings\n=========================\n")

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
    , `:=`(residential_units = round(units * bldg_gross_sqft/gross_sqft))]
cat("\nNumber of buildings in both tables, apartment and commercial:", nrow(aptcom_combo))

# insert the derived residential units into the commercial buildings
prep_buildings_com[aptcom_combo, `:=`(residential_units = i.residential_units), 
                   on = c("pin", "building_number")]

# cases when a record is in both, condo and commercial (adjust the same way as above)
condocom_combo <- merge(prep_buildings_condo[, .(pin, units, sqft_per_unit_apt = sqft_per_unit)], 
                      prep_buildings_com, by = "pin")[!pin %in% aptcom_combo[, pin]] # only if it is not in the appartment table
# set residential units for each commercial records using sqft proportions 
condocom_combo[, `:=`(count = .N, gross_sqft = sum(bldg_gross_sqft)), by = "pin"][
    , `:=`(residential_units = round(units * bldg_gross_sqft/gross_sqft))]
cat("\nNumber of buildings in both tables, condo and commercial:", nrow(condocom_combo))

# insert the derived residential units into the commercial buildings
prep_buildings_com[condocom_combo, `:=`(residential_units = i.residential_units),
                   on = c("pin", "building_number")]

# construct remaining residential buildings
prep_buildings_res <- construct_pin_from_major_minor(resbldg[
    , .(major, minor, bldgnbr, nbrlivingunits, address, stories, sqfttotliving,
        yrbuilt, building_quality_id = condition)])
cat("\nNumber of other residential buildings:", nrow(prep_buildings_res))

# set building_type_id depending on number of units
# TODO: this needs to be reviewed
prep_buildings_res[nbrlivingunits == 1, building_type_id := 19] # SF
prep_buildings_res[nbrlivingunits > 1, building_type_id := 12]

# put everything together
prep_buildings <- rbind(
    prep_buildings_res[
        , .(pin, stories, residential_units = nbrlivingunits, year_built = yrbuilt,
            gross_sqft = sqfttotliving, sqft_per_unit = ceiling(sqfttotliving/nbrlivingunits), 
            building_quality_id, non_residential_sqft = 0, building_type_id)],
    prep_buildings_condo[
        ! pin %in% condocom_combo[, pin],
        .(pin, stories, residential_units = units, year_built, sqft_per_unit, 
          building_quality_id = building_quality, non_residential_sqft = 0,
          building_type_id = 4)
                         ],
    prep_buildings_apt[
        ! pin %in% aptcom_combo[, pin],
        .(pin, stories, residential_units = units, year_built, sqft_per_unit, 
          building_quality_id = building_quality, non_residential_sqft = 0,
          building_type_id = 12)
                        ], 
    prep_buildings_com[
        , .(pin, stories, residential_units, year_built,
            gross_sqft = bldg_gross_sqft, non_residential_sqft = bldg_net_sqft,
            sqft_per_unit, building_quality_id = building_quality, use_code)],
    fill = TRUE
    )

# remove buildings that do not have any match in the parcels table
nbld <- nrow(prep_buildings)
prep_buildings <- prep_buildings[pin %in% parcels_final[, parcel_id_fips]]
cat("\n", nbld - nrow(prep_buildings), "buildings removed due to missing parcels.")

# calculate improvement value
prep_buildings[parcels_final, `:=`(total_improvement_value = i.improvement_value), 
               on = c(pin = "parcel_id_fips")]
prep_buildings[, sqft_tmp := pmax(1, gross_sqft, na.rm = TRUE)][
    , total_sqft := sum(sqft_tmp), by = "pin"][
        , improvement_value := round(sqft_tmp/total_sqft * total_improvement_value)][
           , `:=`(sqft_tmp = NULL, total_sqft = NULL)
        ]
# update improvement values for condos that were not included in the previous call
prep_buildings[prep_buildings_condo,  
               improvement_value := ifelse(is.na(improvement_value), i.improvement_value, improvement_value),
               on = "pin"]
               
# update the parcels' land_value where improvement value is missing based on condo valuation info
parcels_final[prep_buildings_condo,  
              land_value := ifelse(is.na(land_value), i.land_value, land_value),
              on = c(parcel_id_fips = "pin")][, improvement_value := NULL]

# join with reclass table
prep_buildings[, use_code_char := as.character(use_code)][
    bt_reclass[county_id == county.id], building_type_id := ifelse(is.na(building_type_id), i.building_type_id, building_type_id),
               on = c(use_code_char = "county_building_use_code")]
cat("\nMatched", nrow(prep_buildings[!is.na(building_type_id)]), "records with building reclass table")
cat("\nUnmatched: ", nrow(prep_buildings[is.na(building_type_id)]), "records.")
if(nrow(missbt <- prep_buildings[is.na(building_type_id), .N, by = "use_code"]) > 0){
    cat("\nThe following building codes were not found:\n")
    print(missbt[order(-N)])
}

# For buildings that were included in both, commercial as well as apartment or condo, change
# non_residential_sqft and sqft_per_unit depending on the building_type_id
# TODO: check that this step make sense!
prep_buildings[(pin %in% aptcom_combo[, pin] | pin %in% condocom_combo[, pin]) & residential_units > 0 &
                   building_type_id %in% c(19, 12, 4),
               `:=`(sqft_per_unit = ceiling(non_residential_sqft/residential_units),
                    non_residential_sqft = 0) 
               ]

# add parcel_id to buildings
prep_buildings[parcels_final, parcel_id := i.parcel_id, on = c(pin = "parcel_id_fips")]

# create final buildings table
buildings_final <- prep_buildings[, .(building_id = 1:nrow(prep_buildings),
                                      parcel_id, gross_sqft, non_residential_sqft, sqft_per_unit,
                                      year_built, parcel_id_fips = pin, 
                                      residential_units, improvement_value,
                                      building_type_id, use_code, stories)]

cat("\nTotal all: ", nrow(buildings_final), "buildings")
cat("\nAssigned to 2018:", nrow(buildings_final[!is.na(parcel_id) & parcel_id != 0]), "buildings")
cat("\nDifference:", nrow(buildings_final) - nrow(buildings_final[!is.na(parcel_id) & parcel_id != 0]), "\n")


###############
# write results
################

if(write.result){
    fwrite(parcels_final[!is.na(parcel_id) & parcel_id != 0], file = "urbansim_parcels_king.csv")
    fwrite(buildings_final[!is.na(parcel_id) & parcel_id != 0], file = "urbansim_buildings_king.csv")
    fwrite(parcels_final, file = "urbansim_parcels_all_king.csv")
    fwrite(buildings_final, file = "urbansim_buildings_all_king.csv")
    db <- paste0(tolower(county), "_2023_parcel_baseyear")
    connection <- mysql.connection(db)
    dbWriteTable(connection, "urbansim_parcels", parcels_final[!is.na(parcel_id) & parcel_id != 0], overwrite = TRUE, row.names = FALSE)
    dbWriteTable(connection, "urbansim_buildings", buildings_final[!is.na(parcel_id) & parcel_id != 0], overwrite = TRUE, row.names = FALSE)
    dbWriteTable(connection, "urbansim_parcels_all", parcels_final, overwrite = TRUE, row.names = FALSE)
    dbWriteTable(connection, "urbansim_buildings_all", buildings_final, overwrite = TRUE, row.names = FALSE)
    DBI::dbDisconnect(connection)
}
