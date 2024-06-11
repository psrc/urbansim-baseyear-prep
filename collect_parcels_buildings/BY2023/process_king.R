# Script to create parcels and buildings tables from King assessor data
# for the use in urbansim 
# It generates 3 tables: 
#    urbansim_parcels, urbansim_buildings, building_type_crosstab
#
# Hana Sevcikova, last update 06/10/2024
#

library(data.table)

county <- "King"
county.id <- 33

# path to the Assessor text files
#data.dir <- "\\AWS-MODEL10\urbansim data\Projects\2023_Baseyear\Assessor\Extracts\King\Urbansim_Processing" 
data.dir <- "King_data"
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

construct_pin_from_major_minor_for_fakes <- function(dt){
    # construct original and new pins on fake parcels/buildings
    dt[, `:=`(major = orig_major, minor = orig_minor)]
    dt <- construct_pin_from_major_minor(dt)
    dt[, `:=`(orig_pin = pin, major = new_major, minor = new_minor)]
    dt <- construct_pin_from_major_minor(dt)
    dt[, `:=`(new_pin = pin, major = NULL, minor = NULL)]
    dt[is.na(orig_major), orig_pin := NA]
    dt[is.na(new_major), new_pin := NA]
    return(dt)
}

###############
# Load all data
###############

# parcels & tax accounts
parcels.base <- fread(file.path(data.dir, "parcels23_kin_base.csv"))
parcels.full <- fread(file.path(data.dir, "extr_parcel.csv"))
stacked <- fread(file.path(data.dir, "parcels23_stacked_pins.csv")) # stacked parcels

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

# Info about fake parcels and buildings (prepared by Mark)
fake_parcels <- fread(file.path(data.dir, "king_fake_parcel_changes.csv"))
fake_buildings <- fread(file.path(data.dir, "king_fake_building_changes.csv"))

###################
# Process parcels
###################
# King ID: pin = major + minor

cat("\nProcessing King parcels\n=========================\n")

# make column names lowercase
colnames(parcels.full) <- tolower(colnames(parcels.full))
colnames(fake_parcels) <- tolower(colnames(fake_parcels))

# fix types
rpacct[, acctnbr := as.character(acctnbr)]

parcels.full <- construct_pin_from_major_minor(parcels.full)
parcels.full[, pin_old := pin]

parcels.full[stacked, pin := i.new_parcel_id, on = c(pin_old = "pin")]

# parcels in stacked but not in full:
# stacked[!new_parcel_id %in% parcels.full[, pin]]

# parcels that were renamed but their old pin is still in base
# parcels.base[pin %in% stacked[, pin]]



# It looks like all duplicates in rpacct have zero land and improvement values, 
# thus we can just simply remove the duplicates
# checked with the following which returned zero rows: 
# rpacct[acctnbr %in% rpacct[duplicated(acctnbr), acctnbr]][order(acctnbr)][apprimpsval > 0 | apprlandval > 0]
rpacct.nodupl <- rpacct[!duplicated(acctnbr)]

# construct column "pin" in the rpacct table
rpacct.nodupl <- construct_pin_from_major_minor(rpacct.nodupl)

# merge with stacked parcels
rpacct.nodupl[, pin_old := pin]
rpacct.nodupl[stacked, pin := i.new_parcel_id, on = c(pin_old = "pin")]

# construct original and new pins on fake parcels
fake_parcels <- construct_pin_from_major_minor_for_fakes(fake_parcels)

# join fake parcels with the full attribute dataset
if(nrow((old.fakes <- fake_parcels[!is.na(orig_pin)])) > 0){ # this section is untested since there are no such parcels
    fake_parcels[rpacct.nodupl[pin %in% old.fakes[, orig_pin]], 
                 `:=`(apprlandval = i.apprlandval * land_proportion,
                      apprimpsval = i.apprimpsval * land_proportion,
                      taxstat = i.taxstat
                 ), 
                 on = c(orig_pin = "pin")]
    rpacct.nodupl <- rpacct.nodupl[! pin %in% unique(old.fakes[, orig_pin])]
} else {
    fake_parcels[, `:=`(apprlandval = 0, apprimpsval = 0, taxstat = "T")]
}
new.fakes.add <- fake_parcels[!(new_pin %in% rpacct.nodupl[, pin])]
new.fakes.mod <- fake_parcels[(new_pin %in% rpacct.nodupl[, pin])]
if(nrow(new.fakes.add) > 0){
    # fake parcels to add
    rpacct.nodupl <- rbind(rpacct.nodupl, new.fakes.add[
        , .(pin = new_pin, major = new_major, minor = new_minor,
            presentuse = new_presentuse, apprlandval, apprimpsval, taxstat
        )],
        fill = TRUE)
}
if(nrow(new.fakes.mod) > 0){
    # fake parcels to modify (untested; currently no such case)
    rpacct.nodupl[new.fakes.mod, `:=`(major = i.new_major, minor = i.new_minor,
                                      presentuse = i.new_presentuse, 
                                      apprlandval = i.apprlandval, 
                                      apprimpsval = i.apprimpsval, taxstat = i.taxstat
    ), on = c(pin = "new_pin")]
}
cat("\nIncorporated info for ", nrow(fake_parcels[new_pin %in% rpacct.nodupl[, pin]]),
    "fake parcels")


# group records with the same pin
rpacct_grouped <- rpacct.nodupl[, .(apprlandval = as.double(sum(apprlandval)), 
                                    apprimpsval = as.double(sum(apprimpsval)),
                                    exempt = sum(taxstat != "T")), by = "pin"]
rpacct_grouped_old <- rpacct.nodupl[, .(apprlandval = as.double(sum(apprlandval)), 
                                    apprimpsval = as.double(sum(apprimpsval)),
                                    exempt = sum(taxstat != "T")), by = "pin_old"]
setnames(rpacct_grouped_old, "pin_old", "pin")

# merge base parcels with rpacct
prep_parcels <- merge(parcels.base, rpacct_grouped[, .(pin, apprlandval, apprimpsval, exempt)],
                      by = "pin", all.x = TRUE)

# for land use code, use the base parcel code
parcels.full[, is_base := pin == pin_old]
parcels.full[parcels.full[is_base == TRUE], presentuse_base := i.presentuse, on = "pin"]
# for parcels without a base parcel, use the first value from each pin
parcels.full[is.na(presentuse_base), presentuse_base := presentuse[1], by = "pin"] 

prep_parcels[unique(parcels.full[, .(pin, presentuse_base)]), use_code := as.character(i.presentuse_base), on = "pin"]                      

# join with reclass table
prep_parcels[lu_reclass[county_id == county.id], land_use_type_id := i.land_use_type_id, 
             on = c(use_code = "county_land_use_code")]
## check mismatches with
#prep_parcels[is.na(land_use_type_id) & !is.na(apprlandval), .N, by = "use_code"]

cat("\nMatched", nrow(prep_parcels[!is.na(land_use_type_id) & land_use_type_id > 0]), "records with land use reclass table")
cat("\nUnmatched: ", nrow(prep_parcels[ !is.na(apprlandval) &  !is.na(use_code) & (is.na(land_use_type_id) | land_use_type_id == 0)]), "records.")
if(nrow(notfoundlu <- prep_parcels[ !is.na(apprlandval) & (is.na(land_use_type_id)  | land_use_type_id == 0) & !is.na(use_code), .N, by = "use_code"]) > 0){
    cat("\nThe following land use codes were not found:\n")
    print(notfoundlu[order(use_code)])
}

cat("\nMissing ", nrow(prep_parcels[is.na(land_use_type_id)]), "land use codes due missing parcels in extr_parcel.")
prep_parcels[is.na(land_use_type_id), land_use_type_id := 0]

cat("\n", nrow(rpacct_grouped[!pin %in% prep_parcels[, pin]]), "records from rpacct were not matched with parcels.\n",
    nrow(prep_parcels[is.na(apprlandval)]), "parcel records did not have a record in rpacct.")
cat("\n", nrow(parcels.full[!pin %in% prep_parcels[, pin]]), "records from extr_parcel were not matched with base parcels.\n",
    nrow(prep_parcels[!pin %in% parcels.full[, pin]]), " base parcel records did not have a record in extr_parcel.")


# construct final parcels                
parcels_final <- prep_parcels[, .(
    parcel_id = 1:nrow(prep_parcels), parcel_id_fips = pin, land_use_type_id, use_code, gross_sqft = gis_sqft,
    land_value = apprlandval, improvement_value = apprimpsval, exemption = as.integer(exempt > 0),
    y_coord_sp = point_y, x_coord_sp = point_x, county_id = county.id
    )]

cat("\nTotal:", nrow(parcels_final), "parcels")

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
colnames(fake_buildings) <- tolower(colnames(fake_buildings))

prep_buildings_apt <- construct_pin_from_major_minor(aptcomplex)[
    , .(pin, stories = nbrstories, units = nbrunits, sqft_per_unit = avgunitsize,
        building_quality = bldgquality, year_built = yrbuilt)]

cat("\nNumber of apartments:", nrow(prep_buildings_apt))

prep_buildings_condo <- construct_pin_from_major_minor(condocomplex[, minor := "0000"])[
    , .(pin, major, stories = nbrstories, units = nbrunits, sqft_per_unit = avgunitsize,
        building_quality = bldgquality, year_built = yrbuilt, descr = complexdescr)]

cat("\nNumber of condo complexes:", nrow(prep_buildings_condo))

prep_buildings_com <- construct_pin_from_major_minor(commbldg)[
    , .(pin, stories = nbrstories, building_number = bldgnbr,
        use_code = predominantuse, building_quality = bldgquality, 
        bldg_gross_sqft = bldggrosssqft, bldg_net_sqft = bldgnetsqft,
        year_built = yrbuilt, residential_units = 0, sqft_per_unit = 1)]

cat("\nNumber of commercial buildings:", nrow(prep_buildings_com))

# add fake buildings to the commercial set
# construct original and new pins on fake buildings
fake_buildings <- construct_pin_from_major_minor_for_fakes(fake_buildings)
new_fake_buildings <- fake_buildings[is.na(orig_pin), 
                                     .(pin = new_pin, stories = NA, building_number = new_bldgnbr,
                                       use_code = new_predominantuse, building_quality = NA,
                                       bldg_gross_sqft = new_bldggrosssqft, 
                                       bldg_net_sqft = new_bldgnetsqft, year_built = NA, 
                                       residential_units = 0, sqft_per_unit = 1)
                        ]
nbld <- nrow(prep_buildings_com)
prep_buildings_com <- rbind(prep_buildings_com, new_fake_buildings)
cat("\nAdded ", nrow(prep_buildings_com) - nbld, " fake buidlings.")

if(nrow((mod_fake_buildings <- fake_buildings[!is.na(orig_pin)])) > 0){
    # move buildings into different parcels
    prep_buildings_com[mod_fake_buildings, `:=`(
        pin = i.new_pin, building_number = i.new_bldgnbr,
        use_code = i.new_predominantuse, bldg_gross_sqft = i.new_bldggrosssqft, 
        bldg_net_sqft = i.new_bldgnetsqft),
    on = c(pin = "orig_pin", building_number = "orig_bldgnbr")]
    cat("\nModified ", nrow(mod_fake_buildings), " fake buildings.")
}

# sum land and improvement values over condos in condo complexes
condo_value <- merge(construct_pin_from_major_minor(condounit), rpacct_grouped_old, by = "pin")
condo_land_imp_value <- condo_value[, .(land_value = sum(apprlandval), improvement_value = sum(apprimpsval),
                                        Nrec = .N), by = "major"]

prep_buildings_condo <- merge(prep_buildings_condo, condo_land_imp_value, by = "major")
prep_buildings_condo[grepl(" of ", descr), units := Nrec]

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
                      prep_buildings_com, by = "pin")[!pin %in% aptcom_combo[, pin]] # only if it is not in the apartment table
# set residential units for each commercial records using sqft proportions
condocom_combo[, bldg_gross_sqft_tmp := pmax(1, bldg_gross_sqft)]
condocom_combo[, `:=`(count = .N, gross_sqft = sum(bldg_gross_sqft_tmp)), by = "pin"][
    , `:=`(residential_units = round(units * bldg_gross_sqft_tmp/gross_sqft))]
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
prep_buildings_res[nbrlivingunits == 1, `:=`(building_type_id = 19, use_code = 351)] # SF
prep_buildings_res[nbrlivingunits > 1, `:=`(building_type_id = 12)]

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


# assign new pin
prep_buildings[, pin_old := pin]
prep_buildings <- merge(prep_buildings, stacked, by = "pin", all.x = TRUE)
prep_buildings[, pin := ifelse(is.na(new_parcel_id), pin, new_parcel_id)]


# remove buildings that do not have any match in the parcels table
nbld <- nrow(prep_buildings)
prep_buildings <- prep_buildings[pin %in% parcels_final[, parcel_id_fips]]
cat("\n", nbld - nrow(prep_buildings), "buildings removed due to missing parcels.")

# calculate improvement value
prep_buildings[rpacct_grouped, `:=`(total_improvement_value = i.apprimpsval), on = "pin"]
prep_buildings[, sqft_tmp := pmax(1, gross_sqft, na.rm = TRUE)][
    , total_sqft := sum(sqft_tmp), by = "pin"][
        , improvement_value := round(sqft_tmp/total_sqft * total_improvement_value)][
           , `:=`(sqft_tmp = NULL, total_sqft = NULL)
        ]

# assign new_parcel_id to condos
prep_buildings_condo[, pin_old := pin]
prep_buildings_condo <- merge(prep_buildings_condo, stacked, by = "pin", all.x = TRUE)
prep_buildings_condo[, pin := ifelse(is.na(new_parcel_id), pin, new_parcel_id)]

# update improvement values for condos that were not included in the previous call of calculating improvement value
prep_buildings[prep_buildings_condo,  
               improvement_value := ifelse(is.na(improvement_value), i.improvement_value, improvement_value),
               on = "pin"]
               
# update the parcels' land_value where it is missing based on condo valuation info
parcels_final[prep_buildings_condo[, .(land_value = sum(land_value)), by = "pin"],  
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

prep_buildings[parcels_final, parcel_id := i.parcel_id, on = c(pin = "parcel_id_fips")]

# create final buildings table
buildings_final <- prep_buildings[, .(building_id = 1:nrow(prep_buildings),
                                      parcel_id, parcel_id_fips = pin, 
                                      gross_sqft, non_residential_sqft, sqft_per_unit,
                                      year_built, residential_units, improvement_value,
                                      building_type_id, use_code, stories, 
                                      land_area = round(gross_sqft/stories))]

cat("\nTotal: ", nrow(buildings_final), "buildings")

# generate building type crosstabs
bt.tab <- buildings_final[!is.na(parcel_id) & parcel_id != 0, .(N = .N, DU = sum(residential_units), 
                                                                nonres_sqft = sum(non_residential_sqft)), 
                          by = .(use_code, building_type_id)]
bt.tab <- merge(bt.tab, bt_reclass[county_id == county.id, .(county_building_use_code = as.integer(county_building_use_code), 
                                                             county_building_use_description, building_type_id, building_type_name)],
                by.x = c("use_code", "building_type_id"), by.y = c("county_building_use_code", "building_type_id"),
                all.x = TRUE)
setcolorder(bt.tab, c("use_code", "county_building_use_description", "building_type_id", "building_type_name"))


###############
# write results
################

if(write.result){
    fwrite(parcels_final, file = "urbansim_parcels_king.csv")
    fwrite(buildings_final, file = "urbansim_buildings_king.csv")
    db <- paste0(tolower(county), "_2023_parcel_baseyear")
    connection <- mysql.connection(db)
    dbWriteTable(connection, "urbansim_parcels", parcels_final, overwrite = TRUE, row.names = FALSE)
    dbWriteTable(connection, "urbansim_buildings", buildings_final, overwrite = TRUE, row.names = FALSE)
    dbWriteTable(connection, "building_type_crosstab", bt.tab, overwrite = TRUE, row.names = FALSE)
    DBI::dbDisconnect(connection)
}
