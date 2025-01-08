# Script for consolidating MPDs.
# It does the following:
#     - gets old MPDs
#     - consolidates new (CoStar) MPDs into records by parcel & building type
#       and imputes missing residential_units for MF records if needed
#     - removes records that have a high probability that they were already built
#.    - finds template_id for new records
#.    - converts MPDs into project proposal format and exports 
#     - identifies if an MPD is an addition or replacement and 
#       adds a column not_demolish to the buildings table
#     - creates a table of mpds in a building format that is needed for urbansim2
#
# Datasets needed - can be read directly from mysql (from 2 different DBs) or from csv files:
#     buildings_mpd_in, parcels, buildings, development_project_proposals (old proposals assigned to new parcels),
#     development_templates, development_template_components
# 
# Hana Sevcikova, 2024/12/17
#
#options(error=quote(dump.frames("last.dump", TRUE)))
#load("last.dump.rda"); debugger()

library(data.table)

setwd("~/psrc/urbansim-baseyear-prep/mpds")

source("mpd_functions.R") # contains function for finding template_id

save.into.mysql <- FALSE
read.from.mysql <- TRUE

mpd.table.name.in <- "buildings_mpd_in"
mpd.table.name.out <- "buildings_mpd_cons" # table name of the consolidated CoStar records (will also be exported) 
buildings.name.out <- "buildings_with_not_demolish" # output table of the buildings that will have the column not_demolish attached
proposal.name.out <- "development_project_proposals"
mpd.buildings.name.out <- "mpds"

data.dir <- "data2023" # used if read.from.mysql is FALSE

if(save.into.mysql || read.from.mysql){
  source("../collect_parcels_buildings/BY2023/mysql_connection.R")
  db <- "psrc_2023_parcel_baseyear_working" # where to take the MPD tables from (old and new)
  db.main <- "psrc_2023_parcel_baseyear" # where to take parcels, buildings and templates tables from
  connection <- mysql.connection(db)
  connection.main <- mysql.connection(db.main)
}

if(read.from.mysql){
  mpds <- data.table(dbReadTable(connection, mpd.table.name.in))
  pcl <- data.table(dbGetQuery(connection.main, "SELECT parcel_id, parcel_sqft, gross_sqft, land_use_type_id FROM parcels"))
  bld <- data.table(dbGetQuery(connection.main, "SELECT * FROM buildings"))
  prevprops <- data.table(dbGetQuery(connection, "SELECT * FROM development_project_proposals")) # old proposals re-coded to new parcels
  templ <- data.table(dbGetQuery(connection.main, "SELECT * FROM development_templates"))
  templcomp <- data.table(dbGetQuery(connection.main, "SELECT * FROM development_template_components"))
} else { # read from csv
  mpds <- fread(file.path(data.dir, paste0(mpd.table.name.in, ".csv")))
  pcl <- fread(file.path(data.dir, "parcels.csv"))
  bld <- fread(file.path(data.dir, "buildings.csv"))
  prevprops <- fread(file.path(data.dir, "development_project_proposals.csv")) # old proposals re-coded to new parcels
  templ <- fread(file.path(data.dir, "development_templates.csv"))
  templcomp <- fread(file.path(data.dir, "development_template_components.csv"))
}

# preprocess previous proposals
# disaggregate into components by merging with template components
templcomp <- merge(templcomp, templ[, .(template_id, density_type)], by = "template_id")
prevpropcomp <- merge(prevprops, templcomp, by = "template_id")[start_year > 2022] # alternatively investigate if the old proposals were actually built
prevpropcomp[, is_residential := ifelse(building_type_id %in% c(12, 19, 4), TRUE, FALSE)]
prevpropcomp[, units_proposed := units_proposed * percent_building_sqft/100]
prevpropcomp[is_residential == TRUE & density_type == "far" & units_proposed > 5000, units_proposed := units_proposed/building_sqft_per_unit]
#prevpropcomp[templ, land_area := (i.land_sqft_max - i.land_sqft_min)/2, by = "template_id"]
prevpropcomp[, `:=`(residential_units = ifelse(is_residential == TRUE, units_proposed, 0.0),
                    non_residential_sqft = ifelse(is_residential == TRUE, 0.0, as.double(units_proposed)),
                    improvement_value = 0, land_area = 0, stories = 0, year_built = start_year, 
                    gross_sqft = units_proposed * building_sqft_per_unit)]
# summarize by parcels
prevproppcl <- prevpropcomp[, .(proposed_du = sum(ifelse(is_residential == TRUE, as.double(units_proposed), 0.0)), 
                              proposed_nr_sqft = sum(ifelse(is_residential == TRUE, 0.0, as.double(units_proposed))), 
                              proposed_year_built = min(start_year)), by = "parcel_id"]


# preprocess new CoStar data
# there is a mixed-use record, which can be converted into MF (Mark checked that it makes sense for this parcel)
mpds[building_type_id == 10] # check that it is just one record or records for which this makes sense
mpds[building_type_id == 10, building_type_id := 12]

# consolidate by parcel and building type
mpds[pcl, `:=`(parcel_sqft = i.parcel_sqft, parcel_gross_sqft = i.gross_sqft), on = "parcel_id"]

# Derive buildings' footprint
# (the land_area attribute is taken from CoStar and it's not clear if it reflects the building footprint)
mpds[, costar_land_area := land_area][, land_area := NULL]
mpds[stories > 0, land_area := gross_sqft/stories] # it looks like yes, the costar land_area is exactly this ratio
mpds[stories == 0, far := gross_sqft/parcel_gross_sqft][, stories_new := pmax(1, cut(far, c(0, seq(0.5, 99, by = 1)), 
                                                                             include.lowest = TRUE, labels = FALSE) - 1)]
mpds[stories == 0, `:=`(land_area = gross_sqft/stories_new, stories = stories_new)] 
# set the building footprint not to be larger than 90% of the parcel size
mpds[land_area > parcel_gross_sqft, land_area := pmin(land_area, parcel_gross_sqft)]
mpds[, `:=`(far = NULL, stories_new = NULL)]

mpdsc <- mpds[parcel_id > 0, 
              .(non_residential_sqft = sum(non_residential_sqft), 
                land_area = pmin(sum(land_area), mean(parcel_gross_sqft)), # land area should not be larger than the parcel size
                  improvement_value = sum(improvement_value), gross_sqft = sum(gross_sqft),
                  year_built = min(year_built), stories = sum(stories), residential_units = sum(residential_units),
                  parcel_gross_sqft = mean(parcel_gross_sqft), Nagg = .N),
              by = c("parcel_id", "building_type_id")]


# remove "other" and MH components
mpdsc <- mpdsc[!building_type_id %in% c(23, 11, 6)]

# check building type of MPDs with zero units
# mpdsc[residential_units == 0 & non_residential_sqft == 0, .N, by = "building_type_id"]

# impute residential units for MF buildings
mpdsc[, residential_units_orig := residential_units]
mpdsc[, `:=`(N = .N), by = "parcel_id"]

mpdsc[residential_units == 0 & non_residential_sqft == 0 & building_type_id == 12,
     residential_units := round(gross_sqft / 800)]
# check the largest values
#mpdsc[residential_units_orig == 0 & non_residential_sqft == 0 & building_type_id == 12][order(-residential_units)]


# try to identify MPDs that were already built and remove them
# analyze by summing for each parcel
bldpcl <- bld[building_type_id != 23, .(existing_du = sum(residential_units), existing_nr_sqft = sum(non_residential_sqft), 
                  existing_year_built = max(year_built), existing_land_area = sum(land_area)), by = "parcel_id"]

# summarize by parcel and join with previous proposals, while removing duplicates
proposed <- rbind(
              mpdsc[, .(proposed_du = sum(residential_units), proposed_nr_sqft = sum(non_residential_sqft), 
                        proposed_year_built = min(year_built), proposed_land_area = sum(land_area)), by = "parcel_id"],
              prevproppcl[!parcel_id %in% mpdsc$parcel_id], fill = TRUE)

# if the duplicates removal above is switched off, 
# one can here check what parcel duplicates we get
#proposed[, .N, by = "parcel_id"][N > 1] 

# merge with building info
propbuilt <- merge(proposed, bldpcl, by = "parcel_id")

# select candidates for removal
remcand <- propbuilt[proposed_year_built < 2026 & existing_year_built > 2017] 
remcand[, `:=`(difres = (proposed_du - existing_du)/existing_du * 100, 
               difnonres =  (proposed_nr_sqft - existing_nr_sqft)/existing_nr_sqft * 100) ]
rem <- remcand[(proposed_du == 0 & difnonres < 50) | (proposed_nr_sqft == 0 & difres < 100)] # 20 proposals

# identify proposals on parcels that should not be demolished
propbuilt <- propbuilt[!parcel_id %in% rem$parcel_id]
not.demolish <- c(propbuilt[proposed_nr_sqft == 0 & proposed_du < existing_du, parcel_id],
                  propbuilt[proposed_du == 0 & proposed_nr_sqft < existing_nr_sqft, parcel_id],
                  propbuilt[proposed_nr_sqft > 0 & proposed_du == 0 & existing_du > 0 & existing_year_built > 2014, parcel_id],
                  propbuilt[proposed_du > 0 & proposed_nr_sqft == 0 & existing_nr_sqft > 0 & existing_year_built > 2014, parcel_id]
                  )
#propbuilt[parcel_id %in% not.demolish] # check
updbld <- bld[, not_demolish := as.integer(parcel_id %in% not.demolish)]

# join old MPDs and new ones and remove the selected MPDs
mpdb <- mpdsc[!parcel_id %in% rem$parcel_id]
mpdb <- rbind(mpdb, prevpropcomp[!parcel_id %in% c(mpdb$parcel_id, rem$parcel_id)], fill = TRUE)

# assemble final MPD table
mpds.final <- mpdb[, .(building_id = 1:nrow(mpdb), parcel_id, building_type_id, non_residential_sqft,
                        residential_units, land_area = round(land_area), improvement_value, gross_sqft,
                        year_built, stories, template_id)][is.na(template_id), template_id := 0]
mpds.final[, sqft_per_unit := 1][residential_units > 0, sqft_per_unit := gross_sqft/residential_units]
mpds.final <- mpds.final[residential_units > 0 | non_residential_sqft > 0] # may not be necessary

mpds.props <- find.templates(mpds.final[template_id == 0], templ, templcomp, pcl)

props.new <- mpds.props[[2]][, `:=`(is_redevelopment = parcel_id %in% 
                                      setdiff(propbuilt[existing_du > 0 | 
                                                          existing_nr_sqft > 0, parcel_id], not.demolish))]
props.final <- rbind(prevprops[! (parcel_id %in% props.new$parcel_id) & parcel_id %in% mpds.final$parcel_id],
                     props.new[template_id > 0]
                     )
props.final[, proposal_id := 1:nrow(props.final)]
mpds.new <- mpds.props[[1]]
mpds.all <- mpds.final[parcel_id %in% props.final$parcel_id][, template_id := NULL]

# export MPD and the buildings table
if(save.into.mysql) {
  dbWriteTable(connection, mpd.table.name.out, mpds.new, overwrite = TRUE, row.names = FALSE)
  dbWriteTable(connection.main, buildings.name.out, updbld, overwrite = TRUE, row.names = FALSE)
  dbWriteTable(connection.main, proposal.name.out, props.final, overwrite = TRUE, row.names = FALSE)
  dbWriteTable(connection.main, mpd.buildings.name.out, mpds.all, overwrite = TRUE, row.names = FALSE)
}

if(save.into.mysql || read.from.mysql) {
  DBI::dbDisconnect(connection)
  DBI::dbDisconnect(connection.main)
}
