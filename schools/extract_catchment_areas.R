# Extract attributes elem_id, mschool_id and hschool_id from 2018 parcels,
# assign it to 2023 parcels and convert it to the new school_id in the schools table.

library(data.table)

# read 2018 parcels
setwd("~/psrc/urbansim-baseyear-prep/schools")
pcl18 <- readRDS("~/psrc/R/shinyserver/baseyear2018explorer/data/parcels.rds")

# read Xwalk between 2018 and 2023 parcels
xwalk <- fread("parcel2018to2023.csv")

# merge together
res <- merge(xwalk, pcl18[, .(parcel_id, elem_id, mschool_id, hschool_id)], all.x = TRUE,
             by.x = "parcel2018_id", by.y = "parcel_id", sort = FALSE)
res <- res[, .(parcel_id = parcel2023_id, elem_id_old = elem_id, 
               mschool_id_old = mschool_id, hschool_id_old = hschool_id)]

# read schools table
schools <- fread("schools.csv")

# convert old ids to the new ones
res[schools, elem_id := i.school_id, on = c(elem_id_old = "schoolcode")][is.na(elem_id), elem_id := 0]
res[schools, mschool_id := i.school_id, on = c(mschool_id_old = "schoolcode")][is.na(mschool_id), mschool_id := 0]
res[schools, hschool_id := i.school_id, on = c(hschool_id_old = "schoolcode")][is.na(hschool_id), hschool_id := 0]

# write results
fwrite(res, file = "parcels_catchment_areas.csv")

# update the parcels table from 2023 explorer
pcl23 <- readRDS("~/psrc/R/shinyserver/baseyear2023explorer/data/parcels.rds")
pcl23[, `:=`(elem_id = NULL, mschool_id = NULL, hschool_id = NULL)]
res2 <- merge(pcl23, res[, .(parcel_id, elem_id, mschool_id, hschool_id)], 
              by = "parcel_id", all.x = TRUE)
saveRDS(res2, "parcels.rds")

missing.schools.id <- c(unique(res[elem_id == 0, elem_id_old]), unique(res[mschool_id == 0, mschool_id_old]),
                     unique(res[hschool_id == 0, hschool_id_old]))
schools18 <- fread("schools2018.csv")
missing.schools <- schools18[schoolcode %in% missing.schools.id]
print(missing.schools[, .(school_id, schoolcode, category, student_count, sname)][order(category)])
xwalk[parcel2018_id %in% missing.schools$parcel_id]
