library(data.table)

setwd("~/psrc/urbansim-baseyear-prep/schools")
schools <- fread("schools.csv")
catch <- fread("parcels_catchment_areas.csv")

schools[, `:=`(has_catchment_E = 0, has_catchment_M = 0, has_catchment_H = 0)]
id.list <- list(E = "elem_id", M = "mschool_id", H = "hschool_id")

for(cat in c("E", "M", "H")){
    schools[grepl(cat, category), (paste0("has_catchment_", cat)) := school_id %in% catch[[id.list[[cat]]]]]
}
fwrite(schools, file = "schools_with_has_catchment.csv")
