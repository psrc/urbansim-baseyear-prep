library(data.table)

pclc <- fread("data2018/parcel_points_2018.csv")
#range(pclc$Join_Count)
setnames(pclc, "parcel_id", "parcel_id_2014")
setnames(pclc, "PIN", "parcel_id")
pclcor <- pclc[, .(parcel_id, parcel_id_2014)]
fwrite(pclcor, file = "data2018/parcel_lookup_2018_2014.csv")
