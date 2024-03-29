# This script will append column headers to Pierce County Assessors files
# and export as new set of txt files 

rootDir <- 'E:/Assessor23/Pierce'
setwd(rootDir)
outDir <- 'data_formatted'
attributes <- c('appraisal_account', 'improvement', 'improvement_builtas', 'improvement_detail', 'land_attribute', 'sale', 'tax_account', 'tax_description')

aa <- c('parcel_number', 
        'appraisal_account_type', 
        'business_name', 
        'value_area_id', 
        'land_economic_area', 
        'buildings', 
        'group_acct_number', 
        'land_gross_acres', 
        'land_net_acres', 
        'land_gross_square_feet', 
        'land_net_square_feet', 
        'land_gross_front_feet', 
        'land_width', 
        'land_depth', 
        'submerged_area_square_feet', 
        'appraisal_date', 
        'waterfront_type', 
        'view_quality', 
        'utility_electric', 
        'utility_sewer', 
        'utility_water', 
        'street_type', 
        'latitude', 
        'longitude')
i <- c('parcel_number', 
       'building_id', 
       'property_type', 
       'neighborhood', 
       'neighborhood_extension', 
       'square_feet', 
       'net_square_feet', 
       'percent_complete', 
       'condition', 
       'quality', 
       'primary_occupancy_code', 
       'primary_occupancy_description', 
       'mobile_home_serial_number', 
       'mobile_home_total_length', 
       'mobile_home_make', 
       'attic_finished_square_feet', 
       'basement_square_feet', 
       'basement_finished_square_feet', 
       'carport_square_feet', 
       'balcony_square_feet', 
       'porch_square_feet', 
       'attached_garage_square_feet', 
       'detatched_garage_square_feet', 
       'fireplaces', 
       'basement_garage_door')
ib <- c('parcel_number', 
        'building_id', 
        'built_as_number', 
        'built_as_id', 
        'built_as_description', 
        'built_as_square_feet', 
        'hvac', 
        'hvac_description', 
        'exterior', 
        'interior', 
        'stories', 
        'story_height', 
        'sprinkler_square_feet', 
        'roof_cover', 
        'bedrooms', 
        'bathrooms', 
        'units', 
        'class_code', 
        'class_description', 
        'year_built', 
        'year_remodeled', 
        'adjusted_year_built', 
        'physical_age', 
        'built_as_length', 
        'built_as_width', 
        'mobile_home_model')
id <- c('parcel_number', 
        'building_id', 
        'detail_type', 
        'detail_description', 
        'units')
la <- c('parcel_number', 
        'attribute', 
        'attribute_description')
s <- c('etn', 
       'parcel_count', 
       'parcel_number', 
       'sale_date', 
       'sale_price', 
       'deed_type', 
       'grantor', 
       'grantee', 
       'valid_invalid', 
       'confirmed_unconfirmed', 
       'exclude_reason', 
       'improved_vacant',
       'apprasisal_account_type')
ta <- c('parcel_number', 
        'account_type', 
        'property_type', 
        'site_address', 
        'use_code', 
        'use_description', 
        'tax_year_prior', 
        'tax_code_area_prior_year', 
        'exemption_type_prior_year', 
        'current_use_code_prior_year', 
        'land_value_prior_year', 
        'improvement_value_prior_year', 
        'total_market_value_prior_year', 
        'taxable_value_prior_year', 
        'tax_year_current', 
        'tax_code_area_current_year', 
        'exemption_type_current_year', 
        'current_use_code_current_year', 
        'land_value_current_year', 
        'improvement_value_current_year', 
        'total_market_value_current_year', 
        'taxable_value_current_year', 
        'range', 
        'township', 
        'section', 
        'quarter_section', 
        'subdivision_name', 
        'located_on_parcel')
td <- c('parcel_number', 
        'line_number', 
        'tax_description_line')

headers <- list(aa, i, ib, id, la, s, ta, td)

for (a in 1:length(attributes)) {
  
  table <- read.table(paste0(attributes[a], '.txt'), 
                      header = FALSE, 
                      fill = TRUE, 
                      sep = '|', 
                      quote = "", 
                      comment.char = "",
                      col.names = headers[[a]])
  
  write.table(table, file.path(outDir, paste0(attributes[a],'.txt')), row.names = FALSE, quote = FALSE, sep = "|")
  print(paste("Exported", attributes[a]))
}

print("Exporting Completed")