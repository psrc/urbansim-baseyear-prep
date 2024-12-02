# Function that selects the best template for given MPDs
find.templates <- function(mpds, templates, template.comps, parcels){
  # compute some attributes that are needed
  mpd <- copy(mpds)[, `:=`(is_residential = building_type_id %in% c(4, 12, 19, 11), template_id = 0)]
  mpd[parcels, land_use_type_id := i.land_use_type_id, on = "parcel_id"]
  
  template.comps[, number_of_components := .N, by = "template_id"]
  templates <- merge(templates, template.comps[, .(number_of_components = max(number_of_components)), 
                                               by = "template_id"], by = "template_id")
  templates[, density_converter := ifelse(density_type == "units_per_acre", 1/43560, 1)]
  
  parcel.ids <- unique(mpd$parcel_id) # which parcels to process
  # iterate over parcels
  for(pcl in parcel.ids){
    bidx <- which(mpd$parcel_id == pcl)
    # match number of components & their building type (all components must match)
    template.comps.cand <- template.comps[building_type_id %in% mpd$building_type_id[bidx] & number_of_components == length(bidx)]
    template.comps.cand[, this_number_of_components := .N, by = "template_id"]
    template.comps.cand <- template.comps.cand[number_of_components == this_number_of_components]
    templates.cand <- templates[template_id %in% template.comps.cand$template_id]
    if(nrow(templates.cand) == 0){
      cat("\nNo template found for building type(s) ", paste(mpd[bidx, building_type_id], collapse = ","),
          "due to building type mismatch")
      next
    }
    # match land area
    sum.land.sqft <- mpd[bidx, sum(land_area)]
    templates.cand <- templates.cand[sum.land.sqft <= land_sqft_max & sum.land.sqft >= land_sqft_min]

    if(nrow(templates.cand) == 0){
      if(sum.land.sqft > 0)
        cat("\nNo template found for building type(s) ", paste(mpd[bidx, building_type_id], collapse = ","),
            "due to land area mismatch")
      next
    }
    # compute number of units and improvement value for each of the template candidates, given the MPD's land area
    templates.cand[, units := pmax(1, round(sum.land.sqft * (1 - percent_land_overhead/100) * density * density_converter))]
    template.comps.cand  <- template.comps[template_id %in% templates.cand$template_id][templates.cand, units := i.units, on = "template_id"]
    template.comps.cand[, improvement_value := construction_cost_per_unit * percent_building_sqft/100 * building_sqft_per_unit * units]
    templates.cand[template.comps.cand[, .(improvement_value = sum(improvement_value)), by = "template_id"], 
                   improvement_value := i.improvement_value, on = "template_id"]
    
    # compute total number of units per MPD
    is.mixed.mpd <- mpd[bidx, !all(is_residential == is_residential[1])]
    mpd[bidx, factor := if(is.mixed.mpd) sqft_per_unit else 1]
    mpd[bidx, units := ifelse(is_residential & residential_units > 0, residential_units, non_residential_sqft) * factor]
    btotalunits <- mpd[bidx, sum(units)]
    
    # which template is the closest to the total proposed units in the MPDs
    winner.templ <- templates.cand[which.min(abs(btotalunits - units))]
    all.winners <- templates.cand[units == winner.templ$units]
    if(nrow(all.winners) > 1){ # more than one winner
      # choose the one with matching land use type
      winners.lu <- all.winners[land_use_type_id %in% mpd[bidx]$land_use_type_id[1]]
      if(nrow(winners.lu) > 0)
        all.winners <- winners.lu
      if(nrow(all.winners) > 1) # more still multiple winners choose one that matches improvement value
        winners.impr <- all.winners[which.min(abs(improvement_value - sum(mpd[bidx]$improvement_value)))]
    }
    mpd[bidx, template_id := winners.impr$template_id] # assign the final template
  }
  if(nrow(miss <- mpd[is.na(template_id) | template_id == 0]) > 0)
    cat("\nNo template found for ", nrow(miss), "MPDs on", length(unique(miss$parcel_id)), "parcels.\n")
  else
    cat("\nA template assigned to all MPDs.\n")
  
  mpds[mpd, template_id := i.template_id, on = "building_id"]
  
  props <- mpd[, .(start_year = min(year_built), status_id = 3, units_proposed = sum(units)), by = c("parcel_id", "template_id")]
  props[, proposal_id := 1:nrow(props)]
  return(list(mpds, props))
}