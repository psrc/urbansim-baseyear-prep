# find schools that do not have a catchment area
# flag schools with catchment area and without with different colors

library(tidyverse)
library(sf)
library(leaflet)
library(here)

files <- c('buildings', 'households', 'parcels_geo', 'parcels', 'persons')
data <- map(files, ~readRDS(here('data', paste0(.x, '.rds')))) |>
  set_names(files)

schools_cat <- c('E', 'M', 'H')
schools <- read.csv('data/schools.csv')

all_ps <- schools |> 
  filter(public == 1 & category %in% schools_cat)

ps <- map(schools_cat, ~all_ps |> filter(category == .x)) |> 
  set_names(schools_cat)

walk(ps, ~print(nrow(.x)))

filter_parcels_for_catchment <- function(df, var) {
  df |> 
    distinct(across(all_of(var))) |> 
    arrange(across(all_of(var))) |> 
    rename(school_id = var)
}

# identify unique catchment areas by school cat
catchments <- map(paste0(c('elem', 'mschool', 'hschool'), "_id"), ~filter_parcels_for_catchment(data$parcels, .x)) |> 
  set_names(schools_cat)

walk(catchments, ~print(head(.x)))

my_function <- function(ps_df, compare_df, has_catchment_num) {
  ps_df |> 
    filter(!(school_id %in% compare_df[['school_id']])) |> 
    mutate(has_catchment = has_catchment_num)
}

schools_no_catchment <- map2(ps, catchments, ~my_function(.x, .y, has_catchment_num = 0))|> 
  set_names(schools_cat)

walk(schools_no_catchment, ~print(nrow(.x)))

schools_w_catchment <- map2(ps, schools_no_catchment, ~my_function(.x, .y, has_catchment_num = 1)) |> 
  set_names(schools_cat)

walk(schools_w_catchment, ~print(nrow(.x)))

schools_flagged <- map2(schools_w_catchment, schools_no_catchment, ~bind_rows(.x, .y)) |> 
  set_names(schools_cat)

walk(schools_flagged, ~print(nrow(.x)))
