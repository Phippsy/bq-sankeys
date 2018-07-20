library(bigQueryR)
library(stringr)
library(tidyverse)
library(googleCloudStorageR)
library(networkD3)

source("R/helper-functions.R")
source("R/set-google-scopes.R")

# Get pageflow data -------------------------------------------------------
pageflow_data <- get_pageflow(
  page_filter = "\\/home",
  # campaign_filter = "AW - Dynamic Search Ads Whole Site",
  date_from = "2017-01-01",
  date_to = "2017-01-15",
  bq_project = "flash-zenith-121319",
  bq_dataset = "pageflows",
  bq_table = "pageflows_example",
  gcs_bucket = "pageflows",
  delete_storage = FALSE,
  service_key = "~/auth/dartistics-key.json")

# Create network diagrams -------------------------------------------------
chart_network <- network_graph(page_table = pageflow_data, 
                        type = "network",
                         n_pages = 20,
                         net_height = 800,
                         net_width = 1200,
                         net_charge = -300,
                         net_font_size = 12)

chart_sank <- network_graph(page_table = pageflow_data, 
                            type = "sankey",
                            # n_pages = 35,
                            sank_links = 35)

