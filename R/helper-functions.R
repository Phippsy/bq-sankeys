# Get the pageflow data from BQ -------------------------------------------
get_pageflow <- function(page_filter = NULL,
                         campaign_filter = NULL,
                         medium_filter = NULL,
                         date_from = as.character(Sys.Date()-5),
                         date_to = as.character(Sys.Date()-1),
                         bq_project = "flash-zenith-121319",
                         bq_dataset = "pageflows",
                         bq_table = "pageflows_example",
                         gcs_bucket = "pageflows",
                         delete_storage = FALSE,
                         service_key = NULL) {
  
  if(is.null(service_key)) stop("You must provide a service key to proceed.\nhttp://bit.ly/2JDPysT for more information.")
  if(is.null(bq_project)) stop("You must provide a BigQuery project name to proceed.")
  if(is.null(bq_dataset)) stop("You must provide a BigQuery dataset name to proceed.")
  if(is.null(bq_table)) stop("You must provide a BigQuery table name to proceed.")
  if(is.null(gcs_bucket)) stop("You must provide a Cloud Storage Bucket to proceed.")
  
  set_google_scopes("bigquery")
  
  # Replace '~/auth/service-key.json' with a path to your own service key
  # https://code.markedmondson.me/googleAuthR/reference/gar_auth_service.html
  googleAuthR::gar_auth_service(json_file = service_key) 
  
  # Tidy up our input values from user/entry-friendly into suitable format for API requests
  date_from <- stringr::str_replace_all(date_from, "-", "")
  date_to <- stringr::str_replace_all(date_to, "-", "")
  campaign_filter <- ifelse(!is.null(campaign_filter), paste0("AND REGEXP_CONTAINS(trafficSource.campaign, '", 
                                                              campaign_filter, 
                                                              "')"), "")
  medium_filter <- ifelse(!is.null(medium_filter), paste0("AND REGEXP_CONTAINS(trafficSource.medium, '", 
                                                            medium_filter, "')"), "")
  page_filter <- ifelse(!is.null(page_filter), paste0("AND REGEXP_CONTAINS(hits.page.pagePath, '", 
                                                          page_filter, "')"), "")
  
  # Read in the raw SQL query, swap in any user-provided parameters (e.g. medium, campaign)
  fileName <- "sql/pagepaths.sql"
  pageflow_query <- readChar(fileName, file.info(fileName)$size)
  query <- stringr::str_replace_all(pageflow_query, c(
    "\\{\\{page_filter\\}\\}" = page_filter,
    "\\{\\{campaign_filter\\}\\}" = campaign_filter,
    "\\{\\{medium_filter\\}\\}" = medium_filter,
    "\\{\\{date_from\\}\\}" = date_from,
    "\\{\\{date_to\\}\\}" = date_to,
    "\\r|\\n|\\t" = " "
  ))
  
  # Run the BigQuery job and save results to BQ table in user's project.dataset.table
  message("Running BigQuery Query")
  job <- bqr_query_asynch(projectId = bq_project, 
                          datasetId = bq_dataset,
                          query = query,
                          useLegacySql = FALSE,
                          destinationTableId = bq_table,
                          writeDisposition = "WRITE_TRUNCATE")
  
  bqr_wait_for_job(job, wait = 10)
  
  job_stats <- bqr_get_job(projectId = bq_project, jobId = str_replace(job$id, ".*\\.|.*\\:", ""))
  # Approx $5 per TB queried - https://cloud.google.com/bigquery/pricing
  estimated_cost_usd = (as.numeric(job_stats$statistics$totalBytesProcessed) / 1000000000000) * 5
  
  # Transfer from project.dataset.table over to user-defined cloud storage bucket
  page_data <- bqr_extract_data(projectId = bq_project,
                                datasetId = bq_dataset,
                                tableId = bq_table,
                                cloudStorageBucket = gcs_bucket)
  
  bqr_wait_for_job(page_data)
  
  set_google_scopes("storage")
  googleAuthR::gar_auth_service(json_file = service_key)
  
  # Get a pointer to the Cloud Storage Object containing the pageflow data
  results <- gcs_list_objects(bucket = gcs_bucket,
                              prefix = str_replace_all(page_data$configuration$extract$destinationUri,
                                                       c(".*\\/" = "",
                                                         "\\*.*" = "")))
  
  # Read in and parse the gcs object into R
  page_table <- read_csv(gcs_get_object(object_name = results$name,
                                        bucket = gcs_bucket)) %>% 
    mutate(job_cost = estimated_cost_usd)
  
  if (delete_storage) {
    # Optional - delete GCS object --------------------------------------------
    gcs_delete_object(object_name = results$name,
                      bucket = gcs_bucket)
  }
  page_table
}

# Generate network diagrams -----------------------------------------------
network_graph <- function(page_table = NULL,
                           type = "network",
                           n_pages = NULL,
                           net_height = 800,
                           net_width = 800,
                           net_linkwidth = 0.5,
                           net_charge = -30,
                           sank_links = NULL,
                           sank_height = 1000,
                           sank_width = 1200,
                           sank_padding = 10,
                          net_font_size = 12) {
  
  # Filter out pages if specified -------------------------------------------------
  if(!is.null(n_pages)) {
    top_pages <- page_table %>% 
      group_by(pagePath) %>% 
      summarise(count = n()) %>% 
      top_n(n_pages, count)
    page_table <- page_table %>%
      semi_join(top_pages, by = "pagePath")
  }
  
  # Add next page visited, needed to define links
  page_table <- page_table %>% 
    mutate(next_page = case_when(
      is.na(next_page) ~ "Exit",
      TRUE ~ next_page
    ))
  
  # Aggregate page volumes
  page_counts <- select(page_table, name = pagePath) %>% 
    group_by(name) %>% 
    summarise(count = n())
  
  # Calculate number of links between pages
  link_counts <- select(page_table, name = pagePath, next_page) %>% 
    group_by(name, next_page) %>% 
    summarise(link_count = n())
  
  # Create a nodes object - needed for forceNetwork function
  # https://www.rdocumentation.org/packages/networkD3/versions/0.4/topics/forceNetwork
  nodes <- select(page_table, name = pagePath) %>% 
    ungroup() %>% 
    distinct() %>% 
    bind_rows(data.frame(
      name = "Exit"
    )) %>% 
    mutate(id = row_number()-1) %>% 
    left_join(page_counts, by = "name") %>% 
    mutate(display_name = paste0(name, "\n(", count, " unique pageviews)"),
           count = count / max(count, na.rm = TRUE) * 200 ,
           # Simple page categorisation. 
           # 'group' is used to set the node colours in the Sankey chart
            group = case_when(
             str_detect(name, "^\\/home") ~ "Home",
             str_detect(name, ".*google\\+") ~ "Google +",
             TRUE ~ "Other"
           )) %>% 
    select(name, id, count, group, display_name)
  
  
  links <- select(page_table, name = pagePath, next_page) %>% 
    mutate(next_page = case_when(
      is.na(next_page) ~ "Exit",
      TRUE ~ next_page
    )) %>% 
    distinct() %>% 
    left_join(nodes, by = "name") %>% 
    rename(source = id) %>% 
    left_join(nodes, by = c("next_page" = "name")) %>% 
    rename(target = id) %>%
    left_join(link_counts, by = c("name" = "name",
                                  "next_page" = "next_page")) %>% 
    select(source, target, link_count) %>% 
    filter(!is.na(target)) %>% 
    mutate(link_count_scaled = (link_count / max(link_count))*30)
  
  nodes_noid <- nodes %>% 
    select(-id)
      
  if (type == "network") {
  chart <- forceNetwork(Links = links, 
                          Nodes = nodes_noid,
                          Source = "source", 
                          Target = "target",
                          fontSize = net_font_size,
                          # linkWidth = net_linkwidth,
                          linkDistance = 150,
                          height = net_height,
                        Value = "link_count_scaled",
                          width = net_width,
                          charge = net_charge,
                          Nodesize = "count",
                        Group = "group",
                          NodeID = "display_name",
                          bounded = FALSE,
                        legend = TRUE,
                          zoom = TRUE,
                          opacityNoHover = 0,
                          opacity = 1)
  }
  
  if ( type == "sankey") {
    
    if (type == "sankey" & !is.null(sank_links)) {
     links2 <- links %>% 
       top_n(sank_links, link_count) %>% 
       left_join(nodes, by = c("source" = "id")) %>% 
       select(curr_page = name, everything()) %>% 
       left_join(nodes, by = c("target" = "id")) %>% 
       select(next_page = name, everything())
     
     nodes2 <- select(links2, page = curr_page) %>% 
       bind_rows(select(links2, page = next_page)) %>% 
       distinct() %>% 
       mutate(id = row_number()-1,
              group = case_when(
                str_detect(page, "^\\/home") ~ "Home",
                str_detect(page, ".*google\\+") ~ "Google +",
                TRUE ~ "Other"
              )) 
     # Simple page categorisation. 'group' is used to set the node colours in the Sankey chart
     links2 <- select(links2, next_page, curr_page, link_count) %>% 
       left_join(nodes2, by = c(
         "curr_page" = "page")) %>% 
       select(source = id, everything()) %>% 
       left_join(nodes2, by = c(
         "next_page" = "page")) %>% 
       select(source, target = id, link_count) 
      
    }
    
    chart <- sankeyNetwork(Links = links2, 
                            Nodes = nodes2, 
                            Source = "source",
                            Target = "target", 
                            Value = "link_count",
                            height = sank_height,
                            width = sank_width,
                            nodeWidth = 10,
                            NodeID = "page",
                           NodeGroup = "group",
                            units = "visitors", 
                            fontSize = 8,
                            sinksRight = TRUE) 
  }
  
  chart
}
