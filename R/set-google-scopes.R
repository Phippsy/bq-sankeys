#' Get list of Google API scopes
#'
#' @param update
#' (Default FALSE). If TRUE, will attempt to read the latest list of scopes from https://developers.google.com/identity/protocols/googlescopes.
#'
#' @return
#' Returns a data frame of scope urls
#'
#' @export
#'
#' @examples
#' scopes <- get_google_scopes()
get_google_scopes <- function() {
    library(dplyr)
    scopes_page <- xml2::read_html("https://developers.google.com/identity/protocols/googlescopes")
    
    # Get vector of API names from h2 elements
    scope_headers <- scopes_page %>%
      rvest::html_nodes("h2") %>%
      rvest::html_text()
    
    # Get list of scope urls + descriptions from tables
    scope_links <- scopes_page %>%
      rvest::html_nodes("table") %>%
      rvest::html_table(header = FALSE) # assigns duplicate var names ('Scopes') if we exclude header = FALSE
    
    # Combine API names with scope urls
    scope_list <- purrr::map2_df(scope_headers, scope_links,
                                 ~ .y %>% dplyr::mutate(api_name = .x)) %>%
      dplyr::filter(X1 != "Scopes") %>%
      dplyr::select(api_name, scope_url = X1, scope_description = X2)
    scope_list
}


#' Set Scopes for Google Authentication
#'
#' @param packages
#' A shorthand list of packages to set scopes for. Currently available: "analytics", "bigquery", "youtube", "compute engine", "drive", "sheets", "storage", "tag manager", "search console"
#' @return
#' Returns nothing
#' @export
#'
#' @examples
#' set_google_scopes(c("analytics", "drive"))
set_google_scopes <- function(packages) {
  
  library(dplyr)
  scopes <- get_google_scopes()
  
  # Lookup list of user-friendly API names
  scope_names <- tibble::tibble(
    short_names = c("analytics", "bigquery", "youtube", "compute engine", "drive", "sheets", "storage", "tag manager",
                    "search console", "shortener", "gmail", "vision"),
    long_names = c( "Google Analytics Reporting API, v4", "BigQuery API, v2",
                    "YouTube Analytics API, v1", "Compute Engine API, v1", "Drive API, v3",
                    "Google Sheets API, v4", "Cloud Storage JSON API, v1", "Tag Manager API, v2",
                    "Search Console API, v3", "URL Shortener API, v1", "Gmail API, v1", "Google Cloud Vision API, v1")
  )
  
  # Convert user-provided package vector to tibble for semi join
  packages <- tibble(short_names = packages)
  
  # filter lookup list to user-selected packages only
  selected_packages <- scope_names %>%
    dplyr::semi_join(packages)
  
  # Filter full scope list based on filtered lookup table
  selected_scopes <- scopes %>%
    semi_join(selected_packages, by = c("api_name" = "long_names")) %>%
    distinct()
  
  # Set options
  options("googleAuthR.scopes.selected" = selected_scopes$scope_url)
  
  message("Setting the following scopes:")
  writeLines(selected_scopes$scope_url, sep = "\n")
  
}



