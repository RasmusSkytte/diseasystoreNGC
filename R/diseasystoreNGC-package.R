#' @keywords internal
"_PACKAGE"

#' @import diseasystore
NULL

# Set default options for the package related to the Google COVID-19 store
rlang::on_load({
  options(diseasystore.target_conn = \() diseasystore::mg_get_connection())
  options(diseasystore.target_conn = NULL)
})
