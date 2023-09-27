# Create a connection to test on
test_conn <- function() {
  conn <- diseasystore::mg_get_connection()
  DBI::dbExecute(con = conn, "SET ROLE ssi_mg") # Lower to ssi_mg privileges if elevated
  return(conn)
}
