#' @title Base class for individual level feature stores
#'
#' @description
#'   This `DiseasystoreGeneric` [R6][R6::R6Class] brings support for individual level feature stores
#' @export
DiseasystoreGeneric <- R6::R6Class( # nolint: object_name_linter
  classname = "DiseasystoreGeneric",
  inherit = diseasystore::DiseasystoreBase,

  private = list(
    fs_generic = list("geography"       = "region_id",
                      "geography"       = "province_id",
                      "geography"       = "municipality_id",
                      "geography"       = "parish_id",
                      "geography"       = "n_population",
                      "age"             = "age",
                      "gender"          = "gender",
                      "active_c_status" = "active_c_status",
                      "active_c_status" = "birth"),
    fs_specific     = NULL,
    case_definition = NULL,

    generic_active_c_status = NULL,
    generic_geography       = NULL,
    generic_age             = NULL,
    generic_gender          = NULL,

    initialize_feature_handlers = function() {

      # Here we initialize each of the feature handlers for the class
      # See the documentation above at the corresponding methods
      private$generic_active_c_status <- generic_active_c_status_(self)
      private$generic_geography       <- generic_geography_(self)
      private$generic_age             <- generic_age_(self)
      private$generic_gender          <- generic_gender_(self)
    }
  )
)


generic_active_c_status_ <- function(self) {
  diseasystore::FeatureHandler$new(
    compute = function(start_date, end_date, slice_ts, source_conn) {
      out <- diseasystore::mg_get_table(conn = source_conn,
                                        db_table_id = "mg.epicpr_c_status",
                                        slice_ts = slice_ts)

      if (testthat::is_testing()) {
        out <- out |>
          dplyr::filter(.data$valid_from >= as.Date("2021-03-01"), .data$valid_from <= as.Date("2021-03-06")) |>
          dplyr::arrange(.data$cprnr) |>
          utils::head(10000) |>
          dplyr::compute()
      }

      out <- out |>
        dplyr::rename("key_cprnr" = "cprnr") |>
        dplyr::filter(.data$c_status %in% c("01", "03"),
                      .data$valid_from <= !!end_date, (!!start_date < .data$valid_until) | is.na(.data$valid_until)) |>
        dplyr::distinct()

      return(out)
    },
    key_join = diseasystore::key_join_count
  )
}

generic_geography_ <- function(self) {
  diseasystore::FeatureHandler$new(
    compute = function(start_date, end_date, slice_ts, source_conn) {
      # We want all persons with c_status 01 or 03
      active_c_status <- self$get_feature("birth", start_date, end_date, slice_ts)

      # And their addresses
      address <- diseasystore::mg_get_table(conn = source_conn,
                                            db_table_id = "mg.epicpr_adresse",
                                            slice_ts = slice_ts,
                                            include_slice_info = TRUE)

      if (testthat::is_testing()) {
        address <- address |>
          dplyr::filter(.data$d_tilflyt_dato >= as.Date("2021-03-01"), .data$d_tilflyt_dato <= as.Date("2021-03-06")) |>
          dplyr::arrange(.data$v_pnr) |>
          utils::head(10000) |>
          dplyr::compute()
      }

      address <- address |>
        dplyr::transmute("key_cprnr" = .data$v_pnr, "municipality_id" = .data$c_kom, "parish_id" = .data$c_mynkod,
                         "valid_from" = .data$d_tilflyt_dato, "valid_until" = .data$d_fraflyt_dato,
                         "checksum" = .data$checksum) |>
        dplyr::filter(.data$valid_from <= !!end_date, (!!start_date < .data$valid_until) | is.na(.data$valid_until))

      # We could interlace these directly, but for speed we do a step-wise approach
      out <- dplyr::left_join(address, active_c_status, by = "key_cprnr", suffix = c("", ".p")) |>
        dplyr::compute()

      # Those addresses with a single c_status match, can be handled easily
      # We find the overlap of the c_status information and the address information
      single_unique_match <- out |>
        dplyr::group_by(.data$key_cprnr, .data$checksum) |>
        dplyr::filter(dplyr::n() == 1) |>
        dplyr::ungroup() |>
        dplyr::mutate("valid_from"  = pmax(.data$valid_from,  .data$valid_from.p, na.rm = TRUE),
                      "valid_until" = pmin(.data$valid_until, .data$valid_until,  na.rm = TRUE)) |>
        dplyr::select(!tidyselect::ends_with(".p")) |>
        dplyr::compute()


      if (diseasystore::mg_nrow(address) != diseasystore::mg_nrow(single_unique_match)) {
        stop("This case has not been tested / developed yet")
        # # Those with multiple single c_status matches, needs to be interlaced
        # multiple_matches <- out |>
        #   dplyr::anti_join(single_unique_match, by = c("key_cprnr", "checksum")) |>
        #   dplyr::select(tidyselect::all_of(colnames(address)))
        #
        # tmp <- diseasystore::mg_interlace_sql(list(multiple_matches, active_c_status), by = "key_cprnr")

        #out <- dplyr::union_all(single_unique_match, multiple_matches)
      } else {
        out <- single_unique_match
      }

      # Combine and add geographical information
      geography_index <- diseasystore::mg_get_table(source_conn, "prod.municipalities") |>
        dplyr::select("region_id", "province_id", "municipality_id")
      out <- dplyr::left_join(out, geography_index,
                         by = "municipality_id") |>
        dplyr::select("key_cprnr", "region_id", "province_id", "municipality_id", "parish_id",
                      tidyselect::starts_with("valid_"))

      return(out)
    },
    key_join = diseasystore::key_join_count
  )
}

# Define global variables
utils::globalVariables(c("DATE_PART", "AGE"))

generic_age_ <- function(self) {
  diseasystore::FeatureHandler$new(
    compute = function(start_date, end_date, slice_ts, source_conn) {
      age <- self$get_feature("birth", start_date, end_date, slice_ts) |> # nolint: object_usage_linter
        dplyr::mutate(age_at_start = DATE_PART("year", AGE(!!start_date, .data$birth))) |>
        dplyr::compute()

      # Get all birth dates within the study period
      out <- purrr::map(
        seq(0, ceiling(lubridate::interval(start_date, end_date) / lubridate::years(1))),
        ~ age |>
          dplyr::mutate("age" = .data$age_at_start + .x,
                        "birth_date"      = .data$birth      + .data$age * dplyr::sql("interval '1 year'"),
                        "next_birth_date" = .data$birth_date +       dplyr::sql("interval '1 year'")) |>
          dplyr::filter(.data$birth_date <= !!end_date,  # birthday should fall within study period
                        .data$birth_date < .data$valid_until | is.na(.data$valid_until)) |> # c_status should be 01/03
          dplyr::transmute("key_cprnr" = .data$key_cprnr,
                           "age" = .data$age,
                           "valid_from" = as.Date(.data$birth_date),
                           "valid_until" = pmin(.data$valid_until, as.Date(.data$next_birth_date), na.rm = TRUE))) |>
        purrr::reduce(dplyr::union_all) |>
        dplyr::relocate(tidyselect::starts_with("valid"), .after = dplyr::everything())

      return(out)
    },
    key_join = diseasystore::key_join_count
  )
}

generic_gender_ <- function(self) {
  diseasystore::FeatureHandler$new(
    compute = function(start_date, end_date, slice_ts, source_conn) {

      cpr3_t_person <- diseasystore::mg_get_table(conn = source_conn,
                                                  db_table_id = "prod.cpr3_t_person",
                                                  slice_ts = slice_ts,
                                                  include_slice_info = TRUE)

      out <- self$get_feature("active_c_status", start_date, end_date, slice_ts) |>
        dplyr::left_join(cpr3_t_person |>
                           dplyr::transmute("key_cprnr" = .data$v_pnr,
                                            "gender" = dplyr::if_else(.data$c_kon == "M", "Male", "Female")),
                         by = "key_cprnr") |>
        dplyr::select("key_cprnr", "gender", "valid_from", "valid_until") |>
        dplyr::distinct()

      return(out)
    },
    key_join = diseasystore::key_join_count
  )
}
