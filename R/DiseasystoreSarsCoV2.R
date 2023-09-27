#' @title Feature store for individual level SARS-CoV-2
#'
#' @description
#'   This `DiseasystoreSarsCoV2` [R6][R6::R6Class] brings support for individual level feature stores
#' @export
DiseasystoreSarsCoV2 <- R6::R6Class( # nolint: object_name_linter
  classname = "DiseasystoreSarsCoV2",
  inherit = DiseasystoreGeneric,

  private = list(
    fs_specific = list("vaccine_status" = "vaccine_status",
                       "admission"      = "n_admission",
                       "hospital"       = "n_hospital",
                       "hospital"       = "due_to_covid",
                       "positive"       = "n_positive",
                       "test"           = "n_test"),
    .case_definition = "SARS-CoV-2",

    sars_cov_2_vaccine_status = NULL,
    sars_cov_2_admission      = NULL,
    sars_cov_2_hospital       = NULL,
    sars_cov_2_positive       = NULL,
    sars_cov_2_test           = NULL,

    initialize_feature_handlers = function() {

      # Here we initialize each of the feature handlers for the class
      # See the documentation above at the corresponding methods
      private$sars_cov_2_vaccine_status <- sars_cov_2_vaccine_status_(self)
      private$sars_cov_2_admission      <- sars_cov_2_admission_(self)
      private$sars_cov_2_hospital       <- sars_cov_2_hospital_(self)
      private$sars_cov_2_positive       <- sars_cov_2_positive_(self)
      private$sars_cov_2_test           <- sars_cov_2_test_(self)

      super$initialize_feature_handlers()
    }
  )
)



sars_cov_2_vaccine_status_ <- function(self) {
  diseasystore::FeatureHandler$new(
    compute = function(start_date, end_date, slice_ts, source_conn) {
      out <- diseasystore::mg_get_table(source_conn, "mg.vaccine_history", slice_ts = slice_ts)

      if (testthat::is_testing()) {
        out <- out |>
          dplyr::filter(.data$valid_from >= as.Date("2021-03-01"), .data$valid_from <= as.Date("2021-03-06")) |>
          dplyr::arrange(.data$cprnr) |>
          utils::head(10000) |>
          dplyr::compute()
      }

      out <- out |>
        dplyr::filter((!!start_date < .data$valid_until) | is.na(.data$valid_until), .data$valid_from <= !!end_date) |>
        dplyr::rename("key_cprnr" = "cprnr") |>
        dplyr::select("key_cprnr", "vaccine_status", "valid_from", "valid_until")

      return(out)
    },
    key_join = diseasystore::key_join_count
  )
}


sars_cov_2_admission_ <- function(self) {
  diseasystore::FeatureHandler$new(
    compute = function(start_date, end_date, slice_ts, source_conn) {
      out <- diseasystore::mg_get_table(source_conn, "prod.covid_19_patientlinelist", slice_ts = slice_ts)

      if (testthat::is_testing()) {
        out <- out |>
          dplyr::filter(.data$newlyadmdate >= as.Date("2021-03-01"), .data$newlyadmdate <= as.Date("2021-03-06")) |>
          dplyr::arrange(.data$cpr) |>
          utils::head(10000) |>
          dplyr::compute()
      }

      out <- out |>
        dplyr::filter(!is.na(.data$newlyadmdate)) |>
        dplyr::transmute("key_cprnr"    = .data$cpr,
                         "due_to_covid" = stringr::str_detect(.data$firstadmclassv2, "% pga. covid-19"),
                         "valid_from"   = .data$newlyadmdate,
                         "valid_until"  = as.Date(.data$newlyadmdate + lubridate::days(1))) |>
        dplyr::filter((!!start_date < .data$valid_until) | is.na(.data$valid_until), .data$valid_from <= !!end_date)

      return(out)
    },
    key_join = diseasystore::key_join_count
  )
}

sars_cov_2_hospital_ <- function(self) {
  diseasystore::FeatureHandler$new(
    compute = function(start_date, end_date, slice_ts, source_conn) {
      out <- diseasystore::mg_get_table(source_conn, "prod.covid_19_patientlinelist", slice_ts = slice_ts)

      if (testthat::is_testing()) {
        out <- out |>
          dplyr::filter(.data$newlyadmdate <= as.Date("2021-03-06"),
                        pmin(.data$firstdsc + lubridate::days(1),
                             as.Date(.data$newlyadmdate + lubridate::days(91)),
                             na.rm = TRUE) >= as.Date("2021-03-01")) |>
          dplyr::arrange(.data$cpr) |>
          utils::head(10000) |>
          dplyr::compute()
      }

      out <- out |>
        dplyr::filter(!is.na(.data$newlyadmdate)) |>
        dplyr::transmute("key_cprnr"    = .data$cpr,
                         "due_to_covid" = stringr::str_detect(.data$firstadmclassv2, "% pga. covid-19"),
                         "valid_from"   = .data$newlyadmdate,
                         "valid_until"  = pmin(.data$firstdsc + lubridate::days(1),
                                             as.Date(.data$newlyadmdate + lubridate::days(91)), na.rm = TRUE)) |>
        dplyr::filter((!!start_date < .data$valid_until) | is.na(.data$valid_until), .data$valid_from <= !!end_date)

      return(out)
    },
    key_join = diseasystore::key_join_count
  )
}

sars_cov_2_positive_ <- function(self) {
  diseasystore::FeatureHandler$new(
    compute = function(start_date, end_date, slice_ts, source_conn) {
      out <- diseasystore::mg_get_table(source_conn, "mg.miba", slice_ts = slice_ts)

      if (testthat::is_testing()) {
        out <- out |>
          dplyr::filter(.data$prdate >= as.Date("2021-03-01"), .data$prdate <= as.Date("2021-03-06")) |>
          dplyr::arrange(.data$cprnr) |>
          utils::head(10000) |>
          dplyr::compute()
      }

      out <- out |>
        dplyr::filter(.data$casedef == "SARS2", .data$new_infection == 1,
                      .data$prdate <= !!end_date, !!start_date <= .data$prdate) |>
        dplyr::transmute("key_cprnr"     = .data$cprnr,
                         "focus_lineage" = .data$focus_lineage,
                         "valid_from"    = .data$prdate,
                         "valid_until"   = as.Date(.data$prdate + lubridate::days(1)))

      return(out)
    },
    key_join = diseasystore::key_join_count
  )
}

sars_cov_2_test_ <- function(self) {
  diseasystore::FeatureHandler$new(
    compute = function(start_date, end_date, slice_ts, source_conn) {
      out <- diseasystore::mg_get_table(source_conn, "mg.miba", slice_ts = slice_ts)

      if (testthat::is_testing()) {
        out <- out |>
          dplyr::filter(.data$prdate >= as.Date("2021-03-01"), .data$prdate <= as.Date("2021-03-06")) |>
          dplyr::arrange(.data$cprnr) |>
          utils::head(10000) |>
          dplyr::compute()
      }

      out <- out |>
        dplyr::filter(.data$casedef == "SARS2",
                      .data$prdate <= !!end_date, !!start_date <= .data$prdate) |>
        dplyr::transmute("key_cprnr"     = .data$cprnr,
                         "focus_lineage" = .data$focus_lineage,
                         "valid_from"    = .data$prdate,
                         "valid_until"   = as.Date(.data$prdate + lubridate::days(1)))

      return(out)
    },
    key_join = diseasystore::key_join_count
  )
}

# Set default options for the package related to the Google COVID-19 store
rlang::on_load({
  options(diseasystore.DiseasystoreSarsCoV2.target_conn = \() diseasystore::mg_get_connection())
})
