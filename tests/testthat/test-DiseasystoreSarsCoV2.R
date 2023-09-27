test_that("DiseasystoreSarsCoV2 works", {

  conn <- test_conn()

  diseasystore::drop_diseasystore(schema = "test_ds")

  start_date <- as.Date("2021-03-03")
  end_date   <- as.Date("2021-03-05")
  fs <- DiseasystoreSarsCoV2$new(start_date = start_date,
                                 end_date   = end_date,
                                 verbose = !testthat::is_testing(),
                                 source_conn = conn,
                                 target_schema = "test_ds")

  # Check feature store has been created
  checkmate::expect_class(fs, "DiseasystoreSarsCoV2")

  # Check all FeatureHandlers have been initialized
  private <- fs$.__enclos_env__$private
  self <- fs$.__enclos_env__$self
  feature_handlers <- purrr::keep(ls(private), ~ startsWith(., "sars_cov_2")) |>
    purrr::map(~ purrr::pluck(private, .))

  purrr::walk(feature_handlers, ~ {
    checkmate::expect_class(.x, "FeatureHandler")
    checkmate::expect_function(.x %.% compute)
    checkmate::expect_function(.x %.% get)
    checkmate::expect_function(.x %.% key_join)
  })

  # Create function to test the output
  date_tester <- function(output, start_date, end_date) {
    # The output should have data on start and end date (inclusive)
    expect_true(diseasystore::mg_slice_time(output, start_date, from_ts = valid_from, until_ts = valid_until) |>
                  diseasystore::mg_nrow() > 0)
    expect_true(diseasystore::mg_slice_time(output, end_date,   from_ts = valid_from, until_ts = valid_until) |>
                  diseasystore::mg_nrow() > 0)
  }


  # Attempt to get features from the feature store
  # then check that they match the expected value from the generators
  available_features <- fs$available_features[startsWith(names(fs$fs_map), "sars_cov_2_")]
  feature_handlers <- names(fs$fs_map)[startsWith(names(fs$fs_map), "sars_cov_2_")]

  purrr::walk2(available_features, feature_handlers, ~ {
    feature <- fs$get_feature(.x, start_date = start_date, end_date = end_date)

    feature_checksum <- feature |>
      dplyr::collect() |>
      mg_digest_to_checksum() |>
      dplyr::pull("checksum") |>
      sort()

    reference_generator <- eval(parse(text = paste0(.y, "_(self)"))) %.% compute

    reference <- reference_generator(start_date  = start_date,
                                     end_date    = end_date,
                                     slice_ts    = fs %.% slice_ts,
                                     source_conn = fs %.% source_conn)

    reference_checksum <- reference |>
      dplyr::collect() |>
      mg_digest_to_checksum() |>
      dplyr::pull("checksum") |>
      sort()

    expect_identical(feature_checksum, reference_checksum)
    date_tester(feature, start_date, end_date)
  })


  # Attempt to get features from the feature store (using different dates)
  # then check that they match the expected value from the generators
  purrr::walk2(available_features, feature_handlers, ~ {
    start_date <- as.Date("2021-03-03")
    end_date   <- as.Date("2021-03-06")

    feature <- fs$get_feature(.x, start_date = start_date, end_date = end_date)

    feature_checksum <- feature |>
      dplyr::collect() |>
      mg_digest_to_checksum() |>
      dplyr::pull("checksum") |>
      sort()

    reference_generator <- eval(parse(text = paste0(.y, "_(self)"))) %.% compute

    reference <- reference_generator(start_date  = start_date,
                                     end_date    = end_date,
                                     slice_ts    = fs %.% slice_ts,
                                     source_conn = fs %.% source_conn)

    reference_checksum <- reference |>
      dplyr::collect() |>
      mg_digest_to_checksum() |>
      dplyr::pull("checksum") |>
      sort()

    expect_identical(feature_checksum, reference_checksum)
    date_tester(feature, start_date, end_date)
  })


  # Attempt to get features from the feature store (using different slice_ts)
  # then check that they match the expected value from the generators
  purrr::walk2(available_features, feature_handlers, ~ {

    feature <- fs$get_feature(.x, slice_ts = "2022-07-01 09:00:00")

    feature_checksum <- feature |>
      dplyr::collect() |>
      mg_digest_to_checksum() |>
      dplyr::pull("checksum") |>
      sort()

    reference_generator <- eval(parse(text = paste0(.y, "_(self)"))) %.% compute

    reference <- reference_generator(start_date  = start_date,
                                     end_date    = end_date,
                                     slice_ts    = "2022-07-01 09:00:00",
                                     source_conn = fs %.% source_conn)

    reference_checksum <- reference |>
      dplyr::collect() |>
      mg_digest_to_checksum() |>
      dplyr::pull("checksum") |>
      sort()

    expect_identical(feature_checksum, reference_checksum)
    date_tester(feature, start_date, end_date)
  })


  # Attempt to perform the possible key_joins
  available_observables  <- purrr::keep(available_features,    ~ startsWith(., "n_"))
  available_aggregations <- purrr::discard(fs$available_features, ~ startsWith(., "n_"))

  tryCatch({
    dbplyr::cross_join.tbl_lazy() # Only run these tests if this function exists

    expand.grid(observable  = available_observables,
                aggregation = available_aggregations) |>
      purrr::pwalk(~ {
        # This code may fail (gracefully) in some cases. These we catch here
        output <- tryCatch({
          fs$key_join_features(observable = as.character(..1),
                               aggregation = eval(parse(text = glue::glue("rlang::quos({..2})"))))
        }, error = function(e) {
          expect_equal(e$message, paste("(At least one) aggregation feature does not match observable aggregator.",
                                        "Not implemented yet."))
          return(NULL)
        })

        # If the code does not fail, we test the output
        if (!is.null(output)) {
          date_tester(dplyr::collect(output), start_date, end_date)
        }
      })
  }, error = function(e) NULL)

  # Cleanup
  rm(fs)
  DBI::dbDisconnect(conn)
})
