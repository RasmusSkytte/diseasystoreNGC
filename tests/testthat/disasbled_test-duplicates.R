test_that("We dont get duplicates anymore", {
  conn <- test_conn()

  slice_ts_1 <- "2022-06-14 09:00:00"
  slice_ts_2 <- "2023-09-01 09:00:00"

  diseasystore::drop_diseasystore(schema = "test_ds")

  start_date <- as.Date("2021-03-03")
  end_date   <- as.Date("2021-03-05")
  fs <- DiseasystoreGeneric$new(start_date = start_date,
                                end_date   = end_date,
                                verbose = TRUE,
                                target_conn = conn,
                                target_schema = "test_ds",
                                slice_ts = slice_ts_1)


  t1 <- mg::get_table(conn, "mg.epicpr_c_status", slice_ts = slice_ts_1) |>
    dplyr::filter(cprnr == "00540e69a0d9fa80473768797b514e00")
  expect_equal(mg::nrow(t1), 1)

  fs$get_feature("active_c_status")

  t1 <- mg::get_table(conn, "test_ds.generic_active_c_status", slice_ts = slice_ts_1, include_slice_info = T) |>
    dplyr::filter(key_cprnr == "00540e69a0d9fa80473768797b514e00")
  expect_equal(mg::nrow(t1), 1)


  t2 <- mg::get_table(conn, "mg.epicpr_c_status", slice_ts = slice_ts_2) |>
    dplyr::filter(cprnr == "00540e69a0d9fa80473768797b514e00")
  expect_equal(mg::nrow(t2), 2)

  fs$get_feature("active_c_status", slice_ts = slice_ts_2)

  t2 <- mg::get_table(conn, "test_ds.generic_active_c_status", slice_ts = slice_ts_2, include_slice_info = T) |>
    dplyr::filter(key_cprnr == "00540e69a0d9fa80473768797b514e00")

  expect_equal(mg::nrow(t2), 1)
})

