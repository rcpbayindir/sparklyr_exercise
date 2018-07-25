read_sale <- function(shop_id, year, month) {
  
  path_template = "../future_sales_data/sales_train.parquet/shop_id={{{shop_id}}}/year={{{year}}}/month={{{month}}}/"
  data = list(
    shop_id=shop_id,
    month=month,
    year=year)
  path <- whisker.render(path_template, data)
  if (dir.exists(gsub("[\\]", "", path))) {
    spark_read_parquet(sc, "sales", path) %>%
      mutate(
        shop_id = as.integer(shop_id),
        year=as.integer(year),
        month=as.integer(month))
  } else {
    NULL
  }
}
sales_sdf <- read_sale(0, 2013, 1)
sales_sdf


read_sales <- function(shop_ids, years, months) {
  sdf <- NULL
  for (shop_id in shop_ids) {
    for (year in years) {
      for (month in months) {
        new_sdf <- read_sale(shop_id, year, month)
        if (!is.null(sdf)) {
          if (!is.null(new_sdf)) {
            sdf <- union_all(sdf, new_sdf)
          }
        } else {
          sdf <- new_sdf
        }
      }
    }
  }
  sdf
}