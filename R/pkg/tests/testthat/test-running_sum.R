context("running_sum")

test_that("running_sum", {
  skip_if_not_installed("rqfin")

  n <- NA

  # left aligned
  x <- c(1,2,3,4, 5, 6, 7, 8, 9)
  s <- c(1,3,6,9,12,15,18,21,24)
  expect_equal(s, running_sum(x, -3))

  x <- c(1,2,3,4, 5, 6, 7, 8, 9)
  s <- c(n,3,6,9,12,15,18,21,24)
  expect_equal(s, running_sum(x, -3, 2))

  x <- c(1,2,n,n,5, 6, 7, 8, 9)
  s <- c(n,3,3,n,n,11,18,21,24)
  expect_equal(s, running_sum(x, -3, 2))


  # right aligned
  x <- c(1,2, 3, 4, 5, 6, 7, 8,9)
  s <- c(6,9,12,15,18,21,24,17,9)
  expect_equal(s, running_sum(x, 3))

  x <- c(1,2, 3,4, 5, 6, 7, 8, 9)
  s <- c(6,9,12,15,18,21,24,17,n)
  expect_equal(s, running_sum(x, 3, 2))

  x <- c(1,n,3,4,n, n, 7, 8, 9)
  s <- c(4,7,7,n,n,15,24,17, n)
  expect_equal(s, running_sum(x, 3, 2))


})



test_that("running_mean", {
  skip_if_not_installed("rqfin")

  n <- NA

  # left aligned
  x <- c(1,2,3,4, 5, 6, 7, 8, 9)
  s <- c(1,3,6,9,12,15,18,21,24) / c(1,2,3,3,3,3,3,3,3)
  expect_equal(s, running_mean(x, -3))

  x <- c(1,2,3,4, 5, 6, 7, 8, 9)
  s <- c(n,3,6,9,12,15,18,21,24) / c(1,2,3,3,3,3,3,3,3)
  expect_equal(s, running_mean(x, -3, 2))

  x <- c(1,2,n,n,5, 6, 7, 8, 9) 
  s <- c(n,3,3,n,n,11,18,21,24) / c(n,2,2,n,n,2,3,3,3)
  expect_equal(s, running_mean(x, -3, 2))


  # right aligned
  x <- c(1,2, 3, 4, 5, 6, 7, 8,9)
  s <- c(6,9,12,15,18,21,24,17,9) / c(3,3,3,3,3,3,3,2,1)
  expect_equal(s, running_mean(x, 3))

  x <- c(1,2, 3,4, 5, 6, 7, 8, 9)
  s <- c(6,9,12,15,18,21,24,17,n) / c(3,3,3,3,3,3,3,2,1)
  expect_equal(s, running_mean(x, 3, 2))

  x <- c(1,n,3,4,n, n, 7, 8, 9)
  s <- c(4,7,7,n,n,15,24,17, n) / c(2,2,2,n,n,2,3,2,1)
  expect_equal(s, running_mean(x, 3, 2))


})