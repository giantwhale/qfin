context("EMA")

test_that("test exp_moving_avg", {
    skip_if_not_installed("rqfin")

    set.seed(527)

    x <- cumsum(rnorm(100))
    
    decay <- 0.95
    dd  <- 0.95 ^ (100:1)
    ema <- cumsum(x * dd) / cumsum(dd)

    expect_equal(ema, exp_moving_avg(x, decay, 1), tolerance=1e-8)
})