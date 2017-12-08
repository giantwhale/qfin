#include <iostream>
#include <Rcpp.h>
using namespace Rcpp;

//' Running sum
//'
//' @param x numeric vector
//' @param w integer, windows size, results are right aligned if w < 0, left aligned if w > 0
//' @param min_size if number of non-NA elements is fewer than min_size, return NA
//'
//' @export
// [[Rcpp::export]]
NumericVector running_sum(NumericVector x, int w, int min_size=1) {
    size_t sz  = x.size();
    size_t cnt = 0;  // non_na counts
    NumericVector res(sz, NA_REAL);

    if (w == 0) throw std::range_error("window size w must be non-zero");
    
    bool left_aligned = w < 0;
    w = w < 0 ? -w : w;

    if (w > sz) throw std::range_error("Invalid window size");
    if (min_size <= 0) throw std::range_error("min_size must be positive");

    size_t i = 0;
    double s = 0;
    if (left_aligned) {
        for (i=0; i<sz; ++i) {
            if (!NumericVector::is_na(x[i])) {
                ++cnt;
                s += x[i];
            }
            if (i >= w && !NumericVector::is_na(x[i-w])) {
                --cnt;
                s -= x[i-w];
            }
            if (cnt >= min_size) {
                res[i] = s;
            }
        }
    } else {
        // right aligned
        for (i=0; i<w; ++i) {
            if (!NumericVector::is_na(x[i])) {
                ++cnt;
                s += x[i];
            }
        }

        for (; i<sz+w; ++i) {
            if (cnt >= min_size) {
                res[i-w] = s;
            }
            if (i<sz && !NumericVector::is_na(x[i])) {
                ++cnt;
                s += x[i];
            }
            if (!NumericVector::is_na(x[i-w])) {
                --cnt;
                s -= x[i-w];
            }
        }
    }

    return res;

}
