#include <iostream>
#include <Rcpp.h>
using namespace Rcpp;

//' index of the first TRUE value
//' @export
// [[Rcpp::export]]
int first_true(LogicalVector x) {
    for (size_t i=0; i<x.size(); ++i) {
        if (x[i] == TRUE) {
            return i + 1; // R is indexed by i + 1
        }
    }
    return NA_INTEGER;
}


//' Index of the last TRUE value
//' @export
// [[Rcpp::export]]
int last_true(LogicalVector x) {
    size_t n = x.size();
    for (size_t i=n; i>0; --i) {
        if (x[i-1] == TRUE) {
            return i; // R is indexed by i + 1
        }
    }
    return NA_INTEGER;
}

