library(devtools)
library(roxygen2)

clean_source('pkg')
clean_dll('pkg')
roxygenize('pkg')
document('pkg')
