.phony: install clean test

install:
	Rscript compile.R
	R CMD INSTALL pkg

test:
	Rscript compile.R
	Rscript run_test.R

clean:
	find . -type f -name '*.o' -exec rm -f {} \;
	find . -type f -name '*.so' -exec rm -f {} \;
