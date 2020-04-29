#!/bin/bash
ERRMSG="Usage: ./mvn-test.bash '<output filename>'"

if [[ "$#" -ne 1 ]]; then
    echo $ERRMSG
    exit 1
fi

outputfile=$1

echo "Running tests..."


# Measuring tests
mvn test -Dspark.master=local[8] -Dtest=ir.ac.itrc.rotbenegar.Measuring.VisitorByBrowserTest >${outputfile} 2>&1

mvn test -Dspark.master=local[8] -Dtest=ir.ac.itrc.rotbenegar.Measuring.VisitorByCountryTest >>${outputfile} 2>&1
mvn test -Dspark.master=local[8] -Dtest=ir.ac.itrc.rotbenegar.Measuring.VisitorByOSTest >>${outputfile} 2>&1
mvn test -Dspark.master=local[8] -Dtest=ir.ac.itrc.rotbenegar.Measuring.VisitorByProvinceTest >>${outputfile} 2>&1
mvn test -Dspark.master=local[8] -Dtest=ir.ac.itrc.rotbenegar.Measuring.VisitorByReferrerTest >>${outputfile} 2>&1

# Pipeline tests
mvn test -Dspark.master=local[8] -Dtest=ir.ac.itrc.rotbenegar.Pipeline.RanksAndMeasuresTest >>${outputfile} 2>&1

#Ranking tests
mvn test -Dspark.master=local[8] -Dtest=ir.ac.itrc.rotbenegar.Ranking.LogRankTest >>${outputfile} 2>&1


echo "Tests results:"
cat ${outputfile} | grep Error
