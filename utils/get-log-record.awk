#! /usr/bin/env -S gawk --use-lc-numeric -f

BEGIN {
    REC_NR = 0
}

match($0, ENVIRON["RECORD_PREFIX"] "([0-9]+\\.?[0-9]*)", m) {
    gsub("\\.", ",", m[1]); printf "%s%f", (REC_NR++ ? ";" : ""), m[1]
}

END {
    printf "\n"
}
