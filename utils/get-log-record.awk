#! /usr/bin/env -S gawk --use-lc-numeric -f

match($0, ENVIRON["RECORD_PREFIX"] "([0-9]+\\.?[0-9]*)", m) {
    gsub("\\.", ",", m[1]); printf "%f;", m[1]
}

END {
    printf "\n"
}
