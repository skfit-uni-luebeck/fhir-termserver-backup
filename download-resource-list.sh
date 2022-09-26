#!/bin/bash

die () {
    echo >&2 "$@"
    exit 1
}

[ "$#" -eq 2 ] || die "2 arguments (URL and output path) required, $# provided"
if [ -d $2 ]; then false; else die "The output directory $2 does not exist."; fi

endpoint="$1"
common_params="url,version,id,name,title,status"
cs_params="$common_params,valueSet"
vs_params="$common_params"
cm_params="$common_params,sourceUri,targetUri,sourceCanonical,targetCanonical"
size="1000"
output_dir="$2"

cs_url="$endpoint/CodeSystem?_elements=$cs_params&_count=$size&_format=json"
vs_url="$endpoint/ValueSet?_elements=$vs_params&_count=$size&_format=json"
cm_url="$endpoint/ConceptMap?_elements=$cm_params&_count=$size&_format=json"

today=$(date -I)
yesterday=$(date -I -d "yesterday")

today_cs="$output_dir/CodeSystem-$today.ndjson"
today_vs="$output_dir/ValueSet-$today.ndjson"
today_cm="$output_dir/ConceptMap-$today.ndjson"

echo "requesting from $cs_url"
curl -s $cs_url | jq ".entry[].resource" | jq -sc "sort_by(.url) | .[] | { $cs_params }" > $today_cs
echo "requesting from $vs_url"
curl -s $vs_url | jq ".entry[].resource" | jq -sc "sort_by(.url) | .[] | { $vs_params }" > $today_vs
echo "requesting from $cm_url"
curl -s $cm_url | jq ".entry[].resource" | jq -sc "sort_by(.url) | .[] | { $cm_params }" > $today_cm

source .venv/bin/activate
python ./utils/format_ndjson.py --in="$today_cs" --out "$today_cs.html" --title "CodeSystem $today"
python ./utils/format_ndjson.py --in="$today_vs" --out "$today_vs.html" --title "ValueSet $today"
python ./utils/format_ndjson.py --in="$today_cm" --out "$today_cm.html" --title "ConceptMap $today"

yesterday_cs="$output_dir/CodeSystem-$yesterday.ndjson"
yesterday_vs="$output_dir/ValueSet-$yesterday.ndjson"
yesterday_cm="$output_dir/ConceptMap-$yesterday.ndjson"

diff_cs="$output_dir/Diff-CodeSystem-$yesterday-$today.html"
diff_vs="$output_dir/Diff-ValueSet-$yesterday-$today.html"
diff_cm="$output_dir/Diff-ConceptMap-$yesterday-$today.html"

if [ -f $yesterday_cs ]; then diff -u $yesterday_cs $today_cs | ./utils/diff2html.sh > $diff_cs; fi
if [ -f $yesterday_vs ]; then diff -u $yesterday_vs $today_vs | ./utils/diff2html.sh > $diff_vs; fi
if [ -f $yesterday_cm ]; then diff -u $yesterday_cm $today_cm | ./utils/diff2html.sh > $diff_cm; fi
