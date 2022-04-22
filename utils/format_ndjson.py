from ast import arg
import html
import ndjson
import argparse
from yattag import Doc, indent
from datetime import date, datetime

parser = argparse.ArgumentParser()
parser.add_argument("--in", "-i", type=str, dest="in_file", required=True)
parser.add_argument("--out", "-o", type=str, dest="out_file", required=True)
parser.add_argument("--title", "-t", type=str, required=True)
args = parser.parse_args()

now = datetime.now().isoformat()

with open(args.in_file.strip(), "r") as ndf:
  data = ndjson.load(ndf)

doc, tag, text, line = Doc().ttl()
with (tag('html')):
  with tag('head'):
    doc.stag(
      'link', 
      rel='stylesheet', 
      href='https://unpkg.com/purecss@2.1.0/build/pure-min.css', 
      integrity='sha384-yHIFVG6ClnONEA5yB5DJXfW2/KC173DIQrYoZMEtBvGzmf0PKiGyNEqe9N6BNDBH', 
      crossorigin='anonymous'
    )
    with tag('style'):
      text("""
      table {
        table-layout: fixed;
        width: 100%;
      }
      td {
        word-wrap: break-word;
      }
      """)
  with tag('body'):
    with tag('div', klass='pure-g'):
      with tag('div', klass='pure-u-1'):
        line('h2', args.title)

      with tag('div', klass='pure-u-1'):
        with tag('table', klass='pure-table pure-table-striped pure-table-bordered'):
          with tag('thead'):
            with tag('tr'):
              for key in data[0].keys():
                with tag('td'):
                  with tag('b'):
                    with tag('i'):
                      text(key)

          with tag('tbody'):
            for row in data:
              with tag('tr'):
                for key in row.keys():
                  with tag('td'):
                    value = row[key]
                    if value != None:
                      text(row[key])

with open(args.out_file, "w") as html_f:
  html_f.write(indent(doc.getvalue()))