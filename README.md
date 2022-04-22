# fhir-termserver-backup
Backup Resources on a terminology server that uses FHIR

To run, you will need Python 3 with `requests` installed - it is recommended to use a virtual environment and install the requirements from `requirements.txt`:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python3 backup.py --help
```

Invoke the help with `python3 backup.py --help` to get started.

Additionally, there is a script for downloading and diffing the resource list, which could be invoked in a cron job alongside the backup script, in order to get a human-readable list of resources that are present on the server (and which have changed metadata).

Run this script using:

```bash
./download-resource-list.sh http://your-server-endpoint.domain/fhir /path/to/output/dir
```