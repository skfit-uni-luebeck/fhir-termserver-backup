import argparse
import json
import multiprocessing as mp
import os
import re
import shutil
import sys
import tarfile
import unicodedata
from dataclasses import dataclass
from datetime import date, timedelta, datetime
from os import path, makedirs, listdir
from typing import List, Dict, Any

import requests


def parse_args(print_args: bool = True) -> argparse.Namespace:
    """create the argument parser

    Args:
        print_args (bool, optional): If true, the arguments will be printed to stdout after parsing. Defaults to True.

    Returns:
        argparse.Namespace: the parsed arguments as a Namespace object
    """
    default_url = "http://localhost:8080/fhir"

    default_resource_types = ["CodeSystem", "ValueSet", "ConceptMap", "StructureDefinition"]

    parser = argparse.ArgumentParser(
        "Terminology Server Backup")
    parser.add_argument("--endpoint",
                        "-e",
                        default=default_url,
                        type=str,
                        dest="endpoint",
                        help="The FHIR endpoint of the server (Default: '%(default)s')")
    parser.add_argument("--resource-types",
                        "-r",
                        nargs="+",
                        default=default_resource_types,
                        type=str,
                        dest='resource_types',
                        help="the resource types to back-up. Separate with Space (Default: '%(default)s')")
    parser.add_argument("--header",
                        help="headers to pass to the HTTP method. Use for authentication if required! "
                             "If multiple required, repeat argument. (Default: None)",
                        dest="headers",
                        action="append")
    parser.add_argument("--out-dir", "-o",
                        default="./output",
                        help="destination directory (Default %(default)s)",
                        dest="out_dir",
                        type=str)
    parser.add_argument("--delete-days", "-d",
                        default=0,
                        type=int,
                        dest='delete_days',
                        help="remove folders from at least (>=) these many days ago "
                             "(Default %(default)s, for no removal)")
    parser.add_argument("--tarball", "-t",
                        action='store_true',
                        help='create a tarball from the downloaded file')
    parser.add_argument("--parallel", "-l",
                        default=1,
                        type=int,
                        help="number of parallel GETs to carry out (Default %(default)s for no parallel execution)")

    parsed_args = parser.parse_args()

    parsed_args.endpoint = parsed_args.endpoint.strip("/")
    parsed_args.parallel = max(parsed_args.parallel, 1)
    parsed_args.delete_days = max(parsed_args.delete_days, 0)

    if print_args:
        for arg in vars(parsed_args):
            print(f" - {arg}: {getattr(parsed_args, arg)}")

    return parsed_args


@dataclass
class BundleResponse:
    """store the entry in a bundle GET operation
    """
    resource_id: str
    title: str
    canonical_url: str
    url: str
    version: str


def perform_request_as_json(url: str, headers: List[str] = None) -> Dict[str, Any]:
    """perform a GET request to url with headers and return the JSON representation of the response

    Args:
        url (str): the endpoint to query
        headers (List[str], optional): the headers to pass to requests.get. Defaults to None.

    Raises:
        ValueError: if the status code is not 200

    Returns:
        Dict[str, Any]: The JSON body as a Dict
    """
    rx = requests.get(url, headers=headers)
    if rx.status_code != 200:
        raise ValueError(
            f"HTTP Error {rx.status_code} getting from {url}", rx)
    return rx.json()


def get_resource_urls_from_server(fhir_endpoint: str, resource_type: str, headers: List[str] = None) \
        -> List[BundleResponse]:
    """get the urls of all the resources of the given type from the server. Walks through bundles!

    Args:
        fhir_endpoint (str): the endpoint
        resource_type (str): the resource type (gets included in the request url)
        headers (List[str], optional): the headers to pass to requests.get. Defaults to None.

    Returns:
        List[BundleResponse]: The parsed entries of the original bundle
    """
    request_url = f"{fhir_endpoint}/{resource_type}"
    bundle_responses: List[BundleResponse] = []
    next_link = request_url

    while next_link:
        print("Requesting from: ", next_link)
        bundle_json = perform_request_as_json(next_link, headers)
        bundle_responses += bundle_json_to_bundle_response_list(bundle_json)
        next_link = bundle_json_get_next_link(bundle_json)

    return bundle_responses


def bundle_json_get_next_link(bundle_json: Dict[str, Any]) -> str:
    """get the 'next' link from a bundle, if it exists

    Args:
        bundle_json (Dict[str, Any]): the bundle to examine

    Returns:
        str: the url of the 'next' bundle or None
    """
    filtered_link = [link for link in bundle_json["link"] if link["relation"] == "next"]
    if len(filtered_link) > 0:
        return filtered_link[0]["url"]
    return ""


def bundle_json_to_bundle_response_list(bundle_json: Dict[str, Any]) -> List[BundleResponse]:
    """parse every entry in the bundle as a BundleResponse

    Args:
        bundle_json (Dict[str, Any]): the bundle to parse

    Returns:
        List[BundleResponse]: the parsed entries
    """
    if 'entry' not in bundle_json.keys():
        return []
    responses: List[BundleResponse] = []
    for entry in bundle_json['entry']:
        url = entry["fullUrl"]
        title = entry["resource"].get("name", None)
        canonical_url = entry["resource"]["url"]
        version = entry["resource"].get("version", None)
        resource_id = entry["resource"]["id"]
        responses.append(BundleResponse(
            resource_id,
            title,
            canonical_url,
            url,
            version))

    return responses


def download_resource(resource_type: str, r: BundleResponse, out_dir: str) -> str:
    """download a resource from the fully-qualified url in r to out_dir

    Args:
        resource_type (str): the resource type, used in the output filename
        r (BundleResponse): the parsed entry from the search bundle
        out_dir (str): the output directory

    Returns:
        str: the fully-qualified output path
    """
    rx = perform_request_as_json(r.url)

    target_filename = f"{resource_type}-{r.resource_id}_{r.title}_{today}"
    target_abspath = path.join(out_dir, slugify(target_filename)) + ".json"
    with open(target_abspath, "w") as fs:
        json.dump(rx, fs, indent=2)
    sys.stdout.flush()
    return target_abspath


def slugify(value, allow_unicode=False):
    """
    Convert to ASCII if 'allow_unicode' is False. Convert spaces or repeated
    dashes to single dashes. Remove characters that aren't alphanumerics,
    underscores, or hyphens. Convert to lowercase. Also strip leading and
    trailing whitespace, dashes, and underscores.
    via https://stackoverflow.com/a/295466
    """
    value = str(value)
    if allow_unicode:
        value = unicodedata.normalize('NFKC', value)
    else:
        value = unicodedata.normalize('NFKD', value).encode(
            'ascii', 'ignore').decode('ascii')
    value = re.sub(r'[^\w\s-]', '', value.lower())
    return re.sub(r'[-\s]+', '-', value).strip('-_')


def remove_old_directories():
    print("\n\n########\n\n")
    print("REMOVING")
    if args.delete_days <= 0:
        print("No directories were removed")
        return
    print(f"Removing from {args.out_dir}, >= {args.delete_days} ago")
    folder_names = sorted(listdir(args.out_dir))
    today_date = date.fromisoformat(today)
    cutoff_date = today_date - timedelta(days=args.delete_days)
    print("Cutoff Date:", cutoff_date)
    to_delete = [
        fn for fn in folder_names if date.fromisoformat(fn) <= cutoff_date]
    print("These folders will be deleted:", to_delete)
    for fn in to_delete:
        full_path = path.abspath(path.join(args.out_dir, fn))
        print(f" - {fn}: ", end='')
        try:
            print(f"{len(listdir(full_path))} sub-directories", end='')
            shutil.rmtree(full_path)
            print(" -- deleted")
        except PermissionError:
            print()
            permissions = os.stat(full_path)
            error_print(f"***Permission Error for '{full_path}': {permissions}")


def download_all_resource_types():
    """main entry point into the app"""
    for resource_type in args.resource_types:
        print("\n\n########\n\n")
        resource_list = get_resource_urls_from_server(
            args.endpoint, resource_type, args.headers)
        print(f"got {len(resource_list)} resources of type {resource_type}")
        if len(resource_list) == 0:
            continue
        out_dir = path.join(path.abspath(args.out_dir), today, resource_type)
        if not (path.isdir(out_dir)):
            makedirs(out_dir)
        print(f"downloading with {args.parallel} parallel execution(s)")
        if args.parallel == 1:
            for r in resource_list:
                download_resource_to_file(resource_type, r, out_dir)
                sys.stdout.flush()
        else:
            pool = mp.Pool(args.parallel)
            [pool.apply(download_resource_to_file, args=(resource_type, r, out_dir)) for r in resource_list]
            pool.close()
        sys.stdout.flush()


def download_resource_to_file(resource_type: str, r: BundleResponse, out_dir: str):
    fn = download_resource(resource_type, r, out_dir)
    print(f"   - {r.url} (canonical {r.canonical_url}) -> {fn}")
    sys.stdout.flush()
    return fn


def error_print(*the_args, **the_kwargs):
    """https://stackoverflow.com/a/14981125/2333678"""
    print(*the_args, file=sys.stderr, **the_kwargs)


def create_tarball():
    """create a tarball for the downloaded files"""
    if not args.tarball:
        return
    print("\n\n########\n\n")
    output_path = path.join(path.abspath(args.out_dir), today)
    tar_filename = f"{today}.tar.gz"
    tar_path = path.join(output_path, tar_filename)
    print(f"creating tarball at {tar_path}")
    file_list = sorted([path.join(output_path, f) for f in listdir(output_path) if f != tar_filename])
    with tarfile.open(tar_path, "w:gz") as tar:
        for f in file_list:
            tar.add(f, arcname=f"{today}/{os.path.basename(f)}")
            print(f" - added {f} to tarball")


if __name__ == "__main__":
    today = date.today().isoformat()
    print("##########################################")
    print(f"executing at {datetime.now().isoformat()} UTC")
    print("------------------------------------------")
    args = parse_args()
    download_all_resource_types()
    create_tarball()
    remove_old_directories()
    print("##########################################\n\n")
