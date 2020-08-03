import requests
import argparse
import json
from typing import List, Dict, Any
from dataclasses import dataclass
from datetime import date
from os import path, makedirs


def parse_args(print_args: bool = True) -> argparse.Namespace:
    """create the argument parser

    Args:
        print_args (bool, optional): If true, the arguments will be printed to stdout after parsing. Defaults to True.

    Returns:
        argparse.Namespace: the parsed arguments as a Namespace object
    """
    default_url = "http://localhost:8080/fhir"

    default_resource_types = ["CodeSystem", "ValueSet", "ConceptMap"]

    parser = argparse.ArgumentParser(
        "Terminology Server Backup")
    parser.add_argument("--endpoint",
                        "-e",
                        default=default_url,
                        type=str,
                        help="The FHIR endpoint of the server (Default: '%(default)s')")
    parser.add_argument("--resource-types",
                        "-r",
                        nargs="+",
                        #default=" ".join(default_resource_types),
                        default=default_resource_types,
                        type=str,
                        dest='resource_types',
                        help="the resource types to back-up. Seperate with Space (Default: '%(default)s')")
    parser.add_argument("--header",
                        help="headers to pass to the HTTP method. Use for authentication if required! If multiple required, repeat argument. (Default: None)",
                        dest="headers",
                        action="append")
    parser.add_argument("--out-dir", "-o",
                        default="./output",
                        help="destination directory (Default %(default)s)",
                        dest="out_dir",
                        type=str)

    parsed_args = parser.parse_args()

    parsed_args.endpoint = parsed_args.endpoint.strip("/")
    #parsed_args.resource_types = parsed_args.resource_types.split(" ")

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
            f"HTTP Error {rx.status_code} getting from {request_url}", rx)
    return rx.json()


def get_resource_urls_from_server(fhir_endpoint: str, resource_type: str, headers: List[str] = None) -> List[BundleResponse]:
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

    while next_link != None:
        print("Requesting from: ", next_link)
        bundle_json = perform_request_as_json(next_link, headers)
        bundle_responses += bundlejson_to_bundle_response_list(bundle_json)
        next_link = bundle_json_get_next_link(bundle_json)

    return bundle_responses


def bundle_json_get_next_link(bundle_json: Dict[str, Any]) -> str:
    """get the 'next' link from a bundle, if it exists

    Args:
        bundle_json (Dict[str, Any]): the bundle to examine

    Returns:
        str: the url of the 'next' bundle or None
    """
    filtered_link = [l for l in bundle_json["link"] if l["relation"] == "next"]
    if (len(filtered_link) > 0):
        return filtered_link[0]["url"]
    return None


def bundlejson_to_bundle_response_list(bundle_json: Dict[str, Any]) -> List[BundleResponse]:
    """parse every entry in the bundle as a BundleResponse

    Args:
        bundle_json (Dict[str, Any]): the bundle to parse

    Returns:
        List[BundleResponse]: the parsed entries
    """
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


def download_resource(resource_type: str, r: BundleResponse, out_dir: str, today: str) -> str:
    """download a resource from the fully-qualified url in r to out_dir

    Args:
        resource_type (str): the resource type, used in the output filename
        r (BundleResponse): the parsed entry from the search bundle
        out_dir (str): the output directory
        today (str): the current date (or anything else), used in the output filename. Lifted out to avoid changing filenames around midnight local time.

    Returns:
        str: the fully-qualified output path
    """
    rx = perform_request_as_json(r.url)

    target_filename = f"{resource_type}-{r.resource_id}_{r.title}_{today}.json"
    target_abspath = path.join(out_dir, target_filename)
    with open(target_abspath, "w") as fs:
        json.dump(rx, fs, indent=2)
    return target_abspath


def slugify(value):
    """
    Normalizes string, converts to lowercase, removes non-alpha characters,
    and converts spaces to hyphens.
    via https://stackoverflow.com/a/295466

    """
    import unicodedata
    value = unicodedata.normalize('NFKD', value).encode('ascii', 'ignore')
    value = unicode(re.sub('[^\w\s-]', '', value).strip().lower())
    value = unicode(re.sub('[-\s]+', '-', value))
    return value


def download_all_resource_types(args: argparse.Namespace):
    """main entry point into the app

    Args:
        args (argparse.Namespace): the parsed arguments
    """
    today = date.today().isoformat()
    for resource_type in args.resource_types:
        print("\n\n########\n\n")
        resource_list = get_resource_urls_from_server(
            args.endpoint, resource_type, args.headers)
        print(f"  got resources: ")
        out_dir = path.join(path.abspath(args.out_dir), today, resource_type)
        if not(path.isdir(out_dir)):
            makedirs(out_dir)
        for i, r in enumerate(resource_list):
            print(
                f"   - ({i+1}/{len(resource_list)}) {r.url} (canonical {r.canonical_url}) -> ", end="")
            print(download_resource(resource_type, r, out_dir, today))


if __name__ == "__main__":
    args = parse_args()
    download_all_resource_types(args)
