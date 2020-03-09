import boto3
import json
import argparse
import sys
import webbrowser
import time

desc = """\
A tool to open or close SageMaker Instance"""

parser = argparse.ArgumentParser(description=desc)

parser.add_argument('-o', '--option', help='Choose `open` to open the instance' \
' and `close` to close the instance',required=True)

parser.add_argument('-n', '--nameinstance', help='Name of the Instance',
                    required=True)

parser.add_argument('-p', '--pathjson', help='Local path where the json including ' \
                    'the credentiel is saved Including the file name',
                    required=True)

args = parser.parse_args()
option = args.option
nameinstance = args.nameinstance
pathjson = args.pathjson

if len(sys.argv) == 1:
    parser.print_help()
    sys.exit()

with open(pathjson) as json_file:
    data = json.load(json_file)


client = boto3.client(
    'sagemaker',
    aws_access_key_id=data['aws_access_key_id'],
    aws_secret_access_key=data['aws_secret_access_key'],
)

if option == 'open':
    try:

        client.start_notebook_instance(
        NotebookInstanceName=nameinstance)

        time.sleep(200)
        print("Instance {} is open.".format(nameinstance))
        url = "https://{}.notebook.eu-west-3.sagemaker.aws/lab".format(
        nameinstance)
        webbrowser.open_new(url)
    except Exception as e: print(e)


else:
    print("Instance {} is closed.".format(nameinstance))
    client.stop_notebook_instance(
    NotebookInstanceName=str(nameinstance)
)
