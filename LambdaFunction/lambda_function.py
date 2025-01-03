import json
import requests
import os
from datetime import datetime
import boto3

data = {
    "grant_type":"client_credentials",
    "client_id":"CLIENT_ID",
    "client_secret":"CLIENT_SECRET"
}

def authorise():
    print("Trying autorising:")
    try:
        url = "https://accounts.spotify.com/api/token"
        response = requests.post(url,data=data)
        response.raise_for_status()
        if response.status_code==200:
            body = response.json()
            token = body['access_token']
            print("token: ",token)
            return token

    except requests.exceptions.HTTPError as error:
        print(error)    


def getArtist(token):
    print("Getting Artist details:")
    header={"Authorization": f"Bearer {token}"}
    print(header)
    try:
        response = requests.get("https://api.spotify.com/v1/artists/0DyGWIBa9OCOlCex789Xvp?si=57vFJaGRScC9YjlcYqggXw",headers=header)
        response.raise_for_status()
        if response.status_code==200:
            print(response.json())    
    except requests.exceptions.HTTPError as e:
        print(e)   

def getPlaylist(token):
   try: 
    print("Getting Playlist details:")
    header={"Authorization": f"Bearer {token}"}
    globalTop100_1='https://open.spotify.com/playlist/4zzUm9eZmeb4t4nUCaNoo5'
    globalTop100_2='https://open.spotify.com/playlist/37i9dQZEVXbNG2KDcFcKOF'
    playlist_id = (globalTop100_1.split('/')[-1])
    # myPlaylistURL = "https://open.spotify.com/playlist/37i9dQZEVXbNG2KDcFcKOF?si=9o1-_BGwQDaC2aat9uRk_Q"
    # playlist_id = (myPlaylistURL.split('/')[-1]).split('?')[0]
    url = f"https://api.spotify.com/v1/playlists/{playlist_id}"
    # url  = 'https://cdn-api.co-vin.in/api/v2/admin/location/states'
    response = requests.get(url,headers=header)
    response.raise_for_status()
    if response.status_code==200:
        response_dict=response.json()
        if response_dict:
            print("Uploading data to S3 bucket")
            s3_cilent = boto3.client('s3')
            current_time = datetime.now()
            year=current_time.year
            month=current_time.month
            day=current_time.day
            fileName = 'output.json'

            print(current_time)
            try:
                s3_cilent.put_object(
                    Body = json.dumps(response_dict, indent=4, sort_keys=True),
                    Bucket='prayantdatasetbucket',
                    Key = f'myspotifyAPIData/source_raw_data_json/year={year}/month={month}/day={day}/{fileName}')

            except botocore.exceptions.ClientError as error:
                print("Error uploading file to S3 ",error)
                raise error    
        else:
            print("Nothing to write")

    else:
        print("Error")

   except requests.exceptions.HTTPError as e:
        print(e) 


def main():
    print("Hello, All the best. Scipt execution started")
    token='BQAMzq7c37bcqZ_paLRPk28VsilkK39zjOlO6PY6qc4rXyJBs6ihdm6VZf0cpu9MzeawOv3HP01MnhBwE1n9_7YgPCuxKUxKZrbNS9_kEK-Bzww2huw'
    token = authorise()
    getPlaylist(token)

def lambda_handler(event, context):
    main()
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
