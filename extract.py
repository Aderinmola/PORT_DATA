import requests


link = "https://drive.google.com/file/d/1VyCGCAfFuEK7vB1C9Vq8iPdgBdu-LDM4/view"

def download_google_drive_file(file_name):
    URL = "https://drive.google.com/uc"

    response = requests.get(URL, params= {'id': '1VyCGCAfFuEK7vB1C9Vq8iPdgBdu-LDM4', 'export': 'download', 'confirm': 1}, stream=True)
    
    with open(file_name, "wb") as f:
        f.write(response.content)
    print('Data successfully downloaded to a file!!!')
